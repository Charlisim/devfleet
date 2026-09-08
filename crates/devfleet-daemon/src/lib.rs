//! Local daemon: private IPC, persistent state, and supervisor composition.

use devfleet_core::{
    BindingIntegration as CoreBindingIntegration, BindingPhase as CoreBindingPhase, Invalidation,
    PlanOptions, compile_manifest, discover_git_worktree, metro_adapter, render_metro_config,
    resolve_bindings,
};
use devfleet_logging::LogStore;
use devfleet_protocol::{
    BindingApplyRequest, BindingIntegration, BindingInvalidation, BindingPhase, BindingSnapshot,
    ComposeReconcileRequest, ComposeUpRequest, DaemonRequest, DaemonResponse, EndpointSnapshot,
    EnvironmentPlanSnapshot, EnvironmentSnapshot, ErrorCode, EventSnapshot,
    InitializationRunRequest, InitializationTask, InitializationTaskKind,
    InitializationTaskSnapshot, InitializationTaskStatus, IosAppRunRequest, IosAppSnapshot,
    IosSimulatorAcquireRequest, IosSimulatorAction, IosSimulatorActionRequest,
    IosSimulatorLifecycle, IosSimulatorSnapshot, JsonRpcRequest, JsonRpcResponse, LifecycleState,
    LogSnapshot, MetroReconcileRequest, MetroUpRequest, OperationAccepted, OperationSnapshot,
    PROTOCOL_VERSION, PluginAction, PluginReconcileRequest, PluginSnapshot, PreparationRunRequest,
    PreparationTaskKind, PreparationTaskSnapshot, PreparationTaskStatus, ProcessSnapshot,
    ReadinessState, RpcFailure, SupervisorRequest, SupervisorResponse, SupervisorStartRequest,
    TaskRerunPolicy, TrustedTaskPlugin, read_json_line, write_json_line,
};
use devfleet_state::{
    StateStore, StoredBinding, StoredEndpoint, StoredEnvironment, StoredEvent,
    StoredInitializationTask, StoredIosApp, StoredIosDevice, StoredLifecycle, StoredOperation,
    StoredPreparationTask, StoredProcess, StoredReadiness, VerifiedGitContext,
};
use fs2::FileExt;
use nix::unistd::{getpeereid, getuid};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    net::{SocketAddr, TcpStream},
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        net::UnixListener,
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use uuid::Uuid;

const SOCKET_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const SOCKET_RETRY_INTERVAL: Duration = Duration::from_millis(25);
const SUPERVISOR_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_SOCKET_PATH_BYTES: usize = 96;
const MAX_RESOURCE_LOG_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct DaemonConfig {
    pub state_dir: PathBuf,
    pub socket: PathBuf,
    pub supervisor: PathBuf,
    pub process_plugin: PathBuf,
}

/// Returns the short, per-user private Unix-socket directory for this host.
#[must_use]
pub fn default_socket_dir() -> PathBuf {
    PathBuf::from(format!("/tmp/df-{}", getuid().as_raw()))
}

/// Runs one foreground daemon until its process is terminated.
///
/// # Errors
///
/// Returns an error when private storage, the writer lock, socket, state store,
/// or a client request cannot be initialized.
pub fn run(config: DaemonConfig) -> anyhow::Result<()> {
    prepare_private_dir(&config.state_dir)?;
    let _writer_lock = acquire_writer_lock(&config.state_dir)?;
    let socket_parent = config
        .socket
        .parent()
        .ok_or_else(|| anyhow::anyhow!("daemon socket has no parent directory"))?;
    prepare_private_dir(socket_parent)?;
    ensure_short_socket_path(&config.socket)?;
    clear_stale_daemon_socket(&config.socket)?;
    let listener = UnixListener::bind(&config.socket)?;
    let _socket_cleanup = DaemonSocketCleanup(config.socket.clone());
    fs::set_permissions(&config.socket, fs::Permissions::from_mode(0o600))?;
    let mut state = StateStore::open(&config.state_dir.join("state.sqlite3"))?;
    reconcile_supervisors(&mut state)?;
    let mut daemon = Daemon { config, state };

    loop {
        let (stream, _) = listener.accept()?;
        assert_same_user(&stream)?;
        if let Err(error) = daemon.handle_connection(stream) {
            eprintln!("daemon client request failed: {error:#}");
        }
    }
}

struct Daemon {
    config: DaemonConfig,
    state: StateStore,
}

#[derive(serde::Deserialize)]
struct IosDiscovery {
    devices: Vec<IosDiscoveredDevice>,
}

#[derive(serde::Deserialize)]
struct IosDiscoveredDevice {
    udid: String,
    name: String,
    available: bool,
    state: String,
}

impl Daemon {
    fn handle_connection(
        &mut self,
        mut stream: std::os::unix::net::UnixStream,
    ) -> anyhow::Result<()> {
        let request = read_json_line::<JsonRpcRequest>(&mut BufReader::new(stream.try_clone()?))?;
        let response = if request.jsonrpc != "2.0" || request.method != "daemon.request" {
            JsonRpcResponse::failure(
                request.id,
                RpcFailure {
                    code: ErrorCode::ProtocolInvalid,
                    message: "expected daemon.request JSON-RPC 2.0 method".into(),
                    retryable: false,
                },
            )
        } else {
            match serde_json::from_value::<DaemonRequest>(request.params.clone()) {
                Ok(command) => match self.handle(command) {
                    Ok(response) => JsonRpcResponse::success(request.id, &response),
                    Err(error) => JsonRpcResponse::failure(request.id, error.into()),
                },
                Err(_) => JsonRpcResponse::failure(
                    request.id,
                    RpcFailure {
                        code: ErrorCode::ProtocolInvalid,
                        message: "invalid daemon request parameters".into(),
                        retryable: false,
                    },
                ),
            }
        };
        stream.write_all(&write_json_line(&response)?)?;
        stream.flush()?;
        Ok(())
    }

    fn handle(&mut self, request: DaemonRequest) -> Result<DaemonResponse, DaemonError> {
        match request {
            DaemonRequest::Plan(request) => self.plan(request),
            DaemonRequest::Up(request) => self.up(request),
            DaemonRequest::ComposeUp(request) => self.compose_up(request),
            DaemonRequest::MetroUp(request) => self.metro_up(request),
            DaemonRequest::InitializationRun(request) => self.run_initialization(&request),
            DaemonRequest::PreparationRun(request) => self.run_preparation(&request),
            DaemonRequest::BindingApply(request) => self.apply_bindings(request),
            DaemonRequest::IosSimulatorAcquire(request) => self.ios_simulator_acquire(&request),
            DaemonRequest::IosSimulatorAction(request) => self.ios_simulator_action(&request),
            DaemonRequest::IosAppRun(request) => self.ios_app_run(&request),
            DaemonRequest::Status { environment_id } => self.status(environment_id),
            DaemonRequest::List => self.list(),
            DaemonRequest::Stop { environment_id } => self.stop(environment_id),
            DaemonRequest::OperationGet { operation_id } => self.operation_get(operation_id),
            DaemonRequest::OperationCancel { operation_id } => self.operation_cancel(operation_id),
            DaemonRequest::Events {
                environment_id,
                after,
            }
            | DaemonRequest::EventsSubscribe {
                environment_id,
                after,
            } => self.events(environment_id, after),
            DaemonRequest::LogsTail {
                environment_id,
                resource,
                after,
                limit,
            } => self.logs_tail(environment_id, &resource, after, limit),
            DaemonRequest::PluginList => self.plugin_list(),
            DaemonRequest::PluginInstall { path } => self.plugin_install(Path::new(&path)),
            DaemonRequest::PluginTrust { id } => self.plugin_trust(&id),
        }
    }

    fn plan(
        &mut self,
        request: devfleet_protocol::EnvironmentPlanRequest,
    ) -> Result<DaemonResponse, DaemonError> {
        if request.profile.is_empty() {
            return Err(DaemonError::InvalidConfig("profile is required".into()));
        }
        let git_identity = discover_git_worktree(Path::new(&request.worktree))
            .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        let manifest = canonical_plan_path(&request.manifest, &git_identity.checkout)?;
        let local_override = request
            .local_override
            .as_deref()
            .map(|path| canonical_plan_path(path, &git_identity.checkout))
            .transpose()?;
        let known_providers = self
            .state
            .plugins()?
            .into_iter()
            .map(|plugin| plugin.id)
            .collect();
        let environment_key = environment_key(
            &git_identity.project_fingerprint,
            &git_identity.worktree_fingerprint,
            &request.profile,
            None,
        );
        let environment = BTreeMap::from([
            ("id".into(), serde_json::json!(environment_key)),
            (
                "state_dir".into(),
                serde_json::json!(self.config.state_dir.display().to_string()),
            ),
            (
                "generation".into(),
                serde_json::json!("${{ environment.generation }}"),
            ),
        ]);
        let options = PlanOptions {
            known_providers,
            available_secret_references: request
                .secret_references
                .into_iter()
                .collect::<BTreeSet<_>>(),
            runtime_outputs: request.runtime_outputs,
            planning_endpoints: request.planning_endpoints,
            operation_overrides: request.operation_overrides,
            environment,
        };
        let plan = compile_manifest(
            &manifest,
            local_override.as_deref(),
            &request.profile,
            &options,
        )
        .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        let stored = self.state.record_effective_plan(
            &git_identity.project_fingerprint,
            &git_identity.worktree_fingerprint,
            &git_identity.checkout.display().to_string(),
            &request.profile,
            &plan.effective_configuration,
        )?;
        let bindings = self
            .state
            .environments()?
            .into_iter()
            .find(|environment| {
                environment.project_fingerprint == git_identity.project_fingerprint
                    && environment.worktree_fingerprint == git_identity.worktree_fingerprint
                    && environment.profile == request.profile
            })
            .map(|environment| self.state.bindings(environment.id))
            .transpose()?
            .unwrap_or_default()
            .into_iter()
            .map(binding_snapshot)
            .collect();
        Ok(DaemonResponse::Plan(EnvironmentPlanSnapshot {
            worktree: stored.worktree,
            profile: stored.profile,
            generation: stored.generation,
            effective_configuration: stored.effective_configuration,
            bindings,
        }))
    }

    fn apply_bindings(
        &mut self,
        request: BindingApplyRequest,
    ) -> Result<DaemonResponse, DaemonError> {
        let environment = self
            .state
            .environment(request.environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let contract = resolve_bindings(
            core_phase(request.phase),
            core_integration(request.integration),
            request.public_values,
            request.backend_only_values,
            request.reserved_endpoints,
        )
        .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        let directory = self
            .config
            .state_dir
            .join("bindings")
            .join(environment.id.to_string());
        prepare_private_dir(&directory)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let path = directory.join(format!("{}.json", phase_name(contract.phase)));
        let private = serde_json::json!({
            "phase": contract.phase,
            "integration": contract.integration,
            "public_values": contract.public_values,
            "backend_only_values": contract.backend_only_values,
        });
        write_private_atomically(
            &path,
            &serde_json::to_vec(&private)
                .map_err(|error| DaemonError::Operation(error.to_string()))?,
        )?;
        let public = serde_json::to_string(&contract.public_values)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        self.state.record_binding(&StoredBinding {
            environment_id: environment.id,
            phase: phase_name(contract.phase).into(),
            integration: integration_name(contract.integration).into(),
            fingerprint: contract.fingerprint,
            invalidation: invalidation_name(contract.invalidation).into(),
            public_values_json: public,
            private_path: path.display().to_string(),
        })?;
        self.emit_event(
            environment.id,
            environment.generation,
            None,
            "binding_applied",
        )?;
        Ok(DaemonResponse::Bindings(
            self.state
                .bindings(environment.id)?
                .into_iter()
                .map(binding_snapshot)
                .collect(),
        ))
    }

    #[allow(clippy::too_many_lines)]
    fn up(
        &mut self,
        request: devfleet_protocol::EnvironmentUpRequest,
    ) -> Result<DaemonResponse, DaemonError> {
        validate_up_request(&request)?;
        let process_plugin = self.trusted_process_plugin()?;
        let git_identity = discover_git_worktree(Path::new(&request.worktree))
            .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        let git_context = VerifiedGitContext {
            common_git_dir: git_identity.common_git_dir.display().to_string(),
            git_dir: git_identity.git_dir.display().to_string(),
            worktree: request.worktree.clone(),
            head: git_identity.head.clone(),
            branch: git_identity.branch.clone(),
            detached: git_identity.detached,
        };
        let environments = self.state.environments()?;
        let matching_worktree = environments.iter().find(|environment| {
            environment.project_fingerprint == git_identity.project_fingerprint
                && environment.worktree_fingerprint == git_identity.worktree_fingerprint
        });
        let matching_environment = environments.iter().find(|environment| {
            environment.project_fingerprint == git_identity.project_fingerprint
                && environment.worktree_fingerprint == git_identity.worktree_fingerprint
                && environment.profile == request.profile
                && environment.instance == request.instance
        });
        if let Some(environment) = matching_environment.filter(|environment| {
            matches!(
                environment.lifecycle,
                StoredLifecycle::Starting | StoredLifecycle::Running
            )
        }) {
            self.state
                .update_worktree_context(environment.id, &git_context)?;
            return Ok(DaemonResponse::Accepted(OperationAccepted {
                operation_id: Uuid::nil(),
                environment_id: environment.id,
            }));
        }
        cleanup_stopped_supervisor_socket(matching_environment)?;

        let project_id = environments
            .iter()
            .find(|environment| environment.project_fingerprint == git_identity.project_fingerprint)
            .map_or_else(Uuid::now_v7, |environment| environment.project_id);
        let worktree_id =
            matching_worktree.map_or_else(Uuid::now_v7, |environment| environment.worktree_id);

        self.state.orphan_replaced_worktree(
            &request.worktree,
            &request.profile,
            request.instance.as_deref(),
            &git_identity.project_fingerprint,
            &git_identity.worktree_fingerprint,
        )?;

        let environment = StoredEnvironment {
            id: matching_environment.map_or_else(Uuid::now_v7, |environment| environment.id),
            key: environment_key(
                &git_identity.project_fingerprint,
                &git_identity.worktree_fingerprint,
                &request.profile,
                request.instance.as_deref(),
            ),
            project_id,
            worktree_id,
            project_fingerprint: git_identity.project_fingerprint,
            worktree_fingerprint: git_identity.worktree_fingerprint,
            common_git_dir: git_context.common_git_dir,
            git_dir: git_context.git_dir,
            worktree: request.worktree,
            profile: request.profile,
            instance: request.instance,
            head: git_context.head,
            branch: git_context.branch,
            detached: git_context.detached,
            generation: 1,
            lifecycle: StoredLifecycle::Starting,
            reconciliation_diagnostic: None,
            process: None,
            endpoints: Vec::new(),
        };
        let operation_id = Uuid::now_v7();
        self.state.record_start_intent(&environment, operation_id)?;
        self.emit_event(
            environment.id,
            environment.generation,
            Some(operation_id),
            "up_accepted",
        )?;
        self.state.set_operation_context(
            operation_id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| DaemonError::Operation(error.to_string()))?
                .as_millis()
                + 60_000,
            "process",
        )?;
        let control_socket = supervisor_socket_path(environment.id)?;
        let log_directory = log_directory(&self.config.state_dir, environment.id);
        prepare_private_dir(&log_directory)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let redactions = known_secret_values(&request.process.environment);
        let start = SupervisorStartRequest {
            environment_id: environment.id,
            operation_id,
            action: PluginAction::Reconcile(PluginReconcileRequest {
                protocol: PROTOCOL_VERSION,
                operation_id,
                process: request.process,
            }),
            log_directory: log_directory.display().to_string(),
            log_max_bytes: MAX_RESOURCE_LOG_BYTES,
            redactions,
            task_plugins: self.trusted_task_plugins()?,
        };
        if let Err(error) = self.start_supervisor(&control_socket, &process_plugin, &start) {
            self.state.mark_failed(environment.id)?;
            return Err(error);
        }
        let response = request_supervisor(&control_socket, SupervisorRequest::Inspect)?;
        let process = supervisor_process(response)?;
        self.state
            .record_running_process(environment.id, &stored_process(&process))?;
        self.emit_event(
            environment.id,
            environment.generation,
            Some(operation_id),
            "process_running",
        )?;
        if process.readiness != ReadinessState::ProcessAlive {
            self.state.mark_failed(environment.id)?;
            return Err(DaemonError::Operation(
                "process exited before its initial readiness observation".into(),
            ));
        }
        Ok(DaemonResponse::Accepted(OperationAccepted {
            operation_id,
            environment_id: environment.id,
        }))
    }

    #[allow(clippy::too_many_lines)]
    fn metro_up(&mut self, request: MetroUpRequest) -> Result<DaemonResponse, DaemonError> {
        if !request.trusted
            || request.profile.is_empty()
            || request.port == 0
            || request.max_workers == 0
        {
            return Err(if request.trusted {
                DaemonError::InvalidConfig("Metro requires profile, port, and worker budget".into())
            } else {
                DaemonError::TrustRequired
            });
        }
        let worktree = Path::new(&request.worktree)
            .canonicalize()
            .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        let cache_key = format!(
            "{:x}",
            Sha256::digest(format!(
                "{}|{}|{}",
                worktree.display(),
                request.profile,
                request.instance.as_deref().unwrap_or_default()
            ))
        );
        let directory = self.config.state_dir.join("metro").join(&cache_key);
        prepare_private_dir(&directory)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let adapter = metro_adapter(
            worktree.clone(),
            directory.join("cache"),
            request.port,
            request.max_workers,
            cache_key[..16].into(),
            core_integration(request.binding_integration),
        )
        .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        let expected_identity = adapter.environment_identity.clone();
        prepare_private_dir(&adapter.cache_directory)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let configuration_path = adapter.cache_directory.join("metro.config.cjs");
        write_private_atomically(
            &configuration_path,
            render_metro_config(&adapter).as_bytes(),
        )?;
        let plugin = self.trusted_plugin("official.metro", "metro.reconcile")?;
        let spawn: devfleet_protocol::HostSpawnRequest =
            serde_json::from_value(Self::plugin_action(
                &plugin,
                "metro.reconcile",
                serde_json::to_value(MetroReconcileRequest {
                    protocol: PROTOCOL_VERSION,
                    operation_id: Uuid::now_v7(),
                    environment_id: Uuid::nil(),
                    worktree: worktree.display().to_string(),
                    port: request.port,
                    cache_directory: adapter.cache_directory.display().to_string(),
                    configuration_path: configuration_path.display().to_string(),
                    max_workers: request.max_workers,
                })
                .map_err(|error| DaemonError::Operation(error.to_string()))?,
            )?)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let response = self.up(devfleet_protocol::EnvironmentUpRequest {
            worktree: worktree.display().to_string(),
            profile: request.profile,
            instance: request.instance,
            trusted: true,
            process: devfleet_protocol::ProcessSpec {
                argv: spawn.argv,
                cwd: spawn.cwd,
                environment: spawn.environment,
            },
        })?;
        let DaemonResponse::Accepted(accepted) = response else {
            return Err(DaemonError::Operation(
                "Metro startup returned an unexpected response".into(),
            ));
        };
        if !wait_for_metro_identity(request.port, &expected_identity) {
            let _ = self.stop(accepted.environment_id);
            return Err(DaemonError::Operation(
                "Metro did not expose the selected endpoint identity before its deadline".into(),
            ));
        }
        let endpoint = StoredEndpoint {
            name: "metro".into(),
            service: "metro".into(),
            internal_url: format!("http://127.0.0.1:{}", request.port),
            host_url: format!("http://127.0.0.1:{}", request.port),
            device_url: format!("http://127.0.0.1:{}", request.port),
            readiness_path: "/devfleet-identity".into(),
            healthy: true,
        };
        self.state
            .set_endpoints(accepted.environment_id, &[endpoint])?;
        self.emit_event(
            accepted.environment_id,
            1,
            Some(accepted.operation_id),
            "metro_ready",
        )?;
        self.status(accepted.environment_id)
    }

    #[allow(clippy::too_many_lines)]
    fn compose_up(&mut self, request: ComposeUpRequest) -> Result<DaemonResponse, DaemonError> {
        validate_compose_up_request(&request)?;
        let compose_plugin = self.trusted_plugin("official.compose", "compose.reconcile")?;
        let git_identity = discover_git_worktree(Path::new(&request.worktree))
            .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        let git_context = VerifiedGitContext {
            common_git_dir: git_identity.common_git_dir.display().to_string(),
            git_dir: git_identity.git_dir.display().to_string(),
            worktree: request.worktree.clone(),
            head: git_identity.head.clone(),
            branch: git_identity.branch.clone(),
            detached: git_identity.detached,
        };
        let environments = self.state.environments()?;
        let matching_worktree = environments.iter().find(|environment| {
            environment.project_fingerprint == git_identity.project_fingerprint
                && environment.worktree_fingerprint == git_identity.worktree_fingerprint
        });
        let matching_environment = environments.iter().find(|environment| {
            environment.project_fingerprint == git_identity.project_fingerprint
                && environment.worktree_fingerprint == git_identity.worktree_fingerprint
                && environment.profile == request.profile
                && environment.instance == request.instance
        });
        if let Some(environment) = matching_environment.filter(|environment| {
            matches!(
                environment.lifecycle,
                StoredLifecycle::Starting | StoredLifecycle::Running
            )
        }) {
            self.state
                .update_worktree_context(environment.id, &git_context)?;
            return Ok(DaemonResponse::Accepted(OperationAccepted {
                operation_id: Uuid::nil(),
                environment_id: environment.id,
            }));
        }
        cleanup_stopped_supervisor_socket(matching_environment)?;
        let project_id = environments
            .iter()
            .find(|environment| environment.project_fingerprint == git_identity.project_fingerprint)
            .map_or_else(Uuid::now_v7, |environment| environment.project_id);
        let worktree_id =
            matching_worktree.map_or_else(Uuid::now_v7, |environment| environment.worktree_id);
        self.state.orphan_replaced_worktree(
            &request.worktree,
            &request.profile,
            request.instance.as_deref(),
            &git_identity.project_fingerprint,
            &git_identity.worktree_fingerprint,
        )?;
        let environment = StoredEnvironment {
            id: matching_environment.map_or_else(Uuid::now_v7, |environment| environment.id),
            key: environment_key(
                &git_identity.project_fingerprint,
                &git_identity.worktree_fingerprint,
                &request.profile,
                request.instance.as_deref(),
            ),
            project_id,
            worktree_id,
            project_fingerprint: git_identity.project_fingerprint,
            worktree_fingerprint: git_identity.worktree_fingerprint,
            common_git_dir: git_context.common_git_dir,
            git_dir: git_context.git_dir,
            worktree: request.worktree.clone(),
            profile: request.profile,
            instance: request.instance,
            head: git_context.head,
            branch: git_context.branch,
            detached: git_context.detached,
            generation: 1,
            lifecycle: StoredLifecycle::Starting,
            reconciliation_diagnostic: None,
            process: None,
            endpoints: stored_compose_endpoints(&request.endpoints),
        };
        let operation_id = Uuid::now_v7();
        self.state.record_start_intent(&environment, operation_id)?;
        self.emit_event(
            environment.id,
            environment.generation,
            Some(operation_id),
            "compose_up_accepted",
        )?;
        self.state.set_operation_context(
            operation_id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| DaemonError::Operation(error.to_string()))?
                .as_millis()
                + 60_000,
            "compose",
        )?;
        let control_socket = supervisor_socket_path(environment.id)?;
        let log_directory = log_directory(&self.config.state_dir, environment.id);
        prepare_private_dir(&log_directory)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let redactions = known_secret_values(&request.environment);
        let start = SupervisorStartRequest {
            environment_id: environment.id,
            operation_id,
            action: PluginAction::ComposeReconcile(Box::new(ComposeReconcileRequest {
                protocol: PROTOCOL_VERSION,
                operation_id,
                environment_id: environment.id,
                worktree: request.worktree,
                project_name: compose_project_name(environment.project_id, environment.id),
                files: request.files,
                environment: request.environment,
                endpoints: request.endpoints,
                authorizations: request.authorizations,
            })),
            log_directory: log_directory.display().to_string(),
            log_max_bytes: MAX_RESOURCE_LOG_BYTES,
            redactions,
            task_plugins: self.trusted_task_plugins()?,
        };
        if let Err(error) = self.start_supervisor(&control_socket, &compose_plugin, &start) {
            self.state.mark_failed(environment.id)?;
            return Err(error);
        }
        let response = request_supervisor(&control_socket, SupervisorRequest::Inspect)?;
        let process = supervisor_process(response)?;
        self.state
            .record_running_process(environment.id, &stored_process(&process))?;
        self.emit_event(
            environment.id,
            environment.generation,
            Some(operation_id),
            "compose_running",
        )?;
        if process.readiness != ReadinessState::ProcessAlive {
            self.state.mark_failed(environment.id)?;
            return Err(DaemonError::Operation(
                "Compose exited before its initial readiness observation".into(),
            ));
        }
        Ok(DaemonResponse::Accepted(OperationAccepted {
            operation_id,
            environment_id: environment.id,
        }))
    }

    #[allow(clippy::too_many_lines)]
    fn run_initialization(
        &mut self,
        request: &InitializationRunRequest,
    ) -> Result<DaemonResponse, DaemonError> {
        validate_initialization_tasks(&request.tasks)?;
        let environment = self
            .state
            .environment(request.environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let process = environment
            .process
            .clone()
            .ok_or(DaemonError::OwnershipUncertain)?;
        if !matches!(
            environment.lifecycle,
            StoredLifecycle::Running | StoredLifecycle::Starting
        ) {
            return Err(DaemonError::Operation(
                "initialization requires a running owned environment".into(),
            ));
        }
        let ordered = ordered_initialization_tasks(&request.tasks)?;
        let prior = self
            .state
            .initialization_tasks(environment.id)?
            .into_iter()
            .map(|task| (task.id.clone(), task))
            .collect::<BTreeMap<_, _>>();
        for task in ordered {
            let fingerprint = task_fingerprint(task)?;
            if let Some(stored) = prior.get(&task.id) {
                if stored.status == "running" && stored.fingerprint == fingerprint {
                    return Err(DaemonError::Operation(format!(
                        "initialization task '{}' has an uncertain prior outcome; inspect it before an explicit rerun",
                        task.id
                    )));
                }
                if task_is_complete_for_policy(stored, task, environment.generation, &fingerprint) {
                    self.emit_event(
                        environment.id,
                        environment.generation,
                        None,
                        "initialization_task_skipped",
                    )?;
                    continue;
                }
            }
            let operation_id = Uuid::now_v7();
            let stored = StoredInitializationTask {
                environment_id: environment.id,
                id: task.id.clone(),
                kind: task_kind_name(task.kind).into(),
                rerun_policy: rerun_policy_name(task.rerun_policy).into(),
                fingerprint,
                generation: environment.generation,
                attempts: 0,
                status: "running".into(),
                diagnostic: None,
            };
            self.state.record_initialization_task_intent(&stored)?;
            self.emit_event(
                environment.id,
                environment.generation,
                Some(operation_id),
                "initialization_task_running",
            )?;
            let response = request_supervisor(
                Path::new(&process.supervisor_socket),
                SupervisorRequest::RunTask {
                    operation_id,
                    process: task.process.clone(),
                },
            )?;
            match response {
                SupervisorResponse::TaskCompleted => {
                    self.state
                        .complete_initialization_task(environment.id, &task.id)?;
                    self.emit_event(
                        environment.id,
                        environment.generation,
                        Some(operation_id),
                        "initialization_task_completed",
                    )?;
                }
                SupervisorResponse::TaskFailed { .. } => {
                    let diagnostic = "task process exited unsuccessfully";
                    self.state
                        .fail_initialization_task(environment.id, &task.id, diagnostic)?;
                    self.emit_event(
                        environment.id,
                        environment.generation,
                        Some(operation_id),
                        "initialization_task_failed",
                    )?;
                    return Err(DaemonError::Operation(format!("{}: {diagnostic}", task.id)));
                }
                SupervisorResponse::Process(_)
                | SupervisorResponse::TaskOutput { .. }
                | SupervisorResponse::TaskTimedOut => {
                    return Err(DaemonError::Operation(
                        "supervisor returned a process response for an initialization task".into(),
                    ));
                }
            }
        }
        Ok(DaemonResponse::InitializationTasks(
            self.state
                .initialization_tasks(environment.id)?
                .into_iter()
                .map(initialization_task_snapshot)
                .collect(),
        ))
    }

    #[allow(clippy::too_many_lines)]
    fn run_preparation(
        &mut self,
        request: &PreparationRunRequest,
    ) -> Result<DaemonResponse, DaemonError> {
        if request.timeout_seconds == 0 || request.timeout_seconds > 3_600 {
            return Err(DaemonError::InvalidConfig(
                "preparation deadline must be between one second and one hour".into(),
            ));
        }
        let environment = self
            .state
            .environment(request.environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let process = environment
            .process
            .clone()
            .ok_or(DaemonError::OwnershipUncertain)?;
        if !matches!(
            environment.lifecycle,
            StoredLifecycle::Running | StoredLifecycle::Starting
        ) {
            return Err(DaemonError::Operation(
                "preparation requires a running owned environment".into(),
            ));
        }
        let prior = self
            .state
            .preparation_tasks(environment.id)?
            .into_iter()
            .map(|task| (task.kind.clone(), task))
            .collect::<BTreeMap<_, _>>();
        for (kind, capability) in [
            (
                PreparationTaskKind::Dependencies,
                "preparation.dependencies",
            ),
            (PreparationTaskKind::Pods, "preparation.pods"),
        ] {
            let name = preparation_kind_name(kind);
            let fingerprint = preparation_fingerprint(Path::new(&environment.worktree), kind)?;
            if let Some(stored) = prior.get(name) {
                if stored.status == "running" && stored.fingerprint == fingerprint {
                    return Err(DaemonError::Operation(format!(
                        "preparation task '{name}' has an uncertain prior outcome; inspect it before an explicit rerun"
                    )));
                }
                if stored.status == "completed" && stored.fingerprint == fingerprint {
                    self.emit_event(
                        environment.id,
                        environment.generation,
                        None,
                        "preparation_task_skipped",
                    )?;
                    continue;
                }
            }
            let operation_id = Uuid::now_v7();
            self.state
                .record_preparation_intent(&StoredPreparationTask {
                    environment_id: environment.id,
                    kind: name.into(),
                    fingerprint,
                    attempts: 0,
                    status: "running".into(),
                    diagnostic: None,
                })?;
            self.emit_event(
                environment.id,
                environment.generation,
                Some(operation_id),
                "preparation_task_running",
            )?;
            let response = request_supervisor(
                Path::new(&process.supervisor_socket),
                SupervisorRequest::RunPluginTask {
                    operation_id,
                    plugin_id: "official.react-native".into(),
                    action: PluginAction::Action {
                        name: capability.into(),
                        input: serde_json::json!({
                            "worktree": environment.worktree,
                            "timeout_seconds": request.timeout_seconds,
                        }),
                    },
                    timeout_seconds: Some(request.timeout_seconds),
                },
            )
            .map_err(|error| match error {
                DaemonError::Operation(message)
                    if message.contains("requested plugin capability is not authorized") =>
                {
                    DaemonError::CapabilityUnsupported(capability.into())
                }
                error => error,
            })?;
            let diagnostic = match response {
                SupervisorResponse::TaskOutput { stdout, stderr } => {
                    self.append_preparation_log(environment.id, name, &stdout, &stderr)?;
                    None
                }
                SupervisorResponse::TaskCompleted => {
                    Some("supervisor returned a generic task response")
                }
                SupervisorResponse::TaskFailed { stdout, stderr } => {
                    self.append_preparation_log(environment.id, name, &stdout, &stderr)?;
                    Some("task process exited unsuccessfully")
                }
                SupervisorResponse::TaskTimedOut => Some("task deadline exceeded"),
                SupervisorResponse::Process(_) => Some("supervisor returned a process response"),
            };
            self.state
                .finish_preparation_task(environment.id, name, diagnostic)?;
            self.emit_event(
                environment.id,
                environment.generation,
                Some(operation_id),
                if diagnostic.is_some() {
                    "preparation_task_failed"
                } else {
                    "preparation_task_completed"
                },
            )?;
            if let Some(diagnostic) = diagnostic {
                return Err(DaemonError::Operation(format!("{name}: {diagnostic}")));
            }
        }
        Ok(DaemonResponse::PreparationTasks(
            self.state
                .preparation_tasks(environment.id)?
                .into_iter()
                .map(preparation_task_snapshot)
                .collect(),
        ))
    }

    #[allow(clippy::too_many_lines)]
    fn ios_simulator_acquire(
        &mut self,
        request: &IosSimulatorAcquireRequest,
    ) -> Result<DaemonResponse, DaemonError> {
        let environment = self
            .state
            .environment(request.environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let process = environment
            .process
            .clone()
            .ok_or(DaemonError::OwnershipUncertain)?;
        if !matches!(
            environment.lifecycle,
            StoredLifecycle::Running | StoredLifecycle::Starting
        ) {
            return Err(DaemonError::OwnershipUncertain);
        }
        let plugin = self.trusted_plugin("official.ios", "device.discover")?;
        let mut device = match self.state.ios_device(environment.id)? {
            Some(device) if device.udid.is_some() => {
                if device.runtime != request.runtime || device.device_type != request.device_type {
                    return Err(DaemonError::InvalidConfig(
                        "the environment already owns a simulator with a different pinned runtime or device type"
                            .into(),
                    ));
                }
                device
            }
            Some(device) => {
                let discovery = Self::ios_discovery(&plugin)?;
                let partials = discovery
                    .devices
                    .iter()
                    .filter(|candidate| candidate.name == device.expected_name)
                    .count();
                self.state.set_ios_lifecycle(environment.id, "uncertain")?;
                return Err(DaemonError::OwnershipUncertainPartial(partials));
            }
            None => {
                let reservation = StoredIosDevice {
                    environment_id: environment.id,
                    udid: None,
                    runtime: request.runtime.clone(),
                    device_type: request.device_type.clone(),
                    expected_name: format!("DevFleet-{}", environment.id),
                    lifecycle: "reserved".into(),
                };
                self.state.record_ios_reservation(&reservation)?;
                let output = Self::run_ios_task(
                    &process,
                    "device.create",
                    serde_json::json!({
                        "runtime": request.runtime,
                        "device_type": request.device_type,
                        "name": reservation.expected_name,
                    }),
                )?;
                let udid = Uuid::parse_str(output.trim())
                    .map_err(|_| {
                        DaemonError::DeviceUnavailable("simctl create returned no UDID".into())
                    })?
                    .to_string();
                if !self.state.bind_ios_udid(environment.id, &udid)? {
                    self.state.set_ios_lifecycle(environment.id, "uncertain")?;
                    return Err(DaemonError::OwnershipUncertain);
                }
                self.emit_event(
                    environment.id,
                    environment.generation,
                    None,
                    "ios_device_reserved",
                )?;
                self.state
                    .ios_device(environment.id)?
                    .ok_or(DaemonError::OwnershipUncertain)?
            }
        };
        let udid = device.udid.clone().ok_or(DaemonError::OwnershipUncertain)?;
        let discovery = Self::ios_discovery(&plugin)?;
        let present = discovery
            .devices
            .iter()
            .find(|candidate| candidate.udid.eq_ignore_ascii_case(&udid))
            .filter(|candidate| candidate.available)
            .ok_or_else(|| DaemonError::DeviceUnavailable(udid.clone()))?;
        if present.state != "Booted" {
            Self::run_ios_task(
                &process,
                "device.boot",
                serde_json::json!({"udid": udid, "step": "start"}),
            )?;
        }
        Self::run_ios_task(
            &process,
            "device.boot",
            serde_json::json!({"udid": udid, "step": "wait"}),
        )?;
        self.state.set_ios_lifecycle(environment.id, "booted")?;
        self.emit_event(
            environment.id,
            environment.generation,
            None,
            "ios_device_booted",
        )?;
        device.lifecycle = "booted".into();
        Ok(DaemonResponse::IosSimulator(ios_snapshot(device)?))
    }

    fn ios_simulator_action(
        &mut self,
        request: &IosSimulatorActionRequest,
    ) -> Result<DaemonResponse, DaemonError> {
        let environment = self
            .state
            .environment(request.environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let process = environment
            .process
            .clone()
            .ok_or(DaemonError::OwnershipUncertain)?;
        let device =
            self.state
                .ios_device(environment.id)?
                .ok_or(DaemonError::DeviceUnavailable(
                    "environment has no owned simulator".into(),
                ))?;
        let udid = device.udid.clone().ok_or(DaemonError::OwnershipUncertain)?;
        let (capability, lifecycle) = match request.action {
            IosSimulatorAction::Open => ("device.open", "booted"),
            IosSimulatorAction::Stop => ("device.stop", "stopped"),
        };
        self.trusted_plugin("official.ios", capability)?;
        Self::run_ios_task(&process, capability, serde_json::json!({"udid": udid}))?;
        self.state.set_ios_lifecycle(environment.id, lifecycle)?;
        self.emit_event(
            environment.id,
            environment.generation,
            None,
            match request.action {
                IosSimulatorAction::Open => "ios_device_opened",
                IosSimulatorAction::Stop => "ios_device_stopped",
            },
        )?;
        let device = self
            .state
            .ios_device(environment.id)?
            .ok_or(DaemonError::OwnershipUncertain)?;
        Ok(DaemonResponse::IosSimulator(ios_snapshot(device)?))
    }

    #[allow(clippy::too_many_lines)]
    fn ios_app_run(&mut self, request: &IosAppRunRequest) -> Result<DaemonResponse, DaemonError> {
        let environment = self
            .state
            .environment(request.environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let process = environment
            .process
            .clone()
            .ok_or(DaemonError::OwnershipUncertain)?;
        if environment.lifecycle != StoredLifecycle::Running {
            return Err(DaemonError::Operation(
                "iOS app launch requires a running owned environment".into(),
            ));
        }
        let device = self
            .state
            .ios_device(environment.id)?
            .filter(|device| device.lifecycle == "booted")
            .ok_or_else(|| {
                DaemonError::DeviceUnavailable("environment has no booted owned simulator".into())
            })?;
        let udid = device.udid.ok_or(DaemonError::OwnershipUncertain)?;
        let metro = self
            .state
            .environments()?
            .into_iter()
            .filter(|candidate| {
                candidate.worktree_id == environment.worktree_id
                    && candidate.lifecycle == StoredLifecycle::Running
            })
            .flat_map(|candidate| candidate.endpoints)
            .filter(|endpoint| endpoint.name == "metro" && endpoint.healthy)
            .collect::<Vec<_>>();
        if metro.len() != 1 {
            return Err(DaemonError::Operation(
                "iOS app launch requires exactly one healthy Metro endpoint for this worktree"
                    .into(),
            ));
        }
        let metro_port = endpoint_port(&metro[0].device_url)?;
        let derived_data = self
            .config
            .state_dir
            .join("ios-builds")
            .join(environment.id.to_string())
            .join("DerivedData");
        prepare_private_dir(&derived_data)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let native_fingerprint = native_build_fingerprint(
            Path::new(&environment.worktree),
            metro_port,
            request.scheme.as_deref(),
        )?;
        let operation_id = Uuid::now_v7();
        self.emit_event(
            environment.id,
            environment.generation,
            Some(operation_id),
            "ios_app_launch_running",
        )?;
        let response = request_supervisor(
            Path::new(&process.supervisor_socket),
            SupervisorRequest::RunPluginTask {
                operation_id,
                plugin_id: "official.react-native".into(),
                action: PluginAction::Action {
                    name: "app.run_ios".into(),
                    input: serde_json::json!({
                        "worktree": environment.worktree,
                        "udid": udid,
                        "metro_port": metro_port,
                        "derived_data": derived_data,
                        "bundle_id": request.bundle_id,
                        "scheme": request.scheme,
                    }),
                },
                timeout_seconds: Some(1_800),
            },
        )
        .map_err(|error| map_react_native_capability(error, "app.run_ios"))?;
        match response {
            SupervisorResponse::TaskOutput { stdout, stderr } => {
                self.append_preparation_log(environment.id, "ios-app", &stdout, &stderr)?;
            }
            SupervisorResponse::TaskFailed { stdout, stderr } => {
                self.append_preparation_log(environment.id, "ios-app", &stdout, &stderr)?;
                return Err(DaemonError::Operation(
                    "iOS app build, install, or launch failed; inspect preparation-ios-app logs"
                        .into(),
                ));
            }
            SupervisorResponse::TaskTimedOut => {
                return Err(DaemonError::Operation(
                    "iOS app build deadline exceeded; inspect before retrying".into(),
                ));
            }
            SupervisorResponse::TaskCompleted | SupervisorResponse::Process(_) => {
                return Err(DaemonError::Operation(
                    "supervisor returned an invalid iOS app response".into(),
                ));
            }
        }
        let app = StoredIosApp {
            environment_id: environment.id,
            bundle_id: request.bundle_id.clone(),
            udid,
            metro_port,
            derived_data: derived_data.display().to_string(),
            native_fingerprint,
            launched: true,
        };
        self.state.record_ios_app(&app)?;
        self.emit_event(
            environment.id,
            environment.generation,
            Some(operation_id),
            "app_launched",
        )?;
        Ok(DaemonResponse::IosApp(ios_app_snapshot(app)))
    }

    fn ios_discovery(plugin: &Path) -> Result<IosDiscovery, DaemonError> {
        let value = Self::plugin_action(plugin, "device.discover", serde_json::json!({}))?;
        serde_json::from_value(value).map_err(|_| {
            DaemonError::DeviceUnavailable("iOS discovery returned malformed data".into())
        })
    }

    fn run_ios_task(
        process: &StoredProcess,
        capability: &str,
        input: serde_json::Value,
    ) -> Result<String, DaemonError> {
        let response = request_supervisor(
            Path::new(&process.supervisor_socket),
            SupervisorRequest::RunPluginTask {
                operation_id: Uuid::now_v7(),
                plugin_id: "official.ios".into(),
                action: PluginAction::Action {
                    name: capability.into(),
                    input,
                },
                timeout_seconds: None,
            },
        )
        .map_err(|error| match error {
            DaemonError::Operation(message)
                if message.contains("requested plugin capability is not authorized") =>
            {
                DaemonError::CapabilityUnsupported(capability.into())
            }
            error => error,
        })?;
        match response {
            SupervisorResponse::TaskOutput { stdout, .. } => Ok(stdout),
            SupervisorResponse::TaskFailed { .. } => {
                Err(DaemonError::DeviceUnavailable(capability.into()))
            }
            SupervisorResponse::TaskCompleted
            | SupervisorResponse::TaskTimedOut
            | SupervisorResponse::Process(_) => Err(DaemonError::Operation(
                "supervisor returned an invalid iOS task response".into(),
            )),
        }
    }

    fn plugin_action(
        plugin: &Path,
        name: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, DaemonError> {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Uuid::now_v7(),
            method: "plugin.call".into(),
            params: serde_json::to_value(devfleet_protocol::PluginCall {
                protocol: PROTOCOL_VERSION,
                plugin_state_version: 1,
                operation_id: Uuid::now_v7(),
                action: PluginAction::Action {
                    name: name.into(),
                    input,
                },
            })
            .map_err(|error| DaemonError::Operation(error.to_string()))?,
        };
        let mut command = Command::new(plugin);
        command
            .arg("--serve")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = command
            .spawn()
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| DaemonError::Operation("plugin stdin unavailable".into()))?;
        stdin
            .write_all(&write_json_line(&request).map_err(|error| protocol_error(&error))?)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        drop(stdin);
        let output = child
            .wait_with_output()
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        if !output.status.success() || output.stdout.len() > devfleet_protocol::MAX_FRAME_BYTES {
            return Err(DaemonError::CapabilityUnsupported(name.into()));
        }
        let response =
            read_json_line::<JsonRpcResponse>(&mut BufReader::new(output.stdout.as_slice()))
                .map_err(|error| protocol_error(&error))?;
        if response.id != request.id {
            return Err(DaemonError::Operation(
                "plugin response identity does not match request".into(),
            ));
        }
        if let Some(error) = response.error {
            if let Some((_, message)) = error.message.split_once("DEVICE_UNAVAILABLE: ") {
                return Err(DaemonError::DeviceUnavailable(message.into()));
            }
            if let Some((_, message)) = error.message.split_once("CAPABILITY_UNSUPPORTED: ") {
                return Err(DaemonError::CapabilityUnsupported(message.into()));
            }
            return Err(DaemonError::Operation(error.message));
        }
        response
            .result
            .ok_or_else(|| DaemonError::Operation("plugin returned no result".into()))
    }

    fn status(&self, environment_id: Uuid) -> Result<DaemonResponse, DaemonError> {
        let environment = self
            .state
            .environment(environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let tasks = self.state.initialization_tasks(environment_id)?;
        let preparation = self.state.preparation_tasks(environment_id)?;
        let bindings = self.state.bindings(environment_id)?;
        let ios = self.state.ios_device(environment_id)?;
        let app = self.state.ios_app(environment_id)?;
        Ok(DaemonResponse::Environment(snapshot(
            environment,
            tasks,
            preparation,
            bindings,
            ios,
            app,
        )))
    }

    fn list(&self) -> Result<DaemonResponse, DaemonError> {
        let environments = self
            .state
            .environments()?
            .into_iter()
            .map(|environment| {
                let tasks = self.state.initialization_tasks(environment.id)?;
                let preparation = self.state.preparation_tasks(environment.id)?;
                let bindings = self.state.bindings(environment.id)?;
                let ios = self.state.ios_device(environment.id)?;
                let app = self.state.ios_app(environment.id)?;
                Ok::<_, DaemonError>(snapshot(
                    environment,
                    tasks,
                    preparation,
                    bindings,
                    ios,
                    app,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(DaemonResponse::Environments(environments))
    }

    fn stop(&mut self, environment_id: Uuid) -> Result<DaemonResponse, DaemonError> {
        let environment = self
            .state
            .environment(environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let process = environment.process.ok_or(DaemonError::OwnershipUncertain)?;
        let response = request_supervisor(
            Path::new(&process.supervisor_socket),
            SupervisorRequest::Stop,
        )?;
        let observed = supervisor_process(response)?;
        if observed.readiness != ReadinessState::ProcessStopped {
            return Err(DaemonError::Operation(
                "supervisor did not reap process".into(),
            ));
        }
        self.state.mark_stopped(environment_id)?;
        remove_supervisor_socket(&process.supervisor_socket)?;
        self.emit_event(environment_id, environment.generation, None, "stopped")?;
        self.status(environment_id)
    }

    fn operation_get(&self, operation_id: Uuid) -> Result<DaemonResponse, DaemonError> {
        let operation = self
            .state
            .operation(operation_id)?
            .ok_or(DaemonError::NotFound)?;
        Ok(DaemonResponse::Operation(operation_snapshot(operation)))
    }

    fn operation_cancel(&mut self, operation_id: Uuid) -> Result<DaemonResponse, DaemonError> {
        let operation = self
            .state
            .request_operation_cancel(operation_id)?
            .ok_or(DaemonError::NotFound)?;
        if let Some(environment) = self.state.environment(operation.environment_id)?
            && let Some(process) = environment.process
        {
            let response = request_supervisor(
                Path::new(&process.supervisor_socket),
                SupervisorRequest::Stop,
            )?;
            let process = supervisor_process(response)?;
            if process.readiness != ReadinessState::ProcessStopped {
                return Err(DaemonError::Operation(
                    "supervisor did not reap cancelled process".into(),
                ));
            }
            self.state.mark_stopped(operation.environment_id)?;
            remove_supervisor_socket(&process.supervisor_socket)?;
        }
        self.state
            .mark_operation_cancelled(operation_id, "reaped")?;
        self.emit_event(
            operation.environment_id,
            0,
            Some(operation_id),
            "operation_cancelled",
        )?;
        self.operation_get(operation_id)
    }

    fn events(
        &self,
        environment_id: Uuid,
        after: Option<u64>,
    ) -> Result<DaemonResponse, DaemonError> {
        Ok(DaemonResponse::Events(
            self.state
                .events_after(environment_id, after)?
                .into_iter()
                .map(event_snapshot)
                .collect(),
        ))
    }

    fn logs_tail(
        &self,
        environment_id: Uuid,
        resource: &str,
        after: Option<u64>,
        limit: usize,
    ) -> Result<DaemonResponse, DaemonError> {
        self.state
            .environment(environment_id)?
            .ok_or(DaemonError::NotFound)?;
        if resource != "process" {
            return Err(DaemonError::NotFound);
        }
        let logs = LogStore::new(
            log_directory(&self.config.state_dir, environment_id),
            MAX_RESOURCE_LOG_BYTES,
            Vec::new(),
        )
        .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let tail = logs
            .tail_from(resource, after, limit)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        Ok(DaemonResponse::Logs(LogSnapshot {
            environment_id,
            resource: resource.into(),
            cursor: tail.cursor,
            reset: tail.reset,
            lines: tail.lines,
        }))
    }

    fn emit_event(
        &mut self,
        environment_id: Uuid,
        generation: u64,
        operation_id: Option<Uuid>,
        kind: &str,
    ) -> Result<(), DaemonError> {
        self.state.append_event(&StoredEvent {
            sequence: 0,
            id: Uuid::now_v7(),
            timestamp_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| DaemonError::Operation(error.to_string()))?
                .as_millis(),
            environment_id,
            resource_id: None,
            generation,
            operation_id,
            kind: kind.into(),
            payload: serde_json::json!({}),
        })?;
        Ok(())
    }

    fn append_preparation_log(
        &self,
        environment_id: Uuid,
        kind: &str,
        stdout: &str,
        stderr: &str,
    ) -> Result<(), DaemonError> {
        let logs = LogStore::new(
            log_directory(&self.config.state_dir, environment_id),
            MAX_RESOURCE_LOG_BYTES,
            Vec::new(),
        )
        .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let resource = format!("preparation-{kind}");
        for line in stdout.lines().chain(stderr.lines()) {
            logs.append(&resource, line)
                .map_err(|error| DaemonError::Operation(error.to_string()))?;
        }
        Ok(())
    }

    fn start_supervisor(
        &self,
        control_socket: &Path,
        process_plugin: &Path,
        request: &SupervisorStartRequest,
    ) -> Result<(), DaemonError> {
        clear_stale_supervisor_socket(control_socket)?;
        let mut supervisor = Command::new(&self.config.supervisor)
            .arg("--serve")
            .arg("--control-socket")
            .arg(control_socket)
            .arg("--process-plugin")
            .arg(process_plugin)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let mut stdin = supervisor
            .stdin
            .take()
            .ok_or_else(|| DaemonError::Operation("supervisor stdin unavailable".into()))?;
        stdin
            .write_all(&write_json_line(request).map_err(|error| protocol_error(&error))?)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        drop(stdin);
        thread::spawn(move || {
            let _ = supervisor.wait();
        });
        wait_for_socket(control_socket)
    }

    fn plugin_list(&self) -> Result<DaemonResponse, DaemonError> {
        Ok(DaemonResponse::Plugins(
            self.state
                .plugins()?
                .into_iter()
                .map(plugin_snapshot)
                .collect(),
        ))
    }

    fn plugin_install(&mut self, path: &Path) -> Result<DaemonResponse, DaemonError> {
        let inspected = inspect_plugin(path)?;
        if let Some(existing) = self.state.plugin(&inspected.id)? {
            if existing.plugin_state_version != inspected.plugin_state_version {
                return Err(DaemonError::InvalidConfig(
                    "plugin state migration is not supported for this provider".into(),
                ));
            }
            if existing.checksum != inspected.checksum && self.state.has_active_environments()? {
                return Err(DaemonError::Operation(
                    "plugin updates are blocked while environments are active".into(),
                ));
            }
        }
        self.state.record_plugin(&inspected)?;
        self.write_plugin_lockfile()?;
        Ok(DaemonResponse::Plugin(plugin_snapshot(inspected)))
    }

    fn plugin_trust(&mut self, id: &str) -> Result<DaemonResponse, DaemonError> {
        let plugin = self.state.plugin(id)?.ok_or(DaemonError::NotFound)?;
        self.verify_plugin_lock(&plugin)?;
        if !self.state.set_plugin_trusted(id)? {
            return Err(DaemonError::NotFound);
        }
        let trusted = self.state.plugin(id)?.ok_or(DaemonError::NotFound)?;
        self.write_plugin_lockfile()?;
        Ok(DaemonResponse::Plugin(plugin_snapshot(trusted)))
    }

    fn trusted_process_plugin(&self) -> Result<PathBuf, DaemonError> {
        self.trusted_plugin("official.process", "process.reconcile")
    }

    fn trusted_task_plugins(&self) -> Result<Vec<TrustedTaskPlugin>, DaemonError> {
        self.state
            .plugins()?
            .into_iter()
            .filter(|plugin| {
                plugin.trusted
                    && matches!(plugin.id.as_str(), "official.ios" | "official.react-native")
            })
            .map(|plugin| {
                self.verify_plugin_lock(&plugin)?;
                let inspected = inspect_plugin(Path::new(&plugin.path))?;
                if !same_plugin_metadata(&plugin, &inspected) {
                    return Err(DaemonError::InvalidConfig(
                        "installed task-provider descriptor or checksum no longer matches its lockfile"
                            .into(),
                    ));
                }
                Ok(TrustedTaskPlugin {
                    id: plugin.id,
                    path: plugin.path,
                    checksum: plugin.checksum,
                    capabilities: plugin.capabilities,
                })
            })
            .collect()
    }

    fn trusted_plugin(&self, id: &str, capability: &str) -> Result<PathBuf, DaemonError> {
        let plugin = self
            .state
            .plugin(id)?
            .ok_or(DaemonError::PluginTrustRequired)?;
        if !plugin.trusted {
            return Err(DaemonError::PluginTrustRequired);
        }
        self.verify_plugin_lock(&plugin)?;
        let inspected = inspect_plugin(Path::new(&plugin.path))?;
        if !same_plugin_metadata(&plugin, &inspected)
            || !inspected.capabilities.iter().any(|item| item == capability)
        {
            return Err(DaemonError::InvalidConfig(format!(
                "installed {id} descriptor or checksum no longer matches its lockfile"
            )));
        }
        Ok(PathBuf::from(plugin.path))
    }

    fn write_plugin_lockfile(&self) -> Result<(), DaemonError> {
        let lockfile = self.config.state_dir.join("plugins.lock.json");
        let temporary = self.config.state_dir.join("plugins.lock.json.new");
        let snapshots: Vec<_> = self
            .state
            .plugins()?
            .into_iter()
            .map(plugin_snapshot)
            .collect();
        let bytes = serde_json::to_vec(&snapshots)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        fs::write(&temporary, bytes).map_err(|error| DaemonError::Operation(error.to_string()))?;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        fs::rename(temporary, lockfile).map_err(|error| DaemonError::Operation(error.to_string()))
    }

    fn verify_plugin_lock(&self, plugin: &devfleet_state::StoredPlugin) -> Result<(), DaemonError> {
        let bytes = fs::read(self.config.state_dir.join("plugins.lock.json"))
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let entries: Vec<PluginSnapshot> = serde_json::from_slice(&bytes)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        if entries
            .iter()
            .any(|entry| plugin_matches_snapshot(plugin, entry))
        {
            Ok(())
        } else {
            Err(DaemonError::InvalidConfig(
                "installed plugin is absent from the lockfile".into(),
            ))
        }
    }
}

#[derive(Debug, Error)]
enum DaemonError {
    #[error("explicit project trust is required before executing a process")]
    TrustRequired,
    #[error("explicit plugin trust is required before executing this provider")]
    PluginTrustRequired,
    #[error("process configuration is invalid: {0}")]
    InvalidConfig(String),
    #[error("environment was not found")]
    NotFound,
    #[error("resource ownership cannot be proven")]
    OwnershipUncertain,
    #[error(
        "partial simulator creation was inspected ({0} matching device labels); no device was adopted"
    )]
    OwnershipUncertainPartial(usize),
    #[error("DEVICE_UNAVAILABLE: {0}")]
    DeviceUnavailable(String),
    #[error("CAPABILITY_UNSUPPORTED: {0}")]
    CapabilityUnsupported(String),
    #[error("operation failed: {0}")]
    Operation(String),
    #[error(transparent)]
    State(#[from] devfleet_state::StateError),
}

impl From<DaemonError> for RpcFailure {
    fn from(error: DaemonError) -> Self {
        let code = match error {
            DaemonError::TrustRequired | DaemonError::PluginTrustRequired => {
                ErrorCode::PermissionDenied
            }
            DaemonError::InvalidConfig(_) => ErrorCode::ConfigInvalid,
            DaemonError::NotFound => ErrorCode::ResourceNotFound,
            DaemonError::OwnershipUncertain | DaemonError::OwnershipUncertainPartial(_) => {
                ErrorCode::OwnershipUncertain
            }
            DaemonError::DeviceUnavailable(_) => ErrorCode::DeviceUnavailable,
            DaemonError::CapabilityUnsupported(_) => ErrorCode::CapabilityUnsupported,
            DaemonError::State(devfleet_state::StateError::CursorExpired) => {
                ErrorCode::CursorExpired
            }
            DaemonError::Operation(_) | DaemonError::State(_) => ErrorCode::OperationFailed,
        };
        Self {
            code,
            message: error.to_string(),
            retryable: false,
        }
    }
}

#[derive(serde::Deserialize)]
struct PluginDescriptor {
    id: String,
    version: String,
    platform: String,
    protocol: devfleet_protocol::ProtocolVersion,
    capabilities: Vec<String>,
    plugin_state_version: u32,
}

fn inspect_plugin(path: &Path) -> Result<devfleet_state::StoredPlugin, DaemonError> {
    let output = Command::new(path)
        .arg("--descriptor")
        .output()
        .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
    if !output.status.success() || output.stdout.len() > devfleet_protocol::MAX_FRAME_BYTES {
        return Err(DaemonError::InvalidConfig(
            "plugin descriptor command failed or exceeded the protocol limit".into(),
        ));
    }
    let descriptor: PluginDescriptor = serde_json::from_slice(&output.stdout)
        .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
    if descriptor.id.is_empty()
        || descriptor.platform != current_platform()
        || !PROTOCOL_VERSION.compatible_with(descriptor.protocol)
    {
        return Err(DaemonError::InvalidConfig(
            "plugin descriptor identity, platform, or protocol is incompatible".into(),
        ));
    }
    let mut binary =
        File::open(path).map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = binary
            .read(&mut buffer)
            .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(devfleet_state::StoredPlugin {
        id: descriptor.id,
        path: path.display().to_string(),
        platform: descriptor.platform,
        version: descriptor.version,
        checksum: format!("{:x}", digest.finalize()),
        protocol_major: descriptor.protocol.major,
        protocol_minor: descriptor.protocol.minor,
        plugin_state_version: descriptor.plugin_state_version,
        capabilities: descriptor.capabilities,
        trusted: false,
    })
}

fn plugin_snapshot(plugin: devfleet_state::StoredPlugin) -> PluginSnapshot {
    PluginSnapshot {
        id: plugin.id,
        path: plugin.path,
        platform: plugin.platform,
        version: plugin.version,
        checksum: plugin.checksum,
        protocol: devfleet_protocol::ProtocolVersion {
            major: plugin.protocol_major,
            minor: plugin.protocol_minor,
        },
        plugin_state_version: plugin.plugin_state_version,
        capabilities: plugin.capabilities,
        trusted: plugin.trusted,
    }
}

fn same_plugin_metadata(
    expected: &devfleet_state::StoredPlugin,
    observed: &devfleet_state::StoredPlugin,
) -> bool {
    expected.id == observed.id
        && expected.path == observed.path
        && expected.platform == observed.platform
        && expected.version == observed.version
        && expected.checksum == observed.checksum
        && expected.protocol_major == observed.protocol_major
        && expected.protocol_minor == observed.protocol_minor
        && expected.plugin_state_version == observed.plugin_state_version
        && expected.capabilities == observed.capabilities
}

fn plugin_matches_snapshot(
    plugin: &devfleet_state::StoredPlugin,
    snapshot: &PluginSnapshot,
) -> bool {
    snapshot.id == plugin.id
        && snapshot.path == plugin.path
        && snapshot.platform == plugin.platform
        && snapshot.version == plugin.version
        && snapshot.checksum == plugin.checksum
        && snapshot.protocol.major == plugin.protocol_major
        && snapshot.protocol.minor == plugin.protocol_minor
        && snapshot.plugin_state_version == plugin.plugin_state_version
        && snapshot.capabilities == plugin.capabilities
        && snapshot.trusted == plugin.trusted
}

fn current_platform() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

fn validate_up_request(
    request: &devfleet_protocol::EnvironmentUpRequest,
) -> Result<(), DaemonError> {
    if !request.trusted {
        return Err(DaemonError::TrustRequired);
    }
    if request.profile.is_empty() || request.process.argv.is_empty() {
        return Err(DaemonError::InvalidConfig(
            "profile and argv are required".into(),
        ));
    }
    if request.instance.as_deref().is_some_and(str::is_empty) {
        return Err(DaemonError::InvalidConfig(
            "instance names must be nonempty when specified".into(),
        ));
    }
    if !Path::new(&request.worktree).is_dir() || !Path::new(&request.process.cwd).is_dir() {
        return Err(DaemonError::InvalidConfig(
            "worktree and process cwd must be existing directories".into(),
        ));
    }
    Ok(())
}

fn validate_compose_up_request(request: &ComposeUpRequest) -> Result<(), DaemonError> {
    if !request.trusted {
        return Err(DaemonError::TrustRequired);
    }
    if request.profile.is_empty() || request.files.is_empty() {
        return Err(DaemonError::InvalidConfig(
            "profile and at least one Compose file are required".into(),
        ));
    }
    let worktree = Path::new(&request.worktree)
        .canonicalize()
        .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
    if !worktree.is_dir() || request.instance.as_deref().is_some_and(str::is_empty) {
        return Err(DaemonError::InvalidConfig(
            "worktree must exist and instance names must be nonempty".into(),
        ));
    }
    let mut files = BTreeSet::new();
    for file in &request.files {
        let file = Path::new(file)
            .canonicalize()
            .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        if !file.is_file() || !file.starts_with(&worktree) || !files.insert(file) {
            return Err(DaemonError::InvalidConfig(
                "Compose files must be unique regular files inside the worktree".into(),
            ));
        }
    }
    let mut endpoints = BTreeSet::new();
    for endpoint in &request.endpoints {
        if endpoint.name.is_empty()
            || endpoint.service.is_empty()
            || endpoint.container_port == 0
            || endpoint.host_port == 0
            || !endpoint.readiness_path.starts_with('/')
            || !endpoints.insert((&endpoint.name, endpoint.host_port))
        {
            return Err(DaemonError::InvalidConfig(
                "Compose endpoints require unique names, ports, and readiness paths".into(),
            ));
        }
    }
    Ok(())
}

fn compose_project_name(project_id: Uuid, environment_id: Uuid) -> String {
    let project = project_id.to_string().replace('-', "");
    let environment = environment_id.to_string().replace('-', "");
    format!("df_{}_{}", &project[..8], &environment[..8])
}

fn stored_compose_endpoints(
    endpoints: &[devfleet_protocol::ComposeEndpoint],
) -> Vec<StoredEndpoint> {
    endpoints
        .iter()
        .map(|endpoint| StoredEndpoint {
            name: endpoint.name.clone(),
            service: endpoint.service.clone(),
            internal_url: format!("http://{}:{}", endpoint.service, endpoint.container_port),
            host_url: format!("http://127.0.0.1:{}", endpoint.host_port),
            device_url: format!("http://127.0.0.1:{}", endpoint.host_port),
            readiness_path: endpoint.readiness_path.clone(),
            healthy: false,
        })
        .collect()
}

fn endpoint_is_healthy(endpoint: &StoredEndpoint) -> bool {
    let Some(authority) = endpoint.host_url.strip_prefix("http://") else {
        return endpoint.healthy;
    };
    let Ok(address) = authority.parse::<SocketAddr>() else {
        return endpoint.healthy;
    };
    let timeout = Duration::from_millis(100);
    let Ok(mut stream) = TcpStream::connect_timeout(&address, timeout) else {
        return false;
    };
    if stream.set_read_timeout(Some(timeout)).is_err()
        || stream.set_write_timeout(Some(timeout)).is_err()
    {
        return false;
    }
    let request = format!(
        "GET {} HTTP/1.0\r\nHost: {}\r\nConnection: close\r\n\r\n",
        endpoint.readiness_path, authority
    );
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut response = [0_u8; 64];
    let Ok(read) = stream.read(&mut response) else {
        return false;
    };
    response[..read]
        .split(|byte| *byte == b' ')
        .nth(1)
        .and_then(|status| std::str::from_utf8(status).ok())
        .and_then(|status| status.parse::<u16>().ok())
        .is_some_and(|status| (200..400).contains(&status))
}

fn wait_for_metro_identity(port: u16, expected_identity: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    let expected = format!("\"environment\":\"{expected_identity}\"");
    while Instant::now() < deadline {
        if let Ok(mut stream) = TcpStream::connect_timeout(
            &SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_millis(200),
        ) {
            let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
            let _ = stream.write_all(
                b"GET /devfleet-identity HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            );
            let mut response = String::new();
            if stream.read_to_string(&mut response).is_ok()
                && response.starts_with("HTTP/1.1 200")
                && response.contains(&expected)
            {
                return true;
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    false
}

fn canonical_plan_path(path: &str, worktree: &Path) -> Result<PathBuf, DaemonError> {
    let path = Path::new(path)
        .canonicalize()
        .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
    if !path.is_file() || !path.starts_with(worktree) {
        return Err(DaemonError::InvalidConfig(
            "plan files must be regular files inside the requested worktree".into(),
        ));
    }
    Ok(path)
}

fn validate_initialization_tasks(tasks: &[InitializationTask]) -> Result<(), DaemonError> {
    let ids = tasks
        .iter()
        .map(|task| task.id.as_str())
        .collect::<BTreeSet<_>>();
    if ids.len() != tasks.len()
        || tasks.iter().any(|task| {
            task.id.is_empty()
                || task.process.argv.is_empty()
                || task.process.cwd.is_empty()
                || task
                    .depends_on
                    .iter()
                    .any(|dependency| dependency == &task.id || !ids.contains(dependency.as_str()))
        })
    {
        return Err(DaemonError::InvalidConfig(
            "initialization tasks need unique ids, executable processes, and declared non-self dependencies".into(),
        ));
    }
    let _ = ordered_initialization_tasks(tasks)?;
    Ok(())
}

fn ordered_initialization_tasks(
    tasks: &[InitializationTask],
) -> Result<Vec<&InitializationTask>, DaemonError> {
    let mut remaining = tasks
        .iter()
        .map(|task| (task.id.clone(), task))
        .collect::<BTreeMap<_, _>>();
    let mut completed = BTreeSet::new();
    let mut ordered = Vec::new();
    while !remaining.is_empty() {
        let next = remaining
            .iter()
            .find(|(_, task)| {
                task.depends_on
                    .iter()
                    .all(|dependency| completed.contains(dependency))
            })
            .map(|(id, _)| (*id).clone());
        let Some(id) = next else {
            return Err(DaemonError::InvalidConfig(
                "initialization task dependencies contain a cycle".into(),
            ));
        };
        let task = remaining.remove(&id).expect("task selected from remaining");
        completed.insert(id);
        ordered.push(task);
    }
    Ok(ordered)
}

fn task_fingerprint(task: &InitializationTask) -> Result<String, DaemonError> {
    let bytes =
        serde_json::to_vec(task).map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn task_is_complete_for_policy(
    stored: &StoredInitializationTask,
    task: &InitializationTask,
    generation: u64,
    fingerprint: &str,
) -> bool {
    stored.status == "completed"
        && stored.fingerprint == fingerprint
        && match task.rerun_policy {
            TaskRerunPolicy::Never => true,
            TaskRerunPolicy::Always => false,
            TaskRerunPolicy::OnEnvironmentGeneration => stored.generation == generation,
        }
}

fn task_kind_name(kind: InitializationTaskKind) -> &'static str {
    match kind {
        InitializationTaskKind::Migration => "migration",
        InitializationTaskKind::Seed => "seed",
    }
}

fn rerun_policy_name(policy: TaskRerunPolicy) -> &'static str {
    match policy {
        TaskRerunPolicy::Never => "never",
        TaskRerunPolicy::Always => "always",
        TaskRerunPolicy::OnEnvironmentGeneration => "on_environment_generation",
    }
}

fn initialization_task_snapshot(task: StoredInitializationTask) -> InitializationTaskSnapshot {
    let kind = match task.kind.as_str() {
        "migration" => InitializationTaskKind::Migration,
        _ => InitializationTaskKind::Seed,
    };
    let rerun_policy = match task.rerun_policy.as_str() {
        "never" => TaskRerunPolicy::Never,
        "always" => TaskRerunPolicy::Always,
        _ => TaskRerunPolicy::OnEnvironmentGeneration,
    };
    let status = match task.status.as_str() {
        "completed" => InitializationTaskStatus::Completed,
        "failed" => InitializationTaskStatus::Failed,
        _ => InitializationTaskStatus::Uncertain,
    };
    InitializationTaskSnapshot {
        id: task.id,
        kind,
        rerun_policy,
        generation: task.generation,
        attempts: task.attempts,
        status,
        diagnostic: task.diagnostic,
    }
}

fn preparation_task_snapshot(task: StoredPreparationTask) -> PreparationTaskSnapshot {
    let kind = match task.kind.as_str() {
        "dependencies" => PreparationTaskKind::Dependencies,
        _ => PreparationTaskKind::Pods,
    };
    let status = match task.status.as_str() {
        "completed" => PreparationTaskStatus::Completed,
        "failed" => PreparationTaskStatus::Failed,
        _ => PreparationTaskStatus::Uncertain,
    };
    PreparationTaskSnapshot {
        kind,
        fingerprint: task.fingerprint,
        attempts: task.attempts,
        status,
        diagnostic: task.diagnostic,
    }
}

fn preparation_kind_name(kind: PreparationTaskKind) -> &'static str {
    match kind {
        PreparationTaskKind::Dependencies => "dependencies",
        PreparationTaskKind::Pods => "pods",
    }
}

fn preparation_fingerprint(
    worktree: &Path,
    kind: PreparationTaskKind,
) -> Result<String, DaemonError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(preparation_kind_name(kind).as_bytes());
    let paths = match kind {
        PreparationTaskKind::Dependencies => vec![worktree.join("package-lock.json")],
        PreparationTaskKind::Pods => {
            let ios = worktree.join("ios");
            let lockfile = ios.join("Podfile.lock");
            vec![ios.join("Podfile"), lockfile]
        }
    };
    for path in paths {
        if path.file_name().is_some_and(|name| name == "Podfile.lock") && !path.exists() {
            continue;
        }
        let content = fs::read(&path).map_err(|_| {
            DaemonError::InvalidConfig(format!(
                "preparation input is missing or unreadable: {}",
                path.display()
            ))
        })?;
        bytes.extend_from_slice(path.to_string_lossy().as_bytes());
        bytes.extend_from_slice(&content);
    }
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn native_build_fingerprint(
    worktree: &Path,
    metro_port: u16,
    scheme: Option<&str>,
) -> Result<String, DaemonError> {
    let mut files = Vec::new();
    collect_native_inputs(&worktree.join("ios"), worktree, &mut files)?;
    for lockfile in ["package-lock.json", "ios/Podfile.lock"] {
        let path = worktree.join(lockfile);
        if path.is_file() && !files.contains(&path) {
            files.push(path);
        }
    }
    let mut digest = Sha256::new();
    digest.update(b"metro_port=");
    digest.update(metro_port.to_string().as_bytes());
    digest.update(b"scheme=");
    digest.update(scheme.unwrap_or_default().as_bytes());
    for path in files {
        let relative = path.strip_prefix(worktree).map_err(|_| {
            DaemonError::InvalidConfig("native input escaped its selected worktree".into())
        })?;
        digest.update(relative.to_string_lossy().as_bytes());
        digest.update(fs::read(path).map_err(|error| DaemonError::Operation(error.to_string()))?);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn collect_native_inputs(
    directory: &Path,
    worktree: &Path,
    files: &mut Vec<PathBuf>,
) -> Result<(), DaemonError> {
    let metadata = directory
        .symlink_metadata()
        .map_err(|_| DaemonError::InvalidConfig("iOS native directory is required".into()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(DaemonError::InvalidConfig(
            "iOS native inputs may not traverse a symlink".into(),
        ));
    }
    for entry in
        fs::read_dir(directory).map_err(|error| DaemonError::Operation(error.to_string()))?
    {
        let entry = entry.map_err(|error| DaemonError::Operation(error.to_string()))?;
        let path = entry.path();
        let metadata = path
            .symlink_metadata()
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        if metadata.file_type().is_symlink() {
            return Err(DaemonError::InvalidConfig(
                "iOS native inputs may not traverse a symlink".into(),
            ));
        }
        if metadata.is_dir() {
            let relative = path.strip_prefix(worktree).map_err(|_| {
                DaemonError::InvalidConfig("native input escaped its selected worktree".into())
            })?;
            if relative.starts_with("ios/Pods") || relative.starts_with("ios/build") {
                continue;
            }
            collect_native_inputs(&path, worktree, files)?;
        } else if metadata.is_file() {
            files.push(path);
        }
    }
    files.sort();
    Ok(())
}

fn endpoint_port(url: &str) -> Result<u16, DaemonError> {
    url.rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .filter(|port| *port != 0)
        .ok_or_else(|| {
            DaemonError::InvalidConfig("resolved Metro endpoint has no valid port".into())
        })
}

fn map_react_native_capability(error: DaemonError, capability: &str) -> DaemonError {
    match error {
        DaemonError::Operation(message)
            if message.contains("requested plugin capability is not authorized")
                || message.contains("CAPABILITY_UNSUPPORTED:") =>
        {
            DaemonError::CapabilityUnsupported(capability.into())
        }
        error => error,
    }
}

fn ios_app_snapshot(app: StoredIosApp) -> IosAppSnapshot {
    IosAppSnapshot {
        environment_id: app.environment_id,
        bundle_id: app.bundle_id,
        udid: app.udid,
        metro_port: app.metro_port,
        derived_data: app.derived_data,
        launched: app.launched,
    }
}

fn snapshot(
    environment: StoredEnvironment,
    tasks: Vec<StoredInitializationTask>,
    preparation: Vec<StoredPreparationTask>,
    bindings: Vec<StoredBinding>,
    ios: Option<StoredIosDevice>,
    app: Option<StoredIosApp>,
) -> EnvironmentSnapshot {
    let lifecycle = match environment.lifecycle {
        StoredLifecycle::Starting => LifecycleState::Starting,
        StoredLifecycle::Running => LifecycleState::Running,
        StoredLifecycle::Stopped => LifecycleState::Stopped,
        StoredLifecycle::Failed => LifecycleState::Failed,
        StoredLifecycle::Degraded => LifecycleState::Degraded,
        StoredLifecycle::Orphaned => LifecycleState::Orphaned,
    };
    let probe_health = matches!(
        lifecycle,
        LifecycleState::Starting | LifecycleState::Running
    );
    EnvironmentSnapshot {
        environment_id: environment.id,
        project_id: environment.project_id,
        worktree_id: environment.worktree_id,
        worktree: environment.worktree,
        profile: environment.profile,
        instance: environment.instance,
        generation: environment.generation,
        lifecycle,
        reconciliation_diagnostic: environment.reconciliation_diagnostic,
        process: environment.process.map(|process| ProcessSnapshot {
            pid: process.pid,
            process_group: process.process_group,
            started_at_unix_ms: process.started_at_unix_ms,
            supervisor_socket: process.supervisor_socket,
            readiness: match process.readiness {
                StoredReadiness::ProcessAlive => ReadinessState::ProcessAlive,
                StoredReadiness::ProcessStopped => ReadinessState::ProcessStopped,
            },
        }),
        endpoints: environment
            .endpoints
            .into_iter()
            .map(|endpoint| {
                let healthy = probe_health && endpoint_is_healthy(&endpoint);
                EndpointSnapshot {
                    name: endpoint.name,
                    service: endpoint.service,
                    internal_url: endpoint.internal_url,
                    host_url: endpoint.host_url,
                    device_url: endpoint.device_url,
                    readiness_path: endpoint.readiness_path,
                    healthy,
                }
            })
            .collect(),
        initialization_ready: tasks.iter().all(|task| task.status == "completed"),
        initialization_tasks: tasks
            .into_iter()
            .map(initialization_task_snapshot)
            .collect(),
        preparation_tasks: preparation
            .into_iter()
            .map(preparation_task_snapshot)
            .collect(),
        bindings: bindings.into_iter().map(binding_snapshot).collect(),
        ios_simulator: ios.and_then(|device| ios_snapshot(device).ok()),
        ios_app: app.map(ios_app_snapshot),
    }
}

fn ios_snapshot(device: StoredIosDevice) -> Result<IosSimulatorSnapshot, DaemonError> {
    let udid = device.udid.ok_or(DaemonError::OwnershipUncertain)?;
    let lifecycle = match device.lifecycle.as_str() {
        "reserved" => IosSimulatorLifecycle::Reserved,
        "booted" => IosSimulatorLifecycle::Booted,
        "stopped" => IosSimulatorLifecycle::Stopped,
        "uncertain" => IosSimulatorLifecycle::Uncertain,
        _ => return Err(DaemonError::OwnershipUncertain),
    };
    Ok(IosSimulatorSnapshot {
        environment_id: device.environment_id,
        udid,
        runtime: device.runtime,
        device_type: device.device_type,
        lifecycle,
    })
}

fn reconcile_supervisors(state: &mut StateStore) -> Result<(), anyhow::Error> {
    for environment in state.environments()? {
        let Some(process) = environment.process else {
            continue;
        };
        if !matches!(
            environment.lifecycle,
            StoredLifecycle::Running | StoredLifecycle::Starting | StoredLifecycle::Degraded
        ) {
            continue;
        }
        match request_supervisor(Path::new(&process.supervisor_socket), SupervisorRequest::Inspect) {
            Ok(SupervisorResponse::Process(observed)) if observed.pid == process.pid && observed.process_group == process.process_group => state.mark_recovered(environment.id)?,
            Ok(_) => state.mark_degraded(environment.id, "Supervisor identity differs from the retained journal; ownership uncertain and no process was terminated.")?,
            Err(_) => state.mark_degraded(environment.id, "Supervisor is unavailable after daemon restart; ownership uncertain and no process was terminated.")?,
        }
    }
    Ok(())
}

fn environment_key(
    project_fingerprint: &str,
    worktree_fingerprint: &str,
    profile: &str,
    instance: Option<&str>,
) -> String {
    format!(
        "{project_fingerprint}|{worktree_fingerprint}|{profile}|{}",
        instance.unwrap_or_default()
    )
}

fn stored_process(process: &ProcessSnapshot) -> StoredProcess {
    StoredProcess {
        pid: process.pid,
        process_group: process.process_group,
        started_at_unix_ms: process.started_at_unix_ms,
        supervisor_socket: process.supervisor_socket.clone(),
        readiness: match process.readiness {
            ReadinessState::ProcessAlive => StoredReadiness::ProcessAlive,
            ReadinessState::ProcessStopped => StoredReadiness::ProcessStopped,
        },
    }
}

fn operation_snapshot(operation: StoredOperation) -> OperationSnapshot {
    OperationSnapshot {
        operation_id: operation.id,
        environment_id: operation.environment_id,
        status: match operation.status {
            devfleet_state::OperationStatus::Accepted => {
                devfleet_protocol::OperationStatus::Accepted
            }
            devfleet_state::OperationStatus::CancellationRequested => {
                devfleet_protocol::OperationStatus::CancellationRequested
            }
            devfleet_state::OperationStatus::Cancelled => {
                devfleet_protocol::OperationStatus::Cancelled
            }
            devfleet_state::OperationStatus::Completed => {
                devfleet_protocol::OperationStatus::Completed
            }
            devfleet_state::OperationStatus::Failed => devfleet_protocol::OperationStatus::Failed,
        },
        deadline_unix_ms: operation.deadline_unix_ms,
        phase: operation.phase,
        resource_id: operation.resource_id,
        retryable: false,
    }
}

fn event_snapshot(event: StoredEvent) -> EventSnapshot {
    EventSnapshot {
        sequence: event.sequence,
        id: event.id,
        timestamp_unix_ms: event.timestamp_unix_ms,
        environment_id: event.environment_id,
        resource_id: event.resource_id,
        generation: event.generation,
        operation_id: event.operation_id,
        kind: event.kind,
        payload: event.payload,
    }
}

fn prepare_private_dir(path: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    let metadata = fs::metadata(path)?;
    if metadata.uid() != getuid().as_raw() {
        anyhow::bail!("private directory has a different owner");
    }
    if metadata.mode() & 0o077 != 0 {
        anyhow::bail!("private directory grants group or other access");
    }
    Ok(())
}

fn acquire_writer_lock(state_dir: &Path) -> anyhow::Result<File> {
    let lock_path = state_dir.join("daemon.lock");
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)?;
    fs::set_permissions(
        state_dir.join("daemon.lock"),
        fs::Permissions::from_mode(0o600),
    )?;
    lock.try_lock_exclusive()
        .map_err(|_| anyhow::anyhow!("another daemon owns this state directory"))?;
    Ok(lock)
}

fn ensure_short_socket_path(path: &Path) -> anyhow::Result<()> {
    if path.as_os_str().as_encoded_bytes().len() > MAX_SOCKET_PATH_BYTES {
        anyhow::bail!("daemon socket path exceeds the conservative Unix limit");
    }
    Ok(())
}

fn supervisor_socket_path(environment_id: Uuid) -> Result<PathBuf, DaemonError> {
    let directory = default_socket_dir();
    prepare_private_dir(&directory).map_err(|error| DaemonError::Operation(error.to_string()))?;
    let path = directory.join(format!("s-{environment_id}.sock"));
    ensure_short_socket_path(&path).map_err(|error| DaemonError::Operation(error.to_string()))?;
    Ok(path)
}

struct DaemonSocketCleanup(PathBuf);

impl Drop for DaemonSocketCleanup {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn clear_stale_daemon_socket(path: &Path) -> anyhow::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    match std::os::unix::net::UnixStream::connect(path) {
        Ok(_) => anyhow::bail!("daemon socket already exists; another daemon may own it"),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) =>
        {
            fs::remove_file(path).map_err(Into::into)
        }
        Err(error) => Err(error.into()),
    }
}

fn cleanup_stopped_supervisor_socket(
    environment: Option<&StoredEnvironment>,
) -> Result<(), DaemonError> {
    if let Some(process) = environment
        .filter(|environment| environment.lifecycle == StoredLifecycle::Stopped)
        .and_then(|environment| environment.process.as_ref())
    {
        remove_supervisor_socket(&process.supervisor_socket)?;
    }
    Ok(())
}

fn clear_stale_supervisor_socket(path: &Path) -> Result<(), DaemonError> {
    if !path.exists() {
        return Ok(());
    }
    match std::os::unix::net::UnixStream::connect(path) {
        Ok(_) => Err(DaemonError::OwnershipUncertain),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) =>
        {
            remove_supervisor_socket(&path.display().to_string())
        }
        Err(error) => Err(DaemonError::Operation(format!(
            "inspect existing supervisor socket: {error}"
        ))),
    }
}

fn remove_supervisor_socket(path: &str) -> Result<(), DaemonError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(DaemonError::Operation(format!(
            "remove stopped supervisor socket: {error}"
        ))),
    }
}

fn log_directory(state_dir: &Path, environment_id: Uuid) -> PathBuf {
    state_dir.join("logs").join(environment_id.to_string())
}

fn core_phase(phase: BindingPhase) -> CoreBindingPhase {
    match phase {
        BindingPhase::Process => CoreBindingPhase::Process,
        BindingPhase::Bundle => CoreBindingPhase::Bundle,
        BindingPhase::NativeBuild => CoreBindingPhase::NativeBuild,
        BindingPhase::Runtime => CoreBindingPhase::Runtime,
    }
}
fn core_integration(integration: BindingIntegration) -> CoreBindingIntegration {
    match integration {
        BindingIntegration::ProcessEnvironment => CoreBindingIntegration::ProcessEnvironment,
        BindingIntegration::BundleConfig => CoreBindingIntegration::BundleConfig,
        BindingIntegration::NativeBuildSettings => CoreBindingIntegration::NativeBuildSettings,
        BindingIntegration::RuntimeConfiguration => CoreBindingIntegration::RuntimeConfiguration,
    }
}
fn phase_name(phase: CoreBindingPhase) -> &'static str {
    match phase {
        CoreBindingPhase::Process => "process",
        CoreBindingPhase::Bundle => "bundle",
        CoreBindingPhase::NativeBuild => "native_build",
        CoreBindingPhase::Runtime => "runtime",
    }
}
fn integration_name(value: CoreBindingIntegration) -> &'static str {
    match value {
        CoreBindingIntegration::ProcessEnvironment => "process_environment",
        CoreBindingIntegration::BundleConfig => "bundle_config",
        CoreBindingIntegration::NativeBuildSettings => "native_build_settings",
        CoreBindingIntegration::RuntimeConfiguration => "runtime_configuration",
    }
}
fn invalidation_name(value: Invalidation) -> &'static str {
    match value {
        Invalidation::Restart => "restart",
        Invalidation::Reload => "reload",
        Invalidation::Rebuild => "rebuild",
        Invalidation::Reconnect => "reconnect",
    }
}
fn binding_snapshot(binding: StoredBinding) -> BindingSnapshot {
    let phase = match binding.phase.as_str() {
        "process" => BindingPhase::Process,
        "bundle" => BindingPhase::Bundle,
        "native_build" => BindingPhase::NativeBuild,
        _ => BindingPhase::Runtime,
    };
    let integration = match binding.integration.as_str() {
        "process_environment" => BindingIntegration::ProcessEnvironment,
        "bundle_config" => BindingIntegration::BundleConfig,
        "native_build_settings" => BindingIntegration::NativeBuildSettings,
        _ => BindingIntegration::RuntimeConfiguration,
    };
    let invalidation = match binding.invalidation.as_str() {
        "restart" => BindingInvalidation::Restart,
        "reload" => BindingInvalidation::Reload,
        "rebuild" => BindingInvalidation::Rebuild,
        _ => BindingInvalidation::Reconnect,
    };
    BindingSnapshot {
        phase,
        integration,
        fingerprint: binding.fingerprint,
        invalidation,
        public_values: serde_json::from_str(&binding.public_values_json).unwrap_or_default(),
    }
}
fn write_private_atomically(path: &Path, contents: &[u8]) -> Result<(), DaemonError> {
    let temporary = path.with_extension(format!("{}.tmp", Uuid::now_v7()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| DaemonError::Operation(error.to_string()))?;
    file.write_all(contents)
        .and_then(|()| file.sync_all())
        .map_err(|error| DaemonError::Operation(error.to_string()))?;
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))
        .map_err(|error| DaemonError::Operation(error.to_string()))?;
    fs::rename(&temporary, path).map_err(|error| DaemonError::Operation(error.to_string()))
}

fn known_secret_values(environment: &BTreeMap<String, String>) -> Vec<String> {
    environment
        .iter()
        .filter_map(|(name, value)| {
            let normalized = name.to_ascii_lowercase();
            let secret_name = [
                "secret",
                "token",
                "password",
                "credential",
                "api_key",
                "apikey",
            ]
            .iter()
            .any(|marker| normalized.contains(marker));
            (secret_name && !value.is_empty()).then(|| value.clone())
        })
        .collect()
}

fn wait_for_socket(path: &Path) -> Result<(), DaemonError> {
    let started = Instant::now();
    loop {
        if path.exists() {
            return Ok(());
        }
        if started.elapsed() >= SOCKET_STARTUP_TIMEOUT {
            return Err(DaemonError::Operation(
                "supervisor did not create its control socket".into(),
            ));
        }
        thread::sleep(SOCKET_RETRY_INTERVAL);
    }
}

fn request_supervisor(
    socket: &Path,
    command: SupervisorRequest,
) -> Result<SupervisorResponse, DaemonError> {
    let started = Instant::now();
    let mut stream = loop {
        match std::os::unix::net::UnixStream::connect(socket) {
            Ok(stream) => break stream,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                ) && started.elapsed() < SUPERVISOR_CONNECT_TIMEOUT =>
            {
                thread::sleep(SOCKET_RETRY_INTERVAL);
            }
            Err(error) => return Err(DaemonError::Operation(error.to_string())),
        }
    };
    assert_same_user(&stream).map_err(|error| DaemonError::Operation(error.to_string()))?;
    let request = JsonRpcRequest {
        jsonrpc: "2.0".into(),
        id: Uuid::now_v7(),
        method: "supervisor.request".into(),
        params: serde_json::to_value(command)
            .map_err(|error| DaemonError::Operation(error.to_string()))?,
    };
    stream
        .write_all(&write_json_line(&request).map_err(|error| protocol_error(&error))?)
        .map_err(|error| DaemonError::Operation(error.to_string()))?;
    stream
        .flush()
        .map_err(|error| DaemonError::Operation(error.to_string()))?;
    let response = read_json_line::<JsonRpcResponse>(&mut BufReader::new(stream))
        .map_err(|error| protocol_error(&error))?;
    if response.id != request.id {
        return Err(DaemonError::Operation(
            "supervisor response identity does not match request".into(),
        ));
    }
    if let Some(error) = response.error {
        return Err(DaemonError::Operation(error.message));
    }
    let result = response
        .result
        .ok_or_else(|| DaemonError::Operation("supervisor returned no result".into()))?;
    serde_json::from_value(result).map_err(|error| DaemonError::Operation(error.to_string()))
}

fn supervisor_process(response: SupervisorResponse) -> Result<ProcessSnapshot, DaemonError> {
    match response {
        SupervisorResponse::Process(process) => Ok(process),
        SupervisorResponse::TaskCompleted
        | SupervisorResponse::TaskFailed { .. }
        | SupervisorResponse::TaskTimedOut
        | SupervisorResponse::TaskOutput { .. } => Err(DaemonError::Operation(
            "supervisor returned a task response for process control".into(),
        )),
    }
}

fn assert_same_user(stream: &std::os::unix::net::UnixStream) -> anyhow::Result<()> {
    let (uid, _) = getpeereid(stream)?;
    if uid != getuid() {
        anyhow::bail!("Unix-socket peer has a different uid");
    }
    Ok(())
}

fn protocol_error(error: &devfleet_protocol::ProtocolError) -> DaemonError {
    DaemonError::Operation(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, net::TcpListener, os::unix::net::UnixListener};
    use tempfile::TempDir;

    #[test]
    fn removes_a_verified_stopped_supervisor_socket_idempotently() {
        let temporary = TempDir::new().expect("temporary directory");
        let socket = temporary.path().join("supervisor.sock");
        fs::write(&socket, []).expect("socket fixture");
        remove_supervisor_socket(socket.to_str().expect("UTF-8 socket path"))
            .expect("socket cleanup");
        assert!(!socket.exists());
        remove_supervisor_socket(socket.to_str().expect("UTF-8 socket path"))
            .expect("missing socket cleanup");
    }

    #[test]
    fn preparation_fingerprint_changes_only_with_its_declared_inputs() {
        let temporary = TempDir::new().expect("temporary directory");
        let ios = temporary.path().join("ios");
        fs::create_dir_all(&ios).expect("iOS directory");
        fs::write(temporary.path().join("package-lock.json"), "first").expect("lockfile");
        fs::write(ios.join("Podfile"), "platform :ios, '18.0'").expect("Podfile");
        let first = preparation_fingerprint(temporary.path(), PreparationTaskKind::Dependencies)
            .expect("first fingerprint");
        let pods = preparation_fingerprint(temporary.path(), PreparationTaskKind::Pods)
            .expect("pods fingerprint");
        fs::write(temporary.path().join("package-lock.json"), "second").expect("lockfile");
        assert_ne!(
            first,
            preparation_fingerprint(temporary.path(), PreparationTaskKind::Dependencies)
                .expect("changed fingerprint")
        );
        assert_eq!(
            pods,
            preparation_fingerprint(temporary.path(), PreparationTaskKind::Pods)
                .expect("unrelated input does not affect Pods")
        );
    }

    #[test]
    fn removes_a_refused_supervisor_socket_before_restart() {
        let temporary = TempDir::new().expect("temporary directory");
        let socket = temporary.path().join("supervisor.sock");
        drop(UnixListener::bind(&socket).expect("supervisor socket"));
        clear_stale_supervisor_socket(&socket).expect("stale socket cleanup");
        assert!(!socket.exists());
    }

    #[test]
    fn probes_only_a_successful_loopback_readiness_response() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let port = listener.local_addr().expect("listener address").port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("request");
            let mut request = [0_u8; 256];
            let _ = stream.read(&mut request).expect("read request");
            stream
                .write_all(b"HTTP/1.0 204 No Content\r\n\r\n")
                .expect("response");
        });
        let endpoint = StoredEndpoint {
            name: "api".into(),
            service: "api".into(),
            internal_url: "http://api:3000".into(),
            host_url: format!("http://127.0.0.1:{port}"),
            device_url: format!("http://127.0.0.1:{port}"),
            readiness_path: "/health".into(),
            healthy: false,
        };
        assert!(endpoint_is_healthy(&endpoint));
        server.join().expect("server joins");
    }
}
