//! Local daemon: private IPC, persistent state, and supervisor composition.

use devfleet_core::{PlanOptions, compile_manifest, discover_git_worktree};
use devfleet_logging::LogStore;
use devfleet_protocol::{
    ComposeReconcileRequest, ComposeUpRequest, DaemonRequest, DaemonResponse, EndpointSnapshot,
    EnvironmentPlanSnapshot, EnvironmentSnapshot, ErrorCode, EventSnapshot, JsonRpcRequest,
    JsonRpcResponse, LifecycleState, LogSnapshot, OperationAccepted, OperationSnapshot,
    PROTOCOL_VERSION, PluginAction, PluginReconcileRequest, PluginSnapshot, ProcessSnapshot,
    ReadinessState, RpcFailure, SupervisorRequest, SupervisorResponse, SupervisorStartRequest,
    read_json_line, write_json_line,
};
use devfleet_state::{
    StateStore, StoredEndpoint, StoredEnvironment, StoredEvent, StoredLifecycle, StoredOperation,
    StoredProcess, StoredReadiness, VerifiedGitContext,
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
        Ok(DaemonResponse::Plan(EnvironmentPlanSnapshot {
            worktree: stored.worktree,
            profile: stored.profile,
            generation: stored.generation,
            effective_configuration: stored.effective_configuration,
        }))
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
        };
        if let Err(error) = self.start_supervisor(&control_socket, &process_plugin, &start) {
            self.state.mark_failed(environment.id)?;
            return Err(error);
        }
        let response = request_supervisor(&control_socket, SupervisorRequest::Inspect)?;
        let SupervisorResponse::Process(process) = response;
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
        };
        if let Err(error) = self.start_supervisor(&control_socket, &compose_plugin, &start) {
            self.state.mark_failed(environment.id)?;
            return Err(error);
        }
        let response = request_supervisor(&control_socket, SupervisorRequest::Inspect)?;
        let SupervisorResponse::Process(process) = response;
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

    fn status(&self, environment_id: Uuid) -> Result<DaemonResponse, DaemonError> {
        let environment = self
            .state
            .environment(environment_id)?
            .ok_or(DaemonError::NotFound)?;
        Ok(DaemonResponse::Environment(snapshot(environment)))
    }

    fn list(&self) -> Result<DaemonResponse, DaemonError> {
        let environments = self
            .state
            .environments()?
            .into_iter()
            .map(snapshot)
            .collect();
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
        let SupervisorResponse::Process(observed) = response;
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
            let SupervisorResponse::Process(process) = response;
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
            DaemonError::OwnershipUncertain => ErrorCode::OwnershipUncertain,
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

fn snapshot(environment: StoredEnvironment) -> EnvironmentSnapshot {
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
    }
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
