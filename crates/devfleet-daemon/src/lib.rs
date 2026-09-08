//! Local daemon: private IPC, persistent state, and supervisor composition.

use devfleet_core::{
    BindingIntegration as CoreBindingIntegration, BindingPhase as CoreBindingPhase,
    DEFAULT_BLOCK_SIZE, DEFAULT_PORT_RANGE, HostScheduler, Invalidation, PlanOptions, PortBlock,
    ReservedPorts, SchedulerError, compile_manifest, discover_git_worktree, metro_adapter,
    render_metro_config, resolve_bindings,
};
use devfleet_logging::LogStore;
use devfleet_protocol::{
    ArtifactKind, ArtifactSnapshot, BindingApplyRequest, BindingIntegration, BindingInvalidation,
    BindingPhase, BindingSnapshot, ComposeDestroyRequest, ComposeInspectRequest, ComposeInspection,
    ComposeInspectionState, ComposeReconcileRequest, ComposeUpRequest, DaemonRequest,
    DaemonResponse, DestroyAuthorization, DestroyRequest, DestroySnapshot, DestroyTarget,
    DestroyTargetStatus, DeviceCaptureRequest, DoctorFinding, DoctorPhase, DoctorRequest,
    DoctorSnapshot, DoctorToolInspection, EndpointSnapshot, EnvironmentPlanSnapshot,
    EnvironmentReadiness, EnvironmentSnapshot, ErrorCode, EventSnapshot,
    IdentityVerificationSnapshot, IdentityVerificationStartRequest, InitializationRunRequest,
    InitializationTask, InitializationTaskKind, InitializationTaskSnapshot,
    InitializationTaskStatus, IosAppRunRequest, IosAppSnapshot, IosSimulatorAcquireRequest,
    IosSimulatorAction, IosSimulatorActionRequest, IosSimulatorLifecycle, IosSimulatorSnapshot,
    JsonRpcRequest, JsonRpcResponse, LifecycleState, LogSnapshot, MetroReconcileRequest,
    MetroUpRequest, OperationAccepted, OperationSnapshot, PROTOCOL_VERSION, PluginAction,
    PluginReconcileRequest, PluginSnapshot, PreparationRunRequest, PreparationTaskKind,
    PreparationTaskSnapshot, PreparationTaskStatus, ProcessSnapshot, ProcessSpec,
    ProjectInitRequest, ProjectInitSnapshot, ReactNativeIdentityHook, ReadinessState,
    ResourceAction, ResourceActionRequest, ResourceActionSnapshot, RpcFailure, ScheduledResource,
    SchedulingPriority, SchedulingRequest, SupervisorIdentityVerificationRequest,
    SupervisorRequest, SupervisorResponse, SupervisorStartRequest, TaskRerunPolicy,
    TrustedTaskPlugin, VerificationMode, read_json_line, write_json_line,
};
use devfleet_state::{
    PortLeaseRequest, StateStore, StoredArtifact, StoredBinding, StoredComposeResource,
    StoredEndpoint, StoredEnvironment, StoredEvent, StoredIdentityVerification,
    StoredInitializationTask, StoredIosApp, StoredIosDevice, StoredLifecycle, StoredOperation,
    StoredPreparationTask, StoredProcess, StoredProjectSetup, StoredReadiness, VerifiedGitContext,
};
use fs2::FileExt;
use nix::poll::{PollFd, PollFlags, poll};
use nix::unistd::{getpeereid, getuid};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    net::{SocketAddr, TcpStream},
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        io::AsFd,
        net::UnixListener,
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver, Sender, TryRecvError},
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
const NATIVE_BUILD_MEMORY_ESTIMATE: u64 = 2 * 1024 * 1024 * 1024;
const DEVICE_BOOT_MEMORY_ESTIMATE: u64 = 512 * 1024 * 1024;
const PREPARATION_MEMORY_ESTIMATE: u64 = 1024 * 1024 * 1024;
const METRO_WORKER_MEMORY_ESTIMATE: u64 = 128 * 1024 * 1024;
const DOCTOR_PLUGIN_TIMEOUT: Duration = Duration::from_secs(2);

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
    let state = StateStore::open(&config.state_dir.join("state.sqlite3"))?;
    listener.set_nonblocking(true)?;
    let (scoped_exec_completions, scoped_exec_receiver) = mpsc::channel();
    let mut daemon = Daemon {
        config,
        state,
        scheduler: HostScheduler::default(),
        scoped_exec_completions,
    };
    daemon.reconcile_startup()?;

    loop {
        daemon.drain_scoped_exec_completions(&scoped_exec_receiver);
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false)?;
                assert_same_user(&stream)?;
                if let Err(error) = daemon.handle_connection(stream) {
                    eprintln!("daemon client request failed: {error:#}");
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(SOCKET_RETRY_INTERVAL);
            }
            Err(error) => return Err(error.into()),
        }
    }
}

struct Daemon {
    config: DaemonConfig,
    state: StateStore,
    scheduler: HostScheduler,
    scoped_exec_completions: Sender<ScopedExecCompletion>,
}

struct ScopedExecCompletion {
    environment_id: Uuid,
    generation: u64,
    operation_id: Uuid,
    result: Result<SupervisorResponse, String>,
}

#[derive(serde::Deserialize)]
struct IosDiscovery {
    runtimes: Vec<IosAvailableSelection>,
    device_types: Vec<IosAvailableSelection>,
    devices: Vec<IosDiscoveredDevice>,
}

#[derive(serde::Deserialize)]
struct IosAvailableSelection {
    identifier: String,
    available: bool,
}

#[derive(serde::Deserialize)]
struct IosDiscoveredDevice {
    udid: String,
    name: String,
    runtime: String,
    available: bool,
    state: String,
}

struct CaptureTask {
    environment_id: Uuid,
    process: StoredProcess,
    kind: ArtifactKind,
    udid: String,
    artifact_root: PathBuf,
    path: PathBuf,
    timeout_seconds: u64,
}

struct IdentityLaunchConfig {
    backend_identity: String,
    backend_identity_url: String,
    identity_hook_module: String,
}

struct SnapshotInputs {
    tasks: Vec<StoredInitializationTask>,
    preparation: Vec<StoredPreparationTask>,
    bindings: Vec<StoredBinding>,
    ios: Option<StoredIosDevice>,
    app: Option<StoredIosApp>,
    identity: Option<StoredIdentityVerification>,
    metro_ready: bool,
    scheduler: devfleet_protocol::SchedulerSnapshot,
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
            DaemonRequest::ProjectInit(request) => self.project_init(request),
            DaemonRequest::Doctor(request) => self.doctor(&request),
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
            DaemonRequest::IdentityVerificationStart(request) => {
                self.identity_verification_start(&request)
            }
            DaemonRequest::IdentityVerificationStatus { environment_id } => {
                self.identity_verification_status(environment_id)
            }
            DaemonRequest::DeviceCapture(request) => self.device_capture(&request),
            DaemonRequest::Destroy(request) => self.destroy(&request),
            DaemonRequest::Reconcile { environment_id } => {
                self.reconcile_environment(environment_id)
            }
            DaemonRequest::SchedulerConfigure(config) => self.scheduler_configure(config),
            DaemonRequest::SchedulerStatus => {
                Ok(DaemonResponse::Scheduler(self.scheduler.snapshot()))
            }
            DaemonRequest::SchedulerFocus { environment_id } => {
                self.scheduler_focus(environment_id)
            }
            DaemonRequest::Status { environment_id } => self.status(environment_id),
            DaemonRequest::List => self.list(),
            DaemonRequest::Stop { environment_id } => self.stop(environment_id),
            DaemonRequest::OperationGet { operation_id } => self.operation_get(operation_id),
            DaemonRequest::OperationCancel { operation_id } => self.operation_cancel(operation_id),
            DaemonRequest::ResourceAction(request) => self.resource_action(request),
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

    fn project_init(&mut self, request: ProjectInitRequest) -> Result<DaemonResponse, DaemonError> {
        if !request.trusted {
            return Err(DaemonError::TrustRequired);
        }
        let selection = match (request.runtime, request.device_type) {
            (None, None) => None,
            (Some(runtime), Some(device_type))
                if !runtime.is_empty() && !device_type.is_empty() =>
            {
                Some((runtime, device_type))
            }
            _ => {
                return Err(DaemonError::InvalidConfig(
                    "runtime and device type must be selected together".into(),
                ));
            }
        };
        let identity = discover_git_worktree(Path::new(&request.worktree))
            .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        if let Some((runtime, device_type)) = &selection {
            let plugin = self.trusted_plugin("official.ios", "device.discover")?;
            let discovery = Self::ios_discovery(&plugin)?;
            if !ios_selection_available(&discovery.runtimes, runtime) {
                return Err(DaemonError::DeviceUnavailable(format!(
                    "requested runtime '{runtime}' is not installed and available"
                )));
            }
            if !ios_selection_available(&discovery.device_types, device_type) {
                return Err(DaemonError::DeviceUnavailable(format!(
                    "requested device type '{device_type}' is not installed and available"
                )));
            }
        }
        let (runtime, device_type) = selection.map_or((None, None), |(runtime, device_type)| {
            (Some(runtime), Some(device_type))
        });
        self.state.record_project_setup(&StoredProjectSetup {
            project_fingerprint: identity.project_fingerprint.clone(),
            checkout: identity.checkout.display().to_string(),
            trusted: true,
            runtime: runtime.clone(),
            device_type: device_type.clone(),
        })?;
        Ok(DaemonResponse::ProjectInit(ProjectInitSnapshot {
            worktree: identity.checkout.display().to_string(),
            project_fingerprint: identity.project_fingerprint,
            trusted: true,
            proposed_manifest_path: identity
                .checkout
                .join("devfleet.toml")
                .display()
                .to_string(),
            proposed_manifest: proposed_manifest(),
            proposed_integrations: proposed_integrations(),
            runtime,
            device_type,
        }))
    }

    fn doctor(&self, request: &DoctorRequest) -> Result<DaemonResponse, DaemonError> {
        let requested_worktree = request.worktree.clone();
        let identity = match discover_git_worktree(Path::new(&request.worktree)) {
            Ok(identity) => identity,
            Err(error) => {
                return Ok(DaemonResponse::Doctor(DoctorSnapshot {
                    worktree: requested_worktree,
                    ready: false,
                    findings: vec![doctor_finding(
                        DoctorPhase::Project,
                        "worktree",
                        format!("Git worktree discovery failed: {error}"),
                        false,
                        "Run doctor from an existing Git worktree.",
                    )],
                }));
            }
        };
        let worktree = identity.checkout.display().to_string();
        let mut findings = Vec::new();
        let setup = self.state.project_setup(&identity.project_fingerprint)?;
        match setup.as_ref() {
            None => findings.push(doctor_finding(
                DoctorPhase::Project,
                "project_setup",
                "project trust has not been recorded",
                false,
                "Run `devfleet init --trust --worktree <path>` and review its proposal.",
            )),
            Some(setup) if !setup.trusted => findings.push(doctor_finding(
                DoctorPhase::Project,
                "project_setup",
                "project setup is not explicitly trusted",
                false,
                "Run `devfleet init --trust --worktree <path>` after reviewing the project.",
            )),
            Some(_) => {}
        }
        if !identity.checkout.join("devfleet.toml").is_file() {
            findings.push(doctor_finding(
                DoctorPhase::Project,
                "devfleet.toml",
                "the proposed project manifest has not been added",
                false,
                "Review `devfleet init` output and add devfleet.toml explicitly.",
            ));
        }

        self.doctor_plugin(&mut findings, "official.process", "process.reconcile")?;
        let compose_plugin =
            self.doctor_plugin(&mut findings, "official.compose", "compose.inspect")?;
        let react_native_plugin = self.doctor_plugin(
            &mut findings,
            "official.react-native",
            "preparation.dependencies",
        )?;
        let ios_plugin = self.doctor_plugin(&mut findings, "official.ios", "device.discover")?;

        Self::doctor_toolchain(
            &identity.checkout,
            react_native_plugin.as_deref(),
            ios_plugin.as_deref(),
            &mut findings,
        );
        Self::doctor_docker(&identity.checkout, compose_plugin.as_deref(), &mut findings);
        Self::doctor_runtime(setup.as_ref(), ios_plugin.as_deref(), &mut findings);
        self.doctor_environment(request.environment_id, &identity, &mut findings)?;

        let ready = findings.iter().all(|finding| !finding.blocking);
        Ok(DaemonResponse::Doctor(DoctorSnapshot {
            worktree,
            ready,
            findings,
        }))
    }

    fn doctor_plugin(
        &self,
        findings: &mut Vec<DoctorFinding>,
        id: &str,
        capability: &str,
    ) -> Result<Option<PathBuf>, DaemonError> {
        let Some(plugin) = self.state.plugin(id)? else {
            findings.push(doctor_finding(
                DoctorPhase::Plugin,
                id,
                "plugin is not installed",
                true,
                format!("Install and explicitly trust {id} with capability {capability}."),
            ));
            return Ok(None);
        };
        if !plugin.trusted {
            findings.push(doctor_finding(
                DoctorPhase::Plugin,
                id,
                "plugin is installed but not explicitly trusted",
                false,
                format!("Review and run `devfleet plugin trust {id}`."),
            ));
            return Ok(None);
        }
        match self.trusted_plugin(id, capability) {
            Ok(path) => Ok(Some(path)),
            Err(error) => {
                findings.push(doctor_finding(
                    DoctorPhase::Plugin,
                    id,
                    format!(
                        "plugin descriptor, lockfile, or capability validation failed: {error}"
                    ),
                    true,
                    format!(
                        "Reinstall a compatible {id} and explicitly trust its reviewed lockfile."
                    ),
                ));
                Ok(None)
            }
        }
    }

    fn doctor_toolchain(
        worktree: &Path,
        react_native_plugin: Option<&Path>,
        ios_plugin: Option<&Path>,
        findings: &mut Vec<DoctorFinding>,
    ) {
        Self::doctor_provider_tools(
            react_native_plugin,
            PluginAction::Validate {
                configuration: serde_json::json!({ "worktree": worktree.display().to_string() }),
            },
            "official.react-native",
            findings,
        );
        Self::doctor_provider_tools(ios_plugin, PluginAction::Inspect, "official.ios", findings);
    }

    fn doctor_provider_tools(
        plugin: Option<&Path>,
        action: PluginAction,
        provider: &str,
        findings: &mut Vec<DoctorFinding>,
    ) {
        let Some(plugin) = plugin else {
            return;
        };
        let inspection = Self::doctor_plugin_call(plugin, action).and_then(|value| {
            serde_json::from_value::<DoctorToolInspection>(value).map_err(|_| {
                DaemonError::Operation(format!(
                    "{provider} toolchain inspection returned malformed data"
                ))
            })
        });
        match inspection {
            Ok(inspection) => {
                for check in inspection
                    .checks
                    .into_iter()
                    .filter(|check| !check.available)
                {
                    findings.push(doctor_finding(
                        DoctorPhase::Toolchain,
                        check.resource.as_str(),
                        check
                            .cause
                            .unwrap_or_else(|| format!("{} is unavailable", check.resource)),
                        true,
                        tool_remediation(&check.resource),
                    ));
                }
            }
            Err(error) => findings.push(doctor_finding(
                DoctorPhase::Toolchain,
                provider,
                format!("toolchain inspection failed: {error}"),
                true,
                format!("Repair or reinstall the trusted {provider} plugin, then retry doctor."),
            )),
        }
    }

    fn doctor_docker(
        worktree: &Path,
        compose_plugin: Option<&Path>,
        findings: &mut Vec<DoctorFinding>,
    ) {
        let Some(plugin) = compose_plugin else {
            return;
        };
        let input = serde_json::to_value(ComposeInspectRequest {
            project_name: "df_00000000_00000000".into(),
            worktree: worktree.display().to_string(),
        });
        let inspection = input
            .map_err(|error| DaemonError::Operation(error.to_string()))
            .and_then(|input| Self::doctor_plugin_action(plugin, "compose.inspect", input))
            .and_then(|value| {
                serde_json::from_value::<ComposeInspection>(value).map_err(|_| {
                    DaemonError::Operation("Compose inspection returned malformed data".into())
                })
            });
        match inspection {
            Ok(ComposeInspection {
                state: ComposeInspectionState::Missing | ComposeInspectionState::Present,
                ..
            }) => {}
            Ok(ComposeInspection {
                state: ComposeInspectionState::Unavailable,
                ..
            }) => findings.push(doctor_finding(
                DoctorPhase::Toolchain,
                "docker",
                "Docker is unavailable",
                true,
                "Start local Docker Desktop and select a local Unix-socket context.",
            )),
            Ok(ComposeInspection {
                state: ComposeInspectionState::Uncertain,
                ..
            }) => findings.push(doctor_finding(
                DoctorPhase::Toolchain,
                "docker",
                "Docker context is remote or cannot be verified as local",
                false,
                "Select a local Unix-socket Docker context; v0.1 does not support remote Docker.",
            )),
            Err(error) => findings.push(doctor_finding(
                DoctorPhase::Toolchain,
                "docker",
                format!("Docker inspection failed: {error}"),
                true,
                "Repair the local Docker installation and retry doctor.",
            )),
        }
    }

    fn doctor_runtime(
        setup: Option<&StoredProjectSetup>,
        ios_plugin: Option<&Path>,
        findings: &mut Vec<DoctorFinding>,
    ) {
        let Some(setup) = setup else {
            return;
        };
        let (Some(runtime), Some(device_type)) = (&setup.runtime, &setup.device_type) else {
            findings.push(doctor_finding(
                DoctorPhase::Runtime,
                "simulator_selection",
                "no local iOS runtime and device type have been pinned",
                false,
                "Run `devfleet init --trust --runtime <id> --device-type <id> --worktree <path>`.",
            ));
            return;
        };
        let Some(plugin) = ios_plugin else {
            return;
        };
        match Self::doctor_ios_discovery(plugin) {
            Ok(discovery) => {
                if !ios_selection_available(&discovery.runtimes, runtime) {
                    findings.push(doctor_finding(
                        DoctorPhase::Runtime,
                        runtime,
                        "the pinned runtime is not installed and available",
                        true,
                        "Install the selected iOS runtime, then rerun init to confirm it.",
                    ));
                }
                if !ios_selection_available(&discovery.device_types, device_type) {
                    findings.push(doctor_finding(
                        DoctorPhase::Runtime,
                        device_type,
                        "the pinned device type is not installed and available",
                        true,
                        "Install the selected simulator device type, then rerun init to confirm it.",
                    ));
                }
            }
            Err(error) => findings.push(doctor_finding(
                DoctorPhase::Runtime,
                "ios_simulator",
                format!("iOS runtime discovery failed: {error}"),
                true,
                "Install or select Xcode CoreSimulator, then rerun doctor.",
            )),
        }
    }

    fn doctor_environment(
        &self,
        environment_id: Option<Uuid>,
        identity: &devfleet_core::GitWorktreeIdentity,
        findings: &mut Vec<DoctorFinding>,
    ) -> Result<(), DaemonError> {
        let Some(environment_id) = environment_id else {
            findings.push(doctor_finding(
                DoctorPhase::Bindings,
                "environment",
                "bindings cannot be verified without an environment ID",
                false,
                "Run plan or up, then pass --environment-id to doctor.",
            ));
            findings.push(doctor_finding(
                DoctorPhase::Preparation,
                "environment",
                "preparation cannot be verified without an environment ID",
                false,
                "Run plan or up, then pass --environment-id to doctor.",
            ));
            return Ok(());
        };
        let Some(environment) = self.state.environment(environment_id)? else {
            findings.push(doctor_finding(
                DoctorPhase::Project,
                "environment",
                "requested environment does not exist",
                false,
                "Pass an environment ID returned by plan or up for this worktree.",
            ));
            return Ok(());
        };
        if environment.project_fingerprint != identity.project_fingerprint
            || environment.worktree_fingerprint != identity.worktree_fingerprint
        {
            findings.push(doctor_finding(
                DoctorPhase::Project,
                "environment",
                "requested environment does not belong to this worktree",
                false,
                "Run doctor against the environment's own worktree.",
            ));
            return Ok(());
        }
        let bindings = self.state.bindings(environment_id)?;
        if !bindings
            .iter()
            .any(|binding| binding.phase == "bundle" && binding.integration == "bundle_config")
        {
            findings.push(doctor_finding(
                DoctorPhase::Bindings,
                "bundle_integration",
                "a bundle_config binding has not been applied",
                true,
                "Apply an explicit bundle binding before launching a React Native app.",
            ));
        }
        let preparation = self.state.preparation_tasks(environment_id)?;
        for kind in ["dependencies", "pods"] {
            match preparation.iter().find(|task| task.kind == kind) {
                Some(task) if task.status == "completed" => {}
                Some(task) => findings.push(doctor_finding(
                    DoctorPhase::Preparation,
                    kind,
                    format!("preparation is {}", task.status),
                    true,
                    "Run `devfleet prepare run <environment-id>` and inspect its logs.",
                )),
                None => findings.push(doctor_finding(
                    DoctorPhase::Preparation,
                    kind,
                    "preparation has not run",
                    true,
                    "Run `devfleet prepare run <environment-id>` before native build.",
                )),
            }
        }
        Ok(())
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
        if matching_environment
            .is_some_and(|environment| environment.lifecycle == StoredLifecycle::Degraded)
        {
            return Err(DaemonError::OwnershipUncertain);
        }
        if let Some(environment) = matching_environment.filter(|environment| {
            matches!(
                environment.lifecycle,
                StoredLifecycle::Starting | StoredLifecycle::Running
            )
        }) {
            self.state
                .update_worktree_context(environment.id, &git_context)?;
            let operation = self
                .state
                .latest_operation_of_kind(environment.id, "up")?
                .ok_or_else(|| {
                    DaemonError::Operation(
                        "running environment has no retained up operation identity".into(),
                    )
                })?;
            return Ok(DaemonResponse::Accepted(OperationAccepted {
                operation_id: operation.id,
                environment_id: environment.id,
            }));
        }
        cleanup_stopped_supervisor_socket(matching_environment)?;
        let process_plugin = self.trusted_process_plugin()?;

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
        if !request.trusted || request.profile.is_empty() || request.max_workers == 0 {
            return Err(if request.trusted {
                DaemonError::InvalidConfig("Metro requires profile and worker budget".into())
            } else {
                DaemonError::TrustRequired
            });
        }
        let worktree = Path::new(&request.worktree)
            .canonicalize()
            .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        let git_identity = discover_git_worktree(&worktree)
            .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        let key = environment_key(
            &git_identity.project_fingerprint,
            &git_identity.worktree_fingerprint,
            &request.profile,
            request.instance.as_deref(),
        );
        if let Some(environment) = self.state.environments()?.into_iter().find(|environment| {
            environment.key == key
                && matches!(
                    environment.lifecycle,
                    StoredLifecycle::Starting | StoredLifecycle::Running
                )
        }) {
            return self.status(environment.id);
        }
        let automatically_assigned = request.port == 0;
        let port = if automatically_assigned {
            *self
                .reserve_automatic_ports(
                    &key,
                    &git_identity.project_fingerprint,
                    1,
                    BTreeSet::from(["metro".into()]),
                )?
                .get("metro")
                .ok_or_else(|| DaemonError::InvalidConfig("Metro port assignment missing".into()))?
        } else {
            request.port
        };
        let mut port_reservation = automatically_assigned
            .then(|| ReservedPorts::bind(BTreeMap::from([("metro".into(), port)])))
            .transpose()
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
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
            port,
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
                    port,
                    cache_directory: adapter.cache_directory.display().to_string(),
                    configuration_path: configuration_path.display().to_string(),
                    max_workers: request.max_workers,
                })
                .map_err(|error| DaemonError::Operation(error.to_string()))?,
            )?)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let up_request = devfleet_protocol::EnvironmentUpRequest {
            worktree: worktree.display().to_string(),
            profile: request.profile,
            instance: request.instance,
            trusted: true,
            process: devfleet_protocol::ProcessSpec {
                argv: spawn.argv,
                cwd: spawn.cwd,
                environment: spawn.environment,
            },
        };
        let mut start_failure = None;
        let mut accepted_response = None;
        let handoff = if let Some(reservation) = port_reservation.as_mut() {
            reservation.handoff(
                || match self.up(up_request) {
                    Ok(response) => {
                        accepted_response = Some(response);
                        Ok(())
                    }
                    Err(error) => {
                        start_failure = Some(error);
                        Err(devfleet_core::PortError::PortConflict)
                    }
                },
                |reserved_port| wait_for_metro_identity(reserved_port, &expected_identity),
            )
        } else {
            accepted_response = Some(self.up(up_request)?);
            Ok(())
        };
        if let Some(error) = start_failure {
            return Err(error);
        }
        if let Err(error) = handoff {
            if let Some(DaemonResponse::Accepted(accepted)) = &accepted_response {
                let _ = self.stop(accepted.environment_id);
            }
            return Err(DaemonError::Operation(error.to_string()));
        }
        let response = accepted_response.ok_or_else(|| {
            DaemonError::Operation("Metro startup returned no operation response".into())
        })?;
        let DaemonResponse::Accepted(accepted) = response else {
            return Err(DaemonError::Operation(
                "Metro startup returned an unexpected response".into(),
            ));
        };
        let environment = self
            .state
            .environment(accepted.environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let estimated_memory_bytes = u64::from(request.max_workers)
            .checked_mul(METRO_WORKER_MEMORY_ESTIMATE)
            .ok_or_else(|| DaemonError::InvalidConfig("Metro worker estimate overflow".into()))?;
        if let Err(error) = self.begin_scheduled_work(
            &environment,
            ScheduledResource::MetroWorkers {
                workers: request.max_workers,
            },
            estimated_memory_bytes,
        ) {
            let _ = self.stop(accepted.environment_id);
            return Err(error);
        }
        if !automatically_assigned && !wait_for_metro_identity(port, &expected_identity) {
            let _ = self.stop(accepted.environment_id);
            return Err(DaemonError::Operation(
                "Metro did not expose the selected endpoint identity before its deadline".into(),
            ));
        }
        let endpoint = StoredEndpoint {
            name: "metro".into(),
            service: "metro".into(),
            internal_url: format!("http://127.0.0.1:{port}"),
            host_url: format!("http://127.0.0.1:{port}"),
            device_url: format!("http://127.0.0.1:{port}"),
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
    fn compose_up(&mut self, mut request: ComposeUpRequest) -> Result<DaemonResponse, DaemonError> {
        validate_compose_up_request(&request)?;
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
        if matching_environment
            .is_some_and(|environment| environment.lifecycle == StoredLifecycle::Degraded)
        {
            return Err(DaemonError::OwnershipUncertain);
        }
        if let Some(environment) = matching_environment.filter(|environment| {
            matches!(
                environment.lifecycle,
                StoredLifecycle::Starting | StoredLifecycle::Running
            )
        }) {
            self.state
                .update_worktree_context(environment.id, &git_context)?;
            let operation = self
                .state
                .latest_operation_of_kind(environment.id, "up")?
                .ok_or_else(|| {
                    DaemonError::Operation(
                        "running Compose environment has no retained up operation identity".into(),
                    )
                })?;
            return Ok(DaemonResponse::Accepted(OperationAccepted {
                operation_id: operation.id,
                environment_id: environment.id,
            }));
        }
        let compose_plugin = self.trusted_plugin("official.compose", "compose.reconcile")?;
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
        let mut environment = StoredEnvironment {
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
        let automatic_ports = request
            .endpoints
            .iter()
            .filter(|endpoint| endpoint.host_port == 0)
            .map(|endpoint| endpoint.name.clone())
            .collect::<BTreeSet<_>>();
        let assigned_ports = if automatic_ports.is_empty() {
            BTreeMap::new()
        } else {
            self.reserve_automatic_ports(
                &environment.key,
                &environment.project_fingerprint,
                environment.generation,
                automatic_ports,
            )?
        };
        for endpoint in &mut request.endpoints {
            if endpoint.host_port == 0 {
                endpoint.host_port = *assigned_ports.get(&endpoint.name).ok_or_else(|| {
                    DaemonError::InvalidConfig(format!(
                        "automatic port assignment missing for '{}'",
                        endpoint.name
                    ))
                })?;
            }
        }
        inject_compose_endpoint_port_environment(&mut request.environment, &request.endpoints)?;
        environment.endpoints = stored_compose_endpoints(&request.endpoints);
        let mut port_reservation = (!assigned_ports.is_empty())
            .then(|| ReservedPorts::bind(assigned_ports.clone()))
            .transpose()
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let operation_id = Uuid::now_v7();
        let compose_resource = StoredComposeResource {
            environment_id: environment.id,
            project_name: compose_project_name(environment.project_id, environment.id),
            worktree: request.worktree.clone(),
            files: request.files.clone(),
            lifecycle: "active".into(),
        };
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
                project_name: compose_resource.project_name.clone(),
                files: request.files,
                environment: request.environment,
                endpoints: request.endpoints.clone(),
                authorizations: request.authorizations,
            })),
            log_directory: log_directory.display().to_string(),
            log_max_bytes: MAX_RESOURCE_LOG_BYTES,
            redactions,
            task_plugins: self.trusted_task_plugins()?,
        };
        let mut start_failure = None;
        let handoff = if let Some(reservation) = port_reservation.as_mut() {
            reservation.handoff(
                || match self.start_supervisor(&control_socket, &compose_plugin, &start) {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        start_failure = Some(error);
                        Err(devfleet_core::PortError::PortConflict)
                    }
                },
                |port| {
                    request
                        .endpoints
                        .iter()
                        .find(|endpoint| endpoint.host_port == port)
                        .is_some_and(|endpoint| {
                            wait_for_endpoint_ready(port, &endpoint.readiness_path)
                        })
                },
            )
        } else {
            self.start_supervisor(&control_socket, &compose_plugin, &start)
                .map_err(|_| devfleet_core::PortError::PortConflict)
        };
        if let Some(error) = start_failure {
            self.state.mark_failed(environment.id)?;
            return Err(error);
        }
        if let Err(error) = handoff {
            self.state.mark_failed(environment.id)?;
            return Err(DaemonError::Operation(error.to_string()));
        }
        let response = request_supervisor(&control_socket, SupervisorRequest::Inspect)?;
        let process = supervisor_process(response)?;
        self.state
            .record_running_process(environment.id, &stored_process(&process))?;
        self.state.record_compose_resource(&compose_resource)?;
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
                | SupervisorResponse::TaskAccepted
                | SupervisorResponse::TaskCancelled
                | SupervisorResponse::TaskOutput { .. }
                | SupervisorResponse::TaskTimedOut
                | SupervisorResponse::IdentityVerification(_) => {
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
            let scheduler_job = self.begin_scheduled_work(
                &environment,
                ScheduledResource::Preparation,
                PREPARATION_MEMORY_ESTIMATE,
            )?;
            let response = match request_supervisor(
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
            ) {
                Ok(response) => response,
                Err(error) => {
                    self.finish_scheduled_work(&environment, scheduler_job)?;
                    return Err(map_react_native_capability(error, capability));
                }
            };
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
                SupervisorResponse::TaskAccepted | SupervisorResponse::TaskCancelled => {
                    Some("supervisor returned an async task response")
                }
                SupervisorResponse::Process(_) => Some("supervisor returned a process response"),
                SupervisorResponse::IdentityVerification(_) => {
                    Some("supervisor returned an identity verification response")
                }
            };
            self.finish_scheduled_work(&environment, scheduler_job)?;
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
        if present.state == "Booted" {
            Self::run_ios_task(
                &process,
                "device.boot",
                serde_json::json!({"udid": udid, "step": "wait"}),
            )?;
        } else {
            let scheduler_job = self.begin_scheduled_work(
                &environment,
                ScheduledResource::DeviceBoot,
                DEVICE_BOOT_MEMORY_ESTIMATE,
            )?;
            if let Err(error) = Self::run_ios_task(
                &process,
                "device.boot",
                serde_json::json!({"udid": udid, "step": "start"}),
            ) {
                self.finish_scheduled_work(&environment, scheduler_job)?;
                return Err(error);
            }
            if let Err(error) = Self::run_ios_task(
                &process,
                "device.boot",
                serde_json::json!({"udid": udid, "step": "wait"}),
            ) {
                self.finish_scheduled_work(&environment, scheduler_job)?;
                return Err(error);
            }
            self.finish_scheduled_work(&environment, scheduler_job)?;
        }
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
        let native_bindings = self
            .state
            .bindings(environment.id)?
            .into_iter()
            .filter(|binding| binding.phase == "native_build")
            .map(|binding| binding.fingerprint)
            .collect::<Vec<_>>();
        let native_fingerprint = native_build_fingerprint(
            Path::new(&environment.worktree),
            metro_port,
            request.scheme.as_deref(),
            &native_bindings,
        )?;
        let identity_config = identity_launch_config(request)?;
        if request.verification == VerificationMode::Transport {
            let _ = Self::cancel_launch_identity_verification(&process, true);
        }
        let identity = identity_config
            .as_ref()
            .map(|config| {
                self.start_identity_verification(
                    &environment,
                    &process,
                    &native_fingerprint,
                    &request.bundle_id,
                    &config.backend_identity,
                    &config.backend_identity_url,
                )
            })
            .transpose()?;
        if let Some(existing) = self.state.ios_app(environment.id)?
            && existing.bundle_id == request.bundle_id
            && existing.udid == udid
            && existing.metro_port == metro_port
            && existing.native_fingerprint == native_fingerprint
            && Path::new(&existing.derived_data).is_dir()
            && request.verification == VerificationMode::Transport
        {
            Self::run_ios_task(
                &process,
                "app.launch",
                serde_json::json!({"udid": udid, "bundle_id": request.bundle_id}),
            )?;
            self.state.record_ios_app(&StoredIosApp {
                launched: true,
                ..existing
            })?;
            self.record_transport_verification(environment.id, environment.generation)?;
            self.emit_event(
                environment.id,
                environment.generation,
                None,
                "app_relaunched",
            )?;
            let app = self
                .state
                .ios_app(environment.id)?
                .ok_or(DaemonError::OwnershipUncertain)?;
            return Ok(DaemonResponse::IosApp(ios_app_snapshot(app)));
        }
        let operation_id = Uuid::now_v7();
        let scheduler_job = self.begin_scheduled_work(
            &environment,
            ScheduledResource::NativeBuild,
            NATIVE_BUILD_MEMORY_ESTIMATE,
        )?;
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
                        "identity_hook": identity
                            .as_ref()
                            .zip(identity_config.as_ref())
                            .map(|(snapshot, config)| react_native_identity_hook(snapshot, config)),
                    }),
                },
                timeout_seconds: Some(1_800),
            },
        );
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.finish_scheduled_work(&environment, scheduler_job)?;
                let _ = Self::cancel_launch_identity_verification(&process, identity.is_some());
                let _ = self.mark_identity_unverified(identity.as_ref());
                return Err(map_react_native_capability(error, "app.run_ios"));
            }
        };
        match response {
            SupervisorResponse::TaskOutput { stdout, stderr } => {
                self.append_preparation_log(environment.id, "ios-app", &stdout, &stderr)?;
            }
            SupervisorResponse::TaskFailed { stdout, stderr } => {
                self.finish_scheduled_work(&environment, scheduler_job)?;
                self.append_preparation_log(environment.id, "ios-app", &stdout, &stderr)?;
                let _ = Self::cancel_launch_identity_verification(&process, identity.is_some());
                let _ = self.mark_identity_unverified(identity.as_ref());
                return Err(DaemonError::Operation(
                    "iOS app build, install, or launch failed; inspect preparation-ios-app logs"
                        .into(),
                ));
            }
            SupervisorResponse::TaskTimedOut => {
                self.finish_scheduled_work(&environment, scheduler_job)?;
                let _ = Self::cancel_launch_identity_verification(&process, identity.is_some());
                let _ = self.mark_identity_unverified(identity.as_ref());
                return Err(DaemonError::Operation(
                    "iOS app build deadline exceeded; inspect before retrying".into(),
                ));
            }
            SupervisorResponse::TaskCompleted
            | SupervisorResponse::TaskAccepted
            | SupervisorResponse::TaskCancelled
            | SupervisorResponse::Process(_)
            | SupervisorResponse::IdentityVerification(_) => {
                self.finish_scheduled_work(&environment, scheduler_job)?;
                let _ = Self::cancel_launch_identity_verification(&process, identity.is_some());
                let _ = self.mark_identity_unverified(identity.as_ref());
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
        if let Err(error) = self.state.record_ios_app(&app) {
            self.finish_scheduled_work(&environment, scheduler_job)?;
            let _ = Self::cancel_launch_identity_verification(&process, identity.is_some());
            let _ = self.mark_identity_unverified(identity.as_ref());
            return Err(error.into());
        }
        if request.verification == VerificationMode::Transport {
            self.record_transport_verification(environment.id, environment.generation)?;
        }
        self.finish_scheduled_work(&environment, scheduler_job)?;
        self.emit_event(
            environment.id,
            environment.generation,
            Some(operation_id),
            "app_launched",
        )?;
        Ok(DaemonResponse::IosApp(ios_app_snapshot(app)))
    }

    fn start_identity_verification(
        &mut self,
        environment: &StoredEnvironment,
        process: &StoredProcess,
        build_identity: &str,
        bundle_identity: &str,
        backend_identity: &str,
        backend_identity_url: &str,
    ) -> Result<IdentityVerificationSnapshot, DaemonError> {
        let snapshot = identity_verification_response(request_supervisor(
            Path::new(&process.supervisor_socket),
            SupervisorRequest::StartIdentityVerification {
                request: SupervisorIdentityVerificationRequest {
                    environment_id: environment.id,
                    generation: environment.generation,
                    build_identity: build_identity.into(),
                    bundle_identity: bundle_identity.into(),
                    backend_identity: backend_identity.into(),
                    backend_identity_url: backend_identity_url.into(),
                },
            },
        )?)?;
        self.record_identity_snapshot(&snapshot)?;
        self.emit_event(
            environment.id,
            environment.generation,
            None,
            "identity_verification_pending",
        )?;
        Ok(snapshot)
    }

    fn cancel_launch_identity_verification(
        process: &StoredProcess,
        pending: bool,
    ) -> Result<(), DaemonError> {
        if !pending {
            return Ok(());
        }
        match request_supervisor(
            Path::new(&process.supervisor_socket),
            SupervisorRequest::CancelIdentityVerification,
        )? {
            SupervisorResponse::TaskCompleted => Ok(()),
            _ => Err(DaemonError::Operation(
                "supervisor returned an invalid identity cancellation response".into(),
            )),
        }
    }

    fn record_transport_verification(
        &mut self,
        environment_id: Uuid,
        generation: u64,
    ) -> Result<(), DaemonError> {
        self.state
            .record_identity_verification(&StoredIdentityVerification {
                environment_id,
                generation,
                mode: "transport".into(),
                status: "transport".into(),
            })?;
        Ok(())
    }

    fn record_identity_snapshot(
        &mut self,
        snapshot: &IdentityVerificationSnapshot,
    ) -> Result<(), DaemonError> {
        self.state
            .record_identity_verification(&StoredIdentityVerification {
                environment_id: snapshot.environment_id,
                generation: snapshot.generation,
                mode: identity_mode_name(snapshot.mode).into(),
                status: identity_status_name(snapshot.status).into(),
            })?;
        Ok(())
    }

    fn mark_identity_unverified(
        &mut self,
        snapshot: Option<&IdentityVerificationSnapshot>,
    ) -> Result<(), DaemonError> {
        let Some(snapshot) = snapshot else {
            return Ok(());
        };
        self.state
            .record_identity_verification(&StoredIdentityVerification {
                environment_id: snapshot.environment_id,
                generation: snapshot.generation,
                mode: identity_mode_name(snapshot.mode).into(),
                status: "unverified".into(),
            })?;
        Ok(())
    }

    fn identity_verification_start(
        &mut self,
        request: &IdentityVerificationStartRequest,
    ) -> Result<DaemonResponse, DaemonError> {
        if request.backend_identity.is_empty()
            || !(request.backend_identity_url.starts_with("http://")
                || request.backend_identity_url.starts_with("https://"))
        {
            return Err(DaemonError::InvalidConfig(
                "identity verification requires a backend identity and route".into(),
            ));
        }
        let environment = self
            .state
            .environment(request.environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let process = environment
            .process
            .as_ref()
            .ok_or(DaemonError::OwnershipUncertain)?;
        let app = self
            .state
            .ios_app(environment.id)?
            .filter(|app| app.launched)
            .ok_or_else(|| {
                DaemonError::Operation("identity verification requires a launched app".into())
            })?;
        let snapshot = self.start_identity_verification(
            &environment,
            process,
            &app.native_fingerprint,
            &app.bundle_id,
            &request.backend_identity,
            &request.backend_identity_url,
        )?;
        Ok(DaemonResponse::IdentityVerification(snapshot))
    }

    fn identity_verification_status(
        &mut self,
        environment_id: Uuid,
    ) -> Result<DaemonResponse, DaemonError> {
        if self
            .state
            .identity_verification(environment_id)?
            .is_some_and(|verification| verification.mode == "transport")
        {
            return Err(DaemonError::CapabilityUnsupported(
                "identity-verified readiness requires an app-handshake profile".into(),
            ));
        }
        let environment = self
            .state
            .environment(environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let process = environment.process.ok_or(DaemonError::OwnershipUncertain)?;
        let snapshot = identity_verification_response(request_supervisor(
            Path::new(&process.supervisor_socket),
            SupervisorRequest::InspectIdentityVerification,
        )?)?;
        self.record_identity_snapshot(&snapshot)?;
        if snapshot.status != devfleet_protocol::IdentityVerificationStatus::Pending {
            self.emit_event(
                environment.id,
                environment.generation,
                None,
                match snapshot.status {
                    devfleet_protocol::IdentityVerificationStatus::Verified => "identity_verified",
                    devfleet_protocol::IdentityVerificationStatus::Mismatch => "identity_mismatch",
                    devfleet_protocol::IdentityVerificationStatus::Pending => unreachable!(),
                },
            )?;
        }
        Ok(DaemonResponse::IdentityVerification(snapshot))
    }

    fn device_capture(
        &mut self,
        request: &DeviceCaptureRequest,
    ) -> Result<DaemonResponse, DaemonError> {
        let recording_seconds = request.recording_seconds;
        if recording_seconds.is_some_and(|seconds| !(1..=300).contains(&seconds)) {
            return Err(DaemonError::InvalidConfig(
                "recording duration must be between one second and five minutes".into(),
            ));
        }
        let (environment, process, udid, app) = self.capture_resources(request.environment_id)?;
        let kind = recording_seconds.map_or(ArtifactKind::Screenshot, |_| ArtifactKind::Recording);
        let extension = match kind {
            ArtifactKind::Screenshot => "png",
            ArtifactKind::Recording => "mp4",
        };
        let directory = self
            .config
            .state_dir
            .join("artifacts")
            .join(environment.id.to_string());
        prepare_private_dir(&directory)
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let artifact_id = Uuid::now_v7();
        let path = directory.join(format!("{artifact_id}.{extension}"));
        let timestamp_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| DaemonError::Operation(error.to_string()))?
            .as_millis()
            .try_into()
            .map_err(|_| DaemonError::Operation("artifact timestamp exceeded u64".into()))?;
        let start_code = code_fingerprint(Path::new(&environment.worktree))?;
        self.run_capture_task(&CaptureTask {
            environment_id: environment.id,
            process,
            kind,
            udid: udid.clone(),
            artifact_root: directory.clone(),
            path: path.clone(),
            timeout_seconds: recording_seconds.unwrap_or(30),
        })?;
        if !path.is_file()
            || path
                .symlink_metadata()
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            return Err(DaemonError::Operation(
                "capture did not produce an owned regular artifact".into(),
            ));
        }
        let current = self
            .state
            .environment(environment.id)?
            .ok_or(DaemonError::NotFound)?;
        let obsolete = current.generation != environment.generation
            || code_fingerprint(Path::new(&environment.worktree))? != start_code;
        let command = artifact_command(kind, &udid, &path);
        let artifact = StoredArtifact {
            id: artifact_id,
            timestamp_unix_ms,
            environment_id: environment.id,
            generation: environment.generation,
            worktree: environment.worktree,
            head: environment.head,
            code_fingerprint: start_code,
            build_identity: app.native_fingerprint,
            udid,
            kind: artifact_kind_name(kind).into(),
            command,
            flow: artifact_flow(kind).into(),
            result: if obsolete { "obsolete" } else { "captured" }.into(),
            app_running: app.launched,
            functional_result: "unverified".into(),
            human_review: "unreviewed".into(),
            path: path.display().to_string(),
            obsolete,
        };
        if let Err(error) = self.state.record_artifact(&artifact) {
            remove_generated_artifact(&path);
            return Err(error.into());
        }
        self.emit_event(
            environment.id,
            environment.generation,
            None,
            if obsolete {
                "artifact_obsolete"
            } else {
                "artifact_captured"
            },
        )?;
        Ok(DaemonResponse::Artifact(artifact_snapshot(artifact)))
    }

    fn capture_resources(
        &self,
        environment_id: Uuid,
    ) -> Result<(StoredEnvironment, StoredProcess, String, StoredIosApp), DaemonError> {
        let environment = self
            .state
            .environment(environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let process = environment
            .process
            .clone()
            .ok_or(DaemonError::OwnershipUncertain)?;
        let device = self
            .state
            .ios_device(environment.id)?
            .filter(|device| device.lifecycle == "booted")
            .ok_or_else(|| {
                DaemonError::DeviceUnavailable("capture requires a booted owned simulator".into())
            })?;
        let udid = device.udid.ok_or(DaemonError::OwnershipUncertain)?;
        let app = self.state.ios_app(environment.id)?.ok_or_else(|| {
            DaemonError::Operation("capture requires a built app identity".into())
        })?;
        if !app.launched {
            return Err(DaemonError::Operation(
                "capture requires an app launched on the owned simulator".into(),
            ));
        }
        Ok((environment, process, udid, app))
    }

    fn run_capture_task(&self, task: &CaptureTask) -> Result<(), DaemonError> {
        let response = request_supervisor(
            Path::new(&task.process.supervisor_socket),
            SupervisorRequest::RunPluginTask {
                operation_id: Uuid::now_v7(),
                plugin_id: "official.ios".into(),
                action: PluginAction::Action {
                    name: artifact_flow(task.kind).into(),
                    input: serde_json::json!({
                        "udid": task.udid,
                        "artifact_root": task.artifact_root,
                        "path": task.path,
                    }),
                },
                timeout_seconds: Some(task.timeout_seconds),
            },
        );
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                remove_generated_artifact(&task.path);
                return Err(error);
            }
        };
        match response {
            SupervisorResponse::TaskOutput { stdout, stderr } => {
                self.append_preparation_log(task.environment_id, "capture", &stdout, &stderr)
            }
            SupervisorResponse::TaskFailed { stdout, stderr } => {
                self.append_preparation_log(task.environment_id, "capture", &stdout, &stderr)?;
                remove_generated_artifact(&task.path);
                Err(DaemonError::Operation(
                    "device capture failed; inspect preparation-capture logs".into(),
                ))
            }
            SupervisorResponse::TaskTimedOut if task.kind == ArtifactKind::Recording => Ok(()),
            SupervisorResponse::TaskTimedOut => {
                remove_generated_artifact(&task.path);
                Err(DaemonError::Operation(
                    "screenshot capture deadline exceeded".into(),
                ))
            }
            SupervisorResponse::TaskCompleted
            | SupervisorResponse::TaskAccepted
            | SupervisorResponse::TaskCancelled
            | SupervisorResponse::Process(_)
            | SupervisorResponse::IdentityVerification(_) => {
                remove_generated_artifact(&task.path);
                Err(DaemonError::Operation(
                    "supervisor returned an invalid capture response".into(),
                ))
            }
        }
    }

    fn ios_discovery(plugin: &Path) -> Result<IosDiscovery, DaemonError> {
        let value = Self::plugin_action(plugin, "device.discover", serde_json::json!({}))?;
        serde_json::from_value(value).map_err(|_| {
            DaemonError::DeviceUnavailable("iOS discovery returned malformed data".into())
        })
    }

    fn doctor_ios_discovery(plugin: &Path) -> Result<IosDiscovery, DaemonError> {
        let value = Self::doctor_plugin_action(plugin, "device.discover", serde_json::json!({}))?;
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
            | SupervisorResponse::TaskAccepted
            | SupervisorResponse::TaskCancelled
            | SupervisorResponse::TaskTimedOut
            | SupervisorResponse::Process(_)
            | SupervisorResponse::IdentityVerification(_) => Err(DaemonError::Operation(
                "supervisor returned an invalid iOS task response".into(),
            )),
        }
    }

    fn plugin_action(
        plugin: &Path,
        name: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, DaemonError> {
        Self::plugin_call(
            plugin,
            PluginAction::Action {
                name: name.into(),
                input,
            },
        )
    }

    fn doctor_plugin_action(
        plugin: &Path,
        name: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, DaemonError> {
        Self::doctor_plugin_call(
            plugin,
            PluginAction::Action {
                name: name.into(),
                input,
            },
        )
    }

    fn doctor_plugin_call(
        plugin: &Path,
        action: PluginAction,
    ) -> Result<serde_json::Value, DaemonError> {
        Self::plugin_call_with_timeout(plugin, action, Some(DOCTOR_PLUGIN_TIMEOUT))
    }

    fn plugin_call(plugin: &Path, action: PluginAction) -> Result<serde_json::Value, DaemonError> {
        Self::plugin_call_with_timeout(plugin, action, None)
    }

    fn plugin_call_with_timeout(
        plugin: &Path,
        action: PluginAction,
        timeout: Option<Duration>,
    ) -> Result<serde_json::Value, DaemonError> {
        let action_name = plugin_action_name(&action).to_owned();
        let request = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Uuid::now_v7(),
            method: "plugin.call".into(),
            params: serde_json::to_value(devfleet_protocol::PluginCall {
                protocol: PROTOCOL_VERSION,
                plugin_state_version: 1,
                operation_id: Uuid::now_v7(),
                action,
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
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| DaemonError::Operation("plugin stdout unavailable".into()))?;
        let (status, stdout) = wait_for_plugin_response(child, stdout, timeout)?;
        if !status.success() || stdout.len() > devfleet_protocol::MAX_FRAME_BYTES {
            return Err(DaemonError::CapabilityUnsupported(action_name));
        }
        let response = read_json_line::<JsonRpcResponse>(&mut BufReader::new(stdout.as_slice()))
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
        let identity = self.state.identity_verification(environment_id)?;
        let metro_ready = self.metro_ready(environment.worktree_id)?;
        Ok(DaemonResponse::Environment(snapshot(
            environment,
            SnapshotInputs {
                tasks,
                preparation,
                bindings,
                ios,
                app,
                identity,
                metro_ready,
                scheduler: self.scheduler.snapshot(),
            },
        )))
    }

    fn list(&self) -> Result<DaemonResponse, DaemonError> {
        let environments = self.state.environments()?;
        let scheduler = self.scheduler.snapshot();
        let snapshots = environments
            .into_iter()
            .map(|environment| {
                let tasks = self.state.initialization_tasks(environment.id)?;
                let preparation = self.state.preparation_tasks(environment.id)?;
                let bindings = self.state.bindings(environment.id)?;
                let ios = self.state.ios_device(environment.id)?;
                let app = self.state.ios_app(environment.id)?;
                let identity = self.state.identity_verification(environment.id)?;
                let metro_ready = self.metro_ready(environment.worktree_id)?;
                Ok::<_, DaemonError>(snapshot(
                    environment,
                    SnapshotInputs {
                        tasks,
                        preparation,
                        bindings,
                        ios,
                        app,
                        identity,
                        metro_ready,
                        scheduler: scheduler.clone(),
                    },
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(DaemonResponse::Environments(snapshots))
    }

    fn reconcile_startup(&mut self) -> Result<(), DaemonError> {
        let environment_ids = self
            .state
            .environments()?
            .into_iter()
            .map(|environment| environment.id)
            .collect::<Vec<_>>();
        for environment_id in environment_ids {
            self.reconcile_environment_state(environment_id)?;
        }
        Ok(())
    }

    fn reconcile_environment(
        &mut self,
        environment_id: Uuid,
    ) -> Result<DaemonResponse, DaemonError> {
        self.reconcile_environment_state(environment_id)?;
        self.status(environment_id)
    }

    #[allow(clippy::too_many_lines)]
    fn reconcile_environment_state(&mut self, environment_id: Uuid) -> Result<(), DaemonError> {
        let environment = self
            .state
            .environment(environment_id)?
            .ok_or(DaemonError::NotFound)?;
        if !matches!(
            environment.lifecycle,
            StoredLifecycle::Starting | StoredLifecycle::Running | StoredLifecycle::Degraded
        ) {
            return Ok(());
        }
        let was_degraded = environment.lifecycle == StoredLifecycle::Degraded;
        let operation = self.state.latest_operation(environment.id)?;
        if let Some(process) = environment.process.clone() {
            match request_supervisor(
                Path::new(&process.supervisor_socket),
                SupervisorRequest::Inspect,
            ) {
                Ok(SupervisorResponse::Process(observed))
                    if observed.pid == process.pid
                        && observed.process_group == process.process_group
                        && observed.readiness == ReadinessState::ProcessAlive =>
                {
                    self.state
                        .record_running_process(environment.id, &stored_process(&observed))?;
                }
                Ok(_) => {
                    return self.quarantine_reconciliation(
                        &environment,
                        "supervisor identity differs from the retained journal; no process was adopted or terminated",
                    );
                }
                Err(_) => {
                    return self.quarantine_reconciliation(
                        &environment,
                        "supervisor is unavailable; external resources and leases remain retained",
                    );
                }
            }
        } else {
            if operation
                .as_ref()
                .and_then(|operation| operation.resource_id.as_deref())
                != Some("process")
            {
                return self.quarantine_reconciliation(
                        &environment,
                        "an external effect may have completed before its process identity was persisted",
                    );
            }
            let socket = supervisor_socket_path(environment.id)?;
            match request_supervisor(&socket, SupervisorRequest::Inspect) {
                Ok(SupervisorResponse::Process(observed))
                    if observed.readiness == ReadinessState::ProcessAlive =>
                {
                    self.state
                        .record_running_process(environment.id, &stored_process(&observed))?;
                }
                _ => {
                    return self.quarantine_reconciliation(
                        &environment,
                        "an accepted operation has no persisted process identity",
                    );
                }
            }
        }

        if operation
            .as_ref()
            .and_then(|operation| operation.resource_id.as_deref())
            == Some("compose")
            && self.state.compose_resource(environment.id)?.is_none()
        {
            return self.quarantine_reconciliation(
                &environment,
                "Compose may exist but its exact ownership record was not persisted",
            );
        }
        if let Some(compose) = self
            .state
            .compose_resource(environment.id)?
            .filter(|resource| resource.lifecycle == "active")
        {
            let Ok(plugin) = self.trusted_plugin("official.compose", "compose.inspect") else {
                return self.quarantine_reconciliation(
                    &environment,
                    "the Compose label inspector is unavailable; retained project ownership was not assumed",
                );
            };
            let inspection = Self::plugin_action(
                &plugin,
                "compose.inspect",
                serde_json::json!({
                    "project_name": compose.project_name,
                    "worktree": compose.worktree,
                }),
            )
            .and_then(|value| {
                serde_json::from_value::<ComposeInspection>(value)
                    .map_err(|error| DaemonError::Operation(error.to_string()))
            });
            match inspection {
                Ok(inspection)
                    if inspection.project_name == compose.project_name
                        && inspection.state == ComposeInspectionState::Present => {}
                Ok(ComposeInspection {
                    state: ComposeInspectionState::Missing,
                    ..
                }) => {
                    return self.quarantine_reconciliation(
                        &environment,
                        "the retained Compose labels are absent after inspection",
                    );
                }
                Ok(ComposeInspection {
                    state: ComposeInspectionState::Unavailable,
                    ..
                }) => {
                    return self.quarantine_reconciliation(
                        &environment,
                        "Docker is unavailable after inspection; no Compose resource was recreated",
                    );
                }
                Ok(_) | Err(_) => {
                    self.state.mark_compose_uncertain(environment.id)?;
                    return self.quarantine_reconciliation(
                        &environment,
                        "the retained Compose labels do not prove the exact project identity",
                    );
                }
            }
        }
        if self
            .state
            .initialization_tasks(environment.id)?
            .iter()
            .any(|task| task.status == "running")
            || self
                .state
                .preparation_tasks(environment.id)?
                .iter()
                .any(|task| task.status == "running")
        {
            return self.quarantine_reconciliation(
                &environment,
                "a mutable task was interrupted before its result was persisted",
            );
        }
        if let Some(app) = self.state.ios_app(environment.id)?
            && !Path::new(&app.derived_data).is_dir()
        {
            return self.quarantine_reconciliation(
                &environment,
                "the retained native build identity no longer has its derived-data directory",
            );
        }
        if let Some(device) = self.state.ios_device(environment.id)? {
            let Some(udid) = device.udid else {
                return self.quarantine_reconciliation(
                    &environment,
                    "simulator creation was interrupted before a UDID was persisted",
                );
            };
            let Ok(plugin) = self.trusted_plugin("official.ios", "device.discover") else {
                return self.quarantine_reconciliation(
                    &environment,
                    "the iOS inspector is unavailable; retained UDID ownership was not assumed",
                );
            };
            let Ok(discovery) = Self::ios_discovery(&plugin) else {
                return self.quarantine_reconciliation(
                    &environment,
                    "CoreSimulator inspection failed; retained UDID ownership was not assumed",
                );
            };
            let exact_device = discovery.devices.iter().any(|candidate| {
                candidate.udid.eq_ignore_ascii_case(&udid)
                    && candidate.name == device.expected_name
                    && candidate.runtime == device.runtime
                    && candidate.available
            });
            if !exact_device {
                self.state.set_ios_lifecycle(environment.id, "uncertain")?;
                return self.quarantine_reconciliation(
                    &environment,
                    "the retained UDID no longer matches its exact simulator identity",
                );
            }
        }
        if environment
            .endpoints
            .iter()
            .any(|endpoint| !endpoint_is_healthy(endpoint))
        {
            return self.quarantine_reconciliation(
                &environment,
                "an external endpoint is unavailable after recovery inspection",
            );
        }

        if was_degraded {
            self.state.require_app_reverification(environment.id)?;
        }
        self.state.mark_recovered(environment.id)?;
        self.emit_event(
            environment.id,
            environment.generation,
            None,
            if was_degraded {
                "reconciliation_recovered_reverify_app"
            } else {
                "reconciliation_recovered"
            },
        )?;
        Ok(())
    }

    fn quarantine_reconciliation(
        &mut self,
        environment: &StoredEnvironment,
        detail: &str,
    ) -> Result<(), DaemonError> {
        let diagnostic = format!(
            "{detail}. Run `devfleet reconcile {}` after restoring the required tool or inspecting the retained resource.",
            environment.id
        );
        self.state
            .mark_reconciliation_uncertain(environment.id, &diagnostic)?;
        self.emit_event(
            environment.id,
            environment.generation,
            None,
            "reconciliation_quarantined",
        )
    }

    fn scheduler_configure(
        &mut self,
        config: devfleet_protocol::SchedulerConfig,
    ) -> Result<DaemonResponse, DaemonError> {
        self.scheduler
            .configure(config)
            .map_err(DaemonError::Scheduler)?;
        Ok(DaemonResponse::Scheduler(self.scheduler.snapshot()))
    }

    fn scheduler_focus(&mut self, environment_id: Uuid) -> Result<DaemonResponse, DaemonError> {
        let environment = self
            .state
            .environment(environment_id)?
            .ok_or(DaemonError::NotFound)?;
        self.scheduler.focus(environment.id);
        self.emit_event(
            environment.id,
            environment.generation,
            None,
            "scheduler_focus",
        )?;
        Ok(DaemonResponse::Scheduler(self.scheduler.snapshot()))
    }

    fn begin_scheduled_work(
        &mut self,
        environment: &StoredEnvironment,
        resource: ScheduledResource,
        estimated_memory_bytes: u64,
    ) -> Result<Uuid, DaemonError> {
        let job_id = self.scheduler.enqueue(
            SchedulingRequest {
                environment_id: environment.id,
                project_id: environment.project_id,
                resource,
                priority: SchedulingPriority::Interactive,
                focused: false,
                estimated_memory_bytes,
            },
            unix_now_ms()?,
        )?;
        self.emit_scheduler_event(environment, "scheduler_queued", job_id)?;
        let outcome = match self.scheduler.admit_next(unix_now_ms()?) {
            Ok(Some(admitted)) if admitted == job_id => {
                self.emit_scheduler_event(environment, "scheduler_admitted", job_id)?;
                Ok(job_id)
            }
            Ok(Some(_)) => Err(DaemonError::Operation(
                "a previously queued operation must be resumed by its controller".into(),
            )),
            Ok(None) => Err(DaemonError::Scheduler(SchedulerError::ResourceUnavailable(
                "host resource slots are exhausted".into(),
            ))),
            Err(error) => Err(DaemonError::Scheduler(error)),
        };
        if outcome.is_err() {
            self.scheduler.cancel(job_id)?;
            self.emit_scheduler_event(environment, "scheduler_cancelled", job_id)?;
        }
        outcome
    }

    fn finish_scheduled_work(
        &mut self,
        environment: &StoredEnvironment,
        job_id: Uuid,
    ) -> Result<(), DaemonError> {
        self.scheduler.complete(job_id)?;
        self.emit_scheduler_event(environment, "scheduler_completed", job_id)
    }

    fn release_scheduled_work(
        &mut self,
        environment: &StoredEnvironment,
    ) -> Result<(), DaemonError> {
        for job_id in self.scheduler.complete_environment(environment.id) {
            self.emit_scheduler_event(environment, "scheduler_released", job_id)?;
        }
        Ok(())
    }

    fn emit_scheduler_event(
        &mut self,
        environment: &StoredEnvironment,
        kind: &str,
        job_id: Uuid,
    ) -> Result<(), DaemonError> {
        let scheduler = self.scheduler.snapshot();
        let job = scheduler
            .jobs
            .iter()
            .find(|job| job.job_id == job_id)
            .ok_or(DaemonError::Scheduler(SchedulerError::NotFound))?;
        self.state.append_event(&StoredEvent {
            sequence: 0,
            id: Uuid::now_v7(),
            timestamp_unix_ms: u128::from(unix_now_ms()?),
            environment_id: environment.id,
            resource_id: Some(format!("scheduler:{job_id}")),
            generation: environment.generation,
            operation_id: None,
            kind: kind.into(),
            payload: serde_json::json!({
                "priority": job.priority,
                "resource": job.resource,
                "queued_reason": job.queued_reason,
                "pressure": scheduler.pressure,
            }),
        })?;
        Ok(())
    }

    fn metro_ready(&self, worktree_id: Uuid) -> Result<bool, DaemonError> {
        Ok(self.state.environments()?.into_iter().any(|candidate| {
            candidate.worktree_id == worktree_id
                && matches!(
                    candidate.lifecycle,
                    StoredLifecycle::Starting | StoredLifecycle::Running
                )
                && candidate
                    .endpoints
                    .iter()
                    .any(|endpoint| endpoint.name == "metro" && endpoint_is_healthy(endpoint))
        }))
    }

    fn stop(&mut self, environment_id: Uuid) -> Result<DaemonResponse, DaemonError> {
        let environment = self
            .state
            .environment(environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let process = environment
            .process
            .as_ref()
            .ok_or(DaemonError::OwnershipUncertain)?;
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
        let _ = self.state.mark_port_lease_stopped(&environment.key)?;
        remove_supervisor_socket(&process.supervisor_socket)?;
        self.release_scheduled_work(&environment)?;
        self.emit_event(environment_id, environment.generation, None, "stopped")?;
        self.status(environment_id)
    }

    #[allow(clippy::too_many_lines)]
    fn destroy(&mut self, request: &DestroyRequest) -> Result<DaemonResponse, DaemonError> {
        if !request.dry_run && request.authorization == DestroyAuthorization::None {
            return Err(DaemonError::InvalidConfig(
                "destroy requires explicit confirmation or noninteractive authorization".into(),
            ));
        }
        let environment = self
            .state
            .environment(request.environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let compose = self.state.compose_resource(environment.id)?;
        let device = self.state.ios_device(environment.id)?;
        let active_compose = compose
            .as_ref()
            .is_some_and(|resource| resource.lifecycle == "active");
        if compose
            .as_ref()
            .is_some_and(|resource| resource.lifecycle == "uncertain")
        {
            return Err(DaemonError::OwnershipUncertain);
        }
        let active_device = device
            .as_ref()
            .is_some_and(|resource| resource.lifecycle != "destroyed");
        if device
            .as_ref()
            .is_some_and(|resource| resource.lifecycle == "uncertain")
        {
            return Err(DaemonError::OwnershipUncertain);
        }
        let active_process = environment
            .process
            .as_ref()
            .is_some_and(|process| process.readiness == StoredReadiness::ProcessAlive);
        let mut targets = Vec::new();
        if environment.process.is_some() {
            targets.push(DestroyTarget {
                kind: "process".into(),
                identifier: environment.id.to_string(),
                status: if active_process {
                    DestroyTargetStatus::Planned
                } else {
                    DestroyTargetStatus::Destroyed
                },
            });
        }
        if let Some(resource) = &compose {
            targets.push(DestroyTarget {
                kind: "compose_project".into(),
                identifier: resource.project_name.clone(),
                status: if active_compose {
                    DestroyTargetStatus::Planned
                } else {
                    DestroyTargetStatus::Destroyed
                },
            });
        }
        if let Some(resource) = &device {
            targets.push(DestroyTarget {
                kind: "ios_simulator".into(),
                identifier: resource.udid.clone().unwrap_or_else(|| "unbound".into()),
                status: if active_device {
                    DestroyTargetStatus::Planned
                } else {
                    DestroyTargetStatus::Destroyed
                },
            });
        }
        if request.purge_artifacts {
            targets.push(DestroyTarget {
                kind: "artifacts".into(),
                identifier: environment.id.to_string(),
                status: DestroyTargetStatus::Planned,
            });
        }
        if request.purge_data {
            targets.push(DestroyTarget {
                kind: "recoverable_data".into(),
                identifier: environment.id.to_string(),
                status: DestroyTargetStatus::Planned,
            });
        }
        let mut retained = Vec::new();
        if !request.purge_data {
            retained.push("state metadata, logs, and native build data".into());
        }
        if !request.purge_artifacts {
            retained.push("device evidence artifacts and provenance metadata".into());
        }
        if !request.purge_data {
            retained.push("Compose volumes".into());
        }
        retained.push("Compose images".into());
        if request.dry_run {
            return Ok(DaemonResponse::Destroy(DestroySnapshot {
                environment_id: environment.id,
                dry_run: true,
                targets,
                retained,
                purged: Vec::new(),
            }));
        }

        let process = environment.process.clone();
        if active_compose || active_device {
            let process = process.as_ref().ok_or(DaemonError::OwnershipUncertain)?;
            let observed = supervisor_process(request_supervisor(
                Path::new(&process.supervisor_socket),
                SupervisorRequest::Inspect,
            )?)?;
            if observed.readiness != ReadinessState::ProcessAlive {
                return Err(DaemonError::OwnershipUncertain);
            }
        }

        if let Some(resource) = compose
            .as_ref()
            .filter(|resource| resource.lifecycle == "active")
            && validate_compose_ownership(&environment, resource).is_err()
        {
            self.state.mark_compose_uncertain(environment.id)?;
            self.emit_event(
                environment.id,
                environment.generation,
                None,
                "ownership_uncertain",
            )?;
            return Err(DaemonError::OwnershipUncertain);
        }

        if active_device {
            let device = device.as_ref().ok_or(DaemonError::OwnershipUncertain)?;
            let Some(udid) = device.udid.as_ref() else {
                self.state.set_ios_lifecycle(environment.id, "uncertain")?;
                self.emit_event(
                    environment.id,
                    environment.generation,
                    None,
                    "ownership_uncertain",
                )?;
                return Err(DaemonError::OwnershipUncertain);
            };
            let plugin = self.trusted_plugin("official.ios", "device.discover")?;
            let discovery = Self::ios_discovery(&plugin)?;
            let owned = discovery.devices.iter().any(|candidate| {
                candidate.udid.eq_ignore_ascii_case(udid)
                    && candidate.name == device.expected_name
                    && candidate.runtime == device.runtime
                    && candidate.available
            });
            if !owned {
                self.state.set_ios_lifecycle(environment.id, "uncertain")?;
                self.emit_event(
                    environment.id,
                    environment.generation,
                    None,
                    "ownership_uncertain",
                )?;
                return Err(DaemonError::OwnershipUncertain);
            }
        }

        let mut purged = Vec::new();
        if let Some(resource) = compose.filter(|resource| resource.lifecycle == "active") {
            self.trusted_plugin("official.compose", "compose.destroy")?;
            let process = process.as_ref().ok_or(DaemonError::OwnershipUncertain)?;
            Self::run_destroy_task(
                process,
                "official.compose",
                "compose.destroy",
                serde_json::to_value(ComposeDestroyRequest {
                    worktree: resource.worktree,
                    project_name: resource.project_name,
                    files: resource.files,
                    purge_data: request.purge_data,
                })
                .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?,
            )?;
            self.state.mark_compose_destroyed(environment.id)?;
            mark_destroy_target(
                &mut targets,
                "compose_project",
                DestroyTargetStatus::Destroyed,
            );
            self.emit_event(
                environment.id,
                environment.generation,
                None,
                "compose_destroyed",
            )?;
            if request.purge_data {
                purged.push("Compose volumes".into());
            }
        }

        if let Some(device) = device.filter(|resource| resource.lifecycle != "destroyed") {
            let udid = device.udid.ok_or(DaemonError::OwnershipUncertain)?;
            let process = process.as_ref().ok_or(DaemonError::OwnershipUncertain)?;
            let result = Self::run_destroy_task(
                process,
                "official.ios",
                "device.stop",
                serde_json::json!({"udid": udid}),
            )
            .and_then(|()| {
                Self::run_destroy_task(
                    process,
                    "official.ios",
                    "device.delete",
                    serde_json::json!({"udid": udid}),
                )
            });
            if let Err(error) = result {
                self.state.set_ios_lifecycle(environment.id, "uncertain")?;
                return Err(error);
            }
            self.state.set_ios_lifecycle(environment.id, "destroyed")?;
            mark_destroy_target(
                &mut targets,
                "ios_simulator",
                DestroyTargetStatus::Destroyed,
            );
            self.emit_event(
                environment.id,
                environment.generation,
                None,
                "ios_device_destroyed",
            )?;
        }

        if active_process {
            let process = process.as_ref().ok_or(DaemonError::OwnershipUncertain)?;
            let observed = supervisor_process(request_supervisor(
                Path::new(&process.supervisor_socket),
                SupervisorRequest::Stop,
            )?)?;
            if observed.readiness != ReadinessState::ProcessStopped {
                return Err(DaemonError::Operation(
                    "supervisor did not reap process during destroy".into(),
                ));
            }
            remove_supervisor_socket(&process.supervisor_socket)?;
            mark_destroy_target(&mut targets, "process", DestroyTargetStatus::Destroyed);
        }

        if request.purge_artifacts {
            if remove_owned_state_directory(&self.config.state_dir, "artifacts", environment.id)? {
                purged.push("device evidence artifacts".into());
            }
            self.state.mark_artifacts_obsolete(environment.id)?;
            mark_destroy_target(&mut targets, "artifacts", DestroyTargetStatus::Destroyed);
        }
        if request.purge_data {
            for category in ["logs", "ios-builds"] {
                if remove_owned_state_directory(&self.config.state_dir, category, environment.id)? {
                    purged.push(format!("{category}/{}", environment.id));
                }
            }
            mark_destroy_target(
                &mut targets,
                "recoverable_data",
                DestroyTargetStatus::Destroyed,
            );
        }
        self.release_scheduled_work(&environment)?;
        self.state.mark_destroyed(environment.id)?;
        self.emit_event(environment.id, environment.generation, None, "destroyed")?;
        Ok(DaemonResponse::Destroy(DestroySnapshot {
            environment_id: environment.id,
            dry_run: false,
            targets,
            retained,
            purged,
        }))
    }

    fn run_destroy_task(
        process: &StoredProcess,
        plugin_id: &str,
        capability: &str,
        input: serde_json::Value,
    ) -> Result<(), DaemonError> {
        match request_supervisor(
            Path::new(&process.supervisor_socket),
            SupervisorRequest::RunPluginTask {
                operation_id: Uuid::now_v7(),
                plugin_id: plugin_id.into(),
                action: PluginAction::Action {
                    name: capability.into(),
                    input,
                },
                timeout_seconds: None,
            },
        )? {
            SupervisorResponse::TaskOutput { .. } => Ok(()),
            SupervisorResponse::TaskFailed { stderr, .. } => Err(DaemonError::Operation(format!(
                "{capability} failed: {stderr}"
            ))),
            SupervisorResponse::TaskTimedOut => {
                Err(DaemonError::Operation(format!("{capability} timed out")))
            }
            _ => Err(DaemonError::Operation(format!(
                "supervisor returned an invalid {capability} response"
            ))),
        }
    }

    fn resource_action(
        &mut self,
        request: ResourceActionRequest,
    ) -> Result<DaemonResponse, DaemonError> {
        match request.action {
            ResourceAction::Exec(exec) => {
                self.resource_exec(request.environment_id, &request.resource, exec)
            }
            ResourceAction::Open => self.resource_device_action(
                request.environment_id,
                &request.resource,
                IosSimulatorAction::Open,
                "open",
            ),
            ResourceAction::Stop => self.resource_device_action(
                request.environment_id,
                &request.resource,
                IosSimulatorAction::Stop,
                "stop",
            ),
        }
    }

    fn resource_device_action(
        &mut self,
        environment_id: Uuid,
        resource: &str,
        action: IosSimulatorAction,
        action_name: &str,
    ) -> Result<DaemonResponse, DaemonError> {
        if resource != "ios_simulator" {
            return Err(DaemonError::CapabilityUnsupported(format!(
                "{resource}.{action_name}"
            )));
        }
        let response = self.ios_simulator_action(&IosSimulatorActionRequest {
            environment_id,
            action,
        })?;
        let DaemonResponse::IosSimulator(_) = response else {
            return Err(DaemonError::Operation(
                "simulator action returned an invalid response".into(),
            ));
        };
        Ok(DaemonResponse::ResourceAction(ResourceActionSnapshot {
            environment_id,
            resource: resource.into(),
            action: action_name.into(),
            operation: None,
            stdout: String::new(),
            stderr: String::new(),
        }))
    }

    #[allow(clippy::too_many_lines)]
    fn resource_exec(
        &mut self,
        environment_id: Uuid,
        resource: &str,
        request: devfleet_protocol::ResourceExecRequest,
    ) -> Result<DaemonResponse, DaemonError> {
        if resource != "environment" {
            return Err(DaemonError::CapabilityUnsupported(format!(
                "{resource}.exec"
            )));
        }
        if request.argv.is_empty() || request.timeout_seconds == 0 {
            return Err(DaemonError::InvalidConfig(
                "scoped exec requires argv and a nonzero timeout".into(),
            ));
        }
        if request
            .environment
            .keys()
            .any(|key| !valid_environment_key(key))
        {
            return Err(DaemonError::InvalidConfig(
                "scoped exec environment keys must use shell variable syntax".into(),
            ));
        }
        let environment = self
            .state
            .environment(environment_id)?
            .ok_or(DaemonError::NotFound)?;
        let process = environment
            .process
            .clone()
            .ok_or(DaemonError::OwnershipUncertain)?;
        let worktree = Path::new(&environment.worktree)
            .canonicalize()
            .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        let cwd = Path::new(&request.cwd)
            .canonicalize()
            .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        if !cwd.is_dir() || !cwd.starts_with(&worktree) {
            return Err(DaemonError::InvalidConfig(
                "scoped exec cwd must be an existing directory inside the environment worktree"
                    .into(),
            ));
        }
        let operation_id = Uuid::now_v7();
        self.state
            .record_operation_intent(operation_id, environment_id, "resource.action")?;
        let deadline = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| DaemonError::Operation(error.to_string()))?
            .as_millis()
            .checked_add(u128::from(request.timeout_seconds) * 1_000)
            .ok_or_else(|| {
                DaemonError::InvalidConfig("scoped exec deadline exceeds range".into())
            })?;
        self.state
            .set_operation_context(operation_id, deadline, "environment.exec")?;
        self.emit_event(
            environment_id,
            environment.generation,
            Some(operation_id),
            "resource_action_accepted",
        )?;
        let response = request_supervisor(
            Path::new(&process.supervisor_socket),
            SupervisorRequest::Exec {
                operation_id,
                process: ProcessSpec {
                    argv: request.argv,
                    cwd: cwd.display().to_string(),
                    environment: request.environment,
                },
                timeout_seconds: request.timeout_seconds,
            },
        );
        match response {
            Ok(SupervisorResponse::TaskAccepted) => {}
            Ok(_) => {
                self.state
                    .mark_operation_failed(operation_id, "invalid_response")?;
                return Err(DaemonError::Operation(
                    "supervisor did not accept scoped exec".into(),
                ));
            }
            Err(error) => {
                self.state
                    .mark_operation_failed(operation_id, "supervisor_unavailable")?;
                return Err(error);
            }
        }
        self.monitor_scoped_exec(
            environment_id,
            environment.generation,
            operation_id,
            process.supervisor_socket,
        );
        let operation = self
            .state
            .operation(operation_id)?
            .ok_or(DaemonError::NotFound)?;
        Ok(DaemonResponse::ResourceAction(ResourceActionSnapshot {
            environment_id,
            resource: resource.into(),
            action: "exec".into(),
            operation: Some(operation_snapshot(operation)),
            stdout: String::new(),
            stderr: String::new(),
        }))
    }

    fn monitor_scoped_exec(
        &self,
        environment_id: Uuid,
        generation: u64,
        operation_id: Uuid,
        supervisor_socket: String,
    ) {
        let sender = self.scoped_exec_completions.clone();
        thread::spawn(move || {
            loop {
                let result = request_supervisor(
                    Path::new(&supervisor_socket),
                    SupervisorRequest::ExecStatus { operation_id },
                );
                match result {
                    Ok(SupervisorResponse::TaskAccepted) => thread::sleep(SOCKET_RETRY_INTERVAL),
                    Ok(response) => {
                        let _ = sender.send(ScopedExecCompletion {
                            environment_id,
                            generation,
                            operation_id,
                            result: Ok(response),
                        });
                        return;
                    }
                    Err(error) => {
                        let _ = sender.send(ScopedExecCompletion {
                            environment_id,
                            generation,
                            operation_id,
                            result: Err(error.to_string()),
                        });
                        return;
                    }
                }
            }
        });
    }

    fn drain_scoped_exec_completions(&mut self, receiver: &Receiver<ScopedExecCompletion>) {
        loop {
            match receiver.try_recv() {
                Ok(completion) => {
                    if let Err(error) = self.complete_scoped_exec(completion) {
                        eprintln!("scoped exec completion failed: {error}");
                    }
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
            }
        }
    }

    fn complete_scoped_exec(
        &mut self,
        completion: ScopedExecCompletion,
    ) -> Result<(), DaemonError> {
        let cancelled = self
            .state
            .operation(completion.operation_id)?
            .ok_or(DaemonError::NotFound)?
            .cancellation_requested;
        let resource = scoped_exec_log_resource(completion.operation_id);
        let (stdout, stderr, phase) = match completion.result {
            Ok(SupervisorResponse::TaskOutput { stdout, stderr }) if !cancelled => {
                self.state
                    .mark_operation_completed(completion.operation_id, "completed")?;
                (stdout, stderr, "resource_action_completed")
            }
            Ok(
                SupervisorResponse::TaskOutput { stdout, stderr }
                | SupervisorResponse::TaskFailed { stdout, stderr },
            ) if cancelled => {
                self.state
                    .mark_operation_cancelled(completion.operation_id, "cancelled")?;
                (stdout, stderr, "resource_action_cancelled")
            }
            Ok(SupervisorResponse::TaskFailed { stdout, stderr }) => {
                self.state
                    .mark_operation_failed(completion.operation_id, "failed")?;
                (stdout, stderr, "resource_action_failed")
            }
            Ok(SupervisorResponse::TaskTimedOut) if cancelled => {
                self.state
                    .mark_operation_cancelled(completion.operation_id, "cancelled")?;
                (String::new(), String::new(), "resource_action_cancelled")
            }
            Ok(SupervisorResponse::TaskTimedOut) => {
                self.state
                    .mark_operation_failed(completion.operation_id, "timed_out")?;
                (
                    String::new(),
                    "scoped command exceeded its deadline and its process group was stopped".into(),
                    "resource_action_timed_out",
                )
            }
            Ok(SupervisorResponse::TaskCancelled) => {
                self.state
                    .mark_operation_cancelled(completion.operation_id, "cancelled")?;
                (String::new(), String::new(), "resource_action_cancelled")
            }
            Ok(_) => {
                self.state
                    .mark_operation_failed(completion.operation_id, "invalid_response")?;
                (
                    String::new(),
                    "supervisor returned an invalid scoped exec response".into(),
                    "resource_action_failed",
                )
            }
            Err(error) => {
                self.state
                    .mark_operation_failed(completion.operation_id, "supervisor_unavailable")?;
                (String::new(), error, "resource_action_failed")
            }
        };
        self.append_scoped_exec_log(completion.environment_id, &resource, &stdout, &stderr)?;
        self.emit_event(
            completion.environment_id,
            completion.generation,
            Some(completion.operation_id),
            phase,
        )
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
        if !matches!(
            operation.status,
            devfleet_state::OperationStatus::Accepted
                | devfleet_state::OperationStatus::CancellationRequested
        ) {
            return Ok(DaemonResponse::Operation(operation_snapshot(operation)));
        }
        if operation.kind == "resource.action"
            && operation.resource_id.as_deref() == Some("environment.exec")
        {
            let environment = self
                .state
                .environment(operation.environment_id)?
                .ok_or(DaemonError::NotFound)?;
            let process = environment.process.ok_or(DaemonError::OwnershipUncertain)?;
            match request_supervisor(
                Path::new(&process.supervisor_socket),
                SupervisorRequest::CancelExec { operation_id },
            )? {
                SupervisorResponse::TaskAccepted | SupervisorResponse::TaskCancelled => {}
                _ => {
                    return Err(DaemonError::Operation(
                        "supervisor returned an invalid scoped exec cancellation response".into(),
                    ));
                }
            }
            return self.operation_get(operation_id);
        }
        if operation.kind != "up" {
            return Err(DaemonError::CapabilityUnsupported(
                "operation.cancel for this operation kind".into(),
            ));
        }
        if let Some(environment) = self.state.environment(operation.environment_id)?
            && let Some(process) = environment.process.as_ref()
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
            self.release_scheduled_work(&environment)?;
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
        if resource != "process" && !self.scoped_exec_log_belongs_to(environment_id, resource)? {
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

    fn append_scoped_exec_log(
        &self,
        environment_id: Uuid,
        resource: &str,
        stdout: &str,
        stderr: &str,
    ) -> Result<(), DaemonError> {
        let logs = LogStore::new(
            log_directory(&self.config.state_dir, environment_id),
            MAX_RESOURCE_LOG_BYTES,
            Vec::new(),
        )
        .map_err(|error| DaemonError::Operation(error.to_string()))?;
        for line in stdout.lines().chain(stderr.lines()) {
            logs.append(resource, line)
                .map_err(|error| DaemonError::Operation(error.to_string()))?;
        }
        Ok(())
    }

    fn scoped_exec_log_belongs_to(
        &self,
        environment_id: Uuid,
        resource: &str,
    ) -> Result<bool, DaemonError> {
        let Some(operation_id) = resource
            .strip_prefix("exec-")
            .and_then(|value| Uuid::parse_str(value).ok())
        else {
            return Ok(false);
        };
        Ok(self
            .state
            .operation(operation_id)?
            .is_some_and(|operation| {
                operation.environment_id == environment_id
                    && operation.kind == "resource.action"
                    && operation.resource_id.as_deref() == Some("environment.exec")
            }))
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
                    && matches!(
                        plugin.id.as_str(),
                        "official.compose" | "official.ios" | "official.react-native"
                    )
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
    Scheduler(#[from] SchedulerError),
    #[error(transparent)]
    State(#[from] devfleet_state::StateError),
}

impl From<DaemonError> for RpcFailure {
    fn from(error: DaemonError) -> Self {
        let code = match error {
            DaemonError::TrustRequired | DaemonError::PluginTrustRequired => {
                ErrorCode::PermissionDenied
            }
            DaemonError::NotFound => ErrorCode::ResourceNotFound,
            DaemonError::OwnershipUncertain | DaemonError::OwnershipUncertainPartial(_) => {
                ErrorCode::OwnershipUncertain
            }
            DaemonError::DeviceUnavailable(_) => ErrorCode::DeviceUnavailable,
            DaemonError::CapabilityUnsupported(_) => ErrorCode::CapabilityUnsupported,
            DaemonError::Scheduler(SchedulerError::ResourceBudgetExceeded { .. }) => {
                ErrorCode::ResourceBudgetExceeded
            }
            DaemonError::InvalidConfig(_) | DaemonError::Scheduler(_) => ErrorCode::ConfigInvalid,
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

fn valid_environment_key(key: &str) -> bool {
    let mut characters = key.bytes();
    matches!(characters.next(), Some(byte) if byte.is_ascii_alphabetic() || byte == b'_')
        && characters.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn scoped_exec_log_resource(operation_id: Uuid) -> String {
    format!("exec-{operation_id}")
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

fn inject_compose_endpoint_port_environment(
    environment: &mut BTreeMap<String, String>,
    endpoints: &[devfleet_protocol::ComposeEndpoint],
) -> Result<(), DaemonError> {
    let mut assigned = BTreeSet::new();
    for endpoint in endpoints {
        let key = format!(
            "DEVFLEET_{}_PORT",
            endpoint
                .name
                .bytes()
                .map(|byte| {
                    if byte.is_ascii_alphanumeric() {
                        byte.to_ascii_uppercase() as char
                    } else {
                        '_'
                    }
                })
                .collect::<String>()
        );
        if !assigned.insert(key.clone()) {
            return Err(DaemonError::InvalidConfig(format!(
                "Compose endpoint names resolve to the same managed port variable '{key}'"
            )));
        }
        let value = endpoint.host_port.to_string();
        match environment.get(&key) {
            Some(existing) if existing != &value => {
                return Err(DaemonError::InvalidConfig(format!(
                    "{key} must match DevFleet's resolved endpoint port {value}"
                )));
            }
            Some(_) => {}
            None => {
                environment.insert(key, value);
            }
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

fn wait_for_endpoint_ready(port: u16, readiness_path: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    let endpoint = StoredEndpoint {
        name: "reserved".into(),
        service: "reserved".into(),
        internal_url: String::new(),
        host_url: format!("http://127.0.0.1:{port}"),
        device_url: String::new(),
        readiness_path: readiness_path.into(),
        healthy: false,
    };
    while Instant::now() < deadline {
        if endpoint_is_healthy(&endpoint) {
            return true;
        }
        thread::sleep(Duration::from_millis(50));
    }
    false
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
    native_bindings: &[String],
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
    for binding in native_bindings {
        digest.update(b"binding=");
        digest.update(binding.as_bytes());
    }
    for path in files {
        let relative = path.strip_prefix(worktree).map_err(|_| {
            DaemonError::InvalidConfig("native input escaped its selected worktree".into())
        })?;
        digest.update(relative.to_string_lossy().as_bytes());
        digest.update(fs::read(path).map_err(|error| DaemonError::Operation(error.to_string()))?);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn code_fingerprint(worktree: &Path) -> Result<String, DaemonError> {
    let mut entries = Vec::new();
    collect_code_inputs(worktree, worktree, &mut entries)?;
    let mut digest = Sha256::new();
    for (path, content) in entries {
        let relative = path.strip_prefix(worktree).map_err(|_| {
            DaemonError::InvalidConfig("code input escaped its selected worktree".into())
        })?;
        digest.update(relative.to_string_lossy().as_bytes());
        digest.update(content);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn collect_code_inputs(
    directory: &Path,
    worktree: &Path,
    entries: &mut Vec<(PathBuf, Vec<u8>)>,
) -> Result<(), DaemonError> {
    for entry in
        fs::read_dir(directory).map_err(|error| DaemonError::Operation(error.to_string()))?
    {
        let entry = entry.map_err(|error| DaemonError::Operation(error.to_string()))?;
        let path = entry.path();
        let metadata = path
            .symlink_metadata()
            .map_err(|error| DaemonError::Operation(error.to_string()))?;
        let relative = path.strip_prefix(worktree).map_err(|_| {
            DaemonError::InvalidConfig("code input escaped its selected worktree".into())
        })?;
        if relative.starts_with(".git")
            || relative.starts_with("node_modules")
            || relative.starts_with("ios/Pods")
            || relative.starts_with("ios/build")
        {
            continue;
        }
        if metadata.file_type().is_symlink() {
            entries.push((
                path.clone(),
                std::fs::read_link(path)
                    .map_err(|error| DaemonError::Operation(error.to_string()))?
                    .as_os_str()
                    .as_encoded_bytes()
                    .to_vec(),
            ));
        } else if metadata.is_dir() {
            collect_code_inputs(&path, worktree, entries)?;
        } else if metadata.is_file() {
            entries.push((
                path.clone(),
                fs::read(path).map_err(|error| DaemonError::Operation(error.to_string()))?,
            ));
        }
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(())
}

fn artifact_kind_name(kind: ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::Screenshot => "screenshot",
        ArtifactKind::Recording => "recording",
    }
}

fn artifact_flow(kind: ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::Screenshot => "device.capture",
        ArtifactKind::Recording => "device.record",
    }
}

fn artifact_command(kind: ArtifactKind, udid: &str, path: &Path) -> String {
    match kind {
        ArtifactKind::Screenshot => format!("xcrun simctl io {udid} screenshot {}", path.display()),
        ArtifactKind::Recording => format!(
            "xcrun simctl io {udid} recordVideo --codec h264 {}",
            path.display()
        ),
    }
}

fn remove_generated_artifact(path: &Path) {
    if path
        .symlink_metadata()
        .is_ok_and(|metadata| metadata.file_type().is_file())
    {
        let _ = fs::remove_file(path);
    }
}

fn mark_destroy_target(targets: &mut [DestroyTarget], kind: &str, status: DestroyTargetStatus) {
    if let Some(target) = targets.iter_mut().find(|target| target.kind == kind) {
        target.status = status;
    }
}

fn validate_compose_ownership(
    environment: &StoredEnvironment,
    resource: &StoredComposeResource,
) -> Result<(), DaemonError> {
    if resource.environment_id != environment.id
        || resource.project_name != compose_project_name(environment.project_id, environment.id)
    {
        return Err(DaemonError::OwnershipUncertain);
    }
    let environment_worktree = Path::new(&environment.worktree)
        .canonicalize()
        .map_err(|_| DaemonError::OwnershipUncertain)?;
    let worktree = Path::new(&resource.worktree)
        .canonicalize()
        .map_err(|_| DaemonError::OwnershipUncertain)?;
    if worktree != environment_worktree || resource.files.is_empty() {
        return Err(DaemonError::OwnershipUncertain);
    }
    for file in &resource.files {
        let file = Path::new(file);
        let metadata = file
            .symlink_metadata()
            .map_err(|_| DaemonError::OwnershipUncertain)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(DaemonError::OwnershipUncertain);
        }
        let file = file
            .canonicalize()
            .map_err(|_| DaemonError::OwnershipUncertain)?;
        if !file.starts_with(&worktree) {
            return Err(DaemonError::OwnershipUncertain);
        }
    }
    Ok(())
}

fn remove_owned_state_directory(
    state_dir: &Path,
    category: &str,
    environment_id: Uuid,
) -> Result<bool, DaemonError> {
    let root = state_dir.join(category);
    if !root.exists() {
        return Ok(false);
    }
    let root_metadata = root
        .symlink_metadata()
        .map_err(|error| DaemonError::Operation(error.to_string()))?;
    if !root_metadata.is_dir() || root_metadata.file_type().is_symlink() {
        return Err(DaemonError::OwnershipUncertain);
    }
    let root = root
        .canonicalize()
        .map_err(|error| DaemonError::Operation(error.to_string()))?;
    let target = root.join(environment_id.to_string());
    if !target.exists() {
        return Ok(false);
    }
    let target_metadata = target
        .symlink_metadata()
        .map_err(|error| DaemonError::Operation(error.to_string()))?;
    if !target_metadata.is_dir() || target_metadata.file_type().is_symlink() {
        return Err(DaemonError::OwnershipUncertain);
    }
    validate_owned_tree(&target, &root)?;
    remove_owned_tree(&target, &root)?;
    Ok(true)
}

fn validate_owned_tree(path: &Path, root: &Path) -> Result<(), DaemonError> {
    if !path.starts_with(root) {
        return Err(DaemonError::OwnershipUncertain);
    }
    let metadata = path
        .symlink_metadata()
        .map_err(|error| DaemonError::Operation(error.to_string()))?;
    if metadata.file_type().is_symlink() {
        return Err(DaemonError::OwnershipUncertain);
    }
    if metadata.is_dir() {
        for entry in
            fs::read_dir(path).map_err(|error| DaemonError::Operation(error.to_string()))?
        {
            let entry = entry.map_err(|error| DaemonError::Operation(error.to_string()))?;
            validate_owned_tree(&entry.path(), root)?;
        }
    } else if !metadata.is_file() {
        return Err(DaemonError::OwnershipUncertain);
    }
    Ok(())
}

fn remove_owned_tree(path: &Path, root: &Path) -> Result<(), DaemonError> {
    if !path.starts_with(root) {
        return Err(DaemonError::OwnershipUncertain);
    }
    let metadata = path
        .symlink_metadata()
        .map_err(|error| DaemonError::Operation(error.to_string()))?;
    if metadata.file_type().is_symlink() {
        return Err(DaemonError::OwnershipUncertain);
    }
    if metadata.is_dir() {
        for entry in
            fs::read_dir(path).map_err(|error| DaemonError::Operation(error.to_string()))?
        {
            let entry = entry.map_err(|error| DaemonError::Operation(error.to_string()))?;
            remove_owned_tree(&entry.path(), root)?;
        }
        fs::remove_dir(path).map_err(|error| DaemonError::Operation(error.to_string()))?;
    } else if metadata.is_file() {
        fs::remove_file(path).map_err(|error| DaemonError::Operation(error.to_string()))?;
    } else {
        return Err(DaemonError::OwnershipUncertain);
    }
    Ok(())
}

fn artifact_snapshot(artifact: StoredArtifact) -> ArtifactSnapshot {
    let kind = match artifact.kind.as_str() {
        "recording" => ArtifactKind::Recording,
        _ => ArtifactKind::Screenshot,
    };
    ArtifactSnapshot {
        artifact_id: artifact.id,
        timestamp_unix_ms: artifact.timestamp_unix_ms,
        environment_id: artifact.environment_id,
        generation: artifact.generation,
        worktree: artifact.worktree,
        head: artifact.head,
        code_fingerprint: artifact.code_fingerprint,
        build_identity: artifact.build_identity,
        udid: artifact.udid,
        kind,
        command: artifact.command,
        flow: artifact.flow,
        result: artifact.result,
        app_running: artifact.app_running,
        functional_result: artifact.functional_result,
        human_review: artifact.human_review,
        path: artifact.path,
        obsolete: artifact.obsolete,
    }
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

#[allow(clippy::too_many_lines)]
fn snapshot(environment: StoredEnvironment, inputs: SnapshotInputs) -> EnvironmentSnapshot {
    let SnapshotInputs {
        tasks,
        preparation,
        bindings,
        ios,
        app,
        identity,
        metro_ready,
        scheduler,
    } = inputs;
    let lifecycle = match environment.lifecycle {
        StoredLifecycle::Starting => LifecycleState::Starting,
        StoredLifecycle::Running => LifecycleState::Running,
        StoredLifecycle::Stopped => LifecycleState::Stopped,
        StoredLifecycle::Failed => LifecycleState::Failed,
        StoredLifecycle::Degraded => LifecycleState::Degraded,
        StoredLifecycle::Orphaned => LifecycleState::Orphaned,
        StoredLifecycle::Destroyed => LifecycleState::Destroyed,
    };
    let probe_health = matches!(
        lifecycle,
        LifecycleState::Starting | LifecycleState::Running
    );
    let endpoints = environment
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
        .collect::<Vec<_>>();
    let backend_healthy = endpoints
        .iter()
        .any(|endpoint| endpoint.name != "metro" && endpoint.healthy);
    let device_booted = ios
        .as_ref()
        .is_some_and(|device| device.lifecycle == "booted");
    let app_launched = app.as_ref().is_some_and(|app| app.launched);
    let verification = identity
        .as_ref()
        .map_or(devfleet_protocol::VerificationMode::Transport, |identity| {
            verification_mode(&identity.mode)
        });
    let identity_verified = identity.as_ref().is_some_and(|identity| {
        identity.generation == environment.generation && identity.status == "verified"
    });
    let reconciliation_action = (lifecycle == LifecycleState::Degraded)
        .then(|| format!("devfleet reconcile {}", environment.id));
    let operational = backend_healthy && metro_ready && device_booted && app_launched;
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
        reconciliation_action,
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
        endpoints,
        readiness: EnvironmentReadiness {
            backend_healthy,
            metro_ready,
            device_booted,
            app_launched,
            identity_verified,
            verification,
            operational,
            verified_ready: operational && identity_verified,
        },
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
        scheduler,
    }
}

fn verification_mode(value: &str) -> devfleet_protocol::VerificationMode {
    match value {
        "app_handshake" => devfleet_protocol::VerificationMode::AppHandshake,
        _ => devfleet_protocol::VerificationMode::Transport,
    }
}

fn ios_snapshot(device: StoredIosDevice) -> Result<IosSimulatorSnapshot, DaemonError> {
    let udid = device.udid.ok_or(DaemonError::OwnershipUncertain)?;
    let lifecycle = match device.lifecycle.as_str() {
        "reserved" => IosSimulatorLifecycle::Reserved,
        "booted" => IosSimulatorLifecycle::Booted,
        "stopped" => IosSimulatorLifecycle::Stopped,
        "uncertain" => IosSimulatorLifecycle::Uncertain,
        "destroyed" => IosSimulatorLifecycle::Destroyed,
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

impl Daemon {
    fn reserve_automatic_ports(
        &mut self,
        environment_key: &str,
        project_fingerprint: &str,
        generation: u64,
        names: BTreeSet<String>,
    ) -> Result<BTreeMap<String, u16>, DaemonError> {
        if names.len() > usize::from(DEFAULT_BLOCK_SIZE) {
            return Err(DaemonError::InvalidConfig(format!(
                "at most {DEFAULT_BLOCK_SIZE} automatic endpoints can share one environment"
            )));
        }
        let offsets = names
            .iter()
            .enumerate()
            .map(|(offset, name)| {
                u16::try_from(offset)
                    .map(|offset| (name.clone(), offset))
                    .map_err(|_| {
                        DaemonError::InvalidConfig("automatic port offset overflow".into())
                    })
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let lease = self.state.reserve_port_lease(&PortLeaseRequest {
            environment_key: environment_key.into(),
            project_fingerprint: project_fingerprint.into(),
            generation,
            range_start: DEFAULT_PORT_RANGE.start,
            range_end: DEFAULT_PORT_RANGE.end,
            block_size: DEFAULT_BLOCK_SIZE,
            offsets,
        })?;
        let assigned = PortBlock {
            start: lease.block_start,
            size: lease.block_size,
            offsets: lease.offsets,
        }
        .ports(DEFAULT_PORT_RANGE)
        .map_err(|error| DaemonError::InvalidConfig(error.to_string()))?;
        names
            .into_iter()
            .map(|name| {
                let port = assigned.get(&name).copied().ok_or_else(|| {
                    DaemonError::InvalidConfig(format!(
                        "retained automatic port lease has no assignment for '{name}'"
                    ))
                })?;
                Ok((name, port))
            })
            .collect()
    }
}

fn unix_now_ms() -> Result<u64, DaemonError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| DaemonError::Operation(error.to_string()))?
        .as_millis()
        .try_into()
        .map_err(|_| DaemonError::Operation("clock exceeds supported range".into()))
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
            devfleet_state::OperationStatus::Uncertain => {
                devfleet_protocol::OperationStatus::Uncertain
            }
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
        | SupervisorResponse::TaskAccepted
        | SupervisorResponse::TaskCancelled
        | SupervisorResponse::TaskFailed { .. }
        | SupervisorResponse::TaskTimedOut
        | SupervisorResponse::TaskOutput { .. }
        | SupervisorResponse::IdentityVerification(_) => Err(DaemonError::Operation(
            "supervisor returned a task response for process control".into(),
        )),
    }
}

fn identity_verification_response(
    response: SupervisorResponse,
) -> Result<IdentityVerificationSnapshot, DaemonError> {
    match response {
        SupervisorResponse::IdentityVerification(snapshot) => Ok(snapshot),
        _ => Err(DaemonError::Operation(
            "supervisor returned an invalid identity verification response".into(),
        )),
    }
}

fn identity_launch_config(
    request: &IosAppRunRequest,
) -> Result<Option<IdentityLaunchConfig>, DaemonError> {
    if request.verification == VerificationMode::Transport {
        if request.backend_identity.is_some()
            || request.backend_identity_url.is_some()
            || request.identity_hook_module.is_some()
        {
            return Err(DaemonError::InvalidConfig(
                "transport verification may not receive app-handshake inputs".into(),
            ));
        }
        return Ok(None);
    }
    let backend_identity =
        required_identity_input("backend identity", request.backend_identity.as_ref())?;
    let backend_identity_url = required_identity_input(
        "backend identity URL",
        request.backend_identity_url.as_ref(),
    )?;
    let identity_hook_module = required_identity_input(
        "identity hook module",
        request.identity_hook_module.as_ref(),
    )?;
    if !(backend_identity_url.starts_with("http://")
        || backend_identity_url.starts_with("https://"))
    {
        return Err(DaemonError::InvalidConfig(
            "backend identity URL must use HTTP(S)".into(),
        ));
    }
    Ok(Some(IdentityLaunchConfig {
        backend_identity,
        backend_identity_url,
        identity_hook_module,
    }))
}

fn required_identity_input(name: &str, value: Option<&String>) -> Result<String, DaemonError> {
    value
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| {
            DaemonError::InvalidConfig(format!("app-handshake verification requires {name}"))
        })
}

fn react_native_identity_hook(
    snapshot: &IdentityVerificationSnapshot,
    config: &IdentityLaunchConfig,
) -> ReactNativeIdentityHook {
    ReactNativeIdentityHook {
        callback_url: snapshot.verification_url.clone(),
        authorization_token: snapshot.token.clone(),
        nonce: snapshot.nonce.clone(),
        environment_id: snapshot.environment_id,
        generation: snapshot.generation,
        build_identity: snapshot.build_identity.clone(),
        bundle_identity: snapshot.bundle_identity.clone(),
        backend_identity_url: config.backend_identity_url.clone(),
        expected_backend_identity: config.backend_identity.clone(),
        module: config.identity_hook_module.clone(),
    }
}

fn identity_mode_name(mode: VerificationMode) -> &'static str {
    match mode {
        VerificationMode::Transport => "transport",
        VerificationMode::AppHandshake => "app_handshake",
    }
}

fn identity_status_name(status: devfleet_protocol::IdentityVerificationStatus) -> &'static str {
    match status {
        devfleet_protocol::IdentityVerificationStatus::Pending => "pending",
        devfleet_protocol::IdentityVerificationStatus::Verified => "verified",
        devfleet_protocol::IdentityVerificationStatus::Mismatch => "mismatch",
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

fn proposed_manifest() -> String {
    r#"schema_version = 1

[profiles.process]
resources = ["app"]

[resources.app]
provider = "official.process"
"#
    .into()
}

fn proposed_integrations() -> Vec<String> {
    vec![
        "Add the proposed devfleet.toml only after review; init never writes tracked configuration."
            .into(),
        "Declare project-owned resources and explicit trusted provider IDs in the reviewed manifest."
            .into(),
        "Add a ReactNativeIdentityHook module only when app-handshake verification is selected."
            .into(),
        "Choose explicitly whether generated .devfleet integration artifacts belong in .gitignore."
            .into(),
    ]
}

fn doctor_finding(
    phase: DoctorPhase,
    resource: impl Into<String>,
    cause: impl Into<String>,
    retryable: bool,
    remediation: impl Into<String>,
) -> DoctorFinding {
    DoctorFinding {
        phase,
        resource: resource.into(),
        cause: cause.into(),
        retryable,
        remediation: remediation.into(),
        blocking: true,
    }
}

fn ios_selection_available(selections: &[IosAvailableSelection], selected: &str) -> bool {
    selections
        .iter()
        .any(|candidate| candidate.identifier == selected && candidate.available)
}

fn tool_remediation(resource: &str) -> &'static str {
    match resource {
        "node" => "Install a supported Node.js runtime and retry doctor.",
        "react_native" => "Install the project's dependencies, then rerun doctor.",
        "xcodebuild" => "Install or select the supported Xcode toolchain and retry doctor.",
        _ => "Repair the required tool and retry doctor.",
    }
}

fn plugin_action_name(action: &PluginAction) -> &str {
    match action {
        PluginAction::Initialize => "lifecycle.initialize",
        PluginAction::Validate { .. } => "lifecycle.validate",
        PluginAction::Plan { .. } => "lifecycle.plan",
        PluginAction::Inspect => "lifecycle.inspect",
        PluginAction::Reconcile(_) => "lifecycle.reconcile",
        PluginAction::ComposeReconcile(_) => "compose.reconcile",
        PluginAction::Stop => "lifecycle.stop",
        PluginAction::Destroy => "lifecycle.destroy",
        PluginAction::Action { name, .. } => name,
        PluginAction::Cancel => "lifecycle.cancel",
    }
}

fn wait_for_plugin_response(
    mut child: Child,
    mut stdout: std::process::ChildStdout,
    timeout: Option<Duration>,
) -> Result<(std::process::ExitStatus, Vec<u8>), DaemonError> {
    let started = Instant::now();
    let mut output = Vec::new();
    let mut child_status = None;
    let mut stdout_closed = false;
    loop {
        if let Some(timeout) = timeout
            && started.elapsed() >= timeout
        {
            terminate_plugin_child(&mut child);
            return Err(DaemonError::Operation(format!(
                "plugin call exceeded the {} second doctor deadline",
                timeout.as_secs()
            )));
        }

        if stdout_closed {
            thread::sleep(Duration::from_millis(1));
        } else {
            let remaining = timeout.map_or(Duration::from_millis(10), |timeout| {
                timeout.saturating_sub(started.elapsed())
            });
            let poll_timeout = u16::try_from(remaining.as_millis().min(10)).unwrap_or(10);
            let mut fds = [PollFd::new(
                stdout.as_fd(),
                PollFlags::POLLIN | PollFlags::POLLHUP,
            )];
            if let Err(error) = poll(&mut fds, poll_timeout) {
                terminate_plugin_child(&mut child);
                return Err(DaemonError::Operation(error.to_string()));
            }
            let events = fds[0].revents().unwrap_or_else(PollFlags::empty);
            if events.intersects(PollFlags::POLLIN | PollFlags::POLLHUP) {
                let mut buffer = [0_u8; 8192];
                match stdout.read(&mut buffer) {
                    Ok(0) => stdout_closed = true,
                    Ok(read) => {
                        output.extend_from_slice(&buffer[..read]);
                        if output.len() > devfleet_protocol::MAX_FRAME_BYTES {
                            terminate_plugin_child(&mut child);
                            return Err(DaemonError::Operation(
                                "plugin response exceeded the protocol frame limit".into(),
                            ));
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(error) => {
                        terminate_plugin_child(&mut child);
                        return Err(DaemonError::Operation(error.to_string()));
                    }
                }
            } else if events.contains(PollFlags::POLLERR) {
                terminate_plugin_child(&mut child);
                return Err(DaemonError::Operation("plugin stdout poll failed".into()));
            }
        }

        if child_status.is_none() {
            child_status = child
                .try_wait()
                .map_err(|error| DaemonError::Operation(error.to_string()))?;
        }
        if let Some(status) = child_status
            && stdout_closed
        {
            return Ok((status, output));
        }
    }
}

fn terminate_plugin_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, net::TcpListener, os::unix::net::UnixListener};
    use tempfile::TempDir;

    #[test]
    fn bounded_plugin_wait_kills_an_unresponsive_provider() {
        let mut child = Command::new("/bin/sleep")
            .arg("1")
            .stdout(Stdio::piped())
            .spawn()
            .expect("sleep starts");
        let stdout = child.stdout.take().expect("sleep stdout");
        let error = wait_for_plugin_response(child, stdout, Some(Duration::from_millis(10)))
            .expect_err("unresponsive provider times out");
        assert!(error.to_string().contains("doctor deadline"));
    }

    #[test]
    fn plugin_stdout_reader_drains_a_response_larger_than_a_pipe() {
        let mut child = Command::new("/bin/dd")
            .args(["if=/dev/zero", "bs=131072", "count=1"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("large provider response starts");
        let stdout = child.stdout.take().expect("provider stdout");
        let (status, stdout) =
            wait_for_plugin_response(child, stdout, Some(Duration::from_secs(1)))
                .expect("provider completes while stdout is drained");
        assert!(status.success());
        assert_eq!(stdout.len(), 131_072);
    }

    #[test]
    fn descendant_stdout_cannot_extend_the_doctor_deadline() {
        let mut child = Command::new("/bin/sh")
            .args(["-c", "sleep 1 & exit 0"])
            .stdout(Stdio::piped())
            .spawn()
            .expect("provider starts descendant");
        let stdout = child.stdout.take().expect("provider stdout");
        let error = wait_for_plugin_response(child, stdout, Some(Duration::from_millis(10)))
            .expect_err("inherited stdout times out");
        assert!(error.to_string().contains("doctor deadline"));
    }

    #[test]
    fn compose_destroy_rejects_project_or_symlink_boundary_changes() {
        let temporary = TempDir::new().expect("temporary directory");
        let compose = temporary.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("Compose fixture");
        let environment = destroy_environment(temporary.path());
        let resource = StoredComposeResource {
            environment_id: environment.id,
            project_name: compose_project_name(environment.project_id, environment.id),
            worktree: environment.worktree.clone(),
            files: vec![compose.display().to_string()],
            lifecycle: "active".into(),
        };
        validate_compose_ownership(&environment, &resource).expect("owned Compose validates");
        let external = TempDir::new().expect("external directory");
        let external_file = external.path().join("compose.yaml");
        fs::write(&external_file, "services: {}\n").expect("external fixture");
        fs::remove_file(&compose).expect("replace Compose fixture");
        std::os::unix::fs::symlink(&external_file, &compose).expect("malicious link");
        assert!(matches!(
            validate_compose_ownership(&environment, &resource),
            Err(DaemonError::OwnershipUncertain)
        ));
    }

    #[test]
    fn repeated_compose_up_reuses_the_durable_operation_identity() {
        let temporary = TempDir::new().expect("temporary directory");
        initialize_git_repository(temporary.path());
        let compose = temporary.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("Compose fixture");
        let identity = discover_git_worktree(temporary.path()).expect("Git identity");
        let environment = StoredEnvironment {
            id: Uuid::now_v7(),
            key: environment_key(
                &identity.project_fingerprint,
                &identity.worktree_fingerprint,
                "compose",
                None,
            ),
            project_id: Uuid::now_v7(),
            worktree_id: Uuid::now_v7(),
            project_fingerprint: identity.project_fingerprint,
            worktree_fingerprint: identity.worktree_fingerprint,
            common_git_dir: identity.common_git_dir.display().to_string(),
            git_dir: identity.git_dir.display().to_string(),
            worktree: temporary.path().display().to_string(),
            profile: "compose".into(),
            instance: None,
            head: identity.head,
            branch: identity.branch,
            detached: identity.detached,
            generation: 1,
            lifecycle: StoredLifecycle::Running,
            reconciliation_diagnostic: None,
            process: None,
            endpoints: Vec::new(),
        };
        let operation_id = Uuid::now_v7();
        let mut state = StateStore::open_in_memory().expect("state opens");
        state
            .record_start_intent(&environment, operation_id)
            .expect("accepted Compose persists");
        let (scoped_exec_completions, _) = mpsc::channel();
        let mut daemon = Daemon {
            config: DaemonConfig {
                state_dir: temporary.path().join("state"),
                socket: temporary.path().join("daemon.sock"),
                supervisor: temporary.path().join("supervisor"),
                process_plugin: temporary.path().join("process-plugin"),
            },
            state,
            scheduler: HostScheduler::default(),
            scoped_exec_completions,
        };
        let response = daemon
            .compose_up(ComposeUpRequest {
                worktree: temporary.path().display().to_string(),
                profile: "compose".into(),
                instance: None,
                trusted: true,
                files: vec![compose.display().to_string()],
                environment: BTreeMap::new(),
                endpoints: Vec::new(),
                authorizations: devfleet_protocol::ComposeAuthorizations {
                    global_container_names: None,
                    host_networking: None,
                    external_resources: None,
                    global_volume_names: None,
                    unmanaged_ports: None,
                    shared_mutable_binds: None,
                },
            })
            .expect("repeated Compose up");
        let DaemonResponse::Accepted(accepted) = response else {
            panic!("expected retained accepted operation");
        };
        assert_eq!(accepted.environment_id, environment.id);
        assert_eq!(accepted.operation_id, operation_id);
    }

    #[test]
    fn automatic_ports_are_reserved_by_environment_and_reused_after_stop() {
        let temporary = TempDir::new().expect("temporary directory");
        let (scoped_exec_completions, _) = mpsc::channel();
        let mut daemon = Daemon {
            config: DaemonConfig {
                state_dir: temporary.path().join("state"),
                socket: temporary.path().join("daemon.sock"),
                supervisor: temporary.path().join("supervisor"),
                process_plugin: temporary.path().join("process-plugin"),
            },
            state: StateStore::open_in_memory().expect("state opens"),
            scheduler: HostScheduler::default(),
            scoped_exec_completions,
        };
        let first = daemon
            .reserve_automatic_ports(
                "project|worktree|compose|one",
                "project",
                1,
                BTreeSet::from(["api".into(), "metro".into()]),
            )
            .expect("first reservation");
        assert_eq!(first["api"], 20_000);
        assert_eq!(first["metro"], 20_001);
        assert!(
            daemon
                .state
                .mark_port_lease_stopped("project|worktree|compose|one")
                .expect("retains stopped lease")
        );
        let retained = daemon
            .reserve_automatic_ports(
                "project|worktree|compose|one",
                "project",
                1,
                BTreeSet::from(["api".into(), "metro".into()]),
            )
            .expect("retained reservation");
        assert_eq!(retained, first);
        let second = daemon
            .reserve_automatic_ports(
                "project|worktree|compose|two",
                "project",
                1,
                BTreeSet::from(["api".into()]),
            )
            .expect("second reservation");
        assert_eq!(second["api"], 20_064);
    }

    #[test]
    fn compose_receives_resolved_endpoint_port_variables() {
        let endpoints = vec![
            devfleet_protocol::ComposeEndpoint {
                name: "api".into(),
                service: "api".into(),
                container_port: 3_000,
                host_port: 20_000,
                readiness_path: "/health".into(),
            },
            devfleet_protocol::ComposeEndpoint {
                name: "web-api".into(),
                service: "web".into(),
                container_port: 3_001,
                host_port: 20_001,
                readiness_path: "/ready".into(),
            },
        ];
        let mut environment = BTreeMap::new();
        inject_compose_endpoint_port_environment(&mut environment, &endpoints)
            .expect("resolved ports inject");
        assert_eq!(environment["DEVFLEET_API_PORT"], "20000");
        assert_eq!(environment["DEVFLEET_WEB_API_PORT"], "20001");
        environment.insert("DEVFLEET_API_PORT".into(), "20002".into());
        assert!(inject_compose_endpoint_port_environment(&mut environment, &endpoints).is_err());
    }

    fn destroy_environment(worktree: &Path) -> StoredEnvironment {
        StoredEnvironment {
            id: Uuid::now_v7(),
            key: "destroy-fixture".into(),
            project_id: Uuid::now_v7(),
            worktree_id: Uuid::now_v7(),
            project_fingerprint: "project".into(),
            worktree_fingerprint: "worktree".into(),
            common_git_dir: String::new(),
            git_dir: String::new(),
            worktree: worktree.display().to_string(),
            profile: "compose".into(),
            instance: None,
            head: String::new(),
            branch: None,
            detached: false,
            generation: 1,
            lifecycle: StoredLifecycle::Running,
            reconciliation_diagnostic: None,
            process: None,
            endpoints: Vec::new(),
        }
    }

    fn initialize_git_repository(path: &Path) {
        for arguments in [
            vec!["init"],
            vec!["config", "user.email", "devfleet-test@example.invalid"],
            vec!["config", "user.name", "DevFleet Test"],
        ] {
            let output = Command::new("git")
                .args(arguments)
                .current_dir(path)
                .output()
                .expect("Git starts");
            assert!(
                output.status.success(),
                "Git failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        fs::write(path.join("README.md"), "fixture\n").expect("fixture writes");
        for arguments in [vec!["add", "README.md"], vec!["commit", "-m", "fixture"]] {
            let output = Command::new("git")
                .args(arguments)
                .current_dir(path)
                .output()
                .expect("Git starts");
            assert!(
                output.status.success(),
                "Git failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

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
    fn native_build_fingerprint_ignores_js_and_rejects_stale_native_outputs() {
        let temporary = TempDir::new().expect("temporary directory");
        let ios = temporary.path().join("ios");
        fs::create_dir_all(&ios).expect("iOS directory");
        fs::write(ios.join("AppDelegate.mm"), "native-a").expect("native input");
        fs::write(temporary.path().join("package-lock.json"), "lock-a").expect("lockfile");
        let first = native_build_fingerprint(temporary.path(), 21_001, None, &["config-a".into()])
            .expect("first fingerprint");
        fs::write(temporary.path().join("index.js"), "javascript-only").expect("JS input");
        assert_eq!(
            first,
            native_build_fingerprint(temporary.path(), 21_001, None, &["config-a".into()])
                .expect("JS does not rebuild native")
        );
        fs::write(ios.join("AppDelegate.mm"), "native-b").expect("dirty native input");
        assert_ne!(
            first,
            native_build_fingerprint(temporary.path(), 21_001, None, &["config-a".into()])
                .expect("native change rebuilds")
        );
    }

    #[test]
    fn code_fingerprint_marks_javascript_edits_as_new_evidence() {
        let temporary = TempDir::new().expect("temporary directory");
        fs::write(temporary.path().join("index.js"), "first").expect("JS fixture");
        let first = code_fingerprint(temporary.path()).expect("first fingerprint");
        fs::write(temporary.path().join("index.js"), "second").expect("JS fixture");
        assert_ne!(
            first,
            code_fingerprint(temporary.path()).expect("changed fingerprint")
        );
    }

    #[test]
    fn handshake_requires_an_explicit_hook_and_backend_identity_route() {
        let transport = IosAppRunRequest {
            environment_id: Uuid::now_v7(),
            bundle_id: "com.example.app".into(),
            scheme: None,
            verification: VerificationMode::Transport,
            backend_identity: None,
            backend_identity_url: None,
            identity_hook_module: None,
        };
        assert!(
            identity_launch_config(&transport)
                .expect("transport config")
                .is_none()
        );
        let missing_hook = IosAppRunRequest {
            verification: VerificationMode::AppHandshake,
            backend_identity: Some("backend-a".into()),
            backend_identity_url: Some("http://127.0.0.1:23001/devfleet-identity".into()),
            ..transport
        };
        assert!(identity_launch_config(&missing_hook).is_err());
    }

    #[test]
    fn validates_only_an_installed_available_pinned_ios_selection() {
        let selections = vec![
            IosAvailableSelection {
                identifier: "runtime-18".into(),
                available: true,
            },
            IosAvailableSelection {
                identifier: "runtime-19".into(),
                available: false,
            },
        ];
        assert!(ios_selection_available(&selections, "runtime-18"));
        assert!(!ios_selection_available(&selections, "runtime-19"));
        assert!(!ios_selection_available(&selections, "runtime-missing"));
    }

    #[test]
    fn removes_a_refused_supervisor_socket_before_restart() {
        let temporary = TempDir::new().expect("temporary directory");
        let socket = temporary.path().join("supervisor.sock");
        drop(UnixListener::bind(&socket).expect("supervisor socket"));
        for _ in 0..20 {
            if std::os::unix::net::UnixStream::connect(&socket).is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
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
