//! Local daemon: private IPC, persistent state, and supervisor composition.

use devfleet_core::discover_git_worktree;
use devfleet_protocol::{
    DaemonRequest, DaemonResponse, EnvironmentSnapshot, ErrorCode, JsonRpcRequest, JsonRpcResponse,
    LifecycleState, OperationAccepted, PROTOCOL_VERSION, PluginSnapshot, ProcessSnapshot,
    ReadinessState, RpcFailure, SupervisorRequest, SupervisorResponse, SupervisorStartRequest,
    read_json_line, write_json_line,
};
use devfleet_state::{
    StateStore, StoredEnvironment, StoredLifecycle, StoredProcess, StoredReadiness,
    VerifiedGitContext,
};
use fs2::FileExt;
use nix::unistd::{getpeereid, getuid};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        net::UnixListener,
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use thiserror::Error;
use uuid::Uuid;

const SOCKET_STARTUP_TIMEOUT: Duration = Duration::from_secs(3);
const SOCKET_RETRY_INTERVAL: Duration = Duration::from_millis(25);
const MAX_SOCKET_PATH_BYTES: usize = 96;

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
    if config.socket.exists() {
        anyhow::bail!("daemon socket already exists; another daemon may own it");
    }
    let listener = UnixListener::bind(&config.socket)?;
    fs::set_permissions(&config.socket, fs::Permissions::from_mode(0o600))?;
    let state = StateStore::open(&config.state_dir.join("state.sqlite3"))?;
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
            DaemonRequest::Up(request) => self.up(request),
            DaemonRequest::Status { environment_id } => self.status(environment_id),
            DaemonRequest::List => self.list(),
            DaemonRequest::Stop { environment_id } => self.stop(environment_id),
            DaemonRequest::PluginList => self.plugin_list(),
            DaemonRequest::PluginInstall { path } => self.plugin_install(Path::new(&path)),
            DaemonRequest::PluginTrust { id } => self.plugin_trust(&id),
        }
    }

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
        if let Some(environment) = matching_worktree.filter(|environment| {
            environment.profile == request.profile && environment.instance == request.instance
        }) {
            self.state
                .update_worktree_context(environment.id, &git_context)?;
            return Ok(DaemonResponse::Accepted(OperationAccepted {
                operation_id: Uuid::nil(),
                environment_id: environment.id,
            }));
        }

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
            id: Uuid::now_v7(),
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
        };
        let operation_id = Uuid::now_v7();
        self.state.record_start_intent(&environment, operation_id)?;
        let control_socket = supervisor_socket_path(environment.id)?;
        let start = SupervisorStartRequest {
            environment_id: environment.id,
            operation_id,
            process: request.process,
        };
        if let Err(error) = self.start_supervisor(&control_socket, &process_plugin, &start) {
            self.state.mark_failed(environment.id)?;
            return Err(error);
        }
        let response = request_supervisor(&control_socket, SupervisorRequest::Inspect)?;
        let SupervisorResponse::Process(process) = response;
        self.state
            .record_running_process(environment.id, &stored_process(&process))?;
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
        self.status(environment_id)
    }

    fn start_supervisor(
        &self,
        control_socket: &Path,
        process_plugin: &Path,
        request: &SupervisorStartRequest,
    ) -> Result<(), DaemonError> {
        if control_socket.exists() {
            return Err(DaemonError::OwnershipUncertain);
        }
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
        let plugin = self
            .state
            .plugin("official.process")?
            .ok_or(DaemonError::PluginTrustRequired)?;
        if !plugin.trusted {
            return Err(DaemonError::PluginTrustRequired);
        }
        self.verify_plugin_lock(&plugin)?;
        let inspected = inspect_plugin(Path::new(&plugin.path))?;
        if !same_plugin_metadata(&plugin, &inspected)
            || !inspected
                .capabilities
                .iter()
                .any(|item| item == "process.reconcile")
        {
            return Err(DaemonError::InvalidConfig(
                "installed official.process descriptor or checksum no longer matches its lockfile"
                    .into(),
            ));
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

fn snapshot(environment: StoredEnvironment) -> EnvironmentSnapshot {
    EnvironmentSnapshot {
        environment_id: environment.id,
        project_id: environment.project_id,
        worktree_id: environment.worktree_id,
        worktree: environment.worktree,
        profile: environment.profile,
        instance: environment.instance,
        generation: environment.generation,
        lifecycle: match environment.lifecycle {
            StoredLifecycle::Starting => LifecycleState::Starting,
            StoredLifecycle::Running => LifecycleState::Running,
            StoredLifecycle::Stopped => LifecycleState::Stopped,
            StoredLifecycle::Failed => LifecycleState::Failed,
            StoredLifecycle::Orphaned => LifecycleState::Orphaned,
        },
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
    }
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
    let mut stream = std::os::unix::net::UnixStream::connect(socket)
        .map_err(|error| DaemonError::Operation(error.to_string()))?;
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
