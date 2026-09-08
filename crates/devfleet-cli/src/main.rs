use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use devfleet_cli::request;
use devfleet_protocol::{
    BindingApplyRequest, ComposeAuthorizations, ComposeEndpoint, ComposeUpRequest, DaemonRequest,
    DaemonResponse, DestroyAuthorization, DestroyRequest, DeviceCaptureRequest, DoctorRequest,
    EnvironmentPlanRequest, EnvironmentUpRequest, IdentityVerificationStartRequest,
    InitializationRunRequest, InitializationTask, IosAppRunRequest, IosSimulatorAcquireRequest,
    IosSimulatorAction, IosSimulatorActionRequest, MetroUpRequest,
    PREPARATION_DEFAULT_TIMEOUT_SECONDS, PROTOCOL_VERSION, PreparationRunRequest, ProcessSpec,
    ProjectInitRequest, RESOURCE_EXEC_DEFAULT_TIMEOUT_SECONDS, ResourceAction,
    ResourceActionRequest, ResourceExecRequest, SchedulerConfig, SmokeResponse, SmokeStatus,
    VerificationMode,
};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};
use uuid::Uuid;

#[derive(Parser)]
#[command(about = "DevFleet control client")]
struct Args {
    /// Verify the executable boundary without connecting to a daemon.
    #[arg(long)]
    smoke: bool,
    /// Render the daemon response as JSON.
    #[arg(long, global = true)]
    json: bool,
    /// Private daemon socket path.
    #[arg(long, global = true)]
    socket: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Init(InitArgs),
    Doctor(DoctorArgs),
    Plan(PlanArgs),
    Up(UpArgs),
    Status { environment_id: Uuid },
    List,
    Stop { environment_id: Uuid },
    Destroy(DestroyArgs),
    Reconcile { environment_id: Uuid },
    Scheduler(SchedulerArgs),
    Operation(OperationArgs),
    Events(EventsArgs),
    Logs(LogsArgs),
    Compose(ComposeArgs),
    Metro(MetroArgs),
    Initialize(InitializeArgs),
    Prepare(PrepareArgs),
    Bindings(BindingsArgs),
    Ios(IosArgs),
    Resource(ResourceArgs),
    Identity(IdentityArgs),
    Plugin(PluginArgs),
}

#[derive(ClapArgs)]
struct InitArgs {
    #[arg(long)]
    trust: bool,
    #[arg(long)]
    worktree: PathBuf,
    #[arg(long)]
    runtime: Option<String>,
    #[arg(long = "device-type")]
    device_type: Option<String>,
}

#[derive(ClapArgs)]
struct DoctorArgs {
    #[arg(long)]
    worktree: PathBuf,
    #[arg(long)]
    environment_id: Option<Uuid>,
}

#[derive(ClapArgs)]
struct BindingsArgs {
    #[command(subcommand)]
    command: BindingsCommand,
}
#[derive(Subcommand)]
enum BindingsCommand {
    Apply {
        #[arg(long)]
        file: PathBuf,
    },
}

#[derive(ClapArgs)]
struct IosArgs {
    #[command(subcommand)]
    command: IosCommand,
}

#[derive(ClapArgs)]
struct IdentityArgs {
    #[command(subcommand)]
    command: IdentityCommand,
}

#[derive(Subcommand)]
enum IdentityCommand {
    Start {
        environment_id: Uuid,
        #[arg(long)]
        backend_identity: String,
        #[arg(long)]
        backend_identity_url: String,
    },
    Status {
        environment_id: Uuid,
    },
}

#[derive(Subcommand)]
enum IosCommand {
    Capture {
        environment_id: Uuid,
    },
    Record {
        environment_id: Uuid,
        #[arg(long, default_value_t = 15)]
        seconds: u64,
    },
    Run {
        environment_id: Uuid,
        #[arg(long)]
        bundle_id: String,
        #[arg(long)]
        scheme: Option<String>,
        #[arg(long, value_enum, default_value_t = VerificationModeArg::Transport)]
        verification: VerificationModeArg,
        #[arg(long)]
        backend_identity: Option<String>,
        #[arg(long)]
        backend_identity_url: Option<String>,
        #[arg(long)]
        identity_hook_module: Option<String>,
    },
    Acquire {
        environment_id: Uuid,
        #[arg(long)]
        runtime: String,
        #[arg(long = "device-type")]
        device_type: String,
    },
    Open {
        environment_id: Uuid,
    },
    Stop {
        environment_id: Uuid,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum VerificationModeArg {
    Transport,
    AppHandshake,
}

impl From<VerificationModeArg> for VerificationMode {
    fn from(value: VerificationModeArg) -> Self {
        match value {
            VerificationModeArg::Transport => Self::Transport,
            VerificationModeArg::AppHandshake => Self::AppHandshake,
        }
    }
}

#[derive(ClapArgs)]
struct InitializeArgs {
    #[command(subcommand)]
    command: InitializeCommand,
}

#[derive(ClapArgs)]
struct PrepareArgs {
    #[command(subcommand)]
    command: PrepareCommand,
}

#[derive(Subcommand)]
enum PrepareCommand {
    Run {
        environment_id: Uuid,
        #[arg(long, default_value_t = PREPARATION_DEFAULT_TIMEOUT_SECONDS)]
        timeout_seconds: u64,
    },
}

#[derive(Subcommand)]
enum InitializeCommand {
    Run {
        environment_id: Uuid,
        /// JSON file containing an array of declared migration/seed tasks.
        #[arg(long)]
        file: PathBuf,
    },
}

#[derive(ClapArgs)]
struct ComposeArgs {
    #[command(subcommand)]
    command: ComposeCommand,
}

#[derive(Subcommand)]
enum ComposeCommand {
    Up(ComposeUpArgs),
}

#[derive(ClapArgs)]
struct MetroArgs {
    #[command(subcommand)]
    command: MetroCommand,
}

#[derive(Subcommand)]
enum MetroCommand {
    Up {
        #[arg(long)]
        trust: bool,
        #[arg(long)]
        worktree: PathBuf,
        #[arg(long, default_value = "metro")]
        profile: String,
        #[arg(long)]
        instance: Option<String>,
        /// `0` selects `DevFleet`'s reserved automatic port.
        #[arg(long, default_value_t = 0)]
        port: u16,
        #[arg(long, default_value_t = 1)]
        max_workers: u16,
    },
}

#[derive(ClapArgs)]
struct ComposeUpArgs {
    #[arg(long)]
    trust: bool,
    #[arg(long)]
    worktree: PathBuf,
    #[arg(long, default_value = "compose")]
    profile: String,
    #[arg(long)]
    instance: Option<String>,
    #[arg(long = "file", required = true)]
    files: Vec<PathBuf>,
    #[arg(long = "env")]
    environment: Vec<String>,
    /// `NAME=SERVICE:CONTAINER_PORT:HOST_PORT:/READINESS_PATH`
    #[arg(long = "endpoint")]
    endpoints: Vec<String>,
    #[arg(long = "allow-global-container-names")]
    global_container_names: Option<String>,
    #[arg(long = "allow-host-networking")]
    host_networking: Option<String>,
    #[arg(long = "allow-external-resources")]
    external_resources: Option<String>,
    #[arg(long = "allow-global-volume-names")]
    global_volume_names: Option<String>,
    #[arg(long = "allow-unmanaged-ports")]
    unmanaged_ports: Option<String>,
    #[arg(long = "allow-shared-mutable-binds")]
    shared_mutable_binds: Option<String>,
}

#[derive(ClapArgs)]
struct LogsArgs {
    environment_id: Uuid,
    #[arg(long, default_value = "process")]
    resource: String,
    #[arg(long)]
    follow: bool,
    #[arg(long, default_value_t = 100)]
    limit: usize,
}

#[derive(ClapArgs)]
struct EventsArgs {
    environment_id: Uuid,
    #[arg(long)]
    after: Option<u64>,
    #[arg(long)]
    follow: bool,
}

#[derive(ClapArgs)]
struct OperationArgs {
    #[command(subcommand)]
    command: OperationCommand,
}

#[derive(Subcommand)]
enum OperationCommand {
    Get {
        operation_id: Uuid,
    },
    Cancel {
        operation_id: Uuid,
    },
    Wait {
        operation_id: Uuid,
        #[arg(long, value_enum, default_value_t = OperationCondition::Terminal)]
        until: OperationCondition,
        #[arg(long, default_value_t = 60)]
        timeout_seconds: u64,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum OperationCondition {
    Accepted,
    Terminal,
}

#[derive(ClapArgs)]
struct ResourceArgs {
    #[command(subcommand)]
    command: ResourceCommand,
}

#[derive(Subcommand)]
enum ResourceCommand {
    Exec {
        environment_id: Uuid,
        #[arg(long)]
        cwd: PathBuf,
        #[arg(long = "env")]
        environment: Vec<String>,
        #[arg(long, default_value_t = RESOURCE_EXEC_DEFAULT_TIMEOUT_SECONDS)]
        timeout_seconds: u64,
        #[arg(last = true, required = true)]
        argv: Vec<String>,
    },
    Open {
        environment_id: Uuid,
    },
    Stop {
        environment_id: Uuid,
    },
}

#[derive(ClapArgs)]
struct PlanArgs {
    #[arg(long)]
    worktree: PathBuf,
    #[arg(long, default_value = "process")]
    profile: String,
    /// Defaults to devfleet.toml inside --worktree.
    #[arg(long)]
    manifest: Option<PathBuf>,
    #[arg(long)]
    local_override: Option<PathBuf>,
    /// Declares a required secret reference without sending a secret value.
    #[arg(long = "secret-ref")]
    secret_references: Vec<String>,
    /// JSON operation override in dotted.path=JSON form.
    #[arg(long = "set")]
    operation_overrides: Vec<String>,
    /// Known runtime output in resource.output=JSON form.
    #[arg(long = "output")]
    runtime_outputs: Vec<String>,
    /// Reserved planning endpoint in resource.endpoint=JSON form.
    #[arg(long = "endpoint")]
    planning_endpoints: Vec<String>,
}

#[derive(ClapArgs)]
struct PluginArgs {
    #[command(subcommand)]
    command: PluginCommand,
}

#[derive(Subcommand)]
enum PluginCommand {
    List,
    Install { path: PathBuf },
    Trust { id: String },
}

#[derive(ClapArgs)]
struct UpArgs {
    /// Required acknowledgement that this project's command is trusted.
    #[arg(long)]
    trust: bool,
    #[arg(long)]
    worktree: PathBuf,
    #[arg(long, default_value = "process")]
    profile: String,
    #[arg(long)]
    cwd: PathBuf,
    /// Explicit name for an additional environment instance at this worktree.
    #[arg(long)]
    instance: Option<String>,
    /// Return as soon as the daemon has durably accepted the start operation.
    #[arg(long)]
    detach: bool,
    /// Explicit process environment entry in KEY=VALUE form. Inherited env is never used.
    #[arg(long = "env")]
    environment: Vec<String>,
    #[arg(last = true, required = true)]
    argv: Vec<String>,
}

#[derive(ClapArgs)]
#[allow(clippy::struct_excessive_bools)]
struct DestroyArgs {
    environment_id: Uuid,
    #[arg(long)]
    dry_run: bool,
    #[arg(long, conflicts_with = "non_interactive")]
    confirm: bool,
    #[arg(long)]
    non_interactive: bool,
    #[arg(long)]
    purge_data: bool,
    #[arg(long)]
    purge_artifacts: bool,
}

#[derive(ClapArgs)]
struct SchedulerArgs {
    #[command(subcommand)]
    command: SchedulerCommand,
}

#[derive(Subcommand)]
enum SchedulerCommand {
    Status,
    Focus {
        environment_id: Uuid,
    },
    Configure {
        #[arg(long, default_value_t = 2)]
        native_builds: u16,
        #[arg(long, default_value_t = 2)]
        device_boots: u16,
        #[arg(long, default_value_t = 2)]
        preparations: u16,
        #[arg(long, default_value_t = 8)]
        metro_workers: u16,
        #[arg(long, default_value_t = 8_192)]
        memory_mib: u64,
        #[arg(long, default_value_t = 512)]
        reserve_mib: u64,
        #[arg(long, default_value_t = 30)]
        aging_seconds: u64,
    },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.smoke {
        println!(
            "{}",
            serde_json::to_string(&SmokeResponse {
                component: "cli".into(),
                protocol: PROTOCOL_VERSION,
                status: SmokeStatus::Ready,
            })?
        );
        return Ok(());
    }
    let socket = args
        .socket
        .ok_or_else(|| anyhow::anyhow!("--socket is required"))?;
    let command = args
        .command
        .ok_or_else(|| anyhow::anyhow!("a control command is required"))?;
    match command {
        Command::Logs(logs) => print_logs(&socket, &logs, args.json),
        Command::Events(events) => print_events(&socket, &events, args.json),
        Command::Operation(OperationArgs {
            command:
                OperationCommand::Wait {
                    operation_id,
                    until,
                    timeout_seconds,
                },
        }) => wait_for_operation(&socket, operation_id, until, timeout_seconds, args.json),
        command => {
            let response = request(&socket, request_from(command)?)?;
            print_response(&response, args.json)
        }
    }
}

fn wait_for_operation(
    socket: &Path,
    operation_id: Uuid,
    until: OperationCondition,
    timeout_seconds: u64,
    json: bool,
) -> anyhow::Result<()> {
    if timeout_seconds == 0 {
        anyhow::bail!("--timeout-seconds must be nonzero");
    }
    let started = std::time::Instant::now();
    loop {
        let response = request(socket, DaemonRequest::OperationGet { operation_id })?;
        let DaemonResponse::Operation(operation) = &response else {
            anyhow::bail!("daemon returned an unexpected operation response");
        };
        if operation_matches_condition(operation.status, until) {
            return print_response(&response, json);
        }
        if started.elapsed() >= Duration::from_secs(timeout_seconds) {
            anyhow::bail!("operation did not satisfy the requested condition before the deadline");
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn operation_matches_condition(
    status: devfleet_protocol::OperationStatus,
    condition: OperationCondition,
) -> bool {
    match condition {
        OperationCondition::Accepted => matches!(
            status,
            devfleet_protocol::OperationStatus::Accepted
                | devfleet_protocol::OperationStatus::CancellationRequested
                | devfleet_protocol::OperationStatus::Cancelled
                | devfleet_protocol::OperationStatus::Completed
                | devfleet_protocol::OperationStatus::Failed
                | devfleet_protocol::OperationStatus::Uncertain
        ),
        OperationCondition::Terminal => matches!(
            status,
            devfleet_protocol::OperationStatus::Completed
                | devfleet_protocol::OperationStatus::Failed
                | devfleet_protocol::OperationStatus::Cancelled
                | devfleet_protocol::OperationStatus::Uncertain
        ),
    }
}

fn print_events(socket: &Path, events: &EventsArgs, json: bool) -> anyhow::Result<()> {
    let mut after = events.after;
    let mut deduplicator = EventDeduplicator::new();
    loop {
        let response = request(
            socket,
            DaemonRequest::EventsSubscribe {
                environment_id: events.environment_id,
                after,
            },
        )?;
        let DaemonResponse::Events(received) = response else {
            anyhow::bail!("daemon returned an unexpected events response");
        };
        if let Some(event) = received.last() {
            after = Some(event.sequence);
        }
        let deduplicated = received
            .into_iter()
            .filter(|event| deduplicator.insert(event.id))
            .collect();
        print_response(&DaemonResponse::Events(deduplicated), json)?;
        if !events.follow {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(200));
    }
}

struct EventDeduplicator {
    ids: HashSet<Uuid>,
    order: VecDeque<Uuid>,
}

impl EventDeduplicator {
    const CAPACITY: usize = 256;

    fn new() -> Self {
        Self {
            ids: HashSet::with_capacity(Self::CAPACITY),
            order: VecDeque::with_capacity(Self::CAPACITY),
        }
    }

    fn insert(&mut self, id: Uuid) -> bool {
        if !self.ids.insert(id) {
            return false;
        }
        self.order.push_back(id);
        if self.order.len() > Self::CAPACITY
            && let Some(expired) = self.order.pop_front()
        {
            self.ids.remove(&expired);
        }
        true
    }
}

fn print_logs(socket: &Path, logs: &LogsArgs, json: bool) -> anyhow::Result<()> {
    let mut after = None;
    loop {
        let response = request(
            socket,
            DaemonRequest::LogsTail {
                environment_id: logs.environment_id,
                resource: logs.resource.clone(),
                after,
                limit: logs.limit,
            },
        )?;
        let DaemonResponse::Logs(segment) = response else {
            anyhow::bail!("daemon returned an unexpected log response");
        };
        if json {
            println!("{}", serde_json::to_string(&segment)?);
        } else {
            for line in &segment.lines {
                println!("{line}");
            }
            if segment.reset {
                eprintln!("log cursor reset after rotation; showing the current file");
            }
        }
        after = Some(segment.cursor);
        if !logs.follow {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(200));
    }
}

#[allow(clippy::too_many_lines)]
fn request_from(command: Command) -> anyhow::Result<DaemonRequest> {
    match command {
        Command::Init(init) => Ok(DaemonRequest::ProjectInit(ProjectInitRequest {
            worktree: init.worktree.canonicalize()?.display().to_string(),
            trusted: init.trust,
            runtime: init.runtime,
            device_type: init.device_type,
        })),
        Command::Doctor(doctor) => Ok(DaemonRequest::Doctor(DoctorRequest {
            worktree: absolute_path(&doctor.worktree)?.display().to_string(),
            environment_id: doctor.environment_id,
        })),
        Command::Plan(plan) => {
            let worktree = plan.worktree.canonicalize()?;
            let manifest = plan
                .manifest
                .unwrap_or_else(|| worktree.join("devfleet.toml"))
                .canonicalize()?;
            Ok(DaemonRequest::Plan(EnvironmentPlanRequest {
                worktree: worktree.display().to_string(),
                profile: plan.profile,
                manifest: manifest.display().to_string(),
                local_override: plan
                    .local_override
                    .map(|path| path.canonicalize())
                    .transpose()?
                    .map(|path| path.display().to_string()),
                secret_references: parse_references(plan.secret_references)?,
                operation_overrides: parse_json_values(plan.operation_overrides, "--set")?,
                runtime_outputs: parse_json_values(plan.runtime_outputs, "--output")?,
                planning_endpoints: parse_json_values(plan.planning_endpoints, "--endpoint")?,
            }))
        }
        Command::Up(up) => Ok(DaemonRequest::Up(EnvironmentUpRequest {
            worktree: up.worktree.canonicalize()?.display().to_string(),
            profile: up.profile,
            instance: up.instance,
            trusted: up.trust,
            process: ProcessSpec {
                argv: up.argv,
                cwd: up.cwd.canonicalize()?.display().to_string(),
                environment: parse_environment(up.environment)?,
            },
        })),
        Command::Compose(compose) => match compose.command {
            ComposeCommand::Up(up) => {
                let worktree = up.worktree.canonicalize()?;
                Ok(DaemonRequest::ComposeUp(ComposeUpRequest {
                    worktree: worktree.display().to_string(),
                    profile: up.profile,
                    instance: up.instance,
                    trusted: up.trust,
                    files: up
                        .files
                        .into_iter()
                        .map(|file| file.canonicalize())
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .map(|file| file.display().to_string())
                        .collect(),
                    environment: parse_environment(up.environment)?,
                    endpoints: parse_compose_endpoints(up.endpoints)?,
                    authorizations: ComposeAuthorizations {
                        global_container_names: up.global_container_names,
                        host_networking: up.host_networking,
                        external_resources: up.external_resources,
                        global_volume_names: up.global_volume_names,
                        unmanaged_ports: up.unmanaged_ports,
                        shared_mutable_binds: up.shared_mutable_binds,
                    },
                }))
            }
        },
        Command::Metro(metro) => match metro.command {
            MetroCommand::Up {
                trust,
                worktree,
                profile,
                instance,
                port,
                max_workers,
            } => Ok(DaemonRequest::MetroUp(MetroUpRequest {
                worktree: worktree.canonicalize()?.display().to_string(),
                profile,
                instance,
                trusted: trust,
                port,
                max_workers,
                binding_integration: devfleet_protocol::BindingIntegration::BundleConfig,
            })),
        },
        Command::Initialize(initialization) => match initialization.command {
            InitializeCommand::Run {
                environment_id,
                file,
            } => Ok(DaemonRequest::InitializationRun(InitializationRunRequest {
                environment_id,
                tasks: serde_json::from_slice::<Vec<InitializationTask>>(&std::fs::read(file)?)?,
            })),
        },
        Command::Prepare(preparation) => match preparation.command {
            PrepareCommand::Run {
                environment_id,
                timeout_seconds,
            } => Ok(DaemonRequest::PreparationRun(PreparationRunRequest {
                environment_id,
                timeout_seconds,
            })),
        },
        Command::Bindings(bindings) => match bindings.command {
            BindingsCommand::Apply { file } => {
                Ok(DaemonRequest::BindingApply(serde_json::from_slice::<
                    BindingApplyRequest,
                >(
                    &std::fs::read(file)?
                )?))
            }
        },
        Command::Ios(ios) => match ios.command {
            IosCommand::Capture { environment_id } => {
                Ok(DaemonRequest::DeviceCapture(DeviceCaptureRequest {
                    environment_id,
                    recording_seconds: None,
                }))
            }
            IosCommand::Record {
                environment_id,
                seconds,
            } => Ok(DaemonRequest::DeviceCapture(DeviceCaptureRequest {
                environment_id,
                recording_seconds: Some(seconds),
            })),
            IosCommand::Run {
                environment_id,
                bundle_id,
                scheme,
                verification,
                backend_identity,
                backend_identity_url,
                identity_hook_module,
            } => Ok(DaemonRequest::IosAppRun(IosAppRunRequest {
                environment_id,
                bundle_id,
                scheme,
                verification: verification.into(),
                backend_identity,
                backend_identity_url,
                identity_hook_module,
            })),
            IosCommand::Acquire {
                environment_id,
                runtime,
                device_type,
            } => Ok(DaemonRequest::IosSimulatorAcquire(
                IosSimulatorAcquireRequest {
                    environment_id,
                    runtime,
                    device_type,
                },
            )),
            IosCommand::Open { environment_id } => Ok(DaemonRequest::IosSimulatorAction(
                IosSimulatorActionRequest {
                    environment_id,
                    action: IosSimulatorAction::Open,
                },
            )),
            IosCommand::Stop { environment_id } => Ok(DaemonRequest::IosSimulatorAction(
                IosSimulatorActionRequest {
                    environment_id,
                    action: IosSimulatorAction::Stop,
                },
            )),
        },
        Command::Resource(resource) => match resource.command {
            ResourceCommand::Exec {
                environment_id,
                cwd,
                environment,
                timeout_seconds,
                argv,
            } => Ok(DaemonRequest::ResourceAction(ResourceActionRequest {
                environment_id,
                resource: "environment".into(),
                action: ResourceAction::Exec(ResourceExecRequest {
                    argv,
                    cwd: cwd.canonicalize()?.display().to_string(),
                    environment: parse_environment(environment)?,
                    timeout_seconds,
                }),
            })),
            ResourceCommand::Open { environment_id } => {
                Ok(DaemonRequest::ResourceAction(ResourceActionRequest {
                    environment_id,
                    resource: "ios_simulator".into(),
                    action: ResourceAction::Open,
                }))
            }
            ResourceCommand::Stop { environment_id } => {
                Ok(DaemonRequest::ResourceAction(ResourceActionRequest {
                    environment_id,
                    resource: "ios_simulator".into(),
                    action: ResourceAction::Stop,
                }))
            }
        },
        Command::Identity(identity) => match identity.command {
            IdentityCommand::Start {
                environment_id,
                backend_identity,
                backend_identity_url,
            } => Ok(DaemonRequest::IdentityVerificationStart(
                IdentityVerificationStartRequest {
                    environment_id,
                    backend_identity,
                    backend_identity_url,
                },
            )),
            IdentityCommand::Status { environment_id } => {
                Ok(DaemonRequest::IdentityVerificationStatus { environment_id })
            }
        },
        Command::Status { environment_id } => Ok(DaemonRequest::Status { environment_id }),
        Command::List => Ok(DaemonRequest::List),
        Command::Stop { environment_id } => Ok(DaemonRequest::Stop { environment_id }),
        Command::Destroy(destroy) => {
            let authorization = match (destroy.confirm, destroy.non_interactive) {
                (true, false) => DestroyAuthorization::Confirmed,
                (false, true) => DestroyAuthorization::Noninteractive,
                (false, false) => DestroyAuthorization::None,
                (true, true) => unreachable!("clap rejects conflicting destroy flags"),
            };
            if !destroy.dry_run && authorization == DestroyAuthorization::None {
                anyhow::bail!(
                    "destroy requires --confirm or --non-interactive; use --dry-run to preview"
                );
            }
            Ok(DaemonRequest::Destroy(DestroyRequest {
                environment_id: destroy.environment_id,
                dry_run: destroy.dry_run,
                authorization,
                purge_data: destroy.purge_data,
                purge_artifacts: destroy.purge_artifacts,
            }))
        }
        Command::Reconcile { environment_id } => Ok(DaemonRequest::Reconcile { environment_id }),
        Command::Scheduler(scheduler) => match scheduler.command {
            SchedulerCommand::Status => Ok(DaemonRequest::SchedulerStatus),
            SchedulerCommand::Focus { environment_id } => {
                Ok(DaemonRequest::SchedulerFocus { environment_id })
            }
            SchedulerCommand::Configure {
                native_builds,
                device_boots,
                preparations,
                metro_workers,
                memory_mib,
                reserve_mib,
                aging_seconds,
            } => Ok(DaemonRequest::SchedulerConfigure(SchedulerConfig {
                native_build_limit: native_builds,
                device_boot_limit: device_boots,
                preparation_limit: preparations,
                metro_worker_limit: metro_workers,
                memory_limit_bytes: memory_mib
                    .checked_mul(1024 * 1024)
                    .ok_or_else(|| anyhow::anyhow!("--memory-mib exceeds supported range"))?,
                memory_reserve_bytes: reserve_mib
                    .checked_mul(1024 * 1024)
                    .ok_or_else(|| anyhow::anyhow!("--reserve-mib exceeds supported range"))?,
                aging_interval_ms: aging_seconds
                    .checked_mul(1_000)
                    .ok_or_else(|| anyhow::anyhow!("--aging-seconds exceeds supported range"))?,
            })),
        },
        Command::Operation(operation) => match operation.command {
            OperationCommand::Get { operation_id } => {
                Ok(DaemonRequest::OperationGet { operation_id })
            }
            OperationCommand::Cancel { operation_id } => {
                Ok(DaemonRequest::OperationCancel { operation_id })
            }
            OperationCommand::Wait { .. } => {
                anyhow::bail!("operation wait is handled by the client")
            }
        },
        Command::Events(events) => Ok(DaemonRequest::EventsSubscribe {
            environment_id: events.environment_id,
            after: events.after,
        }),
        Command::Logs(logs) => Ok(DaemonRequest::LogsTail {
            environment_id: logs.environment_id,
            resource: logs.resource,
            after: None,
            limit: logs.limit,
        }),
        Command::Plugin(plugin) => match plugin.command {
            PluginCommand::List => Ok(DaemonRequest::PluginList),
            PluginCommand::Install { path } => Ok(DaemonRequest::PluginInstall {
                path: path.canonicalize()?.display().to_string(),
            }),
            PluginCommand::Trust { id } => Ok(DaemonRequest::PluginTrust { id }),
        },
    }
}

fn parse_references(values: Vec<String>) -> anyhow::Result<Vec<String>> {
    let mut references = values.into_iter().collect::<Vec<_>>();
    references.sort();
    if references.iter().any(String::is_empty)
        || references.windows(2).any(|pair| pair[0] == pair[1])
    {
        anyhow::bail!("--secret-ref values must be nonempty and unique");
    }
    Ok(references)
}

fn parse_json_values(
    values: Vec<String>,
    option: &str,
) -> anyhow::Result<BTreeMap<String, serde_json::Value>> {
    values
        .into_iter()
        .try_fold(BTreeMap::new(), |mut entries, value| {
            let (key, json) = value
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("{option} must use key=JSON"))?;
            if key.is_empty() || entries.contains_key(key) {
                anyhow::bail!("{option} keys must be nonempty and unique");
            }
            entries.insert(
                key.into(),
                serde_json::from_str(json)
                    .map_err(|_| anyhow::anyhow!("{option} values must be valid JSON"))?,
            );
            Ok(entries)
        })
}

fn parse_environment(values: Vec<String>) -> anyhow::Result<BTreeMap<String, String>> {
    values
        .into_iter()
        .try_fold(BTreeMap::new(), |mut entries, value| {
            let (key, value) = value
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("--env must use KEY=VALUE"))?;
            if key.is_empty() || entries.insert(key.into(), value.into()).is_some() {
                anyhow::bail!("--env keys must be nonempty and unique");
            }
            Ok(entries)
        })
}

fn parse_compose_endpoints(values: Vec<String>) -> anyhow::Result<Vec<ComposeEndpoint>> {
    let mut names = std::collections::BTreeSet::new();
    let mut host_ports = std::collections::BTreeSet::new();
    values
        .into_iter()
        .map(|value| {
            let (name, definition) = value.split_once('=').ok_or_else(|| {
                anyhow::anyhow!("--endpoint must use NAME=SERVICE:CONTAINER_PORT:HOST_PORT:/PATH")
            })?;
            let mut parts = definition.splitn(4, ':');
            let service = parts.next().unwrap_or_default();
            let container_port = parts.next().unwrap_or_default().parse::<u16>()?;
            let host_port = parts.next().unwrap_or_default().parse::<u16>()?;
            let readiness_path = parts.next().unwrap_or_default();
            if name.is_empty()
                || service.is_empty()
                || container_port == 0
                || !readiness_path.starts_with('/')
                || !names.insert(name.to_owned())
                || (host_port != 0 && !host_ports.insert(host_port))
            {
                anyhow::bail!("--endpoint names and explicit host ports must be unique with /PATH");
            }
            Ok(ComposeEndpoint {
                name: name.into(),
                service: service.into(),
                container_port,
                host_port,
                readiness_path: readiness_path.into(),
            })
        })
        .collect()
}

#[allow(clippy::too_many_lines)]
fn print_response(response: &DaemonResponse, json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string(response)?);
        return resource_action_result(response);
    }
    match response {
        DaemonResponse::ProjectInit(project) => {
            println!(
                "initialized {} trusted={}{}",
                project.worktree,
                project.trusted,
                project
                    .runtime
                    .as_deref()
                    .map_or_else(String::new, |runtime| { format!(" runtime={runtime}") })
            );
            println!(
                "proposed manifest {} (not written)",
                project.proposed_manifest_path
            );
            print!("{}", project.proposed_manifest);
            if !project.proposed_manifest.ends_with('\n') {
                println!();
            }
            for integration in &project.proposed_integrations {
                println!("proposed integration {integration}");
            }
        }
        DaemonResponse::Doctor(doctor) => {
            println!("doctor {} ready={}", doctor.worktree, doctor.ready);
            for finding in &doctor.findings {
                println!(
                    "{:?} {} blocking={} retryable={} cause={} remediation={}",
                    finding.phase,
                    finding.resource,
                    finding.blocking,
                    finding.retryable,
                    finding.cause,
                    finding.remediation
                );
            }
        }
        DaemonResponse::Plan(plan) => println!(
            "{} {} generation {}",
            plan.worktree, plan.profile, plan.generation
        ),
        DaemonResponse::Accepted(operation) => println!(
            "accepted operation {} for environment {}",
            operation.operation_id, operation.environment_id
        ),
        DaemonResponse::Environment(environment) => {
            println!(
                "{} {}/{} {:?}",
                environment.environment_id,
                environment.project_id,
                environment.worktree_id,
                environment.lifecycle
            );
            if let Some(diagnostic) = &environment.reconciliation_diagnostic {
                println!("reconciliation diagnostic={diagnostic}");
            }
            if let Some(action) = &environment.reconciliation_action {
                println!("reconciliation action={action}");
            }
            for endpoint in &environment.endpoints {
                println!(
                    "endpoint {} internal={} host={} device={} healthy={}",
                    endpoint.name,
                    endpoint.internal_url,
                    endpoint.host_url,
                    endpoint.device_url,
                    endpoint.healthy
                );
            }
            for binding in &environment.bindings {
                println!(
                    "binding {:?} {:?} {}",
                    binding.phase, binding.invalidation, binding.fingerprint
                );
            }
            if let Some(simulator) = &environment.ios_simulator {
                println!(
                    "ios {} {} {:?}",
                    simulator.udid, simulator.runtime, simulator.lifecycle
                );
            }
        }
        DaemonResponse::Environments(environments) => {
            for environment in environments {
                println!(
                    "{} {}/{} {:?}",
                    environment.environment_id,
                    environment.project_id,
                    environment.worktree_id,
                    environment.lifecycle
                );
            }
        }
        DaemonResponse::Operation(operation) => println!(
            "{} {:?} phase={} resource={}",
            operation.operation_id,
            operation.status,
            operation.phase,
            operation.resource_id.as_deref().unwrap_or("environment")
        ),
        DaemonResponse::ResourceAction(action) => {
            println!(
                "{} {} {}{}",
                action.environment_id,
                action.resource,
                action.action,
                action
                    .operation
                    .as_ref()
                    .map_or_else(String::new, |operation| {
                        format!(" operation={}", operation.operation_id)
                    })
            );
            if !action.stdout.is_empty() {
                print!("{}", action.stdout);
            }
            if !action.stderr.is_empty() {
                eprint!("{}", action.stderr);
            }
        }
        DaemonResponse::Events(events) => {
            for event in events {
                println!("{} {} {}", event.sequence, event.id, event.kind);
            }
        }
        DaemonResponse::Logs(logs) => {
            for line in &logs.lines {
                println!("{line}");
            }
        }
        DaemonResponse::Plugin(plugin) => println!(
            "{} {} {} trusted={}",
            plugin.id, plugin.version, plugin.checksum, plugin.trusted
        ),
        DaemonResponse::Plugins(plugins) => {
            for plugin in plugins {
                println!(
                    "{} {} {} trusted={}",
                    plugin.id, plugin.version, plugin.checksum, plugin.trusted
                );
            }
        }
        DaemonResponse::InitializationTasks(tasks) => {
            for task in tasks {
                println!(
                    "{} {:?} {:?} attempts={}{}",
                    task.id,
                    task.status,
                    task.rerun_policy,
                    task.attempts,
                    task.diagnostic
                        .as_deref()
                        .map_or_else(String::new, |diagnostic| format!(
                            " diagnostic={diagnostic}"
                        ))
                );
            }
        }
        DaemonResponse::PreparationTasks(tasks) => {
            for task in tasks {
                println!(
                    "{:?} {:?} attempts={}{}",
                    task.kind,
                    task.status,
                    task.attempts,
                    task.diagnostic
                        .as_deref()
                        .map_or_else(String::new, |diagnostic| format!(
                            " diagnostic={diagnostic}"
                        ))
                );
            }
        }
        DaemonResponse::Bindings(bindings) => {
            for binding in bindings {
                println!(
                    "{:?} {:?} {:?}",
                    binding.phase, binding.invalidation, binding.fingerprint
                );
            }
        }
        DaemonResponse::IosSimulator(simulator) => println!(
            "{} {} {:?}",
            simulator.udid, simulator.runtime, simulator.lifecycle
        ),
        DaemonResponse::IosApp(app) => println!(
            "{} {} metro={} launched={}",
            app.bundle_id, app.udid, app.metro_port, app.launched
        ),
        DaemonResponse::IdentityVerification(identity) => println!(
            "{:?} {} nonce={} token={}{}",
            identity.status,
            identity.verification_url,
            identity.nonce,
            identity.token,
            identity
                .diagnostic
                .as_deref()
                .map_or_else(String::new, |diagnostic| format!(
                    " diagnostic={diagnostic}"
                ))
        ),
        DaemonResponse::Artifact(artifact) => println!(
            "{:?} {} obsolete={}",
            artifact.kind, artifact.path, artifact.obsolete
        ),
        DaemonResponse::Destroy(destroy) => {
            println!(
                "destroy {} dry_run={}",
                destroy.environment_id, destroy.dry_run
            );
            for target in &destroy.targets {
                println!("{} {} {:?}", target.kind, target.identifier, target.status);
            }
            for retained in &destroy.retained {
                println!("retained {retained}");
            }
            for purged in &destroy.purged {
                println!("purged {purged}");
            }
        }
        DaemonResponse::Scheduler(scheduler) => {
            println!(
                "scheduler native={} boots={} preparation={} metro_workers={} memory={}/{}",
                scheduler.pressure.native_builds_active,
                scheduler.pressure.device_boots_active,
                scheduler.pressure.preparations_active,
                scheduler.pressure.metro_workers_active,
                scheduler.pressure.estimated_active_memory_bytes,
                scheduler.pressure.memory_limit_bytes
            );
            for job in &scheduler.jobs {
                println!(
                    "{} {} {:?} {:?}{}",
                    job.job_id,
                    job.environment_id,
                    job.priority,
                    job.state,
                    job.queued_reason
                        .as_deref()
                        .map_or_else(String::new, |reason| format!(" reason={reason}"))
                );
            }
        }
    }
    resource_action_result(response)
}

fn resource_action_result(response: &DaemonResponse) -> anyhow::Result<()> {
    let Some(operation) = matches!(response, DaemonResponse::ResourceAction(_))
        .then(|| response)
        .and_then(|response| match response {
            DaemonResponse::ResourceAction(action) => action.operation.as_ref(),
            _ => None,
        })
    else {
        return Ok(());
    };
    if matches!(
        operation.status,
        devfleet_protocol::OperationStatus::Failed
            | devfleet_protocol::OperationStatus::Cancelled
            | devfleet_protocol::OperationStatus::Uncertain
    ) {
        anyhow::bail!("resource action did not complete successfully");
    }
    Ok(())
}

fn absolute_path(path: &Path) -> anyhow::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_duplicate_or_malformed_environment_entries() {
        assert!(parse_environment(vec!["A=1".into(), "A=2".into()]).is_err());
        assert!(parse_environment(vec!["missing".into()]).is_err());
    }

    #[test]
    fn maps_init_and_doctor_commands_to_typed_requests() {
        let worktree = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("workspace root")
            .to_path_buf();
        let init = request_from(Command::Init(InitArgs {
            trust: true,
            worktree: worktree.clone(),
            runtime: Some("runtime-18".into()),
            device_type: Some("phone-16".into()),
        }))
        .expect("init request builds");
        assert!(matches!(
            init,
            DaemonRequest::ProjectInit(ProjectInitRequest {
                trusted: true,
                runtime: Some(_),
                device_type: Some(_),
                ..
            })
        ));
        let environment_id = Uuid::nil();
        let doctor = request_from(Command::Doctor(DoctorArgs {
            worktree,
            environment_id: Some(environment_id),
        }))
        .expect("doctor request builds");
        assert!(matches!(
            doctor,
            DaemonRequest::Doctor(DoctorRequest {
                environment_id: Some(id),
                ..
            }) if id == environment_id
        ));
    }

    #[test]
    fn doctor_preserves_a_missing_worktree_for_typed_diagnostics() {
        let request = request_from(Command::Doctor(DoctorArgs {
            worktree: PathBuf::from("missing-worktree"),
            environment_id: None,
        }))
        .expect("doctor request builds");
        let DaemonRequest::Doctor(DoctorRequest { worktree, .. }) = request else {
            panic!("expected doctor request");
        };
        assert!(Path::new(&worktree).is_absolute());
        assert!(worktree.ends_with("missing-worktree"));
    }

    #[test]
    fn maps_environment_scoped_exec_to_the_public_resource_action() {
        let worktree = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("workspace root")
            .to_path_buf();
        let request = request_from(Command::Resource(ResourceArgs {
            command: ResourceCommand::Exec {
                environment_id: Uuid::nil(),
                cwd: worktree,
                environment: vec!["FIXTURE=value".into()],
                timeout_seconds: 5,
                argv: vec!["/bin/true".into()],
            },
        }))
        .expect("resource exec request builds");
        let DaemonRequest::ResourceAction(action) = request else {
            panic!("expected resource action");
        };
        assert_eq!(action.resource, "environment");
        assert!(matches!(
            action.action,
            ResourceAction::Exec(ResourceExecRequest {
                timeout_seconds: 5,
                ..
            })
        ));
    }

    #[test]
    fn operation_wait_conditions_distinguish_acceptance_from_terminal_states() {
        assert!(operation_matches_condition(
            devfleet_protocol::OperationStatus::Accepted,
            OperationCondition::Accepted
        ));
        assert!(operation_matches_condition(
            devfleet_protocol::OperationStatus::Completed,
            OperationCondition::Accepted
        ));
        assert!(!operation_matches_condition(
            devfleet_protocol::OperationStatus::Accepted,
            OperationCondition::Terminal
        ));
        assert!(operation_matches_condition(
            devfleet_protocol::OperationStatus::Completed,
            OperationCondition::Terminal
        ));
    }

    #[test]
    fn destroy_requires_authorization_except_for_a_preview() {
        let environment_id = Uuid::now_v7();
        assert!(
            request_from(Command::Destroy(DestroyArgs {
                environment_id,
                dry_run: false,
                confirm: false,
                non_interactive: false,
                purge_data: false,
                purge_artifacts: false,
            }))
            .is_err()
        );
        let request = request_from(Command::Destroy(DestroyArgs {
            environment_id,
            dry_run: true,
            confirm: false,
            non_interactive: false,
            purge_data: false,
            purge_artifacts: false,
        }))
        .expect("preview request builds");
        assert!(matches!(
            request,
            DaemonRequest::Destroy(DestroyRequest {
                dry_run: true,
                authorization: DestroyAuthorization::None,
                ..
            })
        ));
    }

    #[test]
    fn rejects_duplicate_or_malformed_plan_values() {
        assert!(parse_json_values(vec!["api.port=4000".into()], "--output").is_ok());
        assert!(parse_json_values(vec!["missing".into()], "--output").is_err());
        assert!(parse_json_values(vec!["api.port=not-json".into()], "--output").is_err());
        assert!(parse_references(vec!["API_TOKEN".into(), "API_TOKEN".into()]).is_err());
    }

    #[test]
    fn parses_scoped_compose_endpoints() {
        let endpoints = parse_compose_endpoints(vec!["api=api:3000:20001:/health".into()])
            .expect("endpoint parses");
        assert_eq!(endpoints[0].host_port, 20_001);
        let automatic = parse_compose_endpoints(vec!["api=api:3000:0:/health".into()])
            .expect("automatic endpoint parses");
        assert_eq!(automatic[0].host_port, 0);
        assert!(parse_compose_endpoints(vec!["api=api:3000:20001:health".into()]).is_err());
    }

    #[test]
    fn deduplicates_events_without_unbounded_client_memory() {
        let mut deduplicator = EventDeduplicator::new();
        let first = Uuid::now_v7();
        assert!(deduplicator.insert(first));
        assert!(!deduplicator.insert(first));
        for _ in 0..EventDeduplicator::CAPACITY {
            assert!(deduplicator.insert(Uuid::now_v7()));
        }
        assert!(deduplicator.insert(first));
    }
}
