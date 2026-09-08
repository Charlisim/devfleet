use clap::{Args as ClapArgs, Parser, Subcommand};
use devfleet_cli::request;
use devfleet_protocol::{
    BindingApplyRequest, ComposeAuthorizations, ComposeEndpoint, ComposeUpRequest, DaemonRequest,
    DaemonResponse, EnvironmentPlanRequest, EnvironmentUpRequest, InitializationRunRequest,
    InitializationTask, IosAppRunRequest, IosSimulatorAcquireRequest, IosSimulatorAction,
    IosSimulatorActionRequest, MetroUpRequest, PREPARATION_DEFAULT_TIMEOUT_SECONDS,
    PROTOCOL_VERSION, PreparationRunRequest, ProcessSpec, SmokeResponse, SmokeStatus,
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
    Plan(PlanArgs),
    Up(UpArgs),
    Status { environment_id: Uuid },
    List,
    Stop { environment_id: Uuid },
    Operation(OperationArgs),
    Events(EventsArgs),
    Logs(LogsArgs),
    Compose(ComposeArgs),
    Metro(MetroArgs),
    Initialize(InitializeArgs),
    Prepare(PrepareArgs),
    Bindings(BindingsArgs),
    Ios(IosArgs),
    Plugin(PluginArgs),
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

#[derive(Subcommand)]
enum IosCommand {
    Run {
        environment_id: Uuid,
        #[arg(long)]
        bundle_id: String,
        #[arg(long)]
        scheme: Option<String>,
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
        #[arg(long)]
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
    Get { operation_id: Uuid },
    Cancel { operation_id: Uuid },
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
    /// Explicit process environment entry in KEY=VALUE form. Inherited env is never used.
    #[arg(long = "env")]
    environment: Vec<String>,
    #[arg(last = true, required = true)]
    argv: Vec<String>,
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
        command => {
            let response = request(&socket, request_from(command)?)?;
            print_response(&response, args.json)
        }
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
            IosCommand::Run {
                environment_id,
                bundle_id,
                scheme,
            } => Ok(DaemonRequest::IosAppRun(IosAppRunRequest {
                environment_id,
                bundle_id,
                scheme,
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
        Command::Status { environment_id } => Ok(DaemonRequest::Status { environment_id }),
        Command::List => Ok(DaemonRequest::List),
        Command::Stop { environment_id } => Ok(DaemonRequest::Stop { environment_id }),
        Command::Operation(operation) => match operation.command {
            OperationCommand::Get { operation_id } => {
                Ok(DaemonRequest::OperationGet { operation_id })
            }
            OperationCommand::Cancel { operation_id } => {
                Ok(DaemonRequest::OperationCancel { operation_id })
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
                || host_port == 0
                || !readiness_path.starts_with('/')
                || !names.insert(name.to_owned())
                || !host_ports.insert(host_port)
            {
                anyhow::bail!(
                    "--endpoint names and host ports must be unique with nonzero ports and /PATH"
                );
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
        return Ok(());
    }
    match response {
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
    }
    Ok(())
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
