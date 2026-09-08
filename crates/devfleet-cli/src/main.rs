use clap::{Args as ClapArgs, Parser, Subcommand};
use devfleet_cli::request;
use devfleet_protocol::{
    DaemonRequest, DaemonResponse, EnvironmentPlanRequest, EnvironmentUpRequest, PROTOCOL_VERSION,
    ProcessSpec, SmokeResponse, SmokeStatus,
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
    Plugin(PluginArgs),
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
        DaemonResponse::Environment(environment) => println!(
            "{} {}/{} {:?}",
            environment.environment_id,
            environment.project_id,
            environment.worktree_id,
            environment.lifecycle
        ),
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
