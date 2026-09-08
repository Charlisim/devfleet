use clap::Parser;
use devfleet_plugin_sdk::{PluginError, Provider, ProviderDescriptor, serve_once};
use devfleet_protocol::{HostSpawnRequest, MetroReconcileRequest, PROTOCOL_VERSION};
use semver::Version;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::{self, BufReader},
    path::{Path, PathBuf},
};
use thiserror::Error;

const NODE_PATHS: [&str; 3] = [
    "/opt/homebrew/bin/node",
    "/usr/local/bin/node",
    "/usr/bin/node",
];

struct MetroProvider;

impl Provider for MetroProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            id: "official.metro".into(),
            version: Version::new(0, 1, 0),
            platform: current_platform(),
            protocol: PROTOCOL_VERSION,
            capabilities: vec![
                "lifecycle.initialize".into(),
                "lifecycle.validate".into(),
                "lifecycle.plan".into(),
                "lifecycle.inspect".into(),
                "lifecycle.stop".into(),
                "lifecycle.destroy".into(),
                "lifecycle.cancel".into(),
                "metro.reconcile".into(),
            ],
            plugin_state_version: 1,
        }
    }

    fn action(&self, name: String, input: Value) -> Result<Value, PluginError> {
        if name != "metro.reconcile" {
            return Err(PluginError::UnsupportedMethod(name));
        }
        let request: MetroReconcileRequest = serde_json::from_value(input)
            .map_err(|_| PluginError::InvalidCall("invalid metro.reconcile input".into()))?;
        validate(&request).map_err(|error| PluginError::RequestFailed(error.to_string()))?;
        let worktree = Path::new(&request.worktree);
        let cli = worktree.join("node_modules/react-native/cli.js");
        let node = NODE_PATHS
            .iter()
            .map(Path::new)
            .find(|path| path.is_file())
            .ok_or_else(|| {
                PluginError::RequestFailed("CAPABILITY_UNSUPPORTED: Node is unavailable".into())
            })?;
        serde_json::to_value(HostSpawnRequest {
            argv: vec![
                node.display().to_string(),
                cli.display().to_string(),
                "start".into(),
                "--port".into(),
                request.port.to_string(),
                "--config".into(),
                request.configuration_path,
            ],
            cwd: request.worktree,
            environment: BTreeMap::from([(
                "PATH".into(),
                "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin".into(),
            )]),
        })
        .map_err(|error| PluginError::RequestFailed(error.to_string()))
    }
}

#[derive(Debug, Error)]
enum MetroError {
    #[error("METRO_CONFIG_INVALID: incompatible plugin protocol")]
    Protocol,
    #[error("METRO_CONFIG_INVALID: Metro requires a nonzero resolved port and worker budget")]
    InvalidBudget,
    #[error("METRO_CONFIG_INVALID: worktree, cache, and generated configuration must exist")]
    MissingPath,
    #[error("METRO_CONFIG_INVALID: generated configuration must be private to its cache directory")]
    UnscopedConfiguration,
    #[error(
        "METRO_CONFIG_INVALID: React Native CLI must be installed inside the selected worktree"
    )]
    MissingCli,
    #[error(
        "METRO_CONFIG_INVALID: generated configuration does not enforce the selected root, empty watch folders, private cache, and worker budget"
    )]
    IncompleteConfiguration,
    #[error("METRO_CONFIG_INVALID: worktree dependency roots may not resolve through a symlink")]
    SymlinkedDependencies,
}

fn validate(request: &MetroReconcileRequest) -> Result<(), MetroError> {
    if !PROTOCOL_VERSION.compatible_with(request.protocol) {
        return Err(MetroError::Protocol);
    }
    if request.port == 0 || request.max_workers == 0 {
        return Err(MetroError::InvalidBudget);
    }
    let worktree = canonical_directory(Path::new(&request.worktree))?;
    let cache = canonical_directory(Path::new(&request.cache_directory))?;
    let config = Path::new(&request.configuration_path)
        .canonicalize()
        .map_err(|_| MetroError::MissingPath)?;
    if !config.starts_with(&cache) {
        return Err(MetroError::UnscopedConfiguration);
    }
    let node_modules = worktree.join("node_modules");
    if !node_modules.is_dir()
        || node_modules
            .symlink_metadata()
            .map_err(|_| MetroError::MissingPath)?
            .file_type()
            .is_symlink()
    {
        return Err(MetroError::SymlinkedDependencies);
    }
    if !node_modules.join("react-native/cli.js").is_file() {
        return Err(MetroError::MissingCli);
    }
    let contents = std::fs::read_to_string(config).map_err(|_| MetroError::MissingPath)?;
    for required in [
        format!("projectRoot = {}", js_string(&worktree)),
        "watchFolders: []".into(),
        format!("maxWorkers: {}", request.max_workers),
        format!("root: {}", js_string(&cache)),
        "disableHierarchicalLookup: true".into(),
        "devfleet-identity".into(),
    ] {
        if !contents.contains(&required) {
            return Err(MetroError::IncompleteConfiguration);
        }
    }
    Ok(())
}

fn canonical_directory(path: &Path) -> Result<PathBuf, MetroError> {
    let canonical = path.canonicalize().map_err(|_| MetroError::MissingPath)?;
    canonical
        .is_dir()
        .then_some(canonical)
        .ok_or(MetroError::MissingPath)
}

fn js_string(path: &Path) -> String {
    serde_json::to_string(&path.display().to_string()).expect("path serializes")
}

#[derive(Parser)]
struct Args {
    #[arg(long)]
    descriptor: bool,
    #[arg(long)]
    smoke: bool,
    #[arg(long)]
    serve: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.descriptor || args.smoke {
        println!("{}", serde_json::to_string(&MetroProvider.descriptor())?);
        return Ok(());
    }
    if args.serve {
        let stdin = io::stdin();
        let mut reader = BufReader::new(stdin.lock());
        let stdout = io::stdout();
        let mut writer = stdout.lock();
        return Ok(serve_once(&MetroProvider, &mut reader, &mut writer)?);
    }
    anyhow::bail!("Metro plugin requires --descriptor, --serve, or --smoke")
}

fn current_platform() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}
