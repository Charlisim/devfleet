use clap::Parser;
use devfleet_plugin_sdk::{PluginError, Provider, ProviderDescriptor, serve_once};
use devfleet_protocol::{HostSpawnRequest, PROTOCOL_VERSION};
use semver::Version;
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::{self, BufReader},
    path::Path,
};

struct ReactNativeProvider;

impl Provider for ReactNativeProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            id: "official.react-native".into(),
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
                "preparation.dependencies".into(),
                "preparation.pods".into(),
            ],
            plugin_state_version: 1,
        }
    }

    fn action(&self, name: String, input: Value) -> Result<Value, PluginError> {
        let request: PrepareRequest = serde_json::from_value(input)
            .map_err(|_| PluginError::InvalidCall("invalid preparation input".into()))?;
        let worktree = Path::new(&request.worktree).canonicalize().map_err(|_| {
            PluginError::RequestFailed("PREPARATION_INVALID: worktree does not exist".into())
        })?;
        if !worktree.is_dir() || request.timeout_seconds == 0 {
            return Err(PluginError::InvalidCall(
                "preparation requires a worktree and nonzero deadline".into(),
            ));
        }
        let argv = match name.as_str() {
            "preparation.dependencies" => {
                require_file(&worktree.join("package-lock.json"), "package-lock.json")?;
                reject_symlink(&worktree.join("node_modules"))?;
                vec![npm()?, "ci".into()]
            }
            "preparation.pods" => {
                let ios = worktree.join("ios");
                require_file(&ios.join("Podfile"), "ios/Podfile")?;
                reject_symlink(&ios.join("Pods"))?;
                vec![pod()?, "install".into(), "--deployment".into()]
            }
            _ => return Err(PluginError::UnsupportedMethod(name)),
        };
        let cwd = if argv.len() == 3 {
            worktree.join("ios")
        } else {
            worktree
        };
        serde_json::to_value(HostSpawnRequest {
            argv,
            cwd: cwd.display().to_string(),
            environment: BTreeMap::from([(
                "PATH".into(),
                "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin".into(),
            )]),
        })
        .map_err(|error| PluginError::RequestFailed(error.to_string()))
    }
}

#[derive(Deserialize)]
struct PrepareRequest {
    worktree: String,
    timeout_seconds: u64,
}

fn require_file(path: &Path, name: &str) -> Result<(), PluginError> {
    path.is_file().then_some(()).ok_or_else(|| {
        PluginError::RequestFailed(format!("PREPARATION_INVALID: {name} is required"))
    })
}
fn reject_symlink(path: &Path) -> Result<(), PluginError> {
    if path.exists()
        && path
            .symlink_metadata()
            .map_err(|error| PluginError::RequestFailed(error.to_string()))?
            .file_type()
            .is_symlink()
    {
        return Err(PluginError::RequestFailed(
            "PREPARATION_INVALID: mutable dependency output is a symlink".into(),
        ));
    }
    Ok(())
}
fn npm() -> Result<String, PluginError> {
    [
        "/opt/homebrew/bin/npm",
        "/usr/local/bin/npm",
        "/usr/bin/npm",
    ]
    .into_iter()
    .find(|path| Path::new(path).is_file())
    .map(str::to_owned)
    .ok_or_else(|| PluginError::RequestFailed("CAPABILITY_UNSUPPORTED: npm is unavailable".into()))
}
fn pod() -> Result<String, PluginError> {
    ["/opt/homebrew/bin/pod", "/usr/local/bin/pod"]
        .into_iter()
        .find(|path| Path::new(path).is_file())
        .map(str::to_owned)
        .ok_or_else(|| {
            PluginError::RequestFailed("CAPABILITY_UNSUPPORTED: CocoaPods is unavailable".into())
        })
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
        println!(
            "{}",
            serde_json::to_string(&ReactNativeProvider.descriptor())?
        );
        return Ok(());
    }
    if args.serve {
        let stdin = io::stdin();
        let mut reader = BufReader::new(stdin.lock());
        let stdout = io::stdout();
        let mut writer = stdout.lock();
        return Ok(serve_once(&ReactNativeProvider, &mut reader, &mut writer)?);
    }
    anyhow::bail!("React Native plugin requires --descriptor, --serve, or --smoke")
}
fn current_platform() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}
