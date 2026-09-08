use clap::Parser;
use devfleet_plugin_sdk::{PluginError, Provider, ProviderDescriptor, serve_once};
use devfleet_protocol::{HostSpawnRequest, PROTOCOL_VERSION, ReactNativeIosRunRequest};
use semver::Version;
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::{self, BufReader},
    path::{Path, PathBuf},
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
                "app.run_ios".into(),
            ],
            plugin_state_version: 1,
        }
    }

    fn action(&self, name: String, input: Value) -> Result<Value, PluginError> {
        if name == "app.run_ios" {
            let request: ReactNativeIosRunRequest = serde_json::from_value(input)
                .map_err(|_| PluginError::InvalidCall("invalid app.run_ios input".into()))?;
            return ios_run(request);
        }
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
        let (argv, cwd) = match name.as_str() {
            "preparation.dependencies" => {
                require_file(&worktree.join("package-lock.json"), "package-lock.json")?;
                reject_symlink(&worktree.join("node_modules"))?;
                (vec![npm()?, "ci".into()], worktree.clone())
            }
            "preparation.pods" => {
                let ios = worktree.join("ios");
                require_file(&ios.join("Podfile"), "ios/Podfile")?;
                reject_symlink(&ios.join("Pods"))?;
                (vec![pod()?, "install".into(), "--deployment".into()], ios)
            }
            _ => return Err(PluginError::UnsupportedMethod(name)),
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

fn ios_run(request: ReactNativeIosRunRequest) -> Result<Value, PluginError> {
    let worktree = canonical_directory(Path::new(&request.worktree), "worktree")?;
    let derived_data = canonical_directory(Path::new(&request.derived_data), "derived data")?;
    if derived_data.starts_with(&worktree)
        || request.metro_port == 0
        || !valid_udid(&request.udid)
        || !valid_bundle_id(&request.bundle_id)
        || request
            .scheme
            .as_deref()
            .is_some_and(|scheme| !valid_scheme(scheme))
    {
        return Err(PluginError::InvalidCall(
            "app.run_ios requires resolved resources and scoped build configuration".into(),
        ));
    }
    reject_symlink(&worktree.join("node_modules"))?;
    let cli = worktree.join("node_modules/react-native/cli.js");
    if !cli.is_file() {
        return Err(PluginError::RequestFailed(
            "PREPARATION_INVALID: React Native Community CLI is required in this worktree".into(),
        ));
    }
    reject_expo(&worktree)?;
    let mut argv = vec![
        node()?,
        cli.display().to_string(),
        "run-ios".into(),
        "--udid".into(),
        request.udid,
        "--port".into(),
        request.metro_port.to_string(),
        "--no-packager".into(),
        "--buildFolder".into(),
        derived_data.display().to_string(),
    ];
    if let Some(scheme) = request.scheme {
        argv.extend(["--scheme".into(), scheme]);
    }
    serde_json::to_value(HostSpawnRequest {
        argv,
        cwd: worktree.display().to_string(),
        environment: trusted_environment(),
    })
    .map_err(|error| PluginError::RequestFailed(error.to_string()))
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
    match path.symlink_metadata() {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(PluginError::RequestFailed(
                "PREPARATION_INVALID: mutable dependency output is a symlink".into(),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(PluginError::RequestFailed(error.to_string())),
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

fn node() -> Result<String, PluginError> {
    [
        "/opt/homebrew/bin/node",
        "/usr/local/bin/node",
        "/usr/bin/node",
    ]
    .into_iter()
    .find(|path| Path::new(path).is_file())
    .map(str::to_owned)
    .ok_or_else(|| PluginError::RequestFailed("CAPABILITY_UNSUPPORTED: Node is unavailable".into()))
}

fn trusted_environment() -> BTreeMap<String, String> {
    BTreeMap::from([(
        "PATH".into(),
        "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin".into(),
    )])
}

fn canonical_directory(path: &Path, name: &str) -> Result<PathBuf, PluginError> {
    let path = path.canonicalize().map_err(|_| {
        PluginError::RequestFailed(format!("PREPARATION_INVALID: {name} does not exist"))
    })?;
    path.is_dir().then_some(path).ok_or_else(|| {
        PluginError::RequestFailed(format!("PREPARATION_INVALID: {name} is not a directory"))
    })
}

fn valid_udid(udid: &str) -> bool {
    uuid::Uuid::parse_str(udid).is_ok()
}

fn valid_bundle_id(bundle_id: &str) -> bool {
    bundle_id.split('.').count() >= 2
        && bundle_id.split('.').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn valid_scheme(scheme: &str) -> bool {
    !scheme.is_empty()
        && scheme.len() <= 128
        && scheme
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b' ' | b'_' | b'-'))
}

fn reject_expo(worktree: &Path) -> Result<(), PluginError> {
    let package = worktree.join("package.json");
    let app = worktree.join("app.json");
    for path in [package, app] {
        let Ok(contents) = std::fs::read(&path) else {
            continue;
        };
        let value: Value = serde_json::from_slice(&contents).map_err(|_| {
            PluginError::RequestFailed(format!(
                "PREPARATION_INVALID: {} is invalid JSON",
                path.display()
            ))
        })?;
        if value.get("expo").is_some()
            || ["dependencies", "devDependencies"]
                .into_iter()
                .filter_map(|key| value.get(key).and_then(Value::as_object))
                .any(|dependencies| dependencies.contains_key("expo"))
        {
            return Err(PluginError::RequestFailed(
                "CAPABILITY_UNSUPPORTED: Expo requires a verified adapter".into(),
            ));
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_a_broken_symlink_for_mutable_dependencies() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let link = temporary.path().join("node_modules");
        std::os::unix::fs::symlink("missing-target", &link).expect("broken symlink");
        assert!(reject_symlink(&link).is_err());
    }

    #[test]
    fn ios_runs_are_pinned_to_the_requested_device_and_private_build_output() {
        let first = tempfile::tempdir().expect("first worktree");
        let second = tempfile::tempdir().expect("second worktree");
        let first_build = tempfile::tempdir().expect("first build");
        let second_build = tempfile::tempdir().expect("second build");
        for worktree in [&first, &second] {
            let cli = worktree.path().join("node_modules/react-native");
            std::fs::create_dir_all(&cli).expect("CLI directory");
            std::fs::write(cli.join("cli.js"), "// fixture").expect("CLI fixture");
        }
        let first = ios_run(ReactNativeIosRunRequest {
            worktree: first.path().display().to_string(),
            udid: "0186D916-7A5C-4E31-8590-C2F502FA2288".into(),
            metro_port: 21_001,
            derived_data: first_build.path().display().to_string(),
            bundle_id: "com.example.app".into(),
            scheme: None,
        })
        .expect("first command");
        let second = ios_run(ReactNativeIosRunRequest {
            worktree: second.path().display().to_string(),
            udid: "92F9D628-769D-447E-B093-A01B9302ED0D".into(),
            metro_port: 21_065,
            derived_data: second_build.path().display().to_string(),
            bundle_id: "com.example.app".into(),
            scheme: None,
        })
        .expect("second command");
        let first: HostSpawnRequest = serde_json::from_value(first).expect("first spawn");
        let second: HostSpawnRequest = serde_json::from_value(second).expect("second spawn");
        for command in [&first, &second] {
            assert!(command.argv.contains(&"--no-packager".into()));
            assert!(command.argv.contains(&"--udid".into()));
            assert!(command.argv.contains(&"--port".into()));
        }
        assert_ne!(first.cwd, second.cwd);
        assert!(first.argv.contains(&"21001".into()));
        assert!(second.argv.contains(&"21065".into()));
        assert!(
            first.argv.contains(
                &first_build
                    .path()
                    .canonicalize()
                    .expect("first build path")
                    .display()
                    .to_string(),
            )
        );
        assert!(
            second.argv.contains(
                &second_build
                    .path()
                    .canonicalize()
                    .expect("second build path")
                    .display()
                    .to_string(),
            )
        );
    }
}
