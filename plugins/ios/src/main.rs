use clap::Parser;
use devfleet_plugin_sdk::{PluginError, Provider, ProviderDescriptor, serve_once};
use devfleet_protocol::{HostSpawnRequest, PROTOCOL_VERSION};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::{self, BufReader},
    path::Path,
    process::{Command, Output},
};
use thiserror::Error;
use uuid::Uuid;

const XCRUN: &str = "/usr/bin/xcrun";
const OPEN: &str = "/usr/bin/open";
const PATH: &str = "/usr/bin:/bin";

struct IosProvider;

impl Provider for IosProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            id: "official.ios".into(),
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
                "device.discover".into(),
                "device.create".into(),
                "device.boot".into(),
                "device.open".into(),
                "device.stop".into(),
                "app.launch".into(),
                "device.capture".into(),
                "device.record".into(),
            ],
            plugin_state_version: 1,
        }
    }

    fn action(&self, name: String, input: Value) -> Result<Value, PluginError> {
        match name.as_str() {
            "device.discover" => {
                serde_json::to_value(discover().map_err(|error| plugin_error(&error))?)
                    .map_err(|error| PluginError::RequestFailed(error.to_string()))
            }
            "device.create" => {
                let request: CreateRequest = serde_json::from_value(input)
                    .map_err(|_| PluginError::InvalidCall("invalid device.create input".into()))?;
                let discovered = discover().map_err(|error| plugin_error(&error))?;
                validate_create(&discovered, &request).map_err(|error| plugin_error(&error))?;
                spawn(vec![
                    XCRUN.into(),
                    "simctl".into(),
                    "create".into(),
                    request.name,
                    request.device_type,
                    request.runtime,
                ])
            }
            "device.boot" => {
                let request: BootRequest = serde_json::from_value(input)
                    .map_err(|_| PluginError::InvalidCall("invalid device.boot input".into()))?;
                validate_udid(&request.udid).map_err(|error| plugin_error(&error))?;
                let argv = match request.step {
                    BootStep::Start => {
                        vec![XCRUN.into(), "simctl".into(), "boot".into(), request.udid]
                    }
                    BootStep::Wait => vec![
                        XCRUN.into(),
                        "simctl".into(),
                        "bootstatus".into(),
                        request.udid,
                        "-b".into(),
                    ],
                };
                spawn(argv)
            }
            "device.open" => {
                let request: UdidRequest = serde_json::from_value(input)
                    .map_err(|_| PluginError::InvalidCall("invalid device.open input".into()))?;
                validate_udid(&request.udid).map_err(|error| plugin_error(&error))?;
                spawn(vec![
                    OPEN.into(),
                    "-a".into(),
                    "Simulator".into(),
                    "--args".into(),
                    "-CurrentDeviceUDID".into(),
                    request.udid,
                ])
            }
            "device.stop" => {
                let request: UdidRequest = serde_json::from_value(input)
                    .map_err(|_| PluginError::InvalidCall("invalid device.stop input".into()))?;
                validate_udid(&request.udid).map_err(|error| plugin_error(&error))?;
                spawn(vec![
                    XCRUN.into(),
                    "simctl".into(),
                    "shutdown".into(),
                    request.udid,
                ])
            }
            "app.launch" => {
                let request: LaunchRequest = serde_json::from_value(input)
                    .map_err(|_| PluginError::InvalidCall("invalid app.launch input".into()))?;
                validate_udid(&request.udid).map_err(|error| plugin_error(&error))?;
                validate_bundle_id(&request.bundle_id).map_err(|error| plugin_error(&error))?;
                spawn(vec![
                    XCRUN.into(),
                    "simctl".into(),
                    "launch".into(),
                    request.udid,
                    request.bundle_id,
                ])
            }
            "device.capture" => media_action(input, "device.capture", "png", false),
            "device.record" => media_action(input, "device.record", "mp4", true),
            _ => Err(PluginError::UnsupportedMethod(name)),
        }
    }
}

fn spawn(argv: Vec<String>) -> Result<Value, PluginError> {
    serde_json::to_value(HostSpawnRequest {
        argv,
        cwd: "/".into(),
        environment: command_environment(),
    })
    .map_err(|error| PluginError::RequestFailed(error.to_string()))
}

fn media_action(
    input: Value,
    action: &str,
    extension: &str,
    recording: bool,
) -> Result<Value, PluginError> {
    let request: MediaRequest = serde_json::from_value(input)
        .map_err(|_| PluginError::InvalidCall(format!("invalid {action} input")))?;
    validate_udid(&request.udid).map_err(|error| plugin_error(&error))?;
    validate_artifact_path(&request.path, &request.artifact_root, extension)
        .map_err(|error| plugin_error(&error))?;
    let mut argv = vec![XCRUN.into(), "simctl".into(), "io".into(), request.udid];
    if recording {
        argv.extend(["recordVideo".into(), "--codec".into(), "h264".into()]);
    } else {
        argv.push("screenshot".into());
    }
    argv.push(request.path);
    spawn(argv)
}

#[derive(Clone, Debug, Deserialize)]
struct CreateRequest {
    runtime: String,
    device_type: String,
    name: String,
}

#[derive(Clone, Debug, Deserialize)]
struct UdidRequest {
    udid: String,
}

#[derive(Clone, Debug, Deserialize)]
struct BootRequest {
    udid: String,
    step: BootStep,
}

#[derive(Clone, Debug, Deserialize)]
struct LaunchRequest {
    udid: String,
    bundle_id: String,
}

#[derive(Clone, Debug, Deserialize)]
struct MediaRequest {
    udid: String,
    artifact_root: String,
    path: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BootStep {
    Start,
    Wait,
}

#[derive(Clone, Debug, Serialize)]
struct Discovery {
    runtimes: Vec<Runtime>,
    device_types: Vec<DeviceType>,
    devices: Vec<Device>,
}

#[derive(Clone, Debug, Serialize)]
struct Runtime {
    identifier: String,
    available: bool,
}

#[derive(Clone, Debug, Serialize)]
struct DeviceType {
    identifier: String,
    available: bool,
}

#[derive(Clone, Debug, Serialize)]
struct Device {
    udid: String,
    name: String,
    runtime: String,
    available: bool,
    state: String,
}

#[derive(Debug, Error)]
enum IosError {
    #[error("CAPABILITY_UNSUPPORTED: Xcode CoreSimulator is unavailable")]
    Unavailable,
    #[error("DEVICE_UNAVAILABLE: requested runtime '{0}' is not installed and available")]
    RuntimeUnavailable(String),
    #[error("DEVICE_UNAVAILABLE: requested device type '{0}' is not installed and available")]
    DeviceTypeUnavailable(String),
    #[error("DEVICE_UNAVAILABLE: malformed simulator UDID")]
    InvalidUdid,
    #[error("DEVICE_UNAVAILABLE: malformed bundle identifier")]
    InvalidBundleId,
    #[error("DEVICE_UNAVAILABLE: artifact path is not scoped to an existing private directory")]
    InvalidArtifactPath,
    #[error("DEVICE_UNAVAILABLE: simulator discovery returned malformed JSON")]
    InvalidDiscovery,
}

fn plugin_error(error: &IosError) -> PluginError {
    PluginError::RequestFailed(error.to_string())
}

fn discover() -> Result<Discovery, IosError> {
    let output = simctl_output()?;
    if !output.status.success() {
        return Err(IosError::Unavailable);
    }
    let value: Value =
        serde_json::from_slice(&output.stdout).map_err(|_| IosError::InvalidDiscovery)?;
    discovery_from_json(&value)
}

fn simctl_output() -> Result<Output, IosError> {
    let mut command = Command::new(XCRUN);
    command
        .args([
            "simctl",
            "list",
            "--json",
            "runtimes",
            "devicetypes",
            "devices",
        ])
        .env_clear()
        .env("PATH", PATH);
    if let Some(home) = std::env::var_os("HOME") {
        command.env("HOME", home);
    }
    command.output().map_err(|_| IosError::Unavailable)
}

fn discovery_from_json(value: &Value) -> Result<Discovery, IosError> {
    let runtimes = value
        .get("runtimes")
        .and_then(Value::as_array)
        .ok_or(IosError::InvalidDiscovery)?
        .iter()
        .filter_map(|runtime| {
            Some(Runtime {
                identifier: runtime.get("identifier")?.as_str()?.into(),
                available: available(runtime),
            })
        })
        .collect();
    let device_types = value
        .get("devicetypes")
        .and_then(Value::as_array)
        .ok_or(IosError::InvalidDiscovery)?
        .iter()
        .filter_map(|device_type| {
            Some(DeviceType {
                identifier: device_type.get("identifier")?.as_str()?.into(),
                available: available(device_type),
            })
        })
        .collect();
    let mut devices = Vec::new();
    for (runtime, entries) in value
        .get("devices")
        .and_then(Value::as_object)
        .ok_or(IosError::InvalidDiscovery)?
    {
        for entry in entries.as_array().ok_or(IosError::InvalidDiscovery)? {
            let Some(udid) = entry.get("udid").and_then(Value::as_str) else {
                continue;
            };
            let Some(name) = entry.get("name").and_then(Value::as_str) else {
                continue;
            };
            let state = entry
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("Unknown");
            devices.push(Device {
                udid: udid.into(),
                name: name.into(),
                runtime: runtime.clone(),
                available: available(entry),
                state: state.into(),
            });
        }
    }
    Ok(Discovery {
        runtimes,
        device_types,
        devices,
    })
}

fn available(value: &Value) -> bool {
    value
        .get("isAvailable")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| {
            !value
                .get("availability")
                .and_then(Value::as_str)
                .is_some_and(|item| item.contains("unavailable"))
        })
}

fn validate_create(discovery: &Discovery, request: &CreateRequest) -> Result<(), IosError> {
    if !discovery
        .runtimes
        .iter()
        .any(|runtime| runtime.identifier == request.runtime && runtime.available)
    {
        return Err(IosError::RuntimeUnavailable(request.runtime.clone()));
    }
    if !discovery
        .device_types
        .iter()
        .any(|device_type| device_type.identifier == request.device_type && device_type.available)
    {
        return Err(IosError::DeviceTypeUnavailable(request.device_type.clone()));
    }
    if !request.name.starts_with("DevFleet-") || request.name.len() > 128 {
        return Err(IosError::InvalidDiscovery);
    }
    Ok(())
}

fn validate_udid(udid: &str) -> Result<(), IosError> {
    Uuid::parse_str(udid)
        .map(|_| ())
        .map_err(|_| IosError::InvalidUdid)
}

fn validate_bundle_id(bundle_id: &str) -> Result<(), IosError> {
    (bundle_id.split('.').count() >= 2
        && bundle_id.split('.').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        }))
    .then_some(())
    .ok_or(IosError::InvalidBundleId)
}

fn validate_artifact_path(
    path: &str,
    artifact_root: &str,
    extension: &str,
) -> Result<(), IosError> {
    let path = Path::new(path);
    let parent = path.parent().ok_or(IosError::InvalidArtifactPath)?;
    let artifact_root = Path::new(artifact_root)
        .canonicalize()
        .map_err(|_| IosError::InvalidArtifactPath)?;
    let parent = parent
        .canonicalize()
        .map_err(|_| IosError::InvalidArtifactPath)?;
    if !parent.starts_with(artifact_root)
        || path.extension().and_then(|value| value.to_str()) != Some(extension)
        || path.file_name().is_none()
        || path.exists()
        || path
            .symlink_metadata()
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
    {
        return Err(IosError::InvalidArtifactPath);
    }
    Ok(())
}

fn command_environment() -> BTreeMap<String, String> {
    let mut environment = BTreeMap::from([("PATH".into(), PATH.into())]);
    if let Some(home) = std::env::var_os("HOME") {
        environment.insert("HOME".into(), home.to_string_lossy().into_owned());
    }
    environment
}

#[derive(Parser)]
#[command(about = "DevFleet official iOS simulator provider")]
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
        println!("{}", serde_json::to_string(&IosProvider.descriptor())?);
        return Ok(());
    }
    if args.serve {
        let stdin = io::stdin();
        let mut reader = BufReader::new(stdin.lock());
        let stdout = io::stdout();
        let mut writer = stdout.lock();
        return Ok(serve_once(&IosProvider, &mut reader, &mut writer)?);
    }
    anyhow::bail!("iOS plugin requires --descriptor, --serve, or --smoke")
}

fn current_platform() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_available_pinned_resources_without_name_fallbacks() {
        let fixture = json!({
            "runtimes": [{"identifier": "runtime-18", "isAvailable": true}],
            "devicetypes": [{"identifier": "phone-16", "isAvailable": true}],
            "devices": {"runtime-18": [{"udid": "0186D916-7A5C-4E31-8590-C2F502FA2288", "name": "Personal", "isAvailable": true, "state": "Booted"}]}
        });
        let discovery = discovery_from_json(&fixture).expect("fixture parses");
        validate_create(
            &discovery,
            &CreateRequest {
                runtime: "runtime-18".into(),
                device_type: "phone-16".into(),
                name: "DevFleet-019".into(),
            },
        )
        .expect("pinned selection");
        assert!(matches!(
            validate_create(
                &discovery,
                &CreateRequest {
                    runtime: "runtime-19".into(),
                    device_type: "phone-16".into(),
                    name: "DevFleet-019".into()
                }
            ),
            Err(IosError::RuntimeUnavailable(_))
        ));
    }

    #[test]
    fn boot_and_open_actions_require_exact_udids() {
        assert!(validate_udid("not-a-device").is_err());
        let result = IosProvider.action("device.open".into(), json!({"udid": "not-a-device"}));
        assert!(result.is_err());
    }

    #[test]
    fn app_launch_requires_an_exact_device_and_bundle_identifier() {
        assert!(
            IosProvider
                .action(
                    "app.launch".into(),
                    json!({"udid": "0186D916-7A5C-4E31-8590-C2F502FA2288", "bundle_id": "app"})
                )
                .is_err()
        );
    }

    #[test]
    fn capture_action_targets_only_the_requested_udid_and_artifact_path() {
        let temporary = tempfile::tempdir().expect("artifact directory");
        let path = temporary.path().join("capture.png");
        let value = IosProvider
            .action(
                "device.capture".into(),
                json!({
                    "udid": "0186D916-7A5C-4E31-8590-C2F502FA2288",
                    "artifact_root": temporary.path(),
                    "path": path,
                }),
            )
            .expect("capture command");
        let command: HostSpawnRequest = serde_json::from_value(value).expect("capture spawn");
        assert!(command.argv.contains(&"screenshot".into()));
        assert!(
            command
                .argv
                .contains(&"0186D916-7A5C-4E31-8590-C2F502FA2288".into())
        );
        assert!(command.argv.contains(&path.display().to_string()));
    }

    #[test]
    fn capture_commands_keep_two_devices_and_artifacts_separate() {
        let temporary = tempfile::tempdir().expect("artifact directory");
        let first_path = temporary.path().join("first.png");
        let second_path = temporary.path().join("second.png");
        let first = IosProvider
            .action(
                "device.capture".into(),
                json!({
                    "udid": "0186D916-7A5C-4E31-8590-C2F502FA2288",
                    "artifact_root": temporary.path(),
                    "path": first_path,
                }),
            )
            .expect("first capture");
        let second = IosProvider
            .action(
                "device.capture".into(),
                json!({
                    "udid": "06A9C97E-D6BE-4492-AD9B-857CB949C50E",
                    "artifact_root": temporary.path(),
                    "path": second_path,
                }),
            )
            .expect("second capture");
        let first: HostSpawnRequest = serde_json::from_value(first).expect("first command");
        let second: HostSpawnRequest = serde_json::from_value(second).expect("second command");
        assert_ne!(first.argv, second.argv);
        assert!(
            first
                .argv
                .contains(&"0186D916-7A5C-4E31-8590-C2F502FA2288".into())
        );
        assert!(
            second
                .argv
                .contains(&"06A9C97E-D6BE-4492-AD9B-857CB949C50E".into())
        );
        assert!(first.argv.contains(&first_path.display().to_string()));
        assert!(second.argv.contains(&second_path.display().to_string()));
    }
}
