use clap::Parser;
use devfleet_plugin_sdk::{PluginError, Provider, ProviderDescriptor, serve_once};
use devfleet_protocol::{
    ComposeAuthorizations, ComposeDestroyRequest, ComposeReconcileRequest, HostSpawnRequest,
    PROTOCOL_VERSION,
};
use semver::Version;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    io::{self, BufReader},
    path::{Path, PathBuf},
    process::{Command, Output},
};
use thiserror::Error;

const DOCKER_PATH: &str = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin";

struct ComposeProvider;

impl Provider for ComposeProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            id: "official.compose".into(),
            version: Version::new(0, 1, 0),
            platform: current_platform(),
            protocol: PROTOCOL_VERSION,
            capabilities: vec![
                "lifecycle.initialize".into(),
                "lifecycle.validate".into(),
                "lifecycle.plan".into(),
                "lifecycle.inspect".into(),
                "compose.reconcile".into(),
                "compose.destroy".into(),
                "lifecycle.stop".into(),
                "lifecycle.destroy".into(),
                "lifecycle.action".into(),
                "lifecycle.cancel".into(),
            ],
            plugin_state_version: 1,
        }
    }

    fn reconcile_compose(&self, request: ComposeReconcileRequest) -> Result<Value, PluginError> {
        if !PROTOCOL_VERSION.compatible_with(request.protocol) {
            return Err(PluginError::IncompatibleProtocol);
        }
        validate_request(&request).map_err(|error| plugin_error(&error))?;
        ensure_local_docker_context(&request).map_err(|error| plugin_error(&error))?;
        let model = effective_model(&request).map_err(|error| plugin_error(&error))?;
        validate_model(&model, &request).map_err(|error| plugin_error(&error))?;
        serde_json::to_value(HostSpawnRequest {
            argv: compose_spawn_args(&request).map_err(|error| plugin_error(&error))?,
            cwd: request.worktree,
            environment: compose_environment(request.environment),
        })
        .map_err(|error| PluginError::RequestFailed(error.to_string()))
    }

    fn action(&self, name: String, input: Value) -> Result<Value, PluginError> {
        if name != "compose.destroy" {
            return Err(PluginError::UnsupportedMethod(name));
        }
        let request: ComposeDestroyRequest = serde_json::from_value(input)
            .map_err(|_| PluginError::InvalidCall("invalid compose.destroy input".into()))?;
        validate_destroy_request(&request).map_err(|error| plugin_error(&error))?;
        serde_json::to_value(HostSpawnRequest {
            argv: compose_destroy_args(&request).map_err(|error| plugin_error(&error))?,
            cwd: request.worktree,
            environment: compose_environment(BTreeMap::new()),
        })
        .map_err(|error| PluginError::RequestFailed(error.to_string()))
    }
}

#[derive(Debug, Error)]
enum ComposeError {
    #[error("compose configuration is invalid: {0}")]
    Invalid(String),
    #[error("compose configuration requires explicit authorization for {0}")]
    Unsafe(String),
    #[error("Docker context is remote; v0.1 requires a local Unix-socket Docker context")]
    RemoteContext,
    #[error("Docker command failed while validating the effective Compose model")]
    DockerFailed,
    #[error("Docker returned an invalid effective Compose model")]
    InvalidModel,
    #[error("Docker tool is unavailable")]
    DockerUnavailable,
}

fn plugin_error(error: &ComposeError) -> PluginError {
    PluginError::InvalidCall(error.to_string())
}

fn validate_request(request: &ComposeReconcileRequest) -> Result<(), ComposeError> {
    if request.project_name.is_empty()
        || request.project_name.len() > 63
        || !request
            .project_name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(ComposeError::Invalid(
            "project name must be a short lowercase Compose namespace".into(),
        ));
    }
    let worktree = Path::new(&request.worktree)
        .canonicalize()
        .map_err(|_| ComposeError::Invalid("worktree must exist".into()))?;
    if request.files.is_empty() {
        return Err(ComposeError::Invalid(
            "at least one Compose file is required".into(),
        ));
    }
    let mut files = BTreeSet::new();
    for file in &request.files {
        let file = Path::new(file)
            .canonicalize()
            .map_err(|_| ComposeError::Invalid("Compose files must exist".into()))?;
        if !file.starts_with(&worktree) || !files.insert(file) {
            return Err(ComposeError::Invalid(
                "Compose files must be unique and inside the current worktree".into(),
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
            return Err(ComposeError::Invalid(
                "endpoints require unique names, nonzero ports, and a slash-prefixed readiness path"
                    .into(),
            ));
        }
    }
    validate_authorizations(&request.authorizations)
}

fn validate_authorizations(authorizations: &ComposeAuthorizations) -> Result<(), ComposeError> {
    for authorization in [
        &authorizations.global_container_names,
        &authorizations.host_networking,
        &authorizations.external_resources,
        &authorizations.global_volume_names,
        &authorizations.unmanaged_ports,
        &authorizations.shared_mutable_binds,
    ] {
        if authorization
            .as_ref()
            .is_some_and(|reason| reason.trim().is_empty())
        {
            return Err(ComposeError::Invalid(
                "an unsafe-resource authorization must describe its scope".into(),
            ));
        }
    }
    Ok(())
}

fn ensure_local_docker_context(request: &ComposeReconcileRequest) -> Result<(), ComposeError> {
    let context = docker_output(request, ["context", "show"])?;
    if !context.status.success() {
        return Err(ComposeError::DockerFailed);
    }
    let context = String::from_utf8(context.stdout).map_err(|_| ComposeError::DockerFailed)?;
    let name = context.trim();
    if name.is_empty() {
        return Err(ComposeError::DockerFailed);
    }
    let inspected = docker_output(
        request,
        [
            "context",
            "inspect",
            name,
            "--format",
            "{{.Endpoints.docker.Host}}",
        ],
    )?;
    if !inspected.status.success() {
        return Err(ComposeError::DockerFailed);
    }
    let host = String::from_utf8(inspected.stdout).map_err(|_| ComposeError::DockerFailed)?;
    if host.trim().starts_with("unix://") {
        Ok(())
    } else {
        Err(ComposeError::RemoteContext)
    }
}

fn effective_model(request: &ComposeReconcileRequest) -> Result<Value, ComposeError> {
    let output = docker_output_with_args(request, compose_args(request, "config"))?;
    if !output.status.success() {
        return Err(ComposeError::DockerFailed);
    }
    serde_json::from_slice(&output.stdout).map_err(|_| ComposeError::InvalidModel)
}

fn docker_output<const N: usize>(
    request: &ComposeReconcileRequest,
    args: [&str; N],
) -> Result<Output, ComposeError> {
    docker_output_with_args(request, args.into_iter().map(OsString::from).collect())
}

fn docker_output_with_args(
    request: &ComposeReconcileRequest,
    args: Vec<OsString>,
) -> Result<Output, ComposeError> {
    let mut command = Command::new("docker");
    command
        .args(args)
        .current_dir(&request.worktree)
        .env_clear()
        .envs(&request.environment)
        .env("PATH", DOCKER_PATH);
    if let Some(home) = std::env::var_os("HOME") {
        command.env("HOME", home);
    }
    command.output().map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => ComposeError::DockerUnavailable,
        _ => ComposeError::DockerFailed,
    })
}

fn compose_environment(mut environment: BTreeMap<String, String>) -> BTreeMap<String, String> {
    environment.insert("PATH".into(), DOCKER_PATH.into());
    if let Some(home) = std::env::var_os("HOME") {
        environment.insert("HOME".into(), home.to_string_lossy().into_owned());
    }
    environment
}

fn compose_args(request: &ComposeReconcileRequest, command: &str) -> Vec<OsString> {
    let mut args = vec![
        OsString::from("compose"),
        OsString::from("--project-name"),
        OsString::from(&request.project_name),
        OsString::from("--project-directory"),
        OsString::from(&request.worktree),
    ];
    for file in &request.files {
        args.push(OsString::from("--file"));
        args.push(OsString::from(file));
    }
    args.push(OsString::from(command));
    if command == "config" {
        args.push(OsString::from("--format"));
        args.push(OsString::from("json"));
    } else {
        args.push(OsString::from("--remove-orphans"));
    }
    args
}

fn compose_spawn_args(request: &ComposeReconcileRequest) -> Result<Vec<String>, ComposeError> {
    let mut args = compose_args(request, "up");
    args.insert(0, docker_executable()?);
    args.into_iter()
        .map(OsString::into_string)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ComposeError::DockerUnavailable)
}

fn compose_destroy_args(request: &ComposeDestroyRequest) -> Result<Vec<String>, ComposeError> {
    let mut args = compose_destroy_command_args(request);
    args.insert(0, docker_executable()?);
    args.into_iter()
        .map(OsString::into_string)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ComposeError::DockerUnavailable)
}

fn compose_destroy_command_args(request: &ComposeDestroyRequest) -> Vec<OsString> {
    let mut args = vec![
        OsString::from("compose"),
        OsString::from("--project-name"),
        OsString::from(&request.project_name),
        OsString::from("--project-directory"),
        OsString::from(&request.worktree),
    ];
    for file in &request.files {
        args.push(OsString::from("--file"));
        args.push(OsString::from(file));
    }
    args.extend([OsString::from("down"), OsString::from("--remove-orphans")]);
    if request.purge_data {
        args.push(OsString::from("--volumes"));
    }
    args
}

fn validate_destroy_request(request: &ComposeDestroyRequest) -> Result<(), ComposeError> {
    let name = request.project_name.split('_').collect::<Vec<_>>();
    if !matches!(name.as_slice(), ["df", project, environment]
        if project.len() == 8
            && environment.len() == 8
            && project.bytes().all(|byte| byte.is_ascii_hexdigit())
            && environment.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return Err(ComposeError::Invalid(
            "invalid DevFleet Compose project name".into(),
        ));
    }
    let worktree = Path::new(&request.worktree)
        .canonicalize()
        .map_err(|_| ComposeError::Invalid("worktree must exist".into()))?;
    if request.files.is_empty() {
        return Err(ComposeError::Invalid(
            "Compose ownership has no files".into(),
        ));
    }
    for file in &request.files {
        let file = Path::new(file)
            .canonicalize()
            .map_err(|_| ComposeError::Invalid("Compose file no longer exists".into()))?;
        if !file.is_file() || !file.starts_with(&worktree) {
            return Err(ComposeError::Invalid(
                "Compose file escaped the owned worktree".into(),
            ));
        }
    }
    Ok(())
}

fn docker_executable() -> Result<OsString, ComposeError> {
    DOCKER_PATH
        .split(':')
        .map(|directory| Path::new(directory).join("docker"))
        .find(|path| path.is_file())
        .map(PathBuf::into_os_string)
        .ok_or(ComposeError::DockerUnavailable)
}

fn validate_model(model: &Value, request: &ComposeReconcileRequest) -> Result<(), ComposeError> {
    let services = model
        .get("services")
        .and_then(Value::as_object)
        .filter(|services| !services.is_empty())
        .ok_or(ComposeError::InvalidModel)?;
    let worktree = Path::new(&request.worktree)
        .canonicalize()
        .map_err(|_| ComposeError::Invalid("worktree must exist".into()))?;
    let mut actual_ports = BTreeSet::new();
    for (name, service) in services {
        let service = service.as_object().ok_or(ComposeError::InvalidModel)?;
        if service.contains_key("container_name") {
            require(
                request.authorizations.global_container_names.as_ref(),
                "global container names",
            )?;
        }
        if service
            .get("network_mode")
            .and_then(Value::as_str)
            .is_some_and(|mode| mode == "host")
        {
            require(
                request.authorizations.host_networking.as_ref(),
                "host networking",
            )?;
        }
        validate_built_image(service, &request.project_name)?;
        validate_binds(service, &worktree, &request.authorizations)?;
        for port in service
            .get("ports")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let port = parse_port(port).ok_or(ComposeError::InvalidModel)?;
            if !is_loopback(&port.host_ip) {
                return Err(ComposeError::Invalid(
                    "published Compose ports must bind to loopback".into(),
                ));
            }
            actual_ports.insert((name.as_str(), port.target, port.published));
        }
    }
    validate_global_resources(model, request)?;
    let expected_ports = request
        .endpoints
        .iter()
        .map(|endpoint| {
            (
                endpoint.service.as_str(),
                endpoint.container_port,
                endpoint.host_port,
            )
        })
        .collect::<BTreeSet<_>>();
    if actual_ports != expected_ports {
        require(
            request.authorizations.unmanaged_ports.as_ref(),
            "unmanaged or conflicting port publications",
        )?;
    }
    Ok(())
}

fn validate_built_image(
    service: &serde_json::Map<String, Value>,
    project_name: &str,
) -> Result<(), ComposeError> {
    if service.contains_key("build")
        && let Some(image) = service.get("image").and_then(Value::as_str)
        && !image.contains("@sha256:")
        && !image.contains(project_name)
    {
        return Err(ComposeError::Invalid(
            "built services require an environment-specific image tag or immutable digest".into(),
        ));
    }
    Ok(())
}

fn validate_global_resources(
    model: &Value,
    request: &ComposeReconcileRequest,
) -> Result<(), ComposeError> {
    for (kind, authorization) in [
        ("networks", &request.authorizations.external_resources),
        ("volumes", &request.authorizations.external_resources),
    ] {
        let resources = model
            .get(kind)
            .and_then(Value::as_object)
            .into_iter()
            .flatten();
        for resource in resources {
            let resource = resource.1.as_object().ok_or(ComposeError::InvalidModel)?;
            if resource.get("external").and_then(Value::as_bool) == Some(true) {
                require(authorization.as_ref(), "shared external resources")?;
            }
            if kind == "volumes"
                && resource
                    .get("name")
                    .and_then(Value::as_str)
                    .is_some_and(|name| !name.contains(&request.project_name))
            {
                require(
                    request.authorizations.global_volume_names.as_ref(),
                    "global volume names",
                )?;
            }
            if kind == "networks"
                && resource
                    .get("name")
                    .and_then(Value::as_str)
                    .is_some_and(|name| !name.contains(&request.project_name))
            {
                require(authorization.as_ref(), "shared external resources")?;
            }
        }
    }
    Ok(())
}

fn validate_binds(
    service: &serde_json::Map<String, Value>,
    worktree: &Path,
    authorizations: &ComposeAuthorizations,
) -> Result<(), ComposeError> {
    for mount in service
        .get("volumes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let (source, read_only) = match mount {
            Value::String(mount) => parse_short_bind(mount),
            Value::Object(mount) if mount.get("type").and_then(Value::as_str) == Some("bind") => (
                mount.get("source").and_then(Value::as_str),
                mount
                    .get("read_only")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            ),
            _ => (None, true),
        };
        if let Some(source) = source
            && !read_only
            && !source_is_within_worktree(source, worktree)
        {
            require(
                authorizations.shared_mutable_binds.as_ref(),
                "shared mutable data binds",
            )?;
        }
    }
    Ok(())
}

fn parse_short_bind(mount: &str) -> (Option<&str>, bool) {
    let mut fields = mount.rsplitn(3, ':').collect::<Vec<_>>();
    fields.reverse();
    if fields.len() < 2 || (!fields[0].starts_with('.') && !fields[0].starts_with('/')) {
        return (None, true);
    }
    (
        Some(fields[0]),
        fields.last().is_some_and(|field| *field == "ro"),
    )
}

fn source_is_within_worktree(source: &str, worktree: &Path) -> bool {
    let source = PathBuf::from(source);
    let source = if source.is_absolute() {
        source
    } else {
        worktree.join(source)
    };
    source
        .canonicalize()
        .is_ok_and(|source| source.starts_with(worktree))
}

fn require(authorization: Option<&String>, resource: &str) -> Result<(), ComposeError> {
    if authorization.is_some_and(|reason| !reason.trim().is_empty()) {
        Ok(())
    } else {
        Err(ComposeError::Unsafe(resource.into()))
    }
}

struct PublishedPort {
    host_ip: String,
    published: u16,
    target: u16,
}

fn parse_port(value: &Value) -> Option<PublishedPort> {
    match value {
        Value::String(port) => parse_short_port(port),
        Value::Object(port) => Some(PublishedPort {
            host_ip: port.get("host_ip")?.as_str()?.into(),
            published: port_number(port.get("published")?)?,
            target: port_number(port.get("target")?)?,
        }),
        _ => None,
    }
}

fn parse_short_port(port: &str) -> Option<PublishedPort> {
    let port = port.split('/').next()?;
    if let Some(port) = port.strip_prefix("[::1]:") {
        let (published, target) = port.split_once(':')?;
        return Some(PublishedPort {
            host_ip: "::1".into(),
            published: published.parse().ok()?,
            target: target.parse().ok()?,
        });
    }
    let fields = port.split(':').collect::<Vec<_>>();
    let (host_ip, published, target) = match fields.as_slice() {
        [published, target] => ("", *published, *target),
        [host_ip, published, target] => (*host_ip, *published, *target),
        _ => return None,
    };
    Some(PublishedPort {
        host_ip: host_ip.into(),
        published: published.parse().ok()?,
        target: target.parse().ok()?,
    })
}

fn port_number(value: &Value) -> Option<u16> {
    value
        .as_u64()
        .and_then(|value| u16::try_from(value).ok())
        .or_else(|| value.as_str()?.parse().ok())
}

fn is_loopback(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "::1" | "localhost")
}

#[derive(Parser)]
#[command(about = "DevFleet official Docker Compose provider")]
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
        println!("{}", serde_json::to_string(&ComposeProvider.descriptor())?);
        return Ok(());
    }
    if args.serve {
        let stdin = io::stdin();
        let mut reader = BufReader::new(stdin.lock());
        let stdout = io::stdout();
        let mut writer = stdout.lock();
        return Ok(serve_once(&ComposeProvider, &mut reader, &mut writer)?);
    }
    anyhow::bail!("compose plugin requires --descriptor, --serve, or --smoke")
}

fn current_platform() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

#[cfg(test)]
mod tests {
    use super::*;
    use devfleet_protocol::ComposeEndpoint;
    use std::collections::BTreeMap;
    use tempfile::TempDir;
    use uuid::Uuid;

    #[test]
    fn accepts_a_fifteen_service_model_with_one_loopback_endpoint() {
        let fixture = fixture();
        let mut services = serde_json::Map::new();
        for number in 0..14 {
            services.insert(
                format!("service-{number}"),
                serde_json::json!({"image": "registry.test/app:df_demo"}),
            );
        }
        services.insert(
            "api".into(),
            serde_json::json!({
                "image": "registry.test/api:df_demo",
                "ports": ["127.0.0.1:20001:3000"],
                "volumes": [{"type": "volume", "source": "database", "target": "/var/lib/database"}],
            }),
        );
        let model = serde_json::json!({
            "services": services,
            "volumes": {"database": {"name": "df_demo_database"}},
            "networks": {"default": {"name": "df_demo_default"}},
        });
        validate_model(&model, &fixture.request).expect("isolated model validates");
    }

    #[test]
    fn rejects_shared_resources_and_unmanaged_ports_without_scoped_authorization() {
        let fixture = fixture();
        let model = serde_json::json!({
            "services": {
                "api": {
                    "container_name": "shared-api",
                    "network_mode": "host",
                    "ports": ["0.0.0.0:3000:3000"],
                    "volumes": [{"type": "bind", "source": "/tmp", "target": "/data"}],
                }
            },
            "volumes": {"database": {"external": true, "name": "database"}},
        });
        assert!(validate_model(&model, &fixture.request).is_err());
    }

    #[test]
    fn rejects_a_mutable_shared_build_tag() {
        let fixture = fixture();
        let model = serde_json::json!({
            "services": {
                "api": {
                    "build": {"context": "."},
                    "image": "registry.test/api:latest",
                    "ports": ["127.0.0.1:20001:3000"],
                }
            },
        });
        assert!(validate_model(&model, &fixture.request).is_err());
    }

    #[test]
    fn destroy_requires_owned_project_files_and_never_requests_volume_removal() {
        let fixture = fixture();
        let request = ComposeDestroyRequest {
            worktree: fixture.directory.path().display().to_string(),
            project_name: "df_0123abcd_4567efab".into(),
            files: vec![
                fixture
                    .directory
                    .path()
                    .join("compose.yaml")
                    .display()
                    .to_string(),
            ],
            purge_data: false,
        };
        validate_destroy_request(&request).expect("owned request validates");
        let args = compose_destroy_command_args(&request);
        assert!(
            args.windows(2).any(|pair| {
                pair == [OsString::from("down"), OsString::from("--remove-orphans")]
            })
        );
        assert!(!args.iter().any(|argument| argument == "--volumes"));
        assert!(
            compose_destroy_command_args(&ComposeDestroyRequest {
                purge_data: true,
                ..request.clone()
            })
            .iter()
            .any(|argument| argument == "--volumes")
        );
        assert!(
            validate_destroy_request(&ComposeDestroyRequest {
                project_name: "foreign-project".into(),
                ..request
            })
            .is_err()
        );
    }

    struct Fixture {
        directory: TempDir,
        request: ComposeReconcileRequest,
    }

    fn fixture() -> Fixture {
        let directory = tempfile::tempdir().expect("worktree");
        std::fs::write(directory.path().join("compose.yaml"), "services: {}\n").expect("file");
        Fixture {
            request: ComposeReconcileRequest {
                protocol: PROTOCOL_VERSION,
                operation_id: Uuid::now_v7(),
                environment_id: Uuid::now_v7(),
                worktree: directory.path().display().to_string(),
                project_name: "df_demo".into(),
                files: vec![directory.path().join("compose.yaml").display().to_string()],
                environment: BTreeMap::new(),
                endpoints: vec![ComposeEndpoint {
                    name: "api".into(),
                    service: "api".into(),
                    container_port: 3_000,
                    host_port: 20_001,
                    readiness_path: "/health".into(),
                }],
                authorizations: ComposeAuthorizations {
                    global_container_names: None,
                    host_networking: None,
                    external_resources: None,
                    global_volume_names: None,
                    unmanaged_ports: None,
                    shared_mutable_binds: None,
                },
            },
            directory,
        }
    }
}
