use devfleet_cli::request;
use devfleet_protocol::{
    DaemonRequest, DaemonResponse, DoctorPhase, DoctorRequest, EnvironmentPlanRequest,
    EnvironmentUpRequest, ProcessSpec, ProjectInitRequest,
};
use std::os::unix::fs::PermissionsExt;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use tempfile::TempDir;

const TIMEOUT: Duration = Duration::from_secs(5);

struct DaemonFixture {
    child: Child,
    socket: PathBuf,
}

impl Drop for DaemonFixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn init_proposes_without_writing_then_plan_and_up_succeed() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let fixture = start_daemon(temporary.path(), true);
    let manifest = temporary.path().join("devfleet.toml");
    let canonical_manifest = temporary
        .path()
        .canonicalize()
        .expect("worktree canonicalizes")
        .join("devfleet.toml");

    let DaemonResponse::ProjectInit(project) = request(
        &fixture.socket,
        DaemonRequest::ProjectInit(ProjectInitRequest {
            worktree: temporary.path().display().to_string(),
            trusted: true,
            runtime: None,
            device_type: None,
        }),
    )
    .expect("project initializes") else {
        panic!("expected project initialization");
    };
    assert!(project.trusted);
    assert_eq!(
        project.proposed_manifest_path,
        canonical_manifest.display().to_string()
    );
    assert!(!manifest.exists());
    fs::write(&manifest, &project.proposed_manifest).expect("reviewed manifest writes");

    let DaemonResponse::Plan(_) = request(
        &fixture.socket,
        DaemonRequest::Plan(EnvironmentPlanRequest {
            worktree: temporary.path().display().to_string(),
            profile: "process".into(),
            manifest: manifest.display().to_string(),
            local_override: None,
            secret_references: Vec::new(),
            operation_overrides: BTreeMap::new(),
            runtime_outputs: BTreeMap::new(),
            planning_endpoints: BTreeMap::new(),
        }),
    )
    .expect("plan succeeds") else {
        panic!("expected plan");
    };
    let DaemonResponse::Accepted(accepted) = request(
        &fixture.socket,
        DaemonRequest::Up(EnvironmentUpRequest {
            worktree: temporary.path().display().to_string(),
            profile: "process".into(),
            instance: Some("init-plan-up".into()),
            trusted: true,
            process: ProcessSpec {
                argv: vec!["/bin/sleep".into(), "60".into()],
                cwd: temporary.path().display().to_string(),
                environment: BTreeMap::new(),
            },
        }),
    )
    .expect("environment starts") else {
        panic!("expected accepted environment");
    };
    let DaemonResponse::Doctor(doctor) = request(
        &fixture.socket,
        DaemonRequest::Doctor(DoctorRequest {
            worktree: temporary.path().display().to_string(),
            environment_id: Some(accepted.environment_id),
        }),
    )
    .expect("doctor returns typed findings") else {
        panic!("expected doctor response");
    };
    assert!(doctor.findings.iter().any(|finding| {
        finding.phase == DoctorPhase::Runtime && finding.resource == "simulator_selection"
    }));
    assert!(doctor.findings.iter().any(|finding| {
        finding.phase == DoctorPhase::Bindings && finding.resource == "bundle_integration"
    }));
    request(
        &fixture.socket,
        DaemonRequest::Stop {
            environment_id: accepted.environment_id,
        },
    )
    .expect("environment stops");
}

#[test]
fn doctor_reports_missing_project_setup_without_creating_files() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let fixture = start_daemon(temporary.path(), false);
    let DaemonResponse::Doctor(doctor) = request(
        &fixture.socket,
        DaemonRequest::Doctor(DoctorRequest {
            worktree: temporary.path().display().to_string(),
            environment_id: None,
        }),
    )
    .expect("doctor returns typed findings") else {
        panic!("expected doctor response");
    };
    assert!(!doctor.ready);
    assert!(doctor.findings.iter().any(|finding| {
        finding.phase == DoctorPhase::Project && finding.resource == "project_setup"
    }));
    assert!(doctor.findings.iter().any(|finding| {
        finding.phase == DoctorPhase::Plugin && finding.resource == "official.react-native"
    }));
    assert!(doctor.findings.iter().any(|finding| {
        finding.phase == DoctorPhase::Bindings && finding.resource == "environment"
    }));
    assert!(doctor.findings.iter().any(|finding| {
        finding.phase == DoctorPhase::Preparation && finding.resource == "environment"
    }));
    assert!(!temporary.path().join("devfleet.toml").exists());
}

#[test]
fn doctor_reports_an_incompatible_trusted_plugin() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let fixture = start_daemon(temporary.path(), true);
    request(
        &fixture.socket,
        DaemonRequest::ProjectInit(ProjectInitRequest {
            worktree: temporary.path().display().to_string(),
            trusted: true,
            runtime: None,
            device_type: None,
        }),
    )
    .expect("project initializes");
    let incompatible = temporary.path().join("incompatible-compose");
    write_incompatible_compose_plugin(&incompatible);
    request(
        &fixture.socket,
        DaemonRequest::PluginInstall {
            path: incompatible.display().to_string(),
        },
    )
    .expect("plugin installs");
    request(
        &fixture.socket,
        DaemonRequest::PluginTrust {
            id: "official.compose".into(),
        },
    )
    .expect("plugin trust persists");
    let DaemonResponse::Doctor(doctor) = request(
        &fixture.socket,
        DaemonRequest::Doctor(DoctorRequest {
            worktree: temporary.path().display().to_string(),
            environment_id: None,
        }),
    )
    .expect("doctor returns typed findings") else {
        panic!("expected doctor response");
    };
    assert!(doctor.findings.iter().any(|finding| {
        finding.phase == DoctorPhase::Plugin
            && finding.resource == "official.compose"
            && finding.cause.contains("capability")
    }));
}

fn start_daemon(root: &Path, trust_process: bool) -> DaemonFixture {
    let socket = root.join("d.sock");
    let binary_dir = PathBuf::from(env!("CARGO_BIN_EXE_devfleetd"))
        .parent()
        .expect("binary directory")
        .to_owned();
    let process_plugin = binary_dir.join("devfleet-plugin-process");
    let child = Command::new(binary_dir.join("devfleetd"))
        .args([
            "--serve",
            "--state-dir",
            root.join("state").to_str().expect("state UTF-8"),
            "--socket",
            socket.to_str().expect("socket UTF-8"),
            "--supervisor",
            binary_dir
                .join("devfleet-supervisor")
                .to_str()
                .expect("supervisor UTF-8"),
            "--process-plugin",
            process_plugin.to_str().expect("plugin UTF-8"),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("daemon starts");
    wait_for_daemon(&socket);
    if trust_process {
        request(
            &socket,
            DaemonRequest::PluginInstall {
                path: process_plugin.display().to_string(),
            },
        )
        .expect("process plugin installs");
        request(
            &socket,
            DaemonRequest::PluginTrust {
                id: "official.process".into(),
            },
        )
        .expect("process plugin trust persists");
    }
    DaemonFixture { child, socket }
}

fn initialize_git_repository(path: &Path) {
    fs::create_dir_all(path).expect("fixture directory creates");
    run_git(path, ["init"]);
    run_git(
        path,
        ["config", "user.email", "devfleet-test@example.invalid"],
    );
    run_git(path, ["config", "user.name", "DevFleet Test"]);
    fs::write(path.join("README.md"), "fixture\n").expect("fixture writes");
    run_git(path, ["add", "README.md"]);
    run_git(path, ["commit", "-m", "fixture"]);
}

fn run_git<const N: usize>(path: &Path, arguments: [&str; N]) {
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

fn write_incompatible_compose_plugin(path: &Path) {
    let platform = format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH);
    let descriptor = serde_json::json!({
        "id": "official.compose",
        "version": "0.1.0",
        "platform": platform,
        "protocol": {"major": 1, "minor": 0},
        "capabilities": ["lifecycle.inspect"],
        "plugin_state_version": 1,
    });
    let script = format!(
        "#!/bin/sh\nprintf '%s\\n' '{}'\n",
        serde_json::to_string(&descriptor).expect("descriptor serializes")
    );
    fs::write(path, script).expect("plugin fixture writes");
    let mut permissions = fs::metadata(path).expect("plugin metadata").permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(path, permissions).expect("plugin is executable");
}

fn wait_for_daemon(socket: &Path) {
    let started = Instant::now();
    while request(socket, DaemonRequest::PluginList).is_err() {
        assert!(
            started.elapsed() < TIMEOUT,
            "daemon socket startup timed out"
        );
        thread::sleep(Duration::from_millis(10));
    }
}
