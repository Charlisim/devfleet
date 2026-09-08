use devfleet_cli::request;
use devfleet_protocol::{
    DaemonRequest, DaemonResponse, EnvironmentPlanRequest, EnvironmentUpRequest, LifecycleState,
    ProcessSpec, ReadinessState,
};
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
    state_dir: PathBuf,
    socket: PathBuf,
    process_plugin: PathBuf,
}

impl Drop for DaemonFixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn client_reconnects_to_the_same_owned_process_and_stop_preserves_data() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let mut fixture = start_daemon(temporary.path());
    let mut unrelated = Command::new("/bin/sleep")
        .arg("60")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("unrelated process starts");

    let up = request(
        &fixture.socket,
        DaemonRequest::Up(EnvironmentUpRequest {
            worktree: temporary.path().display().to_string(),
            profile: "process".into(),
            instance: None,
            trusted: true,
            process: ProcessSpec {
                argv: vec!["/bin/sleep".into(), "60".into()],
                cwd: temporary.path().display().to_string(),
                environment: BTreeMap::new(),
            },
        }),
    )
    .expect("start accepted");
    let DaemonResponse::Accepted(accepted) = up else {
        panic!("expected accepted operation");
    };

    // The first client has returned; this independent connection observes the same PID.
    let status = request(
        &fixture.socket,
        DaemonRequest::Status {
            environment_id: accepted.environment_id,
        },
    )
    .expect("reconnect status");
    let DaemonResponse::Environment(running) = status else {
        panic!("expected environment");
    };
    assert_eq!(running.lifecycle, LifecycleState::Running);
    assert_eq!(
        running.process.expect("running process").readiness,
        ReadinessState::ProcessAlive
    );

    let stopped = request(
        &fixture.socket,
        DaemonRequest::Stop {
            environment_id: accepted.environment_id,
        },
    )
    .expect("stop owned process");
    let DaemonResponse::Environment(stopped) = stopped else {
        panic!("expected environment");
    };
    assert_eq!(stopped.lifecycle, LifecycleState::Stopped);
    assert_eq!(
        stopped.process.expect("retained process record").readiness,
        ReadinessState::ProcessStopped
    );
    assert!(unrelated.try_wait().expect("unrelated status").is_none());

    fixture.child.kill().expect("daemon terminates");
    fixture.child.wait().expect("daemon reaps");
    let store = devfleet_state::StateStore::open(&fixture.state_dir.join("state.sqlite3"))
        .expect("state reopens after daemon exit");
    let retained = store
        .environment(accepted.environment_id)
        .expect("state query")
        .expect("environment retained");
    assert_eq!(retained.lifecycle, devfleet_state::StoredLifecycle::Stopped);

    unrelated.kill().expect("unrelated cleanup");
    unrelated.wait().expect("unrelated reaps");
}

#[test]
fn untrusted_process_never_reaches_the_supervisor() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let fixture = start_daemon(temporary.path());
    let result = request(
        &fixture.socket,
        DaemonRequest::Up(EnvironmentUpRequest {
            worktree: temporary.path().display().to_string(),
            profile: "process".into(),
            instance: None,
            trusted: false,
            process: ProcessSpec {
                argv: vec!["/usr/bin/false".into()],
                cwd: temporary.path().display().to_string(),
                environment: BTreeMap::new(),
            },
        }),
    );
    assert!(result.is_err());
    assert!(
        devfleet_state::StateStore::open(&fixture.state_dir.join("state.sqlite3"))
            .expect("state opens")
            .environments()
            .expect("state lists")
            .is_empty()
    );
}

#[test]
fn trusted_project_cannot_execute_an_untrusted_plugin() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let fixture = start_daemon_with_plugin_trust(temporary.path(), false);
    let installed = request(
        &fixture.socket,
        DaemonRequest::PluginInstall {
            path: fixture.process_plugin.display().to_string(),
        },
    )
    .expect("plugin installs without trust");
    let DaemonResponse::Plugin(installed) = installed else {
        panic!("expected installed plugin");
    };
    assert!(!installed.trusted);
    let result = request(
        &fixture.socket,
        DaemonRequest::Up(EnvironmentUpRequest {
            worktree: temporary.path().display().to_string(),
            profile: "process".into(),
            instance: None,
            trusted: true,
            process: ProcessSpec {
                argv: vec!["/bin/sleep".into(), "60".into()],
                cwd: temporary.path().display().to_string(),
                environment: BTreeMap::new(),
            },
        }),
    );
    assert!(result.is_err());
    assert!(
        devfleet_state::StateStore::open(&fixture.state_dir.join("state.sqlite3"))
            .expect("state opens")
            .environments()
            .expect("state lists")
            .is_empty()
    );
}

#[test]
fn immediately_exited_process_is_retained_as_failed() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let fixture = start_daemon(temporary.path());
    let result = request(
        &fixture.socket,
        DaemonRequest::Up(EnvironmentUpRequest {
            worktree: temporary.path().display().to_string(),
            profile: "process".into(),
            instance: None,
            trusted: true,
            process: ProcessSpec {
                argv: vec!["/usr/bin/false".into()],
                cwd: temporary.path().display().to_string(),
                environment: BTreeMap::new(),
            },
        }),
    );
    assert!(result.is_err());

    let retained = devfleet_state::StateStore::open(&fixture.state_dir.join("state.sqlite3"))
        .expect("state opens")
        .environments()
        .expect("state lists");
    assert_eq!(retained.len(), 1);
    assert_eq!(
        retained[0].lifecycle,
        devfleet_state::StoredLifecycle::Failed
    );
    assert_eq!(
        retained[0]
            .process
            .as_ref()
            .expect("process retained")
            .readiness,
        devfleet_state::StoredReadiness::ProcessStopped
    );
}

#[test]
fn plan_is_side_effect_free_and_invalid_manifests_cannot_spawn_a_process() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let fixture = start_daemon(temporary.path());
    let marker = temporary.path().join("plan-must-not-spawn");
    let manifest = temporary.path().join("devfleet.toml");
    fs::write(
        &manifest,
        format!(
            r#"schema_version = 1
[profiles.process]
resources = ["fixture"]
[resources.fixture]
provider = "official.process"
[resources.fixture.config]
argv = ["/bin/sh", "-c", "touch {}"]
token = "${{{{ secrets.API_TOKEN }}}}"
"#,
            marker.display()
        ),
    )
    .expect("manifest writes");
    let request_plan = || EnvironmentPlanRequest {
        worktree: temporary.path().display().to_string(),
        profile: "process".into(),
        manifest: manifest.display().to_string(),
        local_override: None,
        secret_references: vec!["API_TOKEN".into()],
        operation_overrides: BTreeMap::new(),
        runtime_outputs: BTreeMap::new(),
        planning_endpoints: BTreeMap::new(),
    };
    let first =
        request(&fixture.socket, DaemonRequest::Plan(request_plan())).expect("plan succeeds");
    let DaemonResponse::Plan(first) = first else {
        panic!("expected plan response");
    };
    let repeated =
        request(&fixture.socket, DaemonRequest::Plan(request_plan())).expect("plan repeats");
    let DaemonResponse::Plan(repeated) = repeated else {
        panic!("expected plan response");
    };
    assert_eq!(first.generation, repeated.generation);
    assert!(!marker.exists());

    fs::write(
        &manifest,
        format!(
            r#"schema_version = 1
[profiles.process]
resources = ["fixture"]
[resources.fixture]
provider = "official.process"
[resources.fixture.config]
argv = ["/bin/sh", "-c", "touch {}"]
token = "${{{{ secrets.API_TOKEN }}}}"
revision = 2
"#,
            marker.display()
        ),
    )
    .expect("changed manifest writes");
    let changed =
        request(&fixture.socket, DaemonRequest::Plan(request_plan())).expect("plan changes");
    let DaemonResponse::Plan(changed) = changed else {
        panic!("expected plan response");
    };
    assert_eq!(changed.generation, first.generation + 1);
    assert!(!marker.exists());

    fs::write(
        &manifest,
        format!(
            r#"schema_version = 1
[profiles.process]
resources = ["fixture"]
[resources.fixture]
provider = "official.process"
[resources.fixture.config]
argv = ["/bin/sh", "-c", "touch {}"]
invalid = "${{{{ unrecognized.token }}}}"
"#,
            marker.display()
        ),
    )
    .expect("invalid manifest writes");
    assert!(request(&fixture.socket, DaemonRequest::Plan(request_plan())).is_err());
    assert!(!marker.exists());
    assert!(
        devfleet_state::StateStore::open(&fixture.state_dir.join("state.sqlite3"))
            .expect("state opens")
            .environments()
            .expect("environments list")
            .is_empty()
    );
}

fn start_daemon(root: &Path) -> DaemonFixture {
    start_daemon_with_plugin_trust(root, true)
}

fn start_daemon_with_plugin_trust(root: &Path, trust_plugin: bool) -> DaemonFixture {
    let socket = root.join("d.sock");
    let state_dir = root.join("state");
    let binary_dir = PathBuf::from(env!("CARGO_BIN_EXE_devfleetd"))
        .parent()
        .expect("binary directory")
        .to_owned();
    let process_plugin = binary_dir.join("devfleet-plugin-process");
    let child = Command::new(binary_dir.join("devfleetd"))
        .args([
            "--serve",
            "--state-dir",
            state_dir.to_str().expect("state UTF-8"),
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
    wait_for_path(&socket);
    if trust_plugin {
        request(
            &socket,
            DaemonRequest::PluginInstall {
                path: process_plugin.display().to_string(),
            },
        )
        .expect("plugin installs");
        request(
            &socket,
            DaemonRequest::PluginTrust {
                id: "official.process".into(),
            },
        )
        .expect("plugin trust persists");
    }
    DaemonFixture {
        child,
        state_dir,
        socket,
        process_plugin,
    }
}

fn wait_for_path(path: &Path) {
    let started = Instant::now();
    while !path.exists() {
        assert!(
            started.elapsed() < TIMEOUT,
            "daemon socket startup timed out"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn initialize_git_repository(path: &Path) {
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
