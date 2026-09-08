use devfleet_cli::request;
use devfleet_protocol::{
    DaemonRequest, DaemonResponse, EnvironmentUpRequest, LifecycleState, ProcessSpec,
    ReadinessState,
};
use std::{
    collections::BTreeMap,
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
            key: "fixture".into(),
            worktree: temporary.path().display().to_string(),
            profile: "process".into(),
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
    let fixture = start_daemon(temporary.path());
    let result = request(
        &fixture.socket,
        DaemonRequest::Up(EnvironmentUpRequest {
            key: "untrusted".into(),
            worktree: temporary.path().display().to_string(),
            profile: "process".into(),
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
fn immediately_exited_process_is_retained_as_failed() {
    let temporary = TempDir::new().expect("temporary directory");
    let fixture = start_daemon(temporary.path());
    let result = request(
        &fixture.socket,
        DaemonRequest::Up(EnvironmentUpRequest {
            key: "exits-immediately".into(),
            worktree: temporary.path().display().to_string(),
            profile: "process".into(),
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

fn start_daemon(root: &Path) -> DaemonFixture {
    let socket = root.join("d.sock");
    let state_dir = root.join("state");
    let binary_dir = PathBuf::from(env!("CARGO_BIN_EXE_devfleetd"))
        .parent()
        .expect("binary directory")
        .to_owned();
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
            binary_dir
                .join("devfleet-plugin-process")
                .to_str()
                .expect("plugin UTF-8"),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("daemon starts");
    wait_for_path(&socket);
    DaemonFixture {
        child,
        state_dir,
        socket,
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
