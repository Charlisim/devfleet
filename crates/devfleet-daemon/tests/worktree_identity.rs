use devfleet_cli::request;
use devfleet_protocol::{
    DaemonRequest, DaemonResponse, EnvironmentUpRequest, LifecycleState, ProcessSpec,
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
    socket: PathBuf,
}

impl Drop for DaemonFixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn moved_renamed_and_detached_worktree_keeps_one_environment_identity() {
    let temporary = TempDir::new().expect("temporary directory");
    let project = temporary.path().join("project space");
    initialize_git_repository(&project);
    let original = temporary.path().join("worktree original");
    run_git(
        &project,
        [
            "worktree",
            "add",
            "-b",
            "feature/original",
            original.to_str().expect("worktree UTF-8"),
        ],
    );
    let fixture = start_daemon(temporary.path());

    let first = up(&fixture.socket, &original, None).expect("first up");
    let moved = temporary.path().join("worktree moved");
    run_git(
        &project,
        [
            "worktree",
            "move",
            original.to_str().expect("old worktree UTF-8"),
            moved.to_str().expect("new worktree UTF-8"),
        ],
    );
    assert_same_environment(first, up(&fixture.socket, &moved, None).expect("moved up"));

    run_git(
        &moved,
        ["branch", "-m", "feature/original", "feature/renamed"],
    );
    assert_same_environment(
        first,
        up(&fixture.socket, &moved, None).expect("renamed branch up"),
    );

    run_git(&moved, ["checkout", "--detach"]);
    assert_same_environment(
        first,
        up(&fixture.socket, &moved, None).expect("detached worktree up"),
    );

    let status_response = request(
        &fixture.socket,
        DaemonRequest::Status {
            environment_id: first,
        },
    )
    .expect("status after move");
    let DaemonResponse::Environment(observed) = status_response else {
        panic!("expected environment");
    };
    assert_eq!(observed.worktree, moved.display().to_string());
    assert_eq!(observed.lifecycle, LifecycleState::Running);

    let additional = up(&fixture.socket, &moved, Some("named")).expect("named instance up");
    assert_ne!(additional, first);
    let first_status = status(&fixture.socket, first);
    let additional_status = status(&fixture.socket, additional);
    assert_eq!(first_status.project_id, additional_status.project_id);
    assert_eq!(first_status.worktree_id, additional_status.worktree_id);
    stop(&fixture.socket, additional);
    stop(&fixture.socket, first);
}

#[test]
fn recreated_path_is_orphaned_and_literal_argv_is_not_a_shell_program() {
    let temporary = TempDir::new().expect("temporary directory");
    let worktree = temporary.path().join("recreated worktree");
    initialize_git_repository(&worktree);
    let fixture = start_daemon(temporary.path());
    let old = up(&fixture.socket, &worktree, None).expect("old worktree starts");
    stop(&fixture.socket, old);

    fs::remove_dir_all(&worktree).expect("old worktree removed in temporary fixture");
    initialize_git_repository(&worktree);
    let marker = temporary.path().join("shell-interpolation-marker");
    let literal = format!("60; touch {}", marker.display());
    let literal_result = request(
        &fixture.socket,
        DaemonRequest::Up(EnvironmentUpRequest {
            worktree: worktree.display().to_string(),
            profile: "process".into(),
            instance: Some("argv-literal".into()),
            trusted: true,
            process: ProcessSpec {
                argv: vec!["/bin/sleep".into(), literal],
                cwd: worktree.display().to_string(),
                environment: BTreeMap::new(),
            },
        }),
    );
    assert!(literal_result.is_err());
    assert!(!marker.exists());

    let replacement = up(&fixture.socket, &worktree, None).expect("replacement starts");
    assert_ne!(old, replacement);
    let listed = request(&fixture.socket, DaemonRequest::List).expect("list environments");
    let DaemonResponse::Environments(environments) = listed else {
        panic!("expected environment list");
    };
    let orphaned = environments
        .iter()
        .find(|environment| environment.environment_id == old)
        .expect("old environment retained");
    assert_eq!(orphaned.lifecycle, LifecycleState::Orphaned);
    assert!(orphaned.reconciliation_diagnostic.is_some());
    stop(&fixture.socket, replacement);
}

fn up(socket: &Path, worktree: &Path, instance: Option<&str>) -> Result<uuid::Uuid, String> {
    let response = request(
        socket,
        DaemonRequest::Up(EnvironmentUpRequest {
            worktree: worktree.display().to_string(),
            profile: "process".into(),
            instance: instance.map(str::to_owned),
            trusted: true,
            process: ProcessSpec {
                argv: vec!["/bin/sleep".into(), "60".into()],
                cwd: worktree.display().to_string(),
                environment: BTreeMap::new(),
            },
        }),
    )
    .map_err(|error| error.to_string())?;
    let DaemonResponse::Accepted(operation) = response else {
        return Err("unexpected daemon response".into());
    };
    Ok(operation.environment_id)
}

fn assert_same_environment(expected: uuid::Uuid, actual: uuid::Uuid) {
    assert_eq!(expected, actual);
}

fn stop(socket: &Path, environment_id: uuid::Uuid) {
    request(socket, DaemonRequest::Stop { environment_id }).expect("owned process stops");
}

fn status(socket: &Path, environment_id: uuid::Uuid) -> devfleet_protocol::EnvironmentSnapshot {
    let response = request(socket, DaemonRequest::Status { environment_id }).expect("status");
    let DaemonResponse::Environment(environment) = response else {
        panic!("expected environment");
    };
    environment
}

fn start_daemon(root: &Path) -> DaemonFixture {
    let socket = root.join("d.sock");
    let binary_dir = PathBuf::from(env!("CARGO_BIN_EXE_devfleetd"))
        .parent()
        .expect("binary directory")
        .to_owned();
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
