use devfleet_cli::request;
use devfleet_protocol::{
    BindingApplyRequest, BindingIntegration, BindingInvalidation, BindingPhase, DaemonRequest,
    DaemonResponse, EnvironmentPlanRequest, EnvironmentUpRequest, InitializationRunRequest,
    InitializationTask, InitializationTaskKind, LifecycleState, ProcessSpec, ReadinessState,
    TaskRerunPolicy,
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
#[allow(clippy::too_many_lines)]
fn bindings_write_private_phase_artifacts_and_expose_only_public_metadata() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let fixture = start_daemon(temporary.path());
    let environment = up_environment(&fixture.socket, temporary.path(), "bindings");
    for (phase, integration, invalidation) in [
        (
            BindingPhase::Process,
            BindingIntegration::ProcessEnvironment,
            BindingInvalidation::Restart,
        ),
        (
            BindingPhase::Bundle,
            BindingIntegration::BundleConfig,
            BindingInvalidation::Reload,
        ),
        (
            BindingPhase::NativeBuild,
            BindingIntegration::NativeBuildSettings,
            BindingInvalidation::Rebuild,
        ),
        (
            BindingPhase::Runtime,
            BindingIntegration::RuntimeConfiguration,
            BindingInvalidation::Reconnect,
        ),
    ] {
        let response = request(
            &fixture.socket,
            DaemonRequest::BindingApply(BindingApplyRequest {
                environment_id: environment,
                phase,
                integration,
                public_values: BTreeMap::from([("APP_PUBLIC_FEATURE".into(), "enabled".into())]),
                backend_only_values: if phase == BindingPhase::Runtime {
                    BTreeMap::from([("DATABASE_PASSWORD".into(), "private".into())])
                } else {
                    BTreeMap::new()
                },
                reserved_endpoints: BTreeMap::from([(
                    "APP_PUBLIC_API_URL".into(),
                    "http://127.0.0.1:20001".into(),
                )]),
            }),
        )
        .expect("binding applies");
        let DaemonResponse::Bindings(bindings) = response else {
            panic!("expected bindings");
        };
        assert!(
            bindings
                .iter()
                .any(|binding| binding.phase == phase && binding.invalidation == invalidation)
        );
    }
    let private = fixture
        .state_dir
        .join("bindings")
        .join(environment.to_string())
        .join("runtime.json");
    assert_eq!(
        fs::metadata(&private)
            .expect("private metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(
        fs::read_to_string(&private)
            .expect("private artifact")
            .contains("private")
    );
    let DaemonResponse::Environment(status) = request(
        &fixture.socket,
        DaemonRequest::Status {
            environment_id: environment,
        },
    )
    .expect("status") else {
        panic!("expected status");
    };
    assert_eq!(status.bindings.len(), 4);
    assert!(
        status
            .bindings
            .iter()
            .all(|binding| !binding.public_values.contains_key("DATABASE_PASSWORD"))
    );
    assert!(
        request(
            &fixture.socket,
            DaemonRequest::BindingApply(BindingApplyRequest {
                environment_id: environment,
                phase: BindingPhase::Bundle,
                integration: BindingIntegration::RuntimeConfiguration,
                public_values: BTreeMap::new(),
                backend_only_values: BTreeMap::new(),
                reserved_endpoints: BTreeMap::new(),
            })
        )
        .is_err()
    );
    assert!(
        request(
            &fixture.socket,
            DaemonRequest::BindingApply(BindingApplyRequest {
                environment_id: environment,
                phase: BindingPhase::Runtime,
                integration: BindingIntegration::RuntimeConfiguration,
                public_values: BTreeMap::from([("APP_PUBLIC_API_URL".into(), "wrong".into())]),
                backend_only_values: BTreeMap::new(),
                reserved_endpoints: BTreeMap::from([("APP_PUBLIC_API_URL".into(), "right".into())]),
            })
        )
        .is_err()
    );
    stop(&fixture.socket, environment);
}

#[test]
#[allow(clippy::too_many_lines)]
fn initialization_tasks_are_ordered_persisted_and_scoped_to_one_environment() {
    let temporary = TempDir::new().expect("temporary directory");
    let first_root = temporary.path().join("first");
    let second_root = temporary.path().join("second");
    fs::create_dir_all(&first_root).expect("first directory");
    fs::create_dir_all(&second_root).expect("second directory");
    initialize_git_repository(&first_root);
    initialize_git_repository(&second_root);
    let fixture = start_daemon(temporary.path());
    let first = up_environment(&fixture.socket, &first_root, "first");
    let second = up_environment(&fixture.socket, &second_root, "second");
    let migrated = first_root.join("migrated");
    let seeded = first_root.join("seeded");
    let tasks = vec![
        InitializationTask {
            id: "migration".into(),
            kind: InitializationTaskKind::Migration,
            depends_on: Vec::new(),
            rerun_policy: TaskRerunPolicy::Never,
            process: ProcessSpec {
                argv: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    format!(
                        "test ! -e {} && printf migrated > {}",
                        migrated.display(),
                        migrated.display()
                    ),
                ],
                cwd: first_root.display().to_string(),
                environment: BTreeMap::new(),
            },
        },
        InitializationTask {
            id: "seed".into(),
            kind: InitializationTaskKind::Seed,
            depends_on: vec!["migration".into()],
            rerun_policy: TaskRerunPolicy::Never,
            process: ProcessSpec {
                argv: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    format!("printf seed > {}", seeded.display()),
                ],
                cwd: first_root.display().to_string(),
                environment: BTreeMap::new(),
            },
        },
    ];
    let response = request(
        &fixture.socket,
        DaemonRequest::InitializationRun(InitializationRunRequest {
            environment_id: first,
            tasks: tasks.clone(),
        }),
    )
    .expect("first initialization succeeds");
    let DaemonResponse::InitializationTasks(completed) = response else {
        panic!("expected task results");
    };
    assert_eq!(completed.len(), 2);
    assert!(migrated.exists());
    assert!(seeded.exists());
    assert!(!second_root.join("migrated").exists());
    let response = request(
        &fixture.socket,
        DaemonRequest::InitializationRun(InitializationRunRequest {
            environment_id: first,
            tasks,
        }),
    )
    .expect("idempotent initialization succeeds");
    let DaemonResponse::InitializationTasks(repeated) = response else {
        panic!("expected repeated task results");
    };
    assert!(repeated.iter().all(|task| task.attempts == 1));
    let failed = request(
        &fixture.socket,
        DaemonRequest::InitializationRun(InitializationRunRequest {
            environment_id: second,
            tasks: vec![InitializationTask {
                id: "migration".into(),
                kind: InitializationTaskKind::Migration,
                depends_on: Vec::new(),
                rerun_policy: TaskRerunPolicy::Never,
                process: ProcessSpec {
                    argv: vec!["/usr/bin/false".into()],
                    cwd: second_root.display().to_string(),
                    environment: BTreeMap::new(),
                },
            }],
        }),
    );
    assert!(failed.is_err());
    let DaemonResponse::Environment(second_status) = request(
        &fixture.socket,
        DaemonRequest::Status {
            environment_id: second,
        },
    )
    .expect("second status") else {
        panic!("expected second environment");
    };
    assert!(!second_status.initialization_ready);
    assert_eq!(
        second_status.initialization_tasks[0].status,
        devfleet_protocol::InitializationTaskStatus::Failed
    );
    stop(&fixture.socket, first);
    stop(&fixture.socket, second);
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
    let events = request(
        &fixture.socket,
        DaemonRequest::EventsSubscribe {
            environment_id: accepted.environment_id,
            after: Some(0),
        },
    )
    .expect("events resume");
    let DaemonResponse::Events(events) = events else {
        panic!("expected events");
    };
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].kind, "up_accepted");
    assert_eq!(events[1].kind, "process_running");

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
fn bounded_redacted_logs_do_not_block_control_operations() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let fixture = start_daemon(temporary.path());
    let secret = "value-that-must-not-reach-disk";
    let environment = request(
        &fixture.socket,
        DaemonRequest::Up(EnvironmentUpRequest {
            worktree: temporary.path().display().to_string(),
            profile: "logs".into(),
            instance: None,
            trusted: true,
            process: ProcessSpec {
                argv: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "i=0; while [ $i -lt 512 ]; do printf 'line-%s\\n' \"$i\"; i=$((i + 1)); done; printf 'token=%s\\n' \"$API_TOKEN\"; exec sleep 60".into(),
                ],
                cwd: temporary.path().display().to_string(),
                environment: BTreeMap::from([("API_TOKEN".into(), secret.into())]),
            },
        }),
    )
    .expect("start accepted");
    let DaemonResponse::Accepted(accepted) = environment else {
        panic!("expected accepted operation");
    };

    let logs = wait_for_logs(&fixture.socket, accepted.environment_id);
    assert!(logs.lines.len() <= 256);
    assert!(logs.lines.iter().any(|line| line == "line-511"));
    assert!(logs.lines.iter().any(|line| line == "token=[REDACTED]"));
    assert!(logs.lines.iter().all(|line| !line.contains(secret)));

    let started = Instant::now();
    let status = request(
        &fixture.socket,
        DaemonRequest::Status {
            environment_id: accepted.environment_id,
        },
    )
    .expect("status stays available while logs are captured");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(matches!(status, DaemonResponse::Environment(_)));

    let resumed = request(
        &fixture.socket,
        DaemonRequest::LogsTail {
            environment_id: accepted.environment_id,
            resource: "process".into(),
            after: Some(logs.cursor),
            limit: 256,
        },
    )
    .expect("resume logs");
    let DaemonResponse::Logs(resumed) = resumed else {
        panic!("expected logs response");
    };
    assert!(resumed.lines.is_empty());

    request(
        &fixture.socket,
        DaemonRequest::Stop {
            environment_id: accepted.environment_id,
        },
    )
    .expect("stop remains available after high-volume logging");
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
fn cancellation_persists_before_reaping_the_owned_process() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let fixture = start_daemon(temporary.path());
    let response = request(
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
    let DaemonResponse::Accepted(accepted) = response else {
        panic!("expected accepted operation");
    };
    let response = request(
        &fixture.socket,
        DaemonRequest::OperationGet {
            operation_id: accepted.operation_id,
        },
    )
    .expect("operation reads");
    let DaemonResponse::Operation(operation) = response else {
        panic!("expected operation");
    };
    assert!(operation.deadline_unix_ms.is_some());
    assert_eq!(operation.resource_id.as_deref(), Some("process"));
    let response = request(
        &fixture.socket,
        DaemonRequest::OperationCancel {
            operation_id: accepted.operation_id,
        },
    )
    .expect("cancel accepted");
    let DaemonResponse::Operation(operation) = response else {
        panic!("expected operation");
    };
    assert_eq!(
        operation.status,
        devfleet_protocol::OperationStatus::Cancelled
    );
    let response = request(
        &fixture.socket,
        DaemonRequest::Status {
            environment_id: accepted.environment_id,
        },
    )
    .expect("status reads");
    let DaemonResponse::Environment(environment) = response else {
        panic!("expected environment");
    };
    assert_eq!(environment.lifecycle, LifecycleState::Stopped);
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

fn up_environment(socket: &Path, worktree: &Path, instance: &str) -> uuid::Uuid {
    let response = request(
        socket,
        DaemonRequest::Up(EnvironmentUpRequest {
            worktree: worktree.display().to_string(),
            profile: "process".into(),
            instance: Some(instance.into()),
            trusted: true,
            process: ProcessSpec {
                argv: vec!["/bin/sleep".into(), "60".into()],
                cwd: worktree.display().to_string(),
                environment: BTreeMap::new(),
            },
        }),
    )
    .expect("environment starts");
    let DaemonResponse::Accepted(accepted) = response else {
        panic!("expected accepted environment");
    };
    accepted.environment_id
}

fn stop(socket: &Path, environment_id: uuid::Uuid) {
    request(socket, DaemonRequest::Stop { environment_id }).expect("environment stops");
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

fn wait_for_logs(socket: &Path, environment_id: uuid::Uuid) -> devfleet_protocol::LogSnapshot {
    let started = Instant::now();
    loop {
        let response = request(
            socket,
            DaemonRequest::LogsTail {
                environment_id,
                resource: "process".into(),
                after: None,
                limit: 256,
            },
        )
        .expect("logs request");
        let DaemonResponse::Logs(logs) = response else {
            panic!("expected logs response");
        };
        if logs.lines.iter().any(|line| line == "token=[REDACTED]") {
            return logs;
        }
        assert!(
            started.elapsed() < TIMEOUT,
            "process logs were not captured"
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
