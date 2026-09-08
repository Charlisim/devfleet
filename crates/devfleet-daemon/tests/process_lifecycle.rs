use devfleet_cli::request;
use devfleet_protocol::{
    BindingApplyRequest, BindingIntegration, BindingInvalidation, BindingPhase, DaemonRequest,
    DaemonResponse, DestroyAuthorization, DestroyRequest, DestroyTargetStatus,
    EnvironmentPlanRequest, EnvironmentUpRequest, InitializationRunRequest, InitializationTask,
    InitializationTaskKind, LifecycleState, ProcessSpec, ReadinessState, ResourceAction,
    ResourceActionRequest, ResourceExecRequest, SchedulerConfig, TaskRerunPolicy,
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
fn noninteractive_public_methods_keep_operation_identity_and_scoped_exec_output() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let fixture = start_daemon(temporary.path());
    let manifest = temporary.path().join("devfleet.toml");
    fs::write(
        &manifest,
        "schema_version = 1\n[profiles.process]\nresources = [\"app\"]\n[resources.app]\nprovider = \"official.process\"\n",
    )
    .expect("manifest writes");
    let plan = request(
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
    .expect("environment plan");
    assert!(matches!(plan, DaemonResponse::Plan(_)));

    let DaemonResponse::Accepted(up) = request(
        &fixture.socket,
        DaemonRequest::Up(EnvironmentUpRequest {
            worktree: temporary.path().display().to_string(),
            profile: "process".into(),
            instance: Some("noninteractive".into()),
            trusted: true,
            process: ProcessSpec {
                argv: vec!["/bin/sleep".into(), "60".into()],
                cwd: temporary.path().display().to_string(),
                environment: BTreeMap::new(),
            },
        }),
    )
    .expect("environment up") else {
        panic!("expected accepted up operation");
    };
    let DaemonResponse::Accepted(repeated_up) = request(
        &fixture.socket,
        DaemonRequest::Up(EnvironmentUpRequest {
            worktree: temporary.path().display().to_string(),
            profile: "process".into(),
            instance: Some("noninteractive".into()),
            trusted: true,
            process: ProcessSpec {
                argv: vec!["/bin/sleep".into(), "60".into()],
                cwd: temporary.path().display().to_string(),
                environment: BTreeMap::new(),
            },
        }),
    )
    .expect("repeated environment up") else {
        panic!("expected repeated accepted up operation");
    };
    assert_eq!(repeated_up.operation_id, up.operation_id);

    assert!(matches!(
        request(
            &fixture.socket,
            DaemonRequest::Status {
                environment_id: up.environment_id
            }
        )
        .expect("environment status"),
        DaemonResponse::Environment(_)
    ));
    let DaemonResponse::Environments(environments) =
        request(&fixture.socket, DaemonRequest::List).expect("environment list")
    else {
        panic!("expected environments");
    };
    assert!(
        environments
            .iter()
            .any(|environment| environment.environment_id == up.environment_id)
    );
    assert!(matches!(
        request(
            &fixture.socket,
            DaemonRequest::OperationGet {
                operation_id: up.operation_id
            }
        )
        .expect("up operation reads"),
        DaemonResponse::Operation(_)
    ));

    let DaemonResponse::ResourceAction(exec) = request(
        &fixture.socket,
        DaemonRequest::ResourceAction(ResourceActionRequest {
            environment_id: up.environment_id,
            resource: "environment".into(),
            action: ResourceAction::Exec(ResourceExecRequest {
                argv: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "printf '%s' \"$FIXTURE\"".into(),
                ],
                cwd: temporary.path().display().to_string(),
                environment: BTreeMap::from([("FIXTURE".into(), "scoped".into())]),
                timeout_seconds: 5,
            }),
        }),
    )
    .expect("scoped exec") else {
        panic!("expected scoped exec response");
    };
    let operation = exec.operation.expect("scoped exec operation");
    assert_eq!(
        operation.status,
        devfleet_protocol::OperationStatus::Accepted
    );
    let operation = wait_for_operation(&fixture.socket, operation.operation_id);
    assert_eq!(
        operation.status,
        devfleet_protocol::OperationStatus::Completed
    );
    assert!(matches!(
        request(
            &fixture.socket,
            DaemonRequest::LogsTail {
                environment_id: up.environment_id,
                resource: "process".into(),
                after: None,
                limit: 10,
            }
        )
        .expect("logs tail"),
        DaemonResponse::Logs(_)
    ));
    let DaemonResponse::Logs(exec_logs) = request(
        &fixture.socket,
        DaemonRequest::LogsTail {
            environment_id: up.environment_id,
            resource: format!("exec-{}", operation.operation_id),
            after: None,
            limit: 10,
        },
    )
    .expect("scoped exec logs tail") else {
        panic!("expected scoped exec logs");
    };
    assert_eq!(exec_logs.lines, vec!["scoped"]);
    assert!(matches!(
        request(
            &fixture.socket,
            DaemonRequest::EventsSubscribe {
                environment_id: up.environment_id,
                after: Some(0),
            }
        )
        .expect("events subscribe"),
        DaemonResponse::Events(_)
    ));
    let DaemonResponse::ResourceAction(timed_out) = request(
        &fixture.socket,
        DaemonRequest::ResourceAction(ResourceActionRequest {
            environment_id: up.environment_id,
            resource: "environment".into(),
            action: ResourceAction::Exec(ResourceExecRequest {
                argv: vec!["/bin/sleep".into(), "60".into()],
                cwd: temporary.path().display().to_string(),
                environment: BTreeMap::new(),
                timeout_seconds: 1,
            }),
        }),
    )
    .expect("scoped timeout returns typed result") else {
        panic!("expected scoped timeout response");
    };
    let timed_out = wait_for_operation(
        &fixture.socket,
        timed_out.operation.expect("timeout operation").operation_id,
    );
    assert_eq!(timed_out.status, devfleet_protocol::OperationStatus::Failed);
    let DaemonResponse::Logs(timeout_logs) = request(
        &fixture.socket,
        DaemonRequest::LogsTail {
            environment_id: up.environment_id,
            resource: format!("exec-{}", timed_out.operation_id),
            after: None,
            limit: 10,
        },
    )
    .expect("timed-out scoped exec logs tail") else {
        panic!("expected timed-out scoped exec logs");
    };
    assert!(
        timeout_logs
            .lines
            .iter()
            .any(|line| line.contains("deadline"))
    );
    let DaemonResponse::ResourceAction(long_running) = request(
        &fixture.socket,
        DaemonRequest::ResourceAction(ResourceActionRequest {
            environment_id: up.environment_id,
            resource: "environment".into(),
            action: ResourceAction::Exec(ResourceExecRequest {
                argv: vec!["/bin/sleep".into(), "60".into()],
                cwd: temporary.path().display().to_string(),
                environment: BTreeMap::new(),
                timeout_seconds: 30,
            }),
        }),
    )
    .expect("long scoped exec is accepted") else {
        panic!("expected long scoped exec response");
    };
    let long_running = long_running.operation.expect("long scoped exec operation");
    assert!(matches!(
        request(
            &fixture.socket,
            DaemonRequest::Status {
                environment_id: up.environment_id
            }
        )
        .expect("daemon remains responsive while scoped exec runs"),
        DaemonResponse::Environment(_)
    ));
    let DaemonResponse::Operation(cancelling) = request(
        &fixture.socket,
        DaemonRequest::OperationCancel {
            operation_id: long_running.operation_id,
        },
    )
    .expect("scoped exec cancellation is accepted") else {
        panic!("expected operation response");
    };
    assert_eq!(
        cancelling.status,
        devfleet_protocol::OperationStatus::CancellationRequested
    );
    assert_eq!(
        wait_for_operation(&fixture.socket, long_running.operation_id).status,
        devfleet_protocol::OperationStatus::Cancelled
    );
    let unsupported = request(
        &fixture.socket,
        DaemonRequest::ResourceAction(ResourceActionRequest {
            environment_id: up.environment_id,
            resource: "unavailable_provider".into(),
            action: ResourceAction::Open,
        }),
    )
    .expect_err("unsupported action rejects");
    assert!(unsupported.to_string().contains("CAPABILITY_UNSUPPORTED"));
    assert!(matches!(
        request(
            &fixture.socket,
            DaemonRequest::OperationCancel {
                operation_id: up.operation_id
            }
        )
        .expect("up operation cancels"),
        DaemonResponse::Operation(_)
    ));
    assert!(matches!(
        request(
            &fixture.socket,
            DaemonRequest::Destroy(DestroyRequest {
                environment_id: up.environment_id,
                dry_run: true,
                authorization: DestroyAuthorization::None,
                purge_data: false,
                purge_artifacts: false,
            })
        )
        .expect("destroy preview"),
        DaemonResponse::Destroy(_)
    ));
}

#[test]
fn scheduler_configures_host_budgets_and_exposes_focus_in_events() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let fixture = start_daemon(temporary.path());
    let environment = up_environment(&fixture.socket, temporary.path(), "scheduler");
    let response = request(
        &fixture.socket,
        DaemonRequest::SchedulerConfigure(SchedulerConfig {
            native_build_limit: 1,
            device_boot_limit: 2,
            preparation_limit: 3,
            metro_worker_limit: 4,
            memory_limit_bytes: 4 * 1024 * 1024 * 1024,
            memory_reserve_bytes: 256 * 1024 * 1024,
            aging_interval_ms: 15_000,
        }),
    )
    .expect("scheduler configures");
    let DaemonResponse::Scheduler(scheduler) = response else {
        panic!("expected scheduler response");
    };
    assert_eq!(scheduler.config.native_build_limit, 1);
    assert_eq!(scheduler.config.preparation_limit, 3);
    let response = request(
        &fixture.socket,
        DaemonRequest::SchedulerFocus {
            environment_id: environment,
        },
    )
    .expect("scheduler focuses environment");
    assert!(matches!(response, DaemonResponse::Scheduler(_)));
    let DaemonResponse::Environment(status) = request(
        &fixture.socket,
        DaemonRequest::Status {
            environment_id: environment,
        },
    )
    .expect("status") else {
        panic!("expected status");
    };
    assert_eq!(status.scheduler.config.metro_worker_limit, 4);
    let DaemonResponse::Events(events) = request(
        &fixture.socket,
        DaemonRequest::Events {
            environment_id: environment,
            after: None,
        },
    )
    .expect("events") else {
        panic!("expected events");
    };
    assert!(events.iter().any(|event| event.kind == "scheduler_focus"));
    stop(&fixture.socket, environment);
}

#[test]
fn reconcile_reuses_the_exact_supervised_process_without_disturbing_other_environments() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let fixture = start_daemon(temporary.path());
    let recovered = up_environment(&fixture.socket, temporary.path(), "recover");
    let independent = up_environment(&fixture.socket, temporary.path(), "independent");
    let DaemonResponse::Environment(before) = request(
        &fixture.socket,
        DaemonRequest::Status {
            environment_id: recovered,
        },
    )
    .expect("status before reconciliation") else {
        panic!("expected environment");
    };
    let DaemonResponse::Environment(after) = request(
        &fixture.socket,
        DaemonRequest::Reconcile {
            environment_id: recovered,
        },
    )
    .expect("reconciliation completes") else {
        panic!("expected environment");
    };
    assert_eq!(after.lifecycle, LifecycleState::Running);
    assert_eq!(
        after.process.expect("recovered process").pid,
        before.process.expect("original process").pid
    );
    let DaemonResponse::Environment(other) = request(
        &fixture.socket,
        DaemonRequest::Status {
            environment_id: independent,
        },
    )
    .expect("independent status") else {
        panic!("expected environment");
    };
    assert_eq!(other.lifecycle, LifecycleState::Running);
    stop(&fixture.socket, recovered);
    stop(&fixture.socket, independent);
}

#[test]
fn daemon_restart_recovers_only_the_exact_retained_supervisor_identity() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let mut first = start_daemon(temporary.path());
    let environment = up_environment(&first.socket, temporary.path(), "daemon-restart");
    let DaemonResponse::Environment(before) = request(
        &first.socket,
        DaemonRequest::Status {
            environment_id: environment,
        },
    )
    .expect("status before restart") else {
        panic!("expected environment");
    };
    first.child.kill().expect("daemon terminates");
    first.child.wait().expect("daemon reaps");
    let second = start_daemon(temporary.path());
    let DaemonResponse::Environment(after) = request(
        &second.socket,
        DaemonRequest::Status {
            environment_id: environment,
        },
    )
    .expect("status after restart") else {
        panic!("expected environment");
    };
    assert_eq!(after.lifecycle, LifecycleState::Running);
    assert_eq!(
        after.process.expect("recovered process").pid,
        before.process.expect("retained process").pid
    );
    stop(&second.socket, environment);
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
#[allow(clippy::too_many_lines)]
fn destroy_is_explicit_scoped_idempotent_and_rejects_symlinked_artifacts() {
    let temporary = TempDir::new().expect("temporary directory");
    initialize_git_repository(temporary.path());
    let fixture = start_daemon(temporary.path());
    let first = up_environment(&fixture.socket, temporary.path(), "destroy-first");
    let second = up_environment(&fixture.socket, temporary.path(), "destroy-second");
    let artifacts = fixture.state_dir.join("artifacts").join(first.to_string());
    fs::create_dir_all(&artifacts).expect("artifact directory");
    fs::write(artifacts.join("evidence.png"), "fixture").expect("artifact writes");

    let dry_run = request(
        &fixture.socket,
        DaemonRequest::Destroy(DestroyRequest {
            environment_id: first,
            dry_run: true,
            authorization: DestroyAuthorization::None,
            purge_data: false,
            purge_artifacts: false,
        }),
    )
    .expect("destroy preview");
    let DaemonResponse::Destroy(dry_run) = dry_run else {
        panic!("expected destroy preview");
    };
    assert!(dry_run
        .targets
        .iter()
        .any(|target| target.kind == "process" && target.status == DestroyTargetStatus::Planned));
    assert!(
        request(
            &fixture.socket,
            DaemonRequest::Destroy(DestroyRequest {
                environment_id: first,
                dry_run: false,
                authorization: DestroyAuthorization::None,
                purge_data: false,
                purge_artifacts: false,
            }),
        )
        .is_err()
    );

    let destroyed = request(
        &fixture.socket,
        DaemonRequest::Destroy(DestroyRequest {
            environment_id: first,
            dry_run: false,
            authorization: DestroyAuthorization::Confirmed,
            purge_data: true,
            purge_artifacts: true,
        }),
    )
    .expect("owned environment destroys");
    let DaemonResponse::Destroy(destroyed) = destroyed else {
        panic!("expected destroy result");
    };
    assert!(
        destroyed
            .targets
            .iter()
            .any(|target| target.kind == "process"
                && target.status == DestroyTargetStatus::Destroyed)
    );
    assert!(!artifacts.exists());
    let DaemonResponse::Environment(first_status) = request(
        &fixture.socket,
        DaemonRequest::Status {
            environment_id: first,
        },
    )
    .expect("destroyed status") else {
        panic!("expected environment status");
    };
    assert_eq!(first_status.lifecycle, LifecycleState::Destroyed);
    let DaemonResponse::Environment(second_status) = request(
        &fixture.socket,
        DaemonRequest::Status {
            environment_id: second,
        },
    )
    .expect("other environment status") else {
        panic!("expected other environment status");
    };
    assert_eq!(second_status.lifecycle, LifecycleState::Running);
    request(
        &fixture.socket,
        DaemonRequest::Destroy(DestroyRequest {
            environment_id: first,
            dry_run: false,
            authorization: DestroyAuthorization::Confirmed,
            purge_data: true,
            purge_artifacts: true,
        }),
    )
    .expect("destroy retry is idempotent");

    let malicious = up_environment(&fixture.socket, temporary.path(), "destroy-malicious");
    let external = temporary.path().join("foreign-evidence");
    fs::create_dir_all(&external).expect("foreign directory");
    fs::write(external.join("keep.txt"), "foreign").expect("foreign artifact");
    let malicious_root = fixture.state_dir.join("artifacts");
    std::os::unix::fs::symlink(&external, malicious_root.join(malicious.to_string()))
        .expect("malicious link");
    assert!(
        request(
            &fixture.socket,
            DaemonRequest::Destroy(DestroyRequest {
                environment_id: malicious,
                dry_run: false,
                authorization: DestroyAuthorization::Confirmed,
                purge_data: false,
                purge_artifacts: true,
            }),
        )
        .is_err()
    );
    assert_eq!(
        fs::read_to_string(external.join("keep.txt")).expect("foreign artifact remains"),
        "foreign"
    );
    stop(&fixture.socket, second);
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
    wait_for_daemon(&socket);
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

fn wait_for_operation(
    socket: &Path,
    operation_id: uuid::Uuid,
) -> devfleet_protocol::OperationSnapshot {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let DaemonResponse::Operation(operation) =
            request(socket, DaemonRequest::OperationGet { operation_id }).expect("operation reads")
        else {
            panic!("expected operation response");
        };
        if matches!(
            operation.status,
            devfleet_protocol::OperationStatus::Completed
                | devfleet_protocol::OperationStatus::Failed
                | devfleet_protocol::OperationStatus::Cancelled
                | devfleet_protocol::OperationStatus::Uncertain
        ) {
            return operation;
        }
        assert!(
            Instant::now() < deadline,
            "operation did not become terminal"
        );
        thread::sleep(Duration::from_millis(25));
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
