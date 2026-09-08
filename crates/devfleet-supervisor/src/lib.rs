//! Per-environment owner of one process group and its private control socket.

use anyhow::Context as _;
use devfleet_logging::LogStore;
use devfleet_protocol::{
    HostSpawnRequest, JsonRpcRequest, JsonRpcResponse, PROTOCOL_VERSION, PluginAction, PluginCall,
    PluginReconcileRequest, ProcessSnapshot, ProcessSpec, ReadinessState, SupervisorRequest,
    SupervisorResponse, SupervisorStartRequest, TrustedTaskPlugin, read_json_line, write_json_line,
};
use nix::{
    errno::Errno,
    poll::{PollFd, PollFlags, poll},
    sys::signal::{Signal, kill},
    unistd::{Pid, getpeereid, getuid},
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::{self, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    os::{
        fd::AsFd,
        unix::{fs::PermissionsExt, net::UnixListener, process::CommandExt},
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI32, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

const SIGNAL_WAIT: Duration = Duration::from_secs(2);
const TASK_SIGNAL_WAIT: Duration = Duration::from_millis(500);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const INITIAL_READINESS_WINDOW: Duration = Duration::from_millis(50);
const LOG_QUEUE_CAPACITY: usize = 128;
const LOG_READ_BUFFER_BYTES: usize = 4 * 1024;
const MAX_CAPTURED_LINE_BYTES: usize = 16 * 1024;
const MAX_SCOPED_EXEC_CAPTURE_BYTES: usize = devfleet_protocol::MAX_FRAME_BYTES;
const SCOPED_EXEC_QUEUE_CAPACITY: usize = 64;
const SCOPED_EXEC_DRAIN_BATCH_CHUNKS: usize = 64;

/// Starts one supervisor using a request received on standard input.
///
/// # Errors
///
/// Returns an error when the initial request, provider, process, or private
/// control socket cannot be created or controlled.
pub fn serve(control_socket: &Path, process_plugin: &Path) -> anyhow::Result<()> {
    if control_socket.exists() {
        anyhow::bail!("supervisor control socket already exists");
    }
    let initial = read_json_line::<SupervisorStartRequest>(&mut BufReader::new(io::stdin().lock()))
        .context("read supervisor start request")?;
    let task_plugins = initial.task_plugins.clone();
    let mut identity_verification = None;
    let mut scoped_exec_tasks = BTreeMap::new();
    let spawn_request = request_spawn(process_plugin, initial.operation_id, initial.action.clone())
        .context("request process provider spawn")?;
    let mut child = spawn(&spawn_request).context("spawn provider process")?;
    let logs = capture_logs(&mut child, &initial).context("capture provider process logs")?;
    let pid = i32::try_from(child.id()).map_err(|_| anyhow::anyhow!("PID exceeds i32"))?;
    let mut snapshot = ProcessSnapshot {
        pid: pid
            .try_into()
            .map_err(|_| anyhow::anyhow!("negative PID"))?,
        process_group: pid,
        started_at_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        supervisor_socket: control_socket.display().to_string(),
        readiness: ReadinessState::ProcessAlive,
    };
    thread::sleep(INITIAL_READINESS_WINDOW);
    observe_child(&mut child, &mut snapshot)?;
    let listener = UnixListener::bind(control_socket).with_context(|| {
        format!(
            "bind supervisor control socket {}",
            control_socket.display()
        )
    })?;
    let _socket_cleanup = SocketCleanup(control_socket.to_path_buf());
    fs::set_permissions(control_socket, fs::Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;

    loop {
        observe_child(&mut child, &mut snapshot)?;
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false)?;
                if handle_control(
                    stream,
                    &mut child,
                    &mut snapshot,
                    process_plugin,
                    &task_plugins,
                    &mut identity_verification,
                    &mut scoped_exec_tasks,
                )? {
                    cancel_scoped_exec_tasks(&scoped_exec_tasks);
                    logs.finish();
                    return Ok(());
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => thread::sleep(POLL_INTERVAL),
            Err(error) => return Err(error.into()),
        }
    }
}

struct SocketCleanup(PathBuf);

impl Drop for SocketCleanup {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn request_spawn(
    plugin: &Path,
    operation_id: Uuid,
    action: PluginAction,
) -> anyhow::Result<HostSpawnRequest> {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".into(),
        id: operation_id,
        method: "plugin.call".into(),
        params: serde_json::to_value(PluginCall {
            protocol: PROTOCOL_VERSION,
            operation_id,
            plugin_state_version: 1,
            action,
        })?,
    };
    let mut command = Command::new(plugin);
    command
        .arg("--serve")
        .env_clear()
        .env("PATH", "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    if let Some(home) = std::env::var_os("HOME") {
        command.env("HOME", home);
    }
    let mut plugin_process = command.spawn()?;
    let mut stdin = plugin_process
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("plugin stdin unavailable"))?;
    stdin.write_all(&write_json_line(&request)?)?;
    drop(stdin);
    let output = plugin_process.wait_with_output()?;
    if !output.status.success() {
        anyhow::bail!("provider exited unsuccessfully");
    }
    let response =
        read_json_line::<JsonRpcResponse>(&mut BufReader::new(output.stdout.as_slice()))?;
    if response.id != request.id {
        anyhow::bail!("provider response identity does not match request");
    }
    if let Some(error) = response.error {
        anyhow::bail!("provider rejected the request: {}", error.message);
    }
    let result = response
        .result
        .ok_or_else(|| anyhow::anyhow!("provider returned no result"))?;
    Ok(serde_json::from_value(result)?)
}

fn spawn(request: &HostSpawnRequest) -> anyhow::Result<Child> {
    let (program, arguments) = request
        .argv
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("official.process returned an empty argv"))?;
    let mut command = Command::new(program);
    command
        .args(arguments)
        .current_dir(&request.cwd)
        .env_clear()
        .envs(&request.environment)
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    Ok(command.spawn()?)
}

struct LogCapture {
    readers: Vec<JoinHandle<()>>,
    writer: JoinHandle<()>,
}

impl LogCapture {
    fn finish(self) {
        for reader in self.readers {
            let _ = reader.join();
        }
        let _ = self.writer.join();
    }
}

fn capture_logs(child: &mut Child, initial: &SupervisorStartRequest) -> anyhow::Result<LogCapture> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("provider process stdout unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("provider process stderr unavailable"))?;
    let logs = LogStore::new(
        PathBuf::from(&initial.log_directory),
        initial.log_max_bytes,
        initial.redactions.clone(),
    )?;
    let (sender, receiver) = mpsc::sync_channel(LOG_QUEUE_CAPACITY);
    let readers = vec![
        capture_stream(stdout, sender.clone()),
        capture_stream(stderr, sender),
    ];
    let writer = thread::spawn(move || {
        while let Ok(line) = receiver.recv() {
            if logs.append("process", &line).is_err() {
                break;
            }
        }
    });
    Ok(LogCapture { readers, writer })
}

fn capture_stream<R>(mut stream: R, sender: SyncSender<String>) -> JoinHandle<()>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut buffer = [0_u8; LOG_READ_BUFFER_BYTES];
        let mut line = Vec::with_capacity(MAX_CAPTURED_LINE_BYTES);
        loop {
            let read = match stream.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            for byte in &buffer[..read] {
                if *byte == b'\n' {
                    if sender
                        .send(String::from_utf8_lossy(&line).into_owned())
                        .is_err()
                    {
                        return;
                    }
                    line.clear();
                } else if line.len() < MAX_CAPTURED_LINE_BYTES {
                    line.push(*byte);
                }
            }
        }
        if !line.is_empty() {
            let _ = sender.send(String::from_utf8_lossy(&line).into_owned());
        }
    })
}

fn handle_control(
    mut stream: std::os::unix::net::UnixStream,
    child: &mut Child,
    snapshot: &mut ProcessSnapshot,
    process_plugin: &Path,
    task_plugins: &[TrustedTaskPlugin],
    identity_verification: &mut Option<IdentityVerification>,
    scoped_exec_tasks: &mut BTreeMap<Uuid, ScopedExecTask>,
) -> anyhow::Result<bool> {
    assert_same_user(&stream)?;
    let request = read_json_line::<JsonRpcRequest>(&mut BufReader::new(stream.try_clone()?))?;
    let action = serde_json::from_value::<SupervisorRequest>(request.params.clone())
        .map_err(|_| anyhow::anyhow!("supervisor request has invalid parameters"))?;
    let exit_after_response = matches!(action, SupervisorRequest::Stop);
    let response = match action {
        SupervisorRequest::Inspect => SupervisorResponse::Process(snapshot.clone()),
        SupervisorRequest::Stop => {
            cancel_scoped_exec_tasks(scoped_exec_tasks);
            stop_child(child, snapshot.process_group)?;
            snapshot.readiness = ReadinessState::ProcessStopped;
            SupervisorResponse::Process(snapshot.clone())
        }
        SupervisorRequest::RunTask {
            operation_id,
            process,
        } => run_task(process_plugin, operation_id, process)?,
        SupervisorRequest::Exec {
            operation_id,
            process,
            timeout_seconds,
        } => {
            if let std::collections::btree_map::Entry::Vacant(entry) =
                scoped_exec_tasks.entry(operation_id)
            {
                entry.insert(start_scoped_exec(&process, timeout_seconds)?);
            }
            SupervisorResponse::TaskAccepted
        }
        SupervisorRequest::ExecStatus { operation_id } => {
            scoped_exec_status(scoped_exec_tasks, operation_id)
        }
        SupervisorRequest::CancelExec { operation_id } => {
            scoped_exec_cancel(scoped_exec_tasks, operation_id)
        }
        SupervisorRequest::RunPluginTask {
            operation_id,
            plugin_id,
            action,
            timeout_seconds,
        } => run_plugin_task(
            task_plugins,
            operation_id,
            &plugin_id,
            action,
            timeout_seconds,
        )?,
        SupervisorRequest::StartIdentityVerification { request } => {
            let verification = IdentityVerification::start(request)?;
            let snapshot = verification.snapshot.clone();
            *identity_verification = Some(verification);
            SupervisorResponse::IdentityVerification(snapshot)
        }
        SupervisorRequest::InspectIdentityVerification => {
            let verification = identity_verification
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("identity verification has not started"))?;
            SupervisorResponse::IdentityVerification(verification.inspect())
        }
        SupervisorRequest::CancelIdentityVerification => {
            *identity_verification = None;
            SupervisorResponse::TaskCompleted
        }
    };
    let rpc = JsonRpcResponse::success(request.id, &response);
    stream.write_all(&write_json_line(&rpc)?)?;
    stream.flush()?;
    Ok(exit_after_response)
}

fn run_plugin_task(
    plugins: &[TrustedTaskPlugin],
    operation_id: Uuid,
    plugin_id: &str,
    action: PluginAction,
    timeout_seconds: Option<u64>,
) -> anyhow::Result<SupervisorResponse> {
    let PluginAction::Action {
        name: capability, ..
    } = &action
    else {
        anyhow::bail!("supervised plugin tasks only accept capability actions");
    };
    let plugin = plugins
        .iter()
        .find(|candidate| candidate.id == plugin_id)
        .filter(|candidate| candidate.capabilities.iter().any(|item| item == capability))
        .ok_or_else(|| anyhow::anyhow!("requested plugin capability is not authorized"))?;
    let path = Path::new(&plugin.path);
    let bytes = fs::read(path).context("read authorized plugin")?;
    if format!("{:x}", Sha256::digest(bytes)) != plugin.checksum {
        anyhow::bail!("authorized plugin checksum changed after supervisor start");
    }
    let request = request_spawn(path, operation_id, action)?;
    let (program, arguments) = request
        .argv
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("provider returned an empty argv"))?;
    let mut command = Command::new(program);
    command
        .args(arguments)
        .current_dir(&request.cwd)
        .env_clear()
        .envs(&request.environment)
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let output = if let Some(seconds) = timeout_seconds {
        run_task_with_deadline(command, seconds, &request.cwd)?
    } else {
        Some(
            command
                .output()
                .with_context(|| format!("run supervised plugin task in {}", request.cwd))?,
        )
    };
    task_output_response(output)
}

struct ScopedExecTask {
    cancellation: Arc<AtomicBool>,
    receiver: Receiver<SupervisorResponse>,
}

fn start_scoped_exec(
    process: &ProcessSpec,
    timeout_seconds: u64,
) -> anyhow::Result<ScopedExecTask> {
    if timeout_seconds == 0 {
        anyhow::bail!("scoped exec deadline must be nonzero");
    }
    let (program, arguments) = process
        .argv
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("scoped exec requires argv"))?;
    let mut command = Command::new(program);
    command
        .args(arguments)
        .current_dir(&process.cwd)
        .env_clear()
        .envs(&process.environment)
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = command
        .spawn()
        .with_context(|| format!("run scoped exec in {}", process.cwd))?;
    let process_group =
        i32::try_from(child.id()).map_err(|_| anyhow::anyhow!("PID exceeds i32"))?;
    let cancellation = Arc::new(AtomicBool::new(false));
    let worker_cancellation = Arc::clone(&cancellation);
    let worker_process_group = Arc::new(AtomicI32::new(process_group));
    let worker_group = Arc::clone(&worker_process_group);
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let response =
            run_scoped_exec_child(child, timeout_seconds, &worker_cancellation, &worker_group)
                .unwrap_or_else(|error| SupervisorResponse::TaskFailed {
                    stdout: String::new(),
                    stderr: error.to_string(),
                });
        let _ = sender.send(response);
    });
    Ok(ScopedExecTask {
        cancellation,
        receiver,
    })
}

fn scoped_exec_status(
    tasks: &mut BTreeMap<Uuid, ScopedExecTask>,
    operation_id: Uuid,
) -> SupervisorResponse {
    let Some(task) = tasks.get(&operation_id) else {
        return SupervisorResponse::TaskFailed {
            stdout: String::new(),
            stderr: "scoped exec operation is not owned by this supervisor".into(),
        };
    };
    match task.receiver.try_recv() {
        Ok(response) => {
            tasks.remove(&operation_id);
            response
        }
        Err(TryRecvError::Empty) => SupervisorResponse::TaskAccepted,
        Err(TryRecvError::Disconnected) => {
            tasks.remove(&operation_id);
            SupervisorResponse::TaskFailed {
                stdout: String::new(),
                stderr: "scoped exec worker stopped before reporting an outcome".into(),
            }
        }
    }
}

fn scoped_exec_cancel(
    tasks: &BTreeMap<Uuid, ScopedExecTask>,
    operation_id: Uuid,
) -> SupervisorResponse {
    let Some(task) = tasks.get(&operation_id) else {
        return SupervisorResponse::TaskFailed {
            stdout: String::new(),
            stderr: "scoped exec operation is not owned by this supervisor".into(),
        };
    };
    task.cancellation.store(true, Ordering::Release);
    SupervisorResponse::TaskAccepted
}

fn cancel_scoped_exec_tasks(tasks: &BTreeMap<Uuid, ScopedExecTask>) {
    for task in tasks.values() {
        task.cancellation.store(true, Ordering::Release);
    }
}

fn run_scoped_exec_child(
    mut child: Child,
    timeout_seconds: u64,
    cancellation: &AtomicBool,
    process_group: &AtomicI32,
) -> anyhow::Result<SupervisorResponse> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("scoped exec stdout unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("scoped exec stderr unavailable"))?;
    let (sender, receiver) = mpsc::sync_channel(SCOPED_EXEC_QUEUE_CAPACITY);
    let stop_capture = Arc::new(AtomicBool::new(false));
    drain_scoped_exec_stream(stdout, false, sender.clone(), Arc::clone(&stop_capture));
    drain_scoped_exec_stream(stderr, true, sender, Arc::clone(&stop_capture));
    let started = Instant::now();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut output_too_large = false;
    let mut stdout_closed = false;
    let mut stderr_closed = false;
    let mut exit_status = None;
    let mut stream_close_deadline = None;
    loop {
        drain_scoped_exec_output(
            &receiver,
            &mut stdout,
            &mut stderr,
            &mut output_too_large,
            &mut stdout_closed,
            &mut stderr_closed,
        );
        if cancellation.load(Ordering::Acquire) {
            stop_capture.store(true, Ordering::Release);
            stop_scoped_exec_child(&mut child, process_group.load(Ordering::Acquire))?;
            return Ok(SupervisorResponse::TaskCancelled);
        }
        if started.elapsed() >= Duration::from_secs(timeout_seconds) {
            stop_capture.store(true, Ordering::Release);
            stop_scoped_exec_child(&mut child, process_group.load(Ordering::Acquire))?;
            return Ok(SupervisorResponse::TaskTimedOut);
        }
        if exit_status.is_none()
            && let Some(status) = child.try_wait()?
        {
            exit_status = Some(status);
            stream_close_deadline = Some(Instant::now() + TASK_SIGNAL_WAIT);
        }
        if let Some(status) = exit_status {
            if stdout_closed && stderr_closed {
                if output_too_large {
                    return Ok(SupervisorResponse::TaskFailed {
                        stdout: String::new(),
                        stderr: "scoped exec output exceeds the protocol frame limit".into(),
                    });
                }
                return task_output_response_bytes(status, stdout, stderr);
            }
            if stream_close_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                stop_capture.store(true, Ordering::Release);
                return Ok(SupervisorResponse::TaskFailed {
                    stdout: String::new(),
                    stderr: "scoped exec output stream did not close after its process exited"
                        .into(),
                });
            }
            drain_scoped_exec_output(
                &receiver,
                &mut stdout,
                &mut stderr,
                &mut output_too_large,
                &mut stdout_closed,
                &mut stderr_closed,
            );
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn drain_scoped_exec_stream<R>(
    mut stream: R,
    stderr: bool,
    sender: SyncSender<(bool, Option<Vec<u8>>)>,
    stop_capture: Arc<AtomicBool>,
) where
    R: AsFd + Read + Send + 'static,
{
    thread::spawn(move || {
        let mut buffer = [0_u8; LOG_READ_BUFFER_BYTES];
        loop {
            if stop_capture.load(Ordering::Acquire) {
                break;
            }
            let mut fds = [PollFd::new(
                stream.as_fd(),
                PollFlags::POLLIN | PollFlags::POLLHUP,
            )];
            match poll(
                &mut fds,
                u16::try_from(POLL_INTERVAL.as_millis()).unwrap_or(25),
            ) {
                Ok(0) => continue,
                Ok(_) => {}
                Err(_) => break,
            }
            let read = match stream.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            if sender
                .send((stderr, Some(buffer[..read].to_vec())))
                .is_err()
            {
                return;
            }
        }
        let _ = sender.send((stderr, None));
    });
}

fn drain_scoped_exec_output(
    receiver: &Receiver<(bool, Option<Vec<u8>>)>,
    stdout: &mut Vec<u8>,
    stderr: &mut Vec<u8>,
    output_too_large: &mut bool,
    stdout_closed: &mut bool,
    stderr_closed: &mut bool,
) {
    for _ in 0..SCOPED_EXEC_DRAIN_BATCH_CHUNKS {
        let Ok((is_stderr, bytes)) = receiver.try_recv() else {
            return;
        };
        let Some(bytes) = bytes else {
            if is_stderr {
                *stderr_closed = true;
            } else {
                *stdout_closed = true;
            }
            continue;
        };
        if *output_too_large {
            continue;
        }
        let total = stdout.len().saturating_add(stderr.len());
        if total.saturating_add(bytes.len()) > MAX_SCOPED_EXEC_CAPTURE_BYTES {
            *output_too_large = true;
            stdout.clear();
            stderr.clear();
            continue;
        }
        if is_stderr {
            stderr.extend(bytes);
        } else {
            stdout.extend(bytes);
        }
    }
}

fn stop_scoped_exec_child(child: &mut Child, process_group: i32) -> anyhow::Result<()> {
    signal_group(process_group, Signal::SIGINT)?;
    let leader_exited = child.try_wait()?.is_some() || wait_for_exit(child, TASK_SIGNAL_WAIT)?;
    if leader_exited {
        thread::sleep(TASK_SIGNAL_WAIT);
    }
    signal_group(process_group, Signal::SIGKILL)?;
    if child.try_wait()?.is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
    Ok(())
}

fn task_output_response(
    output: Option<std::process::Output>,
) -> anyhow::Result<SupervisorResponse> {
    let Some(output) = output else {
        return Ok(SupervisorResponse::TaskTimedOut);
    };
    task_output_response_bytes(output.status, output.stdout, output.stderr)
}

fn task_output_response_bytes(
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
) -> anyhow::Result<SupervisorResponse> {
    if stdout.len().saturating_add(stderr.len()) > devfleet_protocol::MAX_FRAME_BYTES {
        return Ok(SupervisorResponse::TaskFailed {
            stdout: String::new(),
            stderr: "supervised task output exceeds the protocol frame limit".into(),
        });
    }
    let stdout = String::from_utf8(stdout)
        .map_err(|_| anyhow::anyhow!("supervised task output is not UTF-8"))?;
    let stderr = String::from_utf8(stderr)
        .map_err(|_| anyhow::anyhow!("supervised task error output is not UTF-8"))?;
    let response = if status.success() {
        SupervisorResponse::TaskOutput { stdout, stderr }
    } else {
        SupervisorResponse::TaskFailed { stdout, stderr }
    };
    let rpc = JsonRpcResponse::success(Uuid::nil(), &response);
    if write_json_line(&rpc).is_err() {
        return Ok(SupervisorResponse::TaskFailed {
            stdout: String::new(),
            stderr: "supervised task output exceeds the protocol frame limit".into(),
        });
    }
    Ok(response)
}

fn run_task_with_deadline(
    mut command: Command,
    timeout_seconds: u64,
    cwd: &str,
) -> anyhow::Result<Option<std::process::Output>> {
    if timeout_seconds == 0 {
        anyhow::bail!("supervised plugin task deadline must be nonzero");
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("run supervised plugin task in {cwd}"))?;
    let started = SystemTime::now();
    loop {
        if child.try_wait()?.is_some() {
            return child.wait_with_output().map(Some).map_err(Into::into);
        }
        if started.elapsed()? >= Duration::from_secs(timeout_seconds) {
            if let Ok(process_group) = i32::try_from(child.id()) {
                let _ = signal_group(process_group, Signal::SIGINT);
            }
            if !wait_for_exit(&mut child, TASK_SIGNAL_WAIT)? {
                if let Ok(process_group) = i32::try_from(child.id()) {
                    let _ = signal_group(process_group, Signal::SIGKILL);
                }
                let _ = child.kill();
            }
            let _ = child.wait();
            return Ok(None);
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn run_task(
    plugin: &Path,
    operation_id: Uuid,
    process: ProcessSpec,
) -> anyhow::Result<SupervisorResponse> {
    let action = PluginAction::Reconcile(PluginReconcileRequest {
        protocol: PROTOCOL_VERSION,
        operation_id,
        process,
    });
    let request = request_spawn(plugin, operation_id, action)?;
    let (program, arguments) = request
        .argv
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("official.process returned an empty argv"))?;
    let status = Command::new(program)
        .args(arguments)
        .current_dir(&request.cwd)
        .env_clear()
        .envs(&request.environment)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .with_context(|| format!("run initialization task in {}", request.cwd))?;
    Ok(if status.success() {
        SupervisorResponse::TaskCompleted
    } else {
        SupervisorResponse::TaskFailed {
            stdout: String::new(),
            stderr: String::new(),
        }
    })
}

struct IdentityVerification {
    snapshot: devfleet_protocol::IdentityVerificationSnapshot,
    receiver: Receiver<IdentityOutcome>,
    cancelled: Arc<AtomicBool>,
}

struct IdentityOutcome {
    status: devfleet_protocol::IdentityVerificationStatus,
    diagnostic: Option<String>,
}

#[derive(serde::Deserialize)]
struct IdentityReport {
    environment_id: Uuid,
    generation: u64,
    build_identity: String,
    bundle_identity: String,
    backend_identity: String,
    backend_identity_url: String,
    nonce: String,
}

impl IdentityVerification {
    fn start(
        request: devfleet_protocol::SupervisorIdentityVerificationRequest,
    ) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let port = listener.local_addr()?.port();
        let nonce = Uuid::now_v7().to_string();
        let token = Uuid::now_v7().to_string();
        let snapshot = devfleet_protocol::IdentityVerificationSnapshot {
            lease_id: Uuid::now_v7(),
            environment_id: request.environment_id,
            generation: request.generation,
            mode: devfleet_protocol::VerificationMode::AppHandshake,
            verification_url: format!("http://127.0.0.1:{port}/identity"),
            nonce: nonce.clone(),
            token: token.clone(),
            build_identity: request.build_identity.clone(),
            bundle_identity: request.bundle_identity.clone(),
            status: devfleet_protocol::IdentityVerificationStatus::Pending,
            diagnostic: None,
        };
        let (sender, receiver) = mpsc::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let listener_cancelled = Arc::clone(&cancelled);
        thread::spawn(move || {
            serve_identity_listener(listener, request, nonce, token, sender, listener_cancelled);
        });
        Ok(Self {
            snapshot,
            receiver,
            cancelled,
        })
    }

    fn inspect(&mut self) -> devfleet_protocol::IdentityVerificationSnapshot {
        match self.receiver.try_recv() {
            Ok(outcome) => {
                self.snapshot.status = outcome.status;
                self.snapshot.diagnostic = outcome.diagnostic;
            }
            Err(TryRecvError::Disconnected)
                if self.snapshot.status
                    == devfleet_protocol::IdentityVerificationStatus::Pending =>
            {
                self.snapshot.status = devfleet_protocol::IdentityVerificationStatus::Mismatch;
                self.snapshot.diagnostic =
                    Some("verification listener stopped before a report".into());
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }
        self.snapshot.clone()
    }
}

impl Drop for IdentityVerification {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

#[allow(clippy::needless_pass_by_value)]
fn serve_identity_listener(
    listener: TcpListener,
    expected: devfleet_protocol::SupervisorIdentityVerificationRequest,
    nonce: String,
    token: String,
    sender: mpsc::Sender<IdentityOutcome>,
    cancelled: Arc<AtomicBool>,
) {
    let deadline = SystemTime::now() + Duration::from_secs(600);
    while SystemTime::now() < deadline && !cancelled.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                if let Some(outcome) = identity_report(stream, &expected, &nonce, &token) {
                    let _ = sender.send(outcome);
                    return;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(POLL_INTERVAL);
            }
            Err(_) => return,
        }
    }
    if !cancelled.load(Ordering::Acquire) {
        let _ = sender.send(IdentityOutcome {
            status: devfleet_protocol::IdentityVerificationStatus::Mismatch,
            diagnostic: Some("identity report deadline exceeded".into()),
        });
    }
}

fn identity_report(
    mut stream: TcpStream,
    expected: &devfleet_protocol::SupervisorIdentityVerificationRequest,
    nonce: &str,
    token: &str,
) -> Option<IdentityOutcome> {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let mut bytes = vec![0_u8; 16 * 1024];
    let read = stream.read(&mut bytes).ok()?;
    let request = std::str::from_utf8(&bytes[..read]).ok()?;
    let (headers, body) = request.split_once("\r\n\r\n")?;
    let authorized = headers
        .lines()
        .any(|line| line.eq_ignore_ascii_case(&format!("authorization: Bearer {token}")));
    if !headers.starts_with("POST /identity HTTP/") || !authorized {
        let _ = stream.write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n");
        return None;
    }
    let report: IdentityReport = serde_json::from_str(body).ok()?;
    let matches = report.environment_id == expected.environment_id
        && report.generation == expected.generation
        && report.build_identity == expected.build_identity
        && report.bundle_identity == expected.bundle_identity
        && report.backend_identity == expected.backend_identity
        && report.backend_identity_url == expected.backend_identity_url
        && report.nonce == nonce;
    let outcome = if matches {
        IdentityOutcome {
            status: devfleet_protocol::IdentityVerificationStatus::Verified,
            diagnostic: None,
        }
    } else {
        IdentityOutcome {
            status: devfleet_protocol::IdentityVerificationStatus::Mismatch,
            diagnostic: Some(
                "IDENTITY_MISMATCH: identity report does not match this launch".into(),
            ),
        }
    };
    let _ = stream.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n");
    Some(outcome)
}

fn assert_same_user(stream: &std::os::unix::net::UnixStream) -> anyhow::Result<()> {
    let (uid, _) = getpeereid(stream)?;
    if uid != getuid() {
        anyhow::bail!("supervisor control peer has a different uid");
    }
    Ok(())
}

fn observe_child(child: &mut Child, snapshot: &mut ProcessSnapshot) -> anyhow::Result<()> {
    if child.try_wait()?.is_some() {
        snapshot.readiness = ReadinessState::ProcessStopped;
    }
    Ok(())
}

fn stop_child(child: &mut Child, process_group: i32) -> anyhow::Result<()> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }
    for signal in [Signal::SIGINT, Signal::SIGTERM, Signal::SIGKILL] {
        signal_group(process_group, signal)?;
        if wait_for_exit(child, SIGNAL_WAIT)? {
            return Ok(());
        }
    }
    anyhow::bail!("owned process group did not exit after SIGKILL");
}

fn signal_group(process_group: i32, signal: Signal) -> anyhow::Result<()> {
    match kill(Pid::from_raw(-process_group), signal) {
        Ok(()) | Err(Errno::ESRCH) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn wait_for_exit(child: &mut Child, deadline: Duration) -> anyhow::Result<bool> {
    let started = SystemTime::now();
    loop {
        if child.try_wait()?.is_some() {
            return Ok(true);
        }
        if started.elapsed()? >= deadline {
            return Ok(false);
        }
        thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_reaps_the_owned_task_process() {
        let mut command = Command::new("/bin/sleep");
        command.arg("2");
        let started = SystemTime::now();
        let output =
            run_task_with_deadline(command, 1, "/tmp").expect("task deadline handles sleep");
        assert!(output.is_none());
        assert!(started.elapsed().expect("monotonic clock") < Duration::from_secs(2));
    }

    #[test]
    fn scoped_exec_drains_large_output_while_the_process_is_running() {
        let operation_id = Uuid::now_v7();
        let mut tasks = BTreeMap::new();
        tasks.insert(
            operation_id,
            start_scoped_exec(
                &ProcessSpec {
                    argv: vec![
                        "/bin/sh".into(),
                        "-c".into(),
                        "yes x | head -c 131072".into(),
                    ],
                    cwd: "/tmp".into(),
                    environment: BTreeMap::new(),
                },
                3,
            )
            .expect("scoped exec starts"),
        );
        let response = wait_for_scoped_exec(&mut tasks, operation_id);
        let SupervisorResponse::TaskOutput { stdout, .. } = response else {
            panic!("large scoped exec did not complete successfully");
        };
        assert_eq!(stdout.len(), 131_072);
    }

    #[test]
    fn scoped_exec_rejects_json_escaped_output_that_exceeds_one_frame() {
        let operation_id = Uuid::now_v7();
        let mut tasks = BTreeMap::new();
        tasks.insert(
            operation_id,
            start_scoped_exec(
                &ProcessSpec {
                    argv: vec![
                        "/bin/sh".into(),
                        "-c".into(),
                        "dd if=/dev/zero bs=600000 count=1 2>/dev/null | tr '\\000' '\"'".into(),
                    ],
                    cwd: "/tmp".into(),
                    environment: BTreeMap::new(),
                },
                3,
            )
            .expect("scoped exec starts"),
        );
        let response = wait_for_scoped_exec(&mut tasks, operation_id);
        let SupervisorResponse::TaskFailed { stderr, .. } = response else {
            panic!("oversized JSON escaped output must fail structurally");
        };
        assert!(stderr.contains("frame limit"));
    }

    #[test]
    fn scoped_exec_cancellation_reaps_an_int_ignoring_descendant() {
        let operation_id = Uuid::now_v7();
        let marker = std::env::temp_dir().join(format!("devfleet-descendant-{operation_id}"));
        let _ = fs::remove_file(&marker);
        let mut tasks = BTreeMap::new();
        tasks.insert(
            operation_id,
            start_scoped_exec(
                &ProcessSpec {
                    argv: vec![
                        "/bin/sh".into(),
                        "-c".into(),
                        format!(
                            "trap 'exit 0' INT; (trap '' INT; sleep 1; touch '{}') & wait",
                            marker.display()
                        ),
                    ],
                    cwd: "/tmp".into(),
                    environment: BTreeMap::new(),
                },
                5,
            )
            .expect("scoped exec starts"),
        );
        thread::sleep(POLL_INTERVAL);
        assert!(matches!(
            scoped_exec_cancel(&tasks, operation_id),
            SupervisorResponse::TaskAccepted
        ));
        let response = wait_for_scoped_exec(&mut tasks, operation_id);
        assert!(
            matches!(response, SupervisorResponse::TaskCancelled),
            "unexpected scoped exec cancellation response: {response:?}"
        );
        thread::sleep(Duration::from_millis(750));
        assert!(!marker.exists(), "cancellation left the descendant running");
    }

    fn wait_for_scoped_exec(
        tasks: &mut BTreeMap<Uuid, ScopedExecTask>,
        operation_id: Uuid,
    ) -> SupervisorResponse {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let response = scoped_exec_status(tasks, operation_id);
            if !matches!(response, SupervisorResponse::TaskAccepted) {
                return response;
            }
            assert!(Instant::now() < deadline, "scoped exec did not complete");
            thread::sleep(POLL_INTERVAL);
        }
    }

    #[test]
    fn identity_listener_verifies_only_the_expected_launch() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener binds");
        let address = listener.local_addr().expect("listener address");
        let expected = devfleet_protocol::SupervisorIdentityVerificationRequest {
            environment_id: Uuid::now_v7(),
            generation: 3,
            build_identity: "build-a".into(),
            bundle_identity: "com.example.app".into(),
            backend_identity: "backend-a".into(),
            backend_identity_url: "http://127.0.0.1:23001/devfleet-identity".into(),
        };
        let expected_for_thread = expected.clone();
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let (stream, _) = listener.accept().expect("report connects");
            sender
                .send(identity_report(
                    stream,
                    &expected_for_thread,
                    "nonce-a",
                    "token-a",
                ))
                .expect("report result sends");
        });
        let body = serde_json::json!({
            "environment_id": expected.environment_id,
            "generation": expected.generation,
            "build_identity": expected.build_identity,
            "bundle_identity": expected.bundle_identity,
            "backend_identity": expected.backend_identity,
            "backend_identity_url": expected.backend_identity_url,
            "nonce": "nonce-a",
        })
        .to_string();
        let mut stream = TcpStream::connect(address).expect("client connects");
        stream
            .write_all(
                format!(
                    "POST /identity HTTP/1.1\r\nAuthorization: Bearer token-a\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .expect("report writes");
        let outcome = receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("report receives")
            .expect("report accepted");
        assert_eq!(
            outcome.status,
            devfleet_protocol::IdentityVerificationStatus::Verified
        );
    }

    #[test]
    fn identity_listeners_reject_cross_wiring_and_verify_two_launches() {
        let first = identity_request("build-a", "com.example.first", "backend-a");
        let second = identity_request("build-b", "com.example.second", "backend-b");
        let mut crossed = IdentityVerification::start(first.clone()).expect("cross listener");
        send_identity_report(&crossed.snapshot, &second);
        assert_eq!(
            wait_for_identity(&mut crossed).status,
            devfleet_protocol::IdentityVerificationStatus::Mismatch
        );

        let mut first_listener =
            IdentityVerification::start(first.clone()).expect("first listener");
        let mut second_listener =
            IdentityVerification::start(second.clone()).expect("second listener");
        send_identity_report(&first_listener.snapshot, &first);
        send_identity_report(&second_listener.snapshot, &second);
        assert_eq!(
            wait_for_identity(&mut first_listener).status,
            devfleet_protocol::IdentityVerificationStatus::Verified
        );
        assert_eq!(
            wait_for_identity(&mut second_listener).status,
            devfleet_protocol::IdentityVerificationStatus::Verified
        );
    }

    fn identity_request(
        build_identity: &str,
        bundle_identity: &str,
        backend_identity: &str,
    ) -> devfleet_protocol::SupervisorIdentityVerificationRequest {
        devfleet_protocol::SupervisorIdentityVerificationRequest {
            environment_id: Uuid::now_v7(),
            generation: 3,
            build_identity: build_identity.into(),
            bundle_identity: bundle_identity.into(),
            backend_identity: backend_identity.into(),
            backend_identity_url: "http://127.0.0.1:23001/devfleet-identity".into(),
        }
    }

    fn send_identity_report(
        snapshot: &devfleet_protocol::IdentityVerificationSnapshot,
        request: &devfleet_protocol::SupervisorIdentityVerificationRequest,
    ) {
        let port = snapshot
            .verification_url
            .strip_prefix("http://127.0.0.1:")
            .and_then(|value| value.strip_suffix("/identity"))
            .expect("loopback URL")
            .parse::<u16>()
            .expect("port");
        let body = serde_json::json!({
            "environment_id": request.environment_id,
            "generation": request.generation,
            "build_identity": request.build_identity,
            "bundle_identity": request.bundle_identity,
            "backend_identity": request.backend_identity,
            "backend_identity_url": request.backend_identity_url,
            "nonce": snapshot.nonce,
        })
        .to_string();
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("listener connects");
        stream
            .write_all(
                format!(
                    "POST /identity HTTP/1.1\r\nAuthorization: Bearer {}\r\nContent-Length: {}\r\n\r\n{body}",
                    snapshot.token,
                    body.len()
                )
                .as_bytes(),
            )
            .expect("report writes");
    }

    fn wait_for_identity(
        verification: &mut IdentityVerification,
    ) -> devfleet_protocol::IdentityVerificationSnapshot {
        let deadline = SystemTime::now() + Duration::from_secs(2);
        loop {
            let snapshot = verification.inspect();
            if snapshot.status != devfleet_protocol::IdentityVerificationStatus::Pending {
                return snapshot;
            }
            assert!(
                SystemTime::now() < deadline,
                "identity report was not observed"
            );
            thread::sleep(POLL_INTERVAL);
        }
    }
}
