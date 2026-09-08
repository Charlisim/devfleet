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
    sys::signal::{Signal, kill},
    unistd::{Pid, getpeereid, getuid},
};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, BufReader, Read, Write},
    os::unix::{fs::PermissionsExt, net::UnixListener, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc::{self, SyncSender},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

const SIGNAL_WAIT: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const INITIAL_READINESS_WINDOW: Duration = Duration::from_millis(50);
const LOG_QUEUE_CAPACITY: usize = 128;
const LOG_READ_BUFFER_BYTES: usize = 4 * 1024;
const MAX_CAPTURED_LINE_BYTES: usize = 16 * 1024;

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
                if handle_control(
                    stream,
                    &mut child,
                    &mut snapshot,
                    process_plugin,
                    &task_plugins,
                )? {
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
) -> anyhow::Result<bool> {
    assert_same_user(&stream)?;
    let request = read_json_line::<JsonRpcRequest>(&mut BufReader::new(stream.try_clone()?))?;
    let action = serde_json::from_value::<SupervisorRequest>(request.params.clone())
        .map_err(|_| anyhow::anyhow!("supervisor request has invalid parameters"))?;
    let exit_after_response = matches!(action, SupervisorRequest::Stop);
    let response = match action {
        SupervisorRequest::Inspect => SupervisorResponse::Process(snapshot.clone()),
        SupervisorRequest::Stop => {
            stop_child(child, snapshot.process_group)?;
            snapshot.readiness = ReadinessState::ProcessStopped;
            SupervisorResponse::Process(snapshot.clone())
        }
        SupervisorRequest::RunTask {
            operation_id,
            process,
        } => run_task(process_plugin, operation_id, process)?,
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
    let Some(output) = output else {
        return Ok(SupervisorResponse::TaskTimedOut);
    };
    if output.stdout.len().saturating_add(output.stderr.len()) > devfleet_protocol::MAX_FRAME_BYTES
    {
        return Ok(SupervisorResponse::TaskFailed {
            stdout: String::new(),
            stderr: "supervised task output exceeds the protocol frame limit".into(),
        });
    }
    let stdout = String::from_utf8(output.stdout)
        .map_err(|_| anyhow::anyhow!("supervised task output is not UTF-8"))?;
    let stderr = String::from_utf8(output.stderr)
        .map_err(|_| anyhow::anyhow!("supervised task error output is not UTF-8"))?;
    if !output.status.success() {
        return Ok(SupervisorResponse::TaskFailed { stdout, stderr });
    }
    Ok(SupervisorResponse::TaskOutput { stdout, stderr })
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
                let _ = signal_group(process_group, Signal::SIGKILL);
            }
            let _ = child.kill();
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
}
