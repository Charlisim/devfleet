//! Per-environment owner of one process group and its private control socket.

use anyhow::Context as _;
use devfleet_protocol::{
    HostSpawnRequest, JsonRpcRequest, JsonRpcResponse, PROTOCOL_VERSION, PluginAction, PluginCall,
    PluginReconcileRequest, ProcessSnapshot, ReadinessState, SupervisorRequest, SupervisorResponse,
    SupervisorStartRequest, read_json_line, write_json_line,
};
use nix::{
    errno::Errno,
    sys::signal::{Signal, kill},
    unistd::{Pid, getpeereid, getuid},
};
use std::{
    fs,
    io::{self, BufReader, Write},
    os::unix::{fs::PermissionsExt, net::UnixListener, process::CommandExt},
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const SIGNAL_WAIT: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const INITIAL_READINESS_WINDOW: Duration = Duration::from_millis(50);

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
    let spawn_request =
        request_spawn(process_plugin, &initial).context("request process provider spawn")?;
    let mut child = spawn(&spawn_request).context("spawn provider process")?;
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
    fs::set_permissions(control_socket, fs::Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;

    loop {
        observe_child(&mut child, &mut snapshot)?;
        match listener.accept() {
            Ok((stream, _)) => {
                if handle_control(stream, &mut child, &mut snapshot)? {
                    return Ok(());
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => thread::sleep(POLL_INTERVAL),
            Err(error) => return Err(error.into()),
        }
    }
}

fn request_spawn(
    plugin: &Path,
    initial: &SupervisorStartRequest,
) -> anyhow::Result<HostSpawnRequest> {
    let request = JsonRpcRequest {
        jsonrpc: "2.0".into(),
        id: initial.operation_id,
        method: "plugin.call".into(),
        params: serde_json::to_value(PluginCall {
            protocol: PROTOCOL_VERSION,
            operation_id: initial.operation_id,
            plugin_state_version: 1,
            action: PluginAction::Reconcile(PluginReconcileRequest {
                protocol: PROTOCOL_VERSION,
                operation_id: initial.operation_id,
                process: initial.process.clone(),
            }),
        })?,
    };
    let mut plugin_process = Command::new(plugin)
        .arg("--serve")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let mut stdin = plugin_process
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("plugin stdin unavailable"))?;
    stdin.write_all(&write_json_line(&request)?)?;
    drop(stdin);
    let output = plugin_process.wait_with_output()?;
    if !output.status.success() {
        anyhow::bail!("official.process exited unsuccessfully");
    }
    let response =
        read_json_line::<JsonRpcResponse>(&mut BufReader::new(output.stdout.as_slice()))?;
    if response.id != request.id {
        anyhow::bail!("official.process response identity does not match request");
    }
    if let Some(error) = response.error {
        anyhow::bail!("official.process rejected the request: {}", error.message);
    }
    let result = response
        .result
        .ok_or_else(|| anyhow::anyhow!("official.process returned no result"))?;
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
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    Ok(command.spawn()?)
}

fn handle_control(
    mut stream: std::os::unix::net::UnixStream,
    child: &mut Child,
    snapshot: &mut ProcessSnapshot,
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
    };
    let rpc = JsonRpcResponse::success(request.id, &response);
    stream.write_all(&write_json_line(&rpc)?)?;
    stream.flush()?;
    Ok(exit_after_response)
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
