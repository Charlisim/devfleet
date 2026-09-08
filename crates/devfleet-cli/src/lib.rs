//! CLI control client for the private local daemon socket.

use devfleet_protocol::{
    DaemonRequest, DaemonResponse, JsonRpcRequest, JsonRpcResponse, RpcFailure, read_json_line,
    write_json_line,
};
use std::{
    io::{BufReader, Write},
    os::unix::net::UnixStream,
    path::Path,
};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("cannot connect to the local daemon: {0}")]
    Connect(#[source] std::io::Error),
    #[error("daemon socket I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("daemon protocol failed: {0}")]
    Protocol(#[from] devfleet_protocol::ProtocolError),
    #[error("daemon rejected the request: {0}")]
    Rejected(String),
    #[error("daemon response did not match the request")]
    MismatchedResponse,
    #[error("daemon response payload is invalid")]
    InvalidPayload,
}

/// Sends one versioned request to the private daemon and waits for its response.
///
/// # Errors
///
/// Returns an error for connection failures, malformed bounded frames, typed
/// daemon rejections, or a response whose ID differs from the request ID.
pub fn request(socket: &Path, command: DaemonRequest) -> Result<DaemonResponse, ClientError> {
    let mut stream = UnixStream::connect(socket).map_err(ClientError::Connect)?;
    let request = JsonRpcRequest {
        jsonrpc: "2.0".into(),
        id: Uuid::now_v7(),
        method: "daemon.request".into(),
        params: serde_json::to_value(command).map_err(|_| ClientError::InvalidPayload)?,
    };
    stream.write_all(&write_json_line(&request)?)?;
    stream.flush()?;
    let response = read_json_line::<JsonRpcResponse>(&mut BufReader::new(stream))?;
    if response.id != request.id {
        return Err(ClientError::MismatchedResponse);
    }
    if let Some(error) = &response.error {
        return Err(rejection(error));
    }
    let result = response.result.ok_or(ClientError::InvalidPayload)?;
    serde_json::from_value(result).map_err(|_| ClientError::InvalidPayload)
}

fn rejection(error: &RpcFailure) -> ClientError {
    ClientError::Rejected(format!(
        "{}: {}",
        serde_json::to_string(&error.code).unwrap_or_default(),
        error.message
    ))
}
