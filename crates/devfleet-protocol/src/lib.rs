//! Stable, serializable contracts at the `DevFleet` process boundary.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::{BufRead, Read},
};
use thiserror::Error;
use uuid::Uuid;

pub const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion { major: 1, minor: 0 };
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    #[must_use]
    pub const fn compatible_with(self, other: Self) -> bool {
        self.major == other.major
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequestContext {
    pub request_id: Uuid,
    pub protocol: ProtocolVersion,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Uuid,
    pub method: String,
    pub params: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcFailure>,
}

impl JsonRpcResponse {
    /// Creates a successful JSON-RPC response from a serializable protocol DTO.
    ///
    /// # Panics
    ///
    /// Panics only if a DevFleet-owned DTO violates its `Serialize` contract.
    #[must_use]
    pub fn success<T: Serialize>(id: Uuid, result: &T) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: Some(serde_json::to_value(result).expect("serializable protocol DTO")),
            error: None,
        }
    }

    #[must_use]
    pub fn failure(id: Uuid, error: RpcFailure) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RpcFailure {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    ConfigInvalid,
    PermissionDenied,
    ProtocolInvalid,
    ProtocolIncompatible,
    ResourceNotFound,
    OperationFailed,
    OperationCancelled,
    OwnershipUncertain,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProcessSpec {
    pub argv: Vec<String>,
    pub cwd: String,
    pub environment: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentUpRequest {
    pub worktree: String,
    pub profile: String,
    pub instance: Option<String>,
    pub trusted: bool,
    pub process: ProcessSpec,
}

/// Inputs for a side-effect-free declarative plan preview. Secret values are
/// intentionally absent: callers may only declare a reference as available.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentPlanRequest {
    pub worktree: String,
    pub profile: String,
    pub manifest: String,
    pub local_override: Option<String>,
    pub secret_references: Vec<String>,
    pub operation_overrides: BTreeMap<String, Value>,
    pub runtime_outputs: BTreeMap<String, Value>,
    pub planning_endpoints: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", content = "arguments", rename_all = "snake_case")]
pub enum DaemonRequest {
    Plan(EnvironmentPlanRequest),
    Up(EnvironmentUpRequest),
    Status { environment_id: Uuid },
    List,
    Stop { environment_id: Uuid },
    OperationGet { operation_id: Uuid },
    OperationCancel { operation_id: Uuid },
    PluginList,
    PluginInstall { path: String },
    PluginTrust { id: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum DaemonResponse {
    Plan(EnvironmentPlanSnapshot),
    Accepted(OperationAccepted),
    Environment(EnvironmentSnapshot),
    Environments(Vec<EnvironmentSnapshot>),
    Operation(OperationSnapshot),
    Plugin(PluginSnapshot),
    Plugins(Vec<PluginSnapshot>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationSnapshot {
    pub operation_id: Uuid,
    pub environment_id: Uuid,
    pub status: OperationStatus,
    pub deadline_unix_ms: Option<u128>,
    pub phase: String,
    pub resource_id: Option<String>,
    pub retryable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Accepted,
    CancellationRequested,
    Cancelled,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentPlanSnapshot {
    pub worktree: String,
    pub profile: String,
    pub generation: u64,
    pub effective_configuration: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationAccepted {
    pub operation_id: Uuid,
    pub environment_id: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PluginSnapshot {
    pub id: String,
    pub path: String,
    pub platform: String,
    pub version: String,
    pub checksum: String,
    pub protocol: ProtocolVersion,
    pub plugin_state_version: u32,
    pub capabilities: Vec<String>,
    pub trusted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentSnapshot {
    pub environment_id: Uuid,
    pub project_id: Uuid,
    pub worktree_id: Uuid,
    pub worktree: String,
    pub profile: String,
    pub instance: Option<String>,
    pub generation: u64,
    pub lifecycle: LifecycleState,
    pub reconciliation_diagnostic: Option<String>,
    pub process: Option<ProcessSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProcessSnapshot {
    pub pid: u32,
    pub process_group: i32,
    pub started_at_unix_ms: u128,
    pub supervisor_socket: String,
    pub readiness: ReadinessState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    Starting,
    Running,
    Stopped,
    Failed,
    Degraded,
    Orphaned,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessState {
    ProcessAlive,
    ProcessStopped,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PluginReconcileRequest {
    pub protocol: ProtocolVersion,
    pub operation_id: Uuid,
    pub process: ProcessSpec,
}

/// One versioned SDK call delivered inside a bounded JSON-RPC frame.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PluginCall {
    pub protocol: ProtocolVersion,
    pub plugin_state_version: u32,
    pub operation_id: Uuid,
    pub action: PluginAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", content = "arguments", rename_all = "snake_case")]
pub enum PluginAction {
    Initialize,
    Validate { configuration: Value },
    Plan { configuration: Value },
    Inspect,
    Reconcile(PluginReconcileRequest),
    Stop,
    Destroy,
    Action { name: String, input: Value },
    Cancel,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HostExecutionContext {
    pub cwd: String,
    pub environment: BTreeMap<String, String>,
    pub artifact_root: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "request", content = "arguments", rename_all = "snake_case")]
pub enum HostRequest {
    Exec(HostExecRequest),
    Spawn(HostSpawnRequest),
    StopProcess { process_group: i32 },
    WriteArtifact(HostWriteArtifactRequest),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HostExecRequest {
    pub argv: Vec<String>,
    pub context: HostExecutionContext,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HostSpawnRequest {
    pub argv: Vec<String>,
    pub cwd: String,
    pub environment: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HostWriteArtifactRequest {
    pub relative_path: String,
    pub contents: Vec<u8>,
    pub context: HostExecutionContext,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SupervisorStartRequest {
    pub environment_id: Uuid,
    pub operation_id: Uuid,
    pub process: ProcessSpec,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", content = "arguments", rename_all = "snake_case")]
pub enum SupervisorRequest {
    Inspect,
    Stop,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum SupervisorResponse {
    Process(ProcessSnapshot),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SmokeResponse {
    pub component: String,
    pub protocol: ProtocolVersion,
    pub status: SmokeStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SmokeStatus {
    Ready,
}

#[derive(Clone, Debug, Error, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "code", content = "message", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProtocolError {
    #[error("protocol major version is incompatible")]
    IncompatibleVersion,
    #[error("frame exceeds the 1 MiB protocol limit")]
    FrameTooLarge,
    #[error("invalid JSON-RPC frame")]
    InvalidFrame,
    #[error("JSON-RPC frame must end in one newline")]
    MissingFrameTerminator,
    #[error("JSON-RPC payload does not match the expected DTO")]
    InvalidPayload,
}

/// Validates the framing limits before JSON-RPC parsing.
///
/// # Errors
///
/// Returns an error for an oversized or invalid UTF-8 frame.
pub fn validate_frame(frame: &[u8]) -> Result<(), ProtocolError> {
    if frame.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    if !frame.is_ascii() && std::str::from_utf8(frame).is_err() {
        return Err(ProtocolError::InvalidFrame);
    }
    Ok(())
}

/// Reads a bounded single-line JSON-RPC frame and deserializes its payload.
///
/// # Errors
///
/// Returns an error for oversized, unterminated, invalid UTF-8, or invalid JSON
/// frames.
pub fn read_json_line<T: DeserializeOwned>(reader: &mut impl BufRead) -> Result<T, ProtocolError> {
    let mut frame = Vec::new();
    reader
        .by_ref()
        .take((MAX_FRAME_BYTES + 1) as u64)
        .read_until(b'\n', &mut frame)
        .map_err(|_| ProtocolError::InvalidFrame)?;
    validate_frame(&frame)?;
    if !frame.ends_with(b"\n") {
        return Err(ProtocolError::MissingFrameTerminator);
    }
    serde_json::from_slice(&frame).map_err(|_| ProtocolError::InvalidPayload)
}

/// Serializes a DTO as one bounded JSON-RPC line.
///
/// # Errors
///
/// Returns an error if the serialized representation exceeds the protocol frame
/// limit.
pub fn write_json_line<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    let mut frame = serde_json::to_vec(value).map_err(|_| ProtocolError::InvalidPayload)?;
    frame.push(b'\n');
    validate_frame(&frame)?;
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_oversized_frames() {
        assert_eq!(
            validate_frame(&vec![b'x'; MAX_FRAME_BYTES + 1]),
            Err(ProtocolError::FrameTooLarge)
        );
    }

    #[test]
    fn requires_matching_major_protocol_version() {
        assert!(PROTOCOL_VERSION.compatible_with(ProtocolVersion { major: 1, minor: 5 }));
        assert!(!PROTOCOL_VERSION.compatible_with(ProtocolVersion { major: 2, minor: 0 }));
    }

    #[test]
    fn rejects_an_unterminated_frame() {
        let mut input = std::io::Cursor::new(b"{}".to_vec());
        let result = read_json_line::<Value>(&mut input);
        assert_eq!(result, Err(ProtocolError::MissingFrameTerminator));
    }
}
