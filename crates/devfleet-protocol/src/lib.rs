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
pub const PREPARATION_DEFAULT_TIMEOUT_SECONDS: u64 = 900;

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
    CursorExpired,
    OwnershipUncertain,
    DeviceUnavailable,
    CapabilityUnsupported,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComposeUpRequest {
    pub worktree: String,
    pub profile: String,
    pub instance: Option<String>,
    pub trusted: bool,
    pub files: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub endpoints: Vec<ComposeEndpoint>,
    pub authorizations: ComposeAuthorizations,
}

/// An explicitly authorized teardown request. `dry_run` never invokes a
/// provider or removes local state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DestroyRequest {
    pub environment_id: Uuid,
    pub dry_run: bool,
    pub authorization: DestroyAuthorization,
    pub purge_data: bool,
    pub purge_artifacts: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestroyAuthorization {
    None,
    Confirmed,
    Noninteractive,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComposeDestroyRequest {
    pub worktree: String,
    pub project_name: String,
    pub files: Vec<String>,
    pub purge_data: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InitializationRunRequest {
    pub environment_id: Uuid,
    pub tasks: Vec<InitializationTask>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PreparationRunRequest {
    pub environment_id: Uuid,
    pub timeout_seconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BindingApplyRequest {
    pub environment_id: Uuid,
    pub phase: BindingPhase,
    pub integration: BindingIntegration,
    pub public_values: BTreeMap<String, String>,
    pub backend_only_values: BTreeMap<String, String>,
    pub reserved_endpoints: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingPhase {
    Process,
    Bundle,
    NativeBuild,
    Runtime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingIntegration {
    ProcessEnvironment,
    BundleConfig,
    NativeBuildSettings,
    RuntimeConfiguration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingInvalidation {
    Restart,
    Reload,
    Rebuild,
    Reconnect,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BindingSnapshot {
    pub phase: BindingPhase,
    pub integration: BindingIntegration,
    pub fingerprint: String,
    pub invalidation: BindingInvalidation,
    pub public_values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IosSimulatorAcquireRequest {
    pub environment_id: Uuid,
    /// Installed `CoreSimulator` runtime identifier, pinned by the caller.
    pub runtime: String,
    /// Installed `CoreSimulator` device type identifier, pinned by the caller.
    pub device_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IosSimulatorActionRequest {
    pub environment_id: Uuid,
    pub action: IosSimulatorAction,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IosAppRunRequest {
    pub environment_id: Uuid,
    pub bundle_id: String,
    pub scheme: Option<String>,
    pub verification: VerificationMode,
    pub backend_identity: Option<String>,
    pub backend_identity_url: Option<String>,
    pub identity_hook_module: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationMode {
    Transport,
    AppHandshake,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReactNativeIdentityHook {
    pub callback_url: String,
    pub authorization_token: String,
    pub nonce: String,
    pub environment_id: Uuid,
    pub generation: u64,
    pub build_identity: String,
    pub bundle_identity: String,
    pub backend_identity_url: String,
    pub expected_backend_identity: String,
    pub module: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IosAppSnapshot {
    pub environment_id: Uuid,
    pub bundle_id: String,
    pub udid: String,
    pub metro_port: u16,
    pub derived_data: String,
    pub launched: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityVerificationStartRequest {
    pub environment_id: Uuid,
    pub backend_identity: String,
    pub backend_identity_url: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeviceCaptureRequest {
    pub environment_id: Uuid,
    pub recording_seconds: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArtifactSnapshot {
    pub artifact_id: Uuid,
    pub timestamp_unix_ms: u64,
    pub environment_id: Uuid,
    pub generation: u64,
    pub worktree: String,
    pub head: String,
    pub code_fingerprint: String,
    pub build_identity: String,
    pub udid: String,
    pub kind: ArtifactKind,
    pub command: String,
    pub flow: String,
    pub result: String,
    pub app_running: bool,
    pub functional_result: String,
    pub human_review: String,
    pub path: String,
    pub obsolete: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    Screenshot,
    Recording,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityVerificationSnapshot {
    pub lease_id: Uuid,
    pub environment_id: Uuid,
    pub generation: u64,
    pub mode: VerificationMode,
    pub verification_url: String,
    pub nonce: String,
    pub token: String,
    pub build_identity: String,
    pub bundle_identity: String,
    pub status: IdentityVerificationStatus,
    pub diagnostic: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityVerificationStatus {
    Pending,
    Verified,
    Mismatch,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SupervisorIdentityVerificationRequest {
    pub environment_id: Uuid,
    pub generation: u64,
    pub build_identity: String,
    pub bundle_identity: String,
    pub backend_identity: String,
    pub backend_identity_url: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IosSimulatorAction {
    Open,
    Stop,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IosSimulatorSnapshot {
    pub environment_id: Uuid,
    pub udid: String,
    pub runtime: String,
    pub device_type: String,
    pub lifecycle: IosSimulatorLifecycle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IosSimulatorLifecycle {
    Reserved,
    Booted,
    Stopped,
    Uncertain,
    Destroyed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InitializationTask {
    pub id: String,
    pub kind: InitializationTaskKind,
    pub depends_on: Vec<String>,
    pub rerun_policy: TaskRerunPolicy,
    pub process: ProcessSpec,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitializationTaskKind {
    Migration,
    Seed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskRerunPolicy {
    Never,
    Always,
    OnEnvironmentGeneration,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InitializationTaskSnapshot {
    pub id: String,
    pub kind: InitializationTaskKind,
    pub rerun_policy: TaskRerunPolicy,
    pub generation: u64,
    pub attempts: u64,
    pub status: InitializationTaskStatus,
    pub diagnostic: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitializationTaskStatus {
    Running,
    Completed,
    Failed,
    Uncertain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparationTaskKind {
    Dependencies,
    Pods,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparationTaskStatus {
    Completed,
    Failed,
    Uncertain,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PreparationTaskSnapshot {
    pub kind: PreparationTaskKind,
    pub fingerprint: String,
    pub attempts: u64,
    pub status: PreparationTaskStatus,
    pub diagnostic: Option<String>,
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
    ComposeUp(ComposeUpRequest),
    MetroUp(MetroUpRequest),
    InitializationRun(InitializationRunRequest),
    PreparationRun(PreparationRunRequest),
    BindingApply(BindingApplyRequest),
    IosSimulatorAcquire(IosSimulatorAcquireRequest),
    IosSimulatorAction(IosSimulatorActionRequest),
    IosAppRun(IosAppRunRequest),
    IdentityVerificationStart(IdentityVerificationStartRequest),
    IdentityVerificationStatus {
        environment_id: Uuid,
    },
    DeviceCapture(DeviceCaptureRequest),
    Destroy(DestroyRequest),
    Status {
        environment_id: Uuid,
    },
    List,
    Stop {
        environment_id: Uuid,
    },
    OperationGet {
        operation_id: Uuid,
    },
    OperationCancel {
        operation_id: Uuid,
    },
    Events {
        environment_id: Uuid,
        after: Option<u64>,
    },
    #[serde(rename = "events.subscribe")]
    EventsSubscribe {
        environment_id: Uuid,
        after: Option<u64>,
    },
    LogsTail {
        environment_id: Uuid,
        resource: String,
        after: Option<u64>,
        limit: usize,
    },
    PluginList,
    PluginInstall {
        path: String,
    },
    PluginTrust {
        id: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum DaemonResponse {
    Plan(EnvironmentPlanSnapshot),
    Accepted(OperationAccepted),
    Environment(EnvironmentSnapshot),
    Environments(Vec<EnvironmentSnapshot>),
    Operation(OperationSnapshot),
    Events(Vec<EventSnapshot>),
    Logs(LogSnapshot),
    Plugin(PluginSnapshot),
    Plugins(Vec<PluginSnapshot>),
    InitializationTasks(Vec<InitializationTaskSnapshot>),
    PreparationTasks(Vec<PreparationTaskSnapshot>),
    Bindings(Vec<BindingSnapshot>),
    IosSimulator(IosSimulatorSnapshot),
    IosApp(IosAppSnapshot),
    IdentityVerification(IdentityVerificationSnapshot),
    Artifact(ArtifactSnapshot),
    Destroy(DestroySnapshot),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DestroySnapshot {
    pub environment_id: Uuid,
    pub dry_run: bool,
    pub targets: Vec<DestroyTarget>,
    pub retained: Vec<String>,
    pub purged: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DestroyTarget {
    pub kind: String,
    pub identifier: String,
    pub status: DestroyTargetStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestroyTargetStatus {
    Planned,
    Destroyed,
    Retained,
    OwnershipUncertain,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventSnapshot {
    pub sequence: u64,
    pub id: Uuid,
    pub timestamp_unix_ms: u128,
    pub environment_id: Uuid,
    pub resource_id: Option<String>,
    pub generation: u64,
    pub operation_id: Option<Uuid>,
    pub kind: String,
    pub payload: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LogSnapshot {
    pub environment_id: Uuid,
    pub resource: String,
    pub cursor: u64,
    pub reset: bool,
    pub lines: Vec<String>,
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
    pub bindings: Vec<BindingSnapshot>,
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
    pub endpoints: Vec<EndpointSnapshot>,
    pub readiness: EnvironmentReadiness,
    pub initialization_ready: bool,
    pub initialization_tasks: Vec<InitializationTaskSnapshot>,
    pub preparation_tasks: Vec<PreparationTaskSnapshot>,
    pub bindings: Vec<BindingSnapshot>,
    pub ios_simulator: Option<IosSimulatorSnapshot>,
    pub ios_app: Option<IosAppSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct EnvironmentReadiness {
    pub backend_healthy: bool,
    pub metro_ready: bool,
    pub device_booted: bool,
    pub app_launched: bool,
    pub identity_verified: bool,
    pub verification: VerificationMode,
    pub operational: bool,
    pub verified_ready: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EndpointSnapshot {
    pub name: String,
    pub service: String,
    pub internal_url: String,
    pub host_url: String,
    pub device_url: String,
    pub readiness_path: String,
    pub healthy: bool,
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
    Destroyed,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComposeEndpoint {
    pub name: String,
    pub service: String,
    pub container_port: u16,
    pub host_port: u16,
    pub readiness_path: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComposeAuthorizations {
    pub global_container_names: Option<String>,
    pub host_networking: Option<String>,
    pub external_resources: Option<String>,
    pub global_volume_names: Option<String>,
    pub unmanaged_ports: Option<String>,
    pub shared_mutable_binds: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComposeReconcileRequest {
    pub protocol: ProtocolVersion,
    pub operation_id: Uuid,
    pub environment_id: Uuid,
    pub worktree: String,
    pub project_name: String,
    pub files: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub endpoints: Vec<ComposeEndpoint>,
    pub authorizations: ComposeAuthorizations,
}

/// A fully resolved, environment-private Metro adapter invocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetroReconcileRequest {
    pub protocol: ProtocolVersion,
    pub operation_id: Uuid,
    pub environment_id: Uuid,
    pub worktree: String,
    pub port: u16,
    pub cache_directory: String,
    pub configuration_path: String,
    pub max_workers: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetroUpRequest {
    pub worktree: String,
    pub profile: String,
    pub instance: Option<String>,
    pub trusted: bool,
    pub port: u16,
    pub max_workers: u16,
    pub binding_integration: BindingIntegration,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReactNativeIosRunRequest {
    pub worktree: String,
    pub udid: String,
    pub metro_port: u16,
    pub derived_data: String,
    pub bundle_id: String,
    pub scheme: Option<String>,
    pub identity_hook: Option<ReactNativeIdentityHook>,
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
    ComposeReconcile(Box<ComposeReconcileRequest>),
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
    pub action: PluginAction,
    /// Private per-environment directory, created by the daemon before launch.
    pub log_directory: String,
    /// Per-resource disk cap. Payloads never enter daemon state or event rows.
    pub log_max_bytes: u64,
    /// Explicit values that should be redacted before a line reaches disk.
    pub redactions: Vec<String>,
    /// Immutable, checksum-pinned task providers allowed for this supervisor.
    pub task_plugins: Vec<TrustedTaskPlugin>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TrustedTaskPlugin {
    pub id: String,
    pub path: String,
    pub checksum: String,
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", content = "arguments", rename_all = "snake_case")]
pub enum SupervisorRequest {
    Inspect,
    Stop,
    RunTask {
        operation_id: Uuid,
        process: ProcessSpec,
    },
    RunPluginTask {
        operation_id: Uuid,
        plugin_id: String,
        action: PluginAction,
        timeout_seconds: Option<u64>,
    },
    StartIdentityVerification {
        request: SupervisorIdentityVerificationRequest,
    },
    InspectIdentityVerification,
    CancelIdentityVerification,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum SupervisorResponse {
    Process(ProcessSnapshot),
    TaskCompleted,
    TaskFailed { stdout: String, stderr: String },
    TaskTimedOut,
    TaskOutput { stdout: String, stderr: String },
    IdentityVerification(IdentityVerificationSnapshot),
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

    #[test]
    fn serializes_the_public_event_subscription_method() {
        let request = DaemonRequest::EventsSubscribe {
            environment_id: Uuid::nil(),
            after: Some(7),
        };
        let encoded = serde_json::to_value(request).expect("request serializes");
        assert_eq!(encoded["command"], "events.subscribe");
    }
}
