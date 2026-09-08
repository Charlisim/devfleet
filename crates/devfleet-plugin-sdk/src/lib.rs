//! Versioned SDK boundary for trusted, independent Rust provider binaries.

use devfleet_protocol::{
    ComposeReconcileRequest, ErrorCode, JsonRpcRequest, JsonRpcResponse, MAX_FRAME_BYTES,
    PROTOCOL_VERSION, PluginAction, PluginCall, PluginReconcileRequest, ProtocolError,
    ProtocolVersion, RpcFailure, read_json_line, write_json_line,
};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::io::{BufRead, Write};
use thiserror::Error;

pub const SDK_METHOD: &str = "plugin.call";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProviderDescriptor {
    pub id: String,
    pub version: Version,
    pub platform: String,
    pub protocol: ProtocolVersion,
    pub capabilities: Vec<String>,
    pub plugin_state_version: u32,
}

#[derive(Debug, Error)]
pub enum PluginError {
    #[error("plugin transport I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("provider protocol major version is incompatible")]
    IncompatibleProtocol,
    #[error("provider state version {received} is incompatible with {supported}")]
    IncompatibleStateVersion { received: u32, supported: u32 },
    #[error("provider did not declare capability {0}")]
    UndeclaredCapability(String),
    #[error("plugin call is invalid: {0}")]
    InvalidCall(String),
    #[error("plugin does not implement {0}")]
    UnsupportedMethod(String),
    #[error("plugin request failed: {0}")]
    RequestFailed(String),
}

/// Public contract for native provider processes. The SDK offers no daemon
/// storage or plugin-to-plugin channel; effects cross a typed host boundary.
#[allow(clippy::missing_errors_doc)]
pub trait Provider: Send + Sync {
    fn descriptor(&self) -> ProviderDescriptor;

    fn initialize(&self) -> Result<Value, PluginError> {
        serde_json::to_value(self.descriptor())
            .map_err(|error| PluginError::RequestFailed(error.to_string()))
    }

    fn validate(&self, _configuration: Value) -> Result<Value, PluginError> {
        Ok(json!({}))
    }

    fn plan(&self, _configuration: Value) -> Result<Value, PluginError> {
        Ok(json!({}))
    }

    fn inspect(&self) -> Result<Value, PluginError> {
        Ok(json!({}))
    }

    fn reconcile(&self, _request: PluginReconcileRequest) -> Result<Value, PluginError> {
        Err(PluginError::UnsupportedMethod("reconcile".into()))
    }

    fn reconcile_compose(&self, _request: ComposeReconcileRequest) -> Result<Value, PluginError> {
        Err(PluginError::UnsupportedMethod("compose reconcile".into()))
    }

    fn stop(&self) -> Result<Value, PluginError> {
        Ok(json!({}))
    }

    fn destroy(&self) -> Result<Value, PluginError> {
        Ok(json!({}))
    }

    fn action(&self, _name: String, _input: Value) -> Result<Value, PluginError> {
        Err(PluginError::UnsupportedMethod("action".into()))
    }

    fn cancel(&self) -> Result<Value, PluginError> {
        Ok(json!({}))
    }
}

/// Checks whether the provider speaks the supported protocol major version.
///
/// # Errors
///
/// Returns an error before a provider executes when its protocol is incompatible.
pub fn negotiate(descriptor: &ProviderDescriptor) -> Result<(), PluginError> {
    if !PROTOCOL_VERSION.compatible_with(descriptor.protocol) {
        return Err(PluginError::IncompatibleProtocol);
    }
    Ok(())
}

/// Handles one bounded SDK request and emits exactly one JSON-RPC response.
///
/// # Errors
///
/// Returns an error only for framing or output failures. Provider rejections
/// become typed JSON-RPC failures for the daemon to persist.
pub fn serve_once<P: Provider>(
    provider: &P,
    reader: &mut impl BufRead,
    writer: &mut impl Write,
) -> Result<(), PluginError> {
    let request = read_json_line::<JsonRpcRequest>(reader)?;
    let response = handle_request(provider, request);
    writer.write_all(&write_json_line(&response)?)?;
    writer.flush()?;
    Ok(())
}

/// Dispatches one decoded JSON-RPC request without performing I/O.
#[must_use]
pub fn handle_request<P: Provider>(provider: &P, request: JsonRpcRequest) -> JsonRpcResponse {
    if request.jsonrpc != "2.0" || request.method != SDK_METHOD {
        return failure(
            request.id,
            &PluginError::InvalidCall("expected plugin.call JSON-RPC 2.0 method".into()),
        );
    }
    let Ok(call) = serde_json::from_value::<PluginCall>(request.params) else {
        return failure(
            request.id,
            &PluginError::InvalidCall("parameters do not match the SDK schema".into()),
        );
    };
    match dispatch(provider, call) {
        Ok(result) => JsonRpcResponse::success(request.id, &result),
        Err(error) => failure(request.id, &error),
    }
}

fn dispatch<P: Provider>(provider: &P, call: PluginCall) -> Result<Value, PluginError> {
    let descriptor = provider.descriptor();
    negotiate(&descriptor)?;
    if !call.protocol.compatible_with(descriptor.protocol) {
        return Err(PluginError::IncompatibleProtocol);
    }
    if call.plugin_state_version != descriptor.plugin_state_version {
        return Err(PluginError::IncompatibleStateVersion {
            received: call.plugin_state_version,
            supported: descriptor.plugin_state_version,
        });
    }
    let capability = capability(&call.action);
    if !descriptor
        .capabilities
        .iter()
        .any(|item| item == &capability)
    {
        return Err(PluginError::UndeclaredCapability(capability));
    }
    match call.action {
        PluginAction::Initialize => provider.initialize(),
        PluginAction::Validate { configuration } => provider.validate(configuration),
        PluginAction::Plan { configuration } => provider.plan(configuration),
        PluginAction::Inspect => provider.inspect(),
        PluginAction::Reconcile(request) => provider.reconcile(request),
        PluginAction::ComposeReconcile(request) => provider.reconcile_compose(*request),
        PluginAction::Stop => provider.stop(),
        PluginAction::Destroy => provider.destroy(),
        PluginAction::Action { name, input } => provider.action(name, input),
        PluginAction::Cancel => provider.cancel(),
    }
}

fn capability(action: &PluginAction) -> String {
    match action {
        PluginAction::Initialize => "lifecycle.initialize".into(),
        PluginAction::Validate { .. } => "lifecycle.validate".into(),
        PluginAction::Plan { .. } => "lifecycle.plan".into(),
        PluginAction::Inspect => "lifecycle.inspect".into(),
        PluginAction::Reconcile(_) => "process.reconcile".into(),
        PluginAction::ComposeReconcile(_) => "compose.reconcile".into(),
        PluginAction::Stop => "lifecycle.stop".into(),
        PluginAction::Destroy => "lifecycle.destroy".into(),
        PluginAction::Action { name, .. } => name.clone(),
        PluginAction::Cancel => "lifecycle.cancel".into(),
    }
}

fn failure(id: uuid::Uuid, error: &PluginError) -> JsonRpcResponse {
    let code = match error {
        PluginError::IncompatibleProtocol | PluginError::IncompatibleStateVersion { .. } => {
            ErrorCode::ProtocolIncompatible
        }
        PluginError::UndeclaredCapability(_) | PluginError::InvalidCall(_) => {
            ErrorCode::ConfigInvalid
        }
        PluginError::Io(_)
        | PluginError::UnsupportedMethod(_)
        | PluginError::RequestFailed(_)
        | PluginError::Protocol(_) => ErrorCode::OperationFailed,
    };
    JsonRpcResponse::failure(
        id,
        RpcFailure {
            code,
            message: error.to_string(),
            retryable: false,
        },
    )
}

/// Returns the maximum permitted payload for native SDK transports.
#[must_use]
pub const fn max_frame_bytes() -> usize {
    MAX_FRAME_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;
    use devfleet_protocol::{PluginAction, PluginCall};
    use uuid::Uuid;

    struct FixtureProvider {
        capabilities: Vec<String>,
        protocol: ProtocolVersion,
    }

    impl Provider for FixtureProvider {
        fn descriptor(&self) -> ProviderDescriptor {
            ProviderDescriptor {
                id: "fixture".into(),
                version: Version::new(1, 0, 0),
                platform: current_platform(),
                protocol: self.protocol,
                capabilities: self.capabilities.clone(),
                plugin_state_version: 1,
            }
        }
    }

    #[test]
    fn rejects_an_incompatible_provider_before_execution() {
        let descriptor = FixtureProvider {
            capabilities: Vec::new(),
            protocol: ProtocolVersion { major: 2, minor: 0 },
        }
        .descriptor();
        assert!(matches!(
            negotiate(&descriptor),
            Err(PluginError::IncompatibleProtocol)
        ));
    }

    #[test]
    fn rejects_undeclared_capabilities_with_a_typed_error() {
        let provider = FixtureProvider {
            capabilities: vec!["lifecycle.initialize".into()],
            protocol: PROTOCOL_VERSION,
        };
        let request = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Uuid::now_v7(),
            method: SDK_METHOD.into(),
            params: serde_json::to_value(PluginCall {
                protocol: PROTOCOL_VERSION,
                plugin_state_version: 1,
                operation_id: Uuid::now_v7(),
                action: PluginAction::Inspect,
            })
            .expect("serializable request"),
        };
        let response = handle_request(&provider, request);
        assert_eq!(
            response.error.expect("typed rejection").code,
            ErrorCode::ConfigInvalid
        );
    }

    #[test]
    fn action_capabilities_are_checked_by_the_exact_declared_name() {
        let provider = FixtureProvider {
            capabilities: vec!["device.boot".into()],
            protocol: PROTOCOL_VERSION,
        };
        let request = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Uuid::now_v7(),
            method: SDK_METHOD.into(),
            params: serde_json::to_value(PluginCall {
                protocol: PROTOCOL_VERSION,
                plugin_state_version: 1,
                operation_id: Uuid::now_v7(),
                action: PluginAction::Action {
                    name: "device.open".into(),
                    input: json!({}),
                },
            })
            .expect("serializable request"),
        };
        let response = handle_request(&provider, request);
        assert_eq!(
            response.error.expect("typed rejection").code,
            ErrorCode::ConfigInvalid
        );
    }

    fn current_platform() -> String {
        format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
    }
}
