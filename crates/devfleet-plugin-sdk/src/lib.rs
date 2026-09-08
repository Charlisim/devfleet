//! SDK boundary for trusted, independent Rust provider binaries.

use devfleet_protocol::{PROTOCOL_VERSION, ProtocolError, ProtocolVersion};
use semver::Version;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProviderDescriptor {
    pub id: String,
    pub version: Version,
    pub protocol: ProtocolVersion,
    pub capabilities: Vec<String>,
    pub plugin_state_version: u32,
}

#[derive(Debug, Error)]
pub enum PluginError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("provider protocol major version is incompatible")]
    IncompatibleProtocol,
}

pub trait Provider: Send + Sync {
    fn descriptor(&self) -> ProviderDescriptor;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_an_incompatible_provider_before_execution() {
        let descriptor = ProviderDescriptor {
            id: "example".into(),
            version: Version::new(1, 0, 0),
            protocol: ProtocolVersion { major: 2, minor: 0 },
            capabilities: Vec::new(),
            plugin_state_version: 1,
        };
        assert!(matches!(
            negotiate(&descriptor),
            Err(PluginError::IncompatibleProtocol)
        ));
    }
}
