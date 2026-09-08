//! Stable, serializable contracts at the `DevFleet` process boundary.

use serde::{Deserialize, Serialize};
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
}
