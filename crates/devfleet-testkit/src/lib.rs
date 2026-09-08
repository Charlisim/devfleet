//! Shared helpers for black-box binary and provider tests.

use devfleet_protocol::{PROTOCOL_VERSION, SmokeResponse, SmokeStatus};

/// Parses and validates a smoke response emitted by a `DevFleet` binary.
///
/// # Panics
///
/// Panics when the output is not a valid matching smoke response.
pub fn assert_smoke_output(output: &[u8], component: &str) {
    let response: SmokeResponse = serde_json::from_slice(output).expect("valid smoke JSON");
    assert_eq!(response.component, component);
    assert_eq!(response.protocol, PROTOCOL_VERSION);
    assert_eq!(response.status, SmokeStatus::Ready);
}
