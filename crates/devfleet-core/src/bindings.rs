//! Typed, phase-specific endpoint bindings without project-file mutation.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

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
pub enum Invalidation {
    Restart,
    Reload,
    Rebuild,
    Reconnect,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BindingContract {
    pub phase: BindingPhase,
    pub integration: BindingIntegration,
    pub public_values: BTreeMap<String, String>,
    pub backend_only_values: BTreeMap<String, String>,
    pub fingerprint: String,
    pub invalidation: Invalidation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingIntegration {
    ProcessEnvironment,
    BundleConfig,
    NativeBuildSettings,
    RuntimeConfiguration,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum BindingError {
    #[error("binding key '{0}' is not a supported public application key")]
    InvalidPublicKey(String),
    #[error("binding key '{0}' attempts to override a reserved endpoint")]
    ReservedOverride(String),
    #[error("binding phase requires an incompatible integration")]
    InvalidIntegration,
    #[error("backend-only values cannot be delivered by a public integration")]
    SecretExposure,
}

/// Resolves a binding contract without reading or modifying project files.
///
/// # Errors
///
/// Returns an error when the phase/integration pair, public namespace, secret
/// delivery, or reserved endpoint precedence is invalid.
///
/// # Panics
///
/// Panics only if serializing Rust-owned binding values fails.
pub fn resolve_bindings(
    phase: BindingPhase,
    integration: BindingIntegration,
    public_values: BTreeMap<String, String>,
    backend_only_values: BTreeMap<String, String>,
    reserved_endpoints: BTreeMap<String, String>,
) -> Result<BindingContract, BindingError> {
    if !matches!(
        (phase, integration),
        (
            BindingPhase::Process,
            BindingIntegration::ProcessEnvironment
        ) | (BindingPhase::Bundle, BindingIntegration::BundleConfig)
            | (
                BindingPhase::NativeBuild,
                BindingIntegration::NativeBuildSettings
            )
            | (
                BindingPhase::Runtime,
                BindingIntegration::RuntimeConfiguration
            )
    ) {
        return Err(BindingError::InvalidIntegration);
    }
    if !backend_only_values.is_empty()
        && !matches!(phase, BindingPhase::Process | BindingPhase::Runtime)
    {
        return Err(BindingError::SecretExposure);
    }
    let reserved = reserved_endpoints.keys().collect::<BTreeSet<_>>();
    for key in public_values.keys() {
        if !key.starts_with("APP_PUBLIC_") {
            return Err(BindingError::InvalidPublicKey(key.clone()));
        }
        if reserved.contains(key) {
            return Err(BindingError::ReservedOverride(key.clone()));
        }
    }
    let mut values = public_values;
    values.extend(reserved_endpoints);
    let serialized = serde_json::to_vec(&(phase, integration, &values, &backend_only_values))
        .expect("binding values are serializable");
    Ok(BindingContract {
        phase,
        integration,
        public_values: values,
        backend_only_values,
        fingerprint: format!("{:x}", Sha256::digest(serialized)),
        invalidation: match phase {
            BindingPhase::Process => Invalidation::Restart,
            BindingPhase::Bundle => Invalidation::Reload,
            BindingPhase::NativeBuild => Invalidation::Rebuild,
            BindingPhase::Runtime => Invalidation::Reconnect,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applies_reserved_endpoints_last_and_rejects_public_secret_delivery() {
        let contract = resolve_bindings(
            BindingPhase::Runtime,
            BindingIntegration::RuntimeConfiguration,
            BTreeMap::from([("APP_PUBLIC_FEATURE".into(), "on".into())]),
            BTreeMap::from([("DATABASE_PASSWORD".into(), "private".into())]),
            BTreeMap::from([("APP_PUBLIC_API_URL".into(), "http://127.0.0.1:20001".into())]),
        )
        .expect("runtime contract");
        assert_eq!(contract.invalidation, Invalidation::Reconnect);
        assert!(
            resolve_bindings(
                BindingPhase::Bundle,
                BindingIntegration::BundleConfig,
                BTreeMap::new(),
                BTreeMap::from([("DATABASE_PASSWORD".into(), "private".into())]),
                BTreeMap::new(),
            )
            .is_err()
        );
    }
}
