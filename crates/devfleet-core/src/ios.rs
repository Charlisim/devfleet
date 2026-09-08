//! Strict iOS simulator selection. Names are display data, never identities.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IosDevice {
    pub udid: String,
    pub name: String,
    pub runtime: String,
    pub available: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeviceSelection {
    pub udid: String,
    pub runtime: String,
    pub created: bool,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum IosError {
    #[error("requested iOS runtime '{0}' is unavailable")]
    RuntimeUnavailable(String),
    #[error("requested iOS device type '{0}' is unavailable for the pinned runtime")]
    DeviceUnavailable(String),
    #[error("owned simulator '{0}' is unavailable")]
    OwnedDeviceUnavailable(String),
}

/// Selects only a retained UDID, otherwise requests a fresh reservation.
///
/// # Errors
///
/// Returns an error when the pinned runtime, device type, or retained UDID is unavailable.
pub fn select_owned_device(
    devices: &[IosDevice],
    owned_udid: Option<&str>,
    runtime: &str,
    device_type: &str,
    unavailable_udids: &BTreeSet<String>,
) -> Result<DeviceSelection, IosError> {
    if let Some(udid) = owned_udid {
        let device = devices
            .iter()
            .find(|device| device.udid == udid)
            .filter(|device| device.available && !unavailable_udids.contains(&device.udid))
            .ok_or_else(|| IosError::OwnedDeviceUnavailable(udid.into()))?;
        return Ok(DeviceSelection {
            udid: device.udid.clone(),
            runtime: device.runtime.clone(),
            created: false,
        });
    }
    if !devices
        .iter()
        .any(|device| device.runtime == runtime && device.available)
    {
        return Err(IosError::RuntimeUnavailable(runtime.into()));
    }
    if !devices
        .iter()
        .any(|device| device.runtime == runtime && device.name == device_type && device.available)
    {
        return Err(IosError::DeviceUnavailable(device_type.into()));
    }
    Ok(DeviceSelection {
        // An unowned device is never a candidate. The caller must create and
        // persist a new device before it can issue a UDID-scoped operation.
        udid: String::new(),
        runtime: runtime.into(),
        created: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuses_only_owned_udids_and_never_adopts_an_unrelated_device() {
        let devices = vec![
            IosDevice {
                udid: "owned-a".into(),
                name: "iPhone A".into(),
                runtime: "iOS-18".into(),
                available: true,
            },
            IosDevice {
                udid: "owned-b".into(),
                name: "iPhone A".into(),
                runtime: "iOS-18".into(),
                available: true,
            },
            IosDevice {
                udid: "personal".into(),
                name: "iPhone A".into(),
                runtime: "iOS-18".into(),
                available: true,
            },
        ];
        let unavailable = BTreeSet::from(["owned-a".into(), "personal".into()]);
        assert_eq!(
            select_owned_device(
                &devices,
                Some("owned-b"),
                "iOS-18",
                "iPhone A",
                &unavailable
            )
            .expect("owned device"),
            DeviceSelection {
                udid: "owned-b".into(),
                runtime: "iOS-18".into(),
                created: false
            }
        );
        assert!(
            select_owned_device(&devices, None, "iOS-18", "iPhone A", &unavailable)
                .expect("new device reservation")
                .created
        );
        assert!(matches!(
            select_owned_device(&devices, None, "iOS-99", "iPhone A", &BTreeSet::new()),
            Err(IosError::RuntimeUnavailable(_))
        ));
    }
}
