//! Loopback port reservation and typed endpoint construction.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
};
use thiserror::Error;

pub const DEFAULT_PORT_RANGE: PortRange = PortRange {
    start: 20_000,
    end: 39_999,
};
pub const DEFAULT_BLOCK_SIZE: u16 = 64;
pub const DEFAULT_HANDOFF_ATTEMPTS: u8 = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortRange {
    pub start: u16,
    pub end: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortBlock {
    pub start: u16,
    pub size: u16,
    pub offsets: BTreeMap<String, u16>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointUrls {
    pub internal_url: String,
    pub host_url: String,
    pub device_url: String,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum PortError {
    #[error("port range is invalid")]
    InvalidRange,
    #[error("port block is outside the configured range")]
    BlockOutOfRange,
    #[error("semantic offset '{0}' is outside the block or duplicates another offset")]
    InvalidOffset(String),
    #[error("could not reserve loopback port {0}")]
    ReservationFailed(u16),
    #[error("endpoint ownership could not be verified after handoff")]
    PortConflict,
}

impl Default for PortRange {
    fn default() -> Self {
        DEFAULT_PORT_RANGE
    }
}

#[allow(clippy::missing_errors_doc)]
impl PortRange {
    pub fn validate(self) -> Result<(), PortError> {
        if self.start == 0 || self.start > self.end {
            return Err(PortError::InvalidRange);
        }
        Ok(())
    }
    #[must_use]
    pub fn block_starts(self, size: u16) -> Vec<u16> {
        if self.validate().is_err() || size == 0 {
            return Vec::new();
        }
        let mut starts = Vec::new();
        let mut current = u32::from(self.start);
        while current + u32::from(size) - 1 <= u32::from(self.end) {
            let Ok(port) = u16::try_from(current) else {
                return Vec::new();
            };
            starts.push(port);
            current += u32::from(size);
        }
        starts
    }
}

#[allow(clippy::missing_errors_doc)]
impl PortBlock {
    pub fn validate(&self, range: PortRange) -> Result<(), PortError> {
        range.validate()?;
        if self.size == 0
            || u32::from(self.start) + u32::from(self.size) - 1 > u32::from(range.end)
            || self.start < range.start
        {
            return Err(PortError::BlockOutOfRange);
        }
        let mut seen = BTreeSet::new();
        for (name, offset) in &self.offsets {
            if *offset >= self.size || !seen.insert(offset) {
                return Err(PortError::InvalidOffset(name.clone()));
            }
        }
        Ok(())
    }
    pub fn ports(&self, range: PortRange) -> Result<BTreeMap<String, u16>, PortError> {
        self.validate(range)?;
        Ok(self
            .offsets
            .iter()
            .map(|(name, offset)| (name.clone(), self.start + offset))
            .collect())
    }
}

impl EndpointUrls {
    #[must_use]
    pub fn loopback(port: u16, internal_authority: &str, device_authority: &str) -> Self {
        Self {
            internal_url: format!("http://{internal_authority}"),
            host_url: format!("http://127.0.0.1:{port}"),
            device_url: format!("http://{device_authority}"),
        }
    }
}

/// TCP listeners held until the provider takes ownership of each loopback port.
pub struct ReservedPorts {
    listeners: BTreeMap<String, TcpListener>,
    ports: BTreeMap<String, u16>,
}

#[allow(clippy::missing_errors_doc)]
impl ReservedPorts {
    pub fn bind(ports: BTreeMap<String, u16>) -> Result<Self, PortError> {
        let mut listeners = BTreeMap::new();
        for (name, port) in &ports {
            let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), *port);
            let listener =
                TcpListener::bind(address).map_err(|_| PortError::ReservationFailed(*port))?;
            listeners.insert(name.clone(), listener);
        }
        Ok(Self { listeners, ports })
    }
    #[must_use]
    pub fn ports(&self) -> &BTreeMap<String, u16> {
        &self.ports
    }
    pub fn handoff<F>(
        &mut self,
        launch: F,
        verify_owner: impl Fn(u16) -> bool,
    ) -> Result<(), PortError>
    where
        F: FnOnce() -> Result<(), PortError>,
    {
        self.listeners.clear();
        launch()?;
        if self.ports.values().all(|port| verify_owner(*port)) {
            Ok(())
        } else {
            Err(PortError::PortConflict)
        }
    }
}

/// Replans after a conflict at most three times; an unrelated listener is never accepted.
///
/// # Errors
///
/// Returns a launch failure immediately, or `PORT_CONFLICT` after bounded retries.
pub fn retry_handoff<T, F>(attempts: u8, mut operation: F) -> Result<T, PortError>
where
    F: FnMut() -> Result<T, PortError>,
{
    let attempts = attempts.clamp(1, DEFAULT_HANDOFF_ATTEMPTS);
    let mut last = PortError::PortConflict;
    for _ in 0..attempts {
        match operation() {
            Ok(value) => return Ok(value),
            Err(PortError::PortConflict) => last = PortError::PortConflict,
            Err(error) => return Err(error),
        }
    }
    Err(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_blocks_and_exhaustion_deterministically() {
        let range = PortRange {
            start: 20_000,
            end: 20_127,
        };
        assert_eq!(range.block_starts(64), vec![20_000, 20_064]);
        assert!(
            PortBlock {
                start: 20_064,
                size: 64,
                offsets: BTreeMap::from([("api".into(), 64)])
            }
            .validate(range)
            .is_err()
        );
        assert!(
            PortBlock {
                start: 20_000,
                size: 64,
                offsets: BTreeMap::from([("api".into(), 1), ("web".into(), 1)])
            }
            .validate(range)
            .is_err()
        );
    }

    #[test]
    fn rejects_a_competing_listener_during_handoff() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral port");
        let port = listener.local_addr().expect("address").port();
        drop(listener);
        let mut reserved =
            ReservedPorts::bind(BTreeMap::from([("api".into(), port)])).expect("reserves loopback");
        let competitor = std::sync::Mutex::new(None);
        let result = reserved.handoff(
            || {
                *competitor.lock().unwrap() =
                    Some(TcpListener::bind((Ipv4Addr::LOCALHOST, port)).expect("competitor binds"));
                Ok(())
            },
            |_| false,
        );
        assert_eq!(result, Err(PortError::PortConflict));
    }

    #[test]
    fn retries_conflicts_no_more_than_three_times() {
        let attempts = std::cell::Cell::new(0);
        let result = retry_handoff(9, || {
            attempts.set(attempts.get() + 1);
            Err::<(), _>(PortError::PortConflict)
        });
        assert_eq!(result, Err(PortError::PortConflict));
        assert_eq!(attempts.get(), 3);
    }
}
