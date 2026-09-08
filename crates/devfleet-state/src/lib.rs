//! Versioned SQLite storage. Domain orchestration remains in the daemon/core.

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;
use uuid::Uuid;

pub const CURRENT_SCHEMA_VERSION: i64 = 2;

#[derive(Debug, Error)]
pub enum StateError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("database schema {found} is newer than supported schema {supported}")]
    NewerSchema { found: i64, supported: i64 },
    #[error("stored UUID is invalid")]
    InvalidUuid,
    #[error("stored lifecycle state is invalid")]
    InvalidLifecycle,
    #[error("stored readiness state is invalid")]
    InvalidReadiness,
    #[error("stored numeric value is outside the supported range")]
    NumericOutOfRange,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredEnvironment {
    pub id: Uuid,
    pub key: String,
    pub worktree: String,
    pub profile: String,
    pub generation: u64,
    pub lifecycle: StoredLifecycle,
    pub process: Option<StoredProcess>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredLifecycle {
    Starting,
    Running,
    Stopped,
    Failed,
}

impl StoredLifecycle {
    const fn as_db(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }

    fn from_db(value: &str) -> Result<Self, StateError> {
        match value {
            "starting" => Ok(Self::Starting),
            "running" => Ok(Self::Running),
            "stopped" => Ok(Self::Stopped),
            "failed" => Ok(Self::Failed),
            _ => Err(StateError::InvalidLifecycle),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredReadiness {
    ProcessAlive,
    ProcessStopped,
}

impl StoredReadiness {
    const fn as_db(self) -> &'static str {
        match self {
            Self::ProcessAlive => "process_alive",
            Self::ProcessStopped => "process_stopped",
        }
    }

    fn from_db(value: &str) -> Result<Self, StateError> {
        match value {
            "process_alive" => Ok(Self::ProcessAlive),
            "process_stopped" => Ok(Self::ProcessStopped),
            _ => Err(StateError::InvalidReadiness),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredProcess {
    pub pid: u32,
    pub process_group: i32,
    pub started_at_unix_ms: u128,
    pub supervisor_socket: String,
    pub readiness: StoredReadiness,
}

pub struct StateStore {
    connection: Connection,
}

impl StateStore {
    /// Opens an in-memory SQLite store and applies supported migrations.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot open or migrate the database.
    pub fn open_in_memory() -> Result<Self, StateError> {
        let connection = Connection::open_in_memory()?;
        let mut store = Self { connection };
        store.migrate()?;
        Ok(store)
    }

    /// Opens a persistent SQLite store and applies supported migrations.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot open or migrate the database.
    pub fn open(path: &Path) -> Result<Self, StateError> {
        let connection = Connection::open(path)?;
        let mut store = Self { connection };
        store.migrate()?;
        Ok(store)
    }

    /// Reads the SQLite migration version.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot query the database.
    pub fn schema_version(&self) -> Result<i64, StateError> {
        Ok(self
            .connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))?)
    }

    /// Persists an accepted start intent before external effects begin.
    ///
    /// # Errors
    ///
    /// Returns an error when the SQLite transaction cannot be committed.
    pub fn record_start_intent(
        &mut self,
        environment: &StoredEnvironment,
        operation_id: Uuid,
    ) -> Result<(), StateError> {
        let generation =
            i64::try_from(environment.generation).map_err(|_| StateError::NumericOutOfRange)?;
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT INTO environments (id, environment_key, worktree, profile, generation, lifecycle)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(environment_key) DO UPDATE SET
                worktree = excluded.worktree,
                profile = excluded.profile,
                generation = excluded.generation,
                lifecycle = excluded.lifecycle",
            params![
                environment.id.to_string(),
                environment.key,
                environment.worktree,
                environment.profile,
                generation,
                environment.lifecycle.as_db(),
            ],
        )?;
        transaction.execute(
            "INSERT INTO operations (id, environment_id, kind, status)
             VALUES (?1, ?2, 'up', 'accepted')",
            params![operation_id.to_string(), environment.id.to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Persists a supervisor-confirmed process identity after launch.
    ///
    /// # Errors
    ///
    /// Returns an error when the SQLite transaction cannot be committed.
    pub fn record_running_process(
        &mut self,
        environment_id: Uuid,
        process: &StoredProcess,
    ) -> Result<(), StateError> {
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT INTO processes (environment_id, pid, process_group, started_at_unix_ms,
                                    supervisor_socket, readiness)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(environment_id) DO UPDATE SET
                pid = excluded.pid,
                process_group = excluded.process_group,
                started_at_unix_ms = excluded.started_at_unix_ms,
                supervisor_socket = excluded.supervisor_socket,
                readiness = excluded.readiness",
            params![
                environment_id.to_string(),
                i64::from(process.pid),
                process.process_group,
                process.started_at_unix_ms.to_string(),
                process.supervisor_socket,
                process.readiness.as_db(),
            ],
        )?;
        transaction.execute(
            "UPDATE environments SET lifecycle = 'running' WHERE id = ?1",
            params![environment_id.to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Marks the process stopped while retaining its environment and operation data.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the observed state.
    pub fn mark_stopped(&mut self, environment_id: Uuid) -> Result<(), StateError> {
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE environments SET lifecycle = 'stopped' WHERE id = ?1",
            params![environment_id.to_string()],
        )?;
        transaction.execute(
            "UPDATE processes SET readiness = 'process_stopped' WHERE environment_id = ?1",
            params![environment_id.to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Marks a failed start without deleting retained environment state.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the failure state.
    pub fn mark_failed(&mut self, environment_id: Uuid) -> Result<(), StateError> {
        self.connection.execute(
            "UPDATE environments SET lifecycle = 'failed' WHERE id = ?1",
            params![environment_id.to_string()],
        )?;
        Ok(())
    }

    /// Looks up an environment and its retained process record.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot read or decode stored state.
    pub fn environment(&self, id: Uuid) -> Result<Option<StoredEnvironment>, StateError> {
        self.query_environments(Some(id))
            .map(|mut environments| environments.pop())
    }

    /// Lists all environments in creation order.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot read or decode stored state.
    pub fn environments(&self) -> Result<Vec<StoredEnvironment>, StateError> {
        self.query_environments(None)
    }

    fn query_environments(&self, id: Option<Uuid>) -> Result<Vec<StoredEnvironment>, StateError> {
        let mut statement = self.connection.prepare(
            "SELECT e.id, e.environment_key, e.worktree, e.profile, e.generation, e.lifecycle,
                    p.pid, p.process_group, p.started_at_unix_ms, p.supervisor_socket, p.readiness
             FROM environments e
             LEFT JOIN processes p ON p.environment_id = e.id
             WHERE (?1 IS NULL OR e.id = ?1)
             ORDER BY e.created_at ASC",
        )?;
        let id = id.map(|value| value.to_string());
        let rows = statement.query_map(params![id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<i32>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, Option<String>>(10)?,
            ))
        })?;
        rows.map(|row| {
            let (
                id,
                key,
                worktree,
                profile,
                generation,
                lifecycle,
                pid,
                process_group,
                started_at,
                supervisor_socket,
                readiness,
            ) = row?;
            let process = match (pid, process_group, started_at, supervisor_socket, readiness) {
                (
                    Some(pid),
                    Some(process_group),
                    Some(started_at),
                    Some(supervisor_socket),
                    Some(readiness),
                ) => Some(StoredProcess {
                    pid: pid.try_into().map_err(|_| StateError::InvalidUuid)?,
                    process_group,
                    started_at_unix_ms: started_at.parse().map_err(|_| StateError::InvalidUuid)?,
                    supervisor_socket,
                    readiness: StoredReadiness::from_db(&readiness)?,
                }),
                (None, None, None, None, None) => None,
                _ => return Err(StateError::InvalidReadiness),
            };
            Ok(StoredEnvironment {
                id: Uuid::parse_str(&id).map_err(|_| StateError::InvalidUuid)?,
                key,
                worktree,
                profile,
                generation: generation
                    .try_into()
                    .map_err(|_| StateError::InvalidLifecycle)?,
                lifecycle: StoredLifecycle::from_db(&lifecycle)?,
                process,
            })
        })
        .collect()
    }

    fn migrate(&mut self) -> Result<(), StateError> {
        let version = self.schema_version()?;
        if version > CURRENT_SCHEMA_VERSION {
            return Err(StateError::NewerSchema {
                found: version,
                supported: CURRENT_SCHEMA_VERSION,
            });
        }
        if version < 1 {
            self.connection.execute_batch(
                "BEGIN;
                 CREATE TABLE IF NOT EXISTS schema_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                 );
                 PRAGMA user_version = 1;
                 COMMIT;",
            )?;
        }
        if version < 2 {
            self.connection.execute_batch(
                "BEGIN;
                 CREATE TABLE IF NOT EXISTS environments (
                    id TEXT PRIMARY KEY,
                    environment_key TEXT NOT NULL UNIQUE,
                    worktree TEXT NOT NULL,
                    profile TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    lifecycle TEXT NOT NULL,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                 );
                 CREATE TABLE IF NOT EXISTS operations (
                    id TEXT PRIMARY KEY,
                    environment_id TEXT NOT NULL REFERENCES environments(id),
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                 );
                 CREATE TABLE IF NOT EXISTS processes (
                    environment_id TEXT PRIMARY KEY REFERENCES environments(id),
                    pid INTEGER NOT NULL,
                    process_group INTEGER NOT NULL,
                    started_at_unix_ms TEXT NOT NULL,
                    supervisor_socket TEXT NOT NULL,
                    readiness TEXT NOT NULL
                 );
                 PRAGMA user_version = 2;
                 COMMIT;",
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn environment() -> StoredEnvironment {
        StoredEnvironment {
            id: Uuid::now_v7(),
            key: "fixture".into(),
            worktree: "/tmp/worktree".into(),
            profile: "process".into(),
            generation: 1,
            lifecycle: StoredLifecycle::Starting,
            process: None,
        }
    }

    #[test]
    fn persists_intent_process_identity_and_stop_state() {
        let mut store = StateStore::open_in_memory().expect("state store opens");
        let environment = environment();
        store
            .record_start_intent(&environment, Uuid::now_v7())
            .expect("intent persists");
        let process = StoredProcess {
            pid: 42,
            process_group: 42,
            started_at_unix_ms: 7,
            supervisor_socket: "/tmp/df/s.sock".into(),
            readiness: StoredReadiness::ProcessAlive,
        };
        store
            .record_running_process(environment.id, &process)
            .expect("process persists");
        store.mark_stopped(environment.id).expect("stop persists");
        let observed = store
            .environment(environment.id)
            .expect("query works")
            .expect("environment exists");
        assert_eq!(observed.lifecycle, StoredLifecycle::Stopped);
        assert_eq!(
            observed.process.expect("process exists").readiness,
            StoredReadiness::ProcessStopped
        );
    }
}
