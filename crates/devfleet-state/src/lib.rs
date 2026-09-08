//! Versioned SQLite storage. Domain orchestration remains in the daemon/core.

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;
use uuid::Uuid;

pub const CURRENT_SCHEMA_VERSION: i64 = 4;

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
    pub project_id: Uuid,
    pub worktree_id: Uuid,
    pub project_fingerprint: String,
    pub worktree_fingerprint: String,
    pub common_git_dir: String,
    pub git_dir: String,
    pub worktree: String,
    pub profile: String,
    pub instance: Option<String>,
    pub head: String,
    pub branch: Option<String>,
    pub detached: bool,
    pub generation: u64,
    pub lifecycle: StoredLifecycle,
    pub reconciliation_diagnostic: Option<String>,
    pub process: Option<StoredProcess>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredLifecycle {
    Starting,
    Running,
    Stopped,
    Failed,
    Orphaned,
}

impl StoredLifecycle {
    const fn as_db(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
            Self::Orphaned => "orphaned",
        }
    }

    fn from_db(value: &str) -> Result<Self, StateError> {
        match value {
            "starting" => Ok(Self::Starting),
            "running" => Ok(Self::Running),
            "stopped" => Ok(Self::Stopped),
            "failed" => Ok(Self::Failed),
            "orphaned" => Ok(Self::Orphaned),
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedGitContext {
    pub common_git_dir: String,
    pub git_dir: String,
    pub worktree: String,
    pub head: String,
    pub branch: Option<String>,
    pub detached: bool,
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
            "INSERT INTO environments (id, environment_key, project_id, worktree_id,
                                      project_fingerprint, worktree_fingerprint, common_git_dir,
                                      git_dir, worktree, profile, instance, head, branch, detached,
                                      generation, lifecycle,
                                      reconciliation_diagnostic)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
             ON CONFLICT(environment_key) DO UPDATE SET
                project_id = excluded.project_id,
                worktree_id = excluded.worktree_id,
                project_fingerprint = excluded.project_fingerprint,
                worktree_fingerprint = excluded.worktree_fingerprint,
                common_git_dir = excluded.common_git_dir,
                git_dir = excluded.git_dir,
                worktree = excluded.worktree,
                profile = excluded.profile,
                instance = excluded.instance,
                head = excluded.head,
                branch = excluded.branch,
                detached = excluded.detached,
                generation = excluded.generation,
                lifecycle = excluded.lifecycle,
                reconciliation_diagnostic = excluded.reconciliation_diagnostic",
            params![
                environment.id.to_string(),
                environment.key,
                environment.project_id.to_string(),
                environment.worktree_id.to_string(),
                environment.project_fingerprint,
                environment.worktree_fingerprint,
                environment.common_git_dir,
                environment.git_dir,
                environment.worktree,
                environment.profile,
                environment.instance,
                environment.head,
                environment.branch,
                environment.detached,
                generation,
                environment.lifecycle.as_db(),
                environment.reconciliation_diagnostic,
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

    /// Updates verified Git context without changing project or worktree identity.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the verified location.
    pub fn update_worktree_context(
        &mut self,
        id: Uuid,
        context: &VerifiedGitContext,
    ) -> Result<(), StateError> {
        self.connection.execute(
            "UPDATE environments
             SET common_git_dir = ?1, git_dir = ?2, worktree = ?3,
                 head = ?4, branch = ?5, detached = ?6
            WHERE id = ?7",
            params![
                context.common_git_dir,
                context.git_dir,
                context.worktree,
                context.head,
                context.branch,
                context.detached,
                id.to_string(),
            ],
        )?;
        Ok(())
    }

    /// Marks a same-path but different-Git-identity environment as orphaned.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot retain the reconciliation diagnostic.
    pub fn orphan_replaced_worktree(
        &mut self,
        worktree: &str,
        profile: &str,
        instance: Option<&str>,
        project_fingerprint: &str,
        worktree_fingerprint: &str,
    ) -> Result<(), StateError> {
        self.connection.execute(
            "UPDATE environments
             SET lifecycle = 'orphaned',
                 reconciliation_diagnostic = 'Git administrative identity changed at this path; create or reconcile explicitly.'
             WHERE worktree = ?1
               AND profile = ?2
               AND instance IS ?3
               AND (project_fingerprint != ?4 OR worktree_fingerprint != ?5)
               AND lifecycle != 'orphaned'",
            params![
                worktree,
                profile,
                instance,
                project_fingerprint,
                worktree_fingerprint,
            ],
        )?;
        Ok(())
    }

    fn query_environments(&self, id: Option<Uuid>) -> Result<Vec<StoredEnvironment>, StateError> {
        let mut statement = self.connection.prepare(
            "SELECT e.id, e.environment_key, e.project_id, e.worktree_id,
                    e.project_fingerprint, e.worktree_fingerprint, e.common_git_dir, e.git_dir,
                    e.worktree, e.profile, e.instance, e.head, e.branch, e.detached,
                    e.generation, e.lifecycle, e.reconciliation_diagnostic, p.pid,
                    p.process_group, p.started_at_unix_ms, p.supervisor_socket, p.readiness
             FROM environments e
             LEFT JOIN processes p ON p.environment_id = e.id
             WHERE (?1 IS NULL OR e.id = ?1)
             ORDER BY e.created_at ASC",
        )?;
        let id = id.map(|value| value.to_string());
        let mut rows = statement.query(params![id])?;
        let mut environments = Vec::new();
        while let Some(row) = rows.next()? {
            environments.push(stored_environment(row)?);
        }
        Ok(environments)
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
        if version < 3 {
            self.connection.execute_batch(
                "BEGIN;
                 ALTER TABLE environments ADD COLUMN project_id TEXT;
                 ALTER TABLE environments ADD COLUMN worktree_id TEXT;
                 ALTER TABLE environments ADD COLUMN project_fingerprint TEXT;
                 ALTER TABLE environments ADD COLUMN worktree_fingerprint TEXT;
                 ALTER TABLE environments ADD COLUMN instance TEXT;
                 ALTER TABLE environments ADD COLUMN reconciliation_diagnostic TEXT;
                 UPDATE environments
                 SET project_id = id,
                     worktree_id = id,
                     project_fingerprint = 'legacy:' || id,
                     worktree_fingerprint = 'legacy:' || id,
                     lifecycle = 'orphaned',
                     reconciliation_diagnostic = 'Identity metadata predates Git worktree discovery; reconcile explicitly.';
                 CREATE UNIQUE INDEX IF NOT EXISTS environments_identity
                 ON environments (project_fingerprint, worktree_fingerprint, profile, instance);
                 PRAGMA user_version = 3;
                 COMMIT;",
            )?;
        }
        if version < 4 {
            self.connection.execute_batch(
                "BEGIN;
                 ALTER TABLE environments ADD COLUMN common_git_dir TEXT;
                 ALTER TABLE environments ADD COLUMN git_dir TEXT;
                 ALTER TABLE environments ADD COLUMN head TEXT;
                 ALTER TABLE environments ADD COLUMN branch TEXT;
                 ALTER TABLE environments ADD COLUMN detached INTEGER NOT NULL DEFAULT 0;
                 UPDATE environments
                 SET common_git_dir = '', git_dir = '', head = '';
                 PRAGMA user_version = 4;
                 COMMIT;",
            )?;
        }
        Ok(())
    }
}

fn stored_environment(row: &rusqlite::Row<'_>) -> Result<StoredEnvironment, StateError> {
    let pid = row.get::<_, Option<i64>>(17)?;
    let process_group = row.get::<_, Option<i32>>(18)?;
    let started_at = row.get::<_, Option<String>>(19)?;
    let supervisor_socket = row.get::<_, Option<String>>(20)?;
    let readiness = row.get::<_, Option<String>>(21)?;
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
        id: parse_uuid(&row.get::<_, String>(0)?)?,
        key: row.get(1)?,
        project_id: parse_uuid(&row.get::<_, String>(2)?)?,
        worktree_id: parse_uuid(&row.get::<_, String>(3)?)?,
        project_fingerprint: row.get(4)?,
        worktree_fingerprint: row.get(5)?,
        common_git_dir: row.get(6)?,
        git_dir: row.get(7)?,
        worktree: row.get(8)?,
        profile: row.get(9)?,
        instance: row.get(10)?,
        head: row.get(11)?,
        branch: row.get(12)?,
        detached: row.get(13)?,
        generation: row
            .get::<_, i64>(14)?
            .try_into()
            .map_err(|_| StateError::InvalidLifecycle)?,
        lifecycle: StoredLifecycle::from_db(&row.get::<_, String>(15)?)?,
        reconciliation_diagnostic: row.get(16)?,
        process,
    })
}

fn parse_uuid(value: &str) -> Result<Uuid, StateError> {
    Uuid::parse_str(value).map_err(|_| StateError::InvalidUuid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn environment() -> StoredEnvironment {
        StoredEnvironment {
            id: Uuid::now_v7(),
            key: "fixture".into(),
            project_id: Uuid::now_v7(),
            worktree_id: Uuid::now_v7(),
            project_fingerprint: "1:2".into(),
            worktree_fingerprint: "1:3".into(),
            common_git_dir: "/tmp/project/.git".into(),
            git_dir: "/tmp/project/.git/worktrees/fixture".into(),
            worktree: "/tmp/worktree".into(),
            profile: "process".into(),
            instance: None,
            head: "abc".into(),
            branch: Some("refs/heads/main".into()),
            detached: false,
            generation: 1,
            lifecycle: StoredLifecycle::Starting,
            reconciliation_diagnostic: None,
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

    #[test]
    fn migrates_pre_identity_environments_to_explicit_orphans() {
        let legacy_id = Uuid::now_v7();
        let connection = Connection::open_in_memory().expect("legacy database opens");
        connection
            .execute_batch(
                "CREATE TABLE environments (
                    id TEXT PRIMARY KEY,
                    environment_key TEXT NOT NULL UNIQUE,
                    worktree TEXT NOT NULL,
                    profile TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    lifecycle TEXT NOT NULL,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                 );
                 CREATE TABLE operations (
                    id TEXT PRIMARY KEY,
                    environment_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at INTEGER NOT NULL DEFAULT (unixepoch())
                 );
                 CREATE TABLE processes (
                    environment_id TEXT PRIMARY KEY,
                    pid INTEGER NOT NULL,
                    process_group INTEGER NOT NULL,
                    started_at_unix_ms TEXT NOT NULL,
                    supervisor_socket TEXT NOT NULL,
                    readiness TEXT NOT NULL
                 );
                 PRAGMA user_version = 2;",
            )
            .expect("legacy schema creates");
        connection
            .execute(
                "INSERT INTO environments (id, environment_key, worktree, profile, generation, lifecycle)
                 VALUES (?1, 'legacy', '/tmp/old', 'process', 1, 'stopped')",
                params![legacy_id.to_string()],
            )
            .expect("legacy environment inserts");

        let mut store = StateStore { connection };
        store.migrate().expect("identity migration applies");
        let environment = store
            .environment(legacy_id)
            .expect("legacy environment queries")
            .expect("legacy environment retained");
        assert_eq!(environment.lifecycle, StoredLifecycle::Orphaned);
        assert!(environment.reconciliation_diagnostic.is_some());
        assert_eq!(
            environment.project_fingerprint,
            format!("legacy:{legacy_id}")
        );
    }
}
