//! Versioned SQLite storage. Domain orchestration remains in the daemon/core.

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};
use thiserror::Error;
use uuid::Uuid;

pub const CURRENT_SCHEMA_VERSION: i64 = 19;
const MAX_RETAINED_EVENTS_PER_ENVIRONMENT: i64 = 2_048;

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
    #[error("stored JSON is invalid")]
    Json(#[from] serde_json::Error),
    #[error("port lease request is invalid")]
    InvalidPortLease,
    #[error("no port block remains in the configured range")]
    PortRangeExhausted,
    #[error("event cursor expired; recover with an environment snapshot")]
    CursorExpired,
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
    pub endpoints: Vec<StoredEndpoint>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredEndpoint {
    pub name: String,
    pub service: String,
    pub internal_url: String,
    pub host_url: String,
    pub device_url: String,
    pub readiness_path: String,
    pub healthy: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredLifecycle {
    Starting,
    Running,
    Stopped,
    Failed,
    Degraded,
    Orphaned,
    Destroyed,
}

impl StoredLifecycle {
    const fn as_db(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
            Self::Degraded => "degraded",
            Self::Orphaned => "orphaned",
            Self::Destroyed => "destroyed",
        }
    }

    fn from_db(value: &str) -> Result<Self, StateError> {
        match value {
            "starting" => Ok(Self::Starting),
            "running" => Ok(Self::Running),
            "stopped" => Ok(Self::Stopped),
            "failed" => Ok(Self::Failed),
            "degraded" => Ok(Self::Degraded),
            "orphaned" => Ok(Self::Orphaned),
            "destroyed" => Ok(Self::Destroyed),
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredInitializationTask {
    pub environment_id: Uuid,
    pub id: String,
    pub kind: String,
    pub rerun_policy: String,
    pub fingerprint: String,
    pub generation: u64,
    pub attempts: u64,
    pub status: String,
    pub diagnostic: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredBinding {
    pub environment_id: Uuid,
    pub phase: String,
    pub integration: String,
    pub fingerprint: String,
    pub invalidation: String,
    pub public_values_json: String,
    pub private_path: String,
}

/// State for a simulator that `DevFleet` created for exactly one environment.
/// `udid` remains absent only while a creation result is being reconciled.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredIosDevice {
    pub environment_id: Uuid,
    pub udid: Option<String>,
    pub runtime: String,
    pub device_type: String,
    pub expected_name: String,
    pub lifecycle: String,
}

/// Compose ownership retained without environment values or volume contents.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredComposeResource {
    pub environment_id: Uuid,
    pub project_name: String,
    pub worktree: String,
    pub files: Vec<String>,
    pub lifecycle: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredPreparationTask {
    pub environment_id: Uuid,
    pub kind: String,
    pub fingerprint: String,
    pub attempts: u64,
    pub status: String,
    pub diagnostic: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredIosApp {
    pub environment_id: Uuid,
    pub bundle_id: String,
    pub udid: String,
    pub metro_port: u16,
    pub derived_data: String,
    pub native_fingerprint: String,
    pub launched: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredIdentityVerification {
    pub environment_id: Uuid,
    pub generation: u64,
    pub mode: String,
    pub status: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredArtifact {
    pub id: Uuid,
    pub timestamp_unix_ms: u64,
    pub environment_id: Uuid,
    pub generation: u64,
    pub worktree: String,
    pub head: String,
    pub code_fingerprint: String,
    pub build_identity: String,
    pub udid: String,
    pub kind: String,
    pub command: String,
    pub flow: String,
    pub result: String,
    pub app_running: bool,
    pub functional_result: String,
    pub human_review: String,
    pub path: String,
    pub obsolete: bool,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredPlugin {
    pub id: String,
    pub path: String,
    pub platform: String,
    pub version: String,
    pub checksum: String,
    pub protocol_major: u16,
    pub protocol_minor: u16,
    pub plugin_state_version: u32,
    pub capabilities: Vec<String>,
    pub trusted: bool,
}

/// A redacted, provider-free configuration snapshot for one worktree/profile.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredPlan {
    pub key: String,
    pub project_fingerprint: String,
    pub worktree_fingerprint: String,
    pub worktree: String,
    pub profile: String,
    pub generation: u64,
    pub effective_configuration: serde_json::Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredPortLease {
    pub environment_key: String,
    pub project_fingerprint: String,
    pub generation: u64,
    pub range_start: u16,
    pub range_end: u16,
    pub block_start: u16,
    pub block_size: u16,
    pub offsets: BTreeMap<String, u16>,
    pub state: PortLeaseState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortLeaseState {
    Reserved,
    Stopped,
    Quarantined,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortLeaseRequest {
    pub environment_key: String,
    pub project_fingerprint: String,
    pub generation: u64,
    pub range_start: u16,
    pub range_end: u16,
    pub block_size: u16,
    pub offsets: BTreeMap<String, u16>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredOperation {
    pub id: Uuid,
    pub environment_id: Uuid,
    pub kind: String,
    pub status: OperationStatus,
    pub deadline_unix_ms: Option<u128>,
    pub cancellation_requested: bool,
    pub phase: String,
    pub resource_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredEvent {
    pub sequence: u64,
    pub id: Uuid,
    pub timestamp_unix_ms: u128,
    pub environment_id: Uuid,
    pub resource_id: Option<String>,
    pub generation: u64,
    pub operation_id: Option<Uuid>,
    pub kind: String,
    pub payload: serde_json::Value,
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
                                      generation, lifecycle, reconciliation_diagnostic,
                                      endpoints_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
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
                reconciliation_diagnostic = excluded.reconciliation_diagnostic,
                endpoints_json = excluded.endpoints_json",
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
                serde_json::to_string(&environment.endpoints)?,
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

    /// Replaces the resolved endpoint observations for an owned environment.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the endpoint snapshot.
    pub fn set_endpoints(
        &mut self,
        environment_id: Uuid,
        endpoints: &[StoredEndpoint],
    ) -> Result<(), StateError> {
        let endpoints = serde_json::to_string(endpoints)?;
        let transaction = self.connection.transaction()?;
        let existing: String = transaction.query_row(
            "SELECT endpoints_json FROM environments WHERE id = ?1",
            params![environment_id.to_string()],
            |row| row.get(0),
        )?;
        let changed = existing != endpoints;
        transaction.execute(
            "UPDATE environments SET endpoints_json = ?2 WHERE id = ?1",
            params![environment_id.to_string(), &endpoints],
        )?;
        if changed {
            let worktree_id: String = transaction.query_row(
                "SELECT worktree_id FROM environments WHERE id = ?1",
                params![environment_id.to_string()],
                |row| row.get(0),
            )?;
            transaction.execute(
                "UPDATE environments SET generation = generation + 1 WHERE worktree_id = ?1",
                params![worktree_id],
            )?;
            transaction.execute(
                "UPDATE ios_apps SET launched = 0 WHERE environment_id IN (SELECT id FROM environments WHERE worktree_id = ?1)",
                params![worktree_id],
            )?;
            transaction.execute(
                "UPDATE identity_verifications SET status = 'mismatch' WHERE environment_id IN (SELECT id FROM environments WHERE worktree_id = ?1) AND mode = 'app_handshake'",
                params![worktree_id],
            )?;
        }
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

    /// Marks an environment destroyed without discarding retained state needed
    /// for audit, recovery, or an explicitly requested later purge.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the observed teardown.
    pub fn mark_destroyed(&mut self, environment_id: Uuid) -> Result<(), StateError> {
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE environments SET lifecycle = 'destroyed' WHERE id = ?1",
            params![environment_id.to_string()],
        )?;
        transaction.execute(
            "UPDATE processes SET readiness = 'process_stopped' WHERE environment_id = ?1",
            params![environment_id.to_string()],
        )?;
        transaction.execute(
            "UPDATE ios_apps SET launched = 0 WHERE environment_id = ?1",
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

    /// Returns retained initialization progress for an environment.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot read retained task state.
    pub fn initialization_tasks(
        &self,
        environment_id: Uuid,
    ) -> Result<Vec<StoredInitializationTask>, StateError> {
        let mut statement = self.connection.prepare(
            "SELECT environment_id, task_id, kind, rerun_policy, fingerprint, generation, attempts, status, diagnostic
             FROM initialization_tasks WHERE environment_id = ?1 ORDER BY task_id",
        )?;
        let mut rows = statement.query(params![environment_id.to_string()])?;
        let mut tasks = Vec::new();
        while let Some(row) = rows.next()? {
            tasks.push(stored_initialization_task(row)?);
        }
        Ok(tasks)
    }

    /// Persists task intent before supervisor execution.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the task intent.
    pub fn record_initialization_task_intent(
        &mut self,
        task: &StoredInitializationTask,
    ) -> Result<(), StateError> {
        self.connection.execute(
            "INSERT INTO initialization_tasks (
                environment_id, task_id, kind, rerun_policy, fingerprint, generation, attempts, status, diagnostic
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, 'running', NULL)
             ON CONFLICT(environment_id, task_id) DO UPDATE SET
                kind = excluded.kind,
                rerun_policy = excluded.rerun_policy,
                fingerprint = excluded.fingerprint,
                generation = excluded.generation,
                attempts = initialization_tasks.attempts + 1,
                status = 'running',
                diagnostic = NULL",
            params![
                task.environment_id.to_string(),
                task.id,
                task.kind,
                task.rerun_policy,
                task.fingerprint,
                i64::try_from(task.generation).map_err(|_| StateError::NumericOutOfRange)?,
            ],
        )?;
        Ok(())
    }

    /// Marks a task as completed without removing its retained record.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the completion.
    pub fn complete_initialization_task(
        &mut self,
        environment_id: Uuid,
        task_id: &str,
    ) -> Result<(), StateError> {
        self.connection.execute(
            "UPDATE initialization_tasks SET status = 'completed', diagnostic = NULL
             WHERE environment_id = ?1 AND task_id = ?2",
            params![environment_id.to_string(), task_id],
        )?;
        Ok(())
    }

    /// Records a task failure without applying compensating data changes.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the failure.
    pub fn fail_initialization_task(
        &mut self,
        environment_id: Uuid,
        task_id: &str,
        diagnostic: &str,
    ) -> Result<(), StateError> {
        self.connection.execute(
            "UPDATE initialization_tasks SET status = 'failed', diagnostic = ?3
             WHERE environment_id = ?1 AND task_id = ?2",
            params![environment_id.to_string(), task_id, diagnostic],
        )?;
        Ok(())
    }

    /// Reads retained React Native preparation status for one environment.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot query preparation status.
    pub fn preparation_tasks(
        &self,
        environment_id: Uuid,
    ) -> Result<Vec<StoredPreparationTask>, StateError> {
        let mut statement = self.connection.prepare("SELECT environment_id, kind, fingerprint, attempts, status, diagnostic FROM preparation_tasks WHERE environment_id = ?1 ORDER BY kind")?;
        let mut rows = statement.query(params![environment_id.to_string()])?;
        let mut tasks = Vec::new();
        while let Some(row) = rows.next()? {
            tasks.push(stored_preparation_task(row)?);
        }
        Ok(tasks)
    }

    /// Persists intent before a mutable dependency or Pods command begins.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist task intent.
    pub fn record_preparation_intent(
        &mut self,
        task: &StoredPreparationTask,
    ) -> Result<(), StateError> {
        self.connection.execute("INSERT INTO preparation_tasks (environment_id, kind, fingerprint, attempts, status, diagnostic) VALUES (?1, ?2, ?3, 1, 'running', NULL) ON CONFLICT(environment_id, kind) DO UPDATE SET fingerprint = excluded.fingerprint, attempts = preparation_tasks.attempts + 1, status = 'running', diagnostic = NULL", params![task.environment_id.to_string(), task.kind, task.fingerprint])?;
        Ok(())
    }

    /// Completes or fails an owned preparation task without discarding history.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the result.
    pub fn finish_preparation_task(
        &mut self,
        environment_id: Uuid,
        kind: &str,
        diagnostic: Option<&str>,
    ) -> Result<(), StateError> {
        self.connection.execute("UPDATE preparation_tasks SET status = CASE WHEN ?3 IS NULL THEN 'completed' ELSE 'failed' END, diagnostic = ?3 WHERE environment_id = ?1 AND kind = ?2", params![environment_id.to_string(), kind, diagnostic])?;
        Ok(())
    }

    /// Reads the last owned iOS app installation for one environment.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot query the installation record.
    pub fn ios_app(&self, environment_id: Uuid) -> Result<Option<StoredIosApp>, StateError> {
        self.connection
            .query_row(
                "SELECT environment_id, bundle_id, udid, metro_port, derived_data, native_fingerprint, launched FROM ios_apps WHERE environment_id = ?1",
                params![environment_id.to_string()],
                stored_ios_app,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Persists an exact iOS app installation after the provider launches it.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the installation record.
    pub fn record_ios_app(&mut self, app: &StoredIosApp) -> Result<(), StateError> {
        self.connection.execute(
            "INSERT INTO ios_apps (environment_id, bundle_id, udid, metro_port, derived_data, native_fingerprint, launched) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(environment_id) DO UPDATE SET bundle_id = excluded.bundle_id, udid = excluded.udid, metro_port = excluded.metro_port, derived_data = excluded.derived_data, native_fingerprint = excluded.native_fingerprint, launched = excluded.launched",
            params![app.environment_id.to_string(), app.bundle_id, app.udid, i64::from(app.metro_port), app.derived_data, app.native_fingerprint, app.launched],
        )?;
        Ok(())
    }

    /// Records the public lifecycle state of a per-launch identity verifier.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot retain the verification state.
    pub fn record_identity_verification(
        &mut self,
        verification: &StoredIdentityVerification,
    ) -> Result<(), StateError> {
        let generation =
            i64::try_from(verification.generation).map_err(|_| StateError::NumericOutOfRange)?;
        self.connection.execute(
            "INSERT INTO identity_verifications (environment_id, generation, mode, status) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(environment_id) DO UPDATE SET generation = excluded.generation, mode = excluded.mode, status = excluded.status",
            params![verification.environment_id.to_string(), generation, verification.mode, verification.status],
        )?;
        Ok(())
    }

    /// Returns public identity-verification state without any nonce or token.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot read the verification state.
    pub fn identity_verification(
        &self,
        environment_id: Uuid,
    ) -> Result<Option<StoredIdentityVerification>, StateError> {
        let row = self.connection.query_row(
            "SELECT environment_id, generation, mode, status FROM identity_verifications WHERE environment_id = ?1",
            params![environment_id.to_string()],
            stored_identity_verification,
        );
        match row {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(StateError::Sqlite(error)),
        }
    }

    /// Persists metadata for a media artifact stored outside SQLite event rows.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot retain the metadata.
    pub fn record_artifact(&mut self, artifact: &StoredArtifact) -> Result<(), StateError> {
        let generation =
            i64::try_from(artifact.generation).map_err(|_| StateError::NumericOutOfRange)?;
        let timestamp =
            i64::try_from(artifact.timestamp_unix_ms).map_err(|_| StateError::NumericOutOfRange)?;
        self.connection.execute(
            "INSERT INTO artifacts (id, timestamp_unix_ms, environment_id, generation, worktree, head, code_fingerprint, build_identity, udid, kind, command, flow, result, app_running, functional_result, human_review, path, obsolete) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
            params![artifact.id.to_string(), timestamp, artifact.environment_id.to_string(), generation, artifact.worktree, artifact.head, artifact.code_fingerprint, artifact.build_identity, artifact.udid, artifact.kind, artifact.command, artifact.flow, artifact.result, artifact.app_running, artifact.functional_result, artifact.human_review, artifact.path, artifact.obsolete],
        )?;
        Ok(())
    }

    /// Returns retained media metadata for an environment without loading media bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot read or decode artifact metadata.
    pub fn artifacts(&self, environment_id: Uuid) -> Result<Vec<StoredArtifact>, StateError> {
        let mut statement = self.connection.prepare(
            "SELECT id, timestamp_unix_ms, environment_id, generation, worktree, head, code_fingerprint, build_identity, udid, kind, command, flow, result, app_running, functional_result, human_review, path, obsolete FROM artifacts WHERE environment_id = ?1 ORDER BY timestamp_unix_ms ASC",
        )?;
        let artifacts = statement
            .query_map(params![environment_id.to_string()], stored_artifact)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(artifacts)
    }

    /// Keeps evidence metadata but records that an explicit purge removed its
    /// local files.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot retain the purge observation.
    pub fn mark_artifacts_obsolete(&mut self, environment_id: Uuid) -> Result<(), StateError> {
        self.connection.execute(
            "UPDATE artifacts SET obsolete = 1, result = 'purged' WHERE environment_id = ?1",
            params![environment_id.to_string()],
        )?;
        Ok(())
    }

    /// Persists exact Compose resource ownership after a verified reconcile.
    /// Environment values and volume contents are intentionally absent.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot retain the resource record.
    pub fn record_compose_resource(
        &mut self,
        resource: &StoredComposeResource,
    ) -> Result<(), StateError> {
        self.connection.execute(
            "INSERT INTO compose_resources (environment_id, project_name, worktree, files_json, lifecycle)
             VALUES (?1, ?2, ?3, ?4, 'active')
             ON CONFLICT(environment_id) DO UPDATE SET
                 project_name = excluded.project_name,
                 worktree = excluded.worktree,
                 files_json = excluded.files_json,
                 lifecycle = 'active'",
            params![
                resource.environment_id.to_string(),
                resource.project_name,
                resource.worktree,
                serde_json::to_string(&resource.files)?,
            ],
        )?;
        Ok(())
    }

    /// Returns Compose resources proven to have been created for this environment.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot decode retained ownership state.
    pub fn compose_resource(
        &self,
        environment_id: Uuid,
    ) -> Result<Option<StoredComposeResource>, StateError> {
        let row = self.connection.query_row(
            "SELECT environment_id, project_name, worktree, files_json, lifecycle
             FROM compose_resources WHERE environment_id = ?1",
            params![environment_id.to_string()],
            stored_compose_resource,
        );
        match row {
            Ok(resource) => Ok(Some(resource)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(StateError::Sqlite(error)),
        }
    }

    /// Retains Compose ownership metadata after its project-scoped teardown.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the teardown observation.
    pub fn mark_compose_destroyed(&mut self, environment_id: Uuid) -> Result<(), StateError> {
        self.connection.execute(
            "UPDATE compose_resources SET lifecycle = 'destroyed' WHERE environment_id = ?1",
            params![environment_id.to_string()],
        )?;
        Ok(())
    }

    /// Quarantines a Compose record whose project or file boundaries no longer
    /// prove that it is safe to destroy.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the quarantine.
    pub fn mark_compose_uncertain(&mut self, environment_id: Uuid) -> Result<(), StateError> {
        self.connection.execute(
            "UPDATE compose_resources SET lifecycle = 'uncertain' WHERE environment_id = ?1",
            params![environment_id.to_string()],
        )?;
        Ok(())
    }

    /// Returns phase-specific bindings without reading private values.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot read persisted binding metadata.
    pub fn bindings(&self, environment_id: Uuid) -> Result<Vec<StoredBinding>, StateError> {
        let mut statement = self.connection.prepare(
            "SELECT environment_id, phase, integration, fingerprint, invalidation, public_values_json, private_path
             FROM bindings WHERE environment_id = ?1 ORDER BY phase",
        )?;
        let mut rows = statement.query(params![environment_id.to_string()])?;
        let mut bindings = Vec::new();
        while let Some(row) = rows.next()? {
            bindings.push(stored_binding(row)?);
        }
        Ok(bindings)
    }

    /// Records only public binding metadata and the private artifact path.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist binding metadata.
    pub fn record_binding(&mut self, binding: &StoredBinding) -> Result<(), StateError> {
        self.connection.execute(
            "INSERT INTO bindings (
                environment_id, phase, integration, fingerprint, invalidation, public_values_json, private_path
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(environment_id, phase) DO UPDATE SET
                integration = excluded.integration,
                fingerprint = excluded.fingerprint,
                invalidation = excluded.invalidation,
                public_values_json = excluded.public_values_json,
                private_path = excluded.private_path",
            params![
                binding.environment_id.to_string(),
                binding.phase,
                binding.integration,
                binding.fingerprint,
                binding.invalidation,
                binding.public_values_json,
                binding.private_path,
            ],
        )?;
        Ok(())
    }

    /// Writes an intent before `simctl create`; callers may never associate an
    /// existing UDID with a new environment through this method.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the reservation.
    pub fn record_ios_reservation(&mut self, device: &StoredIosDevice) -> Result<(), StateError> {
        self.connection.execute(
            "INSERT INTO ios_devices (
                environment_id, udid, runtime, device_type, expected_name, lifecycle
             ) VALUES (?1, NULL, ?2, ?3, ?4, 'reserved')
             ON CONFLICT(environment_id) DO NOTHING",
            params![
                device.environment_id.to_string(),
                device.runtime,
                device.device_type,
                device.expected_name,
            ],
        )?;
        Ok(())
    }

    /// Persists the exact `simctl create` result. The unique UDID prevents
    /// double ownership across environments.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot bind the result.
    pub fn bind_ios_udid(&mut self, environment_id: Uuid, udid: &str) -> Result<bool, StateError> {
        Ok(self.connection.execute(
            "UPDATE ios_devices SET udid = ?2, lifecycle = 'reserved'
             WHERE environment_id = ?1 AND udid IS NULL",
            params![environment_id.to_string(), udid],
        )? == 1)
    }

    /// Reads retained iOS ownership without inspecting or adopting host devices.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot read ownership state.
    pub fn ios_device(&self, environment_id: Uuid) -> Result<Option<StoredIosDevice>, StateError> {
        let mut statement = self.connection.prepare(
            "SELECT environment_id, udid, runtime, device_type, expected_name, lifecycle
             FROM ios_devices WHERE environment_id = ?1",
        )?;
        let mut rows = statement.query(params![environment_id.to_string()])?;
        Ok(rows.next()?.map(stored_ios_device).transpose()?)
    }

    /// Stores the observed outcome of a UDID-scoped simulator command.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the lifecycle.
    pub fn set_ios_lifecycle(
        &mut self,
        environment_id: Uuid,
        lifecycle: &str,
    ) -> Result<bool, StateError> {
        Ok(self.connection.execute(
            "UPDATE ios_devices SET lifecycle = ?2 WHERE environment_id = ?1",
            params![environment_id.to_string(), lifecycle],
        )? == 1)
    }

    /// Retains an uncertain process record without adopting or terminating it by PID alone.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot retain the diagnostic.
    pub fn mark_degraded(
        &mut self,
        environment_id: Uuid,
        diagnostic: &str,
    ) -> Result<(), StateError> {
        self.connection.execute(
            "UPDATE environments SET lifecycle = 'degraded', reconciliation_diagnostic = ?1 WHERE id = ?2",
            params![diagnostic, environment_id.to_string()],
        )?;
        Ok(())
    }

    /// Records a supervisor-backed recovery after identity matching, without claiming pipe recovery.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot retain recovered observation.
    pub fn mark_recovered(&mut self, environment_id: Uuid) -> Result<(), StateError> {
        self.connection.execute(
            "UPDATE environments SET lifecycle = 'running', reconciliation_diagnostic = 'Recovered supervisor observation; prior pipes and handles were not adopted.' WHERE id = ?1",
            params![environment_id.to_string()],
        )?;
        Ok(())
    }

    /// Adds deadline and resource metadata to an accepted operation before its first effect.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the context.
    pub fn set_operation_context(
        &mut self,
        id: Uuid,
        deadline_unix_ms: u128,
        resource_id: &str,
    ) -> Result<(), StateError> {
        self.connection.execute(
            "UPDATE operations SET deadline_unix_ms = ?1, phase = 'reconcile', resource_id = ?2 WHERE id = ?3",
            params![deadline_unix_ms.to_string(), resource_id, id.to_string()],
        )?;
        Ok(())
    }

    /// Reads an operation without treating a timeout as proof no provider effect occurred.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot read or decode the operation.
    pub fn operation(&self, id: Uuid) -> Result<Option<StoredOperation>, StateError> {
        let row = self.connection.query_row(
            "SELECT id, environment_id, kind, status, deadline_unix_ms, cancellation_requested, phase, resource_id
             FROM operations WHERE id = ?1", params![id.to_string()], stored_operation_from_row,
        );
        match row {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(StateError::Sqlite(error)),
        }
    }

    /// Appends a compact event; callers must keep large logs and artifacts on disk.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the event.
    pub fn append_event(&mut self, event: &StoredEvent) -> Result<(), StateError> {
        self.connection.execute(
            "INSERT INTO events (id, timestamp_unix_ms, environment_id, resource_id, generation, operation_id, kind, payload_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![event.id.to_string(), event.timestamp_unix_ms.to_string(), event.environment_id.to_string(), event.resource_id,
                i64::try_from(event.generation).map_err(|_| StateError::NumericOutOfRange)?, event.operation_id.map(|value| value.to_string()), event.kind, serde_json::to_string(&event.payload)?],
        )?;
        self.connection.execute(
            "DELETE FROM events
             WHERE environment_id = ?1
               AND sequence NOT IN (
                   SELECT sequence FROM events
                   WHERE environment_id = ?1
                   ORDER BY sequence DESC
                   LIMIT ?2
               )",
            params![
                event.environment_id.to_string(),
                MAX_RETAINED_EVENTS_PER_ENVIRONMENT
            ],
        )?;
        Ok(())
    }

    /// Reads retained events after a cursor. An expired cursor requires snapshot recovery.
    ///
    /// # Errors
    ///
    /// Returns an error for an expired cursor or unreadable event storage.
    pub fn events_after(
        &self,
        environment_id: Uuid,
        after: Option<u64>,
    ) -> Result<Vec<StoredEvent>, StateError> {
        let oldest = self.connection.query_row(
            "SELECT MIN(sequence) FROM events WHERE environment_id = ?1",
            params![environment_id.to_string()],
            |row| row.get::<_, Option<i64>>(0),
        )?;
        if let (Some(after), Some(oldest)) = (after, oldest)
            && i64::try_from(after).map_err(|_| StateError::NumericOutOfRange)?
                < oldest.saturating_sub(1)
        {
            return Err(StateError::CursorExpired);
        }
        let mut statement = self.connection.prepare(
            "SELECT sequence, id, timestamp_unix_ms, environment_id, resource_id, generation, operation_id, kind, payload_json
             FROM events WHERE environment_id = ?1 AND sequence > ?2 ORDER BY sequence ASC LIMIT 256",
        )?;
        statement
            .query_map(
                params![environment_id.to_string(), after.unwrap_or(0)],
                stored_event_from_row,
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StateError::from)
    }

    /// Marks cancellation durably before callers notify a provider or supervisor.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot record the cancellation request.
    pub fn request_operation_cancel(
        &mut self,
        id: Uuid,
    ) -> Result<Option<StoredOperation>, StateError> {
        let changed = self.connection.execute(
            "UPDATE operations SET cancellation_requested = 1, status = 'cancellation_requested', phase = 'cancelling'
             WHERE id = ?1 AND status IN ('accepted', 'cancellation_requested')", params![id.to_string()],
        )?;
        if changed == 0 {
            return self.operation(id);
        }
        self.operation(id)
    }

    /// Records the final cancellation outcome after owned children have been reaped or quarantined.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the final cancellation state.
    pub fn mark_operation_cancelled(&mut self, id: Uuid, phase: &str) -> Result<(), StateError> {
        self.connection.execute(
            "UPDATE operations SET status = 'cancelled', phase = ?1 WHERE id = ?2",
            params![phase, id.to_string()],
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

    /// Lists installed provider binaries and their independent trust decisions.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot read provider metadata.
    pub fn plugins(&self) -> Result<Vec<StoredPlugin>, StateError> {
        let mut statement = self.connection.prepare(
            "SELECT id, path, platform, version, checksum, protocol_major, protocol_minor,
                    plugin_state_version, capabilities_json, trusted
             FROM plugins ORDER BY id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(StoredPlugin {
                id: row.get(0)?,
                path: row.get(1)?,
                platform: row.get(2)?,
                version: row.get(3)?,
                checksum: row.get(4)?,
                protocol_major: row.get(5)?,
                protocol_minor: row.get(6)?,
                plugin_state_version: row.get(7)?,
                capabilities: serde_json::from_str(&row.get::<_, String>(8)?)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
                trusted: row.get(9)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(StateError::from)
    }

    /// Reads one installed provider by stable descriptor identity.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot read provider metadata.
    pub fn plugin(&self, id: &str) -> Result<Option<StoredPlugin>, StateError> {
        Ok(self.plugins()?.into_iter().find(|plugin| plugin.id == id))
    }

    /// Records an inspected provider without granting execution trust.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist provider metadata.
    pub fn record_plugin(&mut self, plugin: &StoredPlugin) -> Result<(), StateError> {
        self.connection.execute(
            "INSERT INTO plugins (id, path, platform, version, checksum, protocol_major,
                                  protocol_minor, plugin_state_version, capabilities_json, trusted)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(id) DO UPDATE SET path = excluded.path, platform = excluded.platform,
                 version = excluded.version, checksum = excluded.checksum,
                 protocol_major = excluded.protocol_major, protocol_minor = excluded.protocol_minor,
                 plugin_state_version = excluded.plugin_state_version,
                 capabilities_json = excluded.capabilities_json, trusted = excluded.trusted",
            params![
                plugin.id,
                plugin.path,
                plugin.platform,
                plugin.version,
                plugin.checksum,
                plugin.protocol_major,
                plugin.protocol_minor,
                plugin.plugin_state_version,
                serde_json::to_string(&plugin.capabilities)?,
                plugin.trusted,
            ],
        )?;
        Ok(())
    }

    /// Changes only the explicit user trust decision for an installed provider.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the trust decision.
    pub fn set_plugin_trusted(&mut self, id: &str) -> Result<bool, StateError> {
        Ok(self
            .connection
            .execute("UPDATE plugins SET trusted = 1 WHERE id = ?1", params![id])?
            == 1)
    }

    /// Reports whether updates could disturb an active environment operation.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot inspect environment lifecycle state.
    pub fn has_active_environments(&self) -> Result<bool, StateError> {
        let count = self.connection.query_row(
            "SELECT COUNT(*) FROM environments WHERE lifecycle IN ('starting', 'running')",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(count > 0)
    }

    /// Persists a redacted plan and advances generation only when its effective
    /// configuration changes.
    ///
    /// # Errors
    ///
    /// Returns an error when the plan cannot be serialized or SQLite cannot
    /// atomically compare and store the generation.
    pub fn record_effective_plan(
        &mut self,
        project_fingerprint: &str,
        worktree_fingerprint: &str,
        worktree: &str,
        profile: &str,
        effective_configuration: &serde_json::Value,
    ) -> Result<StoredPlan, StateError> {
        let key = format!("{project_fingerprint}|{worktree_fingerprint}|{profile}");
        let configuration = serde_json::to_string(effective_configuration)?;
        let transaction = self.connection.transaction()?;
        let existing = transaction.query_row(
            "SELECT generation, effective_configuration_json FROM effective_plans WHERE plan_key = ?1",
            params![key],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        );
        let generation = match existing {
            Ok((generation, existing)) if existing == configuration => generation,
            Ok((generation, _)) => {
                let next = generation
                    .checked_add(1)
                    .ok_or(StateError::NumericOutOfRange)?;
                transaction.execute(
                    "UPDATE effective_plans
                     SET worktree = ?1, generation = ?2, effective_configuration_json = ?3,
                         updated_at = unixepoch()
                     WHERE plan_key = ?4",
                    params![worktree, next, configuration, key],
                )?;
                next
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                transaction.execute(
                    "INSERT INTO effective_plans (plan_key, project_fingerprint, worktree_fingerprint,
                                                  worktree, profile, generation, effective_configuration_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6)",
                    params![
                        key,
                        project_fingerprint,
                        worktree_fingerprint,
                        worktree,
                        profile,
                        configuration,
                    ],
                )?;
                1
            }
            Err(error) => return Err(StateError::Sqlite(error)),
        };
        transaction.commit()?;
        Ok(StoredPlan {
            key,
            project_fingerprint: project_fingerprint.into(),
            worktree_fingerprint: worktree_fingerprint.into(),
            worktree: worktree.into(),
            profile: profile.into(),
            generation: generation
                .try_into()
                .map_err(|_| StateError::NumericOutOfRange)?,
            effective_configuration: effective_configuration.clone(),
        })
    }

    /// Reserves one user-wide port block transactionally. Stopped and quarantined
    /// leases remain unavailable until an explicit reconciliation releases them.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid offsets, exhausted ranges, or failed storage.
    pub fn reserve_port_lease(
        &mut self,
        request: &PortLeaseRequest,
    ) -> Result<StoredPortLease, StateError> {
        validate_port_request(request)?;
        let transaction = self.connection.transaction()?;
        let existing = transaction.query_row(
            "SELECT project_fingerprint, generation, range_start, range_end, block_start, block_size, offsets_json, state
             FROM port_leases WHERE environment_key = ?1",
            params![request.environment_key],
            stored_port_lease_from_row(&request.environment_key),
        );
        let lease = match existing {
            Ok(lease) => lease,
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                let mut occupied =
                    transaction.prepare("SELECT block_start, block_size FROM port_leases")?;
                let occupied = occupied
                    .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .collect::<Vec<_>>();
                let block_start = candidate_block_start(request, &occupied)
                    .ok_or(StateError::PortRangeExhausted)?;
                transaction.execute(
                    "INSERT INTO port_leases (environment_key, project_fingerprint, generation, range_start, range_end,
                                              block_start, block_size, offsets_json, state)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'reserved')",
                    params![request.environment_key, request.project_fingerprint,
                        i64::try_from(request.generation).map_err(|_| StateError::NumericOutOfRange)?,
                        i64::from(request.range_start), i64::from(request.range_end), i64::from(block_start),
                        i64::from(request.block_size), serde_json::to_string(&request.offsets)?],
                )?;
                StoredPortLease {
                    environment_key: request.environment_key.clone(),
                    project_fingerprint: request.project_fingerprint.clone(),
                    generation: request.generation,
                    range_start: request.range_start,
                    range_end: request.range_end,
                    block_start,
                    block_size: request.block_size,
                    offsets: request.offsets.clone(),
                    state: PortLeaseState::Reserved,
                }
            }
            Err(error) => return Err(StateError::Sqlite(error)),
        };
        transaction.commit()?;
        Ok(lease)
    }

    /// Retains slot preference after stop rather than treating a timer as proof it is free.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist the retained lease state.
    pub fn mark_port_lease_stopped(&mut self, environment_key: &str) -> Result<bool, StateError> {
        Ok(self.connection.execute(
            "UPDATE port_leases SET state = 'stopped' WHERE environment_key = ?1",
            params![environment_key],
        )? == 1)
    }

    /// Quarantines a lease whose previous endpoint ownership cannot be proven.
    ///
    /// # Errors
    ///
    /// Returns an error when SQLite cannot persist quarantine state.
    pub fn quarantine_port_lease(&mut self, environment_key: &str) -> Result<bool, StateError> {
        Ok(self.connection.execute(
            "UPDATE port_leases SET state = 'quarantined' WHERE environment_key = ?1",
            params![environment_key],
        )? == 1)
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
                    e.generation, e.lifecycle, e.reconciliation_diagnostic, e.endpoints_json,
                    p.pid, p.process_group, p.started_at_unix_ms, p.supervisor_socket, p.readiness
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

    #[allow(clippy::too_many_lines)]
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
        if version < 5 {
            self.connection.execute_batch(
                "BEGIN;
                 CREATE TABLE IF NOT EXISTS plugins (
                    id TEXT PRIMARY KEY,
                    path TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    version TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    protocol_major INTEGER NOT NULL,
                    protocol_minor INTEGER NOT NULL,
                    plugin_state_version INTEGER NOT NULL,
                    capabilities_json TEXT NOT NULL,
                    trusted INTEGER NOT NULL DEFAULT 0
                 );
                 PRAGMA user_version = 5;
                 COMMIT;",
            )?;
        }
        if version < 6 {
            self.connection.execute_batch(
                "BEGIN;
                 CREATE TABLE IF NOT EXISTS effective_plans (
                    plan_key TEXT PRIMARY KEY,
                    project_fingerprint TEXT NOT NULL,
                    worktree_fingerprint TEXT NOT NULL,
                    worktree TEXT NOT NULL,
                    profile TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    effective_configuration_json TEXT NOT NULL,
                    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
                 );
                 PRAGMA user_version = 6;
                 COMMIT;",
            )?;
        }
        if version < 7 {
            self.connection.execute_batch(
                "BEGIN;
                 CREATE TABLE IF NOT EXISTS port_leases (
                    environment_key TEXT PRIMARY KEY,
                    project_fingerprint TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    range_start INTEGER NOT NULL,
                    range_end INTEGER NOT NULL,
                    block_start INTEGER NOT NULL UNIQUE,
                    block_size INTEGER NOT NULL,
                    offsets_json TEXT NOT NULL,
                    state TEXT NOT NULL
                 );
                 PRAGMA user_version = 7;
                 COMMIT;",
            )?;
        }
        if version < 8 {
            self.connection.execute_batch(
                "BEGIN;
                 ALTER TABLE operations ADD COLUMN deadline_unix_ms TEXT;
                 ALTER TABLE operations ADD COLUMN cancellation_requested INTEGER NOT NULL DEFAULT 0;
                 ALTER TABLE operations ADD COLUMN phase TEXT NOT NULL DEFAULT 'accepted';
                 ALTER TABLE operations ADD COLUMN resource_id TEXT;
                 PRAGMA user_version = 8;
                 COMMIT;",
            )?;
        }
        if version < 9 {
            self.connection.execute_batch(
                "BEGIN;
                 CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL UNIQUE,
                    timestamp_unix_ms TEXT NOT NULL,
                    environment_id TEXT NOT NULL,
                    resource_id TEXT,
                    generation INTEGER NOT NULL,
                    operation_id TEXT,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                 );
                 CREATE INDEX IF NOT EXISTS events_environment_sequence ON events(environment_id, sequence);
                 PRAGMA user_version = 9;
                 COMMIT;",
            )?;
        }
        if version < 10 {
            self.connection.execute_batch(
                "BEGIN;
                 ALTER TABLE environments ADD COLUMN endpoints_json TEXT NOT NULL DEFAULT '[]';
                 PRAGMA user_version = 10;
                 COMMIT;",
            )?;
        }
        if version < 11 {
            self.connection.execute_batch(
                "BEGIN;
                 CREATE TABLE IF NOT EXISTS initialization_tasks (
                    environment_id TEXT NOT NULL REFERENCES environments(id),
                    task_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    rerun_policy TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    attempts INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    diagnostic TEXT,
                    PRIMARY KEY (environment_id, task_id)
                 );
                 PRAGMA user_version = 11;
                 COMMIT;",
            )?;
        }
        if version < 12 {
            self.connection.execute_batch(
                "BEGIN;
                 CREATE TABLE IF NOT EXISTS bindings (
                    environment_id TEXT NOT NULL REFERENCES environments(id),
                    phase TEXT NOT NULL,
                    integration TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    invalidation TEXT NOT NULL,
                    public_values_json TEXT NOT NULL,
                    private_path TEXT NOT NULL,
                    PRIMARY KEY (environment_id, phase)
                 );
                 PRAGMA user_version = 12;
                 COMMIT;",
            )?;
        }
        if version < 13 {
            self.connection.execute_batch(
                "BEGIN;
                 CREATE TABLE IF NOT EXISTS ios_devices (
                    environment_id TEXT PRIMARY KEY REFERENCES environments(id),
                    udid TEXT UNIQUE,
                    runtime TEXT NOT NULL,
                    device_type TEXT NOT NULL,
                    expected_name TEXT NOT NULL,
                    lifecycle TEXT NOT NULL
                 );
                 PRAGMA user_version = 13;
                 COMMIT;",
            )?;
        }
        if version < 14 {
            self.connection.execute_batch("BEGIN; CREATE TABLE IF NOT EXISTS preparation_tasks (environment_id TEXT NOT NULL REFERENCES environments(id), kind TEXT NOT NULL, fingerprint TEXT NOT NULL, attempts INTEGER NOT NULL, status TEXT NOT NULL, diagnostic TEXT, PRIMARY KEY (environment_id, kind)); PRAGMA user_version = 14; COMMIT;")?;
        }
        if version < 15 {
            self.connection.execute_batch("BEGIN; CREATE TABLE IF NOT EXISTS ios_apps (environment_id TEXT PRIMARY KEY REFERENCES environments(id), bundle_id TEXT NOT NULL, udid TEXT NOT NULL UNIQUE, metro_port INTEGER NOT NULL, derived_data TEXT NOT NULL, native_fingerprint TEXT NOT NULL, launched INTEGER NOT NULL); PRAGMA user_version = 15; COMMIT;")?;
        }
        if version < 16 {
            self.connection.execute_batch("BEGIN; CREATE TABLE IF NOT EXISTS artifacts (id TEXT PRIMARY KEY, timestamp_unix_ms INTEGER NOT NULL, environment_id TEXT NOT NULL REFERENCES environments(id), generation INTEGER NOT NULL, worktree TEXT NOT NULL, head TEXT NOT NULL, code_fingerprint TEXT NOT NULL, build_identity TEXT NOT NULL, udid TEXT NOT NULL, kind TEXT NOT NULL, command TEXT NOT NULL, flow TEXT NOT NULL, result TEXT NOT NULL, path TEXT NOT NULL, obsolete INTEGER NOT NULL); CREATE INDEX IF NOT EXISTS artifacts_environment ON artifacts(environment_id); PRAGMA user_version = 16; COMMIT;")?;
        }
        if version < 17 {
            self.connection.execute_batch("BEGIN; ALTER TABLE artifacts ADD COLUMN app_running INTEGER NOT NULL DEFAULT 0; ALTER TABLE artifacts ADD COLUMN functional_result TEXT NOT NULL DEFAULT 'unverified'; ALTER TABLE artifacts ADD COLUMN human_review TEXT NOT NULL DEFAULT 'unreviewed'; PRAGMA user_version = 17; COMMIT;")?;
        }
        if version < 18 {
            self.connection.execute_batch("BEGIN; CREATE TABLE IF NOT EXISTS identity_verifications (environment_id TEXT PRIMARY KEY REFERENCES environments(id), generation INTEGER NOT NULL, mode TEXT NOT NULL, status TEXT NOT NULL); PRAGMA user_version = 18; COMMIT;")?;
        }
        if version < 19 {
            self.connection.execute_batch("BEGIN; CREATE TABLE IF NOT EXISTS compose_resources (environment_id TEXT PRIMARY KEY REFERENCES environments(id), project_name TEXT NOT NULL UNIQUE, worktree TEXT NOT NULL, files_json TEXT NOT NULL, lifecycle TEXT NOT NULL DEFAULT 'active'); PRAGMA user_version = 19; COMMIT;")?;
        }
        Ok(())
    }
}

fn stored_compose_resource(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredComposeResource> {
    Ok(StoredComposeResource {
        environment_id: Uuid::parse_str(&row.get::<_, String>(0)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        project_name: row.get(1)?,
        worktree: row.get(2)?,
        files: serde_json::from_str(&row.get::<_, String>(3)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        lifecycle: row.get(4)?,
    })
}

fn stored_binding(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredBinding> {
    Ok(StoredBinding {
        environment_id: Uuid::parse_str(&row.get::<_, String>(0)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        phase: row.get(1)?,
        integration: row.get(2)?,
        fingerprint: row.get(3)?,
        invalidation: row.get(4)?,
        public_values_json: row.get(5)?,
        private_path: row.get(6)?,
    })
}

fn stored_ios_device(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredIosDevice> {
    Ok(StoredIosDevice {
        environment_id: Uuid::parse_str(&row.get::<_, String>(0)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        udid: row.get(1)?,
        runtime: row.get(2)?,
        device_type: row.get(3)?,
        expected_name: row.get(4)?,
        lifecycle: row.get(5)?,
    })
}

fn stored_preparation_task(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredPreparationTask> {
    let attempts: i64 = row.get(3)?;
    Ok(StoredPreparationTask {
        environment_id: Uuid::parse_str(&row.get::<_, String>(0)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        kind: row.get(1)?,
        fingerprint: row.get(2)?,
        attempts: attempts
            .try_into()
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, attempts))?,
        status: row.get(4)?,
        diagnostic: row.get(5)?,
    })
}

fn stored_ios_app(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredIosApp> {
    let metro_port: i64 = row.get(3)?;
    Ok(StoredIosApp {
        environment_id: Uuid::parse_str(&row.get::<_, String>(0)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        bundle_id: row.get(1)?,
        udid: row.get(2)?,
        metro_port: metro_port
            .try_into()
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, metro_port))?,
        derived_data: row.get(4)?,
        native_fingerprint: row.get(5)?,
        launched: row.get(6)?,
    })
}

fn stored_identity_verification(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<StoredIdentityVerification> {
    Ok(StoredIdentityVerification {
        environment_id: Uuid::parse_str(&row.get::<_, String>(0)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        generation: u64::try_from(row.get::<_, i64>(1)?)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(1, 0))?,
        mode: row.get(2)?,
        status: row.get(3)?,
    })
}

fn stored_artifact(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredArtifact> {
    Ok(StoredArtifact {
        id: Uuid::parse_str(&row.get::<_, String>(0)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        timestamp_unix_ms: u64::try_from(row.get::<_, i64>(1)?)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(1, 0))?,
        environment_id: Uuid::parse_str(&row.get::<_, String>(2)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        generation: u64::try_from(row.get::<_, i64>(3)?)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, 0))?,
        worktree: row.get(4)?,
        head: row.get(5)?,
        code_fingerprint: row.get(6)?,
        build_identity: row.get(7)?,
        udid: row.get(8)?,
        kind: row.get(9)?,
        command: row.get(10)?,
        flow: row.get(11)?,
        result: row.get(12)?,
        app_running: row.get(13)?,
        functional_result: row.get(14)?,
        human_review: row.get(15)?,
        path: row.get(16)?,
        obsolete: row.get(17)?,
    })
}

fn stored_initialization_task(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<StoredInitializationTask> {
    let environment_id =
        Uuid::parse_str(&row.get::<_, String>(0)?).map_err(|_| rusqlite::Error::InvalidQuery)?;
    let generation: i64 = row.get(5)?;
    let attempts: i64 = row.get(6)?;
    Ok(StoredInitializationTask {
        environment_id,
        id: row.get(1)?,
        kind: row.get(2)?,
        rerun_policy: row.get(3)?,
        fingerprint: row.get(4)?,
        generation: generation
            .try_into()
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(5, generation))?,
        attempts: attempts
            .try_into()
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(6, attempts))?,
        status: row.get(7)?,
        diagnostic: row.get(8)?,
    })
}

fn stored_event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredEvent> {
    let sequence: i64 = row.get(0)?;
    let generation: i64 = row.get(5)?;
    let timestamp: String = row.get(2)?;
    Ok(StoredEvent {
        sequence: sequence
            .try_into()
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(0, sequence))?,
        id: Uuid::parse_str(&row.get::<_, String>(1)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        timestamp_unix_ms: timestamp
            .parse()
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        environment_id: Uuid::parse_str(&row.get::<_, String>(3)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        resource_id: row.get(4)?,
        generation: generation
            .try_into()
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(5, generation))?,
        operation_id: row
            .get::<_, Option<String>>(6)?
            .map(|value| Uuid::parse_str(&value).map_err(|_| rusqlite::Error::InvalidQuery))
            .transpose()?,
        kind: row.get(7)?,
        payload: serde_json::from_str(&row.get::<_, String>(8)?)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
    })
}

fn stored_operation_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredOperation> {
    let status: String = row.get(3)?;
    let status = match status.as_str() {
        "accepted" => OperationStatus::Accepted,
        "cancellation_requested" => OperationStatus::CancellationRequested,
        "cancelled" => OperationStatus::Cancelled,
        "completed" => OperationStatus::Completed,
        "failed" => OperationStatus::Failed,
        _ => return Err(rusqlite::Error::InvalidQuery),
    };
    let deadline = row
        .get::<_, Option<String>>(4)?
        .map(|value| {
            value
                .parse::<u128>()
                .map_err(|_| rusqlite::Error::InvalidQuery)
        })
        .transpose()?;
    Ok(StoredOperation {
        id: Uuid::parse_str(&row.get::<_, String>(0)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        environment_id: Uuid::parse_str(&row.get::<_, String>(1)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        kind: row.get(2)?,
        status,
        deadline_unix_ms: deadline,
        cancellation_requested: row.get(5)?,
        phase: row.get(6)?,
        resource_id: row.get(7)?,
    })
}

fn validate_port_request(request: &PortLeaseRequest) -> Result<(), StateError> {
    if request.range_start == 0
        || request.range_start > request.range_end
        || request.block_size == 0
    {
        return Err(StateError::InvalidPortLease);
    }
    let mut offsets = std::collections::BTreeSet::new();
    if request.offsets.is_empty()
        || request.offsets.iter().any(|(name, offset)| {
            name.is_empty() || *offset >= request.block_size || !offsets.insert(offset)
        })
    {
        return Err(StateError::InvalidPortLease);
    }
    Ok(())
}

fn candidate_block_start(request: &PortLeaseRequest, occupied: &[(i64, i64)]) -> Option<u16> {
    let mut start = u32::from(request.range_start);
    while start + u32::from(request.block_size) - 1 <= u32::from(request.range_end) {
        let candidate = u16::try_from(start).ok()?;
        let candidate_end = i64::from(candidate) + i64::from(request.block_size) - 1;
        if occupied
            .iter()
            .all(|(start, size)| candidate_end < *start || i64::from(candidate) > start + size - 1)
        {
            return Some(candidate);
        }
        start += u32::from(request.block_size);
    }
    None
}

fn stored_port_lease_from_row(
    environment_key: &str,
) -> impl FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<StoredPortLease> + '_ {
    move |row| {
        let generation: i64 = row.get(1)?;
        let range_start: i64 = row.get(2)?;
        let range_end: i64 = row.get(3)?;
        let block_start: i64 = row.get(4)?;
        let block_size: i64 = row.get(5)?;
        let state: String = row.get(7)?;
        let offsets: BTreeMap<String, u16> = serde_json::from_str(&row.get::<_, String>(6)?)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let state = match state.as_str() {
            "reserved" => PortLeaseState::Reserved,
            "stopped" => PortLeaseState::Stopped,
            "quarantined" => PortLeaseState::Quarantined,
            _ => return Err(rusqlite::Error::InvalidQuery),
        };
        Ok(StoredPortLease {
            environment_key: environment_key.into(),
            project_fingerprint: row.get(0)?,
            generation: generation
                .try_into()
                .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(1, generation))?,
            range_start: range_start
                .try_into()
                .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(2, range_start))?,
            range_end: range_end
                .try_into()
                .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, range_end))?,
            block_start: block_start
                .try_into()
                .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(4, block_start))?,
            block_size: block_size
                .try_into()
                .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(5, block_size))?,
            offsets,
            state,
        })
    }
}

fn stored_environment(row: &rusqlite::Row<'_>) -> Result<StoredEnvironment, StateError> {
    let pid = row.get::<_, Option<i64>>(18)?;
    let process_group = row.get::<_, Option<i32>>(19)?;
    let started_at = row.get::<_, Option<String>>(20)?;
    let supervisor_socket = row.get::<_, Option<String>>(21)?;
    let readiness = row.get::<_, Option<String>>(22)?;
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
        endpoints: serde_json::from_str(&row.get::<_, String>(17)?)?,
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
            endpoints: vec![StoredEndpoint {
                name: "api".into(),
                service: "api".into(),
                internal_url: "http://api:3000".into(),
                host_url: "http://127.0.0.1:20001".into(),
                device_url: "http://127.0.0.1:20001".into(),
                readiness_path: "/health".into(),
                healthy: false,
            }],
        }
    }

    #[test]
    fn persists_intent_process_identity_and_stop_state() {
        let mut store = StateStore::open_in_memory().expect("state store opens");
        let environment = environment();
        let operation_id = Uuid::now_v7();
        store
            .record_start_intent(&environment, operation_id)
            .expect("intent persists");
        store
            .set_operation_context(operation_id, 100, "process")
            .expect("context persists");
        let cancellation = store
            .request_operation_cancel(operation_id)
            .expect("cancellation persists")
            .expect("operation exists");
        assert_eq!(cancellation.status, OperationStatus::CancellationRequested);
        assert!(cancellation.cancellation_requested);
        store
            .mark_operation_cancelled(operation_id, "reaped")
            .expect("cancelled persists");
        assert_eq!(
            store
                .operation(operation_id)
                .expect("operation reads")
                .expect("operation exists")
                .status,
            OperationStatus::Cancelled
        );
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
        assert_eq!(observed.endpoints[0].host_url, "http://127.0.0.1:20001");
    }

    #[test]
    fn reserves_one_simulator_udid_for_one_environment_only() {
        let mut store = StateStore::open_in_memory().expect("state store opens");
        let first = environment();
        let second = StoredEnvironment {
            id: Uuid::now_v7(),
            key: "second".into(),
            ..environment()
        };
        store
            .record_start_intent(&first, Uuid::now_v7())
            .expect("first environment persists");
        store
            .record_start_intent(&second, Uuid::now_v7())
            .expect("second environment persists");
        for environment in [&first, &second] {
            store
                .record_ios_reservation(&StoredIosDevice {
                    environment_id: environment.id,
                    udid: None,
                    runtime: "runtime-18".into(),
                    device_type: "phone-16".into(),
                    expected_name: format!("DevFleet-{}", environment.id),
                    lifecycle: "reserved".into(),
                })
                .expect("reservation persists");
        }
        let udid = "0186D916-7A5C-4E31-8590-C2F502FA2288";
        assert!(store.bind_ios_udid(first.id, udid).expect("first binds"));
        assert!(store.bind_ios_udid(second.id, udid).is_err());
        let retained = store
            .ios_device(first.id)
            .expect("device reads")
            .expect("device exists");
        assert_eq!(retained.udid.as_deref(), Some(udid));
    }

    #[test]
    fn retains_completed_preparation_by_kind_and_fingerprint() {
        let mut store = StateStore::open_in_memory().expect("state store opens");
        let environment = environment();
        store
            .record_start_intent(&environment, Uuid::now_v7())
            .expect("environment persists");
        let task = StoredPreparationTask {
            environment_id: environment.id,
            kind: "dependencies".into(),
            fingerprint: "lockfile-a".into(),
            attempts: 0,
            status: "running".into(),
            diagnostic: None,
        };
        store
            .record_preparation_intent(&task)
            .expect("intent persists");
        store
            .finish_preparation_task(environment.id, "dependencies", None)
            .expect("completion persists");
        let retained = store.preparation_tasks(environment.id).expect("tasks read");
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].fingerprint, "lockfile-a");
        assert_eq!(retained[0].status, "completed");
    }

    #[test]
    fn retains_an_ios_app_installation_per_environment() {
        let mut store = StateStore::open_in_memory().expect("state store opens");
        let environment = environment();
        store
            .record_start_intent(&environment, Uuid::now_v7())
            .expect("environment persists");
        store
            .record_ios_app(&StoredIosApp {
                environment_id: environment.id,
                bundle_id: "com.example.app".into(),
                udid: "0186D916-7A5C-4E31-8590-C2F502FA2288".into(),
                metro_port: 21_001,
                derived_data: "/private/derived-data".into(),
                native_fingerprint: "native-a".into(),
                launched: true,
            })
            .expect("app persists");
        let app = store
            .ios_app(environment.id)
            .expect("app reads")
            .expect("app exists");
        assert_eq!(app.bundle_id, "com.example.app");
        assert_eq!(app.metro_port, 21_001);
        assert!(app.launched);
    }

    #[test]
    fn retains_identity_status_without_nonce_or_token() {
        let mut store = StateStore::open_in_memory().expect("state store opens");
        let environment = environment();
        store
            .record_start_intent(&environment, Uuid::now_v7())
            .expect("environment persists");
        let verification = StoredIdentityVerification {
            environment_id: environment.id,
            generation: environment.generation,
            mode: "app_handshake".into(),
            status: "pending".into(),
        };
        store
            .record_identity_verification(&verification)
            .expect("verification persists");
        assert_eq!(
            store
                .identity_verification(environment.id)
                .expect("verification reads"),
            Some(verification)
        );
    }

    #[test]
    fn retains_artifact_metadata_without_media_bytes() {
        let mut store = StateStore::open_in_memory().expect("state store opens");
        let environment = environment();
        store
            .record_start_intent(&environment, Uuid::now_v7())
            .expect("environment persists");
        let artifact = StoredArtifact {
            id: Uuid::now_v7(),
            timestamp_unix_ms: 1_725_000_000_000,
            environment_id: environment.id,
            generation: environment.generation,
            worktree: "/private/worktree".into(),
            head: "abc123".into(),
            code_fingerprint: "local-js".into(),
            build_identity: "native-a".into(),
            udid: "0186D916-7A5C-4E31-8590-C2F502FA2288".into(),
            kind: "screenshot".into(),
            command: "xcrun simctl io <udid> screenshot".into(),
            flow: "device.capture".into(),
            result: "captured".into(),
            app_running: true,
            functional_result: "unverified".into(),
            human_review: "unreviewed".into(),
            path: "/private/artifacts/capture.png".into(),
            obsolete: false,
        };
        store.record_artifact(&artifact).expect("artifact persists");
        assert_eq!(
            store.artifacts(environment.id).expect("artifacts read"),
            vec![artifact]
        );
    }

    #[test]
    fn retains_destroyed_compose_metadata_and_environment_history() {
        let mut store = StateStore::open_in_memory().expect("state store opens");
        let environment = environment();
        store
            .record_start_intent(&environment, Uuid::now_v7())
            .expect("environment persists");
        let resource = StoredComposeResource {
            environment_id: environment.id,
            project_name: "devfleet-project-environment".into(),
            worktree: "/private/worktree".into(),
            files: vec!["/private/worktree/compose.yaml".into()],
            lifecycle: "active".into(),
        };
        store
            .record_compose_resource(&resource)
            .expect("Compose ownership persists");
        store
            .mark_compose_destroyed(environment.id)
            .expect("Compose teardown persists");
        store
            .mark_destroyed(environment.id)
            .expect("environment teardown persists");
        let resource = store
            .compose_resource(environment.id)
            .expect("Compose resource reads")
            .expect("Compose resource retained");
        assert_eq!(resource.lifecycle, "destroyed");
        assert_eq!(
            store
                .environment(environment.id)
                .expect("environment reads")
                .expect("environment retained")
                .lifecycle,
            StoredLifecycle::Destroyed
        );
    }

    #[test]
    fn endpoint_change_invalidates_apps_in_the_same_worktree() {
        let mut store = StateStore::open_in_memory().expect("state store opens");
        let first = environment();
        let second = StoredEnvironment {
            id: Uuid::now_v7(),
            key: "second".into(),
            ..first.clone()
        };
        store
            .record_start_intent(&first, Uuid::now_v7())
            .expect("first environment persists");
        store
            .record_start_intent(&second, Uuid::now_v7())
            .expect("second environment persists");
        store
            .record_ios_app(&StoredIosApp {
                environment_id: second.id,
                bundle_id: "com.example.app".into(),
                udid: "0186D916-7A5C-4E31-8590-C2F502FA2288".into(),
                metro_port: 21_001,
                derived_data: "/private/derived-data".into(),
                native_fingerprint: "native-a".into(),
                launched: true,
            })
            .expect("app persists");
        store
            .record_identity_verification(&StoredIdentityVerification {
                environment_id: second.id,
                generation: second.generation,
                mode: "app_handshake".into(),
                status: "verified".into(),
            })
            .expect("verification persists");
        store
            .set_endpoints(
                first.id,
                &[StoredEndpoint {
                    name: "metro".into(),
                    service: "metro".into(),
                    internal_url: "http://127.0.0.1:21001".into(),
                    host_url: "http://127.0.0.1:21001".into(),
                    device_url: "http://127.0.0.1:21001".into(),
                    readiness_path: "/identity".into(),
                    healthy: true,
                }],
            )
            .expect("endpoint update persists");
        assert_eq!(
            store
                .environment(second.id)
                .expect("environment reads")
                .expect("environment exists")
                .generation,
            second.generation + 1
        );
        assert!(
            !store
                .ios_app(second.id)
                .expect("app reads")
                .expect("app exists")
                .launched
        );
        assert_eq!(
            store
                .identity_verification(second.id)
                .expect("verification reads")
                .expect("verification exists")
                .status,
            "mismatch"
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

    #[test]
    fn increments_plan_generation_only_for_a_changed_redacted_snapshot() {
        let mut store = StateStore::open_in_memory().expect("state store opens");
        let snapshot = serde_json::json!({"token": "${{ secrets.API_TOKEN }}", "port": 4000});
        let first = store
            .record_effective_plan("project", "worktree", "/tmp/worktree", "dev", &snapshot)
            .expect("first plan persists");
        let repeated = store
            .record_effective_plan("project", "worktree", "/tmp/worktree", "dev", &snapshot)
            .expect("same plan persists");
        let changed = store
            .record_effective_plan(
                "project",
                "worktree",
                "/tmp/worktree",
                "dev",
                &serde_json::json!({"token": "${{ secrets.API_TOKEN }}", "port": 5000}),
            )
            .expect("changed plan persists");
        assert_eq!(first.generation, 1);
        assert_eq!(repeated.generation, 1);
        assert_eq!(changed.generation, 2);
    }

    #[test]
    fn allocates_distinct_blocks_and_retains_stopped_or_quarantined_preferences() {
        let mut store = StateStore::open_in_memory().expect("state opens");
        let request = |environment_key: &str| PortLeaseRequest {
            environment_key: environment_key.into(),
            project_fingerprint: "project".into(),
            generation: 1,
            range_start: 20_000,
            range_end: 20_127,
            block_size: 64,
            offsets: BTreeMap::from([("api".into(), 1), ("metro".into(), 2)]),
        };
        let first = store
            .reserve_port_lease(&request("one"))
            .expect("first lease");
        let overlapping = PortLeaseRequest {
            environment_key: "overlap".into(),
            project_fingerprint: "other-project".into(),
            generation: 1,
            range_start: 20_032,
            range_end: 20_095,
            block_size: 64,
            offsets: BTreeMap::from([("api".into(), 1)]),
        };
        assert!(matches!(
            store.reserve_port_lease(&overlapping),
            Err(StateError::PortRangeExhausted)
        ));
        let second = store
            .reserve_port_lease(&request("two"))
            .expect("second lease");
        assert_ne!(first.block_start, second.block_start);
        assert!(store.mark_port_lease_stopped("one").expect("stops lease"));
        let retained = store
            .reserve_port_lease(&request("one"))
            .expect("retains preference");
        assert_eq!(retained.block_start, first.block_start);
        assert!(
            store
                .quarantine_port_lease("two")
                .expect("quarantines lease")
        );
        assert!(matches!(
            store.reserve_port_lease(&request("three")),
            Err(StateError::PortRangeExhausted)
        ));
    }

    #[test]
    fn resumes_events_by_sequence_and_rejects_expired_cursor() {
        let mut store = StateStore::open_in_memory().expect("state opens");
        let environment_id = Uuid::now_v7();
        let other_environment_id = Uuid::now_v7();
        for kind in ["planning", "running"] {
            store
                .append_event(&StoredEvent {
                    sequence: 0,
                    id: Uuid::now_v7(),
                    timestamp_unix_ms: 1,
                    environment_id,
                    resource_id: Some("api".into()),
                    generation: 1,
                    operation_id: None,
                    kind: kind.into(),
                    payload: serde_json::json!({"safe": true}),
                })
                .expect("event persists");
        }
        store
            .append_event(&StoredEvent {
                sequence: 0,
                id: Uuid::now_v7(),
                timestamp_unix_ms: 1,
                environment_id: other_environment_id,
                resource_id: Some("other".into()),
                generation: 1,
                operation_id: None,
                kind: "other".into(),
                payload: serde_json::json!({}),
            })
            .expect("other event persists");
        let events = store
            .events_after(environment_id, Some(1))
            .expect("cursor resumes");
        assert_eq!(events.len(), 1);
        let initial = store
            .events_after(environment_id, Some(0))
            .expect("initial cursor reads");
        assert_eq!(initial.len(), 2);
        assert!(
            initial
                .iter()
                .all(|event| event.environment_id == environment_id)
        );
        for sequence in 0..MAX_RETAINED_EVENTS_PER_ENVIRONMENT {
            store
                .append_event(&StoredEvent {
                    sequence: 0,
                    id: Uuid::now_v7(),
                    timestamp_unix_ms: u128::try_from(sequence).expect("positive sequence"),
                    environment_id,
                    resource_id: Some("api".into()),
                    generation: 1,
                    operation_id: None,
                    kind: "retained".into(),
                    payload: serde_json::json!({}),
                })
                .expect("event persists");
        }
        assert!(matches!(
            store.events_after(environment_id, Some(0)),
            Err(StateError::CursorExpired)
        ));
    }
}
