//! Versioned SQLite storage. Domain orchestration remains in the daemon/core.

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};
use thiserror::Error;
use uuid::Uuid;

pub const CURRENT_SCHEMA_VERSION: i64 = 7;

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
        Ok(())
    }
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
}
