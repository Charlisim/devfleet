//! Versioned SQLite storage primitives. Domain policy belongs in the daemon/core.

use rusqlite::Connection;
use thiserror::Error;

pub const CURRENT_SCHEMA_VERSION: i64 = 1;

#[derive(Debug, Error)]
pub enum StateError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("database schema {found} is newer than supported schema {supported}")]
    NewerSchema { found: i64, supported: i64 },
}

pub struct StateStore {
    connection: Connection,
}

impl StateStore {
    /// Opens a SQLite store and applies supported migrations.
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

    fn migrate(&mut self) -> Result<(), StateError> {
        let version = self.schema_version()?;
        if version > CURRENT_SCHEMA_VERSION {
            return Err(StateError::NewerSchema {
                found: version,
                supported: CURRENT_SCHEMA_VERSION,
            });
        }
        if version < CURRENT_SCHEMA_VERSION {
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_a_versioned_schema() {
        let store = StateStore::open_in_memory().expect("state store opens");
        assert_eq!(
            store.schema_version().expect("schema version"),
            CURRENT_SCHEMA_VERSION
        );
    }
}
