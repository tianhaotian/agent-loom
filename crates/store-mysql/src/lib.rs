//! MySQL/InnoDB provider bootstrap and embedded migration manifest.

use agent_loom_durable_store::{EmbeddedMigration, StoreCapabilities};

pub const PROVIDER_KIND: &str = "mysql";
pub const TRANSACTION_ISOLATION: &str = "READ COMMITTED";
pub const CONNECTION_TIME_ZONE: &str = "+00:00";

pub const MIGRATIONS: &[EmbeddedMigration] = &[EmbeddedMigration {
    logical_id: "0000_migration_meta",
    logical_model_version: 1,
    sql: include_str!("../migrations/0000_migration_meta.sql"),
}];

pub const fn capabilities() -> StoreCapabilities {
    StoreCapabilities::PORTABLE_BASELINE
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MySqlConfig {
    pub database: String,
    pub lock_wait_timeout_seconds: u32,
    pub require_strict_sql_mode: bool,
}

impl MySqlConfig {
    pub fn new(database: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            lock_wait_timeout_seconds: 2,
            require_strict_sql_mode: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_migration_is_embedded() {
        assert_eq!(MIGRATIONS.len(), 1);
        assert_eq!(MIGRATIONS[0].logical_id, "0000_migration_meta");
        assert!(MIGRATIONS[0].sql.contains("schema_migrations"));
        assert!(MIGRATIONS[0].sql.contains("ENGINE=InnoDB"));
        assert!(MIGRATIONS[0].sql.contains("datetime(6)"));
    }
}
