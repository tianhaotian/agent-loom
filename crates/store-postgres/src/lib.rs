//! PostgreSQL provider bootstrap and embedded migration manifest.

use agent_loom_durable_store::{EmbeddedMigration, StoreCapabilities};

pub const PROVIDER_KIND: &str = "postgres";
pub const DEFAULT_SCHEMA: &str = "agent_loom";
pub const TRANSACTION_ISOLATION: &str = "READ COMMITTED";

pub const MIGRATIONS: &[EmbeddedMigration] = &[EmbeddedMigration {
    logical_id: "0000_migration_meta",
    logical_model_version: 1,
    sql: include_str!("../migrations/0000_migration_meta.sql"),
}];

pub const fn capabilities() -> StoreCapabilities {
    StoreCapabilities {
        wakeup_notification: false,
        ..StoreCapabilities::PORTABLE_BASELINE
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresConfig {
    pub schema: String,
    pub statement_timeout_ms: u64,
    pub lock_timeout_ms: u64,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            schema: DEFAULT_SCHEMA.to_owned(),
            statement_timeout_ms: 5_000,
            lock_timeout_ms: 1_000,
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
        assert!(MIGRATIONS[0].sql.contains("timestamptz(6)"));
    }
}
