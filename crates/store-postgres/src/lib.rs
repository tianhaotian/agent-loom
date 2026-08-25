//! PostgreSQL provider bootstrap and embedded migration manifest.

use agent_loom_durable_store::{EmbeddedMigration, StoreCapabilities};

pub const PROVIDER_KIND: &str = "postgres";
pub const DEFAULT_SCHEMA: &str = "agent_loom";
pub const TRANSACTION_ISOLATION: &str = "READ COMMITTED";

pub const MIGRATIONS: &[EmbeddedMigration] = &[
    EmbeddedMigration {
        logical_id: "0000_migration_meta",
        logical_model_version: 1,
        created_tables: &["schema_migrations"],
        sql: include_str!("../migrations/0000_migration_meta.sql"),
    },
    EmbeddedMigration {
        logical_id: "0001_identity_definitions",
        logical_model_version: 2,
        created_tables: &[
            "tenants",
            "workflow_definitions",
            "workflow_definition_versions",
            "agent_definitions",
            "agent_definition_versions",
        ],
        sql: include_str!("../migrations/0001_identity_definitions.sql"),
    },
    EmbeddedMigration {
        logical_id: "0002_agent_endpoints",
        logical_model_version: 3,
        created_tables: &["agent_endpoints"],
        sql: include_str!("../migrations/0002_agent_endpoints.sql"),
    },
    EmbeddedMigration {
        logical_id: "0003_run_event_idempotency",
        logical_model_version: 4,
        created_tables: &["runs", "events", "command_receipts"],
        sql: include_str!("../migrations/0003_run_event_idempotency.sql"),
    },
];

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
    fn migration_batch_is_embedded() {
        assert_eq!(MIGRATIONS.len(), 4);
        assert_eq!(MIGRATIONS[0].logical_id, "0000_migration_meta");
        assert!(MIGRATIONS[0].sql.contains("schema_migrations"));
        assert!(MIGRATIONS[0].sql.contains("timestamptz(6)"));
        assert!(MIGRATIONS[3].sql.contains("terminal_event_id"));
    }
}
