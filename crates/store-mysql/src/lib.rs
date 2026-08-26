//! MySQL/InnoDB provider bootstrap and embedded migration manifest.

use agent_loom_durable_store::{EmbeddedMigration, StoreCapabilities};

pub const PROVIDER_KIND: &str = "mysql";
pub const TRANSACTION_ISOLATION: &str = "READ COMMITTED";
pub const CONNECTION_TIME_ZONE: &str = "+00:00";

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
    EmbeddedMigration {
        logical_id: "0004_stage_task_checkpoint",
        logical_model_version: 5,
        created_tables: &["stage_executions", "tasks", "task_attempts", "checkpoints"],
        sql: include_str!("../migrations/0004_stage_task_checkpoint.sql"),
    },
    EmbeddedMigration {
        logical_id: "0005_wait_artifact",
        logical_model_version: 6,
        created_tables: &["wait_subscriptions", "artifact_refs"],
        sql: include_str!("../migrations/0005_wait_artifact.sql"),
    },
    EmbeddedMigration {
        logical_id: "0006_external_executions",
        logical_model_version: 7,
        created_tables: &[
            "tool_executions",
            "tool_execution_attempts",
            "agent_executions",
            "agent_event_receipts",
        ],
        sql: include_str!("../migrations/0006_external_executions.sql"),
    },
    EmbeddedMigration {
        logical_id: "0007_wait_resume_plan",
        logical_model_version: 8,
        created_tables: &[],
        sql: include_str!("../migrations/0007_wait_resume_plan.sql"),
    },
    EmbeddedMigration {
        logical_id: "0008_tool_retry_schedule",
        logical_model_version: 9,
        created_tables: &[],
        sql: include_str!("../migrations/0008_tool_retry_schedule.sql"),
    },
];

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
    fn migration_batch_is_embedded() {
        assert_eq!(MIGRATIONS.len(), 9);
        assert_eq!(MIGRATIONS[0].logical_id, "0000_migration_meta");
        assert!(MIGRATIONS[0].sql.contains("schema_migrations"));
        assert!(MIGRATIONS[0].sql.contains("ENGINE=InnoDB"));
        assert!(MIGRATIONS[0].sql.contains("datetime(6)"));
        assert!(MIGRATIONS[3].sql.contains("terminal_event_id"));
        assert!(MIGRATIONS[4].sql.contains("lease_token"));
        assert!(MIGRATIONS[5].sql.contains("active_slot"));
        assert!(MIGRATIONS[6].sql.contains("agent_event_receipts"));
        assert!(MIGRATIONS[7].sql.contains("resume_task_id"));
        assert!(MIGRATIONS[8].sql.contains("retry_at"));
    }
}
