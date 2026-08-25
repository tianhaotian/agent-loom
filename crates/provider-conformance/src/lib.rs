//! Provider-neutral migration and behavior contract tests.

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use agent_loom_durable_store::{EmbeddedMigration, validate_migration_manifest};
    use agent_loom_store_mysql as mysql;
    use agent_loom_store_postgres as postgres;

    const EXPECTED_TABLES: &[&str] = &[
        "schema_migrations",
        "tenants",
        "workflow_definitions",
        "workflow_definition_versions",
        "agent_definitions",
        "agent_definition_versions",
        "agent_endpoints",
        "runs",
        "events",
        "command_receipts",
        "stage_executions",
        "tasks",
        "task_attempts",
        "checkpoints",
    ];

    #[test]
    fn provider_manifests_are_valid_and_logically_equivalent() {
        assert_eq!(validate_migration_manifest(postgres::MIGRATIONS), Ok(()));
        assert_eq!(validate_migration_manifest(mysql::MIGRATIONS), Ok(()));
        assert_eq!(
            logical_snapshot(postgres::MIGRATIONS),
            logical_snapshot(mysql::MIGRATIONS)
        );
        assert_eq!(created_tables(postgres::MIGRATIONS), EXPECTED_TABLES);
    }

    #[test]
    fn physical_sql_preserves_provider_baselines() {
        for migration in postgres::MIGRATIONS {
            assert!(migration.sql.contains("agent_loom."));
            for table in migration.created_tables {
                assert!(
                    migration
                        .sql
                        .contains(&format!("CREATE TABLE agent_loom.{table}"))
                );
            }
        }
        for migration in mysql::MIGRATIONS {
            assert_eq!(
                migration.sql.matches("ENGINE=InnoDB").count(),
                migration.created_tables.len()
            );
            for table in migration.created_tables {
                assert!(migration.sql.contains(&format!("CREATE TABLE {table}")));
            }
        }
    }

    #[test]
    fn mysql_checks_are_enforced_and_identifiers_fit_engine_limits() {
        let mut constraint_names = HashSet::new();

        for migration in mysql::MIGRATIONS {
            assert_eq!(
                migration.sql.matches("CHECK (").count(),
                migration.sql.matches("ENFORCED").count()
            );

            for name in names_after_keyword(migration.sql, "CONSTRAINT") {
                assert!(name.len() <= 64, "MySQL identifier is too long: {name}");
                assert!(
                    constraint_names.insert(name),
                    "duplicate MySQL constraint name: {name}"
                );
            }
        }
    }

    #[test]
    fn identity_keys_use_deterministic_case_sensitive_collations() {
        let postgres_sql = postgres::MIGRATIONS[1].sql;
        let mysql_sql = mysql::MIGRATIONS[1].sql;

        for key in ["tenant_key", "workflow_key", "agent_key"] {
            assert!(postgres_sql.contains(&format!("{key} varchar(255) COLLATE \"C\"")));
            assert!(mysql_sql.contains(&format!("{key} varchar(255) COLLATE utf8mb4_0900_bin")));
        }
    }

    #[test]
    fn terminal_event_ownership_is_enforced_by_both_providers() {
        for migrations in [postgres::MIGRATIONS, mysql::MIGRATIONS] {
            let sql = migrations[3].sql;
            assert!(sql.contains("fk_runs__terminal_event"));
            assert!(sql.contains("tenant_id, run_id, terminal_event_id"));
            assert!(sql.contains("tenant_id, run_id, event_id"));
        }
    }

    #[test]
    fn task_lease_and_checkpoint_ownership_are_enforced_by_both_providers() {
        for migrations in [postgres::MIGRATIONS, mysql::MIGRATIONS] {
            let sql = migrations[4].sql;
            assert!(sql.contains("ck_tasks__lease"));
            assert!(sql.contains("status = 'leased' AND lease_owner IS NOT NULL"));
            assert!(sql.contains("fk_runs__current_checkpoint"));
            assert!(sql.contains("tenant_id, run_id, current_checkpoint_id"));
            assert!(sql.contains("tenant_id, run_id, checkpoint_id"));
            assert!(sql.contains("fk_runs__parent_task"));
            assert!(sql.contains("tenant_id, parent_run_id, parent_task_id"));
        }
    }

    #[test]
    fn task_claim_indexes_preserve_portable_ordering() {
        for migrations in [postgres::MIGRATIONS, mysql::MIGRATIONS] {
            let sql = migrations[4].sql;
            assert!(sql.contains("ix_tasks__claim_global"));
            assert!(sql.contains("status, available_at, priority DESC, task_id"));
            assert!(sql.contains("ix_tasks__lease_reclaim"));
        }
    }

    fn logical_snapshot(
        migrations: &[EmbeddedMigration],
    ) -> Vec<(&'static str, u64, &'static [&'static str])> {
        migrations
            .iter()
            .map(|migration| {
                (
                    migration.logical_id,
                    migration.logical_model_version,
                    migration.created_tables,
                )
            })
            .collect()
    }

    fn created_tables(migrations: &[EmbeddedMigration]) -> Vec<&'static str> {
        migrations
            .iter()
            .flat_map(|migration| migration.created_tables.iter().copied())
            .collect()
    }

    fn names_after_keyword<'a>(sql: &'a str, keyword: &'a str) -> impl Iterator<Item = &'a str> {
        let mut tokens = sql.split_ascii_whitespace();
        std::iter::from_fn(move || {
            loop {
                if tokens.next()? == keyword {
                    return tokens
                        .next()
                        .map(|token| token.trim_end_matches(['(', ',']));
                }
            }
        })
    }
}
