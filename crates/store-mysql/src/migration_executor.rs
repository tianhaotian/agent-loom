use std::{error::Error, fmt};

use agent_loom_domain::Digest;
use agent_loom_durable_store::{
    MigrationAssessmentError, MigrationCandidate, MigrationJournalEntry, MigrationState,
    assess_migrations,
};
use mysql_async::{Conn, Row, prelude::Queryable};
use serde_json::json;
use sha2::{Digest as _, Sha256};

use crate::{CONNECTION_TIME_ZONE, MIGRATIONS, MySqlConfig, PROVIDER_KIND, TRANSACTION_ISOLATION};

const BOOTSTRAP_LOGICAL_ID: &str = "0000_migration_meta";
const LOCK_TIMEOUT_SECONDS: u32 = 30;

#[derive(Debug)]
pub enum MySqlMigrationError {
    UnsupportedDatabase {
        configured: String,
        selected: String,
    },
    UnsafeSqlMode,
    Database {
        operation: &'static str,
        source: mysql_async::Error,
    },
    Assessment(MigrationAssessmentError),
    BootstrapHistoryMissing,
    BootstrapCandidateMissing,
    InvalidJournalValue {
        field: &'static str,
    },
    SchemaVerification {
        logical_id: &'static str,
        table: &'static str,
    },
    LockUnavailable,
    LockNotHeld,
}

impl fmt::Display for MySqlMigrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedDatabase {
                configured,
                selected,
            } => write!(
                formatter,
                "MySQL connection selected database {selected:?}, not configured database {configured:?}"
            ),
            Self::UnsafeSqlMode => formatter.write_str(
                "MySQL migration session does not enforce STRICT_TRANS_TABLES or STRICT_ALL_TABLES",
            ),
            Self::Database { operation, .. } => {
                write!(formatter, "MySQL migration operation failed: {operation}")
            }
            Self::Assessment(error) => write!(formatter, "migration history is unsafe: {error:?}"),
            Self::BootstrapHistoryMissing => formatter.write_str(
                "schema_migrations exists without the bootstrap migration journal entry",
            ),
            Self::BootstrapCandidateMissing => {
                formatter.write_str("embedded bootstrap migration is missing")
            }
            Self::InvalidJournalValue { field } => {
                write!(formatter, "migration journal contains an invalid {field}")
            }
            Self::SchemaVerification { logical_id, table } => write!(
                formatter,
                "migration {logical_id} did not create required table {table}"
            ),
            Self::LockUnavailable => formatter.write_str("MySQL migration lock is unavailable"),
            Self::LockNotHeld => formatter.write_str("MySQL migration lock was not held"),
        }
    }
}

impl Error for MySqlMigrationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database { source, .. } => Some(source),
            Self::UnsupportedDatabase { .. }
            | Self::UnsafeSqlMode
            | Self::Assessment(_)
            | Self::BootstrapHistoryMissing
            | Self::BootstrapCandidateMissing
            | Self::InvalidJournalValue { .. }
            | Self::SchemaVerification { .. }
            | Self::LockUnavailable
            | Self::LockNotHeld => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MySqlMigrationReport {
    pub previous_model_version: u64,
    pub target_model_version: u64,
    pub applied_logical_ids: Vec<&'static str>,
}

impl MySqlMigrationReport {
    pub fn changed(&self) -> bool {
        !self.applied_logical_ids.is_empty()
    }
}

pub fn migration_candidates() -> Vec<MigrationCandidate> {
    MIGRATIONS
        .iter()
        .map(|descriptor| MigrationCandidate {
            descriptor: *descriptor,
            physical_checksum: Digest::from_bytes(Sha256::digest(descriptor.sql.as_bytes()).into()),
        })
        .collect()
}

#[derive(Debug)]
pub struct MySqlMigrationExecutor<'a> {
    connection: &'a mut Conn,
    config: &'a MySqlConfig,
    runner_version: &'a str,
}

impl<'a> MySqlMigrationExecutor<'a> {
    pub const fn new(
        connection: &'a mut Conn,
        config: &'a MySqlConfig,
        runner_version: &'a str,
    ) -> Self {
        Self {
            connection,
            config,
            runner_version,
        }
    }

    /// Applies the contiguous migration suffix under a MySQL named session lock.
    ///
    /// MySQL DDL implicitly commits, so every migration is journaled as `applying`
    /// before its statements run and is marked `failed` on the first error.
    ///
    /// # Errors
    ///
    /// Returns a safe error for invalid session policy, divergent history,
    /// lock failure, DDL failure, or failed information-schema verification.
    pub async fn migrate(&mut self) -> Result<MySqlMigrationReport, MySqlMigrationError> {
        self.configure_session().await?;
        self.validate_database().await?;
        self.acquire_lock().await?;
        let result = self.migrate_locked().await;
        let release = self.release_lock().await;
        match (result, release) {
            (Ok(report), Ok(())) => Ok(report),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(primary), Err(_)) => Err(primary),
        }
    }

    async fn configure_session(&mut self) -> Result<(), MySqlMigrationError> {
        self.connection
            .query_drop(format!(
                "SET SESSION time_zone = '{CONNECTION_TIME_ZONE}', \
                 SESSION transaction_isolation = '{}', \
                 SESSION innodb_lock_wait_timeout = {}",
                TRANSACTION_ISOLATION.replace(' ', "-"),
                self.config.lock_wait_timeout_seconds
            ))
            .await
            .map_err(|source| database("configure_session", source))?;
        if self.config.require_strict_sql_mode {
            let mode: Option<String> = self
                .connection
                .query_first("SELECT @@SESSION.sql_mode")
                .await
                .map_err(|source| database("read_sql_mode", source))?;
            let strict = mode.is_some_and(|value| {
                value
                    .split(',')
                    .any(|item| matches!(item.trim(), "STRICT_TRANS_TABLES" | "STRICT_ALL_TABLES"))
            });
            if !strict {
                return Err(MySqlMigrationError::UnsafeSqlMode);
            }
        }
        Ok(())
    }

    async fn validate_database(&mut self) -> Result<(), MySqlMigrationError> {
        let selected: Option<String> = self
            .connection
            .query_first("SELECT DATABASE()")
            .await
            .map_err(|source| database("read_selected_database", source))?;
        let selected = selected.unwrap_or_default();
        if selected != self.config.database {
            return Err(MySqlMigrationError::UnsupportedDatabase {
                configured: self.config.database.clone(),
                selected,
            });
        }
        Ok(())
    }

    async fn acquire_lock(&mut self) -> Result<(), MySqlMigrationError> {
        let acquired: Option<u8> = self
            .connection
            .exec_first(
                "SELECT GET_LOCK(?, ?)",
                (self.lock_name(), LOCK_TIMEOUT_SECONDS),
            )
            .await
            .map_err(|source| database("acquire_named_lock", source))?;
        if acquired == Some(1) {
            Ok(())
        } else {
            Err(MySqlMigrationError::LockUnavailable)
        }
    }

    async fn release_lock(&mut self) -> Result<(), MySqlMigrationError> {
        let released: Option<u8> = self
            .connection
            .exec_first("SELECT RELEASE_LOCK(?)", (self.lock_name(),))
            .await
            .map_err(|source| database("release_named_lock", source))?;
        if released == Some(1) {
            Ok(())
        } else {
            Err(MySqlMigrationError::LockNotHeld)
        }
    }

    async fn migrate_locked(&mut self) -> Result<MySqlMigrationReport, MySqlMigrationError> {
        let candidates = migration_candidates();
        let bootstrapped = self.bootstrap_journal(&candidates).await?;
        let history = self.load_history().await?;
        let plan = assess_migrations(PROVIDER_KIND, &candidates, &history)
            .map_err(MySqlMigrationError::Assessment)?;
        let previous_model_version = if bootstrapped {
            0
        } else {
            plan.current_model_version
        };
        let target_model_version = plan.target_model_version;
        let mut applied_logical_ids = bootstrapped
            .then_some(BOOTSTRAP_LOGICAL_ID)
            .into_iter()
            .collect::<Vec<_>>();
        for candidate in plan.pending {
            self.start_journal(candidate).await?;
            if let Err(error) = self.execute_sql(candidate).await {
                let _ = self.mark_failed(candidate, stable_error_code(&error)).await;
                return Err(error);
            }
            if let Err(error) = self.verify_schema(candidate).await {
                let _ = self.mark_failed(candidate, stable_error_code(&error)).await;
                return Err(error);
            }
            self.mark_applied(candidate).await?;
            applied_logical_ids.push(candidate.descriptor.logical_id);
        }
        Ok(MySqlMigrationReport {
            previous_model_version,
            target_model_version,
            applied_logical_ids,
        })
    }

    async fn bootstrap_journal(
        &mut self,
        candidates: &[MigrationCandidate],
    ) -> Result<bool, MySqlMigrationError> {
        let exists: Option<u8> = self
            .connection
            .exec_first(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = DATABASE() AND table_name = 'schema_migrations')",
                (),
            )
            .await
            .map_err(|source| database("probe_migration_journal", source))?;
        if exists == Some(1) {
            let has_bootstrap: Option<u8> = self
                .connection
                .exec_first(
                    "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE logical_id = ?)",
                    (BOOTSTRAP_LOGICAL_ID,),
                )
                .await
                .map_err(|source| database("probe_bootstrap_history", source))?;
            return if has_bootstrap == Some(1) {
                Ok(false)
            } else {
                Err(MySqlMigrationError::BootstrapHistoryMissing)
            };
        }
        let bootstrap = candidates
            .iter()
            .find(|candidate| candidate.descriptor.logical_id == BOOTSTRAP_LOGICAL_ID)
            .copied()
            .ok_or(MySqlMigrationError::BootstrapCandidateMissing)?;
        execute_statements(self.connection, bootstrap.descriptor.sql).await?;
        self.verify_schema(bootstrap).await?;
        self.connection
            .exec_drop(
                "INSERT INTO schema_migrations (logical_id, provider_kind, physical_checksum, \
                 logical_model_version, state, started_at, applied_at, runner_version, details_json) \
                 VALUES (?, ?, ?, ?, 'applied', UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), ?, ?)",
                (
                    bootstrap.descriptor.logical_id,
                    PROVIDER_KIND,
                    bootstrap.physical_checksum.as_bytes().as_slice(),
                    bootstrap.descriptor.logical_model_version,
                    self.runner_version,
                    json!({"bootstrap": true}).to_string(),
                ),
            )
            .await
            .map_err(|source| database("journal_bootstrap", source))?;
        Ok(true)
    }

    async fn load_history(&mut self) -> Result<Vec<MigrationJournalEntry>, MySqlMigrationError> {
        let rows: Vec<Row> = self
            .connection
            .query(
                "SELECT logical_id, provider_kind, physical_checksum, logical_model_version, state \
                 FROM schema_migrations ORDER BY logical_id",
            )
            .await
            .map_err(|source| database("load_migration_history", source))?;
        rows.into_iter()
            .map(|mut row| {
                let logical_id =
                    row.take::<String, _>(0)
                        .ok_or(MySqlMigrationError::InvalidJournalValue {
                            field: "logical_id",
                        })?;
                let provider_kind =
                    row.take::<String, _>(1)
                        .ok_or(MySqlMigrationError::InvalidJournalValue {
                            field: "provider_kind",
                        })?;
                let checksum =
                    row.take::<Vec<u8>, _>(2)
                        .ok_or(MySqlMigrationError::InvalidJournalValue {
                            field: "physical_checksum",
                        })?;
                let checksum: [u8; 32] =
                    checksum
                        .try_into()
                        .map_err(|_| MySqlMigrationError::InvalidJournalValue {
                            field: "physical_checksum",
                        })?;
                let logical_model_version =
                    row.take::<u64, _>(3)
                        .ok_or(MySqlMigrationError::InvalidJournalValue {
                            field: "logical_model_version",
                        })?;
                let state = match row.take::<String, _>(4).as_deref() {
                    Some("applying") => MigrationState::Applying,
                    Some("applied") => MigrationState::Applied,
                    Some("failed") => MigrationState::Failed,
                    _ => {
                        return Err(MySqlMigrationError::InvalidJournalValue { field: "state" });
                    }
                };
                Ok(MigrationJournalEntry {
                    logical_id,
                    provider_kind,
                    physical_checksum: Digest::from_bytes(checksum),
                    logical_model_version,
                    state,
                })
            })
            .collect()
    }

    async fn start_journal(
        &mut self,
        candidate: MigrationCandidate,
    ) -> Result<(), MySqlMigrationError> {
        self.connection
            .exec_drop(
                "INSERT INTO schema_migrations (logical_id, provider_kind, physical_checksum, \
                 logical_model_version, state, started_at, applied_at, runner_version, details_json) \
                 VALUES (?, ?, ?, ?, 'applying', UTC_TIMESTAMP(6), NULL, ?, ?)",
                (
                    candidate.descriptor.logical_id,
                    PROVIDER_KIND,
                    candidate.physical_checksum.as_bytes().as_slice(),
                    candidate.descriptor.logical_model_version,
                    self.runner_version,
                    json!({}).to_string(),
                ),
            )
            .await
            .map_err(|source| database("start_migration_journal", source))
    }

    async fn execute_sql(
        &mut self,
        candidate: MigrationCandidate,
    ) -> Result<(), MySqlMigrationError> {
        execute_statements(self.connection, candidate.descriptor.sql).await
    }

    async fn verify_schema(
        &mut self,
        candidate: MigrationCandidate,
    ) -> Result<(), MySqlMigrationError> {
        for table in candidate.descriptor.created_tables {
            let exists: Option<u8> = self
                .connection
                .exec_first(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
                     WHERE table_schema = DATABASE() AND table_name = ?)",
                    (*table,),
                )
                .await
                .map_err(|source| database("verify_schema", source))?;
            if exists != Some(1) {
                return Err(MySqlMigrationError::SchemaVerification {
                    logical_id: candidate.descriptor.logical_id,
                    table,
                });
            }
        }
        Ok(())
    }

    async fn mark_applied(
        &mut self,
        candidate: MigrationCandidate,
    ) -> Result<(), MySqlMigrationError> {
        self.connection
            .exec_drop(
                "UPDATE schema_migrations SET state = 'applied', applied_at = UTC_TIMESTAMP(6), \
                 details_json = ? WHERE logical_id = ? AND state = 'applying'",
                (json!({}).to_string(), candidate.descriptor.logical_id),
            )
            .await
            .map_err(|source| database("mark_migration_applied", source))?;
        if self.connection.affected_rows() != 1 {
            return Err(MySqlMigrationError::InvalidJournalValue {
                field: "applying row count",
            });
        }
        Ok(())
    }

    async fn mark_failed(
        &mut self,
        candidate: MigrationCandidate,
        error_code: &str,
    ) -> Result<(), MySqlMigrationError> {
        self.connection
            .exec_drop(
                "UPDATE schema_migrations SET state = 'failed', applied_at = NULL, details_json = ? \
                 WHERE logical_id = ? AND state = 'applying'",
                (
                    json!({"error_code": error_code}).to_string(),
                    candidate.descriptor.logical_id,
                ),
            )
            .await
            .map_err(|source| database("mark_migration_failed", source))?;
        if self.connection.affected_rows() != 1 {
            return Err(MySqlMigrationError::InvalidJournalValue {
                field: "failed row count",
            });
        }
        Ok(())
    }

    fn lock_name(&self) -> String {
        format!("agent-loom:migrations:{}", self.config.database)
    }
}

async fn execute_statements(connection: &mut Conn, sql: &str) -> Result<(), MySqlMigrationError> {
    for statement in sql.split(';').map(str::trim).filter(|sql| !sql.is_empty()) {
        connection
            .query_drop(statement)
            .await
            .map_err(|source| database("execute_migration_ddl", source))?;
    }
    Ok(())
}

fn database(operation: &'static str, source: mysql_async::Error) -> MySqlMigrationError {
    MySqlMigrationError::Database { operation, source }
}

const fn stable_error_code(error: &MySqlMigrationError) -> &'static str {
    match error {
        MySqlMigrationError::Database { .. } => "MYSQL_DATABASE_ERROR",
        MySqlMigrationError::SchemaVerification { .. } => "SCHEMA_VERIFICATION_FAILED",
        MySqlMigrationError::UnsupportedDatabase { .. }
        | MySqlMigrationError::UnsafeSqlMode
        | MySqlMigrationError::Assessment(_)
        | MySqlMigrationError::BootstrapHistoryMissing
        | MySqlMigrationError::BootstrapCandidateMissing
        | MySqlMigrationError::InvalidJournalValue { .. }
        | MySqlMigrationError::LockUnavailable
        | MySqlMigrationError::LockNotHeld => "MIGRATION_POLICY_ERROR",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_checksums_are_stable_and_nonzero() {
        let candidates = migration_candidates();
        assert_eq!(candidates.len(), MIGRATIONS.len());
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.physical_checksum != Digest::from_bytes([0; 32]))
        );
    }

    #[tokio::test]
    async fn migration_smoke_test_when_mysql_url_is_configured() {
        let Ok(url) = std::env::var("AGENT_LOOM_TEST_MYSQL_URL") else {
            return;
        };
        let opts = mysql_async::Opts::from_url(&url).expect("parse MySQL test URL");
        let database = opts
            .db_name()
            .expect("MySQL test URL includes a database")
            .to_owned();
        let pool = mysql_async::Pool::new(opts);
        let mut connection = pool.get_conn().await.expect("connect to MySQL");
        let report = MySqlMigrationExecutor::new(
            &mut connection,
            &MySqlConfig::new(database),
            env!("CARGO_PKG_VERSION"),
        )
        .migrate()
        .await
        .expect("apply MySQL migrations");
        assert_eq!(report.target_model_version, 15);
        drop(connection);
        pool.disconnect().await.expect("disconnect MySQL pool");
    }
}
