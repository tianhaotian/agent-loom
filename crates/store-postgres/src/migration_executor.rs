use std::{error::Error, fmt};

use agent_loom_domain::Digest;
use agent_loom_durable_store::{
    MigrationAssessmentError, MigrationCandidate, MigrationExecutionAction,
    MigrationExecutionFailure, MigrationExecutionMachine, MigrationJournalEntry, MigrationState,
    assess_migrations,
};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tokio_postgres::{Client, GenericClient};

use crate::{DEFAULT_SCHEMA, MIGRATIONS, PROVIDER_KIND, PostgresConfig};

const MIGRATION_LOCK_KEY: i64 = 0x4147_4c4f_4f4d_0001;
const BOOTSTRAP_LOGICAL_ID: &str = "0000_migration_meta";

#[derive(Debug)]
pub enum PostgresMigrationError {
    UnsupportedSchema {
        configured: String,
    },
    Database {
        operation: &'static str,
        source: tokio_postgres::Error,
    },
    Assessment(MigrationAssessmentError),
    Execution(MigrationExecutionFailure),
    BootstrapHistoryMissing,
    BootstrapCandidateMissing,
    InvalidJournalValue {
        field: &'static str,
    },
    SchemaVerification {
        logical_id: &'static str,
        table: &'static str,
    },
    LockNotHeld,
}

impl fmt::Display for PostgresMigrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchema { configured } => write!(
                formatter,
                "embedded PostgreSQL migrations target schema {DEFAULT_SCHEMA:?}, not {configured:?}"
            ),
            Self::Database { operation, .. } => {
                write!(
                    formatter,
                    "PostgreSQL migration operation failed: {operation}"
                )
            }
            Self::Assessment(error) => write!(formatter, "migration history is unsafe: {error:?}"),
            Self::Execution(error) => write!(
                formatter,
                "migration execution failed at {:?} for {:?}: {}",
                error.failed_step, error.logical_id, error.error_code
            ),
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
            Self::LockNotHeld => {
                formatter.write_str("PostgreSQL migration advisory lock was not held")
            }
        }
    }
}

impl Error for PostgresMigrationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database { source, .. } => Some(source),
            Self::UnsupportedSchema { .. }
            | Self::Assessment(_)
            | Self::Execution(_)
            | Self::BootstrapHistoryMissing
            | Self::BootstrapCandidateMissing
            | Self::InvalidJournalValue { .. }
            | Self::SchemaVerification { .. }
            | Self::LockNotHeld => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationReport {
    pub previous_model_version: u64,
    pub target_model_version: u64,
    pub applied_logical_ids: Vec<&'static str>,
}

impl MigrationReport {
    pub fn changed(&self) -> bool {
        !self.applied_logical_ids.is_empty()
    }
}

pub fn migration_candidates() -> Vec<MigrationCandidate> {
    MIGRATIONS
        .iter()
        .map(|descriptor| {
            let checksum: [u8; 32] = Sha256::digest(descriptor.sql.as_bytes()).into();
            MigrationCandidate {
                descriptor: *descriptor,
                physical_checksum: Digest::from_bytes(checksum),
            }
        })
        .collect()
}

#[derive(Debug)]
pub struct PostgresMigrationExecutor<'a> {
    client: &'a mut Client,
    config: &'a PostgresConfig,
    runner_version: &'a str,
}

impl<'a> PostgresMigrationExecutor<'a> {
    pub fn new(
        client: &'a mut Client,
        config: &'a PostgresConfig,
        runner_version: &'a str,
    ) -> Self {
        Self {
            client,
            config,
            runner_version,
        }
    }

    /// Applies the contiguous pending migration suffix under a session advisory lock.
    ///
    /// The journal table necessarily bootstraps itself before normal `applying → applied`
    /// journaling can begin. Every later migration follows the shared execution machine.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported configuration, divergent history, database
    /// failures, lock failures, or failed post-DDL schema introspection.
    pub async fn migrate(&mut self) -> Result<MigrationReport, PostgresMigrationError> {
        self.validate_config()?;
        self.configure_session().await?;
        self.acquire_lock().await?;

        let result = self.migrate_locked().await;
        let release_result = self.release_lock().await;
        match (result, release_result) {
            (Ok(report), Ok(())) => Ok(report),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(primary), Err(_release)) => Err(primary),
        }
    }

    fn validate_config(&self) -> Result<(), PostgresMigrationError> {
        if self.config.schema != DEFAULT_SCHEMA {
            return Err(PostgresMigrationError::UnsupportedSchema {
                configured: self.config.schema.clone(),
            });
        }
        Ok(())
    }

    async fn configure_session(&self) -> Result<(), PostgresMigrationError> {
        let statement_timeout = format!("{}ms", self.config.statement_timeout_ms);
        let lock_timeout = format!("{}ms", self.config.lock_timeout_ms);
        self.client
            .query_one(
                "SELECT set_config('statement_timeout', $1, false), \
                        set_config('lock_timeout', $2, false)",
                &[&statement_timeout, &lock_timeout],
            )
            .await
            .map_err(|source| PostgresMigrationError::Database {
                operation: "configure_session",
                source,
            })?;
        Ok(())
    }

    async fn acquire_lock(&self) -> Result<(), PostgresMigrationError> {
        self.client
            .query_one("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK_KEY])
            .await
            .map_err(|source| PostgresMigrationError::Database {
                operation: "acquire_advisory_lock",
                source,
            })?;
        Ok(())
    }

    async fn release_lock(&self) -> Result<(), PostgresMigrationError> {
        let row = self
            .client
            .query_one("SELECT pg_advisory_unlock($1)", &[&MIGRATION_LOCK_KEY])
            .await
            .map_err(|source| PostgresMigrationError::Database {
                operation: "release_advisory_lock",
                source,
            })?;
        if row.get::<_, bool>(0) {
            Ok(())
        } else {
            Err(PostgresMigrationError::LockNotHeld)
        }
    }

    async fn migrate_locked(&mut self) -> Result<MigrationReport, PostgresMigrationError> {
        let candidates = migration_candidates();
        let bootstrapped = self.bootstrap_journal(&candidates).await?;
        let history = self.load_history().await?;
        let plan = assess_migrations(PROVIDER_KIND, &candidates, &history)
            .map_err(PostgresMigrationError::Assessment)?;
        let previous_model_version = if bootstrapped {
            0
        } else {
            plan.current_model_version
        };
        let target_model_version = plan.target_model_version;
        let mut applied_logical_ids = Vec::new();
        if bootstrapped {
            applied_logical_ids.push(BOOTSTRAP_LOGICAL_ID);
        }

        if plan.pending.is_empty() {
            return Ok(MigrationReport {
                previous_model_version,
                target_model_version,
                applied_logical_ids,
            });
        }

        let pending_ids: Vec<_> = plan
            .pending
            .iter()
            .map(|candidate| candidate.descriptor.logical_id)
            .collect();
        let mut machine = MigrationExecutionMachine::new(plan);
        while let Some(action) = machine.next_action() {
            let action_result = match action {
                // The actual session-lock release happens in `migrate`, including
                // pre-machine failures. This action preserves the shared ordering.
                MigrationExecutionAction::AcquireLock | MigrationExecutionAction::ReleaseLock => {
                    Ok(())
                }
                MigrationExecutionAction::StartJournal(candidate) => {
                    self.start_journal(candidate).await
                }
                MigrationExecutionAction::ExecuteSql(candidate) => {
                    self.execute_sql(candidate).await
                }
                MigrationExecutionAction::VerifySchema(candidate) => {
                    self.verify_schema(candidate).await
                }
                MigrationExecutionAction::MarkApplied(candidate) => {
                    self.mark_applied(candidate).await
                }
                MigrationExecutionAction::MarkFailed(candidate) => {
                    let failure_code = machine
                        .failure()
                        .map_or("MIGRATION_FAILED", |failure| failure.error_code.as_str());
                    self.mark_failed(candidate, failure_code).await
                }
            };

            match action_result {
                Ok(()) => machine
                    .report_success()
                    .expect("non-terminal migration action accepts success"),
                Err(error) => machine
                    .report_failure(stable_error_code(&error))
                    .expect("non-terminal migration action accepts failure"),
            }
        }

        if let Some(failure) = machine.failure().cloned() {
            return Err(PostgresMigrationError::Execution(failure));
        }
        applied_logical_ids.extend(pending_ids);
        Ok(MigrationReport {
            previous_model_version,
            target_model_version,
            applied_logical_ids,
        })
    }

    async fn bootstrap_journal(
        &mut self,
        candidates: &[MigrationCandidate],
    ) -> Result<bool, PostgresMigrationError> {
        let exists = self
            .client
            .query_one(
                "SELECT to_regclass('agent_loom.schema_migrations') IS NOT NULL",
                &[],
            )
            .await
            .map_err(|source| PostgresMigrationError::Database {
                operation: "probe_migration_journal",
                source,
            })?
            .get::<_, bool>(0);
        if exists {
            let has_bootstrap = self
                .client
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM agent_loom.schema_migrations \
                     WHERE logical_id = $1)",
                    &[&BOOTSTRAP_LOGICAL_ID],
                )
                .await
                .map_err(|source| PostgresMigrationError::Database {
                    operation: "probe_bootstrap_history",
                    source,
                })?
                .get::<_, bool>(0);
            return if has_bootstrap {
                Ok(false)
            } else {
                Err(PostgresMigrationError::BootstrapHistoryMissing)
            };
        }

        let bootstrap = candidates
            .iter()
            .find(|candidate| candidate.descriptor.logical_id == BOOTSTRAP_LOGICAL_ID)
            .copied()
            .ok_or(PostgresMigrationError::BootstrapCandidateMissing)?;
        let transaction =
            self.client
                .transaction()
                .await
                .map_err(|source| PostgresMigrationError::Database {
                    operation: "begin_bootstrap",
                    source,
                })?;
        transaction
            .batch_execute(bootstrap.descriptor.sql)
            .await
            .map_err(|source| PostgresMigrationError::Database {
                operation: "execute_bootstrap",
                source,
            })?;
        verify_tables(&transaction, bootstrap).await?;
        let checksum = bootstrap.physical_checksum.as_bytes().as_slice();
        let model_version = i64::try_from(bootstrap.descriptor.logical_model_version)
            .expect("logical model version fits i64");
        let details = json!({"bootstrap": true});
        transaction
            .execute(
                "INSERT INTO agent_loom.schema_migrations (\
                    logical_id, provider_kind, physical_checksum, logical_model_version, \
                    state, started_at, applied_at, runner_version, details_json\
                 ) VALUES ($1, $2, $3, $4, 'applied', clock_timestamp(), \
                    clock_timestamp(), $5, $6)",
                &[
                    &bootstrap.descriptor.logical_id,
                    &PROVIDER_KIND,
                    &checksum,
                    &model_version,
                    &self.runner_version,
                    &details,
                ],
            )
            .await
            .map_err(|source| PostgresMigrationError::Database {
                operation: "journal_bootstrap",
                source,
            })?;
        transaction
            .commit()
            .await
            .map_err(|source| PostgresMigrationError::Database {
                operation: "commit_bootstrap",
                source,
            })?;
        Ok(true)
    }

    async fn load_history(&self) -> Result<Vec<MigrationJournalEntry>, PostgresMigrationError> {
        let rows = self
            .client
            .query(
                "SELECT logical_id, provider_kind, physical_checksum, \
                        logical_model_version, state \
                 FROM agent_loom.schema_migrations ORDER BY logical_id",
                &[],
            )
            .await
            .map_err(|source| PostgresMigrationError::Database {
                operation: "load_migration_history",
                source,
            })?;
        rows.into_iter()
            .map(|row| {
                let checksum: Vec<u8> = row.get(2);
                let checksum: [u8; 32] = checksum.try_into().map_err(|_| {
                    PostgresMigrationError::InvalidJournalValue {
                        field: "physical_checksum",
                    }
                })?;
                let state = match row.get::<_, &str>(4) {
                    "applying" => MigrationState::Applying,
                    "applied" => MigrationState::Applied,
                    "failed" => MigrationState::Failed,
                    _ => {
                        return Err(PostgresMigrationError::InvalidJournalValue { field: "state" });
                    }
                };
                let model_version = row.get::<_, i64>(3);
                let logical_model_version = u64::try_from(model_version).map_err(|_| {
                    PostgresMigrationError::InvalidJournalValue {
                        field: "logical_model_version",
                    }
                })?;
                Ok(MigrationJournalEntry {
                    logical_id: row.get(0),
                    provider_kind: row.get(1),
                    physical_checksum: Digest::from_bytes(checksum),
                    logical_model_version,
                    state,
                })
            })
            .collect()
    }

    async fn start_journal(
        &self,
        candidate: MigrationCandidate,
    ) -> Result<(), PostgresMigrationError> {
        let checksum = candidate.physical_checksum.as_bytes().as_slice();
        let model_version = i64::try_from(candidate.descriptor.logical_model_version)
            .expect("logical model version fits i64");
        let details = json!({});
        self.client
            .execute(
                "INSERT INTO agent_loom.schema_migrations (\
                    logical_id, provider_kind, physical_checksum, logical_model_version, \
                    state, started_at, applied_at, runner_version, details_json\
                 ) VALUES ($1, $2, $3, $4, 'applying', clock_timestamp(), NULL, $5, $6)",
                &[
                    &candidate.descriptor.logical_id,
                    &PROVIDER_KIND,
                    &checksum,
                    &model_version,
                    &self.runner_version,
                    &details,
                ],
            )
            .await
            .map_err(|source| PostgresMigrationError::Database {
                operation: "start_migration_journal",
                source,
            })?;
        Ok(())
    }

    async fn execute_sql(
        &mut self,
        candidate: MigrationCandidate,
    ) -> Result<(), PostgresMigrationError> {
        let transaction =
            self.client
                .transaction()
                .await
                .map_err(|source| PostgresMigrationError::Database {
                    operation: "begin_migration_ddl",
                    source,
                })?;
        transaction
            .batch_execute(candidate.descriptor.sql)
            .await
            .map_err(|source| PostgresMigrationError::Database {
                operation: "execute_migration_ddl",
                source,
            })?;
        transaction
            .commit()
            .await
            .map_err(|source| PostgresMigrationError::Database {
                operation: "commit_migration_ddl",
                source,
            })?;
        Ok(())
    }

    async fn verify_schema(
        &self,
        candidate: MigrationCandidate,
    ) -> Result<(), PostgresMigrationError> {
        verify_tables(self.client, candidate).await
    }

    async fn mark_applied(
        &self,
        candidate: MigrationCandidate,
    ) -> Result<(), PostgresMigrationError> {
        let updated = self
            .client
            .execute(
                "UPDATE agent_loom.schema_migrations \
                 SET state = 'applied', applied_at = clock_timestamp(), details_json = $2 \
                 WHERE logical_id = $1 AND state = 'applying'",
                &[&candidate.descriptor.logical_id, &json!({})],
            )
            .await
            .map_err(|source| PostgresMigrationError::Database {
                operation: "mark_migration_applied",
                source,
            })?;
        if updated != 1 {
            return Err(PostgresMigrationError::InvalidJournalValue {
                field: "applying row count",
            });
        }
        Ok(())
    }

    async fn mark_failed(
        &self,
        candidate: MigrationCandidate,
        error_code: &str,
    ) -> Result<(), PostgresMigrationError> {
        let details = json!({"error_code": error_code});
        let updated = self
            .client
            .execute(
                "UPDATE agent_loom.schema_migrations \
                 SET state = 'failed', applied_at = NULL, details_json = $2 \
                 WHERE logical_id = $1 AND state = 'applying'",
                &[&candidate.descriptor.logical_id, &details],
            )
            .await
            .map_err(|source| PostgresMigrationError::Database {
                operation: "mark_migration_failed",
                source,
            })?;
        if updated != 1 {
            return Err(PostgresMigrationError::InvalidJournalValue {
                field: "failed row count",
            });
        }
        Ok(())
    }
}

async fn verify_tables(
    client: &impl GenericClient,
    candidate: MigrationCandidate,
) -> Result<(), PostgresMigrationError> {
    for table in candidate.descriptor.created_tables {
        let qualified = format!("{DEFAULT_SCHEMA}.{table}");
        let exists = client
            .query_one("SELECT to_regclass($1) IS NOT NULL", &[&qualified])
            .await
            .map_err(|source| PostgresMigrationError::Database {
                operation: "verify_migration_schema",
                source,
            })?
            .get::<_, bool>(0);
        if !exists {
            return Err(PostgresMigrationError::SchemaVerification {
                logical_id: candidate.descriptor.logical_id,
                table,
            });
        }
    }
    Ok(())
}

fn stable_error_code(error: &PostgresMigrationError) -> String {
    match error {
        PostgresMigrationError::Database { source, .. } => source.as_db_error().map_or_else(
            || "PG_CONNECTION".to_owned(),
            |db| format!("PG_{}", db.code().code()),
        ),
        PostgresMigrationError::SchemaVerification { .. } => "SCHEMA_VERIFICATION".to_owned(),
        PostgresMigrationError::LockNotHeld => "MIGRATION_LOCK_NOT_HELD".to_owned(),
        PostgresMigrationError::UnsupportedSchema { .. }
        | PostgresMigrationError::Assessment(_)
        | PostgresMigrationError::Execution(_)
        | PostgresMigrationError::BootstrapHistoryMissing
        | PostgresMigrationError::BootstrapCandidateMissing
        | PostgresMigrationError::InvalidJournalValue { .. } => "MIGRATION_INVALID".to_owned(),
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
                .all(|candidate| { candidate.physical_checksum != Digest::from_bytes([0; 32]) })
        );
        assert_ne!(
            candidates[0].physical_checksum,
            candidates[1].physical_checksum
        );
    }

    #[tokio::test]
    async fn migration_smoke_test_when_postgres_url_is_configured() {
        let Ok(url) = std::env::var("AGENT_LOOM_TEST_POSTGRES_URL") else {
            return;
        };
        let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("connect to smoke-test PostgreSQL");
        let connection_task = tokio::spawn(connection);
        let report = PostgresMigrationExecutor::new(
            &mut client,
            &PostgresConfig::default(),
            env!("CARGO_PKG_VERSION"),
        )
        .migrate()
        .await
        .expect("apply PostgreSQL migrations");
        assert_eq!(report.target_model_version, 7);

        drop(client);
        connection_task
            .await
            .expect("connection task joins")
            .expect("PostgreSQL connection stays healthy");
    }
}
