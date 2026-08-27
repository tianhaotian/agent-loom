use std::{error::Error, fmt};

use mysql_async::{Conn, Opts, Pool, prelude::Queryable};

use crate::{
    CONNECTION_TIME_ZONE, MySqlConfig, MySqlMigrationError, MySqlMigrationExecutor,
    MySqlMigrationReport, TRANSACTION_ISOLATION,
};

/// Connection-pooled entry point for the MySQL provider.
///
/// Phase 2B transaction executors obtain connections through this type so every
/// command observes the same UTC, isolation-level and lock-timeout policy.
#[derive(Clone, Debug)]
pub struct MySqlStore {
    pool: Pool,
    config: MySqlConfig,
}

#[derive(Debug)]
pub enum MySqlStoreError {
    InvalidUrl,
    DatabaseMissing,
    Database(mysql_async::Error),
    Migration(MySqlMigrationError),
    UnexpectedSessionPolicy,
}

impl fmt::Display for MySqlStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl => formatter.write_str("MySQL provider URL is invalid"),
            Self::DatabaseMissing => {
                formatter.write_str("MySQL provider URL must select a database")
            }
            Self::Database(_) => formatter.write_str("MySQL provider connection failed"),
            Self::Migration(error) => write!(formatter, "MySQL migration failed: {error}"),
            Self::UnexpectedSessionPolicy => {
                formatter.write_str("MySQL connection did not retain the required session policy")
            }
        }
    }
}

impl Error for MySqlStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::Migration(error) => Some(error),
            Self::InvalidUrl | Self::DatabaseMissing | Self::UnexpectedSessionPolicy => None,
        }
    }
}

impl From<MySqlMigrationError> for MySqlStoreError {
    fn from(value: MySqlMigrationError) -> Self {
        Self::Migration(value)
    }
}

impl MySqlStore {
    /// Builds a lazy MySQL pool from a URL that explicitly names the database.
    ///
    /// # Errors
    ///
    /// Returns a safe configuration error when the URL is invalid or does not
    /// select a database.
    pub fn from_url(url: &str) -> Result<Self, MySqlStoreError> {
        let opts = Opts::from_url(url).map_err(|_| MySqlStoreError::InvalidUrl)?;
        let database = opts
            .db_name()
            .filter(|name| !name.is_empty())
            .ok_or(MySqlStoreError::DatabaseMissing)?
            .to_owned();
        Ok(Self::new(Pool::new(opts), MySqlConfig::new(database)))
    }

    pub const fn new(pool: Pool, config: MySqlConfig) -> Self {
        Self { pool, config }
    }

    pub const fn pool(&self) -> &Pool {
        &self.pool
    }

    pub const fn config(&self) -> &MySqlConfig {
        &self.config
    }

    /// Checks out and configures one command-scoped connection.
    ///
    /// # Errors
    ///
    /// Returns a safe provider error when the pool is unavailable or MySQL
    /// rejects the required UTC/read-committed session policy.
    pub async fn connection(&self) -> Result<Conn, MySqlStoreError> {
        let mut connection = self
            .pool
            .get_conn()
            .await
            .map_err(MySqlStoreError::Database)?;
        connection
            .query_drop(format!(
                "SET SESSION time_zone = '{CONNECTION_TIME_ZONE}', \
                 SESSION transaction_isolation = '{}', \
                 SESSION innodb_lock_wait_timeout = {}",
                TRANSACTION_ISOLATION.replace(' ', "-"),
                self.config.lock_wait_timeout_seconds
            ))
            .await
            .map_err(MySqlStoreError::Database)?;
        Ok(connection)
    }

    /// Applies all embedded migrations under the provider's database-scoped
    /// named lock. Calling this repeatedly is safe.
    ///
    /// # Errors
    ///
    /// Returns migration validation, locking, journaling or DDL failures.
    pub async fn migrate(
        &self,
        runner_version: &str,
    ) -> Result<MySqlMigrationReport, MySqlStoreError> {
        let mut connection = self.connection().await?;
        MySqlMigrationExecutor::new(&mut connection, &self.config, runner_version)
            .migrate()
            .await
            .map_err(Into::into)
    }

    /// Verifies connectivity and the authoritative session policy without
    /// changing application data.
    ///
    /// # Errors
    ///
    /// Returns a provider error when connectivity or session policy is invalid.
    pub async fn health_check(&self) -> Result<(), MySqlStoreError> {
        let mut connection = self.connection().await?;
        let row: Option<(u8, String, String)> = connection
            .query_first("SELECT 1, @@SESSION.time_zone, @@SESSION.transaction_isolation")
            .await
            .map_err(MySqlStoreError::Database)?;
        let Some((one, time_zone, isolation)) = row else {
            return Err(MySqlStoreError::UnexpectedSessionPolicy);
        };
        if one != 1
            || time_zone != CONNECTION_TIME_ZONE
            || isolation.replace('-', " ") != TRANSACTION_ISOLATION
        {
            return Err(MySqlStoreError::UnexpectedSessionPolicy);
        }
        Ok(())
    }

    /// Drains idle connections and closes the pool.
    ///
    /// # Errors
    ///
    /// Returns a provider error when MySQL rejects pool shutdown.
    pub async fn disconnect(self) -> Result<(), MySqlStoreError> {
        self.pool
            .disconnect()
            .await
            .map_err(MySqlStoreError::Database)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_must_select_database() {
        assert!(matches!(
            MySqlStore::from_url("mysql://root@127.0.0.1"),
            Err(MySqlStoreError::DatabaseMissing)
        ));
        assert!(matches!(
            MySqlStore::from_url("not a mysql url"),
            Err(MySqlStoreError::InvalidUrl)
        ));
    }

    #[tokio::test]
    async fn live_pool_policy_and_repeatable_migration_when_configured() {
        let Ok(url) = std::env::var("AGENT_LOOM_TEST_MYSQL_URL") else {
            return;
        };
        let store = MySqlStore::from_url(&url).expect("build MySQL Store");
        store.health_check().await.expect("validate session policy");
        let first = store
            .migrate(env!("CARGO_PKG_VERSION"))
            .await
            .expect("apply migrations");
        let second = store
            .migrate(env!("CARGO_PKG_VERSION"))
            .await
            .expect("replay migrations");
        assert_eq!(first.target_model_version, 12);
        assert_eq!(second.target_model_version, 12);
        assert!(!second.changed());

        let concurrent_a = store.clone();
        let concurrent_b = store.clone();
        let (left, right) = tokio::join!(
            concurrent_a.migrate(env!("CARGO_PKG_VERSION")),
            concurrent_b.migrate(env!("CARGO_PKG_VERSION")),
        );
        assert!(!left.expect("first concurrent migration").changed());
        assert!(!right.expect("second concurrent migration").changed());
        store.disconnect().await.expect("disconnect MySQL pool");
    }
}
