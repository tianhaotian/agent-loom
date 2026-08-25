#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddedMigration {
    pub logical_id: &'static str,
    pub logical_model_version: u64,
    pub created_tables: &'static [&'static str],
    pub sql: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationManifestError {
    Empty,
    InvalidLogicalId { index: usize },
    NonIncreasingLogicalId { index: usize },
    DecreasingModelVersion { index: usize },
    EmptySql { index: usize },
    EmptyTableName { migration_index: usize },
    DuplicateTable { migration_index: usize },
}

/// Validates ordering and logical-table ownership for an embedded migration manifest.
///
/// # Errors
///
/// Returns [`MigrationManifestError`] when IDs, model versions, SQL bodies, or
/// created-table declarations violate the portable manifest contract.
pub fn validate_migration_manifest(
    migrations: &[EmbeddedMigration],
) -> Result<(), MigrationManifestError> {
    if migrations.is_empty() {
        return Err(MigrationManifestError::Empty);
    }

    for (index, migration) in migrations.iter().enumerate() {
        if !valid_logical_id(migration.logical_id) {
            return Err(MigrationManifestError::InvalidLogicalId { index });
        }
        if migration.sql.trim().is_empty() {
            return Err(MigrationManifestError::EmptySql { index });
        }

        if let Some(previous) = index.checked_sub(1).map(|value| &migrations[value]) {
            if previous.logical_id >= migration.logical_id {
                return Err(MigrationManifestError::NonIncreasingLogicalId { index });
            }
            if previous.logical_model_version > migration.logical_model_version {
                return Err(MigrationManifestError::DecreasingModelVersion { index });
            }
        }

        for table in migration.created_tables {
            if table.is_empty() {
                return Err(MigrationManifestError::EmptyTableName {
                    migration_index: index,
                });
            }
            if migrations[..=index]
                .iter()
                .flat_map(|item| item.created_tables.iter())
                .filter(|candidate| *candidate == table)
                .count()
                > 1
            {
                return Err(MigrationManifestError::DuplicateTable {
                    migration_index: index,
                });
            }
        }
    }

    Ok(())
}

fn valid_logical_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() > 5
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'_'
        && bytes[5..]
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MigrationState {
    Applying,
    Applied,
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;

    const SQL: &str = "CREATE TABLE example (id bigint PRIMARY KEY);";

    #[test]
    fn manifest_validation_accepts_ordered_portable_metadata() {
        let migrations = [
            EmbeddedMigration {
                logical_id: "0000_meta",
                logical_model_version: 1,
                created_tables: &["schema_migrations"],
                sql: SQL,
            },
            EmbeddedMigration {
                logical_id: "0001_core",
                logical_model_version: 2,
                created_tables: &["runs"],
                sql: SQL,
            },
        ];

        assert_eq!(validate_migration_manifest(&migrations), Ok(()));
    }

    #[test]
    fn manifest_validation_rejects_duplicate_tables() {
        let migrations = [
            EmbeddedMigration {
                logical_id: "0000_meta",
                logical_model_version: 1,
                created_tables: &["runs"],
                sql: SQL,
            },
            EmbeddedMigration {
                logical_id: "0001_core",
                logical_model_version: 2,
                created_tables: &["runs"],
                sql: SQL,
            },
        ];

        assert_eq!(
            validate_migration_manifest(&migrations),
            Err(MigrationManifestError::DuplicateTable { migration_index: 1 })
        );
    }
}
