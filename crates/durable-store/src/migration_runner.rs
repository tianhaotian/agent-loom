use agent_loom_domain::Digest;

use crate::{
    EmbeddedMigration, MigrationManifestError, MigrationState, validate_migration_manifest,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MigrationCandidate {
    pub descriptor: EmbeddedMigration,
    pub physical_checksum: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationJournalEntry {
    pub logical_id: String,
    pub provider_kind: String,
    pub physical_checksum: Digest,
    pub logical_model_version: u64,
    pub state: MigrationState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationPlan {
    pub current_model_version: u64,
    pub target_model_version: u64,
    pub pending: Vec<MigrationCandidate>,
}

impl MigrationPlan {
    pub fn is_ready(&self) -> bool {
        self.pending.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationAssessmentError {
    Manifest(MigrationManifestError),
    EmptyProviderKind,
    DuplicateHistory {
        logical_id: String,
    },
    UnknownMigration {
        logical_id: String,
    },
    ProviderMismatch {
        logical_id: String,
    },
    DirtyMigration {
        logical_id: String,
        state: MigrationState,
    },
    ChecksumMismatch {
        logical_id: String,
    },
    ModelVersionMismatch {
        logical_id: String,
    },
    HistoryGap {
        missing_logical_id: &'static str,
        applied_logical_id: &'static str,
    },
}

/// Assesses migration history and returns only the contiguous pending suffix.
///
/// The caller must calculate physical checksums from the exact provider SQL
/// bytes before invoking this function. A returned plan is safe to execute in
/// order, but execution still requires an exclusive migration lock and schema
/// introspection after every migration.
///
/// # Errors
///
/// Returns [`MigrationAssessmentError`] when the manifest or journal is dirty,
/// divergent, belongs to another provider, or contains an applied-history gap.
pub fn assess_migrations(
    provider_kind: &str,
    available: &[MigrationCandidate],
    history: &[MigrationJournalEntry],
) -> Result<MigrationPlan, MigrationAssessmentError> {
    if provider_kind.is_empty() {
        return Err(MigrationAssessmentError::EmptyProviderKind);
    }

    let descriptors: Vec<_> = available
        .iter()
        .map(|candidate| candidate.descriptor)
        .collect();
    validate_migration_manifest(&descriptors).map_err(MigrationAssessmentError::Manifest)?;

    for (index, entry) in history.iter().enumerate() {
        if history[..index]
            .iter()
            .any(|previous| previous.logical_id == entry.logical_id)
        {
            return Err(MigrationAssessmentError::DuplicateHistory {
                logical_id: entry.logical_id.clone(),
            });
        }

        let Some(candidate) = available
            .iter()
            .find(|candidate| candidate.descriptor.logical_id == entry.logical_id)
        else {
            return Err(MigrationAssessmentError::UnknownMigration {
                logical_id: entry.logical_id.clone(),
            });
        };

        if entry.provider_kind != provider_kind {
            return Err(MigrationAssessmentError::ProviderMismatch {
                logical_id: entry.logical_id.clone(),
            });
        }
        if entry.state != MigrationState::Applied {
            return Err(MigrationAssessmentError::DirtyMigration {
                logical_id: entry.logical_id.clone(),
                state: entry.state,
            });
        }
        if entry.physical_checksum != candidate.physical_checksum {
            return Err(MigrationAssessmentError::ChecksumMismatch {
                logical_id: entry.logical_id.clone(),
            });
        }
        if entry.logical_model_version != candidate.descriptor.logical_model_version {
            return Err(MigrationAssessmentError::ModelVersionMismatch {
                logical_id: entry.logical_id.clone(),
            });
        }
    }

    let mut first_missing = None;
    let mut current_model_version = 0;
    for (index, candidate) in available.iter().enumerate() {
        let applied = history
            .iter()
            .any(|entry| entry.logical_id == candidate.descriptor.logical_id);
        match (first_missing, applied) {
            (None, true) => {
                current_model_version = candidate.descriptor.logical_model_version;
            }
            (None, false) => first_missing = Some(index),
            (Some(missing_index), true) => {
                return Err(MigrationAssessmentError::HistoryGap {
                    missing_logical_id: available[missing_index].descriptor.logical_id,
                    applied_logical_id: candidate.descriptor.logical_id,
                });
            }
            (Some(_), false) => {}
        }
    }

    let pending = first_missing.map_or_else(Vec::new, |index| available[index..].to_vec());
    let target_model_version = available
        .last()
        .map_or(0, |candidate| candidate.descriptor.logical_model_version);

    Ok(MigrationPlan {
        current_model_version,
        target_model_version,
        pending,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SQL: &str = "CREATE TABLE example (id bigint PRIMARY KEY);";
    const META: EmbeddedMigration = EmbeddedMigration {
        logical_id: "0000_meta",
        logical_model_version: 1,
        created_tables: &["schema_migrations"],
        sql: SQL,
    };
    const CORE: EmbeddedMigration = EmbeddedMigration {
        logical_id: "0001_core",
        logical_model_version: 2,
        created_tables: &["runs"],
        sql: SQL,
    };
    const CANDIDATES: &[MigrationCandidate] = &[
        MigrationCandidate {
            descriptor: META,
            physical_checksum: Digest::from_bytes([1; 32]),
        },
        MigrationCandidate {
            descriptor: CORE,
            physical_checksum: Digest::from_bytes([2; 32]),
        },
    ];

    #[test]
    fn assessment_returns_contiguous_pending_suffix() {
        let history = [journal(META, [1; 32], MigrationState::Applied)];
        let plan = assess_migrations("postgres", CANDIDATES, &history).expect("valid plan");

        assert_eq!(plan.current_model_version, 1);
        assert_eq!(plan.target_model_version, 2);
        assert_eq!(plan.pending, vec![CANDIDATES[1]]);
        assert!(!plan.is_ready());
    }

    #[test]
    fn assessment_rejects_dirty_and_checksum_mismatch() {
        let dirty = [journal(META, [1; 32], MigrationState::Applying)];
        assert_eq!(
            assess_migrations("postgres", CANDIDATES, &dirty),
            Err(MigrationAssessmentError::DirtyMigration {
                logical_id: "0000_meta".to_owned(),
                state: MigrationState::Applying,
            })
        );

        let changed = [journal(META, [9; 32], MigrationState::Applied)];
        assert_eq!(
            assess_migrations("postgres", CANDIDATES, &changed),
            Err(MigrationAssessmentError::ChecksumMismatch {
                logical_id: "0000_meta".to_owned(),
            })
        );
    }

    #[test]
    fn assessment_rejects_applied_history_gap() {
        let history = [journal(CORE, [2; 32], MigrationState::Applied)];
        assert_eq!(
            assess_migrations("postgres", CANDIDATES, &history),
            Err(MigrationAssessmentError::HistoryGap {
                missing_logical_id: "0000_meta",
                applied_logical_id: "0001_core",
            })
        );
    }

    #[test]
    fn assessment_rejects_unknown_and_cross_provider_history() {
        let unknown = [MigrationJournalEntry {
            logical_id: "9999_future".to_owned(),
            provider_kind: "postgres".to_owned(),
            physical_checksum: Digest::from_bytes([9; 32]),
            logical_model_version: 9999,
            state: MigrationState::Applied,
        }];
        assert_eq!(
            assess_migrations("postgres", CANDIDATES, &unknown),
            Err(MigrationAssessmentError::UnknownMigration {
                logical_id: "9999_future".to_owned(),
            })
        );

        let mut wrong_provider = journal(META, [1; 32], MigrationState::Applied);
        wrong_provider.provider_kind = "mysql".to_owned();
        assert_eq!(
            assess_migrations("postgres", CANDIDATES, &[wrong_provider]),
            Err(MigrationAssessmentError::ProviderMismatch {
                logical_id: "0000_meta".to_owned(),
            })
        );
    }

    fn journal(
        descriptor: EmbeddedMigration,
        checksum: [u8; 32],
        state: MigrationState,
    ) -> MigrationJournalEntry {
        MigrationJournalEntry {
            logical_id: descriptor.logical_id.to_owned(),
            provider_kind: "postgres".to_owned(),
            physical_checksum: Digest::from_bytes(checksum),
            logical_model_version: descriptor.logical_model_version,
            state,
        }
    }
}
