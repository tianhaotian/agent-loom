use crate::{MigrationCandidate, MigrationPlan};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationExecutionStep {
    AcquireLock,
    StartJournal,
    ExecuteSql,
    VerifySchema,
    MarkApplied,
    MarkFailed,
    ReleaseLock,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationExecutionAction {
    AcquireLock,
    StartJournal(MigrationCandidate),
    ExecuteSql(MigrationCandidate),
    VerifySchema(MigrationCandidate),
    MarkApplied(MigrationCandidate),
    MarkFailed(MigrationCandidate),
    ReleaseLock,
}

impl MigrationExecutionAction {
    pub const fn step(self) -> MigrationExecutionStep {
        match self {
            Self::AcquireLock => MigrationExecutionStep::AcquireLock,
            Self::StartJournal(_) => MigrationExecutionStep::StartJournal,
            Self::ExecuteSql(_) => MigrationExecutionStep::ExecuteSql,
            Self::VerifySchema(_) => MigrationExecutionStep::VerifySchema,
            Self::MarkApplied(_) => MigrationExecutionStep::MarkApplied,
            Self::MarkFailed(_) => MigrationExecutionStep::MarkFailed,
            Self::ReleaseLock => MigrationExecutionStep::ReleaseLock,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationExecutionFailure {
    pub logical_id: Option<&'static str>,
    pub failed_step: MigrationExecutionStep,
    pub error_code: String,
    pub mark_failed_error: Option<String>,
    pub release_lock_error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationMachineError {
    AlreadyFinished,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    AcquireLock,
    StartJournal,
    ExecuteSql,
    VerifySchema,
    MarkApplied,
    MarkFailed,
    ReleaseLock,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationExecutionMachine {
    pending: Vec<MigrationCandidate>,
    current_index: usize,
    phase: Phase,
    failure: Option<MigrationExecutionFailure>,
}

impl MigrationExecutionMachine {
    pub fn new(plan: MigrationPlan) -> Self {
        let phase = if plan.pending.is_empty() {
            Phase::Succeeded
        } else {
            Phase::AcquireLock
        };
        Self {
            pending: plan.pending,
            current_index: 0,
            phase,
            failure: None,
        }
    }

    pub fn next_action(&self) -> Option<MigrationExecutionAction> {
        let candidate = || self.pending[self.current_index];
        match self.phase {
            Phase::AcquireLock => Some(MigrationExecutionAction::AcquireLock),
            Phase::StartJournal => Some(MigrationExecutionAction::StartJournal(candidate())),
            Phase::ExecuteSql => Some(MigrationExecutionAction::ExecuteSql(candidate())),
            Phase::VerifySchema => Some(MigrationExecutionAction::VerifySchema(candidate())),
            Phase::MarkApplied => Some(MigrationExecutionAction::MarkApplied(candidate())),
            Phase::MarkFailed => Some(MigrationExecutionAction::MarkFailed(candidate())),
            Phase::ReleaseLock => Some(MigrationExecutionAction::ReleaseLock),
            Phase::Succeeded | Phase::Failed => None,
        }
    }

    /// Reports that the current action completed successfully.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationMachineError::AlreadyFinished`] after the machine has
    /// reached a terminal state.
    pub fn report_success(&mut self) -> Result<(), MigrationMachineError> {
        self.phase = match self.phase {
            Phase::AcquireLock => Phase::StartJournal,
            Phase::StartJournal => Phase::ExecuteSql,
            Phase::ExecuteSql => Phase::VerifySchema,
            Phase::VerifySchema => Phase::MarkApplied,
            Phase::MarkApplied => {
                self.current_index += 1;
                if self.current_index == self.pending.len() {
                    Phase::ReleaseLock
                } else {
                    Phase::StartJournal
                }
            }
            Phase::MarkFailed => Phase::ReleaseLock,
            Phase::ReleaseLock => {
                if self.failure.is_some() {
                    Phase::Failed
                } else {
                    Phase::Succeeded
                }
            }
            Phase::Succeeded | Phase::Failed => {
                return Err(MigrationMachineError::AlreadyFinished);
            }
        };
        Ok(())
    }

    /// Reports a stable, redacted error code for the current action.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationMachineError::AlreadyFinished`] after the machine has
    /// reached a terminal state.
    pub fn report_failure(
        &mut self,
        error_code: impl Into<String>,
    ) -> Result<(), MigrationMachineError> {
        let error_code = error_code.into();
        let Some(action) = self.next_action() else {
            return Err(MigrationMachineError::AlreadyFinished);
        };

        match action {
            MigrationExecutionAction::AcquireLock => {
                self.failure = Some(MigrationExecutionFailure {
                    logical_id: None,
                    failed_step: MigrationExecutionStep::AcquireLock,
                    error_code,
                    mark_failed_error: None,
                    release_lock_error: None,
                });
                self.phase = Phase::Failed;
            }
            MigrationExecutionAction::MarkFailed(_) => {
                if let Some(failure) = &mut self.failure {
                    failure.mark_failed_error = Some(error_code);
                }
                self.phase = Phase::ReleaseLock;
            }
            MigrationExecutionAction::ReleaseLock => {
                if let Some(failure) = &mut self.failure {
                    failure.release_lock_error = Some(error_code);
                } else {
                    self.failure = Some(MigrationExecutionFailure {
                        logical_id: None,
                        failed_step: MigrationExecutionStep::ReleaseLock,
                        error_code,
                        mark_failed_error: None,
                        release_lock_error: None,
                    });
                }
                self.phase = Phase::Failed;
            }
            MigrationExecutionAction::StartJournal(candidate)
            | MigrationExecutionAction::ExecuteSql(candidate)
            | MigrationExecutionAction::VerifySchema(candidate)
            | MigrationExecutionAction::MarkApplied(candidate) => {
                self.failure = Some(MigrationExecutionFailure {
                    logical_id: Some(candidate.descriptor.logical_id),
                    failed_step: action.step(),
                    error_code,
                    mark_failed_error: None,
                    release_lock_error: None,
                });
                self.phase = Phase::MarkFailed;
            }
        }
        Ok(())
    }

    pub fn is_finished(&self) -> bool {
        matches!(self.phase, Phase::Succeeded | Phase::Failed)
    }

    pub fn succeeded(&self) -> bool {
        self.phase == Phase::Succeeded
    }

    pub fn failure(&self) -> Option<&MigrationExecutionFailure> {
        self.failure.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use agent_loom_domain::Digest;

    use super::*;
    use crate::EmbeddedMigration;

    const MIGRATION: EmbeddedMigration = EmbeddedMigration {
        logical_id: "0001_core",
        logical_model_version: 1,
        created_tables: &["runs"],
        sql: "CREATE TABLE runs (id bigint PRIMARY KEY);",
    };
    const CANDIDATE: MigrationCandidate = MigrationCandidate {
        descriptor: MIGRATION,
        physical_checksum: Digest::from_bytes([1; 32]),
    };

    #[test]
    fn successful_execution_has_strict_step_order() {
        let mut machine = machine();
        let expected = [
            MigrationExecutionAction::AcquireLock,
            MigrationExecutionAction::StartJournal(CANDIDATE),
            MigrationExecutionAction::ExecuteSql(CANDIDATE),
            MigrationExecutionAction::VerifySchema(CANDIDATE),
            MigrationExecutionAction::MarkApplied(CANDIDATE),
            MigrationExecutionAction::ReleaseLock,
        ];

        for action in expected {
            assert_eq!(machine.next_action(), Some(action));
            machine.report_success().expect("action succeeds");
        }
        assert!(machine.succeeded());
        assert!(machine.is_finished());
        assert_eq!(machine.next_action(), None);
    }

    #[test]
    fn sql_failure_is_journaled_before_lock_release() {
        let mut machine = machine();
        machine.report_success().expect("lock acquired");
        machine.report_success().expect("journal started");
        assert_eq!(
            machine.next_action(),
            Some(MigrationExecutionAction::ExecuteSql(CANDIDATE))
        );

        machine
            .report_failure("DDL_FAILED")
            .expect("failure recorded");
        assert_eq!(
            machine.next_action(),
            Some(MigrationExecutionAction::MarkFailed(CANDIDATE))
        );
        machine.report_success().expect("failure journaled");
        assert_eq!(
            machine.next_action(),
            Some(MigrationExecutionAction::ReleaseLock)
        );
        machine.report_success().expect("lock released");

        assert!(machine.is_finished());
        assert!(!machine.succeeded());
        assert_eq!(
            machine.failure().map(|failure| failure.failed_step),
            Some(MigrationExecutionStep::ExecuteSql)
        );
    }

    fn machine() -> MigrationExecutionMachine {
        MigrationExecutionMachine::new(MigrationPlan {
            current_model_version: 0,
            target_model_version: 1,
            pending: vec![CANDIDATE],
        })
    }
}
