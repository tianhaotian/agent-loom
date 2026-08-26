#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RunStatus {
    Queued,
    Running,
    Waiting,
    ApprovalRequired,
    Retrying,
    Paused,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
}

impl RunStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }

    pub const fn accepts_task_claim(self) -> bool {
        matches!(self, Self::Queued | Self::Running | Self::Retrying)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StageStatus {
    Planned,
    Active,
    WaitingApproval,
    ReworkRequired,
    Succeeded,
    Failed,
    Skipped,
    Cancelled,
}

impl StageStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Skipped | Self::Cancelled
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TaskStatus {
    Scheduled,
    Queued,
    Leased,
    RetryScheduled,
    Succeeded,
    Failed,
    DeadLettered,
    Cancelled,
}

impl TaskStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::DeadLettered | Self::Cancelled
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WaitStatus {
    Open,
    Consumed,
    Expired,
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ToolExecutionStatus {
    Planned,
    Executing,
    RetryScheduled,
    Succeeded,
    Failed,
    OutcomeUnknown,
    Reconciling,
    Compensated,
    ManualReview,
}

impl ToolExecutionStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Compensated)
    }

    pub const fn requires_reconciliation(self) -> bool {
        matches!(self, Self::OutcomeUnknown | Self::Reconciling)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AgentExecutionStatus {
    Planned,
    Submitting,
    Running,
    Stopping,
    Succeeded,
    Failed,
    Cancelled,
    OutcomeUnknown,
    Reconciling,
    ManualReview,
}

impl AgentExecutionStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    pub const fn requires_reconciliation(self) -> bool {
        matches!(self, Self::OutcomeUnknown | Self::Reconciling)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_terminal_set_is_explicit() {
        assert!(RunStatus::Completed.is_terminal());
        assert!(RunStatus::Cancelled.is_terminal());
        assert!(!RunStatus::Paused.is_terminal());
        assert!(!RunStatus::Running.is_terminal());
    }

    #[test]
    fn retrying_run_accepts_the_scheduled_retry_claim() {
        assert!(RunStatus::Queued.accepts_task_claim());
        assert!(RunStatus::Running.accepts_task_claim());
        assert!(RunStatus::Retrying.accepts_task_claim());
        assert!(!RunStatus::Waiting.accepts_task_claim());
        assert!(!RunStatus::Paused.accepts_task_claim());
    }

    #[test]
    fn external_execution_terminal_and_reconciliation_sets_are_explicit() {
        assert!(ToolExecutionStatus::Compensated.is_terminal());
        assert!(ToolExecutionStatus::OutcomeUnknown.requires_reconciliation());
        assert!(AgentExecutionStatus::Cancelled.is_terminal());
        assert!(AgentExecutionStatus::Reconciling.requires_reconciliation());
        assert!(!AgentExecutionStatus::ManualReview.is_terminal());
    }
}
