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
        matches!(self, Self::Queued | Self::Running)
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
}
