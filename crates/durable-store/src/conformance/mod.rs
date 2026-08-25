//! Provider-neutral behavioral cases. Database providers will execute these cases unchanged.

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConformanceCase {
    TenantIsolation,
    CreateRunAtomicity,
    CommandIdempotency,
    ConcurrentTaskClaim,
    LeaseRenewReclaimCompleteRace,
    WaitSingleConsumption,
    PauseGenerationFence,
    UniqueTerminalEvent,
    AgentEventCursorAtomicity,
    AgentEventDeduplication,
    AgentReceiptDigestConflict,
    ToolOutcomeUnknownReconciliation,
    ExternalExecutionGenerationFence,
    CommitOutcomeReconciliation,
    PostCommitHintLoss,
    DeliveryWorkflowEquivalence,
}

impl ConformanceCase {
    pub const fn stable_key(self) -> &'static str {
        match self {
            Self::TenantIsolation => "tenant_isolation",
            Self::CreateRunAtomicity => "create_run_atomicity",
            Self::CommandIdempotency => "command_idempotency",
            Self::ConcurrentTaskClaim => "concurrent_task_claim",
            Self::LeaseRenewReclaimCompleteRace => "lease_renew_reclaim_complete_race",
            Self::WaitSingleConsumption => "wait_single_consumption",
            Self::PauseGenerationFence => "pause_generation_fence",
            Self::UniqueTerminalEvent => "unique_terminal_event",
            Self::AgentEventCursorAtomicity => "agent_event_cursor_atomicity",
            Self::AgentEventDeduplication => "agent_event_deduplication",
            Self::AgentReceiptDigestConflict => "agent_receipt_digest_conflict",
            Self::ToolOutcomeUnknownReconciliation => "tool_outcome_unknown_reconciliation",
            Self::ExternalExecutionGenerationFence => "external_execution_generation_fence",
            Self::CommitOutcomeReconciliation => "commit_outcome_reconciliation",
            Self::PostCommitHintLoss => "post_commit_hint_loss",
            Self::DeliveryWorkflowEquivalence => "delivery_workflow_equivalence",
        }
    }
}

pub const CORE_CASES: &[ConformanceCase] = &[
    ConformanceCase::TenantIsolation,
    ConformanceCase::CreateRunAtomicity,
    ConformanceCase::CommandIdempotency,
    ConformanceCase::ConcurrentTaskClaim,
    ConformanceCase::LeaseRenewReclaimCompleteRace,
    ConformanceCase::WaitSingleConsumption,
    ConformanceCase::PauseGenerationFence,
    ConformanceCase::UniqueTerminalEvent,
    ConformanceCase::AgentEventCursorAtomicity,
    ConformanceCase::AgentEventDeduplication,
    ConformanceCase::AgentReceiptDigestConflict,
    ConformanceCase::ToolOutcomeUnknownReconciliation,
    ConformanceCase::ExternalExecutionGenerationFence,
    ConformanceCase::CommitOutcomeReconciliation,
    ConformanceCase::PostCommitHintLoss,
    ConformanceCase::DeliveryWorkflowEquivalence,
];

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn case_keys_are_unique_and_stable() {
        let keys: HashSet<_> = CORE_CASES.iter().map(|case| case.stable_key()).collect();
        assert_eq!(keys.len(), CORE_CASES.len());
        assert!(keys.contains("unique_terminal_event"));
        assert!(keys.contains("delivery_workflow_equivalence"));
    }
}
