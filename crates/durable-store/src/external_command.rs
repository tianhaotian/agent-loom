use agent_loom_domain::{
    AgentEventReceiptId, AgentExecutionId, AgentExecutionStatus, AgentVersionId, Digest,
    EndpointId, EventId, IdempotencyKey, JsonPayload, RunStatus, ScopeKey, StageExecutionId,
    TaskId, TenantId, ToolAttemptId, ToolExecutionId, ToolExecutionStatus,
};

use crate::{ExpectedRun, LeaseProof};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrepareToolExecution {
    pub expected_run: ExpectedRun,
    pub lease: LeaseProof,
    pub tool_execution_id: ToolExecutionId,
    pub tool_attempt_id: ToolAttemptId,
    pub stage_execution_id: Option<StageExecutionId>,
    pub tool_call_id: String,
    pub tool_name: String,
    pub idempotency_scope: ScopeKey,
    pub idempotency_key: IdempotencyKey,
    pub request_hash: Digest,
    pub request: JsonPayload,
    pub prepared_event_id: EventId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionRetryClass {
    Never,
    SameRequestBackoff,
    ReconnectAndResume,
    QueryOutcome,
    ManualReview,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolRecordedOutcome {
    Completed {
        result: JsonPayload,
    },
    Accepted {
        external_ref: String,
    },
    Failed {
        error_code: String,
        retry: ExecutionRetryClass,
        retry_at: Option<agent_loom_domain::UnixMicros>,
    },
    Uncertain {
        external_ref: Option<String>,
        recovery_action: String,
    },
    Compensated {
        result: JsonPayload,
    },
}

impl ToolRecordedOutcome {
    pub const fn projected_status(&self) -> ToolExecutionStatus {
        match self {
            Self::Completed { .. } => ToolExecutionStatus::Succeeded,
            Self::Accepted { .. } => ToolExecutionStatus::Executing,
            Self::Failed { retry, .. } => match retry {
                ExecutionRetryClass::SameRequestBackoff => ToolExecutionStatus::RetryScheduled,
                ExecutionRetryClass::QueryOutcome | ExecutionRetryClass::ReconnectAndResume => {
                    ToolExecutionStatus::Reconciling
                }
                ExecutionRetryClass::Never => ToolExecutionStatus::Failed,
                ExecutionRetryClass::ManualReview => ToolExecutionStatus::ManualReview,
            },
            Self::Uncertain { .. } => ToolExecutionStatus::OutcomeUnknown,
            Self::Compensated { .. } => ToolExecutionStatus::Compensated,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordToolOutcome {
    pub expected_run: ExpectedRun,
    pub task_id: TaskId,
    pub execution_generation: u64,
    pub tool_execution_id: ToolExecutionId,
    pub expected_attempt: u32,
    pub outcome: ToolRecordedOutcome,
    pub outcome_event_id: EventId,
    pub response_digest: Option<Digest>,
    pub remote_request_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrepareAgentExecution {
    pub expected_run: ExpectedRun,
    pub lease: LeaseProof,
    pub agent_execution_id: AgentExecutionId,
    pub stage_execution_id: Option<StageExecutionId>,
    pub endpoint_id: EndpointId,
    pub agent_version_id: AgentVersionId,
    pub idempotency_key: IdempotencyKey,
    pub request_hash: Digest,
    pub capabilities_snapshot: JsonPayload,
    pub prepared_event_id: EventId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentSubmissionOutcome {
    Accepted {
        remote_run_ref: String,
        remote_session_ref: Option<String>,
    },
    Uncertain,
    Rejected {
        error_code: String,
        retry: ExecutionRetryClass,
        retry_at: Option<agent_loom_domain::UnixMicros>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordAgentSubmission {
    pub expected_run: ExpectedRun,
    pub agent_execution_id: AgentExecutionId,
    pub expected_version: u64,
    pub outcome: AgentSubmissionOutcome,
    pub submission_event_id: EventId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NormalizedAgentEventInput {
    pub receipt_id: AgentEventReceiptId,
    pub dedupe_key: Digest,
    pub source_event_id: Option<String>,
    pub source_sequence: Option<u64>,
    pub source_cursor: Option<String>,
    pub event_kind: String,
    pub authoritative: bool,
    pub raw_digest: Digest,
    pub local_event_id: Option<EventId>,
    pub payload_schema_version: u32,
    pub payload: JsonPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendAgentEvents {
    pub expected_run: ExpectedRun,
    pub agent_execution_id: AgentExecutionId,
    pub expected_cursor_version: u64,
    pub next_cursor: Option<String>,
    pub events: Vec<NormalizedAgentEventInput>,
}

impl AppendAgentEvents {
    /// Validates deterministic identities before opening a database transaction.
    ///
    /// # Errors
    ///
    /// Returns [`AgentEventBatchShapeError`] for duplicate receipts/dedupe keys,
    /// malformed event identity, or an authoritative event without a local ID.
    pub fn validate_shape(&self) -> Result<(), AgentEventBatchShapeError> {
        if self.next_cursor.as_ref().is_some_and(String::is_empty) {
            return Err(AgentEventBatchShapeError::InvalidCursor);
        }
        for (index, event) in self.events.iter().enumerate() {
            if event.event_kind.is_empty()
                || event.payload_schema_version == 0
                || event.source_event_id.as_ref().is_some_and(String::is_empty)
                || event.source_cursor.as_ref().is_some_and(String::is_empty)
            {
                return Err(AgentEventBatchShapeError::InvalidEvent { index });
            }
            if event.authoritative != event.local_event_id.is_some() {
                return Err(AgentEventBatchShapeError::AuthorityMismatch { index });
            }
            if self.events[..index]
                .iter()
                .any(|previous| previous.receipt_id == event.receipt_id)
            {
                return Err(AgentEventBatchShapeError::DuplicateReceipt { index });
            }
            if self.events[..index]
                .iter()
                .any(|previous| previous.dedupe_key == event.dedupe_key)
            {
                return Err(AgentEventBatchShapeError::DuplicateDedupeKey { index });
            }
            if let Some(local_event_id) = event.local_event_id
                && self.events[..index]
                    .iter()
                    .any(|previous| previous.local_event_id == Some(local_event_id))
            {
                return Err(AgentEventBatchShapeError::DuplicateLocalEvent { index });
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentEventBatchShapeError {
    InvalidCursor,
    InvalidEvent { index: usize },
    AuthorityMismatch { index: usize },
    DuplicateReceipt { index: usize },
    DuplicateDedupeKey { index: usize },
    DuplicateLocalEvent { index: usize },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordAgentOutcome {
    pub expected_run: ExpectedRun,
    pub agent_execution_id: AgentExecutionId,
    pub expected_version: u64,
    pub status: AgentExecutionStatus,
    pub result: Option<JsonPayload>,
    pub error_code: Option<String>,
    pub outcome_event_id: EventId,
}

impl RecordAgentOutcome {
    pub fn shape_is_valid(&self) -> bool {
        self.status.is_terminal()
            || self.status.requires_reconciliation()
            || self.status == AgentExecutionStatus::ManualReview
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentEventBatchOutcome {
    pub tenant_id: TenantId,
    pub agent_execution_id: AgentExecutionId,
    pub run_id: agent_loom_domain::RunId,
    pub accepted_receipts: Vec<AgentEventReceiptId>,
    pub duplicate_receipts: Vec<AgentEventReceiptId>,
    pub cursor_version: u64,
    pub run_status: RunStatus,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uncertain_tool_outcome_projects_to_reconciliation_state() {
        let outcome = ToolRecordedOutcome::Uncertain {
            external_ref: None,
            recovery_action: "query_outcome".to_owned(),
        };
        assert_eq!(
            outcome.projected_status(),
            ToolExecutionStatus::OutcomeUnknown
        );
    }

    #[test]
    fn agent_event_batch_rejects_duplicate_dedupe_keys() {
        let mut batch = AppendAgentEvents {
            expected_run: ExpectedRun {
                run_id: agent_loom_domain::RunId::from_bytes([1; 16]),
                version: Some(1),
                execution_generation: Some(0),
            },
            agent_execution_id: AgentExecutionId::from_bytes([2; 16]),
            expected_cursor_version: 0,
            next_cursor: Some("cursor-2".to_owned()),
            events: vec![event(3, 4), event(5, 6)],
        };
        batch.events[1].dedupe_key = batch.events[0].dedupe_key;

        assert_eq!(
            batch.validate_shape(),
            Err(AgentEventBatchShapeError::DuplicateDedupeKey { index: 1 })
        );
    }

    #[test]
    fn agent_event_batch_rejects_an_empty_remote_cursor() {
        let batch = AppendAgentEvents {
            expected_run: ExpectedRun {
                run_id: agent_loom_domain::RunId::from_bytes([1; 16]),
                version: Some(1),
                execution_generation: Some(0),
            },
            agent_execution_id: AgentExecutionId::from_bytes([2; 16]),
            expected_cursor_version: 0,
            next_cursor: Some(String::new()),
            events: Vec::new(),
        };

        assert_eq!(
            batch.validate_shape(),
            Err(AgentEventBatchShapeError::InvalidCursor)
        );
    }

    fn event(receipt_byte: u8, event_byte: u8) -> NormalizedAgentEventInput {
        NormalizedAgentEventInput {
            receipt_id: AgentEventReceiptId::from_bytes([receipt_byte; 16]),
            dedupe_key: Digest::from_bytes([receipt_byte; 32]),
            source_event_id: Some(format!("remote-{receipt_byte}")),
            source_sequence: Some(u64::from(receipt_byte)),
            source_cursor: None,
            event_kind: "agent.progress".to_owned(),
            authoritative: true,
            raw_digest: Digest::from_bytes([event_byte; 32]),
            local_event_id: Some(EventId::from_bytes([event_byte; 16])),
            payload_schema_version: 1,
            payload: JsonPayload::from_validated_bytes(b"{}".to_vec()),
        }
    }
}
