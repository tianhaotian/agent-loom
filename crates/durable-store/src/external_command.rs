use agent_loom_domain::{
    AgentEventReceiptId, AgentExecutionId, AgentExecutionStatus, AgentVersionId, Digest,
    EndpointId, EventId, IdempotencyKey, JsonPayload, RunStatus, ScopeKey, StageExecutionId,
    TaskId, TenantId, ToolAttemptId, ToolExecutionId, ToolExecutionStatus,
};

use crate::{ExpectedRun, LeaseProof, NewArtifactRef, NewTask, NewWaitSubscription};

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
    pub projection: AgentEventProjection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentEventProjection {
    pub workflow_action: AgentWorkflowAction,
    pub artifacts: Vec<NewArtifactRef>,
    pub execution_outcome: Option<AgentEventExecutionOutcome>,
}

impl AgentEventProjection {
    pub const NONE: Self = Self {
        workflow_action: AgentWorkflowAction::None,
        artifacts: Vec::new(),
        execution_outcome: None,
    };

    pub fn is_empty(&self) -> bool {
        self.workflow_action == AgentWorkflowAction::None
            && self.artifacts.is_empty()
            && self.execution_outcome.is_none()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentWorkflowAction {
    None,
    Tasks(Vec<NewTask>),
    Wait(Box<NewWaitSubscription>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentEventExecutionOutcome {
    pub status: AgentExecutionStatus,
    pub result: Option<JsonPayload>,
    pub error_code: Option<String>,
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
            if !event.authoritative && !event.projection.is_empty() {
                return Err(AgentEventBatchShapeError::ProjectionMismatch { index });
            }
            if event.authoritative && !projection_matches_event(event) {
                return Err(AgentEventBatchShapeError::ProjectionMismatch { index });
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
        let action_count = self
            .events
            .iter()
            .filter(|event| event.projection.workflow_action != AgentWorkflowAction::None)
            .count();
        let outcome_count = self
            .events
            .iter()
            .filter(|event| event.projection.execution_outcome.is_some())
            .count();
        if action_count > 1 || outcome_count > 1 {
            return Err(AgentEventBatchShapeError::ConflictingProjection);
        }
        Ok(())
    }
}

fn projection_matches_event(event: &NormalizedAgentEventInput) -> bool {
    let Some(local_event_id) = event.local_event_id else {
        return event.projection.is_empty();
    };
    let action_is_valid = match &event.projection.workflow_action {
        AgentWorkflowAction::None => true,
        AgentWorkflowAction::Tasks(tasks) => {
            !tasks.is_empty()
                && tasks.iter().all(|task| {
                    task.created_event_id == local_event_id
                        && task.based_on_checkpoint_sequence > 0
                        && task.max_attempts > 0
                })
        }
        AgentWorkflowAction::Wait(wait) => {
            wait.created_event_id == local_event_id
                && !wait.wait_type.is_empty()
                && !wait.expected_event_type.is_empty()
                && wait.resume_task.max_attempts > 0
        }
    };
    let artifacts_are_valid = event.projection.artifacts.iter().all(|artifact| {
        artifact.created_event_id == local_event_id
            && artifact.contract_version > 0
            && artifact.version > 0
            && !artifact.kind.is_empty()
            && !artifact.uri.is_empty()
            && !artifact.media_type.is_empty()
            && !artifact.produced_by.is_empty()
    });
    let outcome_is_valid = event
        .projection
        .execution_outcome
        .as_ref()
        .is_none_or(agent_event_outcome_shape_is_valid);
    action_is_valid && artifacts_are_valid && outcome_is_valid
}

fn agent_event_outcome_shape_is_valid(outcome: &AgentEventExecutionOutcome) -> bool {
    if outcome.error_code.as_ref().is_some_and(String::is_empty) {
        return false;
    }
    match outcome.status {
        AgentExecutionStatus::Succeeded => outcome.result.is_some() && outcome.error_code.is_none(),
        AgentExecutionStatus::Failed => outcome.result.is_none() && outcome.error_code.is_some(),
        AgentExecutionStatus::Cancelled
        | AgentExecutionStatus::OutcomeUnknown
        | AgentExecutionStatus::Reconciling
        | AgentExecutionStatus::ManualReview => outcome.result.is_none(),
        AgentExecutionStatus::Planned
        | AgentExecutionStatus::Submitting
        | AgentExecutionStatus::Running
        | AgentExecutionStatus::Stopping => false,
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
    ProjectionMismatch { index: usize },
    ConflictingProjection,
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

    #[test]
    fn transient_agent_event_cannot_carry_a_workflow_projection() {
        let mut transient = event(3, 4);
        transient.authoritative = false;
        transient.local_event_id = None;
        transient.projection = AgentEventProjection {
            workflow_action: AgentWorkflowAction::Tasks(vec![task(EventId::from_bytes([4; 16]))]),
            artifacts: Vec::new(),
            execution_outcome: None,
        };
        let batch = batch(vec![transient]);

        assert_eq!(
            batch.validate_shape(),
            Err(AgentEventBatchShapeError::ProjectionMismatch { index: 0 })
        );
    }

    #[test]
    fn agent_projection_objects_must_belong_to_the_local_event() {
        let mut projected = event(3, 4);
        projected.projection = AgentEventProjection {
            workflow_action: AgentWorkflowAction::Tasks(vec![task(EventId::from_bytes([9; 16]))]),
            artifacts: Vec::new(),
            execution_outcome: None,
        };

        assert_eq!(
            batch(vec![projected]).validate_shape(),
            Err(AgentEventBatchShapeError::ProjectionMismatch { index: 0 })
        );
    }

    #[test]
    fn agent_event_batch_allows_only_one_scheduling_decision() {
        let mut first = event(3, 4);
        first.projection = AgentEventProjection {
            workflow_action: AgentWorkflowAction::Tasks(vec![task(EventId::from_bytes([4; 16]))]),
            artifacts: Vec::new(),
            execution_outcome: None,
        };
        let mut second = event(5, 6);
        second.projection = AgentEventProjection {
            workflow_action: AgentWorkflowAction::Tasks(vec![task(EventId::from_bytes([6; 16]))]),
            artifacts: Vec::new(),
            execution_outcome: None,
        };

        assert_eq!(
            batch(vec![first, second]).validate_shape(),
            Err(AgentEventBatchShapeError::ConflictingProjection)
        );
    }

    fn batch(events: Vec<NormalizedAgentEventInput>) -> AppendAgentEvents {
        AppendAgentEvents {
            expected_run: ExpectedRun {
                run_id: agent_loom_domain::RunId::from_bytes([1; 16]),
                version: Some(1),
                execution_generation: Some(0),
            },
            agent_execution_id: AgentExecutionId::from_bytes([2; 16]),
            expected_cursor_version: 0,
            next_cursor: Some("cursor-2".to_owned()),
            events,
        }
    }

    fn task(created_event_id: EventId) -> NewTask {
        NewTask {
            task_id: TaskId::from_bytes([8; 16]),
            stage_execution_id: None,
            logical_key: agent_loom_domain::LogicalKey::parse("agent/projected-task")
                .expect("logical key"),
            kind: agent_loom_domain::TaskKind::Model,
            generation: 0,
            based_on_checkpoint_sequence: 1,
            priority: 1,
            available_at: agent_loom_domain::UnixMicros::new(1),
            max_attempts: 1,
            input: JsonPayload::from_validated_bytes(b"{}".to_vec()),
            deadline: None,
            created_event_id,
        }
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
            projection: AgentEventProjection::NONE,
        }
    }
}
