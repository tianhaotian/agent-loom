use crate::{
    AgentEventReceiptId, AgentExecutionId, AgentExecutionStatus, AgentVersionId, ArtifactId,
    CheckpointId, Digest, EndpointId, EventId, JsonPayload, LogicalKey, RunId, RunStatus,
    StageExecutionId, StageStatus, TaskId, TaskStatus, TenantId, ToolExecutionId,
    ToolExecutionStatus, UnixMicros, WaitId, WaitStatus, WorkflowId, WorkflowVersionId,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowSnapshot {
    pub tenant_id: TenantId,
    pub workflow_id: WorkflowId,
    pub workflow_version_id: WorkflowVersionId,
    pub workflow_key: String,
    pub name: String,
    pub status: String,
    pub version: u64,
    pub lifecycle: String,
    pub spec: JsonPayload,
    pub spec_digest: Digest,
    pub created_at: UnixMicros,
    pub updated_at: UnixMicros,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageExecutionSnapshot {
    pub tenant_id: TenantId,
    pub stage_execution_id: StageExecutionId,
    pub run_id: RunId,
    pub stage_key: LogicalKey,
    pub definition_stage_key: Option<LogicalKey>,
    pub status: StageStatus,
    pub version: u64,
    pub attempt: u32,
    pub assignee_kind: Option<String>,
    pub assignee_ref: Option<String>,
    pub input_contract: JsonPayload,
    pub output_contract: JsonPayload,
    pub started_at: Option<UnixMicros>,
    pub completed_at: Option<UnixMicros>,
    pub created_at: UnixMicros,
    pub updated_at: UnixMicros,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunSnapshot {
    pub tenant_id: TenantId,
    pub run_id: RunId,
    pub workflow_version_id: Option<WorkflowVersionId>,
    pub status: RunStatus,
    pub suspended_from_status: Option<RunStatus>,
    pub version: u64,
    pub execution_generation: u64,
    pub next_event_sequence: u64,
    pub current_checkpoint_id: Option<CheckpointId>,
    pub terminal_event_id: Option<EventId>,
    pub deadline: Option<UnixMicros>,
    pub updated_at: UnixMicros,
}

impl RunSnapshot {
    pub fn terminal_invariant_holds(&self) -> bool {
        self.status.is_terminal() == self.terminal_event_id.is_some()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TaskKind {
    Model,
    Tool,
    AgentServer,
    ArtifactCheck,
    TimerWakeup,
    Reconcile,
    StopExternalExecution,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskSnapshot {
    pub tenant_id: TenantId,
    pub task_id: TaskId,
    pub run_id: RunId,
    pub stage_execution_id: Option<StageExecutionId>,
    pub logical_key: LogicalKey,
    pub kind: TaskKind,
    pub status: TaskStatus,
    pub generation: u64,
    pub attempt: u32,
    pub max_attempts: u32,
    pub available_at: UnixMicros,
    pub input: JsonPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventRecord {
    pub tenant_id: TenantId,
    pub event_id: EventId,
    pub run_id: RunId,
    pub sequence: u64,
    pub event_type: String,
    pub payload_schema_version: u32,
    pub payload: JsonPayload,
    pub occurred_at: Option<UnixMicros>,
    pub recorded_at: UnixMicros,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WaitSnapshot {
    pub tenant_id: TenantId,
    pub wait_id: WaitId,
    pub run_id: RunId,
    pub stage_execution_id: Option<StageExecutionId>,
    pub wait_type: String,
    pub expected_event_type: String,
    pub match_key_hash: Digest,
    pub status: WaitStatus,
    pub active_slot: Option<u8>,
    pub expires_at: Option<UnixMicros>,
    pub consumed_by_event_id: Option<EventId>,
    pub created_event_id: EventId,
}

impl WaitSnapshot {
    pub fn active_slot_invariant_holds(&self) -> bool {
        match self.status {
            WaitStatus::Open => self.active_slot == Some(1) && self.consumed_by_event_id.is_none(),
            WaitStatus::Consumed => {
                self.active_slot.is_none() && self.consumed_by_event_id.is_some()
            }
            WaitStatus::Expired | WaitStatus::Cancelled => {
                self.active_slot.is_none() && self.consumed_by_event_id.is_none()
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ArtifactVersionRef {
    pub artifact_id: ArtifactId,
    pub version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactRefSnapshot {
    pub tenant_id: TenantId,
    pub artifact_id: ArtifactId,
    pub run_id: RunId,
    pub stage_execution_id: Option<StageExecutionId>,
    pub task_id: Option<TaskId>,
    pub logical_key: LogicalKey,
    pub kind: String,
    pub contract_version: u32,
    pub version: u64,
    pub uri: String,
    pub digest: Digest,
    pub media_type: String,
    pub size_bytes: u64,
    pub sources: Vec<ArtifactVersionRef>,
    pub metadata: JsonPayload,
    pub produced_by: String,
    pub created_event_id: EventId,
    pub created_at: UnixMicros,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolExecutionSnapshot {
    pub tenant_id: TenantId,
    pub tool_execution_id: ToolExecutionId,
    pub run_id: RunId,
    pub stage_execution_id: Option<StageExecutionId>,
    pub task_id: TaskId,
    pub tool_call_id: String,
    pub tool_name: String,
    pub status: ToolExecutionStatus,
    pub attempt_count: u32,
    pub external_ref: Option<String>,
    pub recovery_action: Option<String>,
    pub retry_at: Option<UnixMicros>,
    pub updated_at: UnixMicros,
}

impl ToolExecutionSnapshot {
    pub fn recovery_invariant_holds(&self) -> bool {
        (self.status != ToolExecutionStatus::OutcomeUnknown || self.recovery_action.is_some())
            && ((self.status == ToolExecutionStatus::RetryScheduled) == self.retry_at.is_some())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentExecutionSnapshot {
    pub tenant_id: TenantId,
    pub agent_execution_id: AgentExecutionId,
    pub run_id: RunId,
    pub stage_execution_id: Option<StageExecutionId>,
    pub task_id: TaskId,
    pub endpoint_id: EndpointId,
    pub agent_version_id: AgentVersionId,
    pub status: AgentExecutionStatus,
    pub version: u64,
    pub remote_run_ref: Option<String>,
    pub remote_session_ref: Option<String>,
    pub event_cursor: Option<String>,
    pub cursor_version: u64,
    pub retry_at: Option<UnixMicros>,
    pub updated_at: UnixMicros,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentEventReceiptRecord {
    pub tenant_id: TenantId,
    pub receipt_id: AgentEventReceiptId,
    pub agent_execution_id: AgentExecutionId,
    pub run_id: RunId,
    pub dedupe_key: Digest,
    pub source_event_id: Option<String>,
    pub source_sequence: Option<u64>,
    pub event_kind: String,
    pub raw_digest: Digest,
    pub local_event_id: Option<EventId>,
    pub recorded_at: UnixMicros,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_projection_requires_terminal_event() {
        let snapshot = RunSnapshot {
            tenant_id: TenantId::from_bytes([1; 16]),
            run_id: RunId::from_bytes([2; 16]),
            workflow_version_id: None,
            status: RunStatus::Completed,
            suspended_from_status: None,
            version: 2,
            execution_generation: 0,
            next_event_sequence: 3,
            current_checkpoint_id: None,
            terminal_event_id: None,
            deadline: None,
            updated_at: UnixMicros::new(10),
        };
        assert!(!snapshot.terminal_invariant_holds());
    }

    #[test]
    fn wait_active_slot_tracks_open_and_consumed_states() {
        let mut wait = WaitSnapshot {
            tenant_id: TenantId::from_bytes([1; 16]),
            wait_id: WaitId::from_bytes([2; 16]),
            run_id: RunId::from_bytes([3; 16]),
            stage_execution_id: None,
            wait_type: "approval".to_owned(),
            expected_event_type: "approval.granted".to_owned(),
            match_key_hash: Digest::from_bytes([4; 32]),
            status: WaitStatus::Open,
            active_slot: Some(1),
            expires_at: None,
            consumed_by_event_id: None,
            created_event_id: EventId::from_bytes([5; 16]),
        };
        assert!(wait.active_slot_invariant_holds());

        wait.status = WaitStatus::Consumed;
        wait.active_slot = None;
        wait.consumed_by_event_id = Some(EventId::from_bytes([6; 16]));
        assert!(wait.active_slot_invariant_holds());
    }

    #[test]
    fn unknown_tool_outcome_requires_recovery_action() {
        let snapshot = ToolExecutionSnapshot {
            tenant_id: TenantId::from_bytes([1; 16]),
            tool_execution_id: ToolExecutionId::from_bytes([2; 16]),
            run_id: RunId::from_bytes([3; 16]),
            stage_execution_id: None,
            task_id: TaskId::from_bytes([4; 16]),
            tool_call_id: "deploy".to_owned(),
            tool_name: "devops.deploy".to_owned(),
            status: ToolExecutionStatus::OutcomeUnknown,
            attempt_count: 1,
            external_ref: None,
            recovery_action: None,
            retry_at: None,
            updated_at: UnixMicros::new(10),
        };
        assert!(!snapshot.recovery_invariant_holds());
    }
}
