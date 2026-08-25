use crate::{
    CheckpointId, EventId, JsonPayload, LogicalKey, RunId, RunStatus, StageExecutionId, TaskId,
    TaskStatus, TenantId, UnixMicros, WorkflowVersionId,
};

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
}
