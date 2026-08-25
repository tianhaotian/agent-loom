use agent_loom_domain::{
    AgentVersionId, CheckpointId, CommandId, CorrelationId, Digest, DurationMicros, EventId,
    IdempotencyKey, JsonPayload, LeaseToken, LogicalKey, RunId, ScopeKey, StageExecutionId, TaskId,
    TaskKind, TenantId, UnixMicros, WorkerId, WorkflowVersionId,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandContext {
    pub tenant_id: TenantId,
    pub command_id: CommandId,
    pub correlation_id: CorrelationId,
    pub actor_ref: String,
    pub scope: ScopeKey,
    pub idempotency_key: IdempotencyKey,
    pub request_hash: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryContext {
    pub tenant_id: TenantId,
    pub actor_ref: String,
    pub authoritative: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpectedRun {
    pub run_id: RunId,
    pub version: Option<u64>,
    pub execution_generation: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseProof {
    pub task_id: TaskId,
    pub worker_id: WorkerId,
    pub token: LeaseToken,
    pub execution_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialTask {
    pub task_id: TaskId,
    pub stage_execution_id: Option<StageExecutionId>,
    pub logical_key: LogicalKey,
    pub kind: TaskKind,
    pub priority: i32,
    pub available_at: UnixMicros,
    pub max_attempts: u32,
    pub input: JsonPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateRun {
    pub run_id: RunId,
    pub workflow_version_id: Option<WorkflowVersionId>,
    pub coordinator_agent_version_id: Option<AgentVersionId>,
    pub input: JsonPayload,
    pub deadline: Option<UnixMicros>,
    pub initial_event_id: EventId,
    pub initial_checkpoint_id: CheckpointId,
    pub initial_tasks: Vec<InitialTask>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimTask {
    pub worker_id: WorkerId,
    pub lease_token: LeaseToken,
    pub lease_duration: DurationMicros,
    pub candidate_window: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenewTaskLease {
    pub expected_run: ExpectedRun,
    pub lease: LeaseProof,
    pub extension: DurationMicros,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteTask {
    pub expected_run: ExpectedRun,
    pub lease: LeaseProof,
    pub completion_event_id: EventId,
    pub checkpoint_id: CheckpointId,
    pub result: JsonPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailTask {
    pub expected_run: ExpectedRun,
    pub lease: LeaseProof,
    pub failure_event_id: EventId,
    pub error_code: String,
    pub retry_at: Option<UnixMicros>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyEvent {
    pub expected_run: ExpectedRun,
    pub event_id: EventId,
    pub event_type: String,
    pub payload_schema_version: u32,
    pub payload: JsonPayload,
    pub occurred_at: Option<UnixMicros>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlRun {
    pub expected_run: ExpectedRun,
    pub event_id: EventId,
    pub reason: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventCursor {
    pub run_id: RunId,
    pub after_sequence: u64,
    pub limit: u32,
}
