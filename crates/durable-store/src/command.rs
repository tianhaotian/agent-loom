use agent_loom_domain::{
    AgentVersionId, ArtifactId, ArtifactVersionRef, CheckpointId, CommandId, ContextMergeStrategy,
    ContextPatchId, ContextSnapshotId, CorrelationId, Digest, DurationMicros, EventId,
    IdempotencyKey, JoinPolicy, JsonPayload, LeaseToken, LogicalKey, PlanRevisionId, RunId,
    RunStatus, ScheduleId, ScopeKey, StageExecutionId, StageStatus, TaskId, TaskKind, TenantId,
    UnixMicros, WaitId, WorkerId, WorkflowVersionId,
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
    pub dependencies: Vec<InitialTaskDependency>,
    pub join_policy: JoinPolicy,
    pub context_projection: JsonPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialTaskDependency {
    pub prerequisite_task_id: TaskId,
    pub condition: JsonPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateRun {
    pub run_id: RunId,
    pub replay_of_run_id: Option<RunId>,
    pub parent_run_id: Option<RunId>,
    pub parent_task_id: Option<TaskId>,
    pub parent_event_id: Option<EventId>,
    pub schedule_id: Option<ScheduleId>,
    pub scheduled_fire_at: Option<UnixMicros>,
    pub workflow_version_id: Option<WorkflowVersionId>,
    pub coordinator_agent_version_id: Option<AgentVersionId>,
    pub input: JsonPayload,
    pub deadline: Option<UnixMicros>,
    pub initial_event_id: EventId,
    pub initial_plan_revision: NewPlanRevision,
    pub initial_context: NewContextSnapshot,
    pub initial_checkpoint: NewCheckpoint,
    pub initial_stages: Vec<InitialStage>,
    pub initial_tasks: Vec<InitialTask>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvaluateChildRunJoin {
    pub expected_run: ExpectedRun,
    pub task_id: TaskId,
    pub join_policy: JoinPolicy,
    pub event_id: EventId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewContextSnapshot {
    pub context_snapshot_id: ContextSnapshotId,
    pub schema_version: u32,
    pub value: JsonPayload,
    pub digest: Digest,
    pub created_event_id: EventId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyContextPatch {
    pub expected_run: ExpectedRun,
    pub expected_context_revision: u64,
    pub event_id: EventId,
    pub patch_id: ContextPatchId,
    pub context_snapshot_id: ContextSnapshotId,
    pub schema_version: u32,
    pub patch: JsonPayload,
    pub merge_strategy: ContextMergeStrategy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewPlanRevision {
    pub plan_revision_id: PlanRevisionId,
    pub schema_version: u32,
    pub plan_key: LogicalKey,
    pub plan: JsonPayload,
    pub plan_digest: Digest,
    pub change_summary: JsonPayload,
    pub created_event_id: EventId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevisePlan {
    pub expected_run: ExpectedRun,
    pub expected_plan_revision: u64,
    pub event_id: EventId,
    pub revision: NewPlanRevision,
    pub new_tasks: Vec<InitialTask>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialStage {
    pub stage_execution_id: StageExecutionId,
    pub stage_key: LogicalKey,
    pub definition_stage_key: LogicalKey,
    pub status: StageStatus,
    pub attempt: u32,
    pub assignee_kind: Option<String>,
    pub assignee_ref: Option<String>,
    pub input_contract: JsonPayload,
    pub output_contract: JsonPayload,
    pub policy: JsonPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimTask {
    pub worker_id: WorkerId,
    pub lease_token: LeaseToken,
    pub lease_duration: DurationMicros,
    pub candidate_window: u32,
    pub kind: Option<TaskKind>,
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
    pub checkpoint: NewCheckpoint,
    pub task_result: TaskResult,
    pub stage_mutation: Option<StageMutation>,
    pub additional_stage_mutations: Vec<StageMutation>,
    pub new_stages: Vec<InitialStage>,
    pub artifacts: Vec<NewArtifactRef>,
    pub next: NextActions,
}

impl CompleteTask {
    /// Validates invariants that do not require authoritative database state.
    ///
    /// # Errors
    ///
    /// Returns [`CompletionShapeError`] when event ownership, generation
    /// fencing, checkpoint metadata, or a requested terminal result is invalid.
    pub fn validate_shape(&self) -> Result<(), CompletionShapeError> {
        if self.checkpoint.created_event_id != self.completion_event_id
            || self
                .artifacts
                .iter()
                .any(|artifact| artifact.created_event_id != self.completion_event_id)
            || self.next.created_event_mismatch(self.completion_event_id)
        {
            return Err(CompletionShapeError::CreatedEventMismatch);
        }

        if self.checkpoint.sequence == 0 || self.checkpoint.schema_version == 0 {
            return Err(CompletionShapeError::InvalidCheckpointMetadata);
        }

        if self.artifacts.iter().any(|artifact| {
            artifact.contract_version == 0
                || artifact.version == 0
                || artifact.kind.is_empty()
                || artifact.uri.is_empty()
                || artifact.media_type.is_empty()
                || artifact.produced_by.is_empty()
                || artifact.sources.iter().any(|source| source.version == 0)
        }) {
            return Err(CompletionShapeError::InvalidArtifactMetadata);
        }

        if self.new_stages.iter().any(|stage| {
            stage.attempt == 0
                || stage.assignee_kind.is_some() != stage.assignee_ref.is_some()
                || stage.assignee_kind.as_deref().is_some_and(str::is_empty)
                || stage.assignee_ref.as_deref().is_some_and(str::is_empty)
        }) {
            return Err(CompletionShapeError::InvalidStageMetadata);
        }

        if let Some(error) = self.next.metadata_error() {
            return Err(error);
        }

        let generation = self.lease.execution_generation;
        if self.checkpoint.execution_generation != generation
            || self
                .expected_run
                .execution_generation
                .is_some_and(|expected| expected != generation)
            || self.next.generation_mismatch(generation)
        {
            return Err(CompletionShapeError::GenerationMismatch);
        }

        if self
            .next
            .final_status()
            .is_some_and(|status| !status.is_terminal())
        {
            return Err(CompletionShapeError::FinishRunRequiresTerminalStatus);
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewCheckpoint {
    pub checkpoint_id: CheckpointId,
    pub sequence: u64,
    pub schema_version: u32,
    pub workflow_version_id: Option<WorkflowVersionId>,
    pub coordinator_agent_version_id: Option<AgentVersionId>,
    pub execution_generation: u64,
    pub state: JsonPayload,
    pub state_digest: Digest,
    pub created_event_id: EventId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskResult {
    pub output: JsonPayload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StageMutation {
    pub stage_execution_id: StageExecutionId,
    pub expected_version: u64,
    pub target_status: StageStatus,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewTask {
    pub task_id: TaskId,
    pub stage_execution_id: Option<StageExecutionId>,
    pub logical_key: LogicalKey,
    pub kind: TaskKind,
    pub generation: u64,
    pub based_on_checkpoint_sequence: u64,
    pub priority: i32,
    pub available_at: UnixMicros,
    pub max_attempts: u32,
    pub input: JsonPayload,
    pub deadline: Option<UnixMicros>,
    pub created_event_id: EventId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewWaitSubscription {
    pub wait_id: WaitId,
    pub stage_execution_id: Option<StageExecutionId>,
    pub wait_type: String,
    pub expected_event_type: String,
    pub match_key_hash: Digest,
    pub match_contract: JsonPayload,
    pub expires_at: Option<UnixMicros>,
    pub resume_task: WaitResumeTask,
    pub created_event_id: EventId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WaitResumeTask {
    pub task_id: TaskId,
    pub logical_key: LogicalKey,
    pub kind: TaskKind,
    pub priority: i32,
    pub max_attempts: u32,
    pub input: JsonPayload,
    pub deadline: Option<UnixMicros>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewRetrySchedule {
    pub task: NewTask,
    pub reason_code: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalRunResult {
    pub status: RunStatus,
    pub output: JsonPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewArtifactRef {
    pub artifact_id: ArtifactId,
    pub stage_execution_id: Option<StageExecutionId>,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NextActions {
    Tasks(Vec<NewTask>),
    Wait(NewWaitSubscription),
    Retry(NewRetrySchedule),
    FinishRun(FinalRunResult),
    NoFurtherWork,
}

impl NextActions {
    fn created_event_mismatch(&self, expected: EventId) -> bool {
        match self {
            Self::Tasks(tasks) => tasks.iter().any(|task| task.created_event_id != expected),
            Self::Wait(wait) => wait.created_event_id != expected,
            Self::Retry(retry) => retry.task.created_event_id != expected,
            Self::FinishRun(_) | Self::NoFurtherWork => false,
        }
    }

    fn generation_mismatch(&self, expected: u64) -> bool {
        match self {
            Self::Tasks(tasks) => tasks.iter().any(|task| task.generation != expected),
            Self::Retry(retry) => retry.task.generation != expected,
            Self::Wait(_) | Self::FinishRun(_) | Self::NoFurtherWork => false,
        }
    }

    const fn final_status(&self) -> Option<RunStatus> {
        match self {
            Self::FinishRun(result) => Some(result.status),
            Self::Tasks(_) | Self::Wait(_) | Self::Retry(_) | Self::NoFurtherWork => None,
        }
    }

    fn metadata_error(&self) -> Option<CompletionShapeError> {
        let invalid_task =
            |task: &NewTask| task.based_on_checkpoint_sequence == 0 || task.max_attempts == 0;
        match self {
            Self::Tasks(tasks) if tasks.iter().any(invalid_task) => {
                Some(CompletionShapeError::InvalidTaskMetadata)
            }
            Self::Wait(wait)
                if wait.wait_type.is_empty()
                    || wait.expected_event_type.is_empty()
                    || wait.resume_task.max_attempts == 0 =>
            {
                Some(CompletionShapeError::InvalidWaitMetadata)
            }
            Self::Retry(retry) if retry.reason_code.is_empty() || invalid_task(&retry.task) => {
                Some(CompletionShapeError::InvalidTaskMetadata)
            }
            Self::Tasks(_)
            | Self::Wait(_)
            | Self::Retry(_)
            | Self::FinishRun(_)
            | Self::NoFurtherWork => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionShapeError {
    CreatedEventMismatch,
    InvalidCheckpointMetadata,
    InvalidTaskMetadata,
    InvalidWaitMetadata,
    InvalidStageMetadata,
    InvalidArtifactMetadata,
    GenerationMismatch,
    FinishRunRequiresTerminalStatus,
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
    pub match_key_hash: Digest,
    pub payload_schema_version: u32,
    pub payload: JsonPayload,
    pub signature_verification: SignatureVerification,
    pub occurred_at: Option<UnixMicros>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignatureVerification {
    Verified,
    NotRequired,
    Failed,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_task_completion_shape_is_accepted() {
        assert_eq!(completion().validate_shape(), Ok(()));
    }

    #[test]
    fn completion_rejects_non_terminal_finish_and_event_mismatch() {
        let mut command = completion();
        command.next = NextActions::FinishRun(FinalRunResult {
            status: RunStatus::Running,
            output: json(),
        });
        assert_eq!(
            command.validate_shape(),
            Err(CompletionShapeError::FinishRunRequiresTerminalStatus)
        );

        command.next = NextActions::NoFurtherWork;
        command.checkpoint.created_event_id = EventId::from_bytes([9; 16]);
        assert_eq!(
            command.validate_shape(),
            Err(CompletionShapeError::CreatedEventMismatch)
        );
    }

    #[test]
    fn completion_rejects_empty_wait_contract_identity() {
        let mut command = completion();
        command.next = NextActions::Wait(NewWaitSubscription {
            wait_id: WaitId::from_bytes([8; 16]),
            stage_execution_id: None,
            wait_type: String::new(),
            expected_event_type: "approval.granted".to_owned(),
            match_key_hash: Digest::from_bytes([8; 32]),
            match_contract: json(),
            expires_at: None,
            resume_task: WaitResumeTask {
                task_id: TaskId::from_bytes([10; 16]),
                logical_key: LogicalKey::parse("approval/resume").expect("logical key"),
                kind: TaskKind::Model,
                priority: 0,
                max_attempts: 1,
                input: json(),
                deadline: None,
            },
            created_event_id: command.completion_event_id,
        });

        assert_eq!(
            command.validate_shape(),
            Err(CompletionShapeError::InvalidWaitMetadata)
        );
    }

    fn completion() -> CompleteTask {
        let event_id = EventId::from_bytes([4; 16]);
        CompleteTask {
            expected_run: ExpectedRun {
                run_id: RunId::from_bytes([1; 16]),
                version: Some(2),
                execution_generation: Some(3),
            },
            lease: LeaseProof {
                task_id: TaskId::from_bytes([2; 16]),
                worker_id: WorkerId::from_bytes([3; 16]),
                token: LeaseToken::from_bytes([5; 32]),
                execution_generation: 3,
            },
            completion_event_id: event_id,
            checkpoint: NewCheckpoint {
                checkpoint_id: CheckpointId::from_bytes([6; 16]),
                sequence: 2,
                schema_version: 1,
                workflow_version_id: None,
                coordinator_agent_version_id: None,
                execution_generation: 3,
                state: json(),
                state_digest: Digest::from_bytes([7; 32]),
                created_event_id: event_id,
            },
            task_result: TaskResult { output: json() },
            stage_mutation: None,
            additional_stage_mutations: Vec::new(),
            new_stages: Vec::new(),
            artifacts: Vec::new(),
            next: NextActions::FinishRun(FinalRunResult {
                status: RunStatus::Completed,
                output: json(),
            }),
        }
    }

    fn json() -> JsonPayload {
        JsonPayload::from_validated_bytes(b"{}".to_vec())
    }
}
