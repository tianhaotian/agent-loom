use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use agent_loom_domain::{
    AgentExecutionId, AgentExecutionStatus, AgentVersionId, ArtifactId, CheckpointId,
    DurationMicros, EndpointId, EventId, IdempotencyKey, JsonPayload, LeaseToken, LogicalKey,
    RunStatus, ScopeKey, StageExecutionId, StageStatus, TaskId, TaskKind, TenantId, ToolAttemptId,
    ToolExecutionId, UnixMicros, WaitId, WorkerId,
};
use agent_loom_durable_store::{
    ClaimTask, CompleteTask, DurableStore, ExpectedRun, FinalRunResult, InitialStage, LeaseProof,
    NewArtifactRef, NewCheckpoint, NewTask, NewWaitSubscription, NextActions,
    PrepareAgentExecution, PrepareToolExecution, QueryContext, RecordAgentOutcome, StageMutation,
    StoreError, TaskResult, WaitResumeTask,
};
use agent_loom_runtime::{
    ExternalDispatchError, ExternalRecoveryDispatcher, RecoveryDispatchFence, StartedRecovery,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::{sync::watch, time::sleep};

use crate::{
    execution_plan::stage_execution_id,
    identity::{command_context, derived_id, hash_bytes, now_micros},
    task_handler::{decode_task_input, encode_task_input},
};

const ACTOR: &str = "agent-loom-mock-worker";
const DELIVERY_HANDLER_KEY: &str = "delivery-mvp";
pub(crate) const DELIVERY_STAGES: &[&str] = &[
    "requirements",
    "product_design",
    "technical_design",
    "implementation",
    "self_test",
    "integration_test",
    "deployment",
    "delivery_closure",
];
pub(crate) const DELIVERY_EXECUTION_STAGES: &[&str] = &[
    "requirements",
    "product_design",
    "technical_design",
    "implementation",
    "self_test",
    "integration_test",
    "implementation",
    "self_test",
    "integration_test",
    "deployment",
    "delivery_closure",
];

#[derive(Clone, Debug, Deserialize, Serialize)]
struct MockTaskInput {
    workflow: String,
    step: usize,
    checkpoint_sequence: u64,
    request: Value,
}

#[derive(Debug, Deserialize)]
struct PlannedMockTaskInput {
    workflow: String,
    step: usize,
    checkpoint_sequence: u64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum DeliveryTaskPayload {
    Direct(MockTaskInput),
    Planned {
        task_spec: PlannedMockTaskInput,
        run_input: Value,
    },
}

impl DeliveryTaskPayload {
    fn into_input(self) -> MockTaskInput {
        match self {
            Self::Direct(input) => input,
            Self::Planned {
                task_spec,
                run_input,
            } => MockTaskInput {
                workflow: task_spec.workflow,
                step: task_spec.step,
                checkpoint_sequence: task_spec.checkpoint_sequence,
                request: run_input,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RegisteredTaskHandler {
    DeliveryMvp,
}

impl RegisteredTaskHandler {
    fn resolve(handler_key: &LogicalKey) -> Result<Self, MockWorkerError> {
        match handler_key.as_str() {
            DELIVERY_HANDLER_KEY => Ok(Self::DeliveryMvp),
            _ => Err(MockWorkerError::InvalidTask(
                "Task input references an unregistered handler",
            )),
        }
    }

    const fn supports(self, kind: TaskKind) -> bool {
        match self {
            Self::DeliveryMvp => matches!(kind, TaskKind::AgentServer | TaskKind::Tool),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MockWorkerActivity {
    Idle,
    Completed { task_id: TaskId, terminal: bool },
}

#[derive(Clone, Debug)]
pub struct MockWorkerConfig {
    pub lease_duration: DurationMicros,
    pub candidate_window: u32,
    pub idle_delay: Duration,
    pub error_delay: Duration,
}

impl Default for MockWorkerConfig {
    fn default() -> Self {
        Self {
            lease_duration: DurationMicros::new(30_000_000),
            candidate_window: 16,
            idle_delay: Duration::from_millis(250),
            error_delay: Duration::from_secs(1),
        }
    }
}

pub struct MockWorkflowWorker {
    store: Arc<dyn DurableStore>,
    tenant_id: TenantId,
    worker_id: WorkerId,
    coordinator_agent_version_id: AgentVersionId,
    endpoint_id: EndpointId,
    dispatcher: Arc<dyn ExternalRecoveryDispatcher>,
    sequence: AtomicU64,
    config: MockWorkerConfig,
}

impl fmt::Debug for MockWorkflowWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MockWorkflowWorker")
            .field("tenant_id", &self.tenant_id)
            .field("worker_id", &self.worker_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl MockWorkflowWorker {
    pub fn new(
        store: Arc<dyn DurableStore>,
        tenant_id: TenantId,
        worker_id: WorkerId,
        coordinator_agent_version_id: AgentVersionId,
        endpoint_id: EndpointId,
        dispatcher: Arc<dyn ExternalRecoveryDispatcher>,
        config: MockWorkerConfig,
    ) -> Self {
        Self {
            store,
            tenant_id,
            worker_id,
            coordinator_agent_version_id,
            endpoint_id,
            dispatcher,
            sequence: AtomicU64::new(0),
            config,
        }
    }

    /// Claims and completes at most one deterministic mock delivery stage.
    ///
    /// # Errors
    ///
    /// Returns Store errors and stable validation failures for malformed mock Tasks.
    #[allow(clippy::too_many_lines)]
    pub async fn run_once(&self) -> Result<MockWorkerActivity, MockWorkerError> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let synthetic_run = agent_loom_domain::RunId::from_bytes(derived_id(
            "claim-correlation",
            &self.worker_id.to_string(),
        ));
        let mut supported_kinds = [TaskKind::AgentServer, TaskKind::Tool];
        if sequence % 2 == 1 {
            supported_kinds.rotate_left(1);
        }
        let mut claimed_task = None;
        for kind in supported_kinds {
            let kind_key = match kind {
                TaskKind::AgentServer => "agent-server",
                TaskKind::Tool => "tool",
                _ => unreachable!("worker claim list contains only supported kinds"),
            };
            let claim_identity = format!("mock-claim/{}/{sequence}/{kind_key}", self.worker_id);
            let claim_context = command_context(
                self.tenant_id,
                synthetic_run,
                ACTOR,
                "claim_task",
                &claim_identity,
                claim_identity.as_bytes(),
            )
            .map_err(MockWorkerError::InvalidTask)?;
            let token: [u8; 32] = Sha256::digest(claim_identity.as_bytes()).into();
            if let Some(claimed) = self
                .store
                .claim_task(
                    &claim_context,
                    ClaimTask {
                        worker_id: self.worker_id,
                        lease_token: LeaseToken::from_bytes(token),
                        lease_duration: self.config.lease_duration,
                        candidate_window: self.config.candidate_window,
                        kind: Some(kind),
                    },
                )
                .await?
            {
                claimed_task = Some((claimed.value, token, claim_context));
                break;
            }
        }
        let Some((claimed, token, claim_context)) = claimed_task else {
            return Ok(MockWorkerActivity::Idle);
        };

        let routed = decode_task_input(&claimed.task.input)
            .or_else(|_| legacy_delivery_task_input(&claimed.task.input))
            .map_err(|_| {
                MockWorkerError::InvalidTask("Task input routing envelope is malformed")
            })?;
        let handler = RegisteredTaskHandler::resolve(&routed.handler_key)?;
        if !handler.supports(claimed.task.kind) {
            return Err(MockWorkerError::InvalidTask(
                "Task kind is unsupported by its registered handler",
            ));
        }
        match handler {
            RegisteredTaskHandler::DeliveryMvp => {
                self.run_delivery_handler(&claimed, routed.payload, token, &claim_context)
                    .await
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn run_delivery_handler(
        &self,
        claimed: &agent_loom_durable_store::ClaimedTask,
        routed_payload: Value,
        token: [u8; 32],
        claim_context: &agent_loom_durable_store::CommandContext,
    ) -> Result<MockWorkerActivity, MockWorkerError> {
        let input = serde_json::from_value::<DeliveryTaskPayload>(routed_payload)
            .map(DeliveryTaskPayload::into_input)
            .map_err(|_| MockWorkerError::InvalidTask("delivery Task payload is malformed"))?;
        if input.workflow != "delivery-mvp" || input.step >= DELIVERY_EXECUTION_STAGES.len() {
            return Err(MockWorkerError::InvalidTask(
                "mock Task workflow or step is unsupported",
            ));
        }
        match claimed.task.kind {
            TaskKind::AgentServer => {
                self.prepare_and_dispatch(claimed, &input, token, claim_context)
                    .await?;
            }
            TaskKind::Tool => {
                self.prepare_tool_and_dispatch(claimed, &input, token, claim_context)
                    .await?;
            }
            _ => {
                return Err(MockWorkerError::InvalidTask(
                    "mock delivery Task kind is unsupported",
                ));
            }
        }
        let run = self
            .store
            .get_run(
                &QueryContext {
                    tenant_id: self.tenant_id,
                    actor_ref: ACTOR.to_owned(),
                    authoritative: true,
                },
                claimed.task.run_id,
            )
            .await?
            .ok_or(MockWorkerError::InvalidTask("claimed Run no longer exists"))?;
        let stage_execution_id =
            claimed
                .task
                .stage_execution_id
                .ok_or(MockWorkerError::InvalidTask(
                    "mock Task has no StageExecution",
                ))?;
        let stage = self
            .store
            .list_stages(
                &QueryContext {
                    tenant_id: self.tenant_id,
                    actor_ref: ACTOR.to_owned(),
                    authoritative: true,
                },
                claimed.task.run_id,
            )
            .await?
            .into_iter()
            .find(|stage| stage.stage_execution_id == stage_execution_id)
            .ok_or(MockWorkerError::InvalidTask(
                "mock Task StageExecution no longer exists",
            ))?;
        let completed_sequence = input
            .checkpoint_sequence
            .checked_add(1)
            .ok_or(MockWorkerError::InvalidTask("checkpoint sequence overflow"))?;
        let identity = format!(
            "mock-complete/{}/{}",
            claimed.task.task_id, claimed.task.attempt
        );
        let event_id = EventId::from_bytes(derived_id("event", &identity));
        let state_value = json!({
            "workflow": input.workflow,
            "completed_stage": DELIVERY_EXECUTION_STAGES[input.step],
            "completed_steps": input.step + 1,
            "total_steps": DELIVERY_EXECUTION_STAGES.len(),
            "request": input.request,
        });
        let checkpoint_state = payload(&state_value)?;
        let next_step = input.step + 1;
        let terminal = next_step == DELIVERY_EXECUTION_STAGES.len();
        let next = if terminal {
            NextActions::FinishRun(FinalRunResult {
                status: RunStatus::Completed,
                output: payload(&json!({
                    "workflow": "delivery-mvp",
                    "delivered": true,
                    "stages": DELIVERY_STAGES,
                }))?,
            })
        } else if input.step == 8 {
            let next_input = delivery_task_input(json!({
                "workflow": "delivery-mvp",
                "step": next_step,
                "checkpoint_sequence": completed_sequence,
                "request": input.request,
            }))?;
            NextActions::Wait(NewWaitSubscription {
                wait_id: WaitId::from_bytes(derived_id("wait", &identity)),
                stage_execution_id: Some(execution_stage_id(claimed.task.run_id, next_step)),
                wait_type: "approval".to_owned(),
                expected_event_type: "approval.granted".to_owned(),
                match_key_hash: hash_bytes(b"deployment-approval"),
                match_contract: payload(&json!({
                    "required": ["approved"],
                    "equals": {"approved": true}
                }))?,
                expires_at: run.deadline.or_else(|| {
                    now_micros()
                        .checked_add(5 * 60 * 1_000_000)
                        .map(UnixMicros::new)
                }),
                resume_task: WaitResumeTask {
                    task_id: TaskId::from_bytes(derived_id(
                        "task",
                        &format!("{identity}/{next_step}"),
                    )),
                    logical_key: LogicalKey::parse(format!(
                        "delivery/{}/{}/attempt-{}",
                        claimed.task.run_id,
                        DELIVERY_EXECUTION_STAGES[next_step],
                        execution_attempt(next_step)
                    ))
                    .map_err(|_| {
                        MockWorkerError::InvalidTask("generated resume Task key is invalid")
                    })?,
                    kind: execution_task_kind(next_step),
                    priority: 10,
                    max_attempts: 3,
                    input: next_input,
                    deadline: run.deadline,
                },
                created_event_id: event_id,
            })
        } else {
            let next_input = delivery_task_input(json!({
                "workflow": "delivery-mvp",
                "step": next_step,
                "checkpoint_sequence": completed_sequence,
                "request": input.request,
            }))?;
            NextActions::Tasks(vec![NewTask {
                task_id: TaskId::from_bytes(derived_id("task", &format!("{identity}/{next_step}"))),
                stage_execution_id: Some(execution_stage_id(claimed.task.run_id, next_step)),
                logical_key: LogicalKey::parse(format!(
                    "delivery/{}/{}/attempt-{}",
                    claimed.task.run_id,
                    DELIVERY_EXECUTION_STAGES[next_step],
                    execution_attempt(next_step)
                ))
                .map_err(|_| MockWorkerError::InvalidTask("generated Task key is invalid"))?,
                kind: execution_task_kind(next_step),
                generation: claimed.task.generation,
                based_on_checkpoint_sequence: completed_sequence,
                priority: 10,
                available_at: UnixMicros::new(now_micros()),
                max_attempts: 3,
                input: next_input,
                deadline: run.deadline,
                created_event_id: event_id,
            }])
        };
        let request = serde_json::to_vec(&state_value)
            .map_err(|_| MockWorkerError::InvalidTask("mock completion cannot be encoded"))?;
        let context = command_context(
            self.tenant_id,
            claimed.task.run_id,
            ACTOR,
            "complete_task",
            &identity,
            &request,
        )
        .map_err(MockWorkerError::InvalidTask)?;
        let rework_required = input.step == 5;
        let artifact_status = if rework_required {
            "failed"
        } else {
            "succeeded"
        };
        let artifact_value = json!({
            "stage": DELIVERY_EXECUTION_STAGES[input.step],
            "attempt": execution_attempt(input.step),
            "status": artifact_status,
            "summary": format!(
                "Mock {} {artifact_status} deliverable",
                DELIVERY_EXECUTION_STAGES[input.step]
            ),
        });
        let artifact_bytes = serde_json::to_vec(&artifact_value)
            .map_err(|_| MockWorkerError::InvalidTask("mock Artifact cannot be encoded"))?;
        self.store
            .complete_task(
                &context,
                CompleteTask {
                    expected_run: ExpectedRun {
                        run_id: claimed.task.run_id,
                        version: Some(run.version),
                        execution_generation: Some(claimed.task.generation),
                    },
                    lease: LeaseProof {
                        task_id: claimed.task.task_id,
                        worker_id: self.worker_id,
                        token: LeaseToken::from_bytes(token),
                        execution_generation: claimed.task.generation,
                    },
                    completion_event_id: event_id,
                    checkpoint: NewCheckpoint {
                        checkpoint_id: CheckpointId::from_bytes(derived_id(
                            "checkpoint",
                            &identity,
                        )),
                        sequence: completed_sequence,
                        schema_version: 1,
                        workflow_version_id: run.workflow_version_id,
                        coordinator_agent_version_id: Some(self.coordinator_agent_version_id),
                        execution_generation: claimed.task.generation,
                        state_digest: hash_bytes(checkpoint_state.as_bytes()),
                        state: checkpoint_state,
                        created_event_id: event_id,
                    },
                    task_result: TaskResult {
                        output: payload(&json!({
                            "stage": DELIVERY_EXECUTION_STAGES[input.step],
                            "attempt": execution_attempt(input.step),
                            "status": artifact_status,
                        }))?,
                    },
                    stage_mutation: Some(StageMutation {
                        stage_execution_id,
                        expected_version: stage.version,
                        target_status: if rework_required {
                            StageStatus::ReworkRequired
                        } else {
                            StageStatus::Succeeded
                        },
                    }),
                    additional_stage_mutations: if input.step == 8 {
                        vec![StageMutation {
                            stage_execution_id: execution_stage_id(claimed.task.run_id, next_step),
                            expected_version: 0,
                            target_status: StageStatus::WaitingApproval,
                        }]
                    } else {
                        Vec::new()
                    },
                    new_stages: if rework_required {
                        rework_stages(claimed.task.run_id)?
                    } else {
                        Vec::new()
                    },
                    artifacts: vec![NewArtifactRef {
                        artifact_id: ArtifactId::from_bytes(derived_id("artifact", &identity)),
                        stage_execution_id: Some(stage_execution_id),
                        logical_key: LogicalKey::parse(format!(
                            "delivery/{}/{}",
                            claimed.task.run_id, DELIVERY_EXECUTION_STAGES[input.step]
                        ))
                        .map_err(|_| {
                            MockWorkerError::InvalidTask("generated Artifact key is invalid")
                        })?,
                        kind: if rework_required {
                            "integration_test.defect_report".to_owned()
                        } else {
                            format!("{}.deliverable", DELIVERY_EXECUTION_STAGES[input.step])
                        },
                        contract_version: 1,
                        version: u64::from(execution_attempt(input.step)),
                        uri: format!(
                            "urn:agent-loom:mvp:{}/{}",
                            claimed.task.run_id, DELIVERY_EXECUTION_STAGES[input.step]
                        ),
                        digest: hash_bytes(&artifact_bytes),
                        media_type: "application/json".to_owned(),
                        size_bytes: u64::try_from(artifact_bytes.len()).map_err(|_| {
                            MockWorkerError::InvalidTask("Artifact size exceeds u64")
                        })?,
                        sources: Vec::new(),
                        metadata: JsonPayload::from_validated_bytes(artifact_bytes),
                        produced_by: ACTOR.to_owned(),
                        created_event_id: event_id,
                    }],
                    next,
                },
            )
            .await?;
        Ok(MockWorkerActivity::Completed {
            task_id: claimed.task.task_id,
            terminal,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn prepare_and_dispatch(
        &self,
        claimed: &agent_loom_durable_store::ClaimedTask,
        input: &MockTaskInput,
        token: [u8; 32],
        claim_context: &agent_loom_durable_store::CommandContext,
    ) -> Result<(), MockWorkerError> {
        let identity = format!(
            "mock-agent/{}/{}",
            claimed.task.task_id, claimed.task.attempt
        );
        let execution_id = AgentExecutionId::from_bytes(derived_id("agent-execution", &identity));
        let request = payload(&json!({
            "instructions": format!(
                "Produce the {} delivery artifact",
                DELIVERY_EXECUTION_STAGES[input.step]
            ),
            "input": input.request,
            "budget": {
                "max_duration_micros": 30_000_000_u64,
                "max_output_bytes": 1_048_576_u64
            }
        }))?;
        let prepare_context = command_context(
            self.tenant_id,
            claimed.task.run_id,
            ACTOR,
            "prepare_agent_execution",
            &format!("prepare/{identity}"),
            request.as_bytes(),
        )
        .map_err(MockWorkerError::InvalidTask)?;
        let prepared = self
            .store
            .prepare_agent_execution(
                &prepare_context,
                PrepareAgentExecution {
                    expected_run: ExpectedRun {
                        run_id: claimed.task.run_id,
                        version: Some(claimed.run_version),
                        execution_generation: Some(claimed.task.generation),
                    },
                    lease: LeaseProof {
                        task_id: claimed.task.task_id,
                        worker_id: self.worker_id,
                        token: LeaseToken::from_bytes(token),
                        execution_generation: claimed.task.generation,
                    },
                    agent_execution_id: execution_id,
                    stage_execution_id: claimed.task.stage_execution_id,
                    endpoint_id: self.endpoint_id,
                    agent_version_id: self.coordinator_agent_version_id,
                    idempotency_key: IdempotencyKey::parse(format!("submit/{identity}")).map_err(
                        |_| MockWorkerError::InvalidTask("Agent idempotency is invalid"),
                    )?,
                    request_hash: hash_bytes(request.as_bytes()),
                    request,
                    capabilities_snapshot: payload(&json!({
                        "submission_idempotency": true,
                        "artifact_output": true
                    }))?,
                    prepared_event_id: EventId::from_bytes(derived_id("prepared-event", &identity)),
                },
            )
            .await?;
        let dispatch_run_version = claimed
            .run_version
            .checked_add(1)
            .ok_or(MockWorkerError::InvalidTask("Run version overflow"))?;
        self.dispatcher
            .dispatch(StartedRecovery::Agent {
                execution: prepared.value.clone(),
                disposition: prepared.disposition,
                fence: RecoveryDispatchFence {
                    expected_run: ExpectedRun {
                        run_id: claimed.task.run_id,
                        version: Some(dispatch_run_version),
                        execution_generation: Some(claimed.task.generation),
                    },
                    execution_generation: claimed.task.generation,
                    correlation_id: claim_context.correlation_id,
                    actor_ref: ACTOR.to_owned(),
                },
            })
            .await?;

        let run = self
            .store
            .get_run(
                &QueryContext {
                    tenant_id: self.tenant_id,
                    actor_ref: ACTOR.to_owned(),
                    authoritative: true,
                },
                claimed.task.run_id,
            )
            .await?
            .ok_or(MockWorkerError::InvalidTask("Agent Run disappeared"))?;
        let outcome = payload(&json!({
            "stage": DELIVERY_EXECUTION_STAGES[input.step],
            "status": "succeeded"
        }))?;
        let outcome_identity = format!("outcome/{identity}");
        let outcome_context = command_context(
            self.tenant_id,
            claimed.task.run_id,
            ACTOR,
            "record_agent_outcome",
            &outcome_identity,
            outcome.as_bytes(),
        )
        .map_err(MockWorkerError::InvalidTask)?;
        self.store
            .record_agent_outcome(
                &outcome_context,
                RecordAgentOutcome {
                    expected_run: ExpectedRun {
                        run_id: claimed.task.run_id,
                        version: Some(run.version),
                        execution_generation: Some(claimed.task.generation),
                    },
                    agent_execution_id: execution_id,
                    expected_version: prepared.value.version.saturating_add(1),
                    status: AgentExecutionStatus::Succeeded,
                    result: Some(outcome),
                    error_code: None,
                    outcome_event_id: EventId::from_bytes(derived_id("outcome-event", &identity)),
                },
            )
            .await?;
        Ok(())
    }

    async fn prepare_tool_and_dispatch(
        &self,
        claimed: &agent_loom_durable_store::ClaimedTask,
        input: &MockTaskInput,
        token: [u8; 32],
        claim_context: &agent_loom_durable_store::CommandContext,
    ) -> Result<(), MockWorkerError> {
        let identity = format!(
            "mock-devops/{}/{}",
            claimed.task.task_id, claimed.task.attempt
        );
        let execution_id = ToolExecutionId::from_bytes(derived_id("tool-execution", &identity));
        let request = payload(&json!({
            "operation": "deploy",
            "stage": DELIVERY_EXECUTION_STAGES[input.step],
            "release": input.request,
            "health_check": true
        }))?;
        let context = command_context(
            self.tenant_id,
            claimed.task.run_id,
            ACTOR,
            "prepare_tool_execution",
            &format!("prepare/{identity}"),
            request.as_bytes(),
        )
        .map_err(MockWorkerError::InvalidTask)?;
        let prepared = self
            .store
            .prepare_tool_execution(
                &context,
                PrepareToolExecution {
                    expected_run: ExpectedRun {
                        run_id: claimed.task.run_id,
                        version: Some(claimed.run_version),
                        execution_generation: Some(claimed.task.generation),
                    },
                    lease: LeaseProof {
                        task_id: claimed.task.task_id,
                        worker_id: self.worker_id,
                        token: LeaseToken::from_bytes(token),
                        execution_generation: claimed.task.generation,
                    },
                    tool_execution_id: execution_id,
                    tool_attempt_id: ToolAttemptId::from_bytes(derived_id(
                        "tool-attempt",
                        &identity,
                    )),
                    stage_execution_id: claimed.task.stage_execution_id,
                    tool_call_id: format!("deploy/{}", claimed.task.run_id),
                    tool_name: "devops.deploy".to_owned(),
                    idempotency_scope: ScopeKey::parse("devops.deploy")
                        .map_err(|_| MockWorkerError::InvalidTask("Tool scope is invalid"))?,
                    idempotency_key: IdempotencyKey::parse(format!("execute/{identity}")).map_err(
                        |_| MockWorkerError::InvalidTask("Tool idempotency key is invalid"),
                    )?,
                    request_hash: hash_bytes(request.as_bytes()),
                    request,
                    prepared_event_id: EventId::from_bytes(derived_id("prepared-event", &identity)),
                },
            )
            .await?;
        let dispatch_run_version = claimed
            .run_version
            .checked_add(1)
            .ok_or(MockWorkerError::InvalidTask("Run version overflow"))?;
        self.dispatcher
            .dispatch(StartedRecovery::Tool {
                execution: prepared.value,
                disposition: prepared.disposition,
                fence: RecoveryDispatchFence {
                    expected_run: ExpectedRun {
                        run_id: claimed.task.run_id,
                        version: Some(dispatch_run_version),
                        execution_generation: Some(claimed.task.generation),
                    },
                    execution_generation: claimed.task.generation,
                    correlation_id: claim_context.correlation_id,
                    actor_ref: ACTOR.to_owned(),
                },
            })
            .await?;
        Ok(())
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        while !*shutdown.borrow() {
            let delay = match self.run_once().await {
                Ok(MockWorkerActivity::Completed { .. }) => Duration::from_millis(1),
                Ok(MockWorkerActivity::Idle) => self.config.idle_delay,
                Err(error) => {
                    eprintln!(
                        "{}",
                        json!({
                            "timestamp_micros": now_micros(),
                            "level": "error",
                            "kind": "worker.error",
                            "worker_id": self.worker_id.to_string(),
                            "message": error.to_string(),
                        })
                    );
                    self.config.error_delay
                }
            };
            tokio::select! {
                () = sleep(delay) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum MockWorkerError {
    Store(StoreError),
    Dispatch(ExternalDispatchError),
    InvalidTask(&'static str),
}

impl fmt::Display for MockWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => error.fmt(formatter),
            Self::Dispatch(error) => error.fmt(formatter),
            Self::InvalidTask(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for MockWorkerError {}

impl From<StoreError> for MockWorkerError {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}

impl From<ExternalDispatchError> for MockWorkerError {
    fn from(value: ExternalDispatchError) -> Self {
        Self::Dispatch(value)
    }
}

fn payload(value: &Value) -> Result<JsonPayload, MockWorkerError> {
    serde_json::to_vec(value)
        .map(JsonPayload::from_validated_bytes)
        .map_err(|_| MockWorkerError::InvalidTask("mock payload cannot be encoded"))
}

fn delivery_task_input(value: Value) -> Result<JsonPayload, MockWorkerError> {
    let handler_key = LogicalKey::parse(DELIVERY_HANDLER_KEY)
        .map_err(|_| MockWorkerError::InvalidTask("delivery handler key is invalid"))?;
    encode_task_input(&handler_key, value)
        .map_err(|_| MockWorkerError::InvalidTask("delivery Task input cannot be encoded"))
}

fn legacy_delivery_task_input(
    input: &JsonPayload,
) -> Result<crate::task_handler::RoutedTaskInput, crate::task_handler::TaskInputError> {
    let mut payload: Value = serde_json::from_slice(input.as_bytes())
        .map_err(|_| crate::task_handler::TaskInputError::InvalidEnvelope)?;
    if let Some(resume_input) = payload
        .as_object_mut()
        .and_then(|object| object.remove("resume_input"))
    {
        payload = resume_input;
    }
    serde_json::from_value::<DeliveryTaskPayload>(payload.clone())
        .map_err(|_| crate::task_handler::TaskInputError::InvalidEnvelope)?;
    Ok(crate::task_handler::RoutedTaskInput {
        handler_key: LogicalKey::parse(DELIVERY_HANDLER_KEY)
            .map_err(|_| crate::task_handler::TaskInputError::InvalidHandlerKey)?,
        payload,
    })
}

pub(crate) fn stage_id(run_id: agent_loom_domain::RunId, step: usize) -> StageExecutionId {
    let key = LogicalKey::parse(DELIVERY_STAGES[step]).expect("delivery Stage key is valid");
    stage_execution_id(run_id, &key)
}

fn execution_stage_id(run_id: agent_loom_domain::RunId, execution_step: usize) -> StageExecutionId {
    match execution_step {
        0..=5 => stage_id(run_id, execution_step),
        6..=8 => StageExecutionId::from_bytes(derived_id(
            "stage",
            &format!(
                "{run_id}/{}/{}",
                DELIVERY_EXECUTION_STAGES[execution_step],
                execution_attempt(execution_step)
            ),
        )),
        9 => stage_id(run_id, 6),
        10 => stage_id(run_id, 7),
        _ => unreachable!("execution step is validated before Stage resolution"),
    }
}

const fn execution_attempt(execution_step: usize) -> u32 {
    if execution_step >= 6 && execution_step <= 8 {
        2
    } else {
        1
    }
}

const fn execution_task_kind(execution_step: usize) -> TaskKind {
    if execution_step == 9 {
        TaskKind::Tool
    } else {
        TaskKind::AgentServer
    }
}

fn rework_stages(run_id: agent_loom_domain::RunId) -> Result<Vec<InitialStage>, MockWorkerError> {
    (6..=8)
        .map(|execution_step| {
            let stage = DELIVERY_EXECUTION_STAGES[execution_step];
            Ok(InitialStage {
                stage_execution_id: execution_stage_id(run_id, execution_step),
                stage_key: LogicalKey::parse(format!("delivery/{stage}"))
                    .map_err(|_| MockWorkerError::InvalidTask("rework Stage key is invalid"))?,
                definition_stage_key: LogicalKey::parse(stage.to_owned()).map_err(|_| {
                    MockWorkerError::InvalidTask("rework definition Stage key is invalid")
                })?,
                status: if execution_step == 6 {
                    StageStatus::Active
                } else {
                    StageStatus::Planned
                },
                attempt: 2,
                assignee_kind: Some("agent".to_owned()),
                assignee_ref: Some("mock-delivery-agent".to_owned()),
                input_contract: payload(&json!({"type": "object"}))?,
                output_contract: payload(&json!({
                    "type": "object",
                    "required": ["stage", "attempt", "status"]
                }))?,
                policy: payload(&json!({
                    "max_attempts": 3,
                    "reason": "integration_test_blocker"
                }))?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planned_task_envelope_starts_the_delivery_workflow() {
        let payload = delivery_task_input(json!({
            "task_spec": {
                "workflow": "delivery-mvp",
                "step": 0,
                "checkpoint_sequence": 1
            },
            "run_input": {"goal": "ship"}
        }))
        .expect("payload");
        let routed = decode_task_input(&payload).expect("route");
        assert_eq!(routed.handler_key.as_str(), DELIVERY_HANDLER_KEY);
        let input = serde_json::from_value::<DeliveryTaskPayload>(routed.payload)
            .expect("delivery payload")
            .into_input();
        assert_eq!(input.workflow, "delivery-mvp");
        assert_eq!(input.step, 0);
        assert_eq!(input.request["goal"], "ship");
        assert_eq!(DELIVERY_STAGES.len(), 8);
    }

    #[test]
    fn legacy_delivery_payload_is_routed_for_in_flight_run_compatibility() {
        let legacy = payload(&json!({
            "workflow": "delivery-mvp",
            "step": 4,
            "checkpoint_sequence": 5,
            "request": {"goal": "ship"}
        }))
        .expect("legacy payload");
        let routed = legacy_delivery_task_input(&legacy).expect("legacy route");

        assert_eq!(routed.handler_key.as_str(), DELIVERY_HANDLER_KEY);
        let input = serde_json::from_value::<DeliveryTaskPayload>(routed.payload)
            .expect("delivery payload")
            .into_input();
        assert_eq!(input.step, 4);
    }
}
