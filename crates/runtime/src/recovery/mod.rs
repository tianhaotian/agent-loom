use std::{error::Error, fmt, future::Future, pin::Pin};

use agent_loom_domain::{
    AgentExecutionId, AgentExecutionSnapshot, CommandId, CorrelationId, Digest, DurationMicros,
    EventId, IdempotencyKey, LeaseToken, ScopeKey, TaskKind, TenantId, ToolAttemptId,
    ToolExecutionId, ToolExecutionSnapshot, WorkerId,
};
use agent_loom_durable_store::{
    BeginAgentResubmission, BeginToolRetryAttempt, ClaimTask, ClaimedTask, CommandContext,
    CommandDisposition, Committed, DurableStore, ExpectedRun, LeaseProof, StoreError, StoreFuture,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

/// Minimal Store surface required by a Worker dedicated to external retry Tasks.
pub trait RecoveryWorkerStore: Send + Sync {
    fn claim_task<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ClaimTask,
    ) -> StoreFuture<'a, Option<Committed<ClaimedTask>>>;

    fn begin_tool_retry_attempt<'a>(
        &'a self,
        context: &'a CommandContext,
        command: BeginToolRetryAttempt,
    ) -> StoreFuture<'a, Committed<ToolExecutionSnapshot>>;

    fn begin_agent_resubmission<'a>(
        &'a self,
        context: &'a CommandContext,
        command: BeginAgentResubmission,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>>;
}

impl<T> RecoveryWorkerStore for T
where
    T: DurableStore + ?Sized,
{
    fn claim_task<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ClaimTask,
    ) -> StoreFuture<'a, Option<Committed<ClaimedTask>>> {
        DurableStore::claim_task(self, context, command)
    }

    fn begin_tool_retry_attempt<'a>(
        &'a self,
        context: &'a CommandContext,
        command: BeginToolRetryAttempt,
    ) -> StoreFuture<'a, Committed<ToolExecutionSnapshot>> {
        DurableStore::begin_tool_retry_attempt(self, context, command)
    }

    fn begin_agent_resubmission<'a>(
        &'a self,
        context: &'a CommandContext,
        command: BeginAgentResubmission,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>> {
        DurableStore::begin_agent_resubmission(self, context, command)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryDispatchFence {
    pub expected_run: ExpectedRun,
    pub execution_generation: u64,
    pub correlation_id: CorrelationId,
    pub actor_ref: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartedRecovery {
    Tool {
        execution: ToolExecutionSnapshot,
        disposition: CommandDisposition,
        fence: RecoveryDispatchFence,
    },
    Agent {
        execution: AgentExecutionSnapshot,
        disposition: CommandDisposition,
        fence: RecoveryDispatchFence,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalDispatchError {
    pub safe_message: String,
}

impl fmt::Display for ExternalDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl Error for ExternalDispatchError {}

pub type DispatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), ExternalDispatchError>> + Send + 'a>>;

/// Executes the actual Adapter call after the Store has committed the external
/// retry start intent. Implementations must preserve the Execution's stable
/// idempotency identity and must not treat a duplicate start as a new call.
pub trait ExternalRecoveryDispatcher: Send + Sync {
    fn dispatch(&self, started: StartedRecovery) -> DispatchFuture<'_>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryWorkerConfig {
    pub lease_duration: DurationMicros,
    pub candidate_window: u32,
}

impl Default for RecoveryWorkerConfig {
    fn default() -> Self {
        Self {
            lease_duration: DurationMicros::new(30_000_000),
            candidate_window: 16,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryPollOutcome {
    Idle,
    Dispatched {
        claimed: ClaimedTask,
        started: StartedRecovery,
    },
    DispatchFailed {
        claimed: ClaimedTask,
        started: StartedRecovery,
        error: ExternalDispatchError,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryWorkerError {
    Store(StoreError),
    InvalidTask { safe_message: String },
    InvalidConfig { safe_message: String },
}

impl fmt::Display for RecoveryWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => error.fmt(formatter),
            Self::InvalidTask { safe_message } | Self::InvalidConfig { safe_message } => {
                formatter.write_str(safe_message)
            }
        }
    }
}

impl Error for RecoveryWorkerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::InvalidTask { .. } | Self::InvalidConfig { .. } => None,
        }
    }
}

impl From<StoreError> for RecoveryWorkerError {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}

#[derive(Clone, Debug)]
pub struct RecoveryWorker<S, D> {
    store: S,
    dispatcher: D,
    config: RecoveryWorkerConfig,
}

impl<S, D> RecoveryWorker<S, D>
where
    S: RecoveryWorkerStore,
    D: ExternalRecoveryDispatcher,
{
    pub const fn new(store: S, dispatcher: D, config: RecoveryWorkerConfig) -> Self {
        Self {
            store,
            dispatcher,
            config,
        }
    }

    /// Claims and processes at most one external retry recovery Task.
    ///
    /// The start command commits before `dispatch` is invoked. A dispatcher
    /// failure is returned as an outcome rather than rewriting the committed
    /// intent; stale-execution reconciliation remains authoritative.
    ///
    /// # Errors
    ///
    /// Returns a Store error for claim/start failures, an invalid configuration
    /// error for zero lease settings, or an invalid Task error for malformed
    /// recovery input.
    #[allow(clippy::too_many_lines)]
    pub async fn poll_once(
        &self,
        claim_context: &CommandContext,
        worker_id: WorkerId,
        lease_token: LeaseToken,
    ) -> Result<RecoveryPollOutcome, RecoveryWorkerError> {
        if self.config.lease_duration.get() == 0 || self.config.candidate_window == 0 {
            return Err(RecoveryWorkerError::InvalidConfig {
                safe_message: "Worker lease duration and candidate window must be positive"
                    .to_owned(),
            });
        }
        let claimed = self
            .store
            .claim_task(
                claim_context,
                ClaimTask {
                    worker_id,
                    lease_token: lease_token.clone(),
                    lease_duration: self.config.lease_duration,
                    candidate_window: self.config.candidate_window,
                    kind: Some(TaskKind::Reconcile),
                },
            )
            .await?;
        let Some(claimed) = claimed else {
            return Ok(RecoveryPollOutcome::Idle);
        };
        let claimed = claimed.value;
        if claimed.task.tenant_id != claim_context.tenant_id {
            return Err(invalid_task("claimed Task belongs to another tenant"));
        }
        if claimed.task.kind != TaskKind::Reconcile {
            return Err(invalid_task("Store returned a non-reconcile Task"));
        }
        let input: RecoveryTaskInput = serde_json::from_slice(claimed.task.input.as_bytes())
            .map_err(|_| invalid_task("recovery Task input is malformed"))?;
        if input.expected_revision == 0 {
            return Err(invalid_task("recovery Task revision must be positive"));
        }
        let execution_id = decode_hex_id(&input.execution_id)?;
        let lease = LeaseProof {
            task_id: claimed.task.task_id,
            worker_id,
            token: lease_token,
            execution_generation: claimed.task.generation,
        };
        let expected_run = ExpectedRun {
            run_id: claimed.task.run_id,
            version: Some(claimed.run_version),
            execution_generation: Some(claimed.task.generation),
        };
        let outcome_run_version = claimed
            .run_version
            .checked_add(1)
            .ok_or_else(|| invalid_task("recovery Run version overflow"))?;
        let dispatch_fence = RecoveryDispatchFence {
            expected_run: ExpectedRun {
                run_id: claimed.task.run_id,
                version: Some(outcome_run_version),
                execution_generation: Some(claimed.task.generation),
            },
            execution_generation: claimed.task.generation,
            correlation_id: claim_context.correlation_id,
            actor_ref: claim_context.actor_ref.clone(),
        };
        let identity = format!(
            "recovery-start/{}/{}/{}",
            claimed.task.task_id, claimed.task.attempt, input.expected_revision
        );
        let start_context = start_context(claim_context, claimed.task.tenant_id, &identity)?;
        let started = match input.kind {
            RecoveryTaskKind::ToolRetry => {
                let expected_attempt = u32::try_from(input.expected_revision)
                    .map_err(|_| invalid_task("Tool retry revision exceeds attempt range"))?;
                let committed = self
                    .store
                    .begin_tool_retry_attempt(
                        &start_context,
                        BeginToolRetryAttempt {
                            expected_run,
                            lease,
                            tool_execution_id: ToolExecutionId::from_bytes(execution_id),
                            expected_attempt,
                            tool_attempt_id: ToolAttemptId::from_bytes(derived_id(
                                "tool-attempt",
                                &identity,
                            )),
                            started_event_id: EventId::from_bytes(derived_id("event", &identity)),
                        },
                    )
                    .await?;
                StartedRecovery::Tool {
                    execution: committed.value,
                    disposition: committed.disposition,
                    fence: dispatch_fence,
                }
            }
            RecoveryTaskKind::AgentRetry => {
                let committed = self
                    .store
                    .begin_agent_resubmission(
                        &start_context,
                        BeginAgentResubmission {
                            expected_run,
                            lease,
                            agent_execution_id: AgentExecutionId::from_bytes(execution_id),
                            expected_version: input.expected_revision,
                            started_event_id: EventId::from_bytes(derived_id("event", &identity)),
                        },
                    )
                    .await?;
                StartedRecovery::Agent {
                    execution: committed.value,
                    disposition: committed.disposition,
                    fence: dispatch_fence,
                }
            }
        };
        match self.dispatcher.dispatch(started.clone()).await {
            Ok(()) => Ok(RecoveryPollOutcome::Dispatched { claimed, started }),
            Err(error) => Ok(RecoveryPollOutcome::DispatchFailed {
                claimed,
                started,
                error,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum RecoveryTaskKind {
    ToolRetry,
    AgentRetry,
}

#[derive(Debug, Deserialize)]
struct RecoveryTaskInput {
    #[serde(rename = "due_work_kind")]
    kind: RecoveryTaskKind,
    execution_id: String,
    expected_revision: u64,
}

fn start_context(
    claim: &CommandContext,
    tenant_id: TenantId,
    identity: &str,
) -> Result<CommandContext, RecoveryWorkerError> {
    let scope = ScopeKey::parse("worker.external_recovery")
        .map_err(|_| invalid_task("generated Worker scope is invalid"))?;
    let idempotency_key = IdempotencyKey::parse(identity)
        .map_err(|_| invalid_task("generated Worker idempotency key is invalid"))?;
    Ok(CommandContext {
        tenant_id,
        command_id: CommandId::from_bytes(derived_id("command", identity)),
        correlation_id: claim.correlation_id,
        actor_ref: claim.actor_ref.clone(),
        scope,
        idempotency_key,
        request_hash: hash(&format!("begin/{identity}")),
    })
}

fn hash(value: &str) -> Digest {
    let bytes: [u8; 32] = Sha256::digest(value.as_bytes()).into();
    Digest::from_bytes(bytes)
}

fn derived_id(namespace: &str, identity: &str) -> [u8; 16] {
    let bytes: [u8; 32] = Sha256::digest(format!("{namespace}/{identity}").as_bytes()).into();
    let mut id = [0; 16];
    id.copy_from_slice(&bytes[..16]);
    id
}

fn decode_hex_id(value: &str) -> Result<[u8; 16], RecoveryWorkerError> {
    if value.len() != 32 {
        return Err(invalid_task(
            "recovery execution ID must contain 32 hex digits",
        ));
    }
    let mut bytes = [0; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&value[offset..offset + 2], 16)
            .map_err(|_| invalid_task("recovery execution ID is not hexadecimal"))?;
    }
    Ok(bytes)
}

fn invalid_task(message: &str) -> RecoveryWorkerError {
    RecoveryWorkerError::InvalidTask {
        safe_message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use agent_loom_domain::{
        AgentExecutionStatus, AgentVersionId, CorrelationId, EndpointId, JsonPayload, LogicalKey,
        RunId, TaskId, TaskSnapshot, TaskStatus, ToolExecutionStatus, UnixMicros,
    };
    use agent_loom_durable_store::PostCommitHint;

    use super::*;

    #[derive(Debug)]
    struct FakeStore {
        claimed: Option<ClaimedTask>,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl RecoveryWorkerStore for FakeStore {
        fn claim_task<'a>(
            &'a self,
            _context: &'a CommandContext,
            command: ClaimTask,
        ) -> StoreFuture<'a, Option<Committed<ClaimedTask>>> {
            Box::pin(async move {
                assert_eq!(command.kind, Some(TaskKind::Reconcile));
                self.calls.lock().expect("calls lock").push("claim");
                Ok(self.claimed.clone().map(|value| Committed {
                    disposition: CommandDisposition::Applied,
                    value,
                    event_ids: Vec::new(),
                    durable_follow_ups: Vec::new(),
                    post_commit_hints: Vec::new(),
                }))
            })
        }

        fn begin_tool_retry_attempt<'a>(
            &'a self,
            _context: &'a CommandContext,
            command: BeginToolRetryAttempt,
        ) -> StoreFuture<'a, Committed<ToolExecutionSnapshot>> {
            Box::pin(async move {
                self.calls.lock().expect("calls lock").push("begin");
                Ok(Committed {
                    disposition: CommandDisposition::Applied,
                    value: ToolExecutionSnapshot {
                        tenant_id: self.claimed.as_ref().expect("claimed task").task.tenant_id,
                        tool_execution_id: command.tool_execution_id,
                        run_id: command.expected_run.run_id,
                        stage_execution_id: None,
                        task_id: command.lease.task_id,
                        tool_call_id: "deploy".to_owned(),
                        tool_name: "devops.deploy".to_owned(),
                        status: ToolExecutionStatus::Executing,
                        attempt_count: command.expected_attempt + 1,
                        external_ref: None,
                        recovery_action: None,
                        retry_at: None,
                        updated_at: UnixMicros::new(100),
                    },
                    event_ids: vec![command.started_event_id],
                    durable_follow_ups: Vec::new(),
                    post_commit_hints: vec![PostCommitHint::WakeWorkers],
                })
            })
        }

        fn begin_agent_resubmission<'a>(
            &'a self,
            _context: &'a CommandContext,
            command: BeginAgentResubmission,
        ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>> {
            Box::pin(async move {
                self.calls.lock().expect("calls lock").push("begin");
                Ok(Committed {
                    disposition: CommandDisposition::Applied,
                    value: AgentExecutionSnapshot {
                        tenant_id: self.claimed.as_ref().expect("claimed task").task.tenant_id,
                        agent_execution_id: command.agent_execution_id,
                        run_id: command.expected_run.run_id,
                        stage_execution_id: None,
                        task_id: command.lease.task_id,
                        endpoint_id: EndpointId::from_bytes([10; 16]),
                        agent_version_id: AgentVersionId::from_bytes([11; 16]),
                        status: AgentExecutionStatus::Submitting,
                        version: command.expected_version + 1,
                        remote_run_ref: None,
                        remote_session_ref: None,
                        remote_protocol_version: None,
                        event_cursor: None,
                        cursor_version: 0,
                        retry_at: None,
                        updated_at: UnixMicros::new(100),
                    },
                    event_ids: vec![command.started_event_id],
                    durable_follow_ups: Vec::new(),
                    post_commit_hints: Vec::new(),
                })
            })
        }
    }

    #[derive(Debug)]
    struct FakeDispatcher {
        calls: Arc<Mutex<Vec<&'static str>>>,
        fail: bool,
    }

    impl ExternalRecoveryDispatcher for FakeDispatcher {
        fn dispatch(&self, _started: StartedRecovery) -> DispatchFuture<'_> {
            Box::pin(async move {
                self.calls.lock().expect("calls lock").push("dispatch");
                if self.fail {
                    Err(ExternalDispatchError {
                        safe_message: "adapter unavailable".to_owned(),
                    })
                } else {
                    Ok(())
                }
            })
        }
    }

    fn context() -> CommandContext {
        CommandContext {
            tenant_id: TenantId::from_bytes([1; 16]),
            command_id: CommandId::from_bytes([2; 16]),
            correlation_id: CorrelationId::from_bytes([3; 16]),
            actor_ref: "worker/test".to_owned(),
            scope: ScopeKey::parse("worker.claim").expect("scope"),
            idempotency_key: IdempotencyKey::parse("claim/1").expect("idempotency"),
            request_hash: Digest::from_bytes([4; 32]),
        }
    }

    fn claimed(input: &[u8]) -> ClaimedTask {
        ClaimedTask {
            task: TaskSnapshot {
                tenant_id: TenantId::from_bytes([1; 16]),
                task_id: TaskId::from_bytes([5; 16]),
                run_id: RunId::from_bytes([6; 16]),
                stage_execution_id: None,
                logical_key: LogicalKey::parse("reconcile/tool").expect("logical key"),
                kind: TaskKind::Reconcile,
                status: TaskStatus::Leased,
                generation: 7,
                attempt: 1,
                max_attempts: 3,
                available_at: UnixMicros::new(80),
                input: JsonPayload::from_validated_bytes(input.to_vec()),
            },
            run_version: 9,
            lease_expires_at: UnixMicros::new(200),
        }
    }

    #[tokio::test]
    async fn start_intent_commits_before_external_dispatch() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let task = claimed(
            br#"{"due_work_kind":"tool-retry","execution_id":"08080808080808080808080808080808","expected_revision":2}"#,
        );
        let worker = RecoveryWorker::new(
            FakeStore {
                claimed: Some(task),
                calls: Arc::clone(&calls),
            },
            FakeDispatcher {
                calls: Arc::clone(&calls),
                fail: false,
            },
            RecoveryWorkerConfig::default(),
        );
        let outcome = worker
            .poll_once(
                &context(),
                WorkerId::from_bytes([7; 16]),
                LeaseToken::from_bytes([8; 32]),
            )
            .await
            .expect("worker poll");
        assert!(matches!(
            outcome,
            RecoveryPollOutcome::Dispatched {
                started: StartedRecovery::Tool {
                    fence: RecoveryDispatchFence {
                        expected_run: ExpectedRun {
                            version: Some(10),
                            execution_generation: Some(7),
                            ..
                        },
                        execution_generation: 7,
                        ..
                    },
                    ..
                },
                ..
            }
        ));
        assert_eq!(
            calls.lock().expect("calls lock").as_slice(),
            &["claim", "begin", "dispatch"]
        );
    }

    #[tokio::test]
    async fn dispatch_failure_preserves_started_intent_outcome() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let task = claimed(
            br#"{"due_work_kind":"agent-retry","execution_id":"09090909090909090909090909090909","expected_revision":3}"#,
        );
        let worker = RecoveryWorker::new(
            FakeStore {
                claimed: Some(task),
                calls: Arc::clone(&calls),
            },
            FakeDispatcher { calls, fail: true },
            RecoveryWorkerConfig::default(),
        );
        let outcome = worker
            .poll_once(
                &context(),
                WorkerId::from_bytes([7; 16]),
                LeaseToken::from_bytes([8; 32]),
            )
            .await
            .expect("worker poll");
        assert!(matches!(
            outcome,
            RecoveryPollOutcome::DispatchFailed {
                started: StartedRecovery::Agent { .. },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn malformed_recovery_input_never_starts_external_execution() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let worker = RecoveryWorker::new(
            FakeStore {
                claimed: Some(claimed(br#"{"due_work_kind":"tool-retry"}"#)),
                calls: Arc::clone(&calls),
            },
            FakeDispatcher {
                calls: Arc::clone(&calls),
                fail: false,
            },
            RecoveryWorkerConfig::default(),
        );
        let error = worker
            .poll_once(
                &context(),
                WorkerId::from_bytes([7; 16]),
                LeaseToken::from_bytes([8; 32]),
            )
            .await
            .expect_err("malformed Task must fail");
        assert!(matches!(error, RecoveryWorkerError::InvalidTask { .. }));
        assert_eq!(calls.lock().expect("calls lock").as_slice(), &["claim"]);
    }
}
