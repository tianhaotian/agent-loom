use std::sync::{Arc, Mutex};

use agent_loom_adapter_core::{
    AdapterFuture, AgentCapabilities, CompensationOutcome, CompensationRequest, EventReadLimits,
    NormalizedAgentEvent, RemoteAgentRef, RemoteAgentSnapshot, RemoteEventBatch, ResolvedAuth,
    SideEffectClass, StopRequestOutcome, ToolCapabilities, ToolDescriptor, ToolQueryOutcome,
    TraceContext,
};
use agent_loom_domain::{
    AgentExecutionSnapshot, AgentExecutionStatus, CorrelationId, DurationMicros, RunId, TaskId,
    ToolExecutionSnapshot, ToolExecutionStatus,
};
use agent_loom_durable_store::{CommandDisposition, Committed, ExpectedRun};

use super::*;

type SeenToolCall = Arc<Mutex<Option<(ExecutionId, Vec<u8>)>>>;
type SeenStopCalls = Arc<Mutex<Vec<(IdempotencyKey, Digest, RemoteAgentRef, String)>>>;

#[derive(Debug)]
struct FakeStoreState {
    tool_invocation: ToolInvocation,
    agent_invocation: AgentInvocation,
    tool_outcome: Mutex<Option<RecordToolOutcome>>,
    agent_outcome: Mutex<Option<RecordAgentSubmission>>,
    agent_stop_outcome: Mutex<Option<RecordAgentOutcome>>,
    agent_event_batch: Mutex<Option<AppendAgentEvents>>,
}

#[derive(Clone, Debug)]
struct FakeStore(Arc<FakeStoreState>);

impl AdapterDispatchStore for FakeStore {
    fn get_tool_invocation<'a>(
        &'a self,
        context: &'a QueryContext,
        execution_id: ToolExecutionId,
    ) -> StoreFuture<'a, Option<ToolInvocation>> {
        Box::pin(async move {
            assert_eq!(context.tenant_id, self.0.tool_invocation.tenant_id);
            assert!(context.authoritative);
            Ok((execution_id == self.0.tool_invocation.tool_execution_id)
                .then(|| self.0.tool_invocation.clone()))
        })
    }

    fn get_agent_invocation<'a>(
        &'a self,
        context: &'a QueryContext,
        execution_id: AgentExecutionId,
    ) -> StoreFuture<'a, Option<AgentInvocation>> {
        Box::pin(async move {
            assert_eq!(context.tenant_id, self.0.agent_invocation.tenant_id);
            assert!(context.authoritative);
            Ok((execution_id == self.0.agent_invocation.agent_execution_id)
                .then(|| self.0.agent_invocation.clone()))
        })
    }

    fn record_tool_outcome<'a>(
        &'a self,
        _context: &'a CommandContext,
        command: RecordToolOutcome,
    ) -> StoreFuture<'a, Committed<ToolExecutionSnapshot>> {
        Box::pin(async move {
            *self.0.tool_outcome.lock().expect("tool outcome lock") = Some(command.clone());
            Ok(Committed {
                disposition: CommandDisposition::Applied,
                value: tool_snapshot(),
                event_ids: vec![command.outcome_event_id],
                durable_follow_ups: Vec::new(),
                post_commit_hints: Vec::new(),
            })
        })
    }

    fn record_agent_submission<'a>(
        &'a self,
        _context: &'a CommandContext,
        command: RecordAgentSubmission,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>> {
        Box::pin(async move {
            *self.0.agent_outcome.lock().expect("agent outcome lock") = Some(command.clone());
            Ok(Committed {
                disposition: CommandDisposition::Applied,
                value: agent_snapshot(),
                event_ids: vec![command.submission_event_id],
                durable_follow_ups: Vec::new(),
                post_commit_hints: Vec::new(),
            })
        })
    }

    fn record_agent_outcome<'a>(
        &'a self,
        _context: &'a CommandContext,
        command: RecordAgentOutcome,
    ) -> StoreFuture<'a, Committed<AgentExecutionSnapshot>> {
        Box::pin(async move {
            *self
                .0
                .agent_stop_outcome
                .lock()
                .expect("agent stop outcome lock") = Some(command.clone());
            let mut snapshot = agent_snapshot();
            snapshot.status = command.status;
            snapshot.version = command.expected_version + 1;
            Ok(Committed {
                disposition: CommandDisposition::Applied,
                value: snapshot,
                event_ids: vec![command.outcome_event_id],
                durable_follow_ups: Vec::new(),
                post_commit_hints: Vec::new(),
            })
        })
    }

    fn append_agent_events<'a>(
        &'a self,
        _context: &'a CommandContext,
        command: AppendAgentEvents,
    ) -> StoreFuture<'a, Committed<agent_loom_durable_store::AgentEventBatchOutcome>> {
        Box::pin(async move {
            *self
                .0
                .agent_event_batch
                .lock()
                .expect("agent event batch lock") = Some(command.clone());
            Ok(Committed {
                disposition: CommandDisposition::Applied,
                value: agent_loom_durable_store::AgentEventBatchOutcome {
                    tenant_id: tenant_id(),
                    agent_execution_id: command.agent_execution_id,
                    run_id: command.expected_run.run_id,
                    accepted_receipts: command
                        .events
                        .iter()
                        .map(|event| event.receipt_id)
                        .collect(),
                    duplicate_receipts: Vec::new(),
                    cursor_version: command.expected_cursor_version + 1,
                    run_status: agent_loom_domain::RunStatus::Running,
                },
                event_ids: command
                    .events
                    .iter()
                    .filter_map(|event| event.local_event_id)
                    .collect(),
                durable_follow_ups: Vec::new(),
                post_commit_hints: Vec::new(),
            })
        })
    }
}

#[derive(Debug)]
struct FakeToolAdapter {
    descriptor: ToolDescriptor,
    seen: SeenToolCall,
    fail: bool,
}

impl ToolAdapter for FakeToolAdapter {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn execute<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        request: ToolRequest,
    ) -> AdapterFuture<'a, ToolCallOutcome> {
        Box::pin(async move {
            *self.seen.lock().expect("tool call lock") =
                Some((context.execution_id, request.input.as_bytes().to_vec()));
            if self.fail {
                Err(AdapterError {
                    code: "RATE_LIMITED",
                    retry: AdapterRetryClass::SameRequestBackoff,
                    safe_message: "Tool is rate limited".to_owned(),
                    remote_request_id: Some("request-42".to_owned()),
                    retry_after: Some(DurationMicros::new(5_000_000)),
                })
            } else {
                Ok(ToolCallOutcome::Completed(
                    JsonPayload::from_validated_bytes(br#"{"deployed":true}"#.to_vec()),
                ))
            }
        })
    }

    fn query_outcome<'a>(
        &'a self,
        _context: &'a AdapterCallContext,
        _external_ref: &'a str,
    ) -> AdapterFuture<'a, ToolQueryOutcome> {
        Box::pin(async { Ok(ToolQueryOutcome::Pending) })
    }

    fn compensate<'a>(
        &'a self,
        _context: &'a AdapterCallContext,
        _request: CompensationRequest,
    ) -> AdapterFuture<'a, CompensationOutcome> {
        Box::pin(async { Ok(CompensationOutcome::Uncertain) })
    }
}

#[derive(Debug)]
struct FakeAgentAdapter {
    seen: Arc<Mutex<Option<AgentRunRequest>>>,
    stop_calls: SeenStopCalls,
    stop_outcome: StopRequestOutcome,
    status_snapshot: Option<RemoteAgentSnapshot>,
    event_batch: Option<RemoteEventBatch>,
}

impl AgentServerAdapter for FakeAgentAdapter {
    fn kind(&self) -> &'static str {
        "fake-agent-server"
    }

    fn capabilities(&self) -> AgentCapabilities {
        AgentCapabilities {
            submission_idempotency: true,
            submission_reconciliation: true,
            status_query: true,
            resumable_events: true,
            cooperative_stop: true,
            approvals: false,
            guidance: false,
            artifact_output: true,
        }
    }

    fn submit<'a>(
        &'a self,
        _context: &'a AdapterCallContext,
        request: AgentRunRequest,
    ) -> AdapterFuture<'a, SubmitAgentOutcome> {
        Box::pin(async move {
            *self.seen.lock().expect("agent call lock") = Some(request);
            Ok(SubmitAgentOutcome::Accepted(RemoteAgentRef {
                remote_run_id: "remote-run-1".to_owned(),
                remote_session_id: Some("session-1".to_owned()),
                protocol_version: "test-v1".to_owned(),
            }))
        })
    }

    fn reconcile_submission<'a>(
        &'a self,
        _context: &'a AdapterCallContext,
    ) -> AdapterFuture<'a, Option<RemoteAgentRef>> {
        let remote = self
            .status_snapshot
            .as_ref()
            .map(|snapshot| snapshot.remote.clone());
        Box::pin(async move { Ok(remote) })
    }

    fn get_status<'a>(
        &'a self,
        _context: &'a AdapterCallContext,
        _remote: &'a RemoteAgentRef,
    ) -> AdapterFuture<'a, RemoteAgentSnapshot> {
        let snapshot = self.status_snapshot.clone();
        Box::pin(async move { Ok(snapshot.expect("status is configured for this test")) })
    }

    fn read_events<'a>(
        &'a self,
        _context: &'a AdapterCallContext,
        _remote: &'a RemoteAgentRef,
        _cursor: Option<&'a str>,
        _limits: EventReadLimits,
    ) -> AdapterFuture<'a, RemoteEventBatch> {
        let batch = self.event_batch.clone();
        Box::pin(async move { Ok(batch.expect("events are configured for this test")) })
    }

    fn request_stop<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        remote: &'a RemoteAgentRef,
        reason: &'a str,
    ) -> AdapterFuture<'a, StopRequestOutcome> {
        Box::pin(async move {
            self.stop_calls.lock().expect("stop calls lock").push((
                context.idempotency_key.clone(),
                context.request_hash,
                remote.clone(),
                reason.to_owned(),
            ));
            Ok(self.stop_outcome)
        })
    }
}

struct FakeRegistry {
    tool: Arc<dyn ToolAdapter>,
    agent: Arc<dyn AgentServerAdapter>,
}

impl AdapterRegistry for FakeRegistry {
    fn tool(&self, tool_name: &str) -> Option<Arc<dyn ToolAdapter>> {
        (tool_name == "devops.deploy").then(|| Arc::clone(&self.tool))
    }

    fn agent(
        &self,
        endpoint_id: EndpointId,
        agent_version_id: AgentVersionId,
    ) -> Option<Arc<dyn AgentServerAdapter>> {
        (endpoint_id == endpoint_id_value() && agent_version_id == agent_version_id_value())
            .then(|| Arc::clone(&self.agent))
    }
}

#[derive(Debug)]
struct FakeContextFactory;

impl AdapterContextFactory for FakeContextFactory {
    fn create(&self, seed: AdapterContextSeed) -> AdapterContextFuture<'_> {
        Box::pin(async move {
            Ok(AdapterCallContext {
                tenant_id: seed.tenant_id,
                execution_id: seed.execution_id,
                correlation_id: seed.correlation_id,
                causation_id: None,
                idempotency_key: seed.idempotency_key,
                request_hash: seed.request_hash,
                deadline: UnixMicros::new(10_000),
                trace_context: TraceContext {
                    trace_parent: "00-test".to_owned(),
                    trace_state: None,
                },
                auth: ResolvedAuth::new("bearer", "secret"),
            })
        })
    }
}

#[derive(Debug)]
struct FakeRetrySchedule;

impl AdapterRetrySchedule for FakeRetrySchedule {
    fn retry_at(
        &self,
        _error: &AdapterError,
        attempt: u64,
    ) -> Result<UnixMicros, ExternalDispatchError> {
        Ok(UnixMicros::new(
            20_000 + i64::try_from(attempt).expect("attempt range"),
        ))
    }

    fn status_poll_at(&self, observation: u64) -> Result<UnixMicros, ExternalDispatchError> {
        Ok(UnixMicros::new(
            30_000 + i64::try_from(observation).expect("observation range"),
        ))
    }
}

fn dispatcher(
    store: FakeStore,
    tool: Arc<dyn ToolAdapter>,
    agent: Arc<dyn AgentServerAdapter>,
) -> AdapterRecoveryDispatcher<FakeStore> {
    AdapterRecoveryDispatcher::new(
        store,
        Arc::new(FakeRegistry { tool, agent }),
        Arc::new(FakeContextFactory),
        Arc::new(FakeRetrySchedule),
    )
}

fn state() -> Arc<FakeStoreState> {
    Arc::new(FakeStoreState {
        tool_invocation: ToolInvocation {
            tenant_id: tenant_id(),
            tool_execution_id: tool_execution_id(),
            run_id: run_id(),
            tool_name: "devops.deploy".to_owned(),
            idempotency_scope: ScopeKey::parse("deploy.environment").expect("scope"),
            idempotency_key: IdempotencyKey::parse("release/prod/v1").expect("key"),
            request_hash: Digest::from_bytes([31; 32]),
            request: JsonPayload::from_validated_bytes(br#"{"release":"v1"}"#.to_vec()),
        },
        agent_invocation: AgentInvocation {
            tenant_id: tenant_id(),
            agent_execution_id: agent_execution_id(),
            run_id: run_id(),
            endpoint_id: endpoint_id_value(),
            agent_version_id: agent_version_id_value(),
            idempotency_key: IdempotencyKey::parse("agent/requirements/1").expect("key"),
            request_hash: Digest::from_bytes([32; 32]),
            request: JsonPayload::from_validated_bytes(
                br#"{"instructions":"analyze requirement","input":{"story":"checkout"},"budget":{"max_duration_micros":30000000,"max_output_bytes":4096}}"#.to_vec(),
            ),
            capabilities_snapshot: JsonPayload::from_validated_bytes(
                br#"{"submission_idempotency":true}"#.to_vec(),
            ),
        },
        tool_outcome: Mutex::new(None),
        agent_outcome: Mutex::new(None),
        agent_stop_outcome: Mutex::new(None),
        agent_event_batch: Mutex::new(None),
    })
}

fn tool_snapshot() -> ToolExecutionSnapshot {
    ToolExecutionSnapshot {
        tenant_id: tenant_id(),
        tool_execution_id: tool_execution_id(),
        run_id: run_id(),
        stage_execution_id: None,
        task_id: task_id(),
        tool_call_id: "deploy".to_owned(),
        tool_name: "devops.deploy".to_owned(),
        status: ToolExecutionStatus::Executing,
        attempt_count: 3,
        external_ref: None,
        recovery_action: None,
        retry_at: None,
        updated_at: UnixMicros::new(100),
    }
}

fn agent_snapshot() -> AgentExecutionSnapshot {
    AgentExecutionSnapshot {
        tenant_id: tenant_id(),
        agent_execution_id: agent_execution_id(),
        run_id: run_id(),
        stage_execution_id: None,
        task_id: task_id(),
        endpoint_id: endpoint_id_value(),
        agent_version_id: agent_version_id_value(),
        status: AgentExecutionStatus::Submitting,
        version: 4,
        remote_run_ref: None,
        remote_session_ref: None,
        remote_protocol_version: None,
        status_poll_at: None,
        event_cursor: None,
        cursor_version: 0,
        retry_at: None,
        updated_at: UnixMicros::new(100),
    }
}

fn fence() -> RecoveryDispatchFence {
    RecoveryDispatchFence {
        expected_run: agent_loom_durable_store::ExpectedRun {
            run_id: run_id(),
            version: Some(11),
            execution_generation: Some(7),
        },
        execution_generation: 7,
        correlation_id: CorrelationId::from_bytes([8; 16]),
        actor_ref: "worker/test".to_owned(),
    }
}

fn registry_tool(seen: SeenToolCall, fail: bool) -> Arc<dyn ToolAdapter> {
    Arc::new(FakeToolAdapter {
        descriptor: ToolDescriptor {
            tool_key: "devops.deploy".to_owned(),
            side_effect: SideEffectClass::IdempotentWrite,
            capabilities: ToolCapabilities {
                query_outcome: true,
                compensation: false,
                asynchronous_result: false,
            },
        },
        seen,
        fail,
    })
}

fn registry_agent(seen: Arc<Mutex<Option<AgentRunRequest>>>) -> Arc<dyn AgentServerAdapter> {
    Arc::new(FakeAgentAdapter {
        seen,
        stop_calls: Arc::new(Mutex::new(Vec::new())),
        stop_outcome: StopRequestOutcome::Unsupported,
        status_snapshot: None,
        event_batch: None,
    })
}

#[test]
fn static_registry_rejects_duplicate_bindings() {
    let tool = registry_tool(Arc::new(Mutex::new(None)), false);
    let agent = registry_agent(Arc::new(Mutex::new(None)));
    let mut registry = StaticAdapterRegistry::new();

    registry
        .register_tool(Arc::clone(&tool))
        .expect("first Tool registration");
    assert!(registry.register_tool(tool).is_err());
    registry
        .register_agent(
            endpoint_id_value(),
            agent_version_id_value(),
            Arc::clone(&agent),
        )
        .expect("first Agent registration");
    assert!(
        registry
            .register_agent(endpoint_id_value(), agent_version_id_value(), agent)
            .is_err()
    );
}

#[tokio::test]
async fn tool_dispatch_loads_stable_envelope_and_records_completed_outcome() {
    let state = state();
    let seen = Arc::new(Mutex::new(None));
    let dispatcher = dispatcher(
        FakeStore(Arc::clone(&state)),
        registry_tool(Arc::clone(&seen), false),
        registry_agent(Arc::new(Mutex::new(None))),
    );

    crate::ExternalRecoveryDispatcher::dispatch(
        &dispatcher,
        StartedRecovery::Tool {
            execution: tool_snapshot(),
            disposition: CommandDisposition::Applied,
            fence: fence(),
        },
    )
    .await
    .expect("Tool dispatch");

    let seen = seen.lock().expect("tool call lock").clone().expect("call");
    assert_eq!(seen.0, ExecutionId::Tool(tool_execution_id()));
    assert_eq!(seen.1, br#"{"release":"v1"}"#);
    let recorded = state
        .tool_outcome
        .lock()
        .expect("tool outcome lock")
        .clone()
        .expect("recorded outcome");
    assert!(matches!(
        recorded.outcome,
        ToolRecordedOutcome::Completed { .. }
    ));
    assert_eq!(recorded.expected_attempt, 3);
    assert_eq!(recorded.execution_generation, 7);
    assert!(recorded.response_digest.is_some());
}

#[tokio::test]
async fn retryable_adapter_error_is_converted_to_durable_backoff() {
    let state = state();
    let dispatcher = dispatcher(
        FakeStore(Arc::clone(&state)),
        registry_tool(Arc::new(Mutex::new(None)), true),
        registry_agent(Arc::new(Mutex::new(None))),
    );

    crate::ExternalRecoveryDispatcher::dispatch(
        &dispatcher,
        StartedRecovery::Tool {
            execution: tool_snapshot(),
            disposition: CommandDisposition::Applied,
            fence: fence(),
        },
    )
    .await
    .expect("Tool error recording");

    let recorded = state
        .tool_outcome
        .lock()
        .expect("tool outcome lock")
        .clone()
        .expect("recorded outcome");
    assert!(matches!(
        recorded.outcome,
        ToolRecordedOutcome::Failed {
            retry: ExecutionRetryClass::SameRequestBackoff,
            retry_at: Some(value),
            ..
        } if value == UnixMicros::new(20_003)
    ));
    assert_eq!(recorded.remote_request_id.as_deref(), Some("request-42"));
}

#[tokio::test]
async fn agent_dispatch_decodes_request_and_records_remote_identity() {
    let state = state();
    let seen = Arc::new(Mutex::new(None));
    let dispatcher = dispatcher(
        FakeStore(Arc::clone(&state)),
        registry_tool(Arc::new(Mutex::new(None)), false),
        registry_agent(Arc::clone(&seen)),
    );

    crate::ExternalRecoveryDispatcher::dispatch(
        &dispatcher,
        StartedRecovery::Agent {
            execution: agent_snapshot(),
            disposition: CommandDisposition::Applied,
            fence: fence(),
        },
    )
    .await
    .expect("Agent dispatch");

    let request = seen.lock().expect("agent call lock").clone().expect("call");
    assert_eq!(request.instructions, "analyze requirement");
    assert_eq!(request.budget.max_duration, DurationMicros::new(30_000_000));
    let recorded = state
        .agent_outcome
        .lock()
        .expect("agent outcome lock")
        .clone()
        .expect("recorded outcome");
    assert!(matches!(
        recorded.outcome,
        AgentSubmissionOutcome::Accepted {
            ref remote_run_ref,
            ref remote_session_ref,
            ref remote_protocol_version,
        } if remote_run_ref == "remote-run-1"
            && remote_session_ref.as_deref() == Some("session-1")
            && remote_protocol_version == "test-v1"
    ));
    assert_eq!(recorded.expected_version, 4);
}

#[tokio::test]
async fn uncertain_agent_submission_is_reconciled_before_any_resubmission() {
    let state = state();
    let submitted = Arc::new(Mutex::new(None));
    let remote = RemoteAgentRef {
        remote_run_id: "reconciled-run-1".to_owned(),
        remote_session_id: Some("reconciled-session-1".to_owned()),
        protocol_version: "test-v1".to_owned(),
    };
    let agent: Arc<dyn AgentServerAdapter> = Arc::new(FakeAgentAdapter {
        seen: Arc::clone(&submitted),
        stop_calls: Arc::new(Mutex::new(Vec::new())),
        stop_outcome: StopRequestOutcome::Unsupported,
        status_snapshot: Some(RemoteAgentSnapshot {
            remote: remote.clone(),
            status: RemoteAgentStatus::Running,
            result: None,
        }),
        event_batch: None,
    });
    let dispatcher = dispatcher(
        FakeStore(Arc::clone(&state)),
        registry_tool(Arc::new(Mutex::new(None)), false),
        agent,
    );
    let mut execution = agent_snapshot();
    execution.status = AgentExecutionStatus::OutcomeUnknown;
    execution.version = 5;

    crate::ExternalRecoveryDispatcher::dispatch(
        &dispatcher,
        StartedRecovery::Agent {
            execution,
            disposition: CommandDisposition::Applied,
            fence: fence(),
        },
    )
    .await
    .expect("submission reconciliation");

    assert!(submitted.lock().expect("submission lock").is_none());
    let recorded = state
        .agent_outcome
        .lock()
        .expect("agent outcome lock")
        .clone()
        .expect("recorded reconciliation");
    assert_eq!(recorded.expected_version, 5);
    assert!(matches!(
        recorded.outcome,
        AgentSubmissionOutcome::Accepted {
            ref remote_run_ref,
            ..
        } if remote_run_ref == "reconciled-run-1"
    ));
}

#[tokio::test]
async fn uncertain_agent_submission_is_resubmitted_only_after_reconciliation_misses() {
    let state = state();
    let submitted = Arc::new(Mutex::new(None));
    let dispatcher = dispatcher(
        FakeStore(Arc::clone(&state)),
        registry_tool(Arc::new(Mutex::new(None)), false),
        registry_agent(Arc::clone(&submitted)),
    );
    let mut execution = agent_snapshot();
    execution.status = AgentExecutionStatus::OutcomeUnknown;
    execution.version = 5;

    crate::ExternalRecoveryDispatcher::dispatch(
        &dispatcher,
        StartedRecovery::Agent {
            execution,
            disposition: CommandDisposition::Applied,
            fence: fence(),
        },
    )
    .await
    .expect("submission reconciliation and safe resubmission");

    assert!(submitted.lock().expect("submission lock").is_some());
    let recorded = state
        .agent_outcome
        .lock()
        .expect("agent outcome lock")
        .clone()
        .expect("recorded resubmission");
    assert!(matches!(
        recorded.outcome,
        AgentSubmissionOutcome::Accepted {
            ref remote_run_ref,
            ..
        } if remote_run_ref == "remote-run-1"
    ));
}

#[tokio::test]
async fn agent_stop_uses_stable_identity_and_records_reconciliation() {
    let state = state();
    let stop_calls = Arc::new(Mutex::new(Vec::new()));
    let agent: Arc<dyn AgentServerAdapter> = Arc::new(FakeAgentAdapter {
        seen: Arc::new(Mutex::new(None)),
        stop_calls: Arc::clone(&stop_calls),
        stop_outcome: StopRequestOutcome::Accepted { cooperative: true },
        status_snapshot: None,
        event_batch: None,
    });
    let dispatcher = dispatcher(
        FakeStore(Arc::clone(&state)),
        registry_tool(Arc::new(Mutex::new(None)), false),
        agent,
    );
    let mut execution = agent_snapshot();
    execution.status = AgentExecutionStatus::Stopping;
    execution.version = 5;
    execution.remote_run_ref = Some("remote-run-1".to_owned());
    execution.remote_session_ref = Some("session-1".to_owned());
    execution.remote_protocol_version = Some("test-v1".to_owned());
    let candidate = AgentStopCandidate {
        tenant_id: tenant_id(),
        execution,
        expected_run: ExpectedRun {
            run_id: run_id(),
            version: Some(8),
            execution_generation: Some(7),
        },
    };

    crate::AgentStopDispatcher::request_stop(&dispatcher, candidate.clone())
        .await
        .expect("first stop dispatch");
    crate::AgentStopDispatcher::request_stop(&dispatcher, candidate)
        .await
        .expect("duplicate stop dispatch");

    let calls = stop_calls.lock().expect("stop calls lock");
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].0, calls[1].0);
    assert_eq!(calls[0].1, calls[1].1);
    assert_eq!(calls[0].2.protocol_version, "test-v1");
    assert_eq!(calls[0].3, "run control requested");
    let recorded = state
        .agent_stop_outcome
        .lock()
        .expect("agent stop outcome lock")
        .clone()
        .expect("recorded stop outcome");
    assert_eq!(recorded.status, AgentExecutionStatus::Reconciling);
    assert_eq!(recorded.expected_version, 5);
    assert_eq!(recorded.next_status_poll_at, Some(UnixMicros::new(30_005)));
}

#[tokio::test]
async fn agent_status_query_records_the_authoritative_remote_result() {
    let state = state();
    let remote = RemoteAgentRef {
        remote_run_id: "remote-run-1".to_owned(),
        remote_session_id: Some("session-1".to_owned()),
        protocol_version: "test-v1".to_owned(),
    };
    let result = JsonPayload::from_validated_bytes(br#"{"answer":42}"#.to_vec());
    let agent: Arc<dyn AgentServerAdapter> = Arc::new(FakeAgentAdapter {
        seen: Arc::new(Mutex::new(None)),
        stop_calls: Arc::new(Mutex::new(Vec::new())),
        stop_outcome: StopRequestOutcome::Unsupported,
        status_snapshot: Some(RemoteAgentSnapshot {
            remote: remote.clone(),
            status: RemoteAgentStatus::Completed,
            result: Some(result.clone()),
        }),
        event_batch: None,
    });
    let dispatcher = dispatcher(
        FakeStore(Arc::clone(&state)),
        registry_tool(Arc::new(Mutex::new(None)), false),
        agent,
    );
    let mut execution = agent_snapshot();
    execution.status = AgentExecutionStatus::Reconciling;
    execution.version = 6;
    execution.remote_run_ref = Some(remote.remote_run_id);
    execution.remote_session_ref = remote.remote_session_id;
    execution.remote_protocol_version = Some(remote.protocol_version);
    execution.status_poll_at = Some(UnixMicros::new(100));
    let candidate = AgentStatusCandidate {
        tenant_id: tenant_id(),
        execution,
        expected_run: ExpectedRun {
            run_id: run_id(),
            version: Some(9),
            execution_generation: Some(7),
        },
    };

    crate::AgentStatusDispatcher::get_status(&dispatcher, candidate)
        .await
        .expect("status dispatch");

    let recorded = state
        .agent_stop_outcome
        .lock()
        .expect("agent status outcome lock")
        .clone()
        .expect("recorded status outcome");
    assert_eq!(recorded.status, AgentExecutionStatus::Succeeded);
    assert_eq!(recorded.result, Some(result));
    assert_eq!(recorded.next_status_poll_at, None);
}

#[tokio::test]
async fn agent_event_read_normalizes_deduplicates_and_schedules_terminal_reconciliation() {
    let state = state();
    let payload = JsonPayload::from_validated_bytes(br#"{"progress":100}"#.to_vec());
    let remote_event = NormalizedAgentEvent {
        source_event_id: Some("remote-event-1".to_owned()),
        source_sequence: Some(1),
        kind: "agent.progress".to_owned(),
        authoritative: true,
        payload: payload.clone(),
        raw_digest: payload_digest(&payload),
    };
    let agent: Arc<dyn AgentServerAdapter> = Arc::new(FakeAgentAdapter {
        seen: Arc::new(Mutex::new(None)),
        stop_calls: Arc::new(Mutex::new(Vec::new())),
        stop_outcome: StopRequestOutcome::Unsupported,
        status_snapshot: None,
        event_batch: Some(RemoteEventBatch {
            events: vec![remote_event.clone(), remote_event],
            next_cursor: Some("cursor-2".to_owned()),
            terminal: true,
        }),
    });
    let dispatcher = dispatcher(
        FakeStore(Arc::clone(&state)),
        registry_tool(Arc::new(Mutex::new(None)), false),
        agent,
    );
    let mut execution = agent_snapshot();
    execution.status = AgentExecutionStatus::Running;
    execution.version = 6;
    execution.remote_run_ref = Some("remote-run-1".to_owned());
    execution.remote_session_ref = Some("session-1".to_owned());
    execution.remote_protocol_version = Some("test-v1".to_owned());
    execution.status_poll_at = Some(UnixMicros::new(100));
    execution.event_cursor = Some("cursor-1".to_owned());
    execution.cursor_version = 2;
    let candidate = AgentEventCandidate {
        tenant_id: tenant_id(),
        execution,
        expected_run: ExpectedRun {
            run_id: run_id(),
            version: Some(9),
            execution_generation: Some(7),
        },
    };

    crate::AgentEventDispatcher::read_events(&dispatcher, candidate)
        .await
        .expect("event dispatch");

    let recorded = state
        .agent_event_batch
        .lock()
        .expect("agent event batch lock")
        .clone()
        .expect("recorded event batch");
    assert_eq!(recorded.expected_cursor_version, 2);
    assert_eq!(recorded.next_cursor.as_deref(), Some("cursor-2"));
    assert_eq!(recorded.next_status_poll_at, Some(UnixMicros::new(30_002)));
    assert!(recorded.remote_terminal);
    assert_eq!(recorded.events.len(), 1);
    assert_eq!(
        recorded.events[0].source_cursor.as_deref(),
        Some("cursor-2")
    );
    assert!(recorded.events[0].local_event_id.is_some());
}

#[tokio::test]
async fn agent_resubmission_without_persisted_idempotency_is_not_sent() {
    let mut state = state();
    Arc::get_mut(&mut state)
        .expect("unshared state")
        .agent_invocation
        .capabilities_snapshot = JsonPayload::from_validated_bytes(br"{}".to_vec());
    let seen = Arc::new(Mutex::new(None));
    let dispatcher = dispatcher(
        FakeStore(Arc::clone(&state)),
        registry_tool(Arc::new(Mutex::new(None)), false),
        registry_agent(Arc::clone(&seen)),
    );

    crate::ExternalRecoveryDispatcher::dispatch(
        &dispatcher,
        StartedRecovery::Agent {
            execution: agent_snapshot(),
            disposition: CommandDisposition::Applied,
            fence: fence(),
        },
    )
    .await
    .expect("capability failure recording");

    assert!(seen.lock().expect("agent call lock").is_none());
    let recorded = state
        .agent_outcome
        .lock()
        .expect("agent outcome lock")
        .clone()
        .expect("recorded outcome");
    assert!(matches!(
        recorded.outcome,
        AgentSubmissionOutcome::Rejected {
            ref error_code,
            retry: ExecutionRetryClass::ManualReview,
            ..
        } if error_code == "AGENT_REPLAY_CAPABILITY_MISSING"
    ));
}

const fn tenant_id() -> TenantId {
    TenantId::from_bytes([1; 16])
}

const fn run_id() -> RunId {
    RunId::from_bytes([2; 16])
}

const fn task_id() -> TaskId {
    TaskId::from_bytes([3; 16])
}

const fn tool_execution_id() -> ToolExecutionId {
    ToolExecutionId::from_bytes([4; 16])
}

const fn agent_execution_id() -> AgentExecutionId {
    AgentExecutionId::from_bytes([5; 16])
}

const fn endpoint_id_value() -> EndpointId {
    EndpointId::from_bytes([6; 16])
}

const fn agent_version_id_value() -> AgentVersionId {
    AgentVersionId::from_bytes([7; 16])
}
