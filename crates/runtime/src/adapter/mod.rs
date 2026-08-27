use std::{collections::BTreeMap, fmt, future::Future, pin::Pin, sync::Arc};

use agent_loom_adapter_core::{
    AdapterCallContext, AdapterError, AdapterRetryClass, AgentRunRequest, AgentServerAdapter,
    EventReadLimits, ExecutionBudget, ExecutionId, NormalizedAgentEvent, RemoteAgentRef,
    RemoteAgentSnapshot, RemoteAgentStatus, RemoteEventBatch, StopRequestOutcome,
    SubmitAgentOutcome, ToolAdapter, ToolCallOutcome, ToolRequest,
};
use agent_loom_domain::{
    AgentEventReceiptId, AgentExecutionId, AgentExecutionStatus, AgentVersionId, CommandId,
    CorrelationId, Digest, DurationMicros, EndpointId, EventId, IdempotencyKey, JsonPayload,
    ScopeKey, TenantId, ToolExecutionId, UnixMicros,
};
use agent_loom_durable_store::{
    AgentEventCandidate, AgentEventProjection, AgentInvocation, AgentStatusCandidate,
    AgentStopCandidate, AgentSubmissionOutcome, AppendAgentEvents, CommandContext, DurableStore,
    ExecutionRetryClass, NormalizedAgentEventInput, QueryContext, RecordAgentOutcome,
    RecordAgentSubmission, RecordToolOutcome, StoreFuture, ToolInvocation, ToolRecordedOutcome,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::{ExternalDispatchError, RecoveryDispatchFence, StartedRecovery};

/// Store operations used after a recovery start intent has committed.
pub trait AdapterDispatchStore: Send + Sync {
    fn get_tool_invocation<'a>(
        &'a self,
        context: &'a QueryContext,
        execution_id: ToolExecutionId,
    ) -> StoreFuture<'a, Option<ToolInvocation>>;

    fn get_agent_invocation<'a>(
        &'a self,
        context: &'a QueryContext,
        execution_id: AgentExecutionId,
    ) -> StoreFuture<'a, Option<AgentInvocation>>;

    fn record_tool_outcome<'a>(
        &'a self,
        context: &'a CommandContext,
        command: RecordToolOutcome,
    ) -> StoreFuture<
        'a,
        agent_loom_durable_store::Committed<agent_loom_domain::ToolExecutionSnapshot>,
    >;

    fn record_agent_submission<'a>(
        &'a self,
        context: &'a CommandContext,
        command: RecordAgentSubmission,
    ) -> StoreFuture<
        'a,
        agent_loom_durable_store::Committed<agent_loom_domain::AgentExecutionSnapshot>,
    >;

    fn record_agent_outcome<'a>(
        &'a self,
        context: &'a CommandContext,
        command: RecordAgentOutcome,
    ) -> StoreFuture<
        'a,
        agent_loom_durable_store::Committed<agent_loom_domain::AgentExecutionSnapshot>,
    >;

    fn append_agent_events<'a>(
        &'a self,
        context: &'a CommandContext,
        command: AppendAgentEvents,
    ) -> StoreFuture<
        'a,
        agent_loom_durable_store::Committed<agent_loom_durable_store::AgentEventBatchOutcome>,
    >;
}

impl<T> AdapterDispatchStore for T
where
    T: DurableStore + ?Sized,
{
    fn get_tool_invocation<'a>(
        &'a self,
        context: &'a QueryContext,
        execution_id: ToolExecutionId,
    ) -> StoreFuture<'a, Option<ToolInvocation>> {
        DurableStore::get_tool_invocation(self, context, execution_id)
    }

    fn get_agent_invocation<'a>(
        &'a self,
        context: &'a QueryContext,
        execution_id: AgentExecutionId,
    ) -> StoreFuture<'a, Option<AgentInvocation>> {
        DurableStore::get_agent_invocation(self, context, execution_id)
    }

    fn record_tool_outcome<'a>(
        &'a self,
        context: &'a CommandContext,
        command: RecordToolOutcome,
    ) -> StoreFuture<
        'a,
        agent_loom_durable_store::Committed<agent_loom_domain::ToolExecutionSnapshot>,
    > {
        DurableStore::record_tool_outcome(self, context, command)
    }

    fn record_agent_submission<'a>(
        &'a self,
        context: &'a CommandContext,
        command: RecordAgentSubmission,
    ) -> StoreFuture<
        'a,
        agent_loom_durable_store::Committed<agent_loom_domain::AgentExecutionSnapshot>,
    > {
        DurableStore::record_agent_submission(self, context, command)
    }

    fn record_agent_outcome<'a>(
        &'a self,
        context: &'a CommandContext,
        command: RecordAgentOutcome,
    ) -> StoreFuture<
        'a,
        agent_loom_durable_store::Committed<agent_loom_domain::AgentExecutionSnapshot>,
    > {
        DurableStore::record_agent_outcome(self, context, command)
    }

    fn append_agent_events<'a>(
        &'a self,
        context: &'a CommandContext,
        command: AppendAgentEvents,
    ) -> StoreFuture<
        'a,
        agent_loom_durable_store::Committed<agent_loom_durable_store::AgentEventBatchOutcome>,
    > {
        DurableStore::append_agent_events(self, context, command)
    }
}

/// Resolves runtime Adapter implementations without exposing vendor types to
/// the Worker or Store contracts.
pub trait AdapterRegistry: Send + Sync {
    fn tool(&self, tool_name: &str) -> Option<Arc<dyn ToolAdapter>>;

    fn agent(
        &self,
        endpoint_id: EndpointId,
        agent_version_id: AgentVersionId,
    ) -> Option<Arc<dyn AgentServerAdapter>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdapterRegistrationError {
    pub safe_message: String,
}

impl fmt::Display for AdapterRegistrationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl std::error::Error for AdapterRegistrationError {}

#[derive(Default)]
pub struct StaticAdapterRegistry {
    tools: BTreeMap<String, Arc<dyn ToolAdapter>>,
    agents: BTreeMap<(EndpointId, AgentVersionId), Arc<dyn AgentServerAdapter>>,
}

impl fmt::Debug for StaticAdapterRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticAdapterRegistry")
            .field("tool_count", &self.tools.len())
            .field("agent_count", &self.agents.len())
            .finish()
    }
}

impl StaticAdapterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one Tool implementation under its declared stable key.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is empty or already registered.
    pub fn register_tool(
        &mut self,
        adapter: Arc<dyn ToolAdapter>,
    ) -> Result<(), AdapterRegistrationError> {
        let key = adapter.descriptor().tool_key.clone();
        if key.is_empty() {
            return Err(registration_error("Tool Adapter key must not be empty"));
        }
        if self.tools.contains_key(&key) {
            return Err(registration_error("Tool Adapter key is already registered"));
        }
        self.tools.insert(key, adapter);
        Ok(())
    }

    /// Binds one Agent Server implementation to an immutable Endpoint/version pair.
    ///
    /// # Errors
    ///
    /// Returns an error when the pair is already registered or contains a nil ID.
    pub fn register_agent(
        &mut self,
        endpoint_id: EndpointId,
        agent_version_id: AgentVersionId,
        adapter: Arc<dyn AgentServerAdapter>,
    ) -> Result<(), AdapterRegistrationError> {
        if endpoint_id.is_nil() || agent_version_id.is_nil() {
            return Err(registration_error(
                "Agent Adapter registration IDs must not be nil",
            ));
        }
        let key = (endpoint_id, agent_version_id);
        if self.agents.contains_key(&key) {
            return Err(registration_error(
                "Agent Adapter Endpoint/version is already registered",
            ));
        }
        self.agents.insert(key, adapter);
        Ok(())
    }
}

impl AdapterRegistry for StaticAdapterRegistry {
    fn tool(&self, tool_name: &str) -> Option<Arc<dyn ToolAdapter>> {
        self.tools.get(tool_name).map(Arc::clone)
    }

    fn agent(
        &self,
        endpoint_id: EndpointId,
        agent_version_id: AgentVersionId,
    ) -> Option<Arc<dyn AgentServerAdapter>> {
        self.agents
            .get(&(endpoint_id, agent_version_id))
            .map(Arc::clone)
    }
}

fn registration_error(message: &str) -> AdapterRegistrationError {
    AdapterRegistrationError {
        safe_message: message.to_owned(),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdapterContextSeed {
    pub tenant_id: TenantId,
    pub execution_id: ExecutionId,
    pub correlation_id: agent_loom_domain::CorrelationId,
    pub idempotency_key: IdempotencyKey,
    pub request_hash: Digest,
}

pub type AdapterContextFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AdapterCallContext, AdapterError>> + Send + 'a>>;

/// Resolves short-lived credentials, trace headers, and an absolute deadline
/// immediately before external I/O.
pub trait AdapterContextFactory: Send + Sync {
    fn create(&self, seed: AdapterContextSeed) -> AdapterContextFuture<'_>;
}

/// Converts a same-request retry decision into a durable absolute due time.
pub trait AdapterRetrySchedule: Send + Sync {
    /// Returns a durable absolute retry time for the same logical request.
    ///
    /// # Errors
    ///
    /// Returns a dispatch error when no safe retry time can be produced.
    fn retry_at(
        &self,
        error: &AdapterError,
        attempt: u64,
    ) -> Result<UnixMicros, ExternalDispatchError>;

    /// Returns the next durable due time for a nonterminal remote status.
    ///
    /// # Errors
    ///
    /// Returns a dispatch error when the timestamp cannot be represented safely.
    fn status_poll_at(&self, observation: u64) -> Result<UnixMicros, ExternalDispatchError>;
}

pub struct AdapterRecoveryDispatcher<S> {
    store: S,
    registry: Arc<dyn AdapterRegistry>,
    context_factory: Arc<dyn AdapterContextFactory>,
    retry_schedule: Arc<dyn AdapterRetrySchedule>,
}

impl<S> fmt::Debug for AdapterRecoveryDispatcher<S>
where
    S: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdapterRecoveryDispatcher")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl<S> AdapterRecoveryDispatcher<S>
where
    S: AdapterDispatchStore,
{
    pub fn new(
        store: S,
        registry: Arc<dyn AdapterRegistry>,
        context_factory: Arc<dyn AdapterContextFactory>,
        retry_schedule: Arc<dyn AdapterRetrySchedule>,
    ) -> Self {
        Self {
            store,
            registry,
            context_factory,
            retry_schedule,
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn dispatch_tool(
        &self,
        execution: agent_loom_domain::ToolExecutionSnapshot,
        fence: RecoveryDispatchFence,
    ) -> Result<(), ExternalDispatchError> {
        let query = query_context(execution.tenant_id, &fence);
        let invocation = self
            .store
            .get_tool_invocation(&query, execution.tool_execution_id)
            .await
            .map_err(|error| store_dispatch_error(&error))?;
        let Some(invocation) = invocation else {
            return self
                .record_tool_error(
                    &execution,
                    &fence,
                    configuration_error(
                        "INVOCATION_ENVELOPE_MISSING",
                        "Tool invocation envelope was not found",
                    ),
                )
                .await;
        };
        validate_tool_invocation(&invocation, &execution)?;

        let Some(adapter) = self.registry.tool(&invocation.tool_name) else {
            return self
                .record_tool_error(
                    &execution,
                    &fence,
                    configuration_error("ADAPTER_NOT_REGISTERED", "Tool Adapter is not registered"),
                )
                .await;
        };
        if adapter.descriptor().tool_key != invocation.tool_name {
            return self
                .record_tool_error(
                    &execution,
                    &fence,
                    configuration_error(
                        "ADAPTER_IDENTITY_MISMATCH",
                        "Tool Adapter descriptor does not match the invocation",
                    ),
                )
                .await;
        }
        if !tool_replay_allowed(&*adapter) {
            return self
                .record_tool_error(
                    &execution,
                    &fence,
                    configuration_error(
                        "UNSAFE_TOOL_REPLAY",
                        "Tool Adapter does not declare retry-safe side effects",
                    ),
                )
                .await;
        }
        let seed = context_seed_tool(&invocation, &fence);
        let context = match self.context_factory.create(seed.clone()).await {
            Ok(context) => context,
            Err(error) => {
                return self.record_tool_error(&execution, &fence, error).await;
            }
        };
        if let Err(error) = validate_adapter_context(&context, &seed) {
            return self.record_tool_error(&execution, &fence, error).await;
        }
        let (outcome, remote_request_id) = match adapter
            .execute(
                &context,
                ToolRequest {
                    input: invocation.request.clone(),
                },
            )
            .await
        {
            Ok(ToolCallOutcome::Completed(result)) => {
                (ToolRecordedOutcome::Completed { result }, None)
            }
            Ok(ToolCallOutcome::Accepted { external_ref }) => {
                (ToolRecordedOutcome::Accepted { external_ref }, None)
            }
            Ok(ToolCallOutcome::Uncertain { external_ref }) => (
                ToolRecordedOutcome::Uncertain {
                    external_ref,
                    recovery_action: if adapter.descriptor().capabilities.query_outcome {
                        "query_outcome".to_owned()
                    } else {
                        "manual_review".to_owned()
                    },
                },
                None,
            ),
            Err(error) => {
                let remote_request_id = error.remote_request_id.clone();
                (
                    tool_error_outcome(
                        &*self.retry_schedule,
                        &error,
                        u64::from(execution.attempt_count),
                    )?,
                    remote_request_id,
                )
            }
        };
        self.record_tool(&execution, &fence, outcome, remote_request_id)
            .await
    }

    async fn record_tool_error(
        &self,
        execution: &agent_loom_domain::ToolExecutionSnapshot,
        fence: &RecoveryDispatchFence,
        error: AdapterError,
    ) -> Result<(), ExternalDispatchError> {
        let outcome = tool_error_outcome(
            &*self.retry_schedule,
            &error,
            u64::from(execution.attempt_count),
        )?;
        let remote_request_id = error.remote_request_id;
        self.record_tool(execution, fence, outcome, remote_request_id)
            .await
    }

    async fn record_tool(
        &self,
        execution: &agent_loom_domain::ToolExecutionSnapshot,
        fence: &RecoveryDispatchFence,
        outcome: ToolRecordedOutcome,
        remote_request_id: Option<String>,
    ) -> Result<(), ExternalDispatchError> {
        let identity = format!(
            "tool/{}/attempt/{}",
            execution.tool_execution_id, execution.attempt_count
        );
        let response_digest = match &outcome {
            ToolRecordedOutcome::Completed { result }
            | ToolRecordedOutcome::Compensated { result } => Some(payload_digest(result)),
            _ => None,
        };
        let context = outcome_context(
            execution.tenant_id,
            fence,
            &identity,
            tool_record_digest(&outcome, remote_request_id.as_deref()),
        )?;
        self.store
            .record_tool_outcome(
                &context,
                RecordToolOutcome {
                    expected_run: fence.expected_run,
                    task_id: execution.task_id,
                    execution_generation: fence.execution_generation,
                    tool_execution_id: execution.tool_execution_id,
                    expected_attempt: execution.attempt_count,
                    outcome,
                    outcome_event_id: EventId::from_bytes(derived_id("event", &identity)),
                    response_digest,
                    remote_request_id,
                },
            )
            .await
            .map_err(|error| store_dispatch_error(&error))?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn dispatch_agent(
        &self,
        execution: agent_loom_domain::AgentExecutionSnapshot,
        fence: RecoveryDispatchFence,
    ) -> Result<(), ExternalDispatchError> {
        let query = query_context(execution.tenant_id, &fence);
        let invocation = self
            .store
            .get_agent_invocation(&query, execution.agent_execution_id)
            .await
            .map_err(|error| store_dispatch_error(&error))?;
        let Some(invocation) = invocation else {
            return self
                .record_agent_error(
                    &execution,
                    &fence,
                    configuration_error(
                        "INVOCATION_ENVELOPE_MISSING",
                        "Agent invocation envelope was not found",
                    ),
                )
                .await;
        };
        validate_agent_invocation(&invocation, &execution)?;
        let request = match decode_agent_request(&invocation.request) {
            Ok(request) => request,
            Err(error) => {
                return self.record_agent_error(&execution, &fence, error).await;
            }
        };
        let Some(adapter) = self
            .registry
            .agent(invocation.endpoint_id, invocation.agent_version_id)
        else {
            return self
                .record_agent_error(
                    &execution,
                    &fence,
                    configuration_error(
                        "ADAPTER_NOT_REGISTERED",
                        "Agent Server Adapter is not registered",
                    ),
                )
                .await;
        };
        let reconciling_submission = execution.status == AgentExecutionStatus::OutcomeUnknown;
        if !adapter.capabilities().submission_idempotency
            || !agent_submission_replay_allowed(&invocation.capabilities_snapshot)
        {
            return self
                .record_agent_error(
                    &execution,
                    &fence,
                    configuration_error(
                        "AGENT_REPLAY_CAPABILITY_MISSING",
                        "Agent Server does not guarantee idempotent resubmission",
                    ),
                )
                .await;
        }
        if reconciling_submission && !adapter.capabilities().submission_reconciliation {
            return self
                .record_agent_error(
                    &execution,
                    &fence,
                    configuration_error(
                        "AGENT_RECONCILIATION_CAPABILITY_MISSING",
                        "Agent Server cannot reconcile an uncertain submission",
                    ),
                )
                .await;
        }
        let seed = context_seed_agent(&invocation, &fence);
        let context = match self.context_factory.create(seed.clone()).await {
            Ok(context) => context,
            Err(error) => {
                return self.record_agent_error(&execution, &fence, error).await;
            }
        };
        if let Err(error) = validate_adapter_context(&context, &seed) {
            return self.record_agent_error(&execution, &fence, error).await;
        }
        let reconciled = if reconciling_submission {
            match adapter.reconcile_submission(&context).await {
                Ok(remote) => remote,
                Err(error) => {
                    return self.record_agent_error(&execution, &fence, error).await;
                }
            }
        } else {
            None
        };
        let outcome = if let Some(remote) = reconciled {
            accepted_submission(remote)
        } else {
            match adapter.submit(&context, request).await {
                Ok(SubmitAgentOutcome::Accepted(remote)) => accepted_submission(remote),
                Ok(SubmitAgentOutcome::SubmissionUncertain) => AgentSubmissionOutcome::Uncertain,
                Err(error) => {
                    agent_error_outcome(&*self.retry_schedule, &error, execution.version)?
                }
            }
        };
        self.record_agent(&execution, &fence, outcome).await
    }

    async fn record_agent_error(
        &self,
        execution: &agent_loom_domain::AgentExecutionSnapshot,
        fence: &RecoveryDispatchFence,
        error: AdapterError,
    ) -> Result<(), ExternalDispatchError> {
        let outcome = agent_error_outcome(&*self.retry_schedule, &error, execution.version)?;
        self.record_agent(execution, fence, outcome).await
    }

    async fn record_agent(
        &self,
        execution: &agent_loom_domain::AgentExecutionSnapshot,
        fence: &RecoveryDispatchFence,
        outcome: AgentSubmissionOutcome,
    ) -> Result<(), ExternalDispatchError> {
        let identity = format!(
            "agent/{}/version/{}",
            execution.agent_execution_id, execution.version
        );
        let context = outcome_context(
            execution.tenant_id,
            fence,
            &identity,
            agent_digest(&outcome),
        )?;
        self.store
            .record_agent_submission(
                &context,
                RecordAgentSubmission {
                    expected_run: fence.expected_run,
                    agent_execution_id: execution.agent_execution_id,
                    expected_version: execution.version,
                    outcome,
                    submission_event_id: EventId::from_bytes(derived_id("event", &identity)),
                },
            )
            .await
            .map_err(|error| store_dispatch_error(&error))?;
        Ok(())
    }

    async fn dispatch_agent_stop(
        &self,
        candidate: AgentStopCandidate,
    ) -> Result<(), crate::AgentStopDispatchError> {
        if !candidate.shape_is_valid() {
            return Err(stop_dispatch_error("Agent stop candidate is malformed"));
        }
        let execution = &candidate.execution;
        let correlation_id = CorrelationId::from_bytes(derived_id(
            "agent-stop-correlation",
            &execution.agent_execution_id.to_string(),
        ));
        let fence = RecoveryDispatchFence {
            expected_run: candidate.expected_run,
            execution_generation: candidate
                .expected_run
                .execution_generation
                .unwrap_or_default(),
            correlation_id,
            actor_ref: "agent-loom-agent-stop".to_owned(),
        };
        let query = query_context(candidate.tenant_id, &fence);
        let invocation = self
            .store
            .get_agent_invocation(&query, execution.agent_execution_id)
            .await
            .map_err(|error| stop_dispatch_error(&error.to_string()))?;
        let Some(invocation) = invocation else {
            return self
                .record_stop_projection(
                    &candidate,
                    &fence,
                    AgentExecutionStatus::ManualReview,
                    Some("INVOCATION_ENVELOPE_MISSING".to_owned()),
                )
                .await;
        };
        if validate_agent_invocation(&invocation, execution).is_err() {
            return self
                .record_stop_projection(
                    &candidate,
                    &fence,
                    AgentExecutionStatus::ManualReview,
                    Some("INVOCATION_ENVELOPE_MISMATCH".to_owned()),
                )
                .await;
        }
        let Some(adapter) = self
            .registry
            .agent(invocation.endpoint_id, invocation.agent_version_id)
        else {
            return self
                .record_stop_projection(
                    &candidate,
                    &fence,
                    AgentExecutionStatus::ManualReview,
                    Some("ADAPTER_NOT_REGISTERED".to_owned()),
                )
                .await;
        };
        let remote = RemoteAgentRef {
            remote_run_id: execution
                .remote_run_ref
                .clone()
                .ok_or_else(|| stop_dispatch_error("Agent stop has no remote Run identity"))?,
            remote_session_id: execution.remote_session_ref.clone(),
            protocol_version: execution
                .remote_protocol_version
                .clone()
                .ok_or_else(|| stop_dispatch_error("Agent stop has no remote protocol version"))?,
        };
        let seed = stop_context_seed(&invocation, &remote, correlation_id)?;
        let context = match self.context_factory.create(seed.clone()).await {
            Ok(context) => context,
            Err(error) => {
                let (status, error_code) = stop_error_projection(&error);
                return self
                    .record_stop_projection(&candidate, &fence, status, error_code)
                    .await;
            }
        };
        if let Err(error) = validate_adapter_context(&context, &seed) {
            let (status, error_code) = stop_error_projection(&error);
            return self
                .record_stop_projection(&candidate, &fence, status, error_code)
                .await;
        }
        let (status, error_code) = match adapter
            .request_stop(&context, &remote, "run control requested")
            .await
        {
            Ok(outcome) => stop_request_projection(outcome),
            Err(error) => stop_error_projection(&error),
        };
        self.record_stop_projection(&candidate, &fence, status, error_code)
            .await
    }

    async fn record_stop_projection(
        &self,
        candidate: &AgentStopCandidate,
        fence: &RecoveryDispatchFence,
        status: AgentExecutionStatus,
        error_code: Option<String>,
    ) -> Result<(), crate::AgentStopDispatchError> {
        let execution = &candidate.execution;
        let identity = format!(
            "agent-stop/{}/version/{}",
            execution.agent_execution_id, execution.version
        );
        let request_hash = stop_projection_digest(status, error_code.as_deref());
        let next_status_poll_at = if status == AgentExecutionStatus::Reconciling {
            Some(
                self.retry_schedule
                    .status_poll_at(execution.version)
                    .map_err(|error| stop_dispatch_error(&error.safe_message))?,
            )
        } else {
            None
        };
        let context = outcome_context(candidate.tenant_id, fence, &identity, request_hash)
            .map_err(|error| stop_dispatch_error(&error.safe_message))?;
        self.store
            .record_agent_outcome(
                &context,
                RecordAgentOutcome {
                    expected_run: candidate.expected_run,
                    agent_execution_id: execution.agent_execution_id,
                    expected_version: execution.version,
                    status,
                    result: None,
                    error_code,
                    next_status_poll_at,
                    outcome_event_id: EventId::from_bytes(derived_id("event", &identity)),
                },
            )
            .await
            .map_err(|error| stop_dispatch_error(&error.to_string()))?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn dispatch_agent_status(
        &self,
        candidate: AgentStatusCandidate,
    ) -> Result<(), crate::AgentStatusDispatchError> {
        if !candidate.shape_is_valid() {
            return Err(status_dispatch_error("Agent status candidate is malformed"));
        }
        let execution = &candidate.execution;
        let correlation_id = CorrelationId::from_bytes(derived_id(
            "agent-status-correlation",
            &execution.agent_execution_id.to_string(),
        ));
        let fence = RecoveryDispatchFence {
            expected_run: candidate.expected_run,
            execution_generation: candidate
                .expected_run
                .execution_generation
                .unwrap_or_default(),
            correlation_id,
            actor_ref: "agent-loom-agent-status".to_owned(),
        };
        let query = query_context(candidate.tenant_id, &fence);
        let invocation = self
            .store
            .get_agent_invocation(&query, execution.agent_execution_id)
            .await
            .map_err(|error| status_dispatch_error(&error.to_string()))?;
        let Some(invocation) = invocation else {
            return self
                .record_status_projection(
                    &candidate,
                    &fence,
                    AgentExecutionStatus::ManualReview,
                    None,
                    Some("INVOCATION_ENVELOPE_MISSING".to_owned()),
                )
                .await;
        };
        if validate_agent_invocation(&invocation, execution).is_err() {
            return self
                .record_status_projection(
                    &candidate,
                    &fence,
                    AgentExecutionStatus::ManualReview,
                    None,
                    Some("INVOCATION_ENVELOPE_MISMATCH".to_owned()),
                )
                .await;
        }
        let Some(adapter) = self
            .registry
            .agent(invocation.endpoint_id, invocation.agent_version_id)
        else {
            return self
                .record_status_projection(
                    &candidate,
                    &fence,
                    AgentExecutionStatus::ManualReview,
                    None,
                    Some("ADAPTER_NOT_REGISTERED".to_owned()),
                )
                .await;
        };
        let Ok(remote) = remote_ref(execution) else {
            return self
                .record_status_projection(
                    &candidate,
                    &fence,
                    AgentExecutionStatus::ManualReview,
                    None,
                    Some("REMOTE_IDENTITY_MISSING".to_owned()),
                )
                .await;
        };
        let seed = status_context_seed(&invocation, &remote, correlation_id, execution.version)?;
        let context = match self.context_factory.create(seed.clone()).await {
            Ok(context) => context,
            Err(error) => {
                let (status, error_code) = stop_error_projection(&error);
                return self
                    .record_status_projection(&candidate, &fence, status, None, error_code)
                    .await;
            }
        };
        if let Err(error) = validate_adapter_context(&context, &seed) {
            let (status, error_code) = stop_error_projection(&error);
            return self
                .record_status_projection(&candidate, &fence, status, None, error_code)
                .await;
        }
        let (status, result, error_code) = match adapter.get_status(&context, &remote).await {
            Ok(snapshot) => match project_remote_status(&remote, snapshot) {
                Ok(projected) => projected,
                Err(_) => (
                    AgentExecutionStatus::ManualReview,
                    None,
                    Some("REMOTE_IDENTITY_MISMATCH".to_owned()),
                ),
            },
            Err(error) => {
                let (status, error_code) = stop_error_projection(&error);
                (status, None, error_code)
            }
        };
        self.record_status_projection(&candidate, &fence, status, result, error_code)
            .await
    }

    async fn record_status_projection(
        &self,
        candidate: &AgentStatusCandidate,
        fence: &RecoveryDispatchFence,
        status: AgentExecutionStatus,
        result: Option<JsonPayload>,
        error_code: Option<String>,
    ) -> Result<(), crate::AgentStatusDispatchError> {
        let execution = &candidate.execution;
        let next_status_poll_at = if status == AgentExecutionStatus::Reconciling {
            Some(
                self.retry_schedule
                    .status_poll_at(execution.version)
                    .map_err(|error| status_dispatch_error(&error.safe_message))?,
            )
        } else {
            None
        };
        let identity = format!(
            "agent-status/{}/version/{}",
            execution.agent_execution_id, execution.version
        );
        let context = outcome_context(
            candidate.tenant_id,
            fence,
            &identity,
            stop_projection_digest(status, error_code.as_deref()),
        )
        .map_err(|error| status_dispatch_error(&error.safe_message))?;
        self.store
            .record_agent_outcome(
                &context,
                RecordAgentOutcome {
                    expected_run: candidate.expected_run,
                    agent_execution_id: execution.agent_execution_id,
                    expected_version: execution.version,
                    status,
                    result,
                    error_code,
                    next_status_poll_at,
                    outcome_event_id: EventId::from_bytes(derived_id("event", &identity)),
                },
            )
            .await
            .map_err(|error| status_dispatch_error(&error.to_string()))?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn dispatch_agent_events(
        &self,
        candidate: AgentEventCandidate,
    ) -> Result<(), crate::AgentEventDispatchError> {
        if !candidate.shape_is_valid() {
            return Err(event_dispatch_error("Agent event candidate is malformed"));
        }
        let execution = &candidate.execution;
        let correlation_id = CorrelationId::from_bytes(derived_id(
            "agent-events-correlation",
            &execution.agent_execution_id.to_string(),
        ));
        let fence = RecoveryDispatchFence {
            expected_run: candidate.expected_run,
            execution_generation: candidate
                .expected_run
                .execution_generation
                .unwrap_or_default(),
            correlation_id,
            actor_ref: "agent-loom-agent-events".to_owned(),
        };
        let query = query_context(candidate.tenant_id, &fence);
        let invocation = self
            .store
            .get_agent_invocation(&query, execution.agent_execution_id)
            .await
            .map_err(|error| event_dispatch_error(&error.to_string()))?;
        let Some(invocation) = invocation else {
            return self
                .record_event_projection(
                    &candidate,
                    &fence,
                    AgentExecutionStatus::ManualReview,
                    Some("INVOCATION_ENVELOPE_MISSING".to_owned()),
                )
                .await;
        };
        if validate_agent_invocation(&invocation, execution).is_err() {
            return self
                .record_event_projection(
                    &candidate,
                    &fence,
                    AgentExecutionStatus::ManualReview,
                    Some("INVOCATION_ENVELOPE_MISMATCH".to_owned()),
                )
                .await;
        }
        let Some(adapter) = self
            .registry
            .agent(invocation.endpoint_id, invocation.agent_version_id)
        else {
            return self
                .record_event_projection(
                    &candidate,
                    &fence,
                    AgentExecutionStatus::ManualReview,
                    Some("ADAPTER_NOT_REGISTERED".to_owned()),
                )
                .await;
        };
        if !adapter.capabilities().resumable_events {
            return self
                .record_event_projection(
                    &candidate,
                    &fence,
                    AgentExecutionStatus::ManualReview,
                    Some("RESUMABLE_EVENTS_UNSUPPORTED".to_owned()),
                )
                .await;
        }
        let Ok(remote) = remote_ref(execution) else {
            return self
                .record_event_projection(
                    &candidate,
                    &fence,
                    AgentExecutionStatus::ManualReview,
                    Some("REMOTE_IDENTITY_MISSING".to_owned()),
                )
                .await;
        };
        let seed = event_context_seed(
            &invocation,
            &remote,
            correlation_id,
            execution.cursor_version,
            execution.event_cursor.as_deref(),
        )?;
        let context = match self.context_factory.create(seed.clone()).await {
            Ok(context) => context,
            Err(error) => return self.record_event_error(&candidate, &fence, error).await,
        };
        if let Err(error) = validate_adapter_context(&context, &seed) {
            return self.record_event_error(&candidate, &fence, error).await;
        }
        let batch = match adapter
            .read_events(
                &context,
                &remote,
                execution.event_cursor.as_deref(),
                EventReadLimits {
                    max_events: 100,
                    max_bytes: 1_048_576,
                    max_wait: DurationMicros::new(5_000_000),
                },
            )
            .await
        {
            Ok(batch) => batch,
            Err(error) => return self.record_event_error(&candidate, &fence, error).await,
        };
        self.append_remote_event_batch(&candidate, &fence, batch)
            .await
    }

    async fn record_event_error(
        &self,
        candidate: &AgentEventCandidate,
        fence: &RecoveryDispatchFence,
        error: AdapterError,
    ) -> Result<(), crate::AgentEventDispatchError> {
        match error.retry {
            AdapterRetryClass::SameRequestBackoff | AdapterRetryClass::ReconnectAndResume => {
                self.append_remote_event_batch(
                    candidate,
                    fence,
                    RemoteEventBatch {
                        events: Vec::new(),
                        next_cursor: candidate.execution.event_cursor.clone(),
                        terminal: false,
                    },
                )
                .await
            }
            AdapterRetryClass::QueryOutcome => {
                self.record_event_projection(
                    candidate,
                    fence,
                    AgentExecutionStatus::Reconciling,
                    Some(error.code.to_owned()),
                )
                .await
            }
            AdapterRetryClass::Never | AdapterRetryClass::ManualReview => {
                self.record_event_projection(
                    candidate,
                    fence,
                    AgentExecutionStatus::ManualReview,
                    Some(error.code.to_owned()),
                )
                .await
            }
        }
    }

    async fn append_remote_event_batch(
        &self,
        candidate: &AgentEventCandidate,
        fence: &RecoveryDispatchFence,
        batch: RemoteEventBatch,
    ) -> Result<(), crate::AgentEventDispatchError> {
        let execution = &candidate.execution;
        let events = normalize_remote_events(execution, &batch)?;
        let next_status_poll_at = self
            .retry_schedule
            .status_poll_at(execution.cursor_version)
            .map_err(|error| event_dispatch_error(&error.safe_message))?;
        let identity = format!(
            "agent-events/{}/cursor/{}",
            execution.agent_execution_id, execution.cursor_version
        );
        let context = outcome_context(
            candidate.tenant_id,
            fence,
            &identity,
            remote_event_batch_digest(&batch),
        )
        .map_err(|error| event_dispatch_error(&error.safe_message))?;
        self.store
            .append_agent_events(
                &context,
                AppendAgentEvents {
                    expected_run: candidate.expected_run,
                    agent_execution_id: execution.agent_execution_id,
                    expected_cursor_version: execution.cursor_version,
                    next_cursor: batch.next_cursor,
                    next_status_poll_at: Some(next_status_poll_at),
                    remote_terminal: batch.terminal,
                    events,
                },
            )
            .await
            .map_err(|error| event_dispatch_error(&error.to_string()))?;
        Ok(())
    }

    async fn record_event_projection(
        &self,
        candidate: &AgentEventCandidate,
        fence: &RecoveryDispatchFence,
        status: AgentExecutionStatus,
        error_code: Option<String>,
    ) -> Result<(), crate::AgentEventDispatchError> {
        let execution = &candidate.execution;
        let next_status_poll_at = if status == AgentExecutionStatus::Reconciling {
            Some(
                self.retry_schedule
                    .status_poll_at(execution.version)
                    .map_err(|error| event_dispatch_error(&error.safe_message))?,
            )
        } else {
            None
        };
        let identity = format!(
            "agent-events-control/{}/version/{}",
            execution.agent_execution_id, execution.version
        );
        let context = outcome_context(
            candidate.tenant_id,
            fence,
            &identity,
            stop_projection_digest(status, error_code.as_deref()),
        )
        .map_err(|error| event_dispatch_error(&error.safe_message))?;
        self.store
            .record_agent_outcome(
                &context,
                RecordAgentOutcome {
                    expected_run: candidate.expected_run,
                    agent_execution_id: execution.agent_execution_id,
                    expected_version: execution.version,
                    status,
                    result: None,
                    error_code,
                    next_status_poll_at,
                    outcome_event_id: EventId::from_bytes(derived_id("event", &identity)),
                },
            )
            .await
            .map_err(|error| event_dispatch_error(&error.to_string()))?;
        Ok(())
    }
}

impl<S> crate::ExternalRecoveryDispatcher for AdapterRecoveryDispatcher<S>
where
    S: AdapterDispatchStore,
{
    fn dispatch(&self, started: StartedRecovery) -> crate::DispatchFuture<'_> {
        Box::pin(async move {
            match started {
                StartedRecovery::Tool {
                    execution, fence, ..
                } => self.dispatch_tool(execution, fence).await,
                StartedRecovery::Agent {
                    execution, fence, ..
                } => self.dispatch_agent(execution, fence).await,
            }
        })
    }
}

impl<S> crate::AgentStopDispatcher for AdapterRecoveryDispatcher<S>
where
    S: AdapterDispatchStore,
{
    fn request_stop(&self, candidate: AgentStopCandidate) -> crate::AgentStopFuture<'_> {
        Box::pin(async move { self.dispatch_agent_stop(candidate).await })
    }
}

impl<S> crate::AgentStatusDispatcher for AdapterRecoveryDispatcher<S>
where
    S: AdapterDispatchStore,
{
    fn get_status(&self, candidate: AgentStatusCandidate) -> crate::AgentStatusFuture<'_> {
        Box::pin(async move { self.dispatch_agent_status(candidate).await })
    }
}

impl<S> crate::AgentEventDispatcher for AdapterRecoveryDispatcher<S>
where
    S: AdapterDispatchStore,
{
    fn read_events(&self, candidate: AgentEventCandidate) -> crate::AgentEventFuture<'_> {
        Box::pin(async move { self.dispatch_agent_events(candidate).await })
    }
}

#[derive(Deserialize)]
struct StoredAgentRequest {
    instructions: String,
    input: Value,
    budget: StoredExecutionBudget,
}

#[derive(Deserialize)]
struct StoredExecutionBudget {
    max_duration_micros: u64,
    max_output_bytes: u64,
}

fn decode_agent_request(request: &JsonPayload) -> Result<AgentRunRequest, AdapterError> {
    let stored: StoredAgentRequest = serde_json::from_slice(request.as_bytes()).map_err(|_| {
        configuration_error(
            "INVALID_AGENT_REQUEST",
            "Persisted Agent request does not match the normalized envelope",
        )
    })?;
    if stored.instructions.is_empty()
        || stored.budget.max_duration_micros == 0
        || stored.budget.max_output_bytes == 0
    {
        return Err(configuration_error(
            "INVALID_AGENT_REQUEST",
            "Persisted Agent request has an invalid instruction or budget",
        ));
    }
    let input = serde_json::to_vec(&stored.input).map_err(|_| {
        configuration_error(
            "INVALID_AGENT_REQUEST",
            "Persisted Agent input could not be normalized",
        )
    })?;
    Ok(AgentRunRequest {
        instructions: stored.instructions,
        input: JsonPayload::from_validated_bytes(input),
        budget: ExecutionBudget {
            max_duration: agent_loom_domain::DurationMicros::new(stored.budget.max_duration_micros),
            max_output_bytes: stored.budget.max_output_bytes,
        },
    })
}

fn validate_tool_invocation(
    invocation: &ToolInvocation,
    execution: &agent_loom_domain::ToolExecutionSnapshot,
) -> Result<(), ExternalDispatchError> {
    if invocation.tenant_id != execution.tenant_id
        || invocation.tool_execution_id != execution.tool_execution_id
        || invocation.run_id != execution.run_id
        || invocation.tool_name != execution.tool_name
    {
        return Err(dispatch_error(
            "Tool invocation envelope does not match the started execution",
        ));
    }
    Ok(())
}

fn tool_replay_allowed(adapter: &dyn ToolAdapter) -> bool {
    matches!(
        adapter.descriptor().side_effect,
        agent_loom_adapter_core::SideEffectClass::ReadOnly
            | agent_loom_adapter_core::SideEffectClass::IdempotentWrite
    )
}

fn validate_agent_invocation(
    invocation: &AgentInvocation,
    execution: &agent_loom_domain::AgentExecutionSnapshot,
) -> Result<(), ExternalDispatchError> {
    if invocation.tenant_id != execution.tenant_id
        || invocation.agent_execution_id != execution.agent_execution_id
        || invocation.run_id != execution.run_id
        || invocation.endpoint_id != execution.endpoint_id
        || invocation.agent_version_id != execution.agent_version_id
    {
        return Err(dispatch_error(
            "Agent invocation envelope does not match the started execution",
        ));
    }
    Ok(())
}

fn validate_adapter_context(
    context: &AdapterCallContext,
    seed: &AdapterContextSeed,
) -> Result<(), AdapterError> {
    if context.tenant_id != seed.tenant_id
        || context.execution_id != seed.execution_id
        || context.correlation_id != seed.correlation_id
        || context.idempotency_key != seed.idempotency_key
        || context.request_hash != seed.request_hash
        || context.deadline.get() <= 0
        || context.trace_context.trace_parent.is_empty()
        || context.auth.scheme().is_empty()
        || context.auth.expose_secret().is_empty()
    {
        return Err(configuration_error(
            "INVALID_ADAPTER_CONTEXT",
            "Resolved Adapter context changed immutable identity or is incomplete",
        ));
    }
    Ok(())
}

fn agent_submission_replay_allowed(capabilities: &JsonPayload) -> bool {
    serde_json::from_slice::<Value>(capabilities.as_bytes())
        .ok()
        .and_then(|value| value.get("submission_idempotency").and_then(Value::as_bool))
        == Some(true)
}

fn context_seed_tool(
    invocation: &ToolInvocation,
    fence: &RecoveryDispatchFence,
) -> AdapterContextSeed {
    AdapterContextSeed {
        tenant_id: invocation.tenant_id,
        execution_id: ExecutionId::Tool(invocation.tool_execution_id),
        correlation_id: fence.correlation_id,
        idempotency_key: invocation.idempotency_key.clone(),
        request_hash: invocation.request_hash,
    }
}

fn context_seed_agent(
    invocation: &AgentInvocation,
    fence: &RecoveryDispatchFence,
) -> AdapterContextSeed {
    AdapterContextSeed {
        tenant_id: invocation.tenant_id,
        execution_id: ExecutionId::Agent(invocation.agent_execution_id),
        correlation_id: fence.correlation_id,
        idempotency_key: invocation.idempotency_key.clone(),
        request_hash: invocation.request_hash,
    }
}

fn stop_context_seed(
    invocation: &AgentInvocation,
    remote: &RemoteAgentRef,
    correlation_id: CorrelationId,
) -> Result<AdapterContextSeed, crate::AgentStopDispatchError> {
    let identity = format!("agent-stop-{}", invocation.agent_execution_id);
    let mut hasher = Sha256::new();
    hasher.update(b"agent-stop\0");
    hasher.update(remote.remote_run_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(remote.protocol_version.as_bytes());
    hasher.update(b"\0run control requested");
    Ok(AdapterContextSeed {
        tenant_id: invocation.tenant_id,
        execution_id: ExecutionId::Agent(invocation.agent_execution_id),
        correlation_id,
        idempotency_key: IdempotencyKey::parse(identity)
            .map_err(|_| stop_dispatch_error("generated Agent stop identity is invalid"))?,
        request_hash: Digest::from_bytes(hasher.finalize().into()),
    })
}

fn status_context_seed(
    invocation: &AgentInvocation,
    remote: &RemoteAgentRef,
    correlation_id: CorrelationId,
    version: u64,
) -> Result<AdapterContextSeed, crate::AgentStatusDispatchError> {
    let identity = format!("agent-status-{}-{version}", invocation.agent_execution_id);
    let mut hasher = Sha256::new();
    hasher.update(b"agent-status\0");
    hasher.update(remote.remote_run_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(remote.protocol_version.as_bytes());
    Ok(AdapterContextSeed {
        tenant_id: invocation.tenant_id,
        execution_id: ExecutionId::Agent(invocation.agent_execution_id),
        correlation_id,
        idempotency_key: IdempotencyKey::parse(identity)
            .map_err(|_| status_dispatch_error("generated Agent status identity is invalid"))?,
        request_hash: Digest::from_bytes(hasher.finalize().into()),
    })
}

fn event_context_seed(
    invocation: &AgentInvocation,
    remote: &RemoteAgentRef,
    correlation_id: CorrelationId,
    cursor_version: u64,
    cursor: Option<&str>,
) -> Result<AdapterContextSeed, crate::AgentEventDispatchError> {
    let identity = format!(
        "agent-events-{}-{cursor_version}",
        invocation.agent_execution_id
    );
    let mut hasher = Sha256::new();
    hasher.update(b"agent-events\0");
    hasher.update(remote.remote_run_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(remote.protocol_version.as_bytes());
    if let Some(cursor) = cursor {
        hasher.update(b"\0cursor\0");
        hasher.update(cursor.as_bytes());
    }
    Ok(AdapterContextSeed {
        tenant_id: invocation.tenant_id,
        execution_id: ExecutionId::Agent(invocation.agent_execution_id),
        correlation_id,
        idempotency_key: IdempotencyKey::parse(identity)
            .map_err(|_| event_dispatch_error("generated Agent event identity is invalid"))?,
        request_hash: Digest::from_bytes(hasher.finalize().into()),
    })
}

fn remote_ref(
    execution: &agent_loom_domain::AgentExecutionSnapshot,
) -> Result<RemoteAgentRef, String> {
    Ok(RemoteAgentRef {
        remote_run_id: execution
            .remote_run_ref
            .clone()
            .ok_or_else(|| "Agent execution has no remote Run identity".to_owned())?,
        remote_session_id: execution.remote_session_ref.clone(),
        protocol_version: execution
            .remote_protocol_version
            .clone()
            .ok_or_else(|| "Agent execution has no remote protocol version".to_owned())?,
    })
}

fn project_remote_status(
    expected: &RemoteAgentRef,
    snapshot: RemoteAgentSnapshot,
) -> Result<
    (AgentExecutionStatus, Option<JsonPayload>, Option<String>),
    crate::AgentStatusDispatchError,
> {
    if snapshot.remote != *expected {
        return Err(status_dispatch_error(
            "Agent status response changed the remote execution identity",
        ));
    }
    Ok(match snapshot.status {
        RemoteAgentStatus::Completed => match snapshot.result {
            Some(result) => (AgentExecutionStatus::Succeeded, Some(result), None),
            None => (
                AgentExecutionStatus::ManualReview,
                None,
                Some("REMOTE_RESULT_MISSING".to_owned()),
            ),
        },
        RemoteAgentStatus::Failed => (
            AgentExecutionStatus::Failed,
            None,
            Some("REMOTE_AGENT_FAILED".to_owned()),
        ),
        RemoteAgentStatus::Cancelled => (AgentExecutionStatus::Cancelled, None, None),
        RemoteAgentStatus::Unknown => (
            AgentExecutionStatus::Reconciling,
            None,
            Some("REMOTE_STATUS_UNKNOWN".to_owned()),
        ),
        RemoteAgentStatus::Accepted
        | RemoteAgentStatus::Running
        | RemoteAgentStatus::WaitingForApproval
        | RemoteAgentStatus::WaitingForInput
        | RemoteAgentStatus::Stopping => (AgentExecutionStatus::Reconciling, None, None),
    })
}

fn normalize_remote_events(
    execution: &agent_loom_domain::AgentExecutionSnapshot,
    batch: &RemoteEventBatch,
) -> Result<Vec<NormalizedAgentEventInput>, crate::AgentEventDispatchError> {
    let mut seen = BTreeMap::<String, Digest>::new();
    let mut normalized = Vec::with_capacity(batch.events.len());
    for event in &batch.events {
        let source_identity = remote_event_identity(event)?;
        if let Some(existing) = seen.get(&source_identity) {
            if existing != &event.raw_digest {
                return Err(event_dispatch_error(
                    "remote Agent reused an event identity with different content",
                ));
            }
            continue;
        }
        seen.insert(source_identity.clone(), event.raw_digest);
        let identity = format!("{}/{}", execution.agent_execution_id, source_identity);
        let dedupe_key = Digest::from_bytes(Sha256::digest(identity.as_bytes()).into());
        normalized.push(NormalizedAgentEventInput {
            receipt_id: AgentEventReceiptId::from_bytes(derived_id(
                "agent-event-receipt",
                &identity,
            )),
            dedupe_key,
            source_event_id: event.source_event_id.clone(),
            source_sequence: event.source_sequence,
            source_cursor: batch.next_cursor.clone(),
            event_kind: event.kind.clone(),
            authoritative: event.authoritative,
            raw_digest: event.raw_digest,
            local_event_id: event
                .authoritative
                .then(|| EventId::from_bytes(derived_id("agent-event", &identity))),
            payload_schema_version: 1,
            payload: event.payload.clone(),
            projection: AgentEventProjection::NONE,
        });
    }
    Ok(normalized)
}

fn remote_event_identity(
    event: &NormalizedAgentEvent,
) -> Result<String, crate::AgentEventDispatchError> {
    if event.kind.is_empty() || event.source_event_id.as_ref().is_some_and(String::is_empty) {
        return Err(event_dispatch_error(
            "remote Agent event identity is malformed",
        ));
    }
    if let Some(source_event_id) = &event.source_event_id {
        return Ok(format!("id/{source_event_id}"));
    }
    if let Some(source_sequence) = event.source_sequence {
        return Ok(format!("sequence/{source_sequence}"));
    }
    Ok(format!("digest/{}", hex(event.raw_digest.as_bytes())))
}

fn remote_event_batch_digest(batch: &RemoteEventBatch) -> Digest {
    let mut hasher = Sha256::new();
    hasher.update(if batch.terminal {
        b"terminal\0".as_slice()
    } else {
        b"nonterminal\0".as_slice()
    });
    if let Some(cursor) = &batch.next_cursor {
        hasher.update(cursor.as_bytes());
    }
    for event in &batch.events {
        hasher.update(b"\0event\0");
        if let Some(source_event_id) = &event.source_event_id {
            hasher.update(source_event_id.as_bytes());
        }
        if let Some(source_sequence) = event.source_sequence {
            hasher.update(source_sequence.to_be_bytes());
        }
        hasher.update(event.kind.as_bytes());
        hasher.update([u8::from(event.authoritative)]);
        hasher.update(event.raw_digest.as_bytes());
    }
    Digest::from_bytes(hasher.finalize().into())
}

fn query_context(tenant_id: TenantId, fence: &RecoveryDispatchFence) -> QueryContext {
    QueryContext {
        tenant_id,
        actor_ref: fence.actor_ref.clone(),
        authoritative: true,
    }
}

fn outcome_context(
    tenant_id: TenantId,
    fence: &RecoveryDispatchFence,
    identity: &str,
    request_hash: Digest,
) -> Result<CommandContext, ExternalDispatchError> {
    Ok(CommandContext {
        tenant_id,
        command_id: CommandId::from_bytes(derived_id("command", identity)),
        correlation_id: fence.correlation_id,
        actor_ref: fence.actor_ref.clone(),
        scope: ScopeKey::parse("worker.adapter_outcome")
            .map_err(|_| dispatch_error("generated Adapter outcome scope is invalid"))?,
        idempotency_key: IdempotencyKey::parse(identity)
            .map_err(|_| dispatch_error("generated Adapter outcome identity is invalid"))?,
        request_hash,
    })
}

fn tool_error_outcome(
    retry_schedule: &dyn AdapterRetrySchedule,
    error: &AdapterError,
    attempt: u64,
) -> Result<ToolRecordedOutcome, ExternalDispatchError> {
    let retry = retry_class(error.retry);
    let retry_at = if retry == ExecutionRetryClass::SameRequestBackoff {
        Some(retry_schedule.retry_at(error, attempt)?)
    } else {
        None
    };
    Ok(ToolRecordedOutcome::Failed {
        error_code: error.code.to_owned(),
        retry,
        retry_at,
    })
}

fn agent_error_outcome(
    retry_schedule: &dyn AdapterRetrySchedule,
    error: &AdapterError,
    version: u64,
) -> Result<AgentSubmissionOutcome, ExternalDispatchError> {
    if matches!(
        error.retry,
        AdapterRetryClass::ReconnectAndResume | AdapterRetryClass::QueryOutcome
    ) {
        return Ok(AgentSubmissionOutcome::Uncertain);
    }
    let retry = retry_class(error.retry);
    let retry_at = if retry == ExecutionRetryClass::SameRequestBackoff {
        Some(retry_schedule.retry_at(error, version)?)
    } else {
        None
    };
    Ok(AgentSubmissionOutcome::Rejected {
        error_code: error.code.to_owned(),
        retry,
        retry_at,
    })
}

fn accepted_submission(remote: RemoteAgentRef) -> AgentSubmissionOutcome {
    AgentSubmissionOutcome::Accepted {
        remote_run_ref: remote.remote_run_id,
        remote_session_ref: remote.remote_session_id,
        remote_protocol_version: remote.protocol_version,
    }
}

fn stop_request_projection(outcome: StopRequestOutcome) -> (AgentExecutionStatus, Option<String>) {
    match outcome {
        StopRequestOutcome::Accepted { .. } => (AgentExecutionStatus::Reconciling, None),
        StopRequestOutcome::AlreadyTerminal {
            status: RemoteAgentStatus::Cancelled,
        } => (AgentExecutionStatus::Cancelled, None),
        StopRequestOutcome::AlreadyTerminal {
            status: RemoteAgentStatus::Failed,
        } => (
            AgentExecutionStatus::Failed,
            Some("REMOTE_AGENT_FAILED".to_owned()),
        ),
        StopRequestOutcome::AlreadyTerminal { .. } | StopRequestOutcome::Uncertain => (
            AgentExecutionStatus::Reconciling,
            Some("STOP_OUTCOME_REQUIRES_RECONCILIATION".to_owned()),
        ),
        StopRequestOutcome::Unsupported => (
            AgentExecutionStatus::ManualReview,
            Some("STOP_UNSUPPORTED".to_owned()),
        ),
    }
}

fn stop_error_projection(error: &AdapterError) -> (AgentExecutionStatus, Option<String>) {
    let status = match error.retry {
        AdapterRetryClass::Never | AdapterRetryClass::ManualReview => {
            AgentExecutionStatus::ManualReview
        }
        AdapterRetryClass::SameRequestBackoff
        | AdapterRetryClass::ReconnectAndResume
        | AdapterRetryClass::QueryOutcome => AgentExecutionStatus::Reconciling,
    };
    let code = if error.code.is_empty() {
        "AGENT_STOP_FAILED".to_owned()
    } else {
        error.code.to_owned()
    };
    (status, Some(code))
}

fn stop_projection_digest(status: AgentExecutionStatus, error_code: Option<&str>) -> Digest {
    let mut hasher = Sha256::new();
    hasher.update(format!("{status:?}").as_bytes());
    if let Some(error_code) = error_code {
        hasher.update(b"\0");
        hasher.update(error_code.as_bytes());
    }
    Digest::from_bytes(hasher.finalize().into())
}

const fn retry_class(value: AdapterRetryClass) -> ExecutionRetryClass {
    match value {
        AdapterRetryClass::Never => ExecutionRetryClass::Never,
        AdapterRetryClass::SameRequestBackoff => ExecutionRetryClass::SameRequestBackoff,
        AdapterRetryClass::ReconnectAndResume => ExecutionRetryClass::ReconnectAndResume,
        AdapterRetryClass::QueryOutcome => ExecutionRetryClass::QueryOutcome,
        AdapterRetryClass::ManualReview => ExecutionRetryClass::ManualReview,
    }
}

fn configuration_error(code: &'static str, safe_message: &'static str) -> AdapterError {
    AdapterError {
        code,
        retry: AdapterRetryClass::ManualReview,
        safe_message: safe_message.to_owned(),
        remote_request_id: None,
        retry_after: None,
    }
}

fn tool_digest(outcome: &ToolRecordedOutcome) -> Digest {
    let mut hasher = Sha256::new();
    match outcome {
        ToolRecordedOutcome::Completed { result } => {
            hasher.update(b"completed\0");
            hasher.update(result.as_bytes());
        }
        ToolRecordedOutcome::Accepted { external_ref } => {
            hasher.update(b"accepted\0");
            hasher.update(external_ref.as_bytes());
        }
        ToolRecordedOutcome::Failed {
            error_code,
            retry,
            retry_at,
        } => {
            hasher.update(b"failed\0");
            hasher.update(error_code.as_bytes());
            hasher.update(format!("{retry:?}/{retry_at:?}").as_bytes());
        }
        ToolRecordedOutcome::Uncertain {
            external_ref,
            recovery_action,
        } => {
            hasher.update(b"uncertain\0");
            if let Some(external_ref) = external_ref {
                hasher.update(external_ref.as_bytes());
            }
            hasher.update(recovery_action.as_bytes());
        }
        ToolRecordedOutcome::Compensated { result } => {
            hasher.update(b"compensated\0");
            hasher.update(result.as_bytes());
        }
    }
    Digest::from_bytes(hasher.finalize().into())
}

fn tool_record_digest(outcome: &ToolRecordedOutcome, remote_request_id: Option<&str>) -> Digest {
    let mut hasher = Sha256::new();
    hasher.update(tool_digest(outcome).as_bytes());
    if let Some(remote_request_id) = remote_request_id {
        hasher.update(b"\0remote-request\0");
        hasher.update(remote_request_id.as_bytes());
    }
    Digest::from_bytes(hasher.finalize().into())
}

fn agent_digest(outcome: &AgentSubmissionOutcome) -> Digest {
    let mut hasher = Sha256::new();
    match outcome {
        AgentSubmissionOutcome::Accepted {
            remote_run_ref,
            remote_session_ref,
            remote_protocol_version,
        } => {
            hasher.update(b"accepted\0");
            hasher.update(remote_run_ref.as_bytes());
            if let Some(remote_session_ref) = remote_session_ref {
                hasher.update(remote_session_ref.as_bytes());
            }
            hasher.update(remote_protocol_version.as_bytes());
        }
        AgentSubmissionOutcome::Uncertain => hasher.update(b"uncertain\0"),
        AgentSubmissionOutcome::Rejected {
            error_code,
            retry,
            retry_at,
        } => {
            hasher.update(b"rejected\0");
            hasher.update(error_code.as_bytes());
            hasher.update(format!("{retry:?}/{retry_at:?}").as_bytes());
        }
    }
    Digest::from_bytes(hasher.finalize().into())
}

fn payload_digest(payload: &JsonPayload) -> Digest {
    Digest::from_bytes(Sha256::digest(payload.as_bytes()).into())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(value, "{byte:02x}");
    }
    value
}

fn derived_id(namespace: &str, identity: &str) -> [u8; 16] {
    let bytes: [u8; 32] = Sha256::digest(format!("{namespace}/{identity}").as_bytes()).into();
    let mut id = [0; 16];
    id.copy_from_slice(&bytes[..16]);
    id
}

fn store_dispatch_error(error: &agent_loom_durable_store::StoreError) -> ExternalDispatchError {
    ExternalDispatchError {
        safe_message: error.to_string(),
    }
}

fn dispatch_error(message: &str) -> ExternalDispatchError {
    ExternalDispatchError {
        safe_message: message.to_owned(),
    }
}

fn stop_dispatch_error(message: &str) -> crate::AgentStopDispatchError {
    crate::AgentStopDispatchError {
        safe_message: message.to_owned(),
    }
}

fn status_dispatch_error(message: &str) -> crate::AgentStatusDispatchError {
    crate::AgentStatusDispatchError {
        safe_message: message.to_owned(),
    }
}

fn event_dispatch_error(message: &str) -> crate::AgentEventDispatchError {
    crate::AgentEventDispatchError {
        safe_message: message.to_owned(),
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
