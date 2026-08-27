use std::{fmt, sync::Arc};

use agent_loom_adapter_core::{
    AdapterCallContext, AdapterError, AdapterFuture, AgentCapabilities, AgentRunRequest,
    AgentServerAdapter, CompensationOutcome, CompensationRequest, EventReadLimits,
    NormalizedAgentEvent, RemoteAgentRef, RemoteAgentSnapshot, RemoteAgentStatus, RemoteEventBatch,
    ResolvedAuth, SideEffectClass, StopRequestOutcome, SubmitAgentOutcome, ToolAdapter,
    ToolCallOutcome, ToolCapabilities, ToolDescriptor, ToolQueryOutcome, ToolRequest, TraceContext,
};
use agent_loom_domain::{
    AgentVersionId, Digest, DurationMicros, EndpointId, JsonPayload, UnixMicros,
};
use agent_loom_runtime::{
    AdapterContextFactory, AdapterContextFuture, AdapterContextSeed, AdapterRecoveryDispatcher,
    AdapterRetrySchedule, DispatchFuture, ExternalDispatchError, ExternalRecoveryDispatcher,
    StartedRecovery, StaticAdapterRegistry,
};
use agent_loom_store_postgres::PostgresStore;
use serde_json::json;
use sha2::{Digest as _, Sha256};

#[derive(Clone, Debug, Default)]
pub struct MockDeliveryAgentAdapter;

impl AgentServerAdapter for MockDeliveryAgentAdapter {
    fn kind(&self) -> &'static str {
        "mock"
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
        context: &'a AdapterCallContext,
        request: AgentRunRequest,
    ) -> AdapterFuture<'a, SubmitAgentOutcome> {
        Box::pin(async move {
            if request.instructions.is_empty() || request.input.as_bytes().is_empty() {
                return Err(adapter_error(
                    "MOCK_INVALID_REQUEST",
                    "Mock Agent request is empty",
                ));
            }
            Ok(SubmitAgentOutcome::Accepted(remote_ref(context)))
        })
    }

    fn reconcile_submission<'a>(
        &'a self,
        context: &'a AdapterCallContext,
    ) -> AdapterFuture<'a, Option<RemoteAgentRef>> {
        Box::pin(async move { Ok(Some(remote_ref(context))) })
    }

    fn get_status<'a>(
        &'a self,
        _context: &'a AdapterCallContext,
        remote: &'a RemoteAgentRef,
    ) -> AdapterFuture<'a, RemoteAgentSnapshot> {
        Box::pin(async move {
            Ok(RemoteAgentSnapshot {
                remote: remote.clone(),
                status: RemoteAgentStatus::Completed,
                result: Some(json_payload(&json!({"status": "succeeded"}))),
            })
        })
    }

    fn read_events<'a>(
        &'a self,
        _context: &'a AdapterCallContext,
        remote: &'a RemoteAgentRef,
        _cursor: Option<&'a str>,
        _limits: EventReadLimits,
    ) -> AdapterFuture<'a, RemoteEventBatch> {
        Box::pin(async move {
            let payload = json_payload(&json!({
                "remote_run_id": remote.remote_run_id,
                "status": "completed"
            }));
            Ok(RemoteEventBatch {
                events: vec![NormalizedAgentEvent {
                    source_event_id: Some(format!("{}-completed", remote.remote_run_id)),
                    source_sequence: Some(1),
                    kind: "agent.completed".to_owned(),
                    authoritative: true,
                    raw_digest: digest(payload.as_bytes()),
                    payload,
                }],
                next_cursor: Some("1".to_owned()),
                terminal: true,
            })
        })
    }

    fn request_stop<'a>(
        &'a self,
        _context: &'a AdapterCallContext,
        _remote: &'a RemoteAgentRef,
        _reason: &'a str,
    ) -> AdapterFuture<'a, StopRequestOutcome> {
        Box::pin(async move {
            Ok(StopRequestOutcome::AlreadyTerminal {
                status: RemoteAgentStatus::Completed,
            })
        })
    }
}

#[derive(Clone, Debug)]
pub struct MockDevOpsToolAdapter {
    descriptor: ToolDescriptor,
}

impl Default for MockDevOpsToolAdapter {
    fn default() -> Self {
        Self {
            descriptor: ToolDescriptor {
                tool_key: "devops.deploy".to_owned(),
                side_effect: SideEffectClass::IdempotentWrite,
                capabilities: ToolCapabilities {
                    query_outcome: true,
                    compensation: true,
                    asynchronous_result: false,
                },
            },
        }
    }
}

impl ToolAdapter for MockDevOpsToolAdapter {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn execute<'a>(
        &'a self,
        _context: &'a AdapterCallContext,
        request: ToolRequest,
    ) -> AdapterFuture<'a, ToolCallOutcome> {
        Box::pin(async move {
            if request.input.as_bytes().is_empty() {
                return Err(adapter_error(
                    "MOCK_DEPLOY_INVALID",
                    "Mock deployment request is empty",
                ));
            }
            Ok(ToolCallOutcome::Completed(json_payload(&json!({
                "operation_ref": "mock-deployment",
                "release_status": "healthy",
                "deployed": true
            }))))
        })
    }

    fn query_outcome<'a>(
        &'a self,
        _context: &'a AdapterCallContext,
        external_ref: &'a str,
    ) -> AdapterFuture<'a, ToolQueryOutcome> {
        Box::pin(async move {
            Ok(ToolQueryOutcome::Completed(json_payload(&json!({
                "operation_ref": external_ref,
                "release_status": "healthy",
                "deployed": true
            }))))
        })
    }

    fn compensate<'a>(
        &'a self,
        _context: &'a AdapterCallContext,
        request: CompensationRequest,
    ) -> AdapterFuture<'a, CompensationOutcome> {
        Box::pin(async move {
            Ok(CompensationOutcome::Completed(json_payload(&json!({
                "operation_ref": request.external_ref,
                "rolled_back": true
            }))))
        })
    }
}

fn remote_ref(context: &AdapterCallContext) -> RemoteAgentRef {
    let execution = match context.execution_id {
        agent_loom_adapter_core::ExecutionId::Agent(id) => id.to_string(),
        agent_loom_adapter_core::ExecutionId::Tool(id) => id.to_string(),
    };
    RemoteAgentRef {
        remote_run_id: format!("mock-{execution}"),
        remote_session_id: Some(format!("session-{execution}")),
        protocol_version: "1".to_owned(),
    }
}

#[derive(Clone, Debug, Default)]
struct MvpAdapterContextFactory;

impl AdapterContextFactory for MvpAdapterContextFactory {
    fn create(&self, seed: AdapterContextSeed) -> AdapterContextFuture<'_> {
        Box::pin(async move {
            let trace_parent = format!("00-{}-0000000000000001-01", trace_id(&seed));
            Ok(AdapterCallContext {
                tenant_id: seed.tenant_id,
                execution_id: seed.execution_id,
                correlation_id: seed.correlation_id,
                causation_id: None,
                idempotency_key: seed.idempotency_key,
                request_hash: seed.request_hash,
                deadline: UnixMicros::new(crate::identity::now_micros().saturating_add(60_000_000)),
                trace_context: TraceContext {
                    trace_parent,
                    trace_state: None,
                },
                auth: ResolvedAuth::new("mock", "local-development-only"),
            })
        })
    }
}

#[derive(Clone, Debug, Default)]
struct MvpRetrySchedule;

impl AdapterRetrySchedule for MvpRetrySchedule {
    fn retry_at(
        &self,
        error: &AdapterError,
        _attempt: u64,
    ) -> Result<UnixMicros, ExternalDispatchError> {
        let delay = error.retry_after.unwrap_or(DurationMicros::new(1_000_000));
        let delay = i64::try_from(delay.get()).map_err(|_| ExternalDispatchError {
            safe_message: "Adapter retry delay exceeds timestamp range".to_owned(),
        })?;
        Ok(UnixMicros::new(
            crate::identity::now_micros().saturating_add(delay),
        ))
    }
}

/// Builds the registered Mock Agent dispatcher used by the MVP service.
///
/// # Errors
///
/// Returns an Adapter registration error if the Endpoint/version binding is invalid.
pub fn mock_dispatcher(
    store: PostgresStore,
    endpoint_id: EndpointId,
    agent_version_id: AgentVersionId,
) -> Result<SharedExternalDispatcher, agent_loom_runtime::AdapterRegistrationError> {
    let mut registry = StaticAdapterRegistry::new();
    registry.register_tool(Arc::new(MockDevOpsToolAdapter::default()))?;
    registry.register_agent(
        endpoint_id,
        agent_version_id,
        Arc::new(MockDeliveryAgentAdapter),
    )?;
    Ok(SharedExternalDispatcher(Arc::new(
        AdapterRecoveryDispatcher::new(
            store,
            Arc::new(registry),
            Arc::new(MvpAdapterContextFactory),
            Arc::new(MvpRetrySchedule),
        ),
    )))
}

#[derive(Clone)]
pub struct SharedExternalDispatcher(Arc<dyn ExternalRecoveryDispatcher>);

impl fmt::Debug for SharedExternalDispatcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SharedExternalDispatcher")
            .finish_non_exhaustive()
    }
}

impl SharedExternalDispatcher {
    pub(crate) fn new(dispatcher: Arc<dyn ExternalRecoveryDispatcher>) -> Self {
        Self(dispatcher)
    }
}

impl ExternalRecoveryDispatcher for SharedExternalDispatcher {
    fn dispatch(&self, started: StartedRecovery) -> DispatchFuture<'_> {
        self.0.dispatch(started)
    }
}

fn trace_id(seed: &AdapterContextSeed) -> String {
    let bytes: [u8; 32] = Sha256::digest(format!("{:?}", seed.execution_id).as_bytes()).into();
    hex(&bytes[..16])
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(value, "{byte:02x}");
    }
    value
}

fn digest(bytes: &[u8]) -> Digest {
    Digest::from_bytes(Sha256::digest(bytes).into())
}

fn json_payload(value: &serde_json::Value) -> JsonPayload {
    JsonPayload::from_validated_bytes(
        serde_json::to_vec(&value).expect("static Mock Adapter JSON is serializable"),
    )
}

fn adapter_error(code: &'static str, message: &str) -> AdapterError {
    AdapterError {
        code,
        retry: agent_loom_adapter_core::AdapterRetryClass::Never,
        safe_message: message.to_owned(),
        remote_request_id: None,
        retry_after: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_adapter_declares_replay_and_artifact_capabilities() {
        let capabilities = MockDeliveryAgentAdapter.capabilities();
        assert!(capabilities.submission_idempotency);
        assert!(capabilities.artifact_output);
        let tool = MockDevOpsToolAdapter::default();
        assert_eq!(
            tool.descriptor().side_effect,
            SideEffectClass::IdempotentWrite
        );
        assert!(tool.descriptor().capabilities.query_outcome);
    }
}
