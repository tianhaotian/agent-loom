//! Service-mode Agent Server and Tool adapter contracts.

pub mod conformance;

use std::{error::Error, fmt, future::Future, pin::Pin};

use agent_loom_domain::{
    AgentExecutionId, CausationId, CorrelationId, Digest, DurationMicros, IdempotencyKey,
    JsonPayload, TenantId, ToolExecutionId, UnixMicros,
};

pub type AdapterFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, AdapterError>> + Send + 'a>>;

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentCapabilities {
    pub submission_idempotency: bool,
    pub submission_reconciliation: bool,
    pub status_query: bool,
    pub resumable_events: bool,
    pub cooperative_stop: bool,
    pub approvals: bool,
    pub guidance: bool,
    pub artifact_output: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ToolCapabilities {
    pub query_outcome: bool,
    pub compensation: bool,
    pub asynchronous_result: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExecutionId {
    Agent(AgentExecutionId),
    Tool(ToolExecutionId),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_parent: String,
    pub trace_state: Option<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ResolvedAuth {
    scheme: String,
    secret: String,
}

impl ResolvedAuth {
    pub fn new(scheme: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            scheme: scheme.into(),
            secret: secret.into(),
        }
    }

    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    pub fn expose_secret(&self) -> &str {
        &self.secret
    }
}

impl fmt::Debug for ResolvedAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedAuth")
            .field("scheme", &self.scheme)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdapterCallContext {
    pub tenant_id: TenantId,
    pub execution_id: ExecutionId,
    pub correlation_id: CorrelationId,
    pub causation_id: Option<CausationId>,
    pub idempotency_key: IdempotencyKey,
    pub request_hash: Digest,
    pub deadline: UnixMicros,
    pub trace_context: TraceContext,
    pub auth: ResolvedAuth,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentRunRequest {
    pub instructions: String,
    pub input: JsonPayload,
    pub budget: ExecutionBudget,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionBudget {
    pub max_duration: DurationMicros,
    pub max_output_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteAgentRef {
    pub remote_run_id: String,
    pub remote_session_id: Option<String>,
    pub protocol_version: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteAgentStatus {
    Accepted,
    Running,
    WaitingForApproval,
    WaitingForInput,
    Stopping,
    Completed,
    Failed,
    Cancelled,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteAgentSnapshot {
    pub remote: RemoteAgentRef,
    pub status: RemoteAgentStatus,
    pub result: Option<JsonPayload>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteEventBatch {
    pub events: Vec<NormalizedAgentEvent>,
    pub next_cursor: Option<String>,
    pub terminal: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NormalizedAgentEvent {
    pub source_event_id: Option<String>,
    pub source_sequence: Option<u64>,
    pub kind: String,
    pub authoritative: bool,
    pub payload: JsonPayload,
    pub raw_digest: Digest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventReadLimits {
    pub max_events: u32,
    pub max_bytes: u64,
    pub max_wait: DurationMicros,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmitAgentOutcome {
    Accepted(RemoteAgentRef),
    SubmissionUncertain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopRequestOutcome {
    Accepted { cooperative: bool },
    AlreadyTerminal { status: RemoteAgentStatus },
    Unsupported,
    Uncertain,
}

pub trait AgentServerAdapter: Send + Sync {
    fn kind(&self) -> &'static str;
    fn capabilities(&self) -> AgentCapabilities;

    fn submit<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        request: AgentRunRequest,
    ) -> AdapterFuture<'a, SubmitAgentOutcome>;

    fn reconcile_submission<'a>(
        &'a self,
        context: &'a AdapterCallContext,
    ) -> AdapterFuture<'a, Option<RemoteAgentRef>>;

    fn get_status<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        remote: &'a RemoteAgentRef,
    ) -> AdapterFuture<'a, RemoteAgentSnapshot>;

    fn read_events<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        remote: &'a RemoteAgentRef,
        cursor: Option<&'a str>,
        limits: EventReadLimits,
    ) -> AdapterFuture<'a, RemoteEventBatch>;

    fn request_stop<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        remote: &'a RemoteAgentRef,
        reason: &'a str,
    ) -> AdapterFuture<'a, StopRequestOutcome>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SideEffectClass {
    ReadOnly,
    IdempotentWrite,
    NonIdempotentWrite,
    CompensatableWrite,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolDescriptor {
    pub tool_key: String,
    pub side_effect: SideEffectClass,
    pub capabilities: ToolCapabilities,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolRequest {
    pub input: JsonPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolCallOutcome {
    Completed(JsonPayload),
    Accepted { external_ref: String },
    Uncertain { external_ref: Option<String> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolQueryOutcome {
    Pending,
    Completed(JsonPayload),
    Failed { code: String },
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompensationRequest {
    pub external_ref: String,
    pub input: JsonPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompensationOutcome {
    Completed(JsonPayload),
    Accepted { external_ref: String },
    Uncertain,
}

pub trait ToolAdapter: Send + Sync {
    fn descriptor(&self) -> &ToolDescriptor;

    fn execute<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        request: ToolRequest,
    ) -> AdapterFuture<'a, ToolCallOutcome>;

    fn query_outcome<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        external_ref: &'a str,
    ) -> AdapterFuture<'a, ToolQueryOutcome>;

    fn compensate<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        request: CompensationRequest,
    ) -> AdapterFuture<'a, CompensationOutcome>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdapterRetryClass {
    Never,
    SameRequestBackoff,
    ReconnectAndResume,
    QueryOutcome,
    ManualReview,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdapterError {
    pub code: &'static str,
    pub retry: AdapterRetryClass,
    pub safe_message: String,
    pub remote_request_id: Option<String>,
    pub retry_after: Option<DurationMicros>,
}

impl fmt::Display for AdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl Error for AdapterError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapter_traits_are_dyn_compatible() {
        fn accepts_agent(_: Option<&dyn AgentServerAdapter>) {}
        fn accepts_tool(_: Option<&dyn ToolAdapter>) {}

        accepts_agent(None);
        accepts_tool(None);
    }

    #[test]
    fn resolved_auth_debug_output_is_redacted() {
        let auth = ResolvedAuth::new("bearer", "do-not-log");
        let debug = format!("{auth:?}");

        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("do-not-log"));
        assert_eq!(auth.expose_secret(), "do-not-log");
    }
}
