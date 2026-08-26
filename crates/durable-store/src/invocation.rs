use agent_loom_domain::{
    AgentExecutionId, AgentVersionId, Digest, EndpointId, IdempotencyKey, JsonPayload, RunId,
    ScopeKey, TenantId, ToolExecutionId,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolInvocation {
    pub tenant_id: TenantId,
    pub tool_execution_id: ToolExecutionId,
    pub run_id: RunId,
    pub tool_name: String,
    pub idempotency_scope: ScopeKey,
    pub idempotency_key: IdempotencyKey,
    pub request_hash: Digest,
    pub request: JsonPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentInvocation {
    pub tenant_id: TenantId,
    pub agent_execution_id: AgentExecutionId,
    pub run_id: RunId,
    pub endpoint_id: EndpointId,
    pub agent_version_id: AgentVersionId,
    pub idempotency_key: IdempotencyKey,
    pub request_hash: Digest,
    pub request: JsonPayload,
    pub capabilities_snapshot: JsonPayload,
}
