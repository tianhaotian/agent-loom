use std::{
    env,
    error::Error,
    time::{SystemTime, UNIX_EPOCH},
};

use agent_loom_adapter_core::{
    AdapterCallContext, AgentServerAdapter, EventReadLimits, ExecutionId, RemoteAgentRef,
    ResolvedAuth, ToolAdapter, ToolQueryOutcome, TraceContext,
};
use agent_loom_adapter_http::{
    HTTP_PROTOCOL_VERSION, HttpAgentServerAdapter, HttpDevOpsToolAdapter, HttpEndpointConfig,
};
use agent_loom_domain::{
    AgentExecutionId, CorrelationId, Digest, DurationMicros, IdempotencyKey, TenantId,
    ToolExecutionId, UnixMicros,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let agent_url = required("AGENT_LOOM_AGENT_BASE_URL")?;
    let agent_token = required("AGENT_LOOM_AGENT_TOKEN")?;
    let devops_url = required("AGENT_LOOM_DEVOPS_BASE_URL")?;
    let devops_token = required("AGENT_LOOM_DEVOPS_TOKEN")?;
    let idempotency_key = env::var("AGENT_LOOM_LIVE_IDEMPOTENCY_KEY")
        .unwrap_or_else(|_| "agent-loom-live-probe".to_owned());

    let agent = HttpAgentServerAdapter::new(HttpEndpointConfig::new(&agent_url)?);
    let agent_context = context(
        ExecutionId::Agent(AgentExecutionId::from_bytes([2; 16])),
        &idempotency_key,
        &agent_token,
    )?;
    let remote = match env::var("AGENT_LOOM_LIVE_AGENT_RUN_ID") {
        Ok(run_id) => Some(RemoteAgentRef {
            remote_run_id: run_id,
            remote_session_id: None,
            protocol_version: HTTP_PROTOCOL_VERSION.to_owned(),
        }),
        Err(_) => agent.reconcile_submission(&agent_context).await?,
    };
    if let Some(remote) = remote {
        let snapshot = agent.get_status(&agent_context, &remote).await?;
        let events = agent
            .read_events(
                &agent_context,
                &remote,
                None,
                EventReadLimits {
                    max_events: 10,
                    max_bytes: 1_048_576,
                    max_wait: DurationMicros::new(100_000),
                },
            )
            .await?;
        println!(
            "agent endpoint reachable: status={:?}, events={}, terminal={}",
            snapshot.status,
            events.events.len(),
            events.terminal
        );
    } else {
        println!("agent endpoint reachable: no Run found for the configured idempotency key");
    }

    let tool = HttpDevOpsToolAdapter::new(HttpEndpointConfig::new(&devops_url)?);
    if let Ok(external_ref) = env::var("AGENT_LOOM_LIVE_DEPLOYMENT_REF") {
        let tool_context = context(
            ExecutionId::Tool(ToolExecutionId::from_bytes([3; 16])),
            &idempotency_key,
            &devops_token,
        )?;
        let outcome = tool.query_outcome(&tool_context, &external_ref).await?;
        let state = match outcome {
            ToolQueryOutcome::Pending => "pending",
            ToolQueryOutcome::Completed(_) => "completed_healthy",
            ToolQueryOutcome::Failed { .. } => "failed",
            ToolQueryOutcome::Unknown => "unknown",
        };
        println!("devops endpoint reachable: deployment_status={state}");
    } else {
        println!(
            "devops endpoint configured; set AGENT_LOOM_LIVE_DEPLOYMENT_REF for a read-only status probe"
        );
    }
    Ok(())
}

fn context(
    execution_id: ExecutionId,
    idempotency_key: &str,
    token: &str,
) -> Result<AdapterCallContext, Box<dyn Error>> {
    Ok(AdapterCallContext {
        tenant_id: TenantId::from_bytes([1; 16]),
        execution_id,
        correlation_id: CorrelationId::from_bytes([4; 16]),
        causation_id: None,
        idempotency_key: IdempotencyKey::parse(idempotency_key.to_owned())?,
        request_hash: Digest::from_bytes([5; 32]),
        deadline: UnixMicros::new(now_micros().saturating_add(10_000_000)),
        trace_context: TraceContext {
            trace_parent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01".to_owned(),
            trace_state: None,
        },
        auth: ResolvedAuth::new("bearer", token),
    })
}

fn required(name: &'static str) -> Result<String, Box<dyn Error>> {
    env::var(name)
        .map_err(|_| format!("{name} must be configured for the live endpoint probe").into())
}

fn now_micros() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_micros(),
    )
    .unwrap_or(i64::MAX)
}
