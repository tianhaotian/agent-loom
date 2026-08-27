use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use agent_loom_adapter_core::{
    AdapterCallContext, AgentRunRequest, AgentServerAdapter, ExecutionBudget, ExecutionId,
    ResolvedAuth, ToolRequest, TraceContext,
    conformance::{
        AgentConformanceFixture, ToolConformanceFixture, exercise_agent_server,
        exercise_devops_tool,
    },
};
use agent_loom_domain::{
    AgentExecutionId, CorrelationId, Digest, DurationMicros, IdempotencyKey, JsonPayload, TenantId,
    ToolExecutionId, UnixMicros,
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;

#[derive(Clone, Debug, Default)]
struct FakeRemoteState {
    agents: Arc<Mutex<HashMap<String, String>>>,
    deployments: Arc<Mutex<HashMap<String, String>>>,
}

#[derive(Deserialize)]
struct IdempotencyQuery {
    key: String,
}

async fn spawn_remote() -> String {
    let state = FakeRemoteState::default();
    let app = Router::new()
        .route("/v1/agent-runs", post(submit_agent))
        .route("/v1/agent-runs/by-idempotency", get(reconcile_agent))
        .route("/v1/agent-runs/{run_id}", get(agent_status))
        .route("/v1/agent-runs/{run_id}/events", get(agent_events))
        .route("/v1/agent-runs/{run_id}/stop", post(stop_agent))
        .route("/v1/deployments", post(deploy))
        .route("/v1/deployments/{operation_id}", get(deployment_status))
        .route("/v1/deployments/{operation_id}/rollback", post(rollback))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake remote");
    let address = listener.local_addr().expect("fake remote address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("fake remote remains available");
    });
    format!("http://{address}")
}

async fn submit_agent(
    State(state): State<FakeRemoteState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers)?;
    if body.get("instructions").and_then(Value::as_str).is_none() || body.get("budget").is_none() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let key = idempotency_key(&headers)?;
    let mut agents = state.agents.lock().expect("agent map lock");
    let next = format!("agent-run-{}", agents.len() + 1);
    let run_id = agents.entry(key).or_insert(next).clone();
    Ok(Json(agent_ref(&run_id)))
}

async fn reconcile_agent(
    State(state): State<FakeRemoteState>,
    headers: HeaderMap,
    Query(query): Query<IdempotencyQuery>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers)?;
    let agents = state.agents.lock().expect("agent map lock");
    agents
        .get(&query.key)
        .map(|run_id| Json(agent_ref(run_id)))
        .ok_or(StatusCode::NOT_FOUND)
}

async fn agent_status(
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers)?;
    Ok(Json(json!({
        "run_id": run_id,
        "session_id": "session-1",
        "protocol_version": HTTP_PROTOCOL_VERSION,
        "status": "completed",
        "result": {"artifact_uri": "https://artifacts.example.test/result.json"}
    })))
}

async fn agent_events(
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers)?;
    Ok(Json(json!({
        "events": [{
            "id": format!("{run_id}-completed"),
            "sequence": 1,
            "kind": "run.completed",
            "authoritative": true,
            "payload": {"run_id": run_id, "status": "completed"}
        }],
        "next_cursor": "1",
        "terminal": true
    })))
}

async fn stop_agent(
    headers: HeaderMap,
    Path(_run_id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers)?;
    Ok(Json(json!({
        "status": "already_terminal",
        "terminal_status": "completed"
    })))
}

async fn deploy(
    State(state): State<FakeRemoteState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers)?;
    if body.get("operation").and_then(Value::as_str) != Some("deploy") {
        return Err(StatusCode::BAD_REQUEST);
    }
    let key = idempotency_key(&headers)?;
    let mut deployments = state.deployments.lock().expect("deployment map lock");
    let next = format!("deployment-{}", deployments.len() + 1);
    let operation = deployments.entry(key).or_insert(next).clone();
    Ok(Json(
        json!({"status": "accepted", "external_ref": operation}),
    ))
}

async fn deployment_status(
    headers: HeaderMap,
    Path(operation_id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers)?;
    Ok(Json(json!({
        "status": "completed",
        "healthy": true,
        "result": {
            "operation_ref": operation_id,
            "release": "phase2a",
            "health": "healthy"
        }
    })))
}

async fn rollback(
    headers: HeaderMap,
    Path(operation_id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    authorize(&headers)?;
    Ok(Json(json!({
        "status": "completed",
        "result": {"operation_ref": operation_id, "rolled_back": true}
    })))
}

fn authorize(headers: &HeaderMap) -> Result<(), StatusCode> {
    (headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        == Some("Bearer phase2a-secret"))
    .then_some(())
    .ok_or(StatusCode::UNAUTHORIZED)
}

fn idempotency_key(headers: &HeaderMap) -> Result<String, StatusCode> {
    headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .ok_or(StatusCode::BAD_REQUEST)
}

fn agent_ref(run_id: &str) -> Value {
    json!({
        "run_id": run_id,
        "session_id": "session-1",
        "protocol_version": HTTP_PROTOCOL_VERSION
    })
}

#[tokio::test]
async fn real_http_profiles_satisfy_phase2a_conformance() {
    let base_url = spawn_remote().await;
    let endpoint = HttpEndpointConfig::new(&base_url).expect("loopback endpoint");
    let agent = HttpAgentServerAdapter::new(endpoint.clone());
    let tool = HttpDevOpsToolAdapter::new(endpoint);

    let agent_context = context(ExecutionId::Agent(AgentExecutionId::from_bytes([2; 16])));
    let agent_report = exercise_agent_server(
        &agent,
        AgentConformanceFixture {
            context: agent_context,
            request: AgentRunRequest {
                instructions: "produce the release artifact".to_owned(),
                input: payload(&json!({"release": "phase2a"})),
                budget: ExecutionBudget {
                    max_duration: DurationMicros::new(5_000_000),
                    max_output_bytes: 1_048_576,
                },
            },
            event_limits: agent_loom_adapter_core::EventReadLimits {
                max_events: 100,
                max_bytes: 1_048_576,
                max_wait: DurationMicros::new(500_000),
            },
        },
    )
    .await
    .expect("HTTP Agent profile conforms");
    assert_eq!(agent_report.event_count, 1);

    let tool_context = context(ExecutionId::Tool(ToolExecutionId::from_bytes([3; 16])));
    let tool_report = exercise_devops_tool(
        &tool,
        ToolConformanceFixture {
            context: tool_context,
            request: ToolRequest {
                input: payload(&json!({
                    "operation": "deploy",
                    "environment": "staging",
                    "release": {"digest": "sha256:phase2a"}
                })),
            },
            compensation_input: payload(&json!({"reason": "conformance rollback"})),
        },
    )
    .await
    .expect("HTTP DevOps profile conforms");
    assert_eq!(tool_report.external_ref, "deployment-1");
}

#[tokio::test]
async fn authentication_errors_are_classified_without_leaking_credentials() {
    let base_url = spawn_remote().await;
    let adapter =
        HttpAgentServerAdapter::new(HttpEndpointConfig::new(&base_url).expect("loopback endpoint"));
    let mut call = context(ExecutionId::Agent(AgentExecutionId::from_bytes([4; 16])));
    call.auth = ResolvedAuth::new("bearer", "wrong-secret-that-must-not-leak");
    let error = adapter
        .submit(
            &call,
            AgentRunRequest {
                instructions: "test auth".to_owned(),
                input: payload(&json!({})),
                budget: ExecutionBudget {
                    max_duration: DurationMicros::new(1_000_000),
                    max_output_bytes: 1024,
                },
            },
        )
        .await
        .expect_err("bad auth must fail");
    assert_eq!(error.code, "AUTHENTICATION_FAILED");
    assert!(!format!("{error:?}").contains("wrong-secret"));
}

#[test]
fn endpoint_policy_requires_tls_outside_loopback() {
    assert!(HttpEndpointConfig::new("https://agent.example.test/api").is_ok());
    assert!(HttpEndpointConfig::new("http://agent.example.test/api").is_err());
    assert!(HttpEndpointConfig::new("https://token@agent.example.test/api").is_err());
}

fn context(execution_id: ExecutionId) -> AdapterCallContext {
    AdapterCallContext {
        tenant_id: TenantId::from_bytes([1; 16]),
        execution_id,
        correlation_id: CorrelationId::from_bytes([5; 16]),
        causation_id: None,
        idempotency_key: IdempotencyKey::parse("phase2a-conformance-key").expect("idempotency key"),
        request_hash: Digest::from_bytes([6; 32]),
        deadline: UnixMicros::new(now_micros().saturating_add(5_000_000)),
        trace_context: TraceContext {
            trace_parent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01".to_owned(),
            trace_state: None,
        },
        auth: ResolvedAuth::new("bearer", "phase2a-secret"),
    }
}

fn payload(value: &Value) -> JsonPayload {
    JsonPayload::from_validated_bytes(serde_json::to_vec(value).expect("JSON payload"))
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
