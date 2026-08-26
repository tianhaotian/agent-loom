use std::{
    collections::VecDeque,
    convert::Infallible,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use agent_loom_domain::{
    AgentVersionId, CheckpointId, EventId, JsonPayload, LogicalKey, RunId, RunSnapshot, RunStatus,
    StageStatus, TaskId, TaskKind, TenantId, UnixMicros, WaitStatus, WorkflowId, WorkflowVersionId,
};
use agent_loom_durable_store::{
    ApplyEvent, CommandDisposition, ControlRun, CreateRun, DurableStore, EventCursor, ExpectedRun,
    InitialStage, InitialTask, NewCheckpoint, QueryContext, SignatureVerification, StoreError,
    StoreErrorCode,
};
use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware,
    middleware::Next,
    response::{
        IntoResponse, Response, Sse,
        sse::{Event as SseEvent, KeepAlive},
    },
    routing::{get, post},
};
use futures_util::{Stream, stream};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    identity::{command_context, decode_id, hash_bytes, now_micros, random_id},
    worker::{DELIVERY_EXECUTION_STAGES, DELIVERY_STAGES, initial_task_input, stage_id},
};

const API_ACTOR: &str = "agent-loom-http-api";

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn DurableStore>,
    pub tenant_id: TenantId,
    pub workflow_id: WorkflowId,
    pub workflow_version_id: WorkflowVersionId,
    pub coordinator_agent_version_id: AgentVersionId,
    pub api_key: Arc<str>,
}

impl fmt::Debug for AppState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AppState")
            .field("tenant_id", &self.tenant_id)
            .field("workflow_id", &self.workflow_id)
            .finish_non_exhaustive()
    }
}

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/v1/workflows/{workflow_id}", get(get_workflow))
        .route("/v1/runs", post(create_run))
        .route("/v1/runs/{run_id}", get(get_run))
        .route(
            "/v1/runs/{run_id}/events",
            get(list_events).post(apply_event),
        )
        .route("/v1/runs/{run_id}/events/stream", get(stream_events))
        .route("/v1/runs/{run_id}/stages", get(list_stages))
        .route("/v1/runs/{run_id}/artifacts", get(list_artifacts))
        .route(
            "/v1/runs/{run_id}/pending-actions",
            get(list_pending_actions),
        )
        .route("/v1/runs/{run_id}/pause", post(pause_run))
        .route("/v1/runs/{run_id}/resume", post(resume_run))
        .route("/v1/runs/{run_id}/cancel", post(cancel_run))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_key,
        ));
    Router::new()
        .route("/healthz", get(health))
        .merge(protected)
        .with_state(state)
}

async fn require_api_key(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let started = Instant::now();
    let method = request.method().to_string();
    let path = request.uri().path().to_owned();
    let request_id = hex(&random_id());
    let bearer = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let direct = request
        .headers()
        .get("x-api-key")
        .and_then(|value| value.to_str().ok());
    if bearer.or(direct) != Some(state.api_key.as_ref()) {
        log_request(
            &request_id,
            &method,
            &path,
            StatusCode::UNAUTHORIZED,
            started.elapsed(),
        );
        return Err(ApiError::unauthorized("a valid API key is required"));
    }
    let mut response = next.run(request).await;
    let status = response.status();
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-correlation-id", value);
    }
    log_request(&request_id, &method, &path, status, started.elapsed());
    Ok(response)
}

fn log_request(request_id: &str, method: &str, path: &str, status: StatusCode, elapsed: Duration) {
    let duration_micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    println!(
        "{}",
        json!({
            "timestamp_micros": now_micros(),
            "level": "info",
            "kind": "http.request",
            "request_id": request_id,
            "method": method,
            "path": path,
            "status": status.as_u16(),
            "duration_micros": duration_micros,
        })
    );
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok", "service": "agent-loom"}))
}

#[derive(Debug, Serialize)]
struct WorkflowResponse {
    workflow_id: String,
    workflow_version_id: String,
    workflow_key: String,
    name: String,
    status: String,
    version: u64,
    lifecycle: String,
    spec: Value,
    updated_at_micros: i64,
}

async fn get_workflow(
    State(state): State<AppState>,
    Path(workflow_id): Path<String>,
) -> ApiResult<Json<WorkflowResponse>> {
    let workflow_id = decode_id(&workflow_id)
        .map(WorkflowId::from_bytes)
        .map_err(ApiError::bad_request)?;
    state
        .store
        .get_workflow(&query_context(&state), workflow_id)
        .await?
        .map(|workflow| {
            Json(WorkflowResponse {
                workflow_id: workflow.workflow_id.to_string(),
                workflow_version_id: workflow.workflow_version_id.to_string(),
                workflow_key: workflow.workflow_key,
                name: workflow.name,
                status: workflow.status,
                version: workflow.version,
                lifecycle: workflow.lifecycle,
                spec: serde_json::from_slice(workflow.spec.as_bytes()).unwrap_or(Value::Null),
                updated_at_micros: workflow.updated_at.get(),
            })
        })
        .ok_or_else(|| ApiError::not_found("Workflow was not found"))
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateRunRequest {
    pub input: Value,
    #[serde(default)]
    pub deadline_micros: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct CreateRunResponse {
    pub run: RunResponse,
    pub disposition: &'static str,
}

#[allow(clippy::too_many_lines)]
async fn create_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateRunRequest>,
) -> ApiResult<(StatusCode, Json<CreateRunResponse>)> {
    if request
        .deadline_micros
        .is_some_and(|deadline| deadline <= now_micros())
    {
        return Err(ApiError::bad_request(
            "deadline_micros must be in the future",
        ));
    }
    let run_id = RunId::from_bytes(random_id());
    let idempotency = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map_or_else(|| run_id.to_string(), ToOwned::to_owned);
    let identity = format!("api-create/{idempotency}");
    let request_bytes = serde_json::to_vec(&request)
        .map_err(|_| ApiError::bad_request("request cannot be encoded as JSON"))?;
    let context = command_context(
        state.tenant_id,
        run_id,
        API_ACTOR,
        "create_run",
        &identity,
        &request_bytes,
    )
    .map_err(ApiError::bad_request)?;
    let initial_event_id = EventId::from_bytes(random_id());
    let initial_state = json!({
        "workflow": "delivery-mvp",
        "completed_steps": 0,
        "total_steps": DELIVERY_EXECUTION_STAGES.len(),
        "request": request.input,
    });
    let initial_state_payload = payload(&initial_state)?;
    let command = CreateRun {
        run_id,
        workflow_version_id: Some(state.workflow_version_id),
        coordinator_agent_version_id: Some(state.coordinator_agent_version_id),
        input: payload(&request.input)?,
        deadline: request.deadline_micros.map(UnixMicros::new),
        initial_event_id,
        initial_checkpoint: NewCheckpoint {
            checkpoint_id: CheckpointId::from_bytes(random_id()),
            sequence: 1,
            schema_version: 1,
            workflow_version_id: Some(state.workflow_version_id),
            coordinator_agent_version_id: Some(state.coordinator_agent_version_id),
            execution_generation: 0,
            state_digest: hash_bytes(initial_state_payload.as_bytes()),
            state: initial_state_payload,
            created_event_id: initial_event_id,
        },
        initial_stages: DELIVERY_STAGES
            .iter()
            .enumerate()
            .map(|(step, stage)| {
                Ok(InitialStage {
                    stage_execution_id: stage_id(run_id, step),
                    stage_key: LogicalKey::parse(format!("delivery/{stage}"))
                        .map_err(|_| ApiError::internal("generated Stage key is invalid"))?,
                    definition_stage_key: LogicalKey::parse((*stage).to_owned())
                        .map_err(|_| ApiError::internal("definition Stage key is invalid"))?,
                    status: if step == 0 {
                        StageStatus::Active
                    } else {
                        StageStatus::Planned
                    },
                    attempt: 1,
                    assignee_kind: Some("agent".to_owned()),
                    assignee_ref: Some("mock-delivery-agent".to_owned()),
                    input_contract: payload(&json!({"type": "object"}))?,
                    output_contract: payload(&json!({
                        "type": "object",
                        "required": ["stage", "status"]
                    }))?,
                    policy: payload(&json!({"max_attempts": 3}))?,
                })
            })
            .collect::<ApiResult<Vec<_>>>()?,
        initial_tasks: vec![InitialTask {
            task_id: TaskId::from_bytes(random_id()),
            stage_execution_id: Some(stage_id(run_id, 0)),
            logical_key: LogicalKey::parse(format!("delivery/{run_id}/requirements"))
                .map_err(|_| ApiError::internal("generated Task key is invalid"))?,
            kind: TaskKind::AgentServer,
            priority: 10,
            available_at: UnixMicros::new(now_micros()),
            max_attempts: 3,
            input: initial_task_input(request.input)
                .map_err(|_| ApiError::bad_request("input cannot be encoded as JSON"))?,
        }],
    };
    let committed = state.store.create_run(&context, command).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(CreateRunResponse {
            run: committed.value.into(),
            disposition: disposition(committed.disposition),
        }),
    ))
}

async fn get_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<RunResponse>> {
    let run_id = parse_run_id(&run_id)?;
    state
        .store
        .get_run(&query_context(&state), run_id)
        .await?
        .map(RunResponse::from)
        .map(Json)
        .ok_or_else(|| ApiError::not_found("Run was not found"))
}

#[derive(Debug, Deserialize)]
struct EventQuery {
    #[serde(default)]
    after: u64,
    #[serde(default = "default_event_limit")]
    limit: u32,
}

const fn default_event_limit() -> u32 {
    100
}

#[derive(Debug, Serialize)]
struct EventResponse {
    event_id: String,
    sequence: u64,
    event_type: String,
    payload_schema_version: u32,
    payload: Value,
    occurred_at_micros: Option<i64>,
    recorded_at_micros: i64,
}

#[derive(Debug, Serialize)]
struct EventPageResponse {
    events: Vec<EventResponse>,
    next_after_sequence: Option<u64>,
}

async fn list_events(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    Query(query): Query<EventQuery>,
) -> ApiResult<Json<EventPageResponse>> {
    if query.limit == 0 || query.limit > 500 {
        return Err(ApiError::bad_request("limit must be between 1 and 500"));
    }
    let page = state
        .store
        .list_events(
            &query_context(&state),
            EventCursor {
                run_id: parse_run_id(&run_id)?,
                after_sequence: query.after,
                limit: query.limit,
            },
        )
        .await?;
    Ok(Json(EventPageResponse {
        events: page
            .events
            .into_iter()
            .map(|event| EventResponse {
                event_id: event.event_id.to_string(),
                sequence: event.sequence,
                event_type: event.event_type,
                payload_schema_version: event.payload_schema_version,
                payload: serde_json::from_slice(event.payload.as_bytes()).unwrap_or(Value::Null),
                occurred_at_micros: event.occurred_at.map(UnixMicros::get),
                recorded_at_micros: event.recorded_at.get(),
            })
            .collect(),
        next_after_sequence: page.next_after_sequence,
    }))
}

struct EventStreamState {
    store: Arc<dyn DurableStore>,
    query: QueryContext,
    run_id: RunId,
    after_sequence: u64,
    pending: VecDeque<agent_loom_domain::EventRecord>,
    failed: bool,
}

async fn stream_events(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    Query(query): Query<EventQuery>,
    headers: HeaderMap,
) -> ApiResult<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>> {
    let run_id = parse_run_id(&run_id)?;
    load_run(&state, run_id).await?;
    let after_sequence = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(query.after);
    let query = query_context(&state);
    let stream = stream::unfold(
        EventStreamState {
            store: state.store,
            query,
            run_id,
            after_sequence,
            pending: VecDeque::new(),
            failed: false,
        },
        next_sse_event,
    );
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

async fn next_sse_event(
    mut state: EventStreamState,
) -> Option<(Result<SseEvent, Infallible>, EventStreamState)> {
    if state.failed {
        return None;
    }
    loop {
        if let Some(event) = state.pending.pop_front() {
            state.after_sequence = event.sequence;
            let response = event_response(event);
            let data = serde_json::to_string(&response)
                .unwrap_or_else(|_| "{\"error\":{\"code\":\"serialization_error\"}}".to_owned());
            return Some((
                Ok(SseEvent::default()
                    .id(response.sequence.to_string())
                    .event("run.event")
                    .data(data)),
                state,
            ));
        }
        let page = match state
            .store
            .list_events(
                &state.query,
                EventCursor {
                    run_id: state.run_id,
                    after_sequence: state.after_sequence,
                    limit: 100,
                },
            )
            .await
        {
            Ok(page) => page,
            Err(error) => {
                let data = json!({"error": {"code": "store_error", "message": error.safe_message}});
                state.failed = true;
                return Some((
                    Ok(SseEvent::default()
                        .event("stream.error")
                        .data(data.to_string())),
                    state,
                ));
            }
        };
        state.pending.extend(page.events);
        if !state.pending.is_empty() {
            continue;
        }
        match state.store.get_run(&state.query, state.run_id).await {
            Ok(Some(run)) if run.status.is_terminal() => return None,
            Ok(Some(_)) => tokio::time::sleep(Duration::from_millis(250)).await,
            Ok(None) | Err(_) => return None,
        }
    }
}

fn event_response(event: agent_loom_domain::EventRecord) -> EventResponse {
    EventResponse {
        event_id: event.event_id.to_string(),
        sequence: event.sequence,
        event_type: event.event_type,
        payload_schema_version: event.payload_schema_version,
        payload: serde_json::from_slice(event.payload.as_bytes()).unwrap_or(Value::Null),
        occurred_at_micros: event.occurred_at.map(UnixMicros::get),
        recorded_at_micros: event.recorded_at.get(),
    }
}

#[derive(Debug, Serialize)]
struct StageResponse {
    stage_execution_id: String,
    stage_key: String,
    definition_stage_key: Option<String>,
    status: &'static str,
    version: u64,
    attempt: u32,
    assignee_kind: Option<String>,
    assignee_ref: Option<String>,
    started_at_micros: Option<i64>,
    completed_at_micros: Option<i64>,
}

async fn list_stages(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<Vec<StageResponse>>> {
    let run_id = parse_run_id(&run_id)?;
    load_run(&state, run_id).await?;
    let stages = state
        .store
        .list_stages(&query_context(&state), run_id)
        .await?
        .into_iter()
        .map(|stage| StageResponse {
            stage_execution_id: stage.stage_execution_id.to_string(),
            stage_key: stage.stage_key.into_string(),
            definition_stage_key: stage.definition_stage_key.map(LogicalKey::into_string),
            status: stage_status(stage.status),
            version: stage.version,
            attempt: stage.attempt,
            assignee_kind: stage.assignee_kind,
            assignee_ref: stage.assignee_ref,
            started_at_micros: stage.started_at.map(UnixMicros::get),
            completed_at_micros: stage.completed_at.map(UnixMicros::get),
        })
        .collect();
    Ok(Json(stages))
}

#[derive(Debug, Serialize)]
struct ArtifactResponse {
    artifact_id: String,
    stage_execution_id: Option<String>,
    task_id: Option<String>,
    logical_key: String,
    kind: String,
    version: u64,
    uri: String,
    digest: String,
    media_type: String,
    size_bytes: u64,
    metadata: Value,
    produced_by: String,
    created_at_micros: i64,
}

async fn list_artifacts(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<Vec<ArtifactResponse>>> {
    let run_id = parse_run_id(&run_id)?;
    load_run(&state, run_id).await?;
    let artifacts = state
        .store
        .list_artifacts(&query_context(&state), run_id)
        .await?
        .into_iter()
        .map(|artifact| ArtifactResponse {
            artifact_id: artifact.artifact_id.to_string(),
            stage_execution_id: artifact.stage_execution_id.map(|id| id.to_string()),
            task_id: artifact.task_id.map(|id| id.to_string()),
            logical_key: artifact.logical_key.into_string(),
            kind: artifact.kind,
            version: artifact.version,
            uri: artifact.uri,
            digest: hex(artifact.digest.as_bytes()),
            media_type: artifact.media_type,
            size_bytes: artifact.size_bytes,
            metadata: serde_json::from_slice(artifact.metadata.as_bytes()).unwrap_or(Value::Null),
            produced_by: artifact.produced_by,
            created_at_micros: artifact.created_at.get(),
        })
        .collect();
    Ok(Json(artifacts))
}

#[derive(Debug, Serialize)]
struct PendingActionResponse {
    wait_id: String,
    stage_execution_id: Option<String>,
    action_type: String,
    expected_event_type: String,
    expires_at_micros: Option<i64>,
}

async fn list_pending_actions(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<Vec<PendingActionResponse>>> {
    let run_id = parse_run_id(&run_id)?;
    load_run(&state, run_id).await?;
    let actions = state
        .store
        .list_waits(&query_context(&state), run_id)
        .await?
        .into_iter()
        .filter(|wait| wait.status == WaitStatus::Open)
        .map(|wait| PendingActionResponse {
            wait_id: wait.wait_id.to_string(),
            stage_execution_id: wait.stage_execution_id.map(|id| id.to_string()),
            action_type: wait.wait_type,
            expected_event_type: wait.expected_event_type,
            expires_at_micros: wait.expires_at.map(UnixMicros::get),
        })
        .collect();
    Ok(Json(actions))
}

#[derive(Debug, Deserialize, Serialize)]
struct ApplyEventRequest {
    event_type: String,
    #[serde(default)]
    match_key: String,
    #[serde(default = "default_schema_version")]
    payload_schema_version: u32,
    payload: Value,
    #[serde(default)]
    occurred_at_micros: Option<i64>,
}

const fn default_schema_version() -> u32 {
    1
}

async fn apply_event(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ApplyEventRequest>,
) -> ApiResult<Json<RunResponse>> {
    if request.event_type.is_empty() || request.payload_schema_version == 0 {
        return Err(ApiError::bad_request(
            "event_type must be non-empty and payload_schema_version must be positive",
        ));
    }
    let run_id = parse_run_id(&run_id)?;
    let run = load_run(&state, run_id).await?;
    let key = required_idempotency(&headers)?;
    let request_bytes = serde_json::to_vec(&request)
        .map_err(|_| ApiError::bad_request("event request cannot be encoded"))?;
    let identity = format!("api-event/{run_id}/{key}");
    let context = command_context(
        state.tenant_id,
        run_id,
        API_ACTOR,
        "apply_event",
        &identity,
        &request_bytes,
    )
    .map_err(ApiError::bad_request)?;
    let committed = state
        .store
        .apply_event(
            &context,
            ApplyEvent {
                expected_run: expected(&run),
                event_id: EventId::from_bytes(crate::identity::derived_id("event", &identity)),
                event_type: request.event_type,
                match_key_hash: hash_bytes(request.match_key.as_bytes()),
                payload_schema_version: request.payload_schema_version,
                payload: payload(&request.payload)?,
                signature_verification: SignatureVerification::NotRequired,
                occurred_at: request.occurred_at_micros.map(UnixMicros::new),
            },
        )
        .await?;
    Ok(Json(committed.value.into()))
}

#[derive(Debug, Deserialize)]
struct ControlRequest {
    reason: String,
}

async fn pause_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ControlRequest>,
) -> ApiResult<Json<RunResponse>> {
    control(state, run_id, headers, request, ControlAction::Pause).await
}

async fn resume_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ControlRequest>,
) -> ApiResult<Json<RunResponse>> {
    control(state, run_id, headers, request, ControlAction::Resume).await
}

async fn cancel_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ControlRequest>,
) -> ApiResult<Json<RunResponse>> {
    control(state, run_id, headers, request, ControlAction::Cancel).await
}

#[derive(Clone, Copy)]
enum ControlAction {
    Pause,
    Resume,
    Cancel,
}

impl ControlAction {
    const fn scope(self) -> &'static str {
        match self {
            Self::Pause => "pause_run",
            Self::Resume => "resume_run",
            Self::Cancel => "cancel_run",
        }
    }
}

async fn control(
    state: AppState,
    run_id: String,
    headers: HeaderMap,
    request: ControlRequest,
    action: ControlAction,
) -> ApiResult<Json<RunResponse>> {
    if request.reason.trim().is_empty() {
        return Err(ApiError::bad_request("reason must not be empty"));
    }
    let run_id = parse_run_id(&run_id)?;
    let run = load_run(&state, run_id).await?;
    let key = required_idempotency(&headers)?;
    let identity = format!("api-{}/{run_id}/{key}", action.scope());
    let context = command_context(
        state.tenant_id,
        run_id,
        API_ACTOR,
        action.scope(),
        &identity,
        request.reason.as_bytes(),
    )
    .map_err(ApiError::bad_request)?;
    let command = ControlRun {
        expected_run: expected(&run),
        event_id: EventId::from_bytes(crate::identity::derived_id("event", &identity)),
        reason: request.reason,
    };
    let committed = match action {
        ControlAction::Pause => state.store.pause_run(&context, command).await?,
        ControlAction::Resume => state.store.resume_run(&context, command).await?,
        ControlAction::Cancel => state.store.cancel_run(&context, command).await?,
    };
    Ok(Json(committed.value.into()))
}

#[derive(Debug, Serialize)]
pub struct RunResponse {
    pub tenant_id: String,
    pub run_id: String,
    pub status: &'static str,
    pub version: u64,
    pub execution_generation: u64,
    pub next_event_sequence: u64,
    pub deadline_micros: Option<i64>,
    pub updated_at_micros: i64,
}

impl From<RunSnapshot> for RunResponse {
    fn from(run: RunSnapshot) -> Self {
        Self {
            tenant_id: run.tenant_id.to_string(),
            run_id: run.run_id.to_string(),
            status: run_status(run.status),
            version: run.version,
            execution_generation: run.execution_generation,
            next_event_sequence: run.next_event_sequence,
            deadline_micros: run.deadline.map(UnixMicros::get),
            updated_at_micros: run.updated_at.get(),
        }
    }
}

fn run_status(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Queued => "queued",
        RunStatus::Running => "running",
        RunStatus::Waiting => "waiting",
        RunStatus::ApprovalRequired => "approval_required",
        RunStatus::Retrying => "retrying",
        RunStatus::Paused => "paused",
        RunStatus::Completed => "completed",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
        RunStatus::TimedOut => "timed_out",
    }
}

const fn stage_status(status: StageStatus) -> &'static str {
    match status {
        StageStatus::Planned => "planned",
        StageStatus::Active => "active",
        StageStatus::WaitingApproval => "waiting_approval",
        StageStatus::ReworkRequired => "rework_required",
        StageStatus::Succeeded => "succeeded",
        StageStatus::Failed => "failed",
        StageStatus::Skipped => "skipped",
        StageStatus::Cancelled => "cancelled",
    }
}

fn hex(bytes: &[u8]) -> String {
    use fmt::Write as _;
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(value, "{byte:02x}");
    }
    value
}

const fn disposition(value: CommandDisposition) -> &'static str {
    match value {
        CommandDisposition::Applied => "applied",
        CommandDisposition::Duplicate => "duplicate",
        CommandDisposition::NoOp => "no_op",
    }
}

fn parse_run_id(value: &str) -> ApiResult<RunId> {
    decode_id(value)
        .map(RunId::from_bytes)
        .map_err(ApiError::bad_request)
}

fn query_context(state: &AppState) -> QueryContext {
    QueryContext {
        tenant_id: state.tenant_id,
        actor_ref: API_ACTOR.to_owned(),
        authoritative: true,
    }
}

async fn load_run(state: &AppState, run_id: RunId) -> ApiResult<RunSnapshot> {
    state
        .store
        .get_run(&query_context(state), run_id)
        .await?
        .ok_or_else(|| ApiError::not_found("Run was not found"))
}

const fn expected(run: &RunSnapshot) -> ExpectedRun {
    ExpectedRun {
        run_id: run.run_id,
        version: Some(run.version),
        execution_generation: Some(run.execution_generation),
    }
}

fn required_idempotency(headers: &HeaderMap) -> ApiResult<String> {
    headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| ApiError::bad_request("Idempotency-Key header is required"))
}

fn payload(value: &Value) -> ApiResult<JsonPayload> {
    serde_json::to_vec(value)
        .map(JsonPayload::from_validated_bytes)
        .map_err(|_| ApiError::bad_request("JSON payload cannot be encoded"))
}

type ApiResult<T> = Result<T, ApiError>;

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "bad_request",
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: message.into(),
        }
    }
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        let status = match error.code {
            StoreErrorCode::NotFound => StatusCode::NOT_FOUND,
            StoreErrorCode::StoreUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            StoreErrorCode::InvalidTransition
            | StoreErrorCode::VersionConflict
            | StoreErrorCode::TerminalRun
            | StoreErrorCode::LeaseLost
            | StoreErrorCode::LeaseExpired
            | StoreErrorCode::IdempotencyKeyReused
            | StoreErrorCode::WaitMismatch
            | StoreErrorCode::WaitAlreadyConsumed
            | StoreErrorCode::WaitExpired
            | StoreErrorCode::DeadlineExceeded
            | StoreErrorCode::OutcomeUnknown
            | StoreErrorCode::PauseRecoveryRequired
            | StoreErrorCode::AdapterCapabilityMissing
            | StoreErrorCode::InconsistentProjection
            | StoreErrorCode::SerializationConflict => StatusCode::CONFLICT,
            StoreErrorCode::TenantMismatch
            | StoreErrorCode::ConstraintViolation
            | StoreErrorCode::MigrationRequired => StatusCode::BAD_REQUEST,
        };
        Self {
            status,
            code: "store_error",
            message: error.safe_message,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"error": {"code": self.code, "message": self.message}})),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_statuses_are_stable_api_values() {
        assert_eq!(run_status(RunStatus::ApprovalRequired), "approval_required");
        assert_eq!(run_status(RunStatus::TimedOut), "timed_out");
    }

    #[test]
    fn run_identifier_parser_rejects_invalid_input() {
        assert!(parse_run_id("abc").is_err());
        assert!(parse_run_id("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").is_err());
    }
}
