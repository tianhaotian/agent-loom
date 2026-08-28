use std::{
    collections::VecDeque,
    convert::Infallible,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use agent_loom_domain::{
    AgentVersionId, CheckpointId, ContextMergeStrategy, ContextPatchId, ContextSnapshotId, EventId,
    JoinPolicy, JsonPayload, LogicalKey, PlanRevisionId, RunId, RunSnapshot, RunStatus,
    ScheduleConcurrencyPolicy, ScheduleId, ScheduleMisfirePolicy, ScheduleSnapshot, ScheduleStatus,
    StageStatus, TaskId, TenantId, UnixMicros, WaitStatus, WorkflowId,
};
use agent_loom_durable_store::{
    ApplyContextPatch, ApplyEvent, CommandDisposition, ControlRun, CreateRun, CreateSchedule,
    DurableStore, EvaluateChildRunJoin, EventCursor, ExpectedRun, NewCheckpoint,
    NewContextSnapshot, NewPlanRevision, QueryContext, RevisePlan, SignatureVerification,
    StoreError, StoreErrorCode,
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
    execution_plan::{
        materialize_execution_plan, materialize_plan_task_additions, parse_execution_plan,
    },
    identity::{command_context, decode_id, hash_bytes, now_micros, random_id},
    schedule::{next_fire_after, validate_schedule_definition},
};

const API_ACTOR: &str = "agent-loom-http-api";

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn DurableStore>,
    pub tenant_id: TenantId,
    pub workflow_id: WorkflowId,
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
        .route("/v1/schedules", get(list_schedules).post(create_schedule))
        .route("/v1/schedules/{schedule_id}", get(get_schedule))
        .route("/v1/schedules/{schedule_id}/fires", post(fire_schedule))
        .route("/v1/runs", post(create_run))
        .route("/v1/runs/{run_id}", get(get_run))
        .route("/v1/runs/{run_id}/children", get(list_child_runs))
        .route("/v1/runs/{run_id}/replay", post(replay_run))
        .route(
            "/v1/runs/{run_id}/manual-interventions",
            post(open_manual_intervention),
        )
        .route(
            "/v1/runs/{run_id}/manual-interventions/resolve",
            post(resolve_manual_intervention),
        )
        .route("/v1/runs/{run_id}/handoffs", post(handoff_run))
        .route("/v1/runs/{run_id}/compensations", post(compensate_run))
        .route(
            "/v1/runs/{run_id}/child-joins/{task_id}",
            post(evaluate_child_run_join),
        )
        .route(
            "/v1/runs/{run_id}/plan-revisions",
            get(list_plan_revisions).post(revise_plan),
        )
        .route(
            "/v1/runs/{run_id}/context-snapshots",
            get(list_context_snapshots).post(apply_context_patch),
        )
        .route("/v1/tasks/{task_id}/context", get(get_task_context))
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
struct CreateScheduleRequest {
    cron_expression: String,
    #[serde(default = "utc_timezone")]
    timezone: String,
    #[serde(default)]
    misfire_policy: ScheduleMisfirePolicyRequest,
    #[serde(default)]
    concurrency_policy: ScheduleConcurrencyPolicyRequest,
    #[serde(default = "default_catch_up_limit")]
    catch_up_limit: u32,
    input: Value,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ScheduleMisfirePolicyRequest {
    Skip,
    #[default]
    FireOnce,
    CatchUp,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ScheduleConcurrencyPolicyRequest {
    #[default]
    Allow,
    Forbid,
}

#[derive(Debug, Serialize)]
struct ScheduleResponse {
    schedule_id: String,
    workflow_version_id: String,
    cron_expression: String,
    timezone: String,
    input: Value,
    status: &'static str,
    misfire_policy: &'static str,
    concurrency_policy: &'static str,
    catch_up_limit: u32,
    next_fire_at_micros: i64,
    last_fire_at_micros: Option<i64>,
    version: u64,
    created_at_micros: i64,
    updated_at_micros: i64,
}

impl From<ScheduleSnapshot> for ScheduleResponse {
    fn from(schedule: ScheduleSnapshot) -> Self {
        Self {
            schedule_id: schedule.schedule_id.to_string(),
            workflow_version_id: schedule.workflow_version_id.to_string(),
            cron_expression: schedule.cron_expression,
            timezone: schedule.timezone,
            input: serde_json::from_slice(schedule.input.as_bytes()).unwrap_or(Value::Null),
            status: match schedule.status {
                ScheduleStatus::Active => "active",
                ScheduleStatus::Paused => "paused",
            },
            misfire_policy: match schedule.misfire_policy {
                ScheduleMisfirePolicy::Skip => "skip",
                ScheduleMisfirePolicy::FireOnce => "fire_once",
                ScheduleMisfirePolicy::CatchUp => "catch_up",
            },
            concurrency_policy: match schedule.concurrency_policy {
                ScheduleConcurrencyPolicy::Allow => "allow",
                ScheduleConcurrencyPolicy::Forbid => "forbid",
            },
            catch_up_limit: schedule.catch_up_limit,
            next_fire_at_micros: schedule.next_fire_at.get(),
            last_fire_at_micros: schedule.last_fire_at.map(UnixMicros::get),
            version: schedule.version,
            created_at_micros: schedule.created_at.get(),
            updated_at_micros: schedule.updated_at.get(),
        }
    }
}

#[derive(Debug, Serialize)]
struct CreateScheduleResponse {
    schedule: ScheduleResponse,
    disposition: &'static str,
}

async fn create_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateScheduleRequest>,
) -> ApiResult<(StatusCode, Json<CreateScheduleResponse>)> {
    validate_schedule_definition(&request.cron_expression, &request.timezone)
        .map_err(ApiError::bad_request)?;
    if request.catch_up_limit == 0 || request.catch_up_limit > 100 {
        return Err(ApiError::bad_request(
            "catch_up_limit must be between 1 and 100",
        ));
    }
    let next_fire_at = next_fire_after(
        &request.cron_expression,
        &request.timezone,
        UnixMicros::new(now_micros()),
    )
    .map_err(ApiError::bad_request)?;
    let schedule_id = ScheduleId::from_bytes(random_id());
    let idempotency = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map_or_else(|| schedule_id.to_string(), ToOwned::to_owned);
    let request_bytes = serde_json::to_vec(&request)
        .map_err(|_| ApiError::bad_request("Schedule request cannot be encoded"))?;
    let context = command_context(
        state.tenant_id,
        RunId::from_bytes(schedule_id.into_bytes()),
        API_ACTOR,
        "create_schedule",
        &format!("api-schedule/{idempotency}"),
        &request_bytes,
    )
    .map_err(ApiError::bad_request)?;
    let workflow = state
        .store
        .get_workflow(&query_context(&state), state.workflow_id)
        .await?
        .ok_or_else(|| ApiError::internal("configured Workflow was not found"))?;
    if workflow.status != "active" || workflow.lifecycle != "published" {
        return Err(ApiError::internal(
            "configured Workflow is not active and published",
        ));
    }
    let committed = state
        .store
        .create_schedule(
            &context,
            CreateSchedule {
                schedule_id,
                workflow_version_id: workflow.workflow_version_id,
                cron_expression: request.cron_expression,
                timezone: request.timezone,
                input: payload(&request.input)?,
                misfire_policy: match request.misfire_policy {
                    ScheduleMisfirePolicyRequest::Skip => ScheduleMisfirePolicy::Skip,
                    ScheduleMisfirePolicyRequest::FireOnce => ScheduleMisfirePolicy::FireOnce,
                    ScheduleMisfirePolicyRequest::CatchUp => ScheduleMisfirePolicy::CatchUp,
                },
                concurrency_policy: match request.concurrency_policy {
                    ScheduleConcurrencyPolicyRequest::Allow => ScheduleConcurrencyPolicy::Allow,
                    ScheduleConcurrencyPolicyRequest::Forbid => ScheduleConcurrencyPolicy::Forbid,
                },
                catch_up_limit: request.catch_up_limit,
                next_fire_at,
            },
        )
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(CreateScheduleResponse {
            schedule: committed.value.into(),
            disposition: disposition(committed.disposition),
        }),
    ))
}

async fn get_schedule(
    State(state): State<AppState>,
    Path(schedule_id): Path<String>,
) -> ApiResult<Json<ScheduleResponse>> {
    let schedule_id = parse_schedule_id(&schedule_id)?;
    state
        .store
        .get_schedule(&query_context(&state), schedule_id)
        .await?
        .map(ScheduleResponse::from)
        .map(Json)
        .ok_or_else(|| ApiError::not_found("Schedule was not found"))
}

async fn list_schedules(State(state): State<AppState>) -> ApiResult<Json<Vec<ScheduleResponse>>> {
    Ok(Json(
        state
            .store
            .list_schedules(&query_context(&state))
            .await?
            .into_iter()
            .map(ScheduleResponse::from)
            .collect(),
    ))
}

#[derive(Debug, Deserialize)]
struct FireScheduleRequest {
    scheduled_fire_time_micros: i64,
}

async fn fire_schedule(
    State(state): State<AppState>,
    Path(schedule_id): Path<String>,
    Json(request): Json<FireScheduleRequest>,
) -> ApiResult<(StatusCode, Json<CreateRunResponse>)> {
    let schedule_id = parse_schedule_id(&schedule_id)?;
    if request.scheduled_fire_time_micros <= 0 || request.scheduled_fire_time_micros > now_micros()
    {
        return Err(ApiError::bad_request(
            "scheduled_fire_time_micros must be positive and not in the future",
        ));
    }
    let schedule = state
        .store
        .get_schedule(&query_context(&state), schedule_id)
        .await?
        .ok_or_else(|| ApiError::not_found("Schedule was not found"))?;
    if schedule.status != ScheduleStatus::Active {
        return Err(ApiError::conflict("Schedule is not active"));
    }
    if schedule.concurrency_policy == ScheduleConcurrencyPolicy::Forbid
        && state
            .store
            .has_active_schedule_runs(&query_context(&state), schedule_id)
            .await?
    {
        return Err(ApiError::conflict(
            "Schedule concurrency policy forbids overlapping Runs",
        ));
    }
    let workflow = state
        .store
        .get_workflow(&query_context(&state), state.workflow_id)
        .await?
        .ok_or_else(|| ApiError::internal("configured Workflow was not found"))?;
    if workflow.workflow_version_id != schedule.workflow_version_id {
        return Err(ApiError::conflict(
            "Schedule Workflow version is no longer the configured version",
        ));
    }
    let input = serde_json::from_slice(schedule.input.as_bytes())
        .map_err(|_| ApiError::internal("Schedule input is not valid JSON"))?;
    create_run_impl(
        state,
        HeaderMap::new(),
        CreateRunRequest {
            input,
            deadline_micros: None,
            parent_run_id: None,
            parent_task_id: None,
        },
        Some((
            schedule_id,
            UnixMicros::new(request.scheduled_fire_time_micros),
        )),
        None,
    )
    .await
}

fn utc_timezone() -> String {
    "UTC".to_owned()
}

const fn default_catch_up_limit() -> u32 {
    1
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateRunRequest {
    pub input: Value,
    #[serde(default)]
    pub deadline_micros: Option<i64>,
    #[serde(default)]
    pub parent_run_id: Option<String>,
    #[serde(default)]
    pub parent_task_id: Option<String>,
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
    create_run_impl(state, headers, request, None, None).await
}

#[allow(clippy::too_many_lines)]
async fn create_run_impl(
    state: AppState,
    headers: HeaderMap,
    request: CreateRunRequest,
    schedule_fire: Option<(ScheduleId, UnixMicros)>,
    replay_of_run_id: Option<RunId>,
) -> ApiResult<(StatusCode, Json<CreateRunResponse>)> {
    if request
        .deadline_micros
        .is_some_and(|deadline| deadline <= now_micros())
    {
        return Err(ApiError::bad_request(
            "deadline_micros must be in the future",
        ));
    }
    let run_id = schedule_fire.map_or_else(
        || RunId::from_bytes(random_id()),
        |(schedule_id, fire_at)| {
            RunId::from_bytes(crate::identity::derived_id(
                "schedule-run",
                &format!("{schedule_id}/{}", fire_at.get()),
            ))
        },
    );
    let parent_run_id = request
        .parent_run_id
        .as_deref()
        .map(parse_run_id)
        .transpose()?;
    let parent_task_id = request
        .parent_task_id
        .as_deref()
        .map(parse_task_id)
        .transpose()?;
    if parent_task_id.is_some() && parent_run_id.is_none() {
        return Err(ApiError::bad_request(
            "parent_task_id requires parent_run_id",
        ));
    }
    let idempotency = schedule_fire.map_or_else(
        || {
            replay_of_run_id.map_or_else(
                || {
                    headers
                        .get("idempotency-key")
                        .and_then(|value| value.to_str().ok())
                        .map_or_else(|| run_id.to_string(), ToOwned::to_owned)
                },
                |source| {
                    let key = headers
                        .get("idempotency-key")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("missing");
                    format!("replay/{source}/{key}")
                },
            )
        },
        |(schedule_id, fire_at)| format!("schedule/{schedule_id}/{}", fire_at.get()),
    );
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
    let (workflow_version_id, initial_plan, plan_digest) =
        if let Some(source_run_id) = replay_of_run_id {
            let source = load_run(&state, source_run_id).await?;
            let workflow_version_id = source
                .workflow_version_id
                .ok_or_else(|| ApiError::conflict("Replay source Run has no Workflow version"))?;
            let revision = state
                .store
                .list_plan_revisions(&query_context(&state), source_run_id)
                .await?
                .into_iter()
                .max_by_key(|revision| revision.revision)
                .ok_or_else(|| ApiError::internal("Replay source Run has no Plan revision"))?;
            (workflow_version_id, revision.plan, revision.plan_digest)
        } else {
            let workflow = state
                .store
                .get_workflow(&query_context(&state), state.workflow_id)
                .await?
                .ok_or_else(|| ApiError::internal("configured Workflow was not found"))?;
            if workflow.status != "active" || workflow.lifecycle != "published" {
                return Err(ApiError::internal(
                    "configured Workflow is not active and published",
                ));
            }
            (
                workflow.workflow_version_id,
                workflow.spec,
                workflow.spec_digest,
            )
        };
    let plan = parse_execution_plan(&initial_plan)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let materialized =
        materialize_execution_plan(&plan, run_id, &request.input, UnixMicros::new(now_micros()))
            .map_err(|error| ApiError::internal(error.to_string()))?;
    let initial_event_id = EventId::from_bytes(random_id());
    let command = CreateRun {
        run_id,
        replay_of_run_id,
        parent_run_id,
        parent_task_id,
        parent_event_id: parent_run_id
            .map(|_| EventId::from_bytes(crate::identity::derived_id("parent-event", &identity))),
        schedule_id: schedule_fire.map(|(schedule_id, _)| schedule_id),
        scheduled_fire_at: schedule_fire.map(|(_, fire_at)| fire_at),
        workflow_version_id: Some(workflow_version_id),
        coordinator_agent_version_id: Some(state.coordinator_agent_version_id),
        input: payload(&request.input)?,
        deadline: request.deadline_micros.map(UnixMicros::new),
        initial_event_id,
        initial_plan_revision: NewPlanRevision {
            plan_revision_id: PlanRevisionId::from_bytes(random_id()),
            schema_version: plan.schema_version,
            plan_key: plan.plan_key.clone(),
            plan: initial_plan,
            plan_digest,
            change_summary: payload(&json!({
                "kind": if replay_of_run_id.is_some() { "replay" } else { "initial" },
                "replay_of_run_id": replay_of_run_id.map(|id| id.to_string()),
            }))?,
            created_event_id: initial_event_id,
        },
        initial_context: NewContextSnapshot {
            context_snapshot_id: ContextSnapshotId::from_bytes(random_id()),
            schema_version: 1,
            value: payload(&request.input)?,
            digest: hash_bytes(
                serde_json::to_vec(&request.input)
                    .map_err(|_| ApiError::bad_request("request input cannot be encoded"))?
                    .as_slice(),
            ),
            created_event_id: initial_event_id,
        },
        initial_checkpoint: NewCheckpoint {
            checkpoint_id: CheckpointId::from_bytes(random_id()),
            sequence: 1,
            schema_version: plan.schema_version,
            workflow_version_id: Some(workflow_version_id),
            coordinator_agent_version_id: Some(state.coordinator_agent_version_id),
            execution_generation: 0,
            state_digest: hash_bytes(materialized.checkpoint_state.as_bytes()),
            state: materialized.checkpoint_state,
            created_event_id: initial_event_id,
        },
        initial_stages: materialized.initial_stages,
        initial_tasks: materialized.initial_tasks,
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

pub(crate) async fn dispatch_schedule_fire(
    state: &AppState,
    schedule: &ScheduleSnapshot,
    fire_at: UnixMicros,
) -> Result<(), String> {
    let input = serde_json::from_slice(schedule.input.as_bytes())
        .map_err(|_| "persisted Schedule input is not valid JSON".to_owned())?;
    create_run_impl(
        state.clone(),
        HeaderMap::new(),
        CreateRunRequest {
            input,
            deadline_micros: None,
            parent_run_id: None,
            parent_task_id: None,
        },
        Some((schedule.schedule_id, fire_at)),
        None,
    )
    .await
    .map(|_| ())
    .map_err(|error| error.message)
}

async fn replay_run(
    State(state): State<AppState>,
    Path(source_run_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<(StatusCode, Json<ReplayRunResponse>)> {
    required_idempotency(&headers)?;
    let source_run_id = parse_run_id(&source_run_id)?;
    load_run(&state, source_run_id).await?;
    let input = state
        .store
        .get_run_input(&query_context(&state), source_run_id)
        .await?
        .ok_or_else(|| ApiError::not_found("Replay source Run input was not found"))?;
    let input = serde_json::from_slice(input.as_bytes())
        .map_err(|_| ApiError::internal("Replay source Run input is invalid"))?;
    let (status, Json(response)) = create_run_impl(
        state,
        headers,
        CreateRunRequest {
            input,
            deadline_micros: None,
            parent_run_id: None,
            parent_task_id: None,
        },
        None,
        Some(source_run_id),
    )
    .await?;
    Ok((
        status,
        Json(ReplayRunResponse {
            source_run_id: source_run_id.to_string(),
            run: response.run,
            disposition: response.disposition,
        }),
    ))
}

#[derive(Debug, Serialize)]
struct ReplayRunResponse {
    source_run_id: String,
    run: RunResponse,
    disposition: &'static str,
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

async fn list_child_runs(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<Vec<RunResponse>>> {
    let run_id = parse_run_id(&run_id)?;
    load_run(&state, run_id).await?;
    let children = state
        .store
        .list_child_runs(&query_context(&state), run_id)
        .await?
        .into_iter()
        .map(RunResponse::from)
        .collect();
    Ok(Json(children))
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ChildJoinPolicyRequest {
    All,
    Any,
}

#[derive(Debug, Deserialize, Serialize)]
struct EvaluateChildRunJoinRequest {
    join_policy: ChildJoinPolicyRequest,
}

#[derive(Debug, Serialize)]
struct EvaluateChildRunJoinResponse {
    run: RunResponse,
    disposition: &'static str,
}

async fn evaluate_child_run_join(
    State(state): State<AppState>,
    Path((run_id, task_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<EvaluateChildRunJoinRequest>,
) -> ApiResult<Json<EvaluateChildRunJoinResponse>> {
    let run_id = parse_run_id(&run_id)?;
    let task_id = parse_task_id(&task_id)?;
    let run = load_run(&state, run_id).await?;
    let key = required_idempotency(&headers)?;
    let request_bytes = serde_json::to_vec(&request)
        .map_err(|_| ApiError::bad_request("Child Run join request cannot be encoded"))?;
    let identity = format!("api-child-join/{run_id}/{task_id}/{key}");
    let context = command_context(
        state.tenant_id,
        run_id,
        API_ACTOR,
        "evaluate_child_run_join",
        &identity,
        &request_bytes,
    )
    .map_err(ApiError::bad_request)?;
    let committed = state
        .store
        .evaluate_child_run_join(
            &context,
            EvaluateChildRunJoin {
                expected_run: expected(&run),
                task_id,
                join_policy: match request.join_policy {
                    ChildJoinPolicyRequest::All => JoinPolicy::All,
                    ChildJoinPolicyRequest::Any => JoinPolicy::Any,
                },
                event_id: EventId::from_bytes(crate::identity::derived_id("event", &identity)),
            },
        )
        .await?;
    Ok(Json(EvaluateChildRunJoinResponse {
        run: committed.value.into(),
        disposition: disposition(committed.disposition),
    }))
}

#[derive(Debug, Deserialize, Serialize)]
struct RevisePlanRequest {
    base_revision: u64,
    plan: Value,
    #[serde(default = "empty_object")]
    change_summary: Value,
}

#[derive(Debug, Serialize)]
struct RevisePlanResponse {
    run: RunResponse,
    plan_revision: u64,
    disposition: &'static str,
}

#[derive(Debug, Serialize)]
struct PlanRevisionResponse {
    plan_revision_id: String,
    revision: u64,
    parent_plan_revision_id: Option<String>,
    schema_version: u32,
    plan_key: String,
    plan: Value,
    plan_digest: String,
    change_summary: Value,
    created_event_id: String,
    created_by: String,
    created_at_micros: i64,
}

async fn revise_plan(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<RevisePlanRequest>,
) -> ApiResult<Json<RevisePlanResponse>> {
    if request.base_revision == 0 {
        return Err(ApiError::bad_request("base_revision must be positive"));
    }
    let run_id = parse_run_id(&run_id)?;
    let run = load_run(&state, run_id).await?;
    let plan_payload = payload(&request.plan)?;
    let plan = parse_execution_plan(&plan_payload)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let query = query_context(&state);
    let current_revision = state
        .store
        .list_plan_revisions(&query, run_id)
        .await?
        .into_iter()
        .find(|revision| revision.revision == request.base_revision)
        .ok_or_else(|| ApiError::conflict("base Plan revision was not found"))?;
    let current_plan = parse_execution_plan(&current_revision.plan)
        .map_err(|_| ApiError::internal("current Plan revision is invalid"))?;
    let run_input = state
        .store
        .get_run_input(&query, run_id)
        .await?
        .ok_or_else(|| ApiError::not_found("Run was not found"))?;
    let run_input: Value = serde_json::from_slice(run_input.as_bytes())
        .map_err(|_| ApiError::internal("persisted Run input is invalid"))?;
    let new_tasks = materialize_plan_task_additions(
        &current_plan,
        &plan,
        run_id,
        &run_input,
        UnixMicros::new(now_micros()),
    )
    .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let added_tasks = new_tasks
        .iter()
        .map(|task| task.logical_key.as_str())
        .collect::<Vec<_>>();
    let change_summary = payload(&json!({
        "requested": request.change_summary,
        "added_tasks": added_tasks,
    }))?;
    let key = required_idempotency(&headers)?;
    let request_bytes = serde_json::to_vec(&request)
        .map_err(|_| ApiError::bad_request("Plan revision request cannot be encoded"))?;
    let identity = format!("api-plan-revision/{run_id}/{key}");
    let context = command_context(
        state.tenant_id,
        run_id,
        API_ACTOR,
        "revise_plan",
        &identity,
        &request_bytes,
    )
    .map_err(ApiError::bad_request)?;
    let event_id = EventId::from_bytes(crate::identity::derived_id("event", &identity));
    let committed = state
        .store
        .revise_plan(
            &context,
            RevisePlan {
                expected_run: expected(&run),
                expected_plan_revision: request.base_revision,
                event_id,
                revision: NewPlanRevision {
                    plan_revision_id: PlanRevisionId::from_bytes(crate::identity::derived_id(
                        "plan-revision",
                        &identity,
                    )),
                    schema_version: plan.schema_version,
                    plan_key: plan.plan_key,
                    plan_digest: hash_bytes(plan_payload.as_bytes()),
                    plan: plan_payload,
                    change_summary,
                    created_event_id: event_id,
                },
                new_tasks,
            },
        )
        .await?;
    let plan_revision = request
        .base_revision
        .checked_add(1)
        .ok_or_else(|| ApiError::bad_request("Plan revision exceeds supported range"))?;
    Ok(Json(RevisePlanResponse {
        run: committed.value.into(),
        plan_revision,
        disposition: disposition(committed.disposition),
    }))
}

async fn list_plan_revisions(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<Vec<PlanRevisionResponse>>> {
    let run_id = parse_run_id(&run_id)?;
    load_run(&state, run_id).await?;
    let revisions = state
        .store
        .list_plan_revisions(&query_context(&state), run_id)
        .await?
        .into_iter()
        .map(|revision| PlanRevisionResponse {
            plan_revision_id: revision.plan_revision_id.to_string(),
            revision: revision.revision,
            parent_plan_revision_id: revision.parent_plan_revision_id.map(|id| id.to_string()),
            schema_version: revision.schema_version,
            plan_key: revision.plan_key.into_string(),
            plan: serde_json::from_slice(revision.plan.as_bytes()).unwrap_or(Value::Null),
            plan_digest: hex(revision.plan_digest.as_bytes()),
            change_summary: serde_json::from_slice(revision.change_summary.as_bytes())
                .unwrap_or(Value::Null),
            created_event_id: revision.created_event_id.to_string(),
            created_by: revision.created_by,
            created_at_micros: revision.created_at.get(),
        })
        .collect();
    Ok(Json(revisions))
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ContextMergeStrategyRequest {
    Replace,
    MergePatch,
}

#[derive(Debug, Deserialize, Serialize)]
struct ApplyContextPatchRequest {
    base_revision: u64,
    #[serde(default = "default_context_schema_version")]
    schema_version: u32,
    merge_strategy: ContextMergeStrategyRequest,
    patch: Value,
}

#[derive(Debug, Serialize)]
struct ApplyContextPatchResponse {
    run: RunResponse,
    context_revision: u64,
    disposition: &'static str,
}

#[derive(Debug, Serialize)]
struct ContextSnapshotResponse {
    context_snapshot_id: String,
    revision: u64,
    parent_context_snapshot_id: Option<String>,
    schema_version: u32,
    context: Value,
    context_digest: String,
    created_event_id: String,
    created_by: String,
    created_at_micros: i64,
}

async fn apply_context_patch(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ApplyContextPatchRequest>,
) -> ApiResult<Json<ApplyContextPatchResponse>> {
    if request.base_revision == 0 || request.schema_version == 0 {
        return Err(ApiError::bad_request(
            "base_revision and schema_version must be positive",
        ));
    }
    let run_id = parse_run_id(&run_id)?;
    let run = load_run(&state, run_id).await?;
    let patch = payload(&request.patch)?;
    let key = required_idempotency(&headers)?;
    let request_bytes = serde_json::to_vec(&request)
        .map_err(|_| ApiError::bad_request("Context patch request cannot be encoded"))?;
    let identity = format!("api-context-patch/{run_id}/{key}");
    let context = command_context(
        state.tenant_id,
        run_id,
        API_ACTOR,
        "apply_context_patch",
        &identity,
        &request_bytes,
    )
    .map_err(ApiError::bad_request)?;
    let committed = state
        .store
        .apply_context_patch(
            &context,
            ApplyContextPatch {
                expected_run: expected(&run),
                expected_context_revision: request.base_revision,
                event_id: EventId::from_bytes(crate::identity::derived_id("event", &identity)),
                patch_id: ContextPatchId::from_bytes(crate::identity::derived_id(
                    "context-patch",
                    &identity,
                )),
                context_snapshot_id: ContextSnapshotId::from_bytes(crate::identity::derived_id(
                    "context-snapshot",
                    &identity,
                )),
                schema_version: request.schema_version,
                patch,
                merge_strategy: match request.merge_strategy {
                    ContextMergeStrategyRequest::Replace => ContextMergeStrategy::Replace,
                    ContextMergeStrategyRequest::MergePatch => ContextMergeStrategy::MergePatch,
                },
            },
        )
        .await?;
    Ok(Json(ApplyContextPatchResponse {
        run: committed.value.into(),
        context_revision: request
            .base_revision
            .checked_add(1)
            .ok_or_else(|| ApiError::bad_request("Context revision exceeds supported range"))?,
        disposition: disposition(committed.disposition),
    }))
}

async fn list_context_snapshots(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> ApiResult<Json<Vec<ContextSnapshotResponse>>> {
    let run_id = parse_run_id(&run_id)?;
    load_run(&state, run_id).await?;
    let snapshots = state
        .store
        .list_context_snapshots(&query_context(&state), run_id)
        .await?
        .into_iter()
        .map(|snapshot| ContextSnapshotResponse {
            context_snapshot_id: snapshot.context_snapshot_id.to_string(),
            revision: snapshot.revision,
            parent_context_snapshot_id: snapshot
                .parent_context_snapshot_id
                .map(|id| id.to_string()),
            schema_version: snapshot.schema_version,
            context: serde_json::from_slice(snapshot.value.as_bytes()).unwrap_or(Value::Null),
            context_digest: hex(snapshot.digest.as_bytes()),
            created_event_id: snapshot.created_event_id.to_string(),
            created_by: snapshot.created_by,
            created_at_micros: snapshot.created_at.get(),
        })
        .collect();
    Ok(Json(snapshots))
}

#[derive(Debug, Serialize)]
struct TaskContextResponse {
    task_id: String,
    run_id: String,
    context_snapshot_id: String,
    projection: Value,
    context: Value,
}

async fn get_task_context(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> ApiResult<Json<TaskContextResponse>> {
    let task_id = parse_task_id(&task_id)?;
    let reference = state
        .store
        .get_task_context(&query_context(&state), task_id)
        .await?
        .ok_or_else(|| ApiError::not_found("Task ContextReference was not found"))?;
    Ok(Json(TaskContextResponse {
        task_id: reference.task_id.to_string(),
        run_id: reference.run_id.to_string(),
        context_snapshot_id: reference.context_snapshot_id.to_string(),
        projection: serde_json::from_slice(reference.projection.as_bytes()).unwrap_or(Value::Null),
        context: serde_json::from_slice(reference.context.as_bytes()).unwrap_or(Value::Null),
    }))
}

const fn default_context_schema_version() -> u32 {
    1
}

fn empty_object() -> Value {
    json!({})
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
    ManualOpen,
    ManualResolve,
}

impl ControlAction {
    const fn scope(self) -> &'static str {
        match self {
            Self::Pause => "pause_run",
            Self::Resume => "resume_run",
            Self::Cancel => "cancel_run",
            Self::ManualOpen => "open_manual_intervention",
            Self::ManualResolve => "resolve_manual_intervention",
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
        ControlAction::Pause | ControlAction::ManualOpen => {
            state.store.pause_run(&context, command).await?
        }
        ControlAction::Resume | ControlAction::ManualResolve => {
            state.store.resume_run(&context, command).await?
        }
        ControlAction::Cancel => state.store.cancel_run(&context, command).await?,
    };
    Ok(Json(committed.value.into()))
}

async fn open_manual_intervention(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ControlRequest>,
) -> ApiResult<Json<RunResponse>> {
    control(state, run_id, headers, request, ControlAction::ManualOpen).await
}

async fn resolve_manual_intervention(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ControlRequest>,
) -> ApiResult<Json<RunResponse>> {
    control(
        state,
        run_id,
        headers,
        request,
        ControlAction::ManualResolve,
    )
    .await
}

#[derive(Debug, Deserialize, Serialize)]
struct AdvancedTaskRequest {
    base_revision: u64,
    task_key: String,
    target_handler: String,
    #[serde(default = "default_max_attempts_api")]
    max_attempts: u32,
    #[serde(default = "empty_object")]
    input: Value,
}

const fn default_max_attempts_api() -> u32 {
    3
}

async fn handoff_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<AdvancedTaskRequest>,
) -> ApiResult<Json<RevisePlanResponse>> {
    append_advanced_task(state, run_id, headers, request, "handoff", "agent_server").await
}

async fn compensate_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<AdvancedTaskRequest>,
) -> ApiResult<Json<RevisePlanResponse>> {
    append_advanced_task(state, run_id, headers, request, "compensation", "tool").await
}

async fn append_advanced_task(
    state: AppState,
    run_id: String,
    headers: HeaderMap,
    request: AdvancedTaskRequest,
    action: &'static str,
    kind: &'static str,
) -> ApiResult<Json<RevisePlanResponse>> {
    if request.base_revision == 0
        || request.task_key.trim().is_empty()
        || request.target_handler.trim().is_empty()
        || request.max_attempts == 0
    {
        return Err(ApiError::bad_request(
            "advanced Task metadata must be non-empty and positive",
        ));
    }
    let parsed_run_id = parse_run_id(&run_id)?;
    let revision = state
        .store
        .list_plan_revisions(&query_context(&state), parsed_run_id)
        .await?
        .into_iter()
        .find(|revision| revision.revision == request.base_revision)
        .ok_or_else(|| ApiError::conflict("base Plan revision was not found"))?;
    let mut plan: Value = serde_json::from_slice(revision.plan.as_bytes())
        .map_err(|_| ApiError::internal("persisted Plan revision is invalid"))?;
    let tasks = plan
        .get_mut("initial_tasks")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| ApiError::internal("persisted Plan has no initial_tasks array"))?;
    let task_key = request.task_key;
    tasks.push(json!({
        "key": task_key,
        "handler": request.target_handler,
        "kind": kind,
        "priority": 0,
        "max_attempts": request.max_attempts,
        "input": request.input,
    }));
    revise_plan(
        State(state),
        Path(run_id),
        headers,
        Json(RevisePlanRequest {
            base_revision: request.base_revision,
            plan,
            change_summary: json!({
                "kind": action,
                "task_key": task_key,
            }),
        }),
    )
    .await
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

fn parse_task_id(value: &str) -> ApiResult<TaskId> {
    decode_id(value)
        .map(TaskId::from_bytes)
        .map_err(ApiError::bad_request)
}

fn parse_schedule_id(value: &str) -> ApiResult<ScheduleId> {
    decode_id(value)
        .map(ScheduleId::from_bytes)
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

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "conflict",
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

    #[test]
    fn schedule_accepts_five_field_cron_and_iana_timezone() {
        assert!(validate_schedule_definition("*/5 * * * *", "UTC").is_ok());
        assert!(validate_schedule_definition("0 9 * * 1-5", "Asia/Shanghai").is_ok());
        assert!(validate_schedule_definition("*/5 * * *", "UTC").is_err());
        assert!(validate_schedule_definition("@daily", "UTC").is_err());
        assert!(validate_schedule_definition("60 * * * *", "UTC").is_err());
        assert!(validate_schedule_definition("*/0 * * * *", "UTC").is_err());
        assert!(validate_schedule_definition("0 9 * * *", "Mars/Olympus").is_err());
    }
}
