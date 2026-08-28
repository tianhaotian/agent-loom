use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use agent_loom_domain::{
    AgentExecutionId, AgentExecutionStatus, CheckpointId, CommandId, CorrelationId, Digest,
    DurationMicros, EventId, IdempotencyKey, JsonPayload, LeaseToken, RunId, ScopeKey, TaskKind,
    WorkerId,
};
use agent_loom_durable_store::{
    AgentEventQuery, AgentSubmissionOutcome, ClaimOutbox, ClaimTask, CommandContext,
    CommandDisposition, CompleteTask, ControlRun, DurableStore as _, ExpectedRun, FailTask,
    LeaseProof, NewCheckpoint, NextActions, OutboxDeliveryOutcome, PrepareAgentExecution,
    QueryContext, RecordAgentSubmission, RecordOutboxDelivery, TaskResult,
};
use agent_loom_runtime::{
    AgentEventDispatcher as _, AgentEventPollOutcome, AgentEventWorker, AgentEventWorkerConfig,
    AgentStatusPollOutcome, AgentStatusWorker, AgentStatusWorkerConfig, AgentStopPollOutcome,
    AgentStopWorker, AgentStopWorkerConfig, ExternalRecoveryDispatcher as _, PollingActivity,
    PollingJob as _, RecoveryDispatchFence, StartedRecovery,
};
use agent_loom_server::{
    AppState, MaintenancePollingConfig, MaintenancePollingJob, SchedulePollingConfig,
    SchedulePollingJob, ServerConfig, WorkflowWorker, WorkflowWorkerActivity, WorkflowWorkerConfig,
    bootstrap, mock_dispatcher,
};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tower::ServiceExt as _;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn http_to_postgres_mock_delivery_completes_when_configured() {
    let Ok(database_url) = std::env::var("AGENT_LOOM_TEST_POSTGRES_URL") else {
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let config = ServerConfig {
        database_url,
        bind: "127.0.0.1:0".to_owned(),
        tenant_key: format!("mvp-e2e-{nonce}"),
        api_key: "mvp-e2e-api-key".to_owned(),
        pool_size: 4,
        http_adapters: None,
    };
    let application = bootstrap(&config).await.expect("bootstrap MVP server");
    let unauthorized = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/runs")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"input":{}}"#))
                .expect("build unauthorized request"),
        )
        .await
        .expect("unauthorized response");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    let response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/runs")
                .header("content-type", "application/json")
                .header("authorization", "Bearer mvp-e2e-api-key")
                .header("idempotency-key", format!("mvp-e2e-{nonce}"))
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "input": {"goal": "deliver the Agent Loom MVP"}
                    }))
                    .expect("encode request"),
                ))
                .expect("build request"),
        )
        .await
        .expect("create Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body: Value = serde_json::from_slice(
        &to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read create body"),
    )
    .expect("decode create response");
    let run_id = body["run"]["run_id"]
        .as_str()
        .expect("response contains Run ID")
        .to_owned();

    let dispatcher = mock_dispatcher(
        application.store.clone(),
        application.endpoint_id,
        application.coordinator_agent_version_id,
    )
    .expect("build Mock Agent dispatcher");
    let worker = WorkflowWorker::new(
        Arc::new(application.store.clone()),
        application.tenant_id,
        WorkerId::from_bytes(nonce.to_be_bytes()),
        application.coordinator_agent_version_id,
        application.endpoint_id,
        Arc::new(dispatcher),
        WorkflowWorkerConfig::default(),
    );
    for step in 0..9 {
        let activity = worker.run_once().await.expect("complete mock stage");
        assert!(
            matches!(
                activity,
                WorkflowWorkerActivity::Completed {
                    terminal: false,
                    ..
                }
            ),
            "stage {step} must remain non-terminal"
        );
    }
    let pending = get_json(
        application.router.clone(),
        &format!("/v1/runs/{run_id}/pending-actions"),
    )
    .await;
    assert_eq!(pending.as_array().map(Vec::len), Some(1));
    assert_eq!(pending[0]["action_type"], "approval");

    let approval = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/runs/{run_id}/events"))
                .header("content-type", "application/json")
                .header("authorization", "Bearer mvp-e2e-api-key")
                .header("idempotency-key", format!("deployment-approval-{nonce}"))
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "event_type": "approval.granted",
                        "match_key": "deployment-approval",
                        "payload": {"approved": true}
                    }))
                    .expect("encode approval"),
                ))
                .expect("build approval request"),
        )
        .await
        .expect("apply approval");
    let approval_status = approval.status();
    let approval_body = to_bytes(approval.into_body(), 64 * 1024)
        .await
        .expect("read approval response");
    assert_eq!(
        approval_status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&approval_body)
    );
    let pending = get_json(
        application.router.clone(),
        &format!("/v1/runs/{run_id}/pending-actions"),
    )
    .await;
    assert_eq!(pending.as_array().map(Vec::len), Some(0));
    for step in 9..11 {
        let activity = worker
            .run_once()
            .await
            .expect("complete post-approval stage");
        assert!(
            matches!(activity, WorkflowWorkerActivity::Completed { terminal, .. } if terminal == (step == 10))
        );
    }

    let response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/runs/{run_id}"))
                .header("authorization", "Bearer mvp-e2e-api-key")
                .body(Body::empty())
                .expect("build query request"),
        )
        .await
        .expect("query Run response");
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(
        &to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read query body"),
    )
    .expect("decode query response");
    assert_eq!(body["status"], "completed");
    // Ten Agent chains, one Tool chain, creation, and approval produce 56 Events.
    assert_eq!(body["next_event_sequence"], 57);

    let event_stream = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/runs/{run_id}/events/stream?after=55"))
                .header("authorization", "Bearer mvp-e2e-api-key")
                .body(Body::empty())
                .expect("build SSE request"),
        )
        .await
        .expect("SSE response");
    assert_eq!(event_stream.status(), StatusCode::OK);
    assert_eq!(
        event_stream
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );
    let stream_body = to_bytes(event_stream.into_body(), 64 * 1024)
        .await
        .expect("read terminal SSE stream");
    let stream_body = std::str::from_utf8(&stream_body).expect("SSE is UTF-8");
    assert!(stream_body.contains("id: 56"));
    assert!(stream_body.contains("event: run.event"));
    let events = get_json(
        application.router.clone(),
        &format!("/v1/runs/{run_id}/events?after=0&limit=100"),
    )
    .await;
    assert!(
        events["events"]
            .as_array()
            .expect("Event list")
            .iter()
            .any(|event| event["event_type"] == "tool.execution_prepared")
    );

    let stages = get_json(
        application.router.clone(),
        &format!("/v1/runs/{run_id}/stages"),
    )
    .await;
    assert_eq!(stages.as_array().map(Vec::len), Some(11));
    let stages = stages.as_array().expect("Stage list");
    assert_eq!(
        stages
            .iter()
            .filter(|stage| stage["status"] == "succeeded")
            .count(),
        10
    );
    assert_eq!(
        stages
            .iter()
            .filter(|stage| stage["status"] == "rework_required")
            .count(),
        1
    );
    let artifacts = get_json(
        application.router.clone(),
        &format!("/v1/runs/{run_id}/artifacts"),
    )
    .await;
    assert_eq!(artifacts.as_array().map(Vec::len), Some(11));
    let workflow = get_json(
        application.router.clone(),
        &format!("/v1/workflows/{}", application.workflow_id),
    )
    .await;
    assert_eq!(workflow["workflow_key"], "delivery-mvp");
    assert_eq!(workflow["version"], 3);
    assert_eq!(workflow["spec"]["schema"], "agent-loom.execution-plan/v1");
    assert_eq!(
        workflow["spec"]["initial_tasks"][0]["handler"],
        "delivery-mvp"
    );
    assert_eq!(workflow["spec"]["stages"].as_array().map(Vec::len), Some(8));

    let deadline = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_micros(),
    )
    .expect("timestamp fits i64")
        + 100_000;
    let deadline_response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/runs")
                .header("content-type", "application/json")
                .header("authorization", "Bearer mvp-e2e-api-key")
                .header("idempotency-key", format!("mvp-deadline-{nonce}"))
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "input": {"goal": "exercise database deadline"},
                        "deadline_micros": deadline
                    }))
                    .expect("encode deadline request"),
                ))
                .expect("build deadline request"),
        )
        .await
        .expect("create deadline Run");
    assert_eq!(deadline_response.status(), StatusCode::ACCEPTED);
    let body: Value = serde_json::from_slice(
        &to_bytes(deadline_response.into_body(), 64 * 1024)
            .await
            .expect("read deadline Run"),
    )
    .expect("decode deadline Run");
    let deadline_run_id = body["run"]["run_id"].as_str().expect("deadline Run ID");
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let maintenance = MaintenancePollingJob::new(
        Arc::new(application.store),
        application.tenant_id,
        MaintenancePollingConfig {
            page_size: 100,
            stale_after_micros: 1_000_000,
        },
    );
    assert!(matches!(
        maintenance.run_once(0).await.expect("run maintenance"),
        PollingActivity::Progress { completed, .. } if completed >= 1
    ));
    let deadline_run = get_json(application.router, &format!("/v1/runs/{deadline_run_id}")).await;
    assert_eq!(deadline_run["status"], "timed_out");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn cancel_before_submission_response_still_stops_the_remote_agent() {
    let Ok(database_url) = std::env::var("AGENT_LOOM_TEST_POSTGRES_URL") else {
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let config = ServerConfig {
        database_url,
        bind: "127.0.0.1:0".to_owned(),
        tenant_key: format!("agent-stop-e2e-{nonce}"),
        api_key: "mvp-e2e-api-key".to_owned(),
        pool_size: 4,
        http_adapters: None,
    };
    let application = bootstrap(&config).await.expect("bootstrap MVP server");
    let response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/runs")
                .header("content-type", "application/json")
                .header("authorization", "Bearer mvp-e2e-api-key")
                .header("idempotency-key", format!("agent-stop-run-{nonce}"))
                .body(Body::from(r#"{"input":{"goal":"stop remote work"}}"#))
                .expect("build create Run request"),
        )
        .await
        .expect("create Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body: Value = serde_json::from_slice(
        &to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read create response"),
    )
    .expect("decode create response");
    let run_uuid = uuid::Uuid::parse_str(body["run"]["run_id"].as_str().expect("Run ID"))
        .expect("valid Run ID");
    let run_id = RunId::from_bytes(*run_uuid.as_bytes());
    let worker_id = WorkerId::from_bytes(test_id(nonce, "worker"));
    let lease_token =
        LeaseToken::from_bytes(Sha256::digest(format!("lease-{nonce}").as_bytes()).into());
    let claimed = application
        .store
        .claim_task(
            &test_command_context(application.tenant_id, nonce, "claim-agent-stop"),
            ClaimTask {
                worker_id,
                lease_token: lease_token.clone(),
                lease_duration: DurationMicros::new(60_000_000),
                candidate_window: 16,
                kind: None,
            },
        )
        .await
        .expect("claim initial Task")
        .expect("initial Task is claimable");
    let execution_id = AgentExecutionId::from_bytes(test_id(nonce, "agent-execution"));
    let request = JsonPayload::from_validated_bytes(
        br#"{"instructions":"perform cancellable work","input":{},"budget":{"max_duration_micros":30000000,"max_output_bytes":4096}}"#.to_vec(),
    );
    let prepared = application
        .store
        .prepare_agent_execution(
            &test_command_context(application.tenant_id, nonce, "prepare-agent-stop"),
            PrepareAgentExecution {
                expected_run: ExpectedRun {
                    run_id,
                    version: Some(claimed.value.run_version),
                    execution_generation: Some(claimed.value.task.generation),
                },
                lease: LeaseProof {
                    task_id: claimed.value.task.task_id,
                    worker_id,
                    token: lease_token,
                    execution_generation: claimed.value.task.generation,
                },
                agent_execution_id: execution_id,
                stage_execution_id: claimed.value.task.stage_execution_id,
                endpoint_id: application.endpoint_id,
                agent_version_id: application.coordinator_agent_version_id,
                idempotency_key: IdempotencyKey::parse(format!("submit-agent-stop-{nonce}"))
                    .expect("valid Agent idempotency key"),
                request_hash: Digest::from_bytes(Sha256::digest(request.as_bytes()).into()),
                request,
                capabilities_snapshot: JsonPayload::from_validated_bytes(
                    br#"{"submission_idempotency":true,"cooperative_stop":true}"#.to_vec(),
                ),
                prepared_event_id: EventId::from_bytes(test_id(nonce, "prepared-event")),
            },
        )
        .await
        .expect("prepare Agent execution");
    let before_cancel = application
        .store
        .get_run(
            &QueryContext {
                tenant_id: application.tenant_id,
                actor_ref: "agent-stop-e2e".to_owned(),
                authoritative: true,
            },
            run_id,
        )
        .await
        .expect("query prepared Run")
        .expect("prepared Run exists");
    let cancelled = application
        .store
        .cancel_run(
            &test_command_context(application.tenant_id, nonce, "cancel-agent-stop"),
            ControlRun {
                expected_run: ExpectedRun {
                    run_id,
                    version: Some(before_cancel.version),
                    execution_generation: Some(before_cancel.execution_generation),
                },
                event_id: EventId::from_bytes(test_id(nonce, "cancel-event")),
                reason: "operator cancelled while submit response was in flight".to_owned(),
            },
        )
        .await
        .expect("cancel Run");
    assert_eq!(
        cancelled.value.status,
        agent_loom_domain::RunStatus::Cancelled
    );

    let late_submission = application
        .store
        .record_agent_submission(
            &test_command_context(application.tenant_id, nonce, "late-agent-submission"),
            RecordAgentSubmission {
                expected_run: ExpectedRun {
                    run_id,
                    version: Some(before_cancel.version),
                    execution_generation: Some(before_cancel.execution_generation),
                },
                agent_execution_id: execution_id,
                expected_version: prepared.value.version,
                outcome: AgentSubmissionOutcome::Accepted {
                    remote_run_ref: "remote-stop-e2e".to_owned(),
                    remote_session_ref: Some("remote-session-e2e".to_owned()),
                    remote_protocol_version: "1".to_owned(),
                },
                submission_event_id: EventId::from_bytes(test_id(nonce, "submission-event")),
            },
        )
        .await
        .expect("retain late accepted submission");
    assert_eq!(late_submission.value.status, AgentExecutionStatus::Stopping);
    assert_eq!(
        late_submission.value.remote_protocol_version.as_deref(),
        Some("1")
    );

    let dispatcher = mock_dispatcher(
        application.store.clone(),
        application.endpoint_id,
        application.coordinator_agent_version_id,
    )
    .expect("build Mock Agent dispatcher");
    let stop_worker = AgentStopWorker::new(
        application.store.clone(),
        dispatcher,
        application.tenant_id,
        AgentStopWorkerConfig::default(),
    )
    .expect("build Agent stop worker");
    assert!(matches!(
        stop_worker.poll_once().await.expect("dispatch remote stop"),
        AgentStopPollOutcome::Dispatched(candidate)
            if candidate.execution.agent_execution_id == execution_id
    ));
    let client = application
        .store
        .pool()
        .get()
        .await
        .expect("pool connection");
    let row = client
        .query_one(
            "SELECT status, remote_protocol_version FROM agent_loom.agent_executions \
             WHERE tenant_id = $1 AND agent_execution_id = $2",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &uuid::Uuid::from_bytes(execution_id.into_bytes()),
            ],
        )
        .await
        .expect("query stopped Agent execution");
    assert_eq!(row.get::<_, String>(0), "reconciling");
    assert_eq!(row.get::<_, Option<String>>(1).as_deref(), Some("1"));
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let dispatcher = mock_dispatcher(
        application.store.clone(),
        application.endpoint_id,
        application.coordinator_agent_version_id,
    )
    .expect("build status Mock Agent dispatcher");
    let status_worker = AgentStatusWorker::new(
        application.store.clone(),
        dispatcher,
        application.tenant_id,
        AgentStatusWorkerConfig::default(),
    )
    .expect("build Agent status worker");
    assert!(matches!(
        status_worker.poll_once().await.expect("query remote status"),
        AgentStatusPollOutcome::Dispatched(candidate)
            if candidate.execution.agent_execution_id == execution_id
    ));
    let status: String = client
        .query_one(
            "SELECT status FROM agent_loom.agent_executions \
             WHERE tenant_id = $1 AND agent_execution_id = $2",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &uuid::Uuid::from_bytes(execution_id.into_bytes()),
            ],
        )
        .await
        .expect("query reconciled Agent execution")
        .get(0);
    assert_eq!(status, "succeeded");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn running_agent_events_are_cursor_fenced_deduplicated_and_terminally_reconciled() {
    let Ok(database_url) = std::env::var("AGENT_LOOM_TEST_POSTGRES_URL") else {
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let config = ServerConfig {
        database_url,
        bind: "127.0.0.1:0".to_owned(),
        tenant_key: format!("agent-events-e2e-{nonce}"),
        api_key: "mvp-e2e-api-key".to_owned(),
        pool_size: 4,
        http_adapters: None,
    };
    let application = bootstrap(&config).await.expect("bootstrap MVP server");
    let response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/runs")
                .header("content-type", "application/json")
                .header("authorization", "Bearer mvp-e2e-api-key")
                .header("idempotency-key", format!("agent-events-run-{nonce}"))
                .body(Body::from(r#"{"input":{"goal":"stream remote events"}}"#))
                .expect("build create Run request"),
        )
        .await
        .expect("create Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body: Value = serde_json::from_slice(
        &to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read create response"),
    )
    .expect("decode create response");
    let run_uuid = uuid::Uuid::parse_str(body["run"]["run_id"].as_str().expect("Run ID"))
        .expect("valid Run ID");
    let run_id = RunId::from_bytes(*run_uuid.as_bytes());
    let worker_id = WorkerId::from_bytes(test_id(nonce, "event-worker"));
    let lease_token =
        LeaseToken::from_bytes(Sha256::digest(format!("event-lease-{nonce}").as_bytes()).into());
    let claimed = application
        .store
        .claim_task(
            &test_command_context(application.tenant_id, nonce, "claim-agent-events"),
            ClaimTask {
                worker_id,
                lease_token: lease_token.clone(),
                lease_duration: DurationMicros::new(60_000_000),
                candidate_window: 16,
                kind: None,
            },
        )
        .await
        .expect("claim initial Task")
        .expect("initial Task is claimable");
    let execution_id = AgentExecutionId::from_bytes(test_id(nonce, "event-agent-execution"));
    let request = JsonPayload::from_validated_bytes(
        br#"{"instructions":"stream work","input":{},"budget":{"max_duration_micros":30000000,"max_output_bytes":4096}}"#.to_vec(),
    );
    let prepared = application
        .store
        .prepare_agent_execution(
            &test_command_context(application.tenant_id, nonce, "prepare-agent-events"),
            PrepareAgentExecution {
                expected_run: ExpectedRun {
                    run_id,
                    version: Some(claimed.value.run_version),
                    execution_generation: Some(claimed.value.task.generation),
                },
                lease: LeaseProof {
                    task_id: claimed.value.task.task_id,
                    worker_id,
                    token: lease_token,
                    execution_generation: claimed.value.task.generation,
                },
                agent_execution_id: execution_id,
                stage_execution_id: claimed.value.task.stage_execution_id,
                endpoint_id: application.endpoint_id,
                agent_version_id: application.coordinator_agent_version_id,
                idempotency_key: IdempotencyKey::parse(format!("submit-agent-events-{nonce}"))
                    .expect("valid Agent idempotency key"),
                request_hash: Digest::from_bytes(Sha256::digest(request.as_bytes()).into()),
                request,
                capabilities_snapshot: JsonPayload::from_validated_bytes(
                    br#"{"submission_idempotency":true,"resumable_events":true}"#.to_vec(),
                ),
                prepared_event_id: EventId::from_bytes(test_id(nonce, "event-prepared")),
            },
        )
        .await
        .expect("prepare Agent execution");
    let uncertain = application
        .store
        .record_agent_submission(
            &test_command_context(application.tenant_id, nonce, "uncertain-agent-events"),
            RecordAgentSubmission {
                expected_run: ExpectedRun {
                    run_id,
                    version: Some(claimed.value.run_version),
                    execution_generation: Some(claimed.value.task.generation),
                },
                agent_execution_id: execution_id,
                expected_version: prepared.value.version,
                outcome: AgentSubmissionOutcome::Uncertain,
                submission_event_id: EventId::from_bytes(test_id(nonce, "event-submitted")),
            },
        )
        .await
        .expect("record uncertain Agent submission");
    assert_eq!(uncertain.value.status, AgentExecutionStatus::OutcomeUnknown);
    let query_context = QueryContext {
        tenant_id: application.tenant_id,
        actor_ref: "agent-events-e2e".to_owned(),
        authoritative: true,
    };
    let current_run = application
        .store
        .get_run(&query_context, run_id)
        .await
        .expect("query uncertain Run")
        .expect("uncertain Run exists");
    let dispatcher = mock_dispatcher(
        application.store.clone(),
        application.endpoint_id,
        application.coordinator_agent_version_id,
    )
    .expect("build event Mock Agent dispatcher");
    dispatcher
        .dispatch(StartedRecovery::Agent {
            execution: uncertain.value,
            disposition: CommandDisposition::Applied,
            fence: RecoveryDispatchFence {
                expected_run: ExpectedRun {
                    run_id,
                    version: Some(current_run.version),
                    execution_generation: Some(current_run.execution_generation),
                },
                execution_generation: current_run.execution_generation,
                correlation_id: CorrelationId::from_bytes(test_id(nonce, "reconcile-correlation")),
                actor_ref: "agent-events-reconcile-e2e".to_owned(),
            },
        })
        .await
        .expect("reconcile uncertain submission without resubmitting");

    let stale_candidate = application
        .store
        .scan_agent_events(&query_context, AgentEventQuery { limit: 1 })
        .await
        .expect("scan event candidate")
        .candidates
        .into_iter()
        .next()
        .expect("submitted Agent is immediately due");
    let event_worker = AgentEventWorker::new(
        application.store.clone(),
        dispatcher.clone(),
        application.tenant_id,
        AgentEventWorkerConfig::default(),
    )
    .expect("build Agent event worker");
    assert!(matches!(
        event_worker.poll_once().await.expect("read remote events"),
        AgentEventPollOutcome::Dispatched(candidate)
            if candidate.execution.agent_execution_id == execution_id
    ));
    dispatcher
        .read_events(stale_candidate)
        .await
        .expect("a stale second Worker replays the committed batch receipt");

    let client = application
        .store
        .pool()
        .get()
        .await
        .expect("pool connection");
    let row = client
        .query_one(
            "SELECT status, event_cursor, cursor_version, \
                    (SELECT count(*) FROM agent_loom.agent_event_receipts r \
                     WHERE r.tenant_id = x.tenant_id AND r.agent_execution_id = x.agent_execution_id), \
                    (SELECT count(*) FROM agent_loom.events e \
                     WHERE e.tenant_id = x.tenant_id AND e.run_id = x.run_id \
                       AND e.event_type = 'agent.completed') \
             FROM agent_loom.agent_executions x \
             WHERE tenant_id = $1 AND agent_execution_id = $2",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &uuid::Uuid::from_bytes(execution_id.into_bytes()),
            ],
        )
        .await
        .expect("query event-ingested Agent execution");
    assert_eq!(row.get::<_, String>(0), "reconciling");
    assert_eq!(row.get::<_, Option<String>>(1).as_deref(), Some("1"));
    assert_eq!(row.get::<_, i64>(2), 1);
    assert_eq!(row.get::<_, i64>(3), 1);
    assert_eq!(row.get::<_, i64>(4), 1);

    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let status_worker = AgentStatusWorker::new(
        application.store.clone(),
        dispatcher,
        application.tenant_id,
        AgentStatusWorkerConfig::default(),
    )
    .expect("build Agent status worker");
    assert!(matches!(
        status_worker.poll_once().await.expect("reconcile terminal event"),
        AgentStatusPollOutcome::Dispatched(candidate)
            if candidate.execution.agent_execution_id == execution_id
    ));
    let status: String = client
        .query_one(
            "SELECT status FROM agent_loom.agent_executions \
             WHERE tenant_id = $1 AND agent_execution_id = $2",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &uuid::Uuid::from_bytes(execution_id.into_bytes()),
            ],
        )
        .await
        .expect("query terminal Agent execution")
        .get(0);
    assert_eq!(status, "succeeded");
    assert!(matches!(
        event_worker.poll_once().await.expect("poll after terminal"),
        AgentEventPollOutcome::Idle
    ));
}

#[tokio::test]
async fn transactional_outbox_recovers_expired_publish_leases_without_losing_events() {
    let Ok(database_url) = std::env::var("AGENT_LOOM_TEST_POSTGRES_URL") else {
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let (application, _) = bootstrap_outbox_test(&database_url, nonce).await;
    let query = QueryContext {
        tenant_id: application.tenant_id,
        actor_ref: "outbox-e2e".to_owned(),
        authoritative: true,
    };
    let first_publisher = WorkerId::from_bytes(test_id(nonce, "outbox-publisher-1"));
    let first_token = LeaseToken::from_bytes(Sha256::digest(b"outbox-token-1").into());
    let first = application
        .store
        .claim_outbox(
            &query,
            ClaimOutbox {
                publisher_id: first_publisher,
                lease_token: first_token.clone(),
                lease_duration: DurationMicros::new(10_000),
            },
        )
        .await
        .expect("claim first Outbox lease")
        .expect("Run creation atomically produced an Outbox message");
    assert_eq!(first.attempt, 1);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let second_publisher = WorkerId::from_bytes(test_id(nonce, "outbox-publisher-2"));
    let second_token = LeaseToken::from_bytes(Sha256::digest(b"outbox-token-2").into());
    let second = application
        .store
        .claim_outbox(
            &query,
            ClaimOutbox {
                publisher_id: second_publisher,
                lease_token: second_token.clone(),
                lease_duration: DurationMicros::new(1_000_000),
            },
        )
        .await
        .expect("reclaim expired Outbox lease")
        .expect("expired message is reclaimable after a publisher crash");
    assert_eq!(second.outbox_id, first.outbox_id);
    assert_eq!(second.event_id, first.event_id);
    assert_eq!(second.attempt, 2);
    assert!(
        application
            .store
            .record_outbox_delivery(
                &query,
                RecordOutboxDelivery {
                    outbox_id: first.outbox_id,
                    expected_attempt: first.attempt,
                    publisher_id: first_publisher,
                    lease_token: first_token,
                    outcome: OutboxDeliveryOutcome::Published,
                },
            )
            .await
            .is_err(),
        "the expired first publisher must be fenced"
    );
    application
        .store
        .record_outbox_delivery(
            &query,
            RecordOutboxDelivery {
                outbox_id: second.outbox_id,
                expected_attempt: second.attempt,
                publisher_id: second_publisher,
                lease_token: second_token,
                outcome: OutboxDeliveryOutcome::Published,
            },
        )
        .await
        .expect("acknowledge the active Outbox lease");
    assert!(
        application
            .store
            .claim_outbox(
                &query,
                ClaimOutbox {
                    publisher_id: second_publisher,
                    lease_token: LeaseToken::from_bytes([9; 32]),
                    lease_duration: DurationMicros::new(1_000_000),
                },
            )
            .await
            .expect("scan after publish")
            .is_none()
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn execution_plan_dependencies_gate_task_claims_until_conditions_match() {
    let Ok(database_url) = std::env::var("AGENT_LOOM_TEST_POSTGRES_URL") else {
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let config = ServerConfig {
        database_url: database_url.clone(),
        bind: "127.0.0.1:0".to_owned(),
        tenant_key: format!("dependency-e2e-{nonce}"),
        api_key: "mvp-e2e-api-key".to_owned(),
        pool_size: 4,
        http_adapters: None,
    };
    let application = bootstrap(&config)
        .await
        .expect("bootstrap dependency server");
    let plan = json!({
        "schema": "agent-loom.execution-plan/v1",
        "plan_key": "dependency-e2e",
        "initial_tasks": [
            {"key": "root", "handler": "delivery-mvp", "kind": "model"},
            {
                "key": "joined",
                "handler": "delivery-mvp",
                "kind": "model",
                "join_policy": "all",
                "context_projection": ["/goal"],
                "depends_on": [{
                    "task": "root",
                    "condition": {
                        "result_equals": {"pointer": "/approved", "value": true}
                    }
                }]
            },
            {
                "key": "rejected",
                "handler": "delivery-mvp",
                "kind": "model",
                "depends_on": [{
                    "task": "root",
                    "condition": {
                        "result_equals": {"pointer": "/approved", "value": false}
                    }
                }]
            },
            {
                "key": "fallback",
                "handler": "delivery-mvp",
                "kind": "model",
                "depends_on": [{
                    "task": "root",
                    "condition": {"status": "failed"}
                }]
            }
        ]
    });
    let plan_bytes = serde_json::to_vec(&plan).expect("encode dependency plan");
    let plan_digest: [u8; 32] = Sha256::digest(&plan_bytes).into();
    let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("connect dependency fixture database");
    let connection_task = tokio::spawn(connection);
    client
        .execute(
            "UPDATE agent_loom.workflow_definition_versions SET spec_json = $3, spec_digest = $4 \
             WHERE tenant_id = $1 AND workflow_version_id = $2",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &uuid::Uuid::from_bytes(application.workflow_version_id.into_bytes()),
                &plan,
                &plan_digest.as_slice(),
            ],
        )
        .await
        .expect("install dependency ExecutionPlan");
    drop(client);
    connection_task
        .await
        .expect("join dependency fixture connection")
        .expect("dependency fixture connection remains healthy");

    let response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/runs")
                .header("content-type", "application/json")
                .header("authorization", "Bearer mvp-e2e-api-key")
                .header("idempotency-key", format!("dependency-run-{nonce}"))
                .body(Body::from(r#"{"input":{"goal":"join tasks"}}"#))
                .expect("build dependency Run request"),
        )
        .await
        .expect("create dependency Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let run: Value = serde_json::from_slice(
        &to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read dependency Run"),
    )
    .expect("decode dependency Run");
    let run_id = RunId::from_bytes(
        decode_test_id(run["run"]["run_id"].as_str().expect("Run ID")).expect("decode Run ID"),
    );
    let worker_id = WorkerId::from_bytes(test_id(nonce, "dependency-worker"));
    let lease_token = LeaseToken::from_bytes([7; 32]);
    let claim = application
        .store
        .claim_task(
            &test_command_context(application.tenant_id, nonce, "dependency-claim-root"),
            ClaimTask {
                worker_id,
                lease_token: lease_token.clone(),
                lease_duration: DurationMicros::new(5_000_000),
                candidate_window: 8,
                kind: Some(TaskKind::Model),
            },
        )
        .await
        .expect("claim dependency root")
        .expect("root Task is claimable");
    assert_eq!(claim.value.task.logical_key.as_str(), "root");
    let event_id = EventId::from_bytes(test_id(nonce, "dependency-root-completed"));
    let checkpoint_state = JsonPayload::from_validated_bytes(b"{}".to_vec());
    application
        .store
        .complete_task(
            &test_command_context(application.tenant_id, nonce, "dependency-complete-root"),
            CompleteTask {
                expected_run: ExpectedRun {
                    run_id,
                    version: Some(claim.value.run_version),
                    execution_generation: Some(claim.value.task.generation),
                },
                lease: LeaseProof {
                    task_id: claim.value.task.task_id,
                    worker_id,
                    token: lease_token,
                    execution_generation: claim.value.task.generation,
                },
                completion_event_id: event_id,
                checkpoint: NewCheckpoint {
                    checkpoint_id: CheckpointId::from_bytes(test_id(
                        nonce,
                        "dependency-checkpoint",
                    )),
                    sequence: 2,
                    schema_version: 1,
                    workflow_version_id: Some(application.workflow_version_id),
                    coordinator_agent_version_id: Some(application.coordinator_agent_version_id),
                    execution_generation: claim.value.task.generation,
                    state: checkpoint_state.clone(),
                    state_digest: Digest::from_bytes(
                        Sha256::digest(checkpoint_state.as_bytes()).into(),
                    ),
                    created_event_id: event_id,
                },
                task_result: TaskResult {
                    output: JsonPayload::from_validated_bytes(b"{\"approved\":true}".to_vec()),
                },
                stage_mutation: None,
                additional_stage_mutations: Vec::new(),
                new_stages: Vec::new(),
                artifacts: Vec::new(),
                next: NextActions::NoFurtherWork,
            },
        )
        .await
        .expect("complete dependency root");
    let joined = application
        .store
        .claim_task(
            &test_command_context(application.tenant_id, nonce, "dependency-claim-joined"),
            ClaimTask {
                worker_id,
                lease_token: LeaseToken::from_bytes([8; 32]),
                lease_duration: DurationMicros::new(5_000_000),
                candidate_window: 8,
                kind: Some(TaskKind::Model),
            },
        )
        .await
        .expect("claim joined Task")
        .expect("joined Task becomes claimable");
    assert_eq!(joined.value.task.logical_key.as_str(), "joined");
    let projected_context = get_json(
        application.router.clone(),
        &format!("/v1/tasks/{}/context", joined.value.task.task_id),
    )
    .await;
    assert_eq!(projected_context["projection"], json!(["/goal"]));
    assert_eq!(projected_context["context"]["/goal"], "join tasks");

    let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("reconnect dependency fixture database");
    let connection_task = tokio::spawn(connection);
    let rejected_status: String = client
        .query_one(
            "SELECT status FROM agent_loom.tasks \
             WHERE tenant_id = $1 AND run_id = $2 AND logical_key = 'rejected'",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &uuid::Uuid::from_bytes(run_id.into_bytes()),
            ],
        )
        .await
        .expect("query rejected branch")
        .get(0);
    assert_eq!(rejected_status, "skipped");
    let fallback_status: String = client
        .query_one(
            "SELECT status FROM agent_loom.tasks \
             WHERE tenant_id = $1 AND run_id = $2 AND logical_key = 'fallback'",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &uuid::Uuid::from_bytes(run_id.into_bytes()),
            ],
        )
        .await
        .expect("query fallback branch")
        .get(0);
    assert_eq!(fallback_status, "skipped");
    drop(client);
    connection_task
        .await
        .expect("join dependency verification connection")
        .expect("dependency verification connection remains healthy");

    let response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/runs")
                .header("content-type", "application/json")
                .header("authorization", "Bearer mvp-e2e-api-key")
                .header(
                    "idempotency-key",
                    format!("dependency-fallback-run-{nonce}"),
                )
                .body(Body::from(r#"{"input":{"goal":"activate fallback"}}"#))
                .expect("build fallback Run request"),
        )
        .await
        .expect("create fallback Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let fallback_run: Value = serde_json::from_slice(
        &to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read fallback Run"),
    )
    .expect("decode fallback Run");
    let fallback_run_id = RunId::from_bytes(
        decode_test_id(
            fallback_run["run"]["run_id"]
                .as_str()
                .expect("fallback Run ID"),
        )
        .expect("decode fallback Run ID"),
    );
    let fallback_root_lease = LeaseToken::from_bytes([9; 32]);
    let fallback_root = application
        .store
        .claim_task(
            &test_command_context(application.tenant_id, nonce, "fallback-claim-root"),
            ClaimTask {
                worker_id,
                lease_token: fallback_root_lease.clone(),
                lease_duration: DurationMicros::new(5_000_000),
                candidate_window: 8,
                kind: Some(TaskKind::Model),
            },
        )
        .await
        .expect("claim fallback root")
        .expect("fallback root is claimable");
    assert_eq!(fallback_root.value.task.logical_key.as_str(), "root");
    application
        .store
        .fail_task(
            &test_command_context(application.tenant_id, nonce, "fallback-fail-root"),
            FailTask {
                expected_run: ExpectedRun {
                    run_id: fallback_run_id,
                    version: Some(fallback_root.value.run_version),
                    execution_generation: Some(fallback_root.value.task.generation),
                },
                lease: LeaseProof {
                    task_id: fallback_root.value.task.task_id,
                    worker_id,
                    token: fallback_root_lease,
                    execution_generation: fallback_root.value.task.generation,
                },
                failure_event_id: EventId::from_bytes(test_id(nonce, "fallback-root-failed")),
                error_code: "primary_unavailable".to_owned(),
                retry_at: None,
            },
        )
        .await
        .expect("fail primary Task and activate fallback");
    let fallback = application
        .store
        .claim_task(
            &test_command_context(application.tenant_id, nonce, "fallback-claim-branch"),
            ClaimTask {
                worker_id,
                lease_token: LeaseToken::from_bytes([10; 32]),
                lease_duration: DurationMicros::new(5_000_000),
                candidate_window: 8,
                kind: Some(TaskKind::Model),
            },
        )
        .await
        .expect("claim fallback branch")
        .expect("fallback branch becomes claimable");
    assert_eq!(fallback.value.task.logical_key.as_str(), "fallback");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn schedules_persist_cron_and_fire_runs_idempotently() {
    let Ok(database_url) = std::env::var("AGENT_LOOM_TEST_POSTGRES_URL") else {
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let config = ServerConfig {
        database_url: database_url.clone(),
        bind: "127.0.0.1:0".to_owned(),
        tenant_key: format!("schedule-e2e-{nonce}"),
        api_key: "mvp-e2e-api-key".to_owned(),
        pool_size: 4,
        http_adapters: None,
    };
    let application = bootstrap(&config).await.expect("bootstrap Schedule server");
    let schedule_request = serde_json::to_vec(&json!({
        "cron_expression": "*/5 * * * *",
        "timezone": "America/Chicago",
        "misfire_policy": "catch_up",
        "catch_up_limit": 2,
        "input": {"goal": "scheduled delivery"}
    }))
    .expect("encode Schedule request");
    let mut schedule_bodies = Vec::new();
    for expected_disposition in ["applied", "duplicate"] {
        let response = application
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/schedules")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer mvp-e2e-api-key")
                    .header("idempotency-key", format!("schedule-{nonce}"))
                    .body(Body::from(schedule_request.clone()))
                    .expect("build create Schedule request"),
            )
            .await
            .expect("create Schedule response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let body: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("read Schedule response"),
        )
        .expect("decode Schedule response");
        assert_eq!(body["disposition"], expected_disposition);
        schedule_bodies.push(body);
    }
    let schedule_id = schedule_bodies[0]["schedule"]["schedule_id"]
        .as_str()
        .expect("Schedule ID")
        .to_owned();
    assert_eq!(schedule_bodies[1]["schedule"]["schedule_id"], schedule_id);
    assert_eq!(
        schedule_bodies[0]["schedule"]["timezone"],
        "America/Chicago"
    );
    assert_eq!(schedule_bodies[0]["schedule"]["misfire_policy"], "catch_up");
    assert_eq!(
        schedule_bodies[0]["schedule"]["concurrency_policy"],
        "allow"
    );
    assert_eq!(schedule_bodies[0]["schedule"]["catch_up_limit"], 2);
    assert!(
        schedule_bodies[0]["schedule"]["next_fire_at_micros"]
            .as_i64()
            .is_some_and(|value| value > 0)
    );
    let schedules = get_json(application.router.clone(), "/v1/schedules").await;
    assert!(
        schedules
            .as_array()
            .is_some_and(|items| { items.iter().any(|item| item["schedule_id"] == schedule_id) })
    );

    let scheduled_fire_time_micros = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_micros(),
    )
    .expect("test timestamp fits i64")
    .saturating_sub(1_000_000);
    let fire_body = serde_json::to_vec(&json!({
        "scheduled_fire_time_micros": scheduled_fire_time_micros
    }))
    .expect("encode Schedule fire");
    let mut run_bodies = Vec::new();
    for expected_disposition in ["applied", "duplicate"] {
        let response = application
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/schedules/{schedule_id}/fires"))
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer mvp-e2e-api-key")
                    .body(Body::from(fire_body.clone()))
                    .expect("build Schedule fire request"),
            )
            .await
            .expect("Schedule fire response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("read Schedule fire response"),
        )
        .expect("decode Schedule fire response");
        assert_eq!(body["disposition"], expected_disposition);
        run_bodies.push(body);
        if expected_disposition == "applied" {
            let (client, connection) =
                tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
                    .await
                    .expect("connect Schedule receipt cleanup database");
            let connection_task = tokio::spawn(connection);
            client
                .execute(
                    "DELETE FROM agent_loom.command_receipts \
                     WHERE tenant_id = $1 AND scope = 'create_run' AND idempotency_key = $2",
                    &[
                        &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                        &format!("api-create/schedule/{schedule_id}/{scheduled_fire_time_micros}"),
                    ],
                )
                .await
                .expect("remove transient Schedule fire receipt");
            drop(client);
            connection_task
                .await
                .expect("join Schedule receipt cleanup connection")
                .expect("Schedule receipt cleanup connection remains healthy");
        }
    }
    let run_id = run_bodies[0]["run"]["run_id"]
        .as_str()
        .expect("scheduled Run ID");
    assert_eq!(run_bodies[1]["run"]["run_id"], run_id);

    let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("connect Schedule verification database");
    let connection_task = tokio::spawn(connection);
    let fire_count: i64 = client
        .query_one(
            "SELECT count(*) FROM agent_loom.runs \
             WHERE tenant_id = $1 AND schedule_id = $2 \
               AND scheduled_fire_at = to_timestamp(($3::bigint)::double precision / 1000000.0)",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &uuid::Uuid::from_bytes(decode_test_id(&schedule_id).expect("decode Schedule ID")),
                &scheduled_fire_time_micros,
            ],
        )
        .await
        .expect("count persisted Schedule fires")
        .get(0);
    assert_eq!(fire_count, 1);

    let due_fire_at: i64 = client
        .query_one(
            "UPDATE agent_loom.schedules SET \
                next_fire_at = to_timestamp(\
                    floor(extract(epoch FROM transaction_timestamp()) / 300) * 300 - 300\
                ), version = 0 \
             WHERE tenant_id = $1 AND schedule_id = $2 \
             RETURNING (extract(epoch FROM next_fire_at) * 1000000)::bigint",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &uuid::Uuid::from_bytes(decode_test_id(&schedule_id).expect("decode Schedule ID")),
            ],
        )
        .await
        .expect("make Schedule cursor due")
        .get(0);
    let polling = SchedulePollingJob::new(
        AppState {
            store: Arc::new(application.store.clone()),
            tenant_id: application.tenant_id,
            workflow_id: application.workflow_id,
            coordinator_agent_version_id: application.coordinator_agent_version_id,
            api_key: Arc::<str>::from("mvp-e2e-api-key"),
        },
        application.tenant_id,
        SchedulePollingConfig::default(),
    )
    .run_once(0)
    .await
    .expect("scan and dispatch due Schedule");
    assert!(matches!(
        polling,
        PollingActivity::Progress {
            completed: 1,
            failed: 0,
            ..
        }
    ));
    let automated_fire_count: i64 = client
        .query_one(
            "SELECT count(*) FROM agent_loom.runs \
             WHERE tenant_id = $1 AND schedule_id = $2 AND (\
                scheduled_fire_at = to_timestamp(($3::bigint)::double precision / 1000000.0) \
                OR scheduled_fire_at = \
                    to_timestamp(($3::bigint)::double precision / 1000000.0) + interval '5 minutes'\
             )",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &uuid::Uuid::from_bytes(decode_test_id(&schedule_id).expect("decode Schedule ID")),
                &due_fire_at,
            ],
        )
        .await
        .expect("count automatically dispatched Schedule fires")
        .get(0);
    assert_eq!(automated_fire_count, 2);
    let cursor_advanced: bool = client
        .query_one(
            "SELECT version = 1 AND last_fire_at IS NOT NULL \
                AND next_fire_at > transaction_timestamp() \
             FROM agent_loom.schedules WHERE tenant_id = $1 AND schedule_id = $2",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &uuid::Uuid::from_bytes(decode_test_id(&schedule_id).expect("decode Schedule ID")),
            ],
        )
        .await
        .expect("verify Schedule cursor advance")
        .get(0);
    assert!(cursor_advanced);

    let forbid_response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/schedules")
                .header("content-type", "application/json")
                .header("authorization", "Bearer mvp-e2e-api-key")
                .header("idempotency-key", format!("schedule-forbid-{nonce}"))
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "cron_expression": "* * * * *",
                        "timezone": "UTC",
                        "misfire_policy": "fire_once",
                        "concurrency_policy": "forbid",
                        "catch_up_limit": 1,
                        "input": {"goal": "serialize scheduled delivery"}
                    }))
                    .expect("encode forbid Schedule request"),
                ))
                .expect("build forbid Schedule request"),
        )
        .await
        .expect("create forbid Schedule response");
    assert_eq!(forbid_response.status(), StatusCode::CREATED);
    let forbid_body: Value = serde_json::from_slice(
        &to_bytes(forbid_response.into_body(), 64 * 1024)
            .await
            .expect("read forbid Schedule response"),
    )
    .expect("decode forbid Schedule response");
    assert_eq!(forbid_body["schedule"]["concurrency_policy"], "forbid");
    let forbid_schedule_id = forbid_body["schedule"]["schedule_id"]
        .as_str()
        .expect("forbid Schedule ID");
    let first_forbid_fire = scheduled_fire_time_micros.saturating_sub(2_000_000);
    let first_response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/schedules/{forbid_schedule_id}/fires"))
                .header("content-type", "application/json")
                .header("authorization", "Bearer mvp-e2e-api-key")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "scheduled_fire_time_micros": first_forbid_fire
                    }))
                    .expect("encode first forbid fire"),
                ))
                .expect("build first forbid fire"),
        )
        .await
        .expect("first forbid fire response");
    assert_eq!(first_response.status(), StatusCode::ACCEPTED);
    let overlapping_response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/schedules/{forbid_schedule_id}/fires"))
                .header("content-type", "application/json")
                .header("authorization", "Bearer mvp-e2e-api-key")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "scheduled_fire_time_micros": first_forbid_fire + 1_000_000
                    }))
                    .expect("encode overlapping forbid fire"),
                ))
                .expect("build overlapping forbid fire"),
        )
        .await
        .expect("overlapping forbid fire response");
    assert_eq!(overlapping_response.status(), StatusCode::CONFLICT);

    client
        .execute(
            "UPDATE agent_loom.schedules SET \
                next_fire_at = date_trunc('minute', transaction_timestamp()) - interval '1 minute', \
                version = 0 \
             WHERE tenant_id = $1 AND schedule_id = $2",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &uuid::Uuid::from_bytes(
                    decode_test_id(forbid_schedule_id).expect("decode forbid Schedule ID"),
                ),
            ],
        )
        .await
        .expect("make forbid Schedule due");
    let blocked_polling = SchedulePollingJob::new(
        AppState {
            store: Arc::new(application.store.clone()),
            tenant_id: application.tenant_id,
            workflow_id: application.workflow_id,
            coordinator_agent_version_id: application.coordinator_agent_version_id,
            api_key: Arc::<str>::from("mvp-e2e-api-key"),
        },
        application.tenant_id,
        SchedulePollingConfig::default(),
    )
    .run_once(0)
    .await
    .expect("advance blocked forbid Schedule");
    assert!(matches!(
        blocked_polling,
        PollingActivity::Progress {
            completed: 1,
            failed: 0,
            ..
        }
    ));
    let forbid_state = client
        .query_one(
            "SELECT \
                (SELECT count(*) FROM agent_loom.runs \
                 WHERE tenant_id = $1 AND schedule_id = $2), \
                version, next_fire_at > transaction_timestamp() \
             FROM agent_loom.schedules WHERE tenant_id = $1 AND schedule_id = $2",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &uuid::Uuid::from_bytes(
                    decode_test_id(forbid_schedule_id).expect("decode forbid Schedule ID"),
                ),
            ],
        )
        .await
        .expect("verify forbid Schedule state");
    assert_eq!(forbid_state.get::<_, i64>(0), 1);
    assert_eq!(forbid_state.get::<_, i64>(1), 1);
    assert!(forbid_state.get::<_, bool>(2));
    drop(client);
    connection_task
        .await
        .expect("join Schedule verification connection")
        .expect("Schedule verification connection remains healthy");
}

fn decode_test_id(value: &str) -> Result<[u8; 16], ()> {
    if value.len() != 32 {
        return Err(());
    }
    let mut bytes = [0; 16];
    for (index, chunk) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let text = std::str::from_utf8(chunk).map_err(|_| ())?;
        bytes[index] = u8::from_str_radix(text, 16).map_err(|_| ())?;
    }
    Ok(bytes)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn plan_revisions_are_version_fenced_idempotent_and_auditable() {
    let Ok(database_url) = std::env::var("AGENT_LOOM_TEST_POSTGRES_URL") else {
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let (application, run_id) = bootstrap_outbox_test(&database_url, nonce).await;
    let initial = get_plan_revisions(&application, &run_id).await;
    assert_eq!(initial.len(), 1);
    assert_eq!(initial[0]["revision"], 1);
    assert!(initial[0]["parent_plan_revision_id"].is_null());

    let mut revised_plan = initial[0]["plan"].clone();
    revised_plan["extension"] = json!({"revision_test": 2});
    revised_plan["initial_tasks"]
        .as_array_mut()
        .expect("Plan Tasks are an array")
        .push(json!({
            "key": "revision-added-task",
            "handler": "delivery-mvp",
            "kind": "model",
            "priority": 10000,
            "input": {"operation": "replanned"}
        }));
    let request = json!({
        "base_revision": 1,
        "plan": revised_plan,
        "change_summary": {"reason": "add revision fencing"},
    });
    let applied = post_plan_revision(&application, &run_id, "plan-revision-2", &request).await;
    assert_eq!(applied.status(), StatusCode::OK);
    let applied_body: Value = serde_json::from_slice(
        &to_bytes(applied.into_body(), 64 * 1024)
            .await
            .expect("read Plan revision response"),
    )
    .expect("decode Plan revision response");
    assert_eq!(applied_body["plan_revision"], 2);
    assert_eq!(applied_body["disposition"], "applied");

    let claimed = application
        .store
        .claim_task(
            &test_command_context(application.tenant_id, nonce, "claim-replanned-task"),
            ClaimTask {
                worker_id: WorkerId::from_bytes(test_id(nonce, "replan-worker")),
                lease_token: LeaseToken::from_bytes([31; 32]),
                lease_duration: DurationMicros::new(5_000_000),
                candidate_window: 8,
                kind: Some(TaskKind::Model),
            },
        )
        .await
        .expect("claim dynamically added Task")
        .expect("dynamic Task is queued atomically with Plan revision");
    assert_eq!(
        claimed.value.task.logical_key.as_str(),
        "revision-added-task"
    );
    let claimed_input: Value = serde_json::from_slice(claimed.value.task.input.as_bytes())
        .expect("decode dynamically materialized Task input");
    assert_eq!(
        claimed_input["payload"]["run_input"]["goal"],
        "publish reliably"
    );
    assert_eq!(
        claimed_input["payload"]["task_spec"]["operation"],
        "replanned"
    );

    let duplicate = post_plan_revision(&application, &run_id, "plan-revision-2", &request).await;
    assert_eq!(duplicate.status(), StatusCode::OK);
    let duplicate_body: Value = serde_json::from_slice(
        &to_bytes(duplicate.into_body(), 64 * 1024)
            .await
            .expect("read duplicate Plan revision response"),
    )
    .expect("decode duplicate Plan revision response");
    assert_eq!(duplicate_body["disposition"], "duplicate");

    let stale = post_plan_revision(&application, &run_id, "stale-plan-revision", &request).await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    let revisions = get_plan_revisions(&application, &run_id).await;
    assert_eq!(revisions.len(), 2);
    assert_eq!(revisions[1]["revision"], 2);
    assert_eq!(
        revisions[1]["parent_plan_revision_id"],
        revisions[0]["plan_revision_id"]
    );

    let event_response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/runs/{run_id}/events"))
                .header("authorization", "Bearer mvp-e2e-api-key")
                .body(Body::empty())
                .expect("build Event history request"),
        )
        .await
        .expect("Event history response");
    let events: Value = serde_json::from_slice(
        &to_bytes(event_response.into_body(), 256 * 1024)
            .await
            .expect("read Event history"),
    )
    .expect("decode Event history");
    assert_eq!(
        events["events"]
            .as_array()
            .expect("Event history array")
            .iter()
            .filter(|event| event["event_type"] == "run.plan_revised")
            .count(),
        1
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn p2_controls_replay_handoff_and_compensation_are_durable_and_idempotent() {
    let Ok(database_url) = std::env::var("AGENT_LOOM_TEST_POSTGRES_URL") else {
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let (application, source_run_id) = bootstrap_outbox_test(&database_url, nonce).await;

    let opened = post_api_json(
        &application,
        &format!("/v1/runs/{source_run_id}/manual-interventions"),
        "manual-open",
        &json!({"reason": "operator diagnosis"}),
    )
    .await;
    assert_eq!(opened.status(), StatusCode::OK);
    let opened_body: Value = serde_json::from_slice(
        &to_bytes(opened.into_body(), 64 * 1024)
            .await
            .expect("read manual intervention response"),
    )
    .expect("decode manual intervention response");
    assert_eq!(opened_body["status"], "paused");

    let resolved = post_api_json(
        &application,
        &format!("/v1/runs/{source_run_id}/manual-interventions/resolve"),
        "manual-resolve",
        &json!({"reason": "operator approved continuation"}),
    )
    .await;
    assert_eq!(resolved.status(), StatusCode::OK);
    let resolved_body: Value = serde_json::from_slice(
        &to_bytes(resolved.into_body(), 64 * 1024)
            .await
            .expect("read manual resolution response"),
    )
    .expect("decode manual resolution response");
    assert_eq!(resolved_body["status"], "queued");

    let handoff_request = json!({
        "base_revision": 1,
        "task_key": "handoff-to-specialist",
        "target_handler": "delivery-mvp",
        "max_attempts": 2,
        "input": {"specialty": "recovery"}
    });
    let handoff = post_api_json(
        &application,
        &format!("/v1/runs/{source_run_id}/handoffs"),
        "handoff-specialist",
        &handoff_request,
    )
    .await;
    assert_eq!(handoff.status(), StatusCode::OK);
    let handoff_body: Value = serde_json::from_slice(
        &to_bytes(handoff.into_body(), 64 * 1024)
            .await
            .expect("read handoff response"),
    )
    .expect("decode handoff response");
    assert_eq!(handoff_body["plan_revision"], 2);
    assert_eq!(handoff_body["disposition"], "applied");
    let duplicate_handoff = post_api_json(
        &application,
        &format!("/v1/runs/{source_run_id}/handoffs"),
        "handoff-specialist",
        &handoff_request,
    )
    .await;
    assert_eq!(duplicate_handoff.status(), StatusCode::OK);
    let duplicate_handoff_body: Value = serde_json::from_slice(
        &to_bytes(duplicate_handoff.into_body(), 64 * 1024)
            .await
            .expect("read duplicate handoff response"),
    )
    .expect("decode duplicate handoff response");
    assert_eq!(duplicate_handoff_body["disposition"], "duplicate");

    let compensation = post_api_json(
        &application,
        &format!("/v1/runs/{source_run_id}/compensations"),
        "compensate-delivery",
        &json!({
            "base_revision": 2,
            "task_key": "compensate-delivery",
            "target_handler": "delivery-mvp",
            "input": {"operation": "undo_delivery"}
        }),
    )
    .await;
    assert_eq!(compensation.status(), StatusCode::OK);
    let compensation_body: Value = serde_json::from_slice(
        &to_bytes(compensation.into_body(), 64 * 1024)
            .await
            .expect("read compensation response"),
    )
    .expect("decode compensation response");
    assert_eq!(compensation_body["plan_revision"], 3);
    assert_eq!(compensation_body["disposition"], "applied");

    let events = get_json(
        application.router.clone(),
        &format!("/v1/runs/{source_run_id}/events"),
    )
    .await;
    let event_types = events["events"]
        .as_array()
        .expect("Event history array")
        .iter()
        .filter_map(|event| event["event_type"].as_str())
        .collect::<Vec<_>>();
    assert!(event_types.contains(&"run.handoff_requested"));
    assert!(event_types.contains(&"run.compensation_requested"));

    let replay_uri = format!("/v1/runs/{source_run_id}/replay");
    let replay = post_api_json(&application, &replay_uri, "replay-p2-controls", &json!({})).await;
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    let replay_body: Value = serde_json::from_slice(
        &to_bytes(replay.into_body(), 64 * 1024)
            .await
            .expect("read Replay response"),
    )
    .expect("decode Replay response");
    assert_eq!(replay_body["source_run_id"], source_run_id);
    assert_eq!(replay_body["disposition"], "applied");
    let replay_run_id = replay_body["run"]["run_id"]
        .as_str()
        .expect("Replay Run ID")
        .to_owned();
    let duplicate_replay =
        post_api_json(&application, &replay_uri, "replay-p2-controls", &json!({})).await;
    assert_eq!(duplicate_replay.status(), StatusCode::ACCEPTED);
    let duplicate_replay_body: Value = serde_json::from_slice(
        &to_bytes(duplicate_replay.into_body(), 64 * 1024)
            .await
            .expect("read duplicate Replay response"),
    )
    .expect("decode duplicate Replay response");
    assert_eq!(duplicate_replay_body["disposition"], "duplicate");
    assert_eq!(duplicate_replay_body["run"]["run_id"], replay_run_id);

    let replay_revisions = get_plan_revisions(&application, &replay_run_id).await;
    assert_eq!(replay_revisions.len(), 1);
    assert_eq!(replay_revisions[0]["change_summary"]["kind"], "replay");
    let replay_tasks = replay_revisions[0]["plan"]["initial_tasks"]
        .as_array()
        .expect("Replay Plan Tasks");
    assert!(
        replay_tasks.iter().any(|task| {
            task["key"] == "handoff-to-specialist" && task["kind"] == "agent_server"
        })
    );
    assert!(
        replay_tasks
            .iter()
            .any(|task| task["key"] == "compensate-delivery" && task["kind"] == "tool")
    );

    let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("connect Replay verification database");
    let connection_task = tokio::spawn(connection);
    let replay_source: uuid::Uuid = client
        .query_one(
            "SELECT replay_of_run_id FROM agent_loom.runs \
             WHERE tenant_id = $1 AND run_id = $2",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &uuid::Uuid::from_bytes(
                    decode_test_id(&replay_run_id).expect("decode Replay Run ID"),
                ),
            ],
        )
        .await
        .expect("query Replay lineage")
        .get(0);
    assert_eq!(
        replay_source,
        uuid::Uuid::from_bytes(decode_test_id(&source_run_id).expect("decode source Run ID"))
    );
    drop(client);
    connection_task
        .await
        .expect("join Replay verification connection")
        .expect("Replay verification connection remains healthy");
}

#[tokio::test]
async fn context_patches_are_versioned_merged_idempotent_and_lineaged() {
    let Ok(database_url) = std::env::var("AGENT_LOOM_TEST_POSTGRES_URL") else {
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let (application, run_id) = bootstrap_outbox_test(&database_url, nonce).await;
    let initial = get_context_snapshots(&application, &run_id).await;
    assert_eq!(initial.len(), 1);
    assert_eq!(initial[0]["revision"], 1);
    assert_eq!(initial[0]["context"]["goal"], "publish reliably");

    let request = json!({
        "base_revision": 1,
        "merge_strategy": "merge_patch",
        "patch": {"goal": null, "facts": {"approved": true}}
    });
    let applied = post_context_patch(&application, &run_id, "context-2", &request).await;
    assert_eq!(applied.status(), StatusCode::OK);
    let applied_body: Value = serde_json::from_slice(
        &to_bytes(applied.into_body(), 64 * 1024)
            .await
            .expect("read Context patch response"),
    )
    .expect("decode Context patch response");
    assert_eq!(applied_body["context_revision"], 2);
    assert_eq!(applied_body["disposition"], "applied");

    let duplicate = post_context_patch(&application, &run_id, "context-2", &request).await;
    assert_eq!(duplicate.status(), StatusCode::OK);
    let duplicate_body: Value = serde_json::from_slice(
        &to_bytes(duplicate.into_body(), 64 * 1024)
            .await
            .expect("read duplicate Context response"),
    )
    .expect("decode duplicate Context response");
    assert_eq!(duplicate_body["disposition"], "duplicate");

    let stale = post_context_patch(&application, &run_id, "context-stale", &request).await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    let snapshots = get_context_snapshots(&application, &run_id).await;
    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[1]["revision"], 2);
    assert_eq!(
        snapshots[1]["parent_context_snapshot_id"],
        snapshots[0]["context_snapshot_id"]
    );
    assert!(snapshots[1]["context"].get("goal").is_none());
    assert_eq!(snapshots[1]["context"]["facts"]["approved"], true);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn child_runs_are_parent_scoped_idempotent_and_queryable() {
    let Ok(database_url) = std::env::var("AGENT_LOOM_TEST_POSTGRES_URL") else {
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos();
    let (application, parent_run_id) = bootstrap_outbox_test(&database_url, nonce).await;
    let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("connect Child Run fixture database");
    let connection_task = tokio::spawn(connection);
    let parent_uuid =
        uuid::Uuid::from_bytes(decode_test_id(&parent_run_id).expect("decode parent Run ID"));
    let parent_task_id: uuid::Uuid = client
        .query_one(
            "SELECT task_id FROM agent_loom.tasks WHERE tenant_id = $1 AND run_id = $2 \
             ORDER BY task_id LIMIT 1",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &parent_uuid,
            ],
        )
        .await
        .expect("query parent fan-in Task")
        .get(0);
    for branch in ["research", "review"] {
        let request = json!({
            "input": {"branch": branch},
            "parent_run_id": parent_run_id,
            "parent_task_id": parent_task_id.simple().to_string(),
        });
        let response = application
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/runs")
                    .header("authorization", "Bearer mvp-e2e-api-key")
                    .header("content-type", "application/json")
                    .header("idempotency-key", format!("child-{branch}-{nonce}"))
                    .body(Body::from(
                        serde_json::to_vec(&request).expect("encode Child Run request"),
                    ))
                    .expect("build Child Run request"),
            )
            .await
            .expect("Child Run response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    let children = get_json(
        application.router.clone(),
        &format!("/v1/runs/{parent_run_id}/children"),
    )
    .await;
    let children = children
        .as_array()
        .expect("Child Runs response is an array");
    assert_eq!(children.len(), 2);
    assert!(children.iter().all(|child| child["status"] == "queued"));
    let waiting_status: String = client
        .query_one(
            "SELECT status FROM agent_loom.tasks WHERE tenant_id = $1 AND task_id = $2",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &parent_task_id,
            ],
        )
        .await
        .expect("query waiting fan-in Task")
        .get(0);
    assert_eq!(waiting_status, "scheduled");

    for (index, child) in children.iter().enumerate() {
        let child_id = child["run_id"].as_str().expect("Child Run ID");
        let response = application
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/runs/{child_id}/cancel"))
                    .header("authorization", "Bearer mvp-e2e-api-key")
                    .header("content-type", "application/json")
                    .header("idempotency-key", format!("cancel-child-{index}-{nonce}"))
                    .body(Body::from(r#"{"reason":"fan-in settled"}"#))
                    .expect("build Child Run cancel request"),
            )
            .await
            .expect("Child Run cancel response");
        assert_eq!(response.status(), StatusCode::OK);
    }
    let join_response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/v1/runs/{parent_run_id}/child-joins/{}",
                    parent_task_id.simple()
                ))
                .header("authorization", "Bearer mvp-e2e-api-key")
                .header("content-type", "application/json")
                .header("idempotency-key", format!("join-children-{nonce}"))
                .body(Body::from(r#"{"join_policy":"all"}"#))
                .expect("build Child Run join request"),
        )
        .await
        .expect("Child Run join response");
    assert_eq!(join_response.status(), StatusCode::OK);
    let ready_status: String = client
        .query_one(
            "SELECT status FROM agent_loom.tasks WHERE tenant_id = $1 AND task_id = $2",
            &[
                &uuid::Uuid::from_bytes(application.tenant_id.into_bytes()),
                &parent_task_id,
            ],
        )
        .await
        .expect("query ready fan-in Task")
        .get(0);
    assert_eq!(ready_status, "queued");
    drop(client);
    connection_task
        .await
        .expect("join Child Run fixture connection")
        .expect("Child Run fixture connection remains healthy");
}

async fn get_context_snapshots(
    application: &agent_loom_server::BootstrappedServer,
    run_id: &str,
) -> Vec<Value> {
    let response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/runs/{run_id}/context-snapshots"))
                .header("authorization", "Bearer mvp-e2e-api-key")
                .body(Body::empty())
                .expect("build list Context snapshots request"),
        )
        .await
        .expect("list Context snapshots response");
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(
        &to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("read Context snapshots"),
    )
    .expect("decode Context snapshots")
}

async fn post_context_patch(
    application: &agent_loom_server::BootstrappedServer,
    run_id: &str,
    idempotency_key: &str,
    request: &Value,
) -> axum::response::Response {
    application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/runs/{run_id}/context-snapshots"))
                .header("authorization", "Bearer mvp-e2e-api-key")
                .header("content-type", "application/json")
                .header("idempotency-key", idempotency_key)
                .body(Body::from(
                    serde_json::to_vec(request).expect("encode Context request"),
                ))
                .expect("build Context patch request"),
        )
        .await
        .expect("Context patch response")
}

async fn get_plan_revisions(
    application: &agent_loom_server::BootstrappedServer,
    run_id: &str,
) -> Vec<Value> {
    let response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/runs/{run_id}/plan-revisions"))
                .header("authorization", "Bearer mvp-e2e-api-key")
                .body(Body::empty())
                .expect("build list Plan revisions request"),
        )
        .await
        .expect("list Plan revisions response");
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(
        &to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("read Plan revisions"),
    )
    .expect("decode Plan revisions")
}

async fn post_plan_revision(
    application: &agent_loom_server::BootstrappedServer,
    run_id: &str,
    idempotency_key: &str,
    request: &Value,
) -> axum::response::Response {
    application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/runs/{run_id}/plan-revisions"))
                .header("content-type", "application/json")
                .header("authorization", "Bearer mvp-e2e-api-key")
                .header("idempotency-key", idempotency_key)
                .body(Body::from(
                    serde_json::to_vec(request).expect("encode Plan revision request"),
                ))
                .expect("build Plan revision request"),
        )
        .await
        .expect("Plan revision response")
}

async fn post_api_json(
    application: &agent_loom_server::BootstrappedServer,
    uri: &str,
    idempotency_key: &str,
    request: &Value,
) -> axum::response::Response {
    application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .header("authorization", "Bearer mvp-e2e-api-key")
                .header("idempotency-key", idempotency_key)
                .body(Body::from(
                    serde_json::to_vec(request).expect("encode authenticated API request"),
                ))
                .expect("build authenticated API request"),
        )
        .await
        .expect("authenticated API response")
}

async fn bootstrap_outbox_test(
    database_url: &str,
    nonce: u128,
) -> (agent_loom_server::BootstrappedServer, String) {
    let application = bootstrap(&ServerConfig {
        database_url: database_url.to_owned(),
        bind: "127.0.0.1:0".to_owned(),
        tenant_key: format!("outbox-e2e-{nonce}"),
        api_key: "mvp-e2e-api-key".to_owned(),
        pool_size: 4,
        http_adapters: None,
    })
    .await
    .expect("bootstrap MVP server");
    let response = application
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/runs")
                .header("content-type", "application/json")
                .header("authorization", "Bearer mvp-e2e-api-key")
                .header("idempotency-key", format!("outbox-run-{nonce}"))
                .body(Body::from(r#"{"input":{"goal":"publish reliably"}}"#))
                .expect("build create Run request"),
        )
        .await
        .expect("create Run response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body: Value = serde_json::from_slice(
        &to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read create Run response"),
    )
    .expect("decode create Run response");
    let run_id = body["run"]["run_id"]
        .as_str()
        .expect("create response contains Run ID")
        .to_owned();
    (application, run_id)
}

fn test_id(nonce: u128, label: &str) -> [u8; 16] {
    let digest: [u8; 32] = Sha256::digest(format!("{nonce}/{label}").as_bytes()).into();
    let mut id = [0; 16];
    id.copy_from_slice(&digest[..16]);
    id
}

fn test_command_context(
    tenant_id: agent_loom_domain::TenantId,
    nonce: u128,
    scope: &str,
) -> CommandContext {
    let request_hash =
        Digest::from_bytes(Sha256::digest(format!("{nonce}/{scope}").as_bytes()).into());
    CommandContext {
        tenant_id,
        command_id: CommandId::from_bytes(test_id(nonce, &format!("command/{scope}"))),
        correlation_id: CorrelationId::from_bytes(test_id(nonce, "correlation")),
        actor_ref: "agent-stop-e2e".to_owned(),
        scope: ScopeKey::parse(scope.to_owned()).expect("valid test scope"),
        idempotency_key: IdempotencyKey::parse(format!("{scope}-{nonce}"))
            .expect("valid test idempotency key"),
        request_hash,
    }
}

async fn get_json(router: axum::Router, uri: &str) -> Value {
    let response = router
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("authorization", "Bearer mvp-e2e-api-key")
                .body(Body::empty())
                .expect("build query request"),
        )
        .await
        .expect("query response");
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(
        &to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read query body"),
    )
    .expect("decode query response")
}
