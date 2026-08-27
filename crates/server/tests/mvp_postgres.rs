use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use agent_loom_domain::{
    AgentExecutionId, AgentExecutionStatus, CommandId, CorrelationId, Digest, DurationMicros,
    EventId, IdempotencyKey, JsonPayload, LeaseToken, RunId, ScopeKey, WorkerId,
};
use agent_loom_durable_store::{
    AgentEventQuery, AgentSubmissionOutcome, ClaimOutbox, ClaimTask, CommandContext,
    CommandDisposition, ControlRun, DurableStore as _, ExpectedRun, LeaseProof,
    OutboxDeliveryOutcome, PrepareAgentExecution, QueryContext, RecordAgentSubmission,
    RecordOutboxDelivery,
};
use agent_loom_runtime::{
    AgentEventDispatcher as _, AgentEventPollOutcome, AgentEventWorker, AgentEventWorkerConfig,
    AgentStatusPollOutcome, AgentStatusWorker, AgentStatusWorkerConfig, AgentStopPollOutcome,
    AgentStopWorker, AgentStopWorkerConfig, ExternalRecoveryDispatcher as _, PollingActivity,
    PollingJob as _, RecoveryDispatchFence, StartedRecovery,
};
use agent_loom_server::{
    MaintenancePollingConfig, MaintenancePollingJob, ServerConfig, WorkflowWorker,
    WorkflowWorkerActivity, WorkflowWorkerConfig, bootstrap, mock_dispatcher,
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
