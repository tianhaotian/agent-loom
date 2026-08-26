use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use agent_loom_domain::WorkerId;
use agent_loom_runtime::{PollingActivity, PollingJob as _};
use agent_loom_server::{
    MaintenancePollingConfig, MaintenancePollingJob, MockWorkerActivity, MockWorkerConfig,
    MockWorkflowWorker, ServerConfig, bootstrap, mock_dispatcher,
};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
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
    let worker = MockWorkflowWorker::new(
        Arc::new(application.store.clone()),
        application.tenant_id,
        WorkerId::from_bytes(nonce.to_be_bytes()),
        application.coordinator_agent_version_id,
        application.endpoint_id,
        Arc::new(dispatcher),
        MockWorkerConfig::default(),
    );
    for step in 0..9 {
        let activity = worker.run_once().await.expect("complete mock stage");
        assert!(
            matches!(
                activity,
                MockWorkerActivity::Completed {
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
            matches!(activity, MockWorkerActivity::Completed { terminal, .. } if terminal == (step == 10))
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
