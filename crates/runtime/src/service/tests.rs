use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU32, Ordering},
};

use super::*;
use agent_loom_domain::{CorrelationId, ScopeKey, TenantId};
use agent_loom_domain::{RunId, TaskId, UnixMicros};
use agent_loom_durable_store::{CommandDisposition, LeaseExpiryAction};

#[derive(Clone, Debug)]
struct ConcurrentJob {
    active: Arc<AtomicU32>,
    maximum: Arc<AtomicU32>,
}

impl PollingJob for ConcurrentJob {
    fn run_once(&self, _slot: u32) -> PollingFuture<'_> {
        Box::pin(async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(5)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(PollingActivity::Progress {
                completed: 1,
                failed: 0,
                last_failure: None,
            })
        })
    }
}

#[tokio::test]
async fn polling_service_bounds_concurrency_and_drains_on_shutdown() {
    let active = Arc::new(AtomicU32::new(0));
    let maximum = Arc::new(AtomicU32::new(0));
    let service = PollingService::new(
        ConcurrentJob {
            active: Arc::clone(&active),
            maximum: Arc::clone(&maximum),
        },
        PollingServiceConfig {
            concurrency: 3,
            busy_delay: Duration::from_millis(1),
            idle_delay: Duration::from_millis(1),
            error_delay: Duration::from_millis(1),
        },
    )
    .expect("service config");
    let (shutdown, receiver) = polling_shutdown_channel();
    let running = tokio::spawn(async move { service.run(receiver).await });

    tokio::time::sleep(Duration::from_millis(8)).await;
    assert!(shutdown.request_shutdown());
    let report = running.await.expect("service join");

    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(maximum.load(Ordering::SeqCst) <= 3);
    assert!(report.batches >= 1);
    assert_eq!(report.attempts, report.completed);
    assert_eq!(report.infrastructure_failures, 0);
}

#[test]
fn polling_service_rejects_invalid_limits_and_delays() {
    let job = ConcurrentJob {
        active: Arc::new(AtomicU32::new(0)),
        maximum: Arc::new(AtomicU32::new(0)),
    };
    let error = PollingService::new(
        job,
        PollingServiceConfig {
            concurrency: 0,
            ..PollingServiceConfig::default()
        },
    )
    .expect_err("zero concurrency");
    assert!(error.safe_message.contains("concurrency"));
}

#[test]
fn seeded_recovery_identities_are_unique_and_redacted() {
    let source = SeededRecoveryIdentitySource::new(
        WorkerId::from_bytes([7; 16]),
        LeaseToken::from_bytes([8; 32]),
    )
    .expect("identity source");
    let first = source.next(0).expect("first identity");
    let second = source.next(0).expect("second identity");

    assert_eq!(first.worker_id, second.worker_id);
    assert_ne!(first.lease_token, second.lease_token);
    assert!(format!("{source:?}").contains("[REDACTED]"));
}

#[test]
fn each_recovery_poll_gets_a_distinct_command_receipt_identity() {
    let template = CommandContext {
        tenant_id: TenantId::from_bytes([1; 16]),
        command_id: CommandId::from_bytes([2; 16]),
        correlation_id: CorrelationId::from_bytes([3; 16]),
        actor_ref: "runtime/recovery".to_owned(),
        scope: ScopeKey::parse("worker.claim").expect("scope"),
        idempotency_key: IdempotencyKey::parse("template").expect("key"),
        request_hash: Digest::from_bytes([4; 32]),
    };
    let first = recovery_claim_context(
        &template,
        &RecoveryLeaseIdentity {
            worker_id: WorkerId::from_bytes([5; 16]),
            lease_token: LeaseToken::from_bytes([6; 32]),
        },
    )
    .expect("first context");
    let second = recovery_claim_context(
        &template,
        &RecoveryLeaseIdentity {
            worker_id: WorkerId::from_bytes([5; 16]),
            lease_token: LeaseToken::from_bytes([7; 32]),
        },
    )
    .expect("second context");

    assert_ne!(first.command_id, second.command_id);
    assert_ne!(first.idempotency_key, second.idempotency_key);
    assert_ne!(first.request_hash, second.request_hash);
    assert_eq!(first.correlation_id, template.correlation_id);
}

#[derive(Debug)]
struct FakeLeaseReclaimStore {
    calls: Mutex<Vec<(CommandId, EventId)>>,
    has_expired_lease: bool,
}

impl LeaseReclaimStore for FakeLeaseReclaimStore {
    fn reclaim_expired_lease<'a>(
        &'a self,
        context: &'a CommandContext,
        command: ReclaimExpiredLease,
    ) -> StoreFuture<'a, Option<Committed<LeaseReclaimOutcome>>> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("reclaim calls lock")
                .push((context.command_id, command.reclaimed_event_id));
            Ok(self.has_expired_lease.then(|| Committed {
                disposition: CommandDisposition::Applied,
                value: LeaseReclaimOutcome {
                    tenant_id: context.tenant_id,
                    run_id: RunId::from_bytes([10; 16]),
                    task_id: TaskId::from_bytes([11; 16]),
                    attempt: 1,
                    action: LeaseExpiryAction::RetryScheduled,
                    reclaimed_at: UnixMicros::new(12),
                },
                event_ids: vec![command.reclaimed_event_id],
                durable_follow_ups: Vec::new(),
                post_commit_hints: Vec::new(),
            }))
        })
    }
}

#[tokio::test]
async fn lease_reclaim_job_uses_unique_event_and_receipt_identity() {
    let template = CommandContext {
        tenant_id: TenantId::from_bytes([1; 16]),
        command_id: CommandId::from_bytes([2; 16]),
        correlation_id: CorrelationId::from_bytes([3; 16]),
        actor_ref: "runtime/lease-reclaimer".to_owned(),
        scope: ScopeKey::parse("scheduler.lease_reclaim").expect("scope"),
        idempotency_key: IdempotencyKey::parse("template").expect("key"),
        request_hash: Digest::from_bytes([4; 32]),
    };
    let job = LeaseReclaimPollingJob::new(
        FakeLeaseReclaimStore {
            calls: Mutex::new(Vec::new()),
            has_expired_lease: true,
        },
        template,
        SeededRecoveryIdentitySource::new(
            WorkerId::from_bytes([5; 16]),
            LeaseToken::from_bytes([6; 32]),
        )
        .expect("identity source"),
    );

    assert!(matches!(
        job.run_once(0).await.expect("first reclaim"),
        PollingActivity::Progress { completed: 1, .. }
    ));
    assert!(matches!(
        job.run_once(0).await.expect("second reclaim"),
        PollingActivity::Progress { completed: 1, .. }
    ));
    let calls = job.store.calls.lock().expect("reclaim calls lock");
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].0.into_bytes(), calls[0].1.into_bytes());
    assert_ne!(calls[0].0, calls[1].0);
}
