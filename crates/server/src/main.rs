use std::{error::Error, sync::Arc, time::Duration};

use agent_loom_domain::{CorrelationId, Digest, IdempotencyKey, LeaseToken, ScopeKey, WorkerId};
use agent_loom_durable_store::{CommandContext, QueryContext};
use agent_loom_runtime::{
    DeterministicDueWorkPlanner, DueWorkPollingJob, DueWorkScheduler, DueWorkSchedulerConfig,
    LeaseReclaimPollingJob, PollingService, PollingServiceConfig, RecoveryPollingJob,
    RecoveryWorker, RecoveryWorkerConfig, SeededRecoveryIdentitySource,
};
use agent_loom_server::{
    MaintenancePollingConfig, MaintenancePollingJob, ServerConfig, WorkflowWorker,
    WorkflowWorkerConfig, bootstrap, http_dispatcher, mock_dispatcher,
};
use sha2::{Digest as _, Sha256};
use tokio::sync::watch;
use uuid::Uuid;

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = ServerConfig::from_env()?;
    let application = bootstrap(&config).await?;
    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    let process_instance_id = Uuid::new_v4();
    let worker_id = process_worker_id(&config.tenant_key, "workflow", process_instance_id);
    let dispatcher = match &config.http_adapters {
        Some(settings) => http_dispatcher(
            application.store.clone(),
            application.endpoint_id,
            application.coordinator_agent_version_id,
            settings,
        )?,
        None => mock_dispatcher(
            application.store.clone(),
            application.endpoint_id,
            application.coordinator_agent_version_id,
        )?,
    };
    let workflow_worker = Arc::new(WorkflowWorker::new(
        Arc::new(application.store.clone()),
        application.tenant_id,
        worker_id,
        application.coordinator_agent_version_id,
        application.endpoint_id,
        Arc::new(dispatcher.clone()),
        WorkflowWorkerConfig::default(),
    ));

    let reclaim_worker_id =
        process_worker_id(&config.tenant_key, "lease-reclaimer", process_instance_id);
    let identities = SeededRecoveryIdentitySource::new(
        reclaim_worker_id,
        LeaseToken::from_bytes(random_seed(&config.tenant_key, "lease-reclaimer")),
    )?;
    let reclaim_template = CommandContext {
        tenant_id: application.tenant_id,
        command_id: agent_loom_domain::CommandId::from_bytes([1; 16]),
        correlation_id: CorrelationId::from_bytes(derived_id("correlation", "lease-reclaimer")),
        actor_ref: "agent-loom-lease-reclaimer".to_owned(),
        scope: ScopeKey::parse("reclaim_expired_lease")?,
        idempotency_key: IdempotencyKey::parse("lease-reclaimer-template")?,
        request_hash: Digest::from_bytes([1; 32]),
    };
    let reclaim_service = PollingService::new(
        LeaseReclaimPollingJob::new(application.store.clone(), reclaim_template, identities),
        PollingServiceConfig {
            concurrency: 1,
            busy_delay: Duration::from_millis(10),
            idle_delay: Duration::from_millis(500),
            error_delay: Duration::from_secs(2),
        },
    )?;

    let due_planner = DeterministicDueWorkPlanner::new("agent-loom-scheduler", 100, 3)?;
    let due_scheduler = DueWorkScheduler::new(
        application.store.clone(),
        due_planner,
        DueWorkSchedulerConfig::default(),
    );
    let due_service = PollingService::new(
        DueWorkPollingJob::new(
            due_scheduler,
            QueryContext {
                tenant_id: application.tenant_id,
                actor_ref: "agent-loom-scheduler".to_owned(),
                authoritative: true,
            },
        ),
        PollingServiceConfig {
            concurrency: 1,
            busy_delay: Duration::from_millis(10),
            idle_delay: Duration::from_millis(500),
            error_delay: Duration::from_secs(2),
        },
    )?;

    let recovery_worker_id = process_worker_id(&config.tenant_key, "recovery", process_instance_id);
    let recovery_identities = SeededRecoveryIdentitySource::new(
        recovery_worker_id,
        LeaseToken::from_bytes(random_seed(&config.tenant_key, "recovery")),
    )?;
    let recovery_worker = RecoveryWorker::new(
        application.store.clone(),
        dispatcher,
        RecoveryWorkerConfig::default(),
    );
    let recovery_service = PollingService::new(
        RecoveryPollingJob::new(
            recovery_worker,
            service_context(application.tenant_id, "recovery_worker")?,
            recovery_identities,
        ),
        PollingServiceConfig {
            concurrency: 1,
            busy_delay: Duration::from_millis(10),
            idle_delay: Duration::from_millis(250),
            error_delay: Duration::from_secs(2),
        },
    )?;
    let maintenance_service = PollingService::new(
        MaintenancePollingJob::new(
            Arc::new(application.store.clone()),
            application.tenant_id,
            MaintenancePollingConfig::default(),
        ),
        PollingServiceConfig {
            concurrency: 1,
            busy_delay: Duration::from_millis(10),
            idle_delay: Duration::from_millis(250),
            error_delay: Duration::from_secs(2),
        },
    )?;

    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let worker_shutdown = shutdown_receiver.clone();
    let reclaim_shutdown = shutdown_receiver.clone();
    let due_shutdown = shutdown_receiver.clone();
    let recovery_shutdown = shutdown_receiver.clone();
    let maintenance_shutdown = shutdown_receiver.clone();
    let worker_task = tokio::spawn(async move { workflow_worker.run(worker_shutdown).await });
    let reclaim_task = tokio::spawn(async move { reclaim_service.run(reclaim_shutdown).await });
    let due_task = tokio::spawn(async move { due_service.run(due_shutdown).await });
    let recovery_task = tokio::spawn(async move { recovery_service.run(recovery_shutdown).await });
    let maintenance_task =
        tokio::spawn(async move { maintenance_service.run(maintenance_shutdown).await });

    println!(
        "{}",
        serde_json::json!({
            "level": "info",
            "kind": "server.started",
            "bind": config.bind,
            "tenant_id": application.tenant_id.to_string(),
            "process_instance_id": process_instance_id.to_string(),
            "workflow_worker_id": worker_id.to_string(),
            "lease_reclaimer_worker_id": reclaim_worker_id.to_string(),
            "recovery_worker_id": recovery_worker_id.to_string(),
            "external_adapter_mode": if config.http_adapters.is_some() { "http" } else { "mock" },
        })
    );
    let mut server_shutdown = shutdown_receiver;
    let server = axum::serve(listener, application.router).with_graceful_shutdown(async move {
        while !*server_shutdown.borrow() {
            if server_shutdown.changed().await.is_err() {
                break;
            }
        }
    });
    tokio::select! {
        result = server => result?,
        result = tokio::signal::ctrl_c() => result?,
    }
    let _ = shutdown_sender.send(true);
    let _ = worker_task.await;
    let _ = reclaim_task.await;
    let _ = due_task.await;
    let _ = recovery_task.await;
    let _ = maintenance_task.await;
    Ok(())
}

fn service_context(
    tenant_id: agent_loom_domain::TenantId,
    scope: &str,
) -> Result<CommandContext, Box<dyn Error>> {
    let identity = derived_id("service-context", scope);
    Ok(CommandContext {
        tenant_id,
        command_id: agent_loom_domain::CommandId::from_bytes(identity),
        correlation_id: CorrelationId::from_bytes(identity),
        actor_ref: format!("agent-loom-{scope}"),
        scope: ScopeKey::parse(scope.to_owned())?,
        idempotency_key: IdempotencyKey::parse(format!("{scope}-template"))?,
        request_hash: Digest::from_bytes(Sha256::digest(scope.as_bytes()).into()),
    })
}

fn random_seed(tenant_key: &str, purpose: &str) -> [u8; 32] {
    Sha256::digest(format!("{tenant_key}-{purpose}-{}", Uuid::new_v4()).as_bytes()).into()
}

fn process_worker_id(tenant_key: &str, purpose: &str, process_instance_id: Uuid) -> WorkerId {
    WorkerId::from_bytes(derived_id(
        "process-worker",
        &format!("{tenant_key}/{purpose}/{process_instance_id}"),
    ))
}

fn derived_id(namespace: &str, value: &str) -> [u8; 16] {
    let digest: [u8; 32] = Sha256::digest(format!("{namespace}/{value}").as_bytes()).into();
    let mut id = [0; 16];
    id.copy_from_slice(&digest[..16]);
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_identity_is_stable_only_within_one_process_role() {
        let process = Uuid::from_bytes([1; 16]);
        let first = process_worker_id("tenant-a", "workflow", process);
        let replay = process_worker_id("tenant-a", "workflow", process);
        let other_role = process_worker_id("tenant-a", "recovery", process);

        assert_eq!(first, replay);
        assert_ne!(first, other_role);
        assert!(!first.is_nil());
        assert!(!other_role.is_nil());
    }

    #[test]
    fn restarted_process_never_reuses_worker_identity() {
        let before_restart = process_worker_id("tenant-a", "workflow", Uuid::from_bytes([1; 16]));
        let after_restart = process_worker_id("tenant-a", "workflow", Uuid::from_bytes([2; 16]));

        assert_ne!(before_restart, after_restart);
    }
}
