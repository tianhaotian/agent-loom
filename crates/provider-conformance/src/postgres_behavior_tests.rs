use std::time::{SystemTime, UNIX_EPOCH};

use agent_loom_domain::{RunStatus, TenantId};
use agent_loom_store_postgres::{PostgresConfig, PostgresMigrationExecutor, PostgresStore};
use deadpool_postgres::{Manager, Pool};
use tokio_postgres::NoTls;
use uuid::Uuid;

use crate::{
    LeaseExpiryRetryFixture, Phase2aReliabilityFixture, exercise_lease_expiry_retry,
    exercise_phase2a_reliability,
};

#[tokio::test]
async fn postgres_satisfies_behavior_suite_when_configured() {
    let Ok(url) = std::env::var("AGENT_LOOM_TEST_POSTGRES_URL") else {
        return;
    };
    let (mut setup_client, setup_connection) = tokio_postgres::connect(&url, NoTls)
        .await
        .expect("connect to conformance PostgreSQL");
    let setup_connection_task = tokio::spawn(setup_connection);
    PostgresMigrationExecutor::new(
        &mut setup_client,
        &PostgresConfig::default(),
        env!("CARGO_PKG_VERSION"),
    )
    .migrate()
    .await
    .expect("apply PostgreSQL migrations");

    let identity_seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos()
        .to_be_bytes();
    let tenant_id = TenantId::from_bytes(identity_seed);
    let tenant_uuid = Uuid::from_bytes(identity_seed);
    let tenant_key = format!("conformance-{tenant_uuid}");
    setup_client
        .execute(
            "INSERT INTO agent_loom.tenants (\
                tenant_id, tenant_key, status, policy_json, created_at, updated_at\
             ) VALUES ($1, $2, 'active', '{}'::jsonb, clock_timestamp(), clock_timestamp())",
            &[&tenant_uuid, &tenant_key],
        )
        .await
        .expect("provision conformance Tenant");

    let postgres_config = url.parse().expect("parse PostgreSQL test URL");
    let pool = Pool::builder(Manager::new(postgres_config, NoTls))
        .max_size(4)
        .build()
        .expect("build PostgreSQL conformance pool");
    let store = PostgresStore::new(pool.clone());
    let report = exercise_lease_expiry_retry(
        &store,
        LeaseExpiryRetryFixture {
            tenant_id,
            identity_seed,
            actor_ref: "provider-conformance".to_owned(),
        },
    )
    .await
    .expect("PostgreSQL satisfies expired-Lease behavior");
    assert_eq!(report.first_attempt, 1);
    assert_eq!(report.retry_attempt, 2);
    assert_eq!(report.final_status, RunStatus::Cancelled);

    let phase2a = exercise_phase2a_reliability(
        &store,
        Phase2aReliabilityFixture {
            tenant_id,
            identity_seed,
            actor_ref: "provider-conformance-phase2a".to_owned(),
        },
    )
    .await
    .expect("PostgreSQL satisfies Phase 2A reliability behavior");
    assert_eq!(phase2a.concurrent_claim_winners, 1);
    assert!(matches!(
        phase2a.terminal_race_status,
        RunStatus::Completed | RunStatus::Cancelled
    ));
    assert_eq!(phase2a.consumed_wait_events, 1);
    assert!(phase2a.atomic_rollback_preserved_version);

    pool.close();
    drop(store);
    drop(setup_client);
    setup_connection_task
        .await
        .expect("setup connection task joins")
        .expect("setup PostgreSQL connection stays healthy");
}
