use std::{env, error::Error, fmt, sync::Arc};

use agent_loom_domain::{AgentVersionId, EndpointId, TenantId, WorkflowId, WorkflowVersionId};
use agent_loom_store_postgres::{
    PostgresConfig, PostgresMigrationError, PostgresMigrationExecutor, PostgresStore,
};
use deadpool_postgres::{Manager, Pool};
use tokio_postgres::NoTls;
use uuid::Uuid;

use crate::{
    AppState, HttpAdapterSettings,
    identity::{derived_id, tenant_id},
    router,
    worker::DELIVERY_STAGES,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerConfig {
    pub database_url: String,
    pub bind: String,
    pub tenant_key: String,
    pub api_key: String,
    pub pool_size: usize,
    pub http_adapters: Option<HttpAdapterSettings>,
}

impl ServerConfig {
    /// Loads the MVP configuration from environment variables.
    ///
    /// # Errors
    ///
    /// Returns a safe error when the database URL is missing or a numeric option is invalid.
    pub fn from_env() -> Result<Self, BootstrapError> {
        let database_url = env::var("AGENT_LOOM_DATABASE_URL").map_err(|_| {
            BootstrapError::Configuration(
                "AGENT_LOOM_DATABASE_URL must contain a PostgreSQL connection URL".to_owned(),
            )
        })?;
        let bind = env::var("AGENT_LOOM_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
        let tenant_key =
            env::var("AGENT_LOOM_TENANT_KEY").unwrap_or_else(|_| "mvp-local".to_owned());
        if tenant_key.is_empty() || tenant_key.len() > 255 {
            return Err(BootstrapError::Configuration(
                "AGENT_LOOM_TENANT_KEY must contain 1 to 255 characters".to_owned(),
            ));
        }
        let api_key = env::var("AGENT_LOOM_API_KEY").map_err(|_| {
            BootstrapError::Configuration(
                "AGENT_LOOM_API_KEY must contain the HTTP API bearer token".to_owned(),
            )
        })?;
        if api_key.len() < 16 || api_key.len() > 255 {
            return Err(BootstrapError::Configuration(
                "AGENT_LOOM_API_KEY must contain 16 to 255 characters".to_owned(),
            ));
        }
        let pool_size = env::var("AGENT_LOOM_POOL_SIZE").map_or(Ok(8), |value| {
            value.parse::<usize>().map_err(|_| {
                BootstrapError::Configuration(
                    "AGENT_LOOM_POOL_SIZE must be a positive integer".to_owned(),
                )
            })
        })?;
        if pool_size == 0 {
            return Err(BootstrapError::Configuration(
                "AGENT_LOOM_POOL_SIZE must be positive".to_owned(),
            ));
        }
        let http_adapters = load_http_adapter_settings()?;
        Ok(Self {
            database_url,
            bind,
            tenant_key,
            api_key,
            pool_size,
            http_adapters,
        })
    }
}

#[derive(Clone, Debug)]
pub struct BootstrappedServer {
    pub router: axum::Router,
    pub store: PostgresStore,
    pub tenant_id: TenantId,
    pub workflow_id: WorkflowId,
    pub workflow_version_id: WorkflowVersionId,
    pub coordinator_agent_version_id: AgentVersionId,
    pub endpoint_id: EndpointId,
}

/// Applies migrations, provisions the single MVP tenant, and creates the pooled Store.
///
/// # Errors
///
/// Returns configuration, connection, migration, provisioning, or pool construction failures.
pub async fn bootstrap(config: &ServerConfig) -> Result<BootstrappedServer, BootstrapError> {
    let (mut client, connection) = tokio_postgres::connect(&config.database_url, NoTls)
        .await
        .map_err(|_| BootstrapError::Database("cannot connect to PostgreSQL".to_owned()))?;
    let connection_task = tokio::spawn(connection);
    PostgresMigrationExecutor::new(
        &mut client,
        &PostgresConfig::default(),
        env!("CARGO_PKG_VERSION"),
    )
    .migrate()
    .await?;

    let tenant_id = tenant_id(&config.tenant_key);
    let tenant_uuid = Uuid::from_bytes(tenant_id.into_bytes());
    client
        .execute(
            "INSERT INTO agent_loom.tenants (\
                tenant_id, tenant_key, status, policy_json, created_at, updated_at\
             ) VALUES ($1, $2, 'active', '{}'::jsonb, clock_timestamp(), clock_timestamp()) \
             ON CONFLICT (tenant_id) DO UPDATE SET \
                status = 'active', updated_at = clock_timestamp()",
            &[&tenant_uuid, &config.tenant_key],
        )
        .await
        .map_err(|_| BootstrapError::Database("cannot provision the MVP tenant".to_owned()))?;
    let catalog = provision_catalog(&client, tenant_id, config).await?;
    drop(client);
    connection_task
        .await
        .map_err(|_| BootstrapError::Database("migration connection task failed".to_owned()))?
        .map_err(|_| {
            BootstrapError::Database("migration connection closed with an error".to_owned())
        })?;

    let postgres_config = config
        .database_url
        .parse()
        .map_err(|_| BootstrapError::Configuration("PostgreSQL URL is invalid".to_owned()))?;
    let pool = Pool::builder(Manager::new(postgres_config, NoTls))
        .max_size(config.pool_size)
        .build()
        .map_err(|_| BootstrapError::Configuration("PostgreSQL pool is invalid".to_owned()))?;
    let store = PostgresStore::new(pool);
    let state = AppState {
        store: Arc::new(store.clone()),
        tenant_id,
        workflow_id: catalog.workflow_id,
        coordinator_agent_version_id: catalog.agent_version_id,
        api_key: Arc::<str>::from(config.api_key.clone()),
    };
    Ok(BootstrappedServer {
        router: router(state),
        store,
        tenant_id,
        workflow_id: catalog.workflow_id,
        workflow_version_id: catalog.workflow_version_id,
        coordinator_agent_version_id: catalog.agent_version_id,
        endpoint_id: catalog.endpoint_id,
    })
}

#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy, Debug)]
struct CatalogIds {
    workflow_id: WorkflowId,
    workflow_version_id: WorkflowVersionId,
    agent_version_id: AgentVersionId,
    endpoint_id: EndpointId,
}

#[allow(clippy::too_many_lines)]
async fn provision_catalog(
    client: &tokio_postgres::Client,
    tenant_id: TenantId,
    config: &ServerConfig,
) -> Result<CatalogIds, BootstrapError> {
    use sha2::{Digest as _, Sha256};

    let tenant_key = &config.tenant_key;
    let tenant_uuid = Uuid::from_bytes(tenant_id.into_bytes());
    let workflow_id = WorkflowId::from_bytes(derived_id("workflow", tenant_key));
    let legacy_workflow_version_id =
        WorkflowVersionId::from_bytes(derived_id("workflow-version", &workflow_id.to_string()));
    let plan_workflow_version_id =
        WorkflowVersionId::from_bytes(derived_id("workflow-version", &format!("{workflow_id}/2")));
    let workflow_version_id =
        WorkflowVersionId::from_bytes(derived_id("workflow-version", &format!("{workflow_id}/3")));
    let workflow_uuid = Uuid::from_bytes(workflow_id.into_bytes());
    let legacy_workflow_version_uuid = Uuid::from_bytes(legacy_workflow_version_id.into_bytes());
    let plan_workflow_version_uuid = Uuid::from_bytes(plan_workflow_version_id.into_bytes());
    let workflow_version_uuid = Uuid::from_bytes(workflow_version_id.into_bytes());
    let legacy_spec = serde_json::json!({
        "key": "delivery-mvp",
        "version": 1,
        "stages": DELIVERY_STAGES,
        "artifact_contract": {
            "media_type": "application/json",
            "required_per_stage": true
        }
    });
    let stages = DELIVERY_STAGES
        .iter()
        .enumerate()
        .map(|(index, stage)| {
            serde_json::json!({
                "key": stage,
                "activation": if index == 0 { "active" } else { "planned" },
                "assignee": {
                    "kind": "agent",
                    "reference": "mock-delivery-agent"
                },
                "input_contract": {"type": "object"},
                "output_contract": {
                    "type": "object",
                    "required": ["stage", "status"]
                },
                "policy": {"max_attempts": 3}
            })
        })
        .collect::<Vec<_>>();
    let plan_spec = serde_json::json!({
        "schema": "agent-loom.execution-plan/v1",
        "plan_key": "delivery-mvp",
        "stages": stages,
        "initial_tasks": [{
            "key": "requirements-entry",
            "stage_key": "requirements",
            "kind": "agent_server",
            "priority": 10,
            "max_attempts": 3,
            "input": {
                "workflow": "delivery-mvp",
                "step": 0,
                "checkpoint_sequence": 1
            }
        }],
        "extension": {
            "artifact_contract": {
                "media_type": "application/json",
                "required_per_stage": true
            }
        }
    });
    let mut spec = plan_spec.clone();
    spec["initial_tasks"][0]["handler"] = serde_json::json!("delivery-mvp");
    let legacy_spec_bytes = serde_json::to_vec(&legacy_spec)
        .map_err(|_| BootstrapError::Configuration("Workflow spec is invalid".to_owned()))?;
    let legacy_spec_digest: [u8; 32] = Sha256::digest(&legacy_spec_bytes).into();
    let plan_spec_bytes = serde_json::to_vec(&plan_spec)
        .map_err(|_| BootstrapError::Configuration("Workflow spec is invalid".to_owned()))?;
    let plan_spec_digest: [u8; 32] = Sha256::digest(&plan_spec_bytes).into();
    let spec_bytes = serde_json::to_vec(&spec)
        .map_err(|_| BootstrapError::Configuration("Workflow spec is invalid".to_owned()))?;
    let spec_digest: [u8; 32] = Sha256::digest(&spec_bytes).into();
    client
        .execute(
            "INSERT INTO agent_loom.workflow_definitions (\
                workflow_id, tenant_id, workflow_key, name, status, latest_version, \
                created_at, updated_at\
             ) VALUES ($1, $2, 'delivery-mvp', 'Delivery MVP', 'active', 1, \
                clock_timestamp(), clock_timestamp()) \
             ON CONFLICT (tenant_id, workflow_key) DO UPDATE SET \
                name = EXCLUDED.name, status = 'active', \
                updated_at = clock_timestamp()",
            &[&workflow_uuid, &tenant_uuid],
        )
        .await
        .map_err(|_| BootstrapError::Database("cannot provision Workflow".to_owned()))?;
    client
        .execute(
            "INSERT INTO agent_loom.workflow_definition_versions (\
                workflow_version_id, tenant_id, workflow_id, version, lifecycle, spec_json, \
                spec_digest, created_by, created_at, published_at\
             ) VALUES ($1, $2, $3, 1, 'published', $4, $5, 'agent-loom-bootstrap', \
                clock_timestamp(), clock_timestamp()) \
             ON CONFLICT (tenant_id, workflow_id, version) DO NOTHING",
            &[
                &legacy_workflow_version_uuid,
                &tenant_uuid,
                &workflow_uuid,
                &legacy_spec,
                &legacy_spec_digest.as_slice(),
            ],
        )
        .await
        .map_err(|_| BootstrapError::Database("cannot provision Workflow version 1".to_owned()))?;
    client
        .execute(
            "INSERT INTO agent_loom.workflow_definition_versions (\
                workflow_version_id, tenant_id, workflow_id, version, lifecycle, spec_json, \
                spec_digest, created_by, created_at, published_at\
             ) VALUES ($1, $2, $3, 2, 'published', $4, $5, 'agent-loom-bootstrap', \
                clock_timestamp(), clock_timestamp()) \
             ON CONFLICT (tenant_id, workflow_id, version) DO NOTHING",
            &[
                &plan_workflow_version_uuid,
                &tenant_uuid,
                &workflow_uuid,
                &plan_spec,
                &plan_spec_digest.as_slice(),
            ],
        )
        .await
        .map_err(|_| BootstrapError::Database("cannot provision Workflow version 2".to_owned()))?;
    client
        .execute(
            "INSERT INTO agent_loom.workflow_definition_versions (\
                workflow_version_id, tenant_id, workflow_id, version, lifecycle, spec_json, \
                spec_digest, created_by, created_at, published_at\
             ) VALUES ($1, $2, $3, 3, 'published', $4, $5, 'agent-loom-bootstrap', \
                clock_timestamp(), clock_timestamp()) \
             ON CONFLICT (tenant_id, workflow_id, version) DO NOTHING",
            &[
                &workflow_version_uuid,
                &tenant_uuid,
                &workflow_uuid,
                &spec,
                &spec_digest.as_slice(),
            ],
        )
        .await
        .map_err(|_| BootstrapError::Database("cannot provision Workflow version 3".to_owned()))?;
    client
        .execute(
            "UPDATE agent_loom.workflow_definitions \
             SET latest_version = GREATEST(latest_version, 3), updated_at = clock_timestamp() \
             WHERE tenant_id = $1 AND workflow_id = $2",
            &[&tenant_uuid, &workflow_uuid],
        )
        .await
        .map_err(|_| BootstrapError::Database("cannot publish Workflow version 3".to_owned()))?;

    let agent_id = Uuid::from_bytes(derived_id("agent", tenant_key));
    let agent_version_id =
        AgentVersionId::from_bytes(derived_id("agent-version", &agent_id.to_string()));
    let agent_version_uuid = Uuid::from_bytes(agent_version_id.into_bytes());
    client
        .execute(
            "INSERT INTO agent_loom.agent_definitions (\
                agent_id, tenant_id, agent_key, name, status, latest_version, created_at, updated_at\
             ) VALUES ($1, $2, 'mock-delivery-agent', 'Mock Delivery Agent', 'active', 1, \
                clock_timestamp(), clock_timestamp()) \
             ON CONFLICT (tenant_id, agent_key) DO UPDATE SET \
                status = 'active', latest_version = 1, updated_at = clock_timestamp()",
            &[&agent_id, &tenant_uuid],
        )
        .await
        .map_err(|_| BootstrapError::Database("cannot provision Agent".to_owned()))?;
    let agent_digest: [u8; 32] = Sha256::digest(b"mock-delivery-agent-v1").into();
    client
        .execute(
            "INSERT INTO agent_loom.agent_definition_versions (\
                agent_version_id, tenant_id, agent_id, version, lifecycle, system_instructions, \
                model_config_json, tools_json, capabilities_json, handoff_json, guardrails_json, \
                limits_json, spec_digest, created_by, created_at, published_at\
             ) VALUES ($1, $2, $3, 1, 'published', 'Produce the requested stage artifact', \
                '{}'::jsonb, '[]'::jsonb, '{\"mock\":true}'::jsonb, '{}'::jsonb, \
                '{}'::jsonb, '{}'::jsonb, $4, 'agent-loom-bootstrap', \
                clock_timestamp(), clock_timestamp()) \
             ON CONFLICT (tenant_id, agent_id, version) DO NOTHING",
            &[
                &agent_version_uuid,
                &tenant_uuid,
                &agent_id,
                &agent_digest.as_slice(),
            ],
        )
        .await
        .map_err(|_| BootstrapError::Database("cannot provision Agent version".to_owned()))?;

    let endpoint_id = EndpointId::from_bytes(derived_id("endpoint", tenant_key));
    let endpoint_uuid = Uuid::from_bytes(endpoint_id.into_bytes());
    let (adapter_kind, base_uri, protocol_version, capabilities, credential_ref) =
        config.http_adapters.as_ref().map_or_else(
            || {
                (
                    "mock",
                    "mock://delivery",
                    "1",
                    serde_json::json!({"idempotent_submit": true, "events": true}),
                    "env://AGENT_LOOM_MOCK_CREDENTIAL",
                )
            },
            |settings| {
                (
                    "agent-loom-http-v1",
                    settings.agent_base_url.as_str(),
                    "agent-loom-http-v1",
                    serde_json::json!({
                        "idempotent_submit": true,
                        "submission_reconciliation": true,
                        "status_query": true,
                        "resumable_events": true,
                        "cooperative_stop": true,
                        "artifact_output": true
                    }),
                    "env://AGENT_LOOM_AGENT_TOKEN",
                )
            },
        );
    client
        .execute(
            "INSERT INTO agent_loom.agent_endpoints (\
                endpoint_id, tenant_id, endpoint_key, adapter_kind, base_uri, protocol_version, \
                capabilities_json, credential_ref, status, health_checked_at, created_at, updated_at\
             ) VALUES ($1, $2, 'delivery-primary', $3, $4, $5, \
                $6, $7, 'active', NULL, \
                clock_timestamp(), clock_timestamp()) \
             ON CONFLICT (endpoint_id) DO UPDATE SET \
                endpoint_key = EXCLUDED.endpoint_key, adapter_kind = EXCLUDED.adapter_kind, \
                base_uri = EXCLUDED.base_uri, protocol_version = EXCLUDED.protocol_version, \
                credential_ref = EXCLUDED.credential_ref, status = 'active', \
                capabilities_json = EXCLUDED.capabilities_json, \
                updated_at = clock_timestamp()",
            &[
                &endpoint_uuid,
                &tenant_uuid,
                &adapter_kind,
                &base_uri,
                &protocol_version,
                &capabilities,
                &credential_ref,
            ],
        )
        .await
        .map_err(|_| BootstrapError::Database("cannot provision Agent endpoint".to_owned()))?;

    Ok(CatalogIds {
        workflow_id,
        workflow_version_id,
        agent_version_id,
        endpoint_id,
    })
}

fn load_http_adapter_settings() -> Result<Option<HttpAdapterSettings>, BootstrapError> {
    let agent_base_url = env::var("AGENT_LOOM_AGENT_BASE_URL").ok();
    let agent_token = env::var("AGENT_LOOM_AGENT_TOKEN").ok();
    let devops_base_url = env::var("AGENT_LOOM_DEVOPS_BASE_URL").ok();
    let devops_token = env::var("AGENT_LOOM_DEVOPS_TOKEN").ok();
    if agent_base_url.is_none()
        && agent_token.is_none()
        && devops_base_url.is_none()
        && devops_token.is_none()
    {
        return Ok(None);
    }
    let (Some(agent_base_url), Some(agent_token), Some(devops_base_url), Some(devops_token)) =
        (agent_base_url, agent_token, devops_base_url, devops_token)
    else {
        return Err(BootstrapError::Configuration(
            "AGENT_LOOM_AGENT_BASE_URL, AGENT_LOOM_AGENT_TOKEN, AGENT_LOOM_DEVOPS_BASE_URL and AGENT_LOOM_DEVOPS_TOKEN must be configured together"
                .to_owned(),
        ));
    };
    if agent_token.is_empty() || devops_token.is_empty() {
        return Err(BootstrapError::Configuration(
            "External Adapter tokens must not be empty".to_owned(),
        ));
    }
    Ok(Some(HttpAdapterSettings {
        agent_base_url,
        agent_token,
        devops_base_url,
        devops_token,
    }))
}

#[derive(Debug)]
pub enum BootstrapError {
    Configuration(String),
    Database(String),
    Migration(PostgresMigrationError),
}

impl fmt::Display for BootstrapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(message) | Self::Database(message) => formatter.write_str(message),
            Self::Migration(error) => error.fmt(formatter),
        }
    }
}

impl Error for BootstrapError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Migration(error) => Some(error),
            Self::Configuration(_) | Self::Database(_) => None,
        }
    }
}

impl From<PostgresMigrationError> for BootstrapError {
    fn from(value: PostgresMigrationError) -> Self {
        Self::Migration(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_configuration_values_are_documented_contracts() {
        let config = ServerConfig {
            database_url: "postgresql://localhost/test".to_owned(),
            bind: "127.0.0.1:8080".to_owned(),
            tenant_key: "mvp-local".to_owned(),
            api_key: "local-development-key".to_owned(),
            pool_size: 8,
            http_adapters: None,
        };
        assert_eq!(config.bind, "127.0.0.1:8080");
        assert_eq!(config.pool_size, 8);
    }
}
