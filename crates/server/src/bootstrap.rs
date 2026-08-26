use std::{env, error::Error, fmt, sync::Arc};

use agent_loom_domain::{AgentVersionId, EndpointId, TenantId, WorkflowId, WorkflowVersionId};
use agent_loom_store_postgres::{
    PostgresConfig, PostgresMigrationError, PostgresMigrationExecutor, PostgresStore,
};
use deadpool_postgres::{Manager, Pool};
use tokio_postgres::NoTls;
use uuid::Uuid;

use crate::{
    AppState,
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
        Ok(Self {
            database_url,
            bind,
            tenant_key,
            api_key,
            pool_size,
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
    let catalog = provision_catalog(&client, tenant_id, &config.tenant_key).await?;
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
        workflow_version_id: catalog.workflow_version_id,
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
    tenant_key: &str,
) -> Result<CatalogIds, BootstrapError> {
    use sha2::{Digest as _, Sha256};

    let tenant_uuid = Uuid::from_bytes(tenant_id.into_bytes());
    let workflow_id = WorkflowId::from_bytes(derived_id("workflow", tenant_key));
    let workflow_version_id =
        WorkflowVersionId::from_bytes(derived_id("workflow-version", &workflow_id.to_string()));
    let workflow_uuid = Uuid::from_bytes(workflow_id.into_bytes());
    let workflow_version_uuid = Uuid::from_bytes(workflow_version_id.into_bytes());
    let spec = serde_json::json!({
        "key": "delivery-mvp",
        "version": 1,
        "stages": DELIVERY_STAGES,
        "artifact_contract": {
            "media_type": "application/json",
            "required_per_stage": true
        }
    });
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
                name = EXCLUDED.name, status = 'active', latest_version = 1, \
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
                &workflow_version_uuid,
                &tenant_uuid,
                &workflow_uuid,
                &spec,
                &spec_digest.as_slice(),
            ],
        )
        .await
        .map_err(|_| BootstrapError::Database("cannot provision Workflow version".to_owned()))?;

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
    client
        .execute(
            "INSERT INTO agent_loom.agent_endpoints (\
                endpoint_id, tenant_id, endpoint_key, adapter_kind, base_uri, protocol_version, \
                capabilities_json, credential_ref, status, health_checked_at, created_at, updated_at\
             ) VALUES ($1, $2, 'mock-delivery', 'mock', 'mock://delivery', '1', \
                '{\"idempotent_submit\":true,\"events\":true}'::jsonb, \
                'env://AGENT_LOOM_MOCK_CREDENTIAL', 'active', NULL, \
                clock_timestamp(), clock_timestamp()) \
             ON CONFLICT (tenant_id, endpoint_key) DO UPDATE SET \
                status = 'active', capabilities_json = EXCLUDED.capabilities_json, \
                updated_at = clock_timestamp()",
            &[&endpoint_uuid, &tenant_uuid],
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
        };
        assert_eq!(config.bind, "127.0.0.1:8080");
        assert_eq!(config.pool_size, 8);
    }
}
