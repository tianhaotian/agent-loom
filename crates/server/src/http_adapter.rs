use std::{fmt, sync::Arc};

use agent_loom_adapter_core::{
    AdapterCallContext, AdapterError, ExecutionId, ResolvedAuth, TraceContext,
};
use agent_loom_adapter_http::{HttpAgentServerAdapter, HttpDevOpsToolAdapter, HttpEndpointConfig};
use agent_loom_domain::{AgentVersionId, DurationMicros, EndpointId, UnixMicros};
use agent_loom_runtime::{
    AdapterContextFactory, AdapterContextFuture, AdapterContextSeed, AdapterRecoveryDispatcher,
    AdapterRetrySchedule, ExternalDispatchError, StaticAdapterRegistry,
};
use agent_loom_store_postgres::PostgresStore;
use sha2::{Digest as _, Sha256};

use crate::{SharedExternalDispatcher, identity::now_micros};

#[derive(Clone, PartialEq, Eq)]
pub struct HttpAdapterSettings {
    pub agent_base_url: String,
    pub agent_token: String,
    pub devops_base_url: String,
    pub devops_token: String,
}

impl fmt::Debug for HttpAdapterSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpAdapterSettings")
            .field("agent_base_url", &self.agent_base_url)
            .field("agent_token", &"[REDACTED]")
            .field("devops_base_url", &self.devops_base_url)
            .field("devops_token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone)]
struct HttpAdapterContextFactory {
    agent_token: Arc<str>,
    devops_token: Arc<str>,
}

impl fmt::Debug for HttpAdapterContextFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpAdapterContextFactory")
            .field("agent_token", &"[REDACTED]")
            .field("devops_token", &"[REDACTED]")
            .finish()
    }
}

impl AdapterContextFactory for HttpAdapterContextFactory {
    fn create(&self, seed: AdapterContextSeed) -> AdapterContextFuture<'_> {
        let secret = match seed.execution_id {
            ExecutionId::Agent(_) => Arc::clone(&self.agent_token),
            ExecutionId::Tool(_) => Arc::clone(&self.devops_token),
        };
        Box::pin(async move {
            if secret.is_empty() {
                return Err(AdapterError {
                    code: "AUTHENTICATION_FAILED",
                    retry: agent_loom_adapter_core::AdapterRetryClass::ManualReview,
                    safe_message: "External Adapter credential is empty".to_owned(),
                    remote_request_id: None,
                    retry_after: None,
                });
            }
            let trace_parent = format!("00-{}-0000000000000001-01", trace_id(&seed));
            Ok(AdapterCallContext {
                tenant_id: seed.tenant_id,
                execution_id: seed.execution_id,
                correlation_id: seed.correlation_id,
                causation_id: None,
                idempotency_key: seed.idempotency_key,
                request_hash: seed.request_hash,
                deadline: UnixMicros::new(now_micros().saturating_add(60_000_000)),
                trace_context: TraceContext {
                    trace_parent,
                    trace_state: None,
                },
                auth: ResolvedAuth::new("bearer", secret.as_ref()),
            })
        })
    }
}

#[derive(Clone, Debug, Default)]
struct HttpRetrySchedule;

impl AdapterRetrySchedule for HttpRetrySchedule {
    fn retry_at(
        &self,
        error: &AdapterError,
        attempt: u64,
    ) -> Result<UnixMicros, ExternalDispatchError> {
        let exponent = u32::try_from(attempt.min(6)).unwrap_or(6);
        let default_delay = 1_000_000_u64.saturating_mul(2_u64.saturating_pow(exponent));
        let delay = error
            .retry_after
            .unwrap_or(DurationMicros::new(default_delay));
        let delay = i64::try_from(delay.get()).map_err(|_| ExternalDispatchError {
            safe_message: "Adapter retry delay exceeds timestamp range".to_owned(),
        })?;
        Ok(UnixMicros::new(now_micros().saturating_add(delay)))
    }

    fn status_poll_at(&self, observation: u64) -> Result<UnixMicros, ExternalDispatchError> {
        let exponent = u32::try_from(observation.min(5)).unwrap_or(5);
        let delay = 1_000_000_i64.saturating_mul(2_i64.saturating_pow(exponent));
        Ok(UnixMicros::new(now_micros().saturating_add(delay)))
    }
}

/// Registers the production HTTP Agent Server and DevOps Tool profiles.
///
/// # Errors
///
/// Returns a safe configuration or registration error for invalid endpoints or identities.
pub fn http_dispatcher(
    store: PostgresStore,
    endpoint_id: EndpointId,
    agent_version_id: AgentVersionId,
    settings: &HttpAdapterSettings,
) -> Result<SharedExternalDispatcher, HttpDispatcherError> {
    let agent_endpoint = HttpEndpointConfig::new(&settings.agent_base_url)?;
    let devops_endpoint = HttpEndpointConfig::new(&settings.devops_base_url)?;
    let mut registry = StaticAdapterRegistry::new();
    registry.register_tool(Arc::new(HttpDevOpsToolAdapter::new(devops_endpoint)))?;
    registry.register_agent(
        endpoint_id,
        agent_version_id,
        Arc::new(HttpAgentServerAdapter::new(agent_endpoint)),
    )?;
    let context_factory = HttpAdapterContextFactory {
        agent_token: Arc::from(settings.agent_token.clone()),
        devops_token: Arc::from(settings.devops_token.clone()),
    };
    Ok(SharedExternalDispatcher::new(Arc::new(
        AdapterRecoveryDispatcher::new(
            store,
            Arc::new(registry),
            Arc::new(context_factory),
            Arc::new(HttpRetrySchedule),
        ),
    )))
}

#[derive(Debug)]
pub enum HttpDispatcherError {
    Configuration(agent_loom_adapter_http::HttpAdapterConfigurationError),
    Registration(agent_loom_runtime::AdapterRegistrationError),
}

impl fmt::Display for HttpDispatcherError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(error) => error.fmt(formatter),
            Self::Registration(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for HttpDispatcherError {}

impl From<agent_loom_adapter_http::HttpAdapterConfigurationError> for HttpDispatcherError {
    fn from(value: agent_loom_adapter_http::HttpAdapterConfigurationError) -> Self {
        Self::Configuration(value)
    }
}

impl From<agent_loom_runtime::AdapterRegistrationError> for HttpDispatcherError {
    fn from(value: agent_loom_runtime::AdapterRegistrationError) -> Self {
        Self::Registration(value)
    }
}

fn trace_id(seed: &AdapterContextSeed) -> String {
    let bytes: [u8; 32] = Sha256::digest(format!("{:?}", seed.execution_id).as_bytes()).into();
    hex(&bytes[..16])
}

fn hex(bytes: &[u8]) -> String {
    use fmt::Write as _;
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(value, "{byte:02x}");
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_debug_output_redacts_both_tokens() {
        let settings = HttpAdapterSettings {
            agent_base_url: "https://agent.example.test".to_owned(),
            agent_token: "agent-secret".to_owned(),
            devops_base_url: "https://deploy.example.test".to_owned(),
            devops_token: "devops-secret".to_owned(),
        };
        let debug = format!("{settings:?}");
        assert!(!debug.contains("agent-secret"));
        assert!(!debug.contains("devops-secret"));
    }
}
