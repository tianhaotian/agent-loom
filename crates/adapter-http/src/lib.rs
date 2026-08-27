//! Production HTTP profiles for a remote Agent Server and a DevOps deployment service.

use std::{
    fmt,
    net::IpAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agent_loom_adapter_core::{
    AdapterCallContext, AdapterError, AdapterFuture, AdapterRetryClass, AgentCapabilities,
    AgentRunRequest, AgentServerAdapter, CompensationOutcome, CompensationRequest, EventReadLimits,
    NormalizedAgentEvent, RemoteAgentRef, RemoteAgentSnapshot, RemoteAgentStatus, RemoteEventBatch,
    SideEffectClass, StopRequestOutcome, SubmitAgentOutcome, ToolAdapter, ToolCallOutcome,
    ToolCapabilities, ToolDescriptor, ToolQueryOutcome, ToolRequest,
};
use agent_loom_domain::{Digest, DurationMicros, JsonPayload};
use reqwest::{
    Client, Method, StatusCode, Url,
    header::{HeaderMap, HeaderValue},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

pub const HTTP_PROTOCOL_VERSION: &str = "agent-loom-http-v1";
const DEFAULT_MAX_RESPONSE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone)]
pub struct HttpEndpointConfig {
    base_url: Url,
    max_response_bytes: u64,
    client: Client,
}

impl fmt::Debug for HttpEndpointConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpEndpointConfig")
            .field("base_url", &self.base_url)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish_non_exhaustive()
    }
}

impl HttpEndpointConfig {
    /// Creates a hardened endpoint profile. HTTPS is required except for explicit loopback URLs.
    ///
    /// # Errors
    ///
    /// Returns a safe configuration error for invalid, credential-bearing, or insecure remote URLs.
    pub fn new(base_url: &str) -> Result<Self, HttpAdapterConfigurationError> {
        let base_url =
            Url::parse(base_url).map_err(|_| configuration("Adapter base URL is invalid"))?;
        validate_base_url(&base_url)?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| configuration("HTTP client could not be initialized"))?;
        Ok(Self {
            base_url,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            client,
        })
    }

    /// Overrides the bounded response size used before JSON decoding.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the limit is zero.
    pub fn with_max_response_bytes(
        mut self,
        max_response_bytes: u64,
    ) -> Result<Self, HttpAdapterConfigurationError> {
        if max_response_bytes == 0 {
            return Err(configuration("Adapter response limit must be positive"));
        }
        self.max_response_bytes = max_response_bytes;
        Ok(self)
    }

    fn endpoint(&self, segments: &[&str]) -> Result<Url, AdapterError> {
        let mut url = self.base_url.clone();
        {
            let mut path = url.path_segments_mut().map_err(|()| {
                protocol_error("Adapter base URL cannot contain hierarchical paths")
            })?;
            path.pop_if_empty();
            path.extend(segments.iter().copied());
        }
        Ok(url)
    }
}

#[derive(Clone, Debug)]
pub struct HttpAgentServerAdapter {
    endpoint: HttpEndpointConfig,
}

impl HttpAgentServerAdapter {
    pub const fn new(endpoint: HttpEndpointConfig) -> Self {
        Self { endpoint }
    }
}

impl AgentServerAdapter for HttpAgentServerAdapter {
    fn kind(&self) -> &'static str {
        HTTP_PROTOCOL_VERSION
    }

    fn capabilities(&self) -> AgentCapabilities {
        AgentCapabilities {
            submission_idempotency: true,
            submission_reconciliation: true,
            status_query: true,
            resumable_events: true,
            cooperative_stop: true,
            approvals: false,
            guidance: false,
            artifact_output: true,
        }
    }

    fn submit<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        request: AgentRunRequest,
    ) -> AdapterFuture<'a, SubmitAgentOutcome> {
        Box::pin(async move {
            let url = self.endpoint.endpoint(&["v1", "agent-runs"])?;
            let body = AgentSubmitBody {
                instructions: request.instructions,
                input: decode_payload(&request.input)?,
                budget: AgentBudgetBody {
                    max_duration_micros: request.budget.max_duration.get(),
                    max_output_bytes: request.budget.max_output_bytes,
                },
            };
            let response = send_json::<_, AgentRefBody>(
                &self.endpoint,
                context,
                Method::POST,
                url,
                Some(&body),
            )
            .await;
            match response {
                Ok(response) => Ok(SubmitAgentOutcome::Accepted(response.into_remote()?)),
                Err(error) if submission_may_be_uncertain(&error) => {
                    Ok(SubmitAgentOutcome::SubmissionUncertain)
                }
                Err(error) => Err(error),
            }
        })
    }

    fn reconcile_submission<'a>(
        &'a self,
        context: &'a AdapterCallContext,
    ) -> AdapterFuture<'a, Option<RemoteAgentRef>> {
        Box::pin(async move {
            let mut url = self
                .endpoint
                .endpoint(&["v1", "agent-runs", "by-idempotency"])?;
            url.query_pairs_mut()
                .append_pair("key", context.idempotency_key.as_str());
            match send_json::<(), AgentRefBody>(&self.endpoint, context, Method::GET, url, None)
                .await
            {
                Ok(response) => Ok(Some(response.into_remote()?)),
                Err(error) if error.code == "REMOTE_RUN_NOT_FOUND" => Ok(None),
                Err(error) => Err(error),
            }
        })
    }

    fn get_status<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        remote: &'a RemoteAgentRef,
    ) -> AdapterFuture<'a, RemoteAgentSnapshot> {
        Box::pin(async move {
            let url = self
                .endpoint
                .endpoint(&["v1", "agent-runs", &remote.remote_run_id])?;
            let response =
                send_json::<(), AgentStatusBody>(&self.endpoint, context, Method::GET, url, None)
                    .await?;
            let response_remote = response.remote.into_remote()?;
            if response_remote.remote_run_id != remote.remote_run_id {
                return Err(protocol_error(
                    "Agent status response changed the remote Run identity",
                ));
            }
            Ok(RemoteAgentSnapshot {
                remote: response_remote,
                status: parse_agent_status(&response.status)?,
                result: response.result.as_ref().map(encode_payload).transpose()?,
            })
        })
    }

    fn read_events<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        remote: &'a RemoteAgentRef,
        cursor: Option<&'a str>,
        limits: EventReadLimits,
    ) -> AdapterFuture<'a, RemoteEventBatch> {
        Box::pin(async move {
            if limits.max_events == 0 || limits.max_bytes == 0 || limits.max_wait.get() == 0 {
                return Err(protocol_error("Agent event limits must be positive"));
            }
            let mut url =
                self.endpoint
                    .endpoint(&["v1", "agent-runs", &remote.remote_run_id, "events"])?;
            {
                let mut query = url.query_pairs_mut();
                query.append_pair("limit", &limits.max_events.to_string());
                query.append_pair("max_bytes", &limits.max_bytes.to_string());
                query.append_pair("max_wait_micros", &limits.max_wait.get().to_string());
                if let Some(cursor) = cursor {
                    query.append_pair("cursor", cursor);
                }
            }
            let response = send_json::<(), AgentEventBatchBody>(
                &self.endpoint,
                context,
                Method::GET,
                url,
                None,
            )
            .await?;
            if response.events.len() > usize::try_from(limits.max_events).unwrap_or(usize::MAX) {
                return Err(protocol_error(
                    "Agent event response exceeded the requested event limit",
                ));
            }
            let mut events = Vec::with_capacity(response.events.len());
            for event in response.events {
                if event.kind.is_empty() {
                    return Err(protocol_error("Agent event kind must not be empty"));
                }
                let payload = encode_payload(&event.payload)?;
                events.push(NormalizedAgentEvent {
                    source_event_id: event.id,
                    source_sequence: event.sequence,
                    kind: event.kind,
                    authoritative: event.authoritative,
                    raw_digest: digest(payload.as_bytes()),
                    payload,
                });
            }
            Ok(RemoteEventBatch {
                events,
                next_cursor: response.next_cursor,
                terminal: response.terminal,
            })
        })
    }

    fn request_stop<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        remote: &'a RemoteAgentRef,
        reason: &'a str,
    ) -> AdapterFuture<'a, StopRequestOutcome> {
        Box::pin(async move {
            let url =
                self.endpoint
                    .endpoint(&["v1", "agent-runs", &remote.remote_run_id, "stop"])?;
            let response = send_json::<_, StopBody>(
                &self.endpoint,
                context,
                Method::POST,
                url,
                Some(&StopRequestBody { reason }),
            )
            .await;
            match response {
                Ok(response) if response.status == "accepted" => Ok(StopRequestOutcome::Accepted {
                    cooperative: response.cooperative.unwrap_or(true),
                }),
                Ok(response) if response.status == "already_terminal" => {
                    Ok(StopRequestOutcome::AlreadyTerminal {
                        status: parse_agent_status(
                            response.terminal_status.as_deref().unwrap_or("unknown"),
                        )?,
                    })
                }
                Ok(response) if response.status == "unsupported" => {
                    Ok(StopRequestOutcome::Unsupported)
                }
                Ok(_) => Err(protocol_error("Agent stop response has an unknown status")),
                Err(error) if submission_may_be_uncertain(&error) => {
                    Ok(StopRequestOutcome::Uncertain)
                }
                Err(error) => Err(error),
            }
        })
    }
}

#[derive(Clone, Debug)]
pub struct HttpDevOpsToolAdapter {
    endpoint: HttpEndpointConfig,
    descriptor: ToolDescriptor,
}

impl HttpDevOpsToolAdapter {
    pub fn new(endpoint: HttpEndpointConfig) -> Self {
        Self {
            endpoint,
            descriptor: ToolDescriptor {
                tool_key: "devops.deploy".to_owned(),
                side_effect: SideEffectClass::IdempotentWrite,
                capabilities: ToolCapabilities {
                    query_outcome: true,
                    compensation: true,
                    asynchronous_result: true,
                },
            },
        }
    }
}

impl ToolAdapter for HttpDevOpsToolAdapter {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn execute<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        request: ToolRequest,
    ) -> AdapterFuture<'a, ToolCallOutcome> {
        Box::pin(async move {
            let url = self.endpoint.endpoint(&["v1", "deployments"])?;
            let input = decode_payload(&request.input)?;
            let response = send_json::<_, ToolExecuteBody>(
                &self.endpoint,
                context,
                Method::POST,
                url,
                Some(&input),
            )
            .await;
            match response {
                Ok(response) => response.into_outcome(),
                Err(error) if submission_may_be_uncertain(&error) => {
                    Ok(ToolCallOutcome::Uncertain { external_ref: None })
                }
                Err(error) => Err(error),
            }
        })
    }

    fn query_outcome<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        external_ref: &'a str,
    ) -> AdapterFuture<'a, ToolQueryOutcome> {
        Box::pin(async move {
            let url = self
                .endpoint
                .endpoint(&["v1", "deployments", external_ref])?;
            let response =
                send_json::<(), ToolQueryBody>(&self.endpoint, context, Method::GET, url, None)
                    .await?;
            response.into_outcome()
        })
    }

    fn compensate<'a>(
        &'a self,
        context: &'a AdapterCallContext,
        request: CompensationRequest,
    ) -> AdapterFuture<'a, CompensationOutcome> {
        Box::pin(async move {
            let url = self.endpoint.endpoint(&[
                "v1",
                "deployments",
                &request.external_ref,
                "rollback",
            ])?;
            let input = decode_payload(&request.input)?;
            let response = send_json::<_, CompensationBody>(
                &self.endpoint,
                context,
                Method::POST,
                url,
                Some(&input),
            )
            .await;
            match response {
                Ok(response) => response.into_outcome(),
                Err(error) if submission_may_be_uncertain(&error) => {
                    Ok(CompensationOutcome::Uncertain)
                }
                Err(error) => Err(error),
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpAdapterConfigurationError {
    pub safe_message: String,
}

impl fmt::Display for HttpAdapterConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl std::error::Error for HttpAdapterConfigurationError {}

#[derive(Serialize)]
struct AgentSubmitBody {
    instructions: String,
    input: Value,
    budget: AgentBudgetBody,
}

#[derive(Serialize)]
struct AgentBudgetBody {
    max_duration_micros: u64,
    max_output_bytes: u64,
}

#[derive(Deserialize)]
struct AgentRefBody {
    run_id: String,
    session_id: Option<String>,
    protocol_version: String,
}

impl AgentRefBody {
    fn into_remote(self) -> Result<RemoteAgentRef, AdapterError> {
        if self.run_id.is_empty() || self.protocol_version != HTTP_PROTOCOL_VERSION {
            return Err(protocol_error(
                "Agent response has an invalid identity or protocol version",
            ));
        }
        Ok(RemoteAgentRef {
            remote_run_id: self.run_id,
            remote_session_id: self.session_id,
            protocol_version: self.protocol_version,
        })
    }
}

#[derive(Deserialize)]
struct AgentStatusBody {
    #[serde(flatten)]
    remote: AgentRefBody,
    status: String,
    result: Option<Value>,
}

#[derive(Deserialize)]
struct AgentEventBatchBody {
    events: Vec<AgentEventBody>,
    next_cursor: Option<String>,
    terminal: bool,
}

#[derive(Deserialize)]
struct AgentEventBody {
    id: Option<String>,
    sequence: Option<u64>,
    kind: String,
    #[serde(default)]
    authoritative: bool,
    payload: Value,
}

#[derive(Serialize)]
struct StopRequestBody<'a> {
    reason: &'a str,
}

#[derive(Deserialize)]
struct StopBody {
    status: String,
    cooperative: Option<bool>,
    terminal_status: Option<String>,
}

#[derive(Deserialize)]
struct ToolExecuteBody {
    status: String,
    external_ref: Option<String>,
    result: Option<Value>,
}

impl ToolExecuteBody {
    fn into_outcome(self) -> Result<ToolCallOutcome, AdapterError> {
        match self.status.as_str() {
            "completed" => {
                let result = self
                    .result
                    .ok_or_else(|| protocol_error("Completed deployment omitted its result"))?;
                Ok(ToolCallOutcome::Completed(encode_payload(&result)?))
            }
            "accepted" => Ok(ToolCallOutcome::Accepted {
                external_ref: nonempty_ref(self.external_ref)?,
            }),
            "uncertain" => Ok(ToolCallOutcome::Uncertain {
                external_ref: self.external_ref.filter(|value| !value.is_empty()),
            }),
            _ => Err(protocol_error("Deployment response has an unknown status")),
        }
    }
}

#[derive(Deserialize)]
struct ToolQueryBody {
    status: String,
    result: Option<Value>,
    error_code: Option<String>,
    healthy: Option<bool>,
}

impl ToolQueryBody {
    fn into_outcome(self) -> Result<ToolQueryOutcome, AdapterError> {
        match self.status.as_str() {
            "pending" => Ok(ToolQueryOutcome::Pending),
            "completed" if self.healthy == Some(true) => {
                let result = self.result.ok_or_else(|| {
                    protocol_error("Healthy deployment omitted its release result")
                })?;
                Ok(ToolQueryOutcome::Completed(encode_payload(&result)?))
            }
            "completed" => Ok(ToolQueryOutcome::Failed {
                code: "DEPLOYMENT_UNHEALTHY".to_owned(),
            }),
            "failed" => Ok(ToolQueryOutcome::Failed {
                code: self
                    .error_code
                    .unwrap_or_else(|| "DEPLOYMENT_FAILED".to_owned()),
            }),
            "unknown" => Ok(ToolQueryOutcome::Unknown),
            _ => Err(protocol_error(
                "Deployment query response has an unknown status",
            )),
        }
    }
}

#[derive(Deserialize)]
struct CompensationBody {
    status: String,
    external_ref: Option<String>,
    result: Option<Value>,
}

impl CompensationBody {
    fn into_outcome(self) -> Result<CompensationOutcome, AdapterError> {
        match self.status.as_str() {
            "completed" => {
                let result = self
                    .result
                    .ok_or_else(|| protocol_error("Rollback omitted its result"))?;
                Ok(CompensationOutcome::Completed(encode_payload(&result)?))
            }
            "accepted" => Ok(CompensationOutcome::Accepted {
                external_ref: nonempty_ref(self.external_ref)?,
            }),
            "uncertain" => Ok(CompensationOutcome::Uncertain),
            _ => Err(protocol_error("Rollback response has an unknown status")),
        }
    }
}

async fn send_json<B: Serialize + ?Sized, R: DeserializeOwned>(
    endpoint: &HttpEndpointConfig,
    context: &AdapterCallContext,
    method: Method,
    url: Url,
    body: Option<&B>,
) -> Result<R, AdapterError> {
    let timeout = remaining_timeout(context)?;
    let mut request = endpoint
        .client
        .request(method, url)
        .timeout(timeout)
        .headers(context_headers(context)?);
    if let Some(body) = body {
        request = request.json(body);
    }
    request = apply_auth(request, context)?;
    let response = request
        .send()
        .await
        .map_err(|error| transport_error(&error))?;
    let status = response.status();
    let remote_request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(|seconds| DurationMicros::new(seconds.saturating_mul(1_000_000)));
    let bytes = response
        .bytes()
        .await
        .map_err(|error| transport_error(&error))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > endpoint.max_response_bytes {
        return Err(AdapterError {
            code: "PAYLOAD_TOO_LARGE",
            retry: AdapterRetryClass::Never,
            safe_message: "Remote response exceeded the configured size limit".to_owned(),
            remote_request_id,
            retry_after: None,
        });
    }
    if !status.is_success() {
        return Err(status_error(status, remote_request_id, retry_after));
    }
    serde_json::from_slice(&bytes).map_err(|_| AdapterError {
        code: "INVALID_REMOTE_PAYLOAD",
        retry: AdapterRetryClass::Never,
        safe_message: "Remote service returned an invalid protocol payload".to_owned(),
        remote_request_id,
        retry_after: None,
    })
}

fn context_headers(context: &AdapterCallContext) -> Result<HeaderMap, AdapterError> {
    let mut headers = HeaderMap::new();
    insert_header(
        &mut headers,
        "idempotency-key",
        context.idempotency_key.as_str(),
    )?;
    insert_header(
        &mut headers,
        "traceparent",
        &context.trace_context.trace_parent,
    )?;
    if let Some(trace_state) = &context.trace_context.trace_state {
        insert_header(&mut headers, "tracestate", trace_state)?;
    }
    insert_header(
        &mut headers,
        "x-agent-loom-correlation-id",
        &context.correlation_id.to_string(),
    )?;
    insert_header(
        &mut headers,
        "x-agent-loom-execution-id",
        &format!("{:?}", context.execution_id),
    )?;
    insert_header(
        &mut headers,
        "x-agent-loom-request-digest",
        &hex(context.request_hash.as_bytes()),
    )?;
    Ok(headers)
}

fn insert_header(
    headers: &mut HeaderMap,
    name: &'static str,
    value: &str,
) -> Result<(), AdapterError> {
    let value = HeaderValue::from_str(value)
        .map_err(|_| protocol_error("Adapter context contains an invalid HTTP header value"))?;
    headers.insert(name, value);
    Ok(())
}

fn apply_auth(
    request: reqwest::RequestBuilder,
    context: &AdapterCallContext,
) -> Result<reqwest::RequestBuilder, AdapterError> {
    match context.auth.scheme() {
        "bearer" => Ok(request.bearer_auth(context.auth.expose_secret())),
        "api-key" => {
            let value = HeaderValue::from_str(context.auth.expose_secret())
                .map_err(|_| protocol_error("Resolved API key is not a valid header value"))?;
            Ok(request.header("x-api-key", value))
        }
        _ => Err(AdapterError {
            code: "AUTHENTICATION_FAILED",
            retry: AdapterRetryClass::ManualReview,
            safe_message: "Resolved credential scheme is not supported by the HTTP profile"
                .to_owned(),
            remote_request_id: None,
            retry_after: None,
        }),
    }
}

fn remaining_timeout(context: &AdapterCallContext) -> Result<Duration, AdapterError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| protocol_error("System clock is before the Unix epoch"))?;
    let now_micros = i64::try_from(now.as_micros()).unwrap_or(i64::MAX);
    let remaining = context.deadline.get().saturating_sub(now_micros);
    let micros = u64::try_from(remaining).map_err(|_| AdapterError {
        code: "REMOTE_TIMEOUT",
        retry: AdapterRetryClass::QueryOutcome,
        safe_message: "Adapter call deadline has expired".to_owned(),
        remote_request_id: None,
        retry_after: None,
    })?;
    if micros == 0 {
        return Err(AdapterError {
            code: "REMOTE_TIMEOUT",
            retry: AdapterRetryClass::QueryOutcome,
            safe_message: "Adapter call deadline has expired".to_owned(),
            remote_request_id: None,
            retry_after: None,
        });
    }
    Ok(Duration::from_micros(micros))
}

fn validate_base_url(url: &Url) -> Result<(), HttpAdapterConfigurationError> {
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(configuration(
            "Adapter base URL must not contain credentials, query parameters, or fragments",
        ));
    }
    match url.scheme() {
        "https" => Ok(()),
        "http" if loopback_host(url) => Ok(()),
        _ => Err(configuration(
            "Adapter base URL must use HTTPS; HTTP is allowed only for loopback development",
        )),
    }
}

fn loopback_host(url: &Url) -> bool {
    url.host_str().is_some_and(|value| {
        value.eq_ignore_ascii_case("localhost")
            || value
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    })
}

fn parse_agent_status(value: &str) -> Result<RemoteAgentStatus, AdapterError> {
    match value {
        "accepted" => Ok(RemoteAgentStatus::Accepted),
        "running" => Ok(RemoteAgentStatus::Running),
        "waiting_for_approval" => Ok(RemoteAgentStatus::WaitingForApproval),
        "waiting_for_input" => Ok(RemoteAgentStatus::WaitingForInput),
        "stopping" => Ok(RemoteAgentStatus::Stopping),
        "completed" => Ok(RemoteAgentStatus::Completed),
        "failed" => Ok(RemoteAgentStatus::Failed),
        "cancelled" => Ok(RemoteAgentStatus::Cancelled),
        "unknown" => Ok(RemoteAgentStatus::Unknown),
        _ => Err(protocol_error("Agent response has an unknown status")),
    }
}

fn status_error(
    status: StatusCode,
    remote_request_id: Option<String>,
    retry_after: Option<DurationMicros>,
) -> AdapterError {
    let (code, retry, message) = match status {
        StatusCode::UNAUTHORIZED => (
            "AUTHENTICATION_FAILED",
            AdapterRetryClass::ManualReview,
            "Remote service rejected the credential",
        ),
        StatusCode::FORBIDDEN => (
            "AUTHORIZATION_FAILED",
            AdapterRetryClass::ManualReview,
            "Remote service denied the requested operation",
        ),
        StatusCode::NOT_FOUND => (
            "REMOTE_RUN_NOT_FOUND",
            AdapterRetryClass::Never,
            "Remote operation was not found",
        ),
        StatusCode::TOO_MANY_REQUESTS => (
            "RATE_LIMITED",
            AdapterRetryClass::SameRequestBackoff,
            "Remote service rate limited the request",
        ),
        status if status.is_server_error() => (
            "ENDPOINT_UNAVAILABLE",
            AdapterRetryClass::SameRequestBackoff,
            "Remote service is temporarily unavailable",
        ),
        _ => (
            "REMOTE_REJECTED",
            AdapterRetryClass::Never,
            "Remote service rejected the request",
        ),
    };
    AdapterError {
        code,
        retry,
        safe_message: message.to_owned(),
        remote_request_id,
        retry_after,
    }
}

fn transport_error(error: &reqwest::Error) -> AdapterError {
    AdapterError {
        code: if error.is_timeout() {
            "REMOTE_TIMEOUT"
        } else {
            "ENDPOINT_UNAVAILABLE"
        },
        retry: AdapterRetryClass::QueryOutcome,
        safe_message: if error.is_timeout() {
            "Remote service did not respond before the call deadline"
        } else {
            "Remote service could not be reached"
        }
        .to_owned(),
        remote_request_id: None,
        retry_after: None,
    }
}

fn submission_may_be_uncertain(error: &AdapterError) -> bool {
    matches!(error.code, "REMOTE_TIMEOUT" | "ENDPOINT_UNAVAILABLE")
}

fn protocol_error(message: &str) -> AdapterError {
    AdapterError {
        code: "INVALID_REMOTE_PAYLOAD",
        retry: AdapterRetryClass::Never,
        safe_message: message.to_owned(),
        remote_request_id: None,
        retry_after: None,
    }
}

fn decode_payload(payload: &JsonPayload) -> Result<Value, AdapterError> {
    serde_json::from_slice(payload.as_bytes())
        .map_err(|_| protocol_error("Persisted request payload is not valid JSON"))
}

fn encode_payload(value: &Value) -> Result<JsonPayload, AdapterError> {
    serde_json::to_vec(value)
        .map(JsonPayload::from_validated_bytes)
        .map_err(|_| protocol_error("Remote payload could not be normalized"))
}

fn nonempty_ref(value: Option<String>) -> Result<String, AdapterError> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| protocol_error("Remote operation omitted its stable reference"))
}

fn digest(bytes: &[u8]) -> Digest {
    Digest::from_bytes(Sha256::digest(bytes).into())
}

fn hex(bytes: &[u8]) -> String {
    use fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn configuration(message: &str) -> HttpAdapterConfigurationError {
    HttpAdapterConfigurationError {
        safe_message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests;
