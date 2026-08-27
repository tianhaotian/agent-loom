//! Reusable black-box conformance checks for production Adapter implementations.

use std::{error::Error, fmt};

use crate::{
    AdapterCallContext, AgentRunRequest, AgentServerAdapter, CompensationOutcome,
    CompensationRequest, EventReadLimits, RemoteAgentRef, RemoteAgentStatus, SideEffectClass,
    StopRequestOutcome, SubmitAgentOutcome, ToolAdapter, ToolCallOutcome, ToolQueryOutcome,
    ToolRequest,
};

#[derive(Clone, Debug)]
pub struct AgentConformanceFixture {
    pub context: AdapterCallContext,
    pub request: AgentRunRequest,
    pub event_limits: EventReadLimits,
}

#[derive(Clone, Debug)]
pub struct ToolConformanceFixture {
    pub context: AdapterCallContext,
    pub request: ToolRequest,
    pub compensation_input: agent_loom_domain::JsonPayload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentConformanceReport {
    pub remote: RemoteAgentRef,
    pub event_count: usize,
    pub terminal_status: RemoteAgentStatus,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolConformanceReport {
    pub external_ref: String,
    pub queried_terminal: bool,
    pub compensation_verified: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdapterConformanceError {
    pub case: &'static str,
    pub detail: String,
}

impl fmt::Display for AdapterConformanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.case, self.detail)
    }
}

impl Error for AdapterConformanceError {}

/// Exercises idempotent submission, reconciliation, status, cursor replay and stop semantics.
///
/// # Errors
///
/// Returns the first Adapter error or contract mismatch observed by the suite.
pub async fn exercise_agent_server(
    adapter: &dyn AgentServerAdapter,
    fixture: AgentConformanceFixture,
) -> Result<AgentConformanceReport, AdapterConformanceError> {
    const CASE: &str = "agent-server";
    let capabilities = adapter.capabilities();
    require(
        !adapter.kind().is_empty()
            && capabilities.submission_idempotency
            && capabilities.submission_reconciliation
            && capabilities.status_query
            && capabilities.resumable_events
            && capabilities.cooperative_stop,
        CASE,
        "production profile does not declare the Phase 2A capability baseline",
    )?;

    let first = adapter
        .submit(&fixture.context, fixture.request.clone())
        .await
        .map_err(|error| adapter_failure(CASE, "submit", &error))?;
    let SubmitAgentOutcome::Accepted(remote) = first else {
        return Err(mismatch(CASE, "controlled submit did not return Accepted"));
    };
    require(
        !remote.remote_run_id.is_empty() && !remote.protocol_version.is_empty(),
        CASE,
        "accepted submission omitted its stable remote identity",
    )?;

    let duplicate = adapter
        .submit(&fixture.context, fixture.request)
        .await
        .map_err(|error| adapter_failure(CASE, "duplicate submit", &error))?;
    require(
        duplicate == SubmitAgentOutcome::Accepted(remote.clone()),
        CASE,
        "same idempotency key did not return the same remote Run",
    )?;

    let reconciled = adapter
        .reconcile_submission(&fixture.context)
        .await
        .map_err(|error| adapter_failure(CASE, "reconcile", &error))?;
    require(
        reconciled.as_ref() == Some(&remote),
        CASE,
        "submission reconciliation did not recover the accepted remote Run",
    )?;

    let snapshot = adapter
        .get_status(&fixture.context, &remote)
        .await
        .map_err(|error| adapter_failure(CASE, "status", &error))?;
    require(
        snapshot.remote == remote && snapshot.status == RemoteAgentStatus::Completed,
        CASE,
        "status query did not preserve identity or report the controlled terminal state",
    )?;

    let first_batch = adapter
        .read_events(&fixture.context, &remote, None, fixture.event_limits)
        .await
        .map_err(|error| adapter_failure(CASE, "event read", &error))?;
    require(
        !first_batch.events.is_empty() && first_batch.terminal,
        CASE,
        "event read omitted the authoritative terminal batch",
    )?;
    let replay = adapter
        .read_events(&fixture.context, &remote, None, fixture.event_limits)
        .await
        .map_err(|error| adapter_failure(CASE, "event replay", &error))?;
    require(
        replay == first_batch,
        CASE,
        "replaying the same cursor changed the remote event batch",
    )?;

    let stop = adapter
        .request_stop(&fixture.context, &remote, "conformance terminal race")
        .await
        .map_err(|error| adapter_failure(CASE, "stop", &error))?;
    require(
        matches!(
            stop,
            StopRequestOutcome::AlreadyTerminal {
                status: RemoteAgentStatus::Completed
            }
        ),
        CASE,
        "stop/complete race did not preserve the actual remote terminal state",
    )?;

    Ok(AgentConformanceReport {
        remote,
        event_count: first_batch.events.len(),
        terminal_status: snapshot.status,
    })
}

/// Exercises idempotent deployment, asynchronous outcome query and compensation behavior.
///
/// # Errors
///
/// Returns the first Adapter error or contract mismatch observed by the suite.
pub async fn exercise_devops_tool(
    adapter: &dyn ToolAdapter,
    fixture: ToolConformanceFixture,
) -> Result<ToolConformanceReport, AdapterConformanceError> {
    const CASE: &str = "devops-tool";
    let descriptor = adapter.descriptor();
    require(
        descriptor.tool_key == "devops.deploy"
            && descriptor.side_effect == SideEffectClass::IdempotentWrite
            && descriptor.capabilities.query_outcome
            && descriptor.capabilities.compensation
            && descriptor.capabilities.asynchronous_result,
        CASE,
        "DevOps descriptor does not declare idempotent deploy/query/rollback semantics",
    )?;

    let first = adapter
        .execute(&fixture.context, fixture.request.clone())
        .await
        .map_err(|error| adapter_failure(CASE, "execute", &error))?;
    let ToolCallOutcome::Accepted { external_ref } = first else {
        return Err(mismatch(
            CASE,
            "controlled deployment did not return an asynchronous operation reference",
        ));
    };
    let duplicate = adapter
        .execute(&fixture.context, fixture.request)
        .await
        .map_err(|error| adapter_failure(CASE, "duplicate execute", &error))?;
    require(
        duplicate
            == ToolCallOutcome::Accepted {
                external_ref: external_ref.clone(),
            },
        CASE,
        "same deployment idempotency key created a different operation",
    )?;

    let query = adapter
        .query_outcome(&fixture.context, &external_ref)
        .await
        .map_err(|error| adapter_failure(CASE, "query", &error))?;
    require(
        matches!(query, ToolQueryOutcome::Completed(_)),
        CASE,
        "deployment query did not verify release health before success",
    )?;

    let compensation = adapter
        .compensate(
            &fixture.context,
            CompensationRequest {
                external_ref: external_ref.clone(),
                input: fixture.compensation_input,
            },
        )
        .await
        .map_err(|error| adapter_failure(CASE, "compensate", &error))?;
    require(
        matches!(compensation, CompensationOutcome::Completed(_)),
        CASE,
        "deployment rollback was not confirmed",
    )?;

    Ok(ToolConformanceReport {
        external_ref,
        queried_terminal: true,
        compensation_verified: true,
    })
}

fn require(
    condition: bool,
    case: &'static str,
    detail: &'static str,
) -> Result<(), AdapterConformanceError> {
    if condition {
        Ok(())
    } else {
        Err(mismatch(case, detail))
    }
}

fn mismatch(case: &'static str, detail: impl Into<String>) -> AdapterConformanceError {
    AdapterConformanceError {
        case,
        detail: detail.into(),
    }
}

fn adapter_failure(
    case: &'static str,
    operation: &'static str,
    error: &crate::AdapterError,
) -> AdapterConformanceError {
    mismatch(
        case,
        format!(
            "{operation} failed with {}: {}",
            error.code, error.safe_message
        ),
    )
}
