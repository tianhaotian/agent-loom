use std::{fmt, future::Future, pin::Pin};

use agent_loom_domain::TenantId;
use agent_loom_durable_store::{
    AgentEventCandidate, AgentEventPage, AgentEventQuery, AgentStatusCandidate, AgentStatusPage,
    AgentStatusQuery, AgentStopCandidate, AgentStopPage, AgentStopQuery, DurableStore,
    QueryContext, StoreError, StoreFuture,
};

use crate::{PollingActivity, PollingFuture, PollingJob, PollingJobError};

/// Minimal durable surface needed by the remote Agent stop worker.
pub trait AgentStopStore: Send + Sync {
    fn scan_agent_stops<'a>(
        &'a self,
        context: &'a QueryContext,
        query: AgentStopQuery,
    ) -> StoreFuture<'a, AgentStopPage>;
}

impl<T> AgentStopStore for T
where
    T: DurableStore + ?Sized,
{
    fn scan_agent_stops<'a>(
        &'a self,
        context: &'a QueryContext,
        query: AgentStopQuery,
    ) -> StoreFuture<'a, AgentStopPage> {
        DurableStore::scan_agent_stops(self, context, query)
    }
}

pub trait AgentStatusStore: Send + Sync {
    fn scan_agent_status<'a>(
        &'a self,
        context: &'a QueryContext,
        query: AgentStatusQuery,
    ) -> StoreFuture<'a, AgentStatusPage>;
}

pub trait AgentEventStore: Send + Sync {
    fn scan_agent_events<'a>(
        &'a self,
        context: &'a QueryContext,
        query: AgentEventQuery,
    ) -> StoreFuture<'a, AgentEventPage>;
}

impl<T> AgentEventStore for T
where
    T: DurableStore + ?Sized,
{
    fn scan_agent_events<'a>(
        &'a self,
        context: &'a QueryContext,
        query: AgentEventQuery,
    ) -> StoreFuture<'a, AgentEventPage> {
        DurableStore::scan_agent_events(self, context, query)
    }
}

impl<T> AgentStatusStore for T
where
    T: DurableStore + ?Sized,
{
    fn scan_agent_status<'a>(
        &'a self,
        context: &'a QueryContext,
        query: AgentStatusQuery,
    ) -> StoreFuture<'a, AgentStatusPage> {
        DurableStore::scan_agent_status(self, context, query)
    }
}

pub type AgentStopFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), AgentStopDispatchError>> + Send + 'a>>;

/// Sends one durable remote-stop request and records its outcome.
pub trait AgentStopDispatcher: Send + Sync {
    fn request_stop(&self, candidate: AgentStopCandidate) -> AgentStopFuture<'_>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentStopDispatchError {
    pub safe_message: String,
}

impl fmt::Display for AgentStopDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl std::error::Error for AgentStopDispatchError {}

pub type AgentStatusFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), AgentStatusDispatchError>> + Send + 'a>>;

pub trait AgentStatusDispatcher: Send + Sync {
    fn get_status(&self, candidate: AgentStatusCandidate) -> AgentStatusFuture<'_>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentStatusDispatchError {
    pub safe_message: String,
}

impl fmt::Display for AgentStatusDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl std::error::Error for AgentStatusDispatchError {}

pub type AgentEventFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), AgentEventDispatchError>> + Send + 'a>>;

pub trait AgentEventDispatcher: Send + Sync {
    fn read_events(&self, candidate: AgentEventCandidate) -> AgentEventFuture<'_>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentEventDispatchError {
    pub safe_message: String,
}

impl fmt::Display for AgentEventDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl std::error::Error for AgentEventDispatchError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentStopWorkerConfig {
    pub candidate_window: u32,
}

impl Default for AgentStopWorkerConfig {
    fn default() -> Self {
        Self {
            candidate_window: 16,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentStopPollOutcome {
    Idle,
    Dispatched(AgentStopCandidate),
    DispatchFailed {
        candidate: AgentStopCandidate,
        error: AgentStopDispatchError,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentStopWorkerError {
    Store(StoreError),
    InvalidConfig,
    InvalidCandidate,
}

impl fmt::Display for AgentStopWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => error.fmt(formatter),
            Self::InvalidConfig => formatter.write_str("Agent stop candidate window is invalid"),
            Self::InvalidCandidate => formatter.write_str("Agent stop candidate is malformed"),
        }
    }
}

impl std::error::Error for AgentStopWorkerError {}

#[derive(Debug)]
pub struct AgentStopWorker<S, D> {
    store: S,
    dispatcher: D,
    query_context: QueryContext,
    config: AgentStopWorkerConfig,
}

impl<S, D> AgentStopWorker<S, D>
where
    S: AgentStopStore,
    D: AgentStopDispatcher,
{
    /// Builds a tenant-scoped worker with a bounded candidate scan.
    ///
    /// # Errors
    ///
    /// Returns an error when the candidate window is zero.
    pub fn new(
        store: S,
        dispatcher: D,
        tenant_id: TenantId,
        config: AgentStopWorkerConfig,
    ) -> Result<Self, AgentStopWorkerError> {
        if config.candidate_window == 0 {
            return Err(AgentStopWorkerError::InvalidConfig);
        }
        Ok(Self {
            store,
            dispatcher,
            query_context: QueryContext {
                tenant_id,
                actor_ref: "agent-loom-agent-stop".to_owned(),
                authoritative: true,
            },
            config,
        })
    }

    /// Scans and dispatches at most one durable remote-stop candidate.
    ///
    /// # Errors
    ///
    /// Returns an error when the Store is unavailable or returns a malformed candidate.
    pub async fn poll_once(&self) -> Result<AgentStopPollOutcome, AgentStopWorkerError> {
        let page = self
            .store
            .scan_agent_stops(
                &self.query_context,
                AgentStopQuery {
                    limit: self.config.candidate_window,
                },
            )
            .await
            .map_err(AgentStopWorkerError::Store)?;
        let Some(candidate) = page.candidates.into_iter().next() else {
            return Ok(AgentStopPollOutcome::Idle);
        };
        if !candidate.shape_is_valid() {
            return Err(AgentStopWorkerError::InvalidCandidate);
        }
        match self.dispatcher.request_stop(candidate.clone()).await {
            Ok(()) => Ok(AgentStopPollOutcome::Dispatched(candidate)),
            Err(error) => Ok(AgentStopPollOutcome::DispatchFailed { candidate, error }),
        }
    }
}

#[derive(Debug)]
pub struct AgentStopPollingJob<S, D> {
    worker: AgentStopWorker<S, D>,
}

impl<S, D> AgentStopPollingJob<S, D> {
    pub const fn new(worker: AgentStopWorker<S, D>) -> Self {
        Self { worker }
    }
}

impl<S, D> PollingJob for AgentStopPollingJob<S, D>
where
    S: AgentStopStore,
    D: AgentStopDispatcher,
{
    fn run_once(&self, _slot: u32) -> PollingFuture<'_> {
        Box::pin(async move {
            match self
                .worker
                .poll_once()
                .await
                .map_err(|error| PollingJobError {
                    safe_message: error.to_string(),
                })? {
                AgentStopPollOutcome::Idle => Ok(PollingActivity::Idle),
                AgentStopPollOutcome::Dispatched(_) => Ok(PollingActivity::Progress {
                    completed: 1,
                    failed: 0,
                    last_failure: None,
                }),
                AgentStopPollOutcome::DispatchFailed { error, .. } => {
                    Ok(PollingActivity::Progress {
                        completed: 0,
                        failed: 1,
                        last_failure: Some(error.safe_message),
                    })
                }
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentStatusWorkerConfig {
    pub candidate_window: u32,
}

impl Default for AgentStatusWorkerConfig {
    fn default() -> Self {
        Self {
            candidate_window: 16,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentStatusPollOutcome {
    Idle,
    Dispatched(AgentStatusCandidate),
    DispatchFailed {
        candidate: AgentStatusCandidate,
        error: AgentStatusDispatchError,
    },
}

#[derive(Debug)]
pub struct AgentStatusWorker<S, D> {
    store: S,
    dispatcher: D,
    query_context: QueryContext,
    config: AgentStatusWorkerConfig,
}

impl<S, D> AgentStatusWorker<S, D>
where
    S: AgentStatusStore,
    D: AgentStatusDispatcher,
{
    /// Builds a tenant-scoped status worker.
    ///
    /// # Errors
    ///
    /// Returns an error when the candidate window is zero.
    pub fn new(
        store: S,
        dispatcher: D,
        tenant_id: TenantId,
        config: AgentStatusWorkerConfig,
    ) -> Result<Self, AgentStopWorkerError> {
        if config.candidate_window == 0 {
            return Err(AgentStopWorkerError::InvalidConfig);
        }
        Ok(Self {
            store,
            dispatcher,
            query_context: QueryContext {
                tenant_id,
                actor_ref: "agent-loom-agent-status".to_owned(),
                authoritative: true,
            },
            config,
        })
    }

    /// Scans and dispatches at most one due remote status query.
    ///
    /// # Errors
    ///
    /// Returns an error when the Store is unavailable or returns a malformed candidate.
    pub async fn poll_once(&self) -> Result<AgentStatusPollOutcome, AgentStopWorkerError> {
        let page = self
            .store
            .scan_agent_status(
                &self.query_context,
                AgentStatusQuery {
                    limit: self.config.candidate_window,
                },
            )
            .await
            .map_err(AgentStopWorkerError::Store)?;
        let Some(candidate) = page.candidates.into_iter().next() else {
            return Ok(AgentStatusPollOutcome::Idle);
        };
        if !candidate.shape_is_valid() {
            return Err(AgentStopWorkerError::InvalidCandidate);
        }
        match self.dispatcher.get_status(candidate.clone()).await {
            Ok(()) => Ok(AgentStatusPollOutcome::Dispatched(candidate)),
            Err(error) => Ok(AgentStatusPollOutcome::DispatchFailed { candidate, error }),
        }
    }
}

#[derive(Debug)]
pub struct AgentStatusPollingJob<S, D> {
    worker: AgentStatusWorker<S, D>,
}

impl<S, D> AgentStatusPollingJob<S, D> {
    pub const fn new(worker: AgentStatusWorker<S, D>) -> Self {
        Self { worker }
    }
}

impl<S, D> PollingJob for AgentStatusPollingJob<S, D>
where
    S: AgentStatusStore,
    D: AgentStatusDispatcher,
{
    fn run_once(&self, _slot: u32) -> PollingFuture<'_> {
        Box::pin(async move {
            match self
                .worker
                .poll_once()
                .await
                .map_err(|error| PollingJobError {
                    safe_message: error.to_string(),
                })? {
                AgentStatusPollOutcome::Idle => Ok(PollingActivity::Idle),
                AgentStatusPollOutcome::Dispatched(_) => Ok(PollingActivity::Progress {
                    completed: 1,
                    failed: 0,
                    last_failure: None,
                }),
                AgentStatusPollOutcome::DispatchFailed { error, .. } => {
                    Ok(PollingActivity::Progress {
                        completed: 0,
                        failed: 1,
                        last_failure: Some(error.safe_message),
                    })
                }
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentEventWorkerConfig {
    pub candidate_window: u32,
}

impl Default for AgentEventWorkerConfig {
    fn default() -> Self {
        Self {
            candidate_window: 16,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentEventPollOutcome {
    Idle,
    Dispatched(AgentEventCandidate),
    DispatchFailed {
        candidate: AgentEventCandidate,
        error: AgentEventDispatchError,
    },
}

#[derive(Debug)]
pub struct AgentEventWorker<S, D> {
    store: S,
    dispatcher: D,
    query_context: QueryContext,
    config: AgentEventWorkerConfig,
}

impl<S, D> AgentEventWorker<S, D>
where
    S: AgentEventStore,
    D: AgentEventDispatcher,
{
    /// Builds a tenant-scoped resumable event worker.
    ///
    /// # Errors
    ///
    /// Returns an error when the candidate window is zero.
    pub fn new(
        store: S,
        dispatcher: D,
        tenant_id: TenantId,
        config: AgentEventWorkerConfig,
    ) -> Result<Self, AgentStopWorkerError> {
        if config.candidate_window == 0 {
            return Err(AgentStopWorkerError::InvalidConfig);
        }
        Ok(Self {
            store,
            dispatcher,
            query_context: QueryContext {
                tenant_id,
                actor_ref: "agent-loom-agent-events".to_owned(),
                authoritative: true,
            },
            config,
        })
    }

    /// Scans and dispatches at most one due remote event read.
    ///
    /// # Errors
    ///
    /// Returns an error when the Store is unavailable or returns a malformed candidate.
    pub async fn poll_once(&self) -> Result<AgentEventPollOutcome, AgentStopWorkerError> {
        let page = self
            .store
            .scan_agent_events(
                &self.query_context,
                AgentEventQuery {
                    limit: self.config.candidate_window,
                },
            )
            .await
            .map_err(AgentStopWorkerError::Store)?;
        let Some(candidate) = page.candidates.into_iter().next() else {
            return Ok(AgentEventPollOutcome::Idle);
        };
        if !candidate.shape_is_valid() {
            return Err(AgentStopWorkerError::InvalidCandidate);
        }
        match self.dispatcher.read_events(candidate.clone()).await {
            Ok(()) => Ok(AgentEventPollOutcome::Dispatched(candidate)),
            Err(error) => Ok(AgentEventPollOutcome::DispatchFailed { candidate, error }),
        }
    }
}

#[derive(Debug)]
pub struct AgentEventPollingJob<S, D> {
    worker: AgentEventWorker<S, D>,
}

impl<S, D> AgentEventPollingJob<S, D> {
    pub const fn new(worker: AgentEventWorker<S, D>) -> Self {
        Self { worker }
    }
}

impl<S, D> PollingJob for AgentEventPollingJob<S, D>
where
    S: AgentEventStore,
    D: AgentEventDispatcher,
{
    fn run_once(&self, _slot: u32) -> PollingFuture<'_> {
        Box::pin(async move {
            match self
                .worker
                .poll_once()
                .await
                .map_err(|error| PollingJobError {
                    safe_message: error.to_string(),
                })? {
                AgentEventPollOutcome::Idle => Ok(PollingActivity::Idle),
                AgentEventPollOutcome::Dispatched(_) => Ok(PollingActivity::Progress {
                    completed: 1,
                    failed: 0,
                    last_failure: None,
                }),
                AgentEventPollOutcome::DispatchFailed { error, .. } => {
                    Ok(PollingActivity::Progress {
                        completed: 0,
                        failed: 1,
                        last_failure: Some(error.safe_message),
                    })
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use agent_loom_domain::{
        AgentExecutionId, AgentExecutionSnapshot, AgentExecutionStatus, AgentVersionId, EndpointId,
        RunId, TaskId, UnixMicros,
    };
    use agent_loom_durable_store::{ExpectedRun, StoreResult};

    use super::*;

    #[derive(Clone)]
    struct FakeStore(Option<AgentStopCandidate>);

    impl AgentStopStore for FakeStore {
        fn scan_agent_stops<'a>(
            &'a self,
            _context: &'a QueryContext,
            _query: AgentStopQuery,
        ) -> StoreFuture<'a, AgentStopPage> {
            let candidates = self.0.clone().into_iter().collect();
            Box::pin(async move { StoreResult::Ok(AgentStopPage { candidates }) })
        }
    }

    #[derive(Clone, Default)]
    struct FakeDispatcher(Arc<Mutex<Vec<AgentStopCandidate>>>);

    impl AgentStopDispatcher for FakeDispatcher {
        fn request_stop(&self, candidate: AgentStopCandidate) -> AgentStopFuture<'_> {
            let calls = Arc::clone(&self.0);
            Box::pin(async move {
                calls.lock().expect("calls lock").push(candidate);
                Ok(())
            })
        }
    }

    #[derive(Clone)]
    struct FakeStatusStore(Option<AgentStatusCandidate>);

    impl AgentStatusStore for FakeStatusStore {
        fn scan_agent_status<'a>(
            &'a self,
            _context: &'a QueryContext,
            _query: AgentStatusQuery,
        ) -> StoreFuture<'a, AgentStatusPage> {
            let candidates = self.0.clone().into_iter().collect();
            Box::pin(async move { StoreResult::Ok(AgentStatusPage { candidates }) })
        }
    }

    #[derive(Clone, Default)]
    struct FakeStatusDispatcher(Arc<Mutex<Vec<AgentStatusCandidate>>>);

    impl AgentStatusDispatcher for FakeStatusDispatcher {
        fn get_status(&self, candidate: AgentStatusCandidate) -> AgentStatusFuture<'_> {
            let calls = Arc::clone(&self.0);
            Box::pin(async move {
                calls.lock().expect("status calls lock").push(candidate);
                Ok(())
            })
        }
    }

    fn candidate() -> AgentStopCandidate {
        let tenant_id = TenantId::from_bytes([1; 16]);
        let run_id = RunId::from_bytes([2; 16]);
        AgentStopCandidate {
            tenant_id,
            execution: AgentExecutionSnapshot {
                tenant_id,
                agent_execution_id: AgentExecutionId::from_bytes([3; 16]),
                run_id,
                stage_execution_id: None,
                task_id: TaskId::from_bytes([4; 16]),
                endpoint_id: EndpointId::from_bytes([5; 16]),
                agent_version_id: AgentVersionId::from_bytes([6; 16]),
                status: AgentExecutionStatus::Stopping,
                version: 2,
                remote_run_ref: Some("remote-run".to_owned()),
                remote_session_ref: Some("session".to_owned()),
                remote_protocol_version: Some("1".to_owned()),
                status_poll_at: None,
                event_cursor: None,
                cursor_version: 0,
                retry_at: None,
                updated_at: UnixMicros::new(1),
            },
            expected_run: ExpectedRun {
                run_id,
                version: Some(2),
                execution_generation: Some(1),
            },
        }
    }

    fn status_candidate() -> AgentStatusCandidate {
        let stop = candidate();
        let mut execution = stop.execution;
        execution.status = AgentExecutionStatus::Reconciling;
        execution.status_poll_at = Some(UnixMicros::new(20));
        AgentStatusCandidate {
            tenant_id: stop.tenant_id,
            execution,
            expected_run: stop.expected_run,
        }
    }

    #[tokio::test]
    async fn worker_dispatches_a_durable_stopping_execution() {
        let dispatcher = FakeDispatcher::default();
        let calls = Arc::clone(&dispatcher.0);
        let worker = AgentStopWorker::new(
            FakeStore(Some(candidate())),
            dispatcher,
            TenantId::from_bytes([1; 16]),
            AgentStopWorkerConfig::default(),
        )
        .expect("valid worker");
        assert!(matches!(
            worker.poll_once().await.expect("poll"),
            AgentStopPollOutcome::Dispatched(_)
        ));
        assert_eq!(calls.lock().expect("calls lock").len(), 1);
    }

    #[tokio::test]
    async fn worker_is_idle_without_durable_stop_work() {
        let worker = AgentStopWorker::new(
            FakeStore(None),
            FakeDispatcher::default(),
            TenantId::from_bytes([1; 16]),
            AgentStopWorkerConfig::default(),
        )
        .expect("valid worker");
        assert_eq!(
            worker.poll_once().await.expect("poll"),
            AgentStopPollOutcome::Idle
        );
    }

    #[tokio::test]
    async fn status_worker_dispatches_a_due_reconciliation() {
        let dispatcher = FakeStatusDispatcher::default();
        let calls = Arc::clone(&dispatcher.0);
        let worker = AgentStatusWorker::new(
            FakeStatusStore(Some(status_candidate())),
            dispatcher,
            TenantId::from_bytes([1; 16]),
            AgentStatusWorkerConfig::default(),
        )
        .expect("valid status worker");
        assert!(matches!(
            worker.poll_once().await.expect("status poll"),
            AgentStatusPollOutcome::Dispatched(_)
        ));
        assert_eq!(calls.lock().expect("status calls lock").len(), 1);
    }
}
