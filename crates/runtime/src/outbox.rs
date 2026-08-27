use std::{fmt, future::Future, pin::Pin};

use agent_loom_domain::{DurationMicros, LeaseToken, OutboxMessage, TenantId, WorkerId};
use agent_loom_durable_store::{
    ClaimOutbox, DurableStore, OutboxDeliveryOutcome, QueryContext, RecordOutboxDelivery,
    StoreError, StoreFuture,
};

use crate::{PollingActivity, PollingFuture, PollingJob, PollingJobError};

pub trait OutboxStore: Send + Sync {
    fn claim_outbox<'a>(
        &'a self,
        context: &'a QueryContext,
        command: ClaimOutbox,
    ) -> StoreFuture<'a, Option<OutboxMessage>>;

    fn record_outbox_delivery<'a>(
        &'a self,
        context: &'a QueryContext,
        command: RecordOutboxDelivery,
    ) -> StoreFuture<'a, ()>;
}

impl<T> OutboxStore for T
where
    T: DurableStore + ?Sized,
{
    fn claim_outbox<'a>(
        &'a self,
        context: &'a QueryContext,
        command: ClaimOutbox,
    ) -> StoreFuture<'a, Option<OutboxMessage>> {
        DurableStore::claim_outbox(self, context, command)
    }

    fn record_outbox_delivery<'a>(
        &'a self,
        context: &'a QueryContext,
        command: RecordOutboxDelivery,
    ) -> StoreFuture<'a, ()> {
        DurableStore::record_outbox_delivery(self, context, command)
    }
}

pub type OutboxPublishFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), OutboxPublishError>> + Send + 'a>>;

pub trait OutboxPublisher: Send + Sync {
    fn publish(&self, message: &OutboxMessage) -> OutboxPublishFuture<'_>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxPublishError {
    pub code: String,
    pub safe_message: String,
}

impl fmt::Display for OutboxPublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl std::error::Error for OutboxPublishError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutboxWorkerConfig {
    pub lease_duration: DurationMicros,
}

impl Default for OutboxWorkerConfig {
    fn default() -> Self {
        Self {
            lease_duration: DurationMicros::new(30_000_000),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboxPollOutcome {
    Idle,
    Published(OutboxMessage),
    RetryScheduled {
        message: OutboxMessage,
        error: OutboxPublishError,
    },
}

#[derive(Debug)]
pub enum OutboxWorkerError {
    Store(StoreError),
    InvalidConfig,
}

impl fmt::Display for OutboxWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => error.fmt(formatter),
            Self::InvalidConfig => formatter.write_str("Outbox lease duration is invalid"),
        }
    }
}

impl std::error::Error for OutboxWorkerError {}

#[derive(Debug)]
pub struct OutboxWorker<S, P> {
    store: S,
    publisher: P,
    query_context: QueryContext,
    publisher_id: WorkerId,
    lease_token: LeaseToken,
    config: OutboxWorkerConfig,
}

impl<S, P> OutboxWorker<S, P>
where
    S: OutboxStore,
    P: OutboxPublisher,
{
    /// Builds one tenant-scoped Outbox publisher.
    ///
    /// # Errors
    ///
    /// Returns an error for a nil publisher identity or zero lease duration.
    pub fn new(
        store: S,
        publisher: P,
        tenant_id: TenantId,
        publisher_id: WorkerId,
        lease_token: LeaseToken,
        config: OutboxWorkerConfig,
    ) -> Result<Self, OutboxWorkerError> {
        if publisher_id.is_nil() || config.lease_duration.get() == 0 {
            return Err(OutboxWorkerError::InvalidConfig);
        }
        Ok(Self {
            store,
            publisher,
            query_context: QueryContext {
                tenant_id,
                actor_ref: "agent-loom-outbox-publisher".to_owned(),
                authoritative: true,
            },
            publisher_id,
            lease_token,
            config,
        })
    }

    /// Publishes and fences at most one due message.
    ///
    /// A crash after the external publish and before the acknowledgement causes
    /// an at-least-once replay after lease expiry; consumers dedupe by event ID.
    ///
    /// # Errors
    ///
    /// Returns a Store error for claim or acknowledgement failure.
    pub async fn poll_once(&self) -> Result<OutboxPollOutcome, OutboxWorkerError> {
        let message = self
            .store
            .claim_outbox(
                &self.query_context,
                ClaimOutbox {
                    publisher_id: self.publisher_id,
                    lease_token: self.lease_token.clone(),
                    lease_duration: self.config.lease_duration,
                },
            )
            .await
            .map_err(OutboxWorkerError::Store)?;
        let Some(message) = message else {
            return Ok(OutboxPollOutcome::Idle);
        };
        let publish = self.publisher.publish(&message).await;
        let outcome = match &publish {
            Ok(()) => OutboxDeliveryOutcome::Published,
            Err(error) => OutboxDeliveryOutcome::Retry {
                available_at: message.lease_expires_at,
                error_code: error.code.clone(),
            },
        };
        self.store
            .record_outbox_delivery(
                &self.query_context,
                RecordOutboxDelivery {
                    outbox_id: message.outbox_id,
                    expected_attempt: message.attempt,
                    publisher_id: self.publisher_id,
                    lease_token: self.lease_token.clone(),
                    outcome,
                },
            )
            .await
            .map_err(OutboxWorkerError::Store)?;
        Ok(match publish {
            Ok(()) => OutboxPollOutcome::Published(message),
            Err(error) => OutboxPollOutcome::RetryScheduled { message, error },
        })
    }
}

#[derive(Debug)]
pub struct OutboxPollingJob<S, P> {
    worker: OutboxWorker<S, P>,
}

impl<S, P> OutboxPollingJob<S, P> {
    pub const fn new(worker: OutboxWorker<S, P>) -> Self {
        Self { worker }
    }
}

impl<S, P> PollingJob for OutboxPollingJob<S, P>
where
    S: OutboxStore,
    P: OutboxPublisher,
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
                OutboxPollOutcome::Idle => Ok(PollingActivity::Idle),
                OutboxPollOutcome::Published(_) => Ok(PollingActivity::Progress {
                    completed: 1,
                    failed: 0,
                    last_failure: None,
                }),
                OutboxPollOutcome::RetryScheduled { error, .. } => Ok(PollingActivity::Progress {
                    completed: 0,
                    failed: 1,
                    last_failure: Some(error.safe_message),
                }),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use agent_loom_domain::{EventId, JsonPayload, OutboxId, RunId, UnixMicros};

    use super::*;

    #[derive(Debug)]
    struct FakeStore {
        message: Option<OutboxMessage>,
        recorded: Arc<Mutex<Option<RecordOutboxDelivery>>>,
    }

    impl OutboxStore for FakeStore {
        fn claim_outbox<'a>(
            &'a self,
            _context: &'a QueryContext,
            _command: ClaimOutbox,
        ) -> StoreFuture<'a, Option<OutboxMessage>> {
            Box::pin(async move { Ok(self.message.clone()) })
        }

        fn record_outbox_delivery<'a>(
            &'a self,
            _context: &'a QueryContext,
            command: RecordOutboxDelivery,
        ) -> StoreFuture<'a, ()> {
            Box::pin(async move {
                *self.recorded.lock().expect("record lock") = Some(command);
                Ok(())
            })
        }
    }

    #[derive(Debug)]
    struct FakePublisher(bool);

    impl OutboxPublisher for FakePublisher {
        fn publish(&self, _message: &OutboxMessage) -> OutboxPublishFuture<'_> {
            Box::pin(async move {
                if self.0 {
                    Ok(())
                } else {
                    Err(OutboxPublishError {
                        code: "BROKER_UNAVAILABLE".to_owned(),
                        safe_message: "broker unavailable".to_owned(),
                    })
                }
            })
        }
    }

    fn message() -> OutboxMessage {
        OutboxMessage {
            tenant_id: TenantId::from_bytes([1; 16]),
            outbox_id: OutboxId::from_bytes([2; 16]),
            event_id: EventId::from_bytes([3; 16]),
            run_id: RunId::from_bytes([4; 16]),
            topic: "run.events".to_owned(),
            partition_key: "run-4".to_owned(),
            payload: JsonPayload::from_validated_bytes(b"{}".to_vec()),
            attempt: 2,
            lease_expires_at: UnixMicros::new(500),
            created_at: UnixMicros::new(100),
        }
    }

    #[tokio::test]
    async fn failed_publish_is_durably_retried_after_the_current_lease() {
        let recorded = Arc::new(Mutex::new(None));
        let worker = OutboxWorker::new(
            FakeStore {
                message: Some(message()),
                recorded: Arc::clone(&recorded),
            },
            FakePublisher(false),
            TenantId::from_bytes([1; 16]),
            WorkerId::from_bytes([5; 16]),
            LeaseToken::from_bytes([6; 32]),
            OutboxWorkerConfig::default(),
        )
        .expect("worker");

        assert!(matches!(
            worker.poll_once().await.expect("poll"),
            OutboxPollOutcome::RetryScheduled { .. }
        ));
        let command = recorded
            .lock()
            .expect("record lock")
            .clone()
            .expect("recorded outcome");
        assert!(matches!(
            command.outcome,
            OutboxDeliveryOutcome::Retry {
                available_at,
                ref error_code,
            } if available_at == UnixMicros::new(500) && error_code == "BROKER_UNAVAILABLE"
        ));
        assert_eq!(command.expected_attempt, 2);
    }
}
