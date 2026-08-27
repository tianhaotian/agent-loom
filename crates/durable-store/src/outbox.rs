use agent_loom_domain::{DurationMicros, OutboxId, UnixMicros, WorkerId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimOutbox {
    pub publisher_id: WorkerId,
    pub lease_token: agent_loom_domain::LeaseToken,
    pub lease_duration: DurationMicros,
}

impl ClaimOutbox {
    pub fn shape_is_valid(&self) -> bool {
        !self.publisher_id.is_nil() && self.lease_duration.get() > 0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboxDeliveryOutcome {
    Published,
    Retry {
        available_at: UnixMicros,
        error_code: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordOutboxDelivery {
    pub outbox_id: OutboxId,
    pub expected_attempt: u32,
    pub publisher_id: WorkerId,
    pub lease_token: agent_loom_domain::LeaseToken,
    pub outcome: OutboxDeliveryOutcome,
}

impl RecordOutboxDelivery {
    pub fn shape_is_valid(&self) -> bool {
        self.expected_attempt > 0
            && !self.publisher_id.is_nil()
            && match &self.outcome {
                OutboxDeliveryOutcome::Published => true,
                OutboxDeliveryOutcome::Retry { error_code, .. } => !error_code.is_empty(),
            }
    }
}
