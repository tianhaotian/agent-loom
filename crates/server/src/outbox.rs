use agent_loom_domain::OutboxMessage;
use agent_loom_runtime::{OutboxPublishFuture, OutboxPublisher};

#[derive(Clone, Copy, Debug, Default)]
pub struct JsonLogOutboxPublisher;

impl OutboxPublisher for JsonLogOutboxPublisher {
    fn publish(&self, message: &OutboxMessage) -> OutboxPublishFuture<'_> {
        let envelope = serde_json::from_slice::<serde_json::Value>(message.payload.as_bytes())
            .unwrap_or_else(|_| serde_json::json!({"invalid_payload": true}));
        let log = serde_json::json!({
            "level": "info",
            "kind": "outbox.published",
            "outbox_id": message.outbox_id.to_string(),
            "event_id": message.event_id.to_string(),
            "run_id": message.run_id.to_string(),
            "topic": message.topic,
            "partition_key": message.partition_key,
            "attempt": message.attempt,
            "envelope": envelope,
        });
        Box::pin(async move {
            println!("{log}");
            Ok(())
        })
    }
}
