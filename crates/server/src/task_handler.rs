use std::{error::Error, fmt};

use agent_loom_domain::{JsonPayload, LogicalKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const TASK_INPUT_SCHEMA_V1: &str = "agent-loom.task-input/v1";

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TaskInputEnvelope {
    schema: String,
    handler: String,
    payload: Value,
}

#[derive(Debug, PartialEq)]
pub(crate) struct RoutedTaskInput {
    pub handler_key: LogicalKey,
    pub payload: Value,
}

pub(crate) fn encode_task_input(
    handler_key: &LogicalKey,
    payload: Value,
) -> Result<JsonPayload, TaskInputError> {
    serde_json::to_vec(&TaskInputEnvelope {
        schema: TASK_INPUT_SCHEMA_V1.to_owned(),
        handler: handler_key.as_str().to_owned(),
        payload,
    })
    .map(JsonPayload::from_validated_bytes)
    .map_err(|_| TaskInputError::InvalidPayload)
}

pub(crate) fn decode_task_input(input: &JsonPayload) -> Result<RoutedTaskInput, TaskInputError> {
    let mut value: Value =
        serde_json::from_slice(input.as_bytes()).map_err(|_| TaskInputError::InvalidEnvelope)?;
    if let Some(resume_input) = value
        .as_object_mut()
        .and_then(|object| object.remove("resume_input"))
    {
        value = resume_input;
    }
    let envelope: TaskInputEnvelope =
        serde_json::from_value(value).map_err(|_| TaskInputError::InvalidEnvelope)?;
    if envelope.schema != TASK_INPUT_SCHEMA_V1 {
        return Err(TaskInputError::UnsupportedSchema);
    }
    let handler_key =
        LogicalKey::parse(envelope.handler).map_err(|_| TaskInputError::InvalidHandlerKey)?;
    Ok(RoutedTaskInput {
        handler_key,
        payload: envelope.payload,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TaskInputError {
    InvalidEnvelope,
    UnsupportedSchema,
    InvalidHandlerKey,
    InvalidPayload,
}

impl fmt::Display for TaskInputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidEnvelope => "Task input is not a valid routed envelope",
            Self::UnsupportedSchema => "Task input uses an unsupported envelope schema",
            Self::InvalidHandlerKey => "Task input handler key is invalid",
            Self::InvalidPayload => "Task input payload cannot be encoded",
        })
    }
}

impl Error for TaskInputError {}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn handler_key() -> LogicalKey {
        LogicalKey::parse("delivery-mvp").expect("handler key")
    }

    #[test]
    fn task_input_round_trips_with_stable_handler_key() {
        let encoded = encode_task_input(&handler_key(), json!({"step": 1})).expect("encode");
        let decoded = decode_task_input(&encoded).expect("decode");

        assert_eq!(decoded.handler_key, handler_key());
        assert_eq!(decoded.payload["step"], 1);
    }

    #[test]
    fn wait_metadata_preserves_the_routed_resume_input() {
        let resume = encode_task_input(&handler_key(), json!({"step": 9})).expect("encode");
        let resume: Value = serde_json::from_slice(resume.as_bytes()).expect("value");
        let stored = JsonPayload::from_validated_bytes(
            serde_json::to_vec(&json!({
                "wait_id": "wait-1",
                "event_type": "approval.granted",
                "resume_input": resume
            }))
            .expect("stored input"),
        );
        let decoded = decode_task_input(&stored).expect("decode resume");

        assert_eq!(decoded.handler_key, handler_key());
        assert_eq!(decoded.payload["step"], 9);
    }

    #[test]
    fn legacy_unrouted_input_is_rejected() {
        let input = JsonPayload::from_validated_bytes(br#"{"step":1}"#.to_vec());

        assert_eq!(
            decode_task_input(&input),
            Err(TaskInputError::InvalidEnvelope)
        );
    }
}
