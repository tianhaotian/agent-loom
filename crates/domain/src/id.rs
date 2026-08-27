use std::fmt;

macro_rules! define_id {
    ($($name:ident),+ $(,)?) => {
        $(
            #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
            #[repr(transparent)]
            pub struct $name([u8; 16]);

            impl $name {
                pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                    Self(bytes)
                }

                pub const fn into_bytes(self) -> [u8; 16] {
                    self.0
                }

                pub const fn as_bytes(&self) -> &[u8; 16] {
                    &self.0
                }

                pub fn is_nil(self) -> bool {
                    self.0 == [0; 16]
                }
            }

            impl From<[u8; 16]> for $name {
                fn from(value: [u8; 16]) -> Self {
                    Self::from_bytes(value)
                }
            }

            impl fmt::Display for $name {
                fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    for byte in self.0 {
                        write!(formatter, "{byte:02x}")?;
                    }
                    Ok(())
                }
            }

            impl fmt::Debug for $name {
                fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(formatter, "{}({self})", stringify!($name))
                }
            }
        )+
    };
}

define_id!(
    TenantId,
    WorkflowId,
    WorkflowVersionId,
    AgentId,
    AgentVersionId,
    EndpointId,
    RunId,
    PlanRevisionId,
    ContextSnapshotId,
    ContextPatchId,
    StageExecutionId,
    TaskId,
    TaskAttemptId,
    EventId,
    CheckpointId,
    WaitId,
    ArtifactId,
    ToolExecutionId,
    ToolAttemptId,
    AgentExecutionId,
    AgentEventReceiptId,
    ReceiptId,
    OutboxId,
    CommandId,
    CorrelationId,
    CausationId,
    WorkerId,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_have_stable_hex_representation() {
        let id = RunId::from_bytes([0xab; 16]);
        assert_eq!(id.to_string(), "abababababababababababababababab");
        assert_eq!(id.into_bytes(), [0xab; 16]);
    }

    #[test]
    fn nil_is_explicit_and_not_a_default() {
        assert!(TaskId::from_bytes([0; 16]).is_nil());
        assert!(!TaskId::from_bytes([1; 16]).is_nil());
    }
}
