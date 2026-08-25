use std::{error::Error, fmt};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StoreErrorCode {
    NotFound,
    TenantMismatch,
    InvalidTransition,
    VersionConflict,
    TerminalRun,
    LeaseLost,
    LeaseExpired,
    IdempotencyKeyReused,
    WaitMismatch,
    WaitAlreadyConsumed,
    WaitExpired,
    DeadlineExceeded,
    OutcomeUnknown,
    PauseRecoveryRequired,
    AdapterCapabilityMissing,
    InconsistentProjection,
    ConstraintViolation,
    SerializationConflict,
    StoreUnavailable,
    MigrationRequired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RetryClass {
    Never,
    ReloadState,
    Backoff,
    Reconcile,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreError {
    pub code: StoreErrorCode,
    pub retry: RetryClass,
    pub safe_message: String,
}

impl StoreError {
    pub fn new(code: StoreErrorCode, retry: RetryClass, safe_message: impl Into<String>) -> Self {
        Self {
            code,
            retry,
            safe_message: safe_message.into(),
        }
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.safe_message)
    }
}

impl Error for StoreError {}

pub type StoreResult<T> = Result<T, StoreError>;
