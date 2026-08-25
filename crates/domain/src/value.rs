use std::{error::Error, fmt};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Digest([u8; 32]);

impl Digest {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Digest(sha256:")?;
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        formatter.write_str(")")
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct LeaseToken([u8; 32]);

impl LeaseToken {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for LeaseToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeaseToken([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct UnixMicros(i64);

impl UnixMicros {
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct DurationMicros(u64);

impl DurationMicros {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct JsonPayload(Vec<u8>);

impl JsonPayload {
    /// Creates an opaque payload after the API/runtime layer has validated its JSON schema.
    pub fn from_validated_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl fmt::Debug for JsonPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JsonPayload")
            .field("byte_len", &self.0.len())
            .finish_non_exhaustive()
    }
}

macro_rules! define_key {
    ($name:ident, $max_len:expr) => {
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub const MAX_LEN: usize = $max_len;

            /// Parses a bounded identity key.
            ///
            /// # Errors
            ///
            /// Returns [`KeyValidationError`] when the key is empty, exceeds
            /// the configured length limit, or contains control characters.
            pub fn parse(value: impl Into<String>) -> Result<Self, KeyValidationError> {
                let value = value.into();
                if value.is_empty() {
                    return Err(KeyValidationError::Empty);
                }
                if value.len() > Self::MAX_LEN {
                    return Err(KeyValidationError::TooLong {
                        actual: value.len(),
                        maximum: Self::MAX_LEN,
                    });
                }
                if value.chars().any(char::is_control) {
                    return Err(KeyValidationError::ContainsControlCharacter);
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.0)
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

define_key!(TenantKey, 255);
define_key!(LogicalKey, 255);
define_key!(IdempotencyKey, 255);
define_key!(ScopeKey, 64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyValidationError {
    Empty,
    TooLong { actual: usize, maximum: usize },
    ContainsControlCharacter,
}

impl fmt::Display for KeyValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("key must not be empty"),
            Self::TooLong { actual, maximum } => {
                write!(formatter, "key length {actual} exceeds maximum {maximum}")
            }
            Self::ContainsControlCharacter => {
                formatter.write_str("key must not contain control characters")
            }
        }
    }
}

impl Error for KeyValidationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_preserve_case_and_reject_controls() {
        let upper = LogicalKey::parse("Task/A").expect("valid key");
        let lower = LogicalKey::parse("task/a").expect("valid key");
        assert_ne!(upper, lower);
        assert_eq!(
            LogicalKey::parse("bad\nkey"),
            Err(KeyValidationError::ContainsControlCharacter)
        );
    }

    #[test]
    fn sensitive_values_are_redacted_in_debug_output() {
        let token = LeaseToken::from_bytes([7; 32]);
        assert_eq!(format!("{token:?}"), "LeaseToken([REDACTED])");
    }
}
