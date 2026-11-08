use std::fmt;
use std::str::FromStr;

/// A stable, runtime-wide subscriber identity.
///
/// Names contain 1–128 ASCII bytes. The first byte is a lowercase letter or
/// digit; subsequent bytes may also be `.`, `_`, or `-`. Names are case-sensitive
/// and must not be inferred from Rust type paths.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SubscriptionName(Box<str>);

impl SubscriptionName {
    /// Checks and owns a subscriber name without normalizing it.
    pub fn try_new(name: impl Into<String>) -> Result<Self, InvalidSubscriptionName> {
        let name = name.into();
        if name.is_empty() {
            return Err(InvalidSubscriptionName::Empty);
        }
        if name.len() > 128 {
            return Err(InvalidSubscriptionName::TooLong { length: name.len() });
        }
        for (index, byte) in name.bytes().enumerate() {
            let valid = byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || (index > 0 && matches!(byte, b'.' | b'_' | b'-'));
            if !valid {
                return Err(InvalidSubscriptionName::InvalidByte { index });
            }
        }
        Ok(Self(name.into_boxed_str()))
    }

    /// Returns the validated name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SubscriptionName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for SubscriptionName {
    type Err = InvalidSubscriptionName;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::try_new(name)
    }
}

impl AsRef<str> for SubscriptionName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Why a stable subscriber identity was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidSubscriptionName {
    /// The name was empty.
    #[error("subscription name must not be empty")]
    Empty,
    /// The name exceeded its byte limit.
    #[error("subscription name has {length} bytes; the maximum is 128")]
    TooLong {
        /// The supplied length in bytes.
        length: usize,
    },
    /// The name contained a disallowed byte.
    #[error("invalid subscription name byte at index {index}")]
    InvalidByte {
        /// The byte offset, not a Unicode character index.
        index: usize,
    },
}
