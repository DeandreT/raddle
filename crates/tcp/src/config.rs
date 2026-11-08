use std::time::Duration;

use raddle::wire::EnvelopeLimits;

/// Payload-free TCP failures. Unknown outcomes must retain the message identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid local endpoint, limits, batch, or request configuration.
    #[error("invalid TCP configuration")]
    Configuration,
    /// Message validation failed or the receiver permanently refused it.
    #[error("TCP message refused")]
    Rejected,
    /// No matching custody acknowledgement was observed; acceptance is unknown.
    #[error("TCP acceptance is uncertain")]
    Uncertain,
    /// Custody was acknowledged, but the expected reply was absent or had a
    /// different contract. This does not authorize replaying business effects.
    #[error("TCP reply contract mismatch")]
    Reply,
}

/// Finite per-connection bounds, including application custody work.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Validated envelope limits, applied independently to requests and replies.
    pub envelope: EnvelopeLimits,
    /// Maximum number of messages on one batch connection, at most 256.
    pub batch_messages: usize,
    /// Maximum aggregate request and reply bytes per batch, at most 16 MiB.
    pub batch_bytes: usize,
    /// Maximum live connections, including consumers awaiting custody.
    pub connections: usize,
    /// Total validation, connect, batch IO and custody deadline; no message reset.
    pub deadline: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            envelope: EnvelopeLimits::default(),
            batch_messages: 64,
            batch_bytes: 4_194_304,
            connections: 32,
            deadline: Duration::from_secs(60),
        }
    }
}

impl Limits {
    pub(crate) fn validate(self) -> Result<(), Error> {
        if !(1..=1_048_576).contains(&self.envelope.max_bytes)
            || self.envelope.max_headers > 256
            || !(1..=256).contains(&self.batch_messages)
            || !(1..=16_777_216).contains(&self.batch_bytes)
            || !(1..=256).contains(&self.connections)
            || self.deadline.is_zero()
            || self.deadline > Duration::from_secs(3600)
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}

/// An application-configured host and port. Wire metadata cannot select it.
#[derive(Clone)]
pub struct Endpoint {
    pub(crate) host: String,
    pub(crate) port: u16,
}

impl Endpoint {
    /// Validates a host name or IP literal and a nonzero port. DNS resolution is
    /// bounded by the publisher's total deadline, not performed at construction.
    pub fn try_new(host: impl Into<String>, port: u16) -> Result<Self, Error> {
        let host = host.into();
        if host.is_empty()
            || host.len() > 253
            || host.bytes().any(|byte| {
                !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b':' | b'%'))
            })
            || port == 0
        {
            return Err(Error::Configuration);
        }
        Ok(Self { host, port })
    }
}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Endpoint").finish_non_exhaustive()
    }
}
