//! Backend contracts preserve acknowledged-delivery semantics without hiding protocols.

use std::future::Future;

use crate::wire::Envelope;

/// A transport send failure classification; no payload or credential is included.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum TransportError {
    /// Safe to retry; a send may already have been accepted.
    #[error("transport outcome is transient or uncertain")]
    Transient,
    /// The destination or message is permanently invalid.
    #[error("transport refused the destination or message")]
    Rejected,
}

/// A confirmed publisher. Success means broker acceptance, not consumer completion.
pub trait Transport: Send + Sync {
    /// Publishes to an application-configured endpoint alias, never an untrusted URI.
    fn publish(
        &self,
        destination: &str,
        envelope: &Envelope,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;
}
