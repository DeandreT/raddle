//! Opt-in, finite publication routing over canonical native message contracts.
//!
//! Configuration opens no connections. Sources and predicates are pure startup
//! callbacks; the compiled plans also supply delivery explanations. Projection
//! callbacks run per input, before custody, and must be deterministic and pure.

mod compiler;
mod config;
mod metadata;
mod projection;

pub use compiler::{ExplainedRoute, RoutingExplanation, RoutingStep};
pub(crate) use compiler::{Resolver, Route};
pub use config::{LocalRouting, RouteSource, RoutingConfig, SourceOptions};
pub use metadata::{MessageDeclaration, MessageRole, MessageSelector};

/// Startup or per-input projection failure. Diagnostics never contain payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum RoutingError {
    /// Invalid, duplicate, conflicting or unbounded configuration.
    #[error("invalid routing configuration: {0}")]
    Invalid(&'static str),
    /// A pure startup source or predicate failed or panicked.
    #[error("routing startup callback failed")]
    Callback,
    /// Typed canonical projection failed or panicked for this input.
    #[error("message projection failed")]
    Projection,
    /// Known protected-root framing cannot be projected as plaintext.
    #[error("protected message framing cannot be projected")]
    Protected,
    /// Singular outgoing intent cannot represent different payload branches.
    #[error("multiple outgoing envelopes require intents or returned_many")]
    MultipleEnvelopes,
}

pub(crate) type Key = (String, u32);

pub(crate) fn key<M: raddle::wire::WireMessage>() -> Result<Key, RoutingError> {
    raddle::routing::WireMessageType::of::<M>()
        .map(|_| (M::NAME.to_owned(), M::VERSION))
        .map_err(|_| RoutingError::Invalid("invalid contract"))
}

fn name(value: &str) -> Result<(), RoutingError> {
    raddle::SubscriptionName::try_new(value)
        .map(|_| ())
        .map_err(|_| RoutingError::Invalid("invalid symbolic name"))
}
