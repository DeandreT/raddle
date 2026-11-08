use crate::SubscriptionName;

/// An invalid execution policy, rejected before a runtime can be built.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigurationError {
    /// The concurrency limit was zero or larger than the supported semaphore limit.
    #[error("concurrency must be between 1 and {maximum}, got {requested}")]
    InvalidConcurrency {
        /// The supplied concurrency.
        requested: usize,
        /// The platform's maximum supported concurrency.
        maximum: usize,
    },
}

/// An invalid runtime registration. A failed registration leaves the builder unchanged.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum RegistrationError {
    /// A request type already has its single handler.
    #[error("request handler already registered for {message}")]
    DuplicateRequest {
        /// Rust type name for diagnostics only; never a wire identity.
        message: &'static str,
    },
    /// Subscription names must be unique across the runtime.
    #[error("subscription {name} is already registered")]
    DuplicateSubscription {
        /// The conflicting stable name.
        name: SubscriptionName,
    },
}

/// Admission or handler failure, retaining the concrete domain error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ExecutionError<E> {
    /// The runtime was closed before this handler acquired admission.
    #[error("runtime is closed")]
    Closed,
    /// No handler was registered for this request type.
    #[error("no request handler registered for {message}")]
    Unregistered {
        /// Rust type name for diagnostics only.
        message: &'static str,
    },
    /// The handler returned a domain failure.
    #[error("handler failed: {0}")]
    Handler(#[source] E),
    /// A private type-erasure boundary was inconsistent.
    #[error("internal handler type mismatch")]
    TypeMismatch,
}
