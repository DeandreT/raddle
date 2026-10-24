use std::error::Error;

/// A local request with exactly one registered handler.
///
/// Requests and replies are moved, not serialized or cloned. Use
/// [`std::convert::Infallible`] when a handler has no domain failure.
pub trait Request: Send + 'static {
    /// The successful result produced by the handler.
    type Reply: Send + 'static;

    /// A domain failure, preserved by [`crate::Runtime::invoke`].
    type Error: Error + Send + Sync + 'static;
}

/// A local notification delivered independently to each named subscriber.
///
/// The event is shared immutably across concurrent subscribers, so it must be
/// `Sync` but need not implement `Clone` or any serialization trait.
pub trait Event: Send + Sync + 'static {
    /// A subscriber's domain failure.
    type Error: Error + Send + Sync + 'static;
}
