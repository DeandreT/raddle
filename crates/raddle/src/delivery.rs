use std::sync::Arc;

/// Owned access to one event shared immutably with independent subscribers.
///
/// Shared ownership is necessary because subscriber futures can be polled and
/// canceled independently. Cloning a delivery does not clone the event payload.
#[derive(Debug)]
pub struct EventDelivery<E> {
    message: Arc<E>,
}

impl<E> EventDelivery<E> {
    pub(crate) fn new(message: Arc<E>) -> Self {
        Self { message }
    }

    /// Borrows the event without exposing the ownership implementation.
    pub fn message(&self) -> &E {
        &self.message
    }
}

impl<E> Clone for EventDelivery<E> {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.message))
    }
}
