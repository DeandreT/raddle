//! Receiver-owned protocol work. Dropping a caller only stops polling, not ownership.

use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use fe2o3_amqp::{link::IllegalLinkStateError, Receiver};
use futures_util::FutureExt;

use super::{error::RecoverReceiverError, receiver_recovery::ReceiverRecovery};
use crate::{
    primitives::{
        service_bus_received_message::ServiceBusReceivedMessage,
        service_bus_retry_policy::{ServiceBusRetryPolicy, ServiceBusRetryPolicyError},
    },
    util::time::Instant,
};

#[cfg(not(target_arch = "wasm32"))]
type OwnedFuture<T, E> = Pin<Box<dyn Future<Output = (T, Result<(), E>)> + Send>>;
#[cfg(target_arch = "wasm32")]
type OwnedFuture<T, E> = Pin<Box<dyn Future<Output = (T, Result<(), E>)>>>;

/// Both the resource and its unfinished operation stay in the receiver, never a spawned task.
pub(super) struct OwnedOperation<T, E>(OwnedFuture<T, E>);

impl<T, E> fmt::Debug for OwnedOperation<T, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OwnedOperation { .. }")
    }
}

impl<T, E> OwnedOperation<T, E> {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(operation: impl Future<Output = (T, Result<(), E>)> + Send + 'static) -> Self {
        Self(Box::pin(operation))
    }

    #[cfg(target_arch = "wasm32")]
    pub fn new(operation: impl Future<Output = (T, Result<(), E>)> + 'static) -> Self {
        Self(Box::pin(operation))
    }
}

impl<T, E> Future for OwnedOperation<T, E> {
    type Output = (T, Result<(), E>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

#[derive(Debug, Default)]
pub(super) struct Recovery {
    pub needed: bool,
    retry_at: Option<Instant>,
    failed_attempts: u32,
    pub operation: Option<OwnedOperation<ReceiverRecovery, RecoverReceiverError>>,
    pub cleanup: Option<OwnedOperation<Receiver, IllegalLinkStateError>>,
    pub cleanup_error: Option<IllegalLinkStateError>,
    pub buffer: VecDeque<ServiceBusReceivedMessage>,
    pub session_locked_until: Option<time::OffsetDateTime>,
}

impl Recovery {
    /// Called for receive *and* recovery failures, before returning control to the caller.
    pub fn failed(
        &mut self,
        policy: &dyn ServiceBusRetryPolicy,
        error: &dyn ServiceBusRetryPolicyError,
    ) -> bool {
        self.needed = true;
        self.failed_attempts = self.failed_attempts.saturating_add(1);
        let delay = policy.calculate_retry_delay(error, self.failed_attempts);
        self.retry_at = delay.map(|delay| Instant::now() + delay);
        delay.is_some()
    }

    pub fn remaining(&self) -> Duration {
        self.retry_at
            .map(|at| at.saturating_duration_since(Instant::now()))
            .unwrap_or_default()
    }

    pub fn recovered(&mut self) {
        self.needed = false;
        self.retry_at = None;
    }

    pub fn healthy(&mut self) {
        self.failed_attempts = 0;
    }

    pub fn pending(&self) -> bool {
        self.needed
            || self.operation.is_some()
            || self.cleanup.is_some()
            || self.cleanup_error.is_some()
    }

    pub fn take_batch(&mut self, count: u32) -> Vec<ServiceBusReceivedMessage> {
        let count = self.buffer.len().min(count as usize);
        self.buffer.drain(..count).collect()
    }
}

/// A lease is scoped to a receive attempt, but its cleanup belongs to the receiver.
/// A full outgoing queue must not drop the link: fe2o3's destructor only try_sends
/// a detach to that same queue. Keeping this future also preserves its queue position.
pub(super) struct CreditLease<'a> {
    pub slot: &'a mut Option<Receiver>,
    pub cleanup: &'a mut Option<OwnedOperation<Receiver, IllegalLinkStateError>>,
    pub cleanup_error: &'a mut Option<IllegalLinkStateError>,
    pub manual: bool,
}

impl Drop for CreditLease<'_> {
    fn drop(&mut self) {
        if !self.manual {
            return;
        }
        if let Some(mut receiver) = self.slot.take() {
            let mut operation = OwnedOperation::new(async move {
                let result = receiver.set_credit(0).await;
                (receiver, result)
            });
            // This polls a *reference*. Pending work is retained, never cancelled.
            match (&mut operation).now_or_never() {
                Some((receiver, result)) => {
                    *self.slot = Some(receiver);
                    *self.cleanup_error = result.err();
                }
                None => *self.cleanup = Some(operation),
            }
        }
    }
}

#[cfg(test)]
mod tests;
