//! Bounded ephemeral reply waiters. Durable conversations belong to persistence adapters.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::oneshot;
use uuid::Uuid;

use crate::wire::Envelope;

/// Ephemeral request/reply failure. A timeout does not imply remote failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Capacity, identity, or duration was invalid.
    #[error("invalid waiter or exhausted conversation capacity")]
    Invalid,
    /// The reply deadline elapsed.
    #[error("no reply observed before the deadline")]
    Timeout,
    /// The registry was closed or its synchronization was poisoned.
    #[error("reply registry is closed")]
    Closed,
}

struct Entry {
    token: Uuid,
    responder: String,
    contract: String,
    sender: oneshot::Sender<Envelope>,
}
struct State {
    entries: Mutex<HashMap<Uuid, Entry>>,
    capacity: usize,
}

/// Cloneable routing handle for a finite number of live ephemeral requests.
#[derive(Clone)]
pub struct ReplyHub {
    state: Arc<State>,
}

impl ReplyHub {
    /// Creates a registry with a hard capacity. No background tasks are spawned.
    pub fn try_new(capacity: usize) -> Result<Self, Error> {
        if capacity == 0 || capacity > 100_000 {
            return Err(Error::Invalid);
        }
        Ok(Self {
            state: Arc::new(State {
                entries: Mutex::new(HashMap::new()),
                capacity,
            }),
        })
    }

    /// Registers before sending, avoiding races with a fast reply.
    /// Request IDs must not be reused while delayed replies could still arrive.
    pub fn register(
        &self,
        id: Uuid,
        responder: &str,
        contract: &str,
    ) -> Result<ReplyWaiter, Error> {
        if id.is_nil()
            || crate::SubscriptionName::try_new(responder).is_err()
            || crate::SubscriptionName::try_new(contract).is_err()
        {
            return Err(Error::Invalid);
        }
        let mut entries = self.state.entries.lock().map_err(|_| Error::Closed)?;
        if entries.len() >= self.state.capacity || entries.contains_key(&id) {
            return Err(Error::Invalid);
        }
        let (sender, receiver) = oneshot::channel();
        let token = Uuid::new_v4();
        entries.insert(
            id,
            Entry {
                token,
                responder: responder.into(),
                contract: contract.into(),
                sender,
            },
        );
        Ok(ReplyWaiter {
            id,
            token,
            receiver,
            state: Arc::downgrade(&self.state),
        })
    }

    /// Delivers only a matching reply from an independently authenticated source.
    /// Returns false for late, forged, unknown, or duplicate replies.
    pub fn deliver(&self, trusted_source: &str, reply: Envelope) -> Result<bool, Error> {
        let Some(id) = reply.correlation_id() else {
            return Ok(false);
        };
        let mut entries = self.state.entries.lock().map_err(|_| Error::Closed)?;
        let Some(entry) = entries.get(&id) else {
            return Ok(false);
        };
        if entry.responder != trusted_source || entry.contract != reply.contract() {
            return Ok(false);
        }
        Ok(entries
            .remove(&id)
            .is_some_and(|entry| entry.sender.send(reply).is_ok()))
    }
}

/// One caller-owned waiter. Dropping it removes the registration and releases capacity.
pub struct ReplyWaiter {
    id: Uuid,
    token: Uuid,
    receiver: oneshot::Receiver<Envelope>,
    state: Weak<State>,
}

impl ReplyWaiter {
    /// Waits at most the supplied duration. Cancellation cannot undo remote effects.
    pub async fn wait(mut self, timeout: Duration) -> Result<Envelope, Error> {
        if timeout.is_zero() {
            return Err(Error::Invalid);
        }
        match tokio::time::timeout(timeout, &mut self.receiver).await {
            Ok(Ok(envelope)) => Ok(envelope),
            Ok(Err(_)) => Err(Error::Closed),
            Err(_) => Err(Error::Timeout),
        }
    }
}

impl Drop for ReplyWaiter {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade()
            && let Ok(mut entries) = state.entries.lock()
            && entries
                .get(&self.id)
                .is_some_and(|entry| entry.token == self.token)
        {
            entries.remove(&self.id);
        }
    }
}
