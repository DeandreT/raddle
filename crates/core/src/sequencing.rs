//! Finite process-local execution slots. Not cluster ownership or broker custody.

use std::sync::Arc;

use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use uuid::Uuid;

use crate::grouping::GroupId;

/// Fixed-size shared execution owner; memory never grows with logical key count.
pub struct SlotSet {
    domain: String,
    slots: Vec<Arc<Semaphore>>,
    changed: watch::Sender<u64>,
}

impl SlotSet {
    /// Creates 1..=256 slots for a bounded symbolic domain. Starts no task.
    pub fn new(domain: &str, count: usize) -> Result<Arc<Self>, &'static str> {
        crate::SubscriptionName::try_new(domain).map_err(|_| "invalid slot domain")?;
        if !(1..=256).contains(&count) {
            return Err("invalid slot count");
        }
        Ok(Arc::new(Self {
            domain: domain.into(),
            slots: (0..count).map(|_| Arc::new(Semaphore::new(1))).collect(),
            changed: watch::channel(0).0,
        }))
    }
    /// Configured finite slot count.
    pub fn len(&self) -> usize {
        self.slots.len()
    }
    /// Stable configured execution domain, not a logical message identity.
    pub fn domain(&self) -> &str {
        &self.domain
    }
    /// Always false for a validated slot set.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
    /// Stable version-one native selection; missing groups distribute by message ID.
    pub fn slot(&self, tenant: &str, group: Option<&GroupId>, id: Uuid) -> usize {
        let mut hash = Sha256::new();
        hash.update(b"raddle.execution-slot.v1\0");
        for part in [self.domain.as_bytes(), tenant.as_bytes()] {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part);
        }
        match group {
            Some(group) => {
                hash.update([1]);
                hash.update(group.as_str().as_bytes());
            }
            None => {
                hash.update([0]);
                hash.update(id.as_bytes());
            }
        }
        hash.finalize()
            .iter()
            .fold(0usize, |n, b| (n * 256 + usize::from(*b)) % self.len())
    }
    /// Nonblocking ownership. A busy slot never consumes a handler future.
    pub fn try_reserve(self: &Arc<Self>, slot: usize) -> Option<ExecutionSlotLease> {
        let permit = Arc::clone(self.slots.get(slot)?).try_acquire_owned().ok()?;
        Some(ExecutionSlotLease {
            owner: Arc::clone(self),
            permit: Some(permit),
        })
    }
    /// Observes releases, including work owned by another queue sharing this set.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
}

/// Owned sequencing resource; no mutex guard is held across application code.
pub struct ExecutionSlotLease {
    owner: Arc<SlotSet>,
    permit: Option<OwnedSemaphorePermit>,
}
impl Drop for ExecutionSlotLease {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.owner.changed.send_modify(|n| *n = n.wrapping_add(1));
    }
}
