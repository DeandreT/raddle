use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Validated aggregation bounds. Defaults: 100 originals, 250ms, 10,000 pending.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Options {
    size: usize,
    wait: Duration,
    capacity: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            size: 100,
            wait: Duration::from_millis(250),
            capacity: 10_000,
        }
    }
}

impl Options {
    /// Bounds size to 1..=256, wait to 1ms..=1h and pending to size..=100,000.
    pub fn new(size: usize, wait: Duration, capacity: usize) -> Result<Self, &'static str> {
        if !(1..=256).contains(&size)
            || !(Duration::from_millis(1)..=Duration::from_secs(3600)).contains(&wait)
            || !(size..=100_000).contains(&capacity)
        {
            return Err("invalid batching bounds");
        }
        Ok(Self {
            size,
            wait,
            capacity,
        })
    }
    /// Maximum originals, before coalescing.
    pub fn size(self) -> usize {
        self.size
    }
    /// Maximum accumulation time after first admission.
    pub fn wait(self) -> Duration {
        self.wait
    }
    /// Maximum originals across accumulation and outstanding batches.
    pub fn capacity(self) -> usize {
        self.capacity
    }
}

/// Trusted grouping identity. Never derive tenant or group from arbitrary headers.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BatchKey {
    tenant: String,
    group: Option<String>,
}

impl BatchKey {
    /// Creates a tenant/group boundary; absent group is an explicit ungrouped lane.
    pub fn new(tenant: impl Into<String>, group: Option<String>) -> Result<Self, &'static str> {
        let tenant = tenant.into();
        if tenant.is_empty()
            || tenant.len() > 128
            || group
                .as_ref()
                .is_some_and(|group| group.is_empty() || group.len() > 128)
        {
            return Err("invalid trusted batch key");
        }
        Ok(Self { tenant, group })
    }
    /// Authenticated tenant.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    /// Trusted group, if grouping was requested.
    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }
}

/// Original delivery context, retained even when its input is coalesced away.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberContext {
    /// Original delivery/custody identity, unique across independently routed jobs.
    pub delivery_id: Uuid,
    /// Original message identity, not a synthetic batch identity.
    pub id: Uuid,
    /// Original execution attempt (not the synthetic batch's count).
    pub attempt: u32,
    /// Producer's original timestamp.
    pub sent_at: DateTime<Utc>,
    /// Original expiry. An unbounded member prevents whole-batch expiry.
    pub expires_at: Option<DateTime<Utc>>,
}

/// A typed original. Coalescing never discards this object from a batch.
#[derive(Debug)]
pub struct Member<T> {
    /// Original custody context.
    pub context: MemberContext,
    /// Application message.
    pub value: T,
    /// Optional trusted, bounded last-value-wins key.
    pub coalesce: Option<String>,
}

/// Failed admission returns ownership; callers must retry or settle explicitly.
#[derive(Debug)]
pub struct AdmissionError<T> {
    /// Unaccepted original.
    pub member: Member<T>,
    /// Stable explanation; no payload is formatted.
    pub reason: &'static str,
}

/// Individually expired original awaiting caller settlement. Keep this token
/// until settlement; dropping it releases volatile pending tracking only.
pub struct Expired<T> {
    member: Member<T>,
    pending: Arc<AtomicUsize>,
}

impl<T> Expired<T> {
    /// Original to quarantine/expire through the caller's custody adapter.
    pub fn member(&self) -> &Member<T> {
        &self.member
    }
}

impl<T> Drop for Expired<T> {
    fn drop(&mut self) {
        self.pending.fetch_sub(1, Ordering::AcqRel);
    }
}

struct Bucket<T> {
    first: DateTime<Utc>,
    members: Vec<Member<T>>,
}

/// A volatile, bounded assembler. Use one per listener/type/consumer.
///
/// Methods require exclusive access; put it behind a caller-owned mutex for
/// concurrent producers. No first-use task or timer race is possible.
pub struct Assembler<T> {
    options: Options,
    buckets: BTreeMap<BatchKey, Bucket<T>>,
    pending: Arc<AtomicUsize>,
    identities: BTreeSet<Uuid>,
}

impl<T> Assembler<T> {
    pub(crate) fn counter(&self) -> std::sync::Weak<AtomicUsize> {
        Arc::downgrade(&self.pending)
    }
    /// Creates an empty assembler without spawning any task.
    pub fn new(options: Options) -> Self {
        Self {
            options,
            buckets: BTreeMap::new(),
            pending: Arc::default(),
            identities: BTreeSet::new(),
        }
    }
    /// Pending originals in accumulation and outstanding batches, not coalesced input count.
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }
    /// Admissions currently in this listener's accumulator (excludes execution).
    pub fn accumulating(&self) -> usize {
        self.buckets
            .values()
            .map(|bucket| bucket.members.len())
            .sum()
    }
    /// Accepts one original, or returns it intact. Expiry is checked before admission.
    /// Call `flush_due` after admission and when the caller's clock advances.
    pub fn push(
        &mut self,
        key: BatchKey,
        member: Member<T>,
        now: DateTime<Utc>,
    ) -> Result<(), AdmissionError<T>> {
        let reason = if member
            .context
            .expires_at
            .is_some_and(|expiry| expiry <= now)
        {
            Some("expired")
        } else if member
            .coalesce
            .as_ref()
            .is_some_and(|key| key.is_empty() || key.len() > 128)
        {
            Some("invalid coalescing key")
        } else if self.identities.contains(&member.context.delivery_id) {
            Some("duplicate accumulating identity")
        } else if self.pending() >= self.options.capacity {
            Some("capacity")
        } else if self
            .buckets
            .get(&key)
            .is_some_and(|bucket| bucket.members.len() >= self.options.size)
        {
            Some("flush full bucket before admission")
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(AdmissionError { member, reason });
        }
        self.identities.insert(member.context.delivery_id);
        self.pending.fetch_add(1, Ordering::AcqRel);
        self.buckets
            .entry(key)
            .or_insert_with(|| Bucket {
                first: now,
                members: Vec::new(),
            })
            .members
            .push(member);
        Ok(())
    }
    /// Flushes full/due buckets, shedding individually expired originals first.
    /// Returned expired originals remain caller-owned settlement obligations.
    pub fn flush_due(&mut self, now: DateTime<Utc>) -> (Vec<Batch<T>>, Vec<Expired<T>>) {
        self.flush(now, false)
    }
    /// Bounded synchronous drain. Applications then join/settle the returned batches.
    /// Does not pretend that a stopped handler or a dropped batch has committed.
    pub fn drain(&mut self, now: DateTime<Utc>) -> (Vec<Batch<T>>, Vec<Expired<T>>) {
        self.flush(now, true)
    }
    fn flush(&mut self, now: DateTime<Utc>, drain: bool) -> (Vec<Batch<T>>, Vec<Expired<T>>) {
        let mut batches = Vec::new();
        let mut expired = Vec::new();
        let keys: Vec<_> = self.buckets.keys().cloned().collect();
        for key in keys {
            let Some(mut bucket) = self.buckets.remove(&key) else {
                continue;
            };
            let mut live = Vec::new();
            for member in bucket.members {
                if member
                    .context
                    .expires_at
                    .is_some_and(|expiry| expiry <= now)
                {
                    self.identities.remove(&member.context.delivery_id);
                    expired.push(Expired {
                        member,
                        pending: Arc::clone(&self.pending),
                    });
                } else {
                    live.push(member);
                }
            }
            bucket.members = live;
            let elapsed = now
                .signed_duration_since(bucket.first)
                .to_std()
                .unwrap_or_default();
            if !bucket.members.is_empty()
                && (drain
                    || bucket.members.len() >= self.options.size
                    || elapsed >= self.options.wait)
            {
                for member in &bucket.members {
                    self.identities.remove(&member.context.delivery_id);
                }
                batches.push(Batch {
                    key,
                    members: bucket.members,
                    pending: Some(Arc::clone(&self.pending)),
                });
            } else if !bucket.members.is_empty() {
                self.buckets.insert(key, bucket);
            }
        }
        (batches, expired)
    }
}

impl<T> Drop for Assembler<T> {
    fn drop(&mut self) {
        self.pending
            .fetch_sub(self.accumulating(), Ordering::AcqRel);
    }
}

/// Owned batch and all originals. Retain it across retries until terminal settlement.
/// Dropping it releases volatile tracking, NOT durable custody or broker settlement.
pub struct Batch<T> {
    key: BatchKey,
    members: Vec<Member<T>>,
    pending: Option<Arc<AtomicUsize>>,
}

impl<T> Batch<T> {
    /// Typed handler view with trusted identity, visible input and all contexts.
    pub fn view(&self) -> BatchView<'_, T> {
        BatchView::new(&self.key, &self.members)
    }
    /// Trusted tenant/group boundary.
    pub fn key(&self) -> &BatchKey {
        &self.key
    }
    /// All originals in arrival order, including coalesced-away values.
    pub fn members(&self) -> &[Member<T>] {
        &self.members
    }
    /// Last-value-wins input, ordered by each survivor's original position.
    /// A custom shape is an ordinary typed transformation of these references.
    pub fn input(&self) -> Vec<&Member<T>> {
        survivors(&self.members)
    }
    /// Latest expiry only when every original has a finite expiry.
    pub fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.members
            .iter()
            .map(|member| member.context.expires_at)
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .max()
    }
    /// Whole-batch expiry after assembly; partially expired originals may still execute.
    pub fn expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at().is_some_and(|expiry| expiry <= now)
    }
}

impl<T> Drop for Batch<T> {
    fn drop(&mut self) {
        if let Some(pending) = &self.pending {
            pending.fetch_sub(self.members.len(), Ordering::AcqRel);
        }
    }
}

/// Borrowed typed handler input. All originals are distinct from coalesced values.
pub struct BatchView<'a, T> {
    key: &'a BatchKey,
    originals: &'a [Member<T>],
}

impl<'a, T> BatchView<'a, T> {
    pub(crate) fn new(key: &'a BatchKey, originals: &'a [Member<T>]) -> Self {
        Self { key, originals }
    }
    /// Trusted identity carried by custody, never inferred from wire headers.
    pub fn key(&self) -> &'a BatchKey {
        self.key
    }
    /// Every original custody participant, including coalesced-away contexts.
    pub fn originals(&self) -> &'a [Member<T>] {
        self.originals
    }
    /// Last-value-wins application input in survivor arrival order.
    pub fn input(&self) -> Vec<&'a Member<T>> {
        survivors(self.originals)
    }
}

pub(crate) fn survivors<T>(members: &[Member<T>]) -> Vec<&Member<T>> {
    members
        .iter()
        .enumerate()
        .filter(|(index, member)| {
            member.coalesce.as_ref().is_none_or(|key| {
                !members[index + 1..]
                    .iter()
                    .any(|later| later.coalesce.as_ref() == Some(key))
            })
        })
        .map(|(_, member)| member)
        .collect()
}

/// Owned typed projection of canonical envelopes, not a second custody owner.
/// Settlement continues to use the original execution and delivery identities.
pub struct DecodedBatch<T> {
    key: BatchKey,
    originals: Vec<Member<T>>,
}

impl<T> DecodedBatch<T> {
    /// Typed custom-shape input and all original contexts.
    pub fn view(&self) -> BatchView<'_, T> {
        BatchView::new(&self.key, &self.originals)
    }
}

impl BatchView<'_, raddle::wire::Envelope> {
    /// Decodes a concrete wire contract, retaining independent owned context for
    /// every original. A mismatch fails without changing custody; use canonical
    /// ingress upcasting before staging rather than mutating inbox content here.
    pub fn decode<T: raddle::wire::WireMessage>(
        &self,
    ) -> Result<DecodedBatch<T>, raddle::wire::WireError> {
        let originals = self
            .originals
            .iter()
            .map(|member| {
                Ok(Member {
                    context: member.context.clone(),
                    value: member.value.decode::<T>()?,
                    coalesce: member.coalesce.clone(),
                })
            })
            .collect::<Result<Vec<_>, raddle::wire::WireError>>()?;
        Ok(DecodedBatch {
            key: self.key.clone(),
            originals,
        })
    }
}
