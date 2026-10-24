use std::time::Duration;

use chrono::{DateTime, Utc};
use raddle::wire::Envelope;
use uuid::Uuid;

use crate::Error;

/// Whether work executes locally or must be confirmed by a remote transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DeliveryKind {
    /// A named local subscription.
    Local,
    /// An outbox destination alias.
    Remote,
}

/// Whether a failed attempt can run again.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FailureDisposition {
    /// Reschedule unless the attempt budget is exhausted.
    Retry,
    /// Immediately dead-letter the delivery.
    Reject,
}

impl DeliveryKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

/// One independently retried destination, resolved before transaction commit.
#[derive(Clone, Debug)]
pub struct Delivery {
    /// Queue kind.
    pub kind: DeliveryKind,
    /// Stable subscriber or configured remote alias.
    pub destination: String,
    /// Admission time; future values schedule durable delivery.
    pub available_at: DateTime<Utc>,
    /// Optional deadline after which work is dead-lettered without dispatch.
    pub expires_at: Option<DateTime<Utc>>,
    /// Optional key serializes live local executions in visible enqueue order.
    pub partition_key: Option<String>,
    /// Maximum execution or publication attempts, from 1 through 1000.
    pub max_attempts: u32,
}

impl Delivery {
    /// Constructs an immediately eligible delivery with five attempts.
    pub fn new(kind: DeliveryKind, destination: impl Into<String>, now: DateTime<Utc>) -> Self {
        Self {
            kind,
            destination: destination.into(),
            available_at: now,
            expires_at: None,
            partition_key: None,
            max_attempts: 5,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        raddle::SubscriptionName::try_new(self.destination.clone())
            .map_err(|_| Error::Invalid("invalid destination alias"))?;
        if !(1..=1000).contains(&self.max_attempts) {
            return Err(Error::Invalid("attempt limit outside 1..=1000"));
        }
        if self
            .partition_key
            .as_ref()
            .is_some_and(|key| key.is_empty() || key.len() > 256)
        {
            return Err(Error::Invalid("invalid partition key"));
        }
        Ok(())
    }
}

/// Stable, persisted job state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JobStatus {
    /// Waiting until its due time.
    Ready,
    /// Claimed by an execution owner.
    Active,
    /// Completed successfully; retained for duplicate detection.
    Completed,
    /// Rejected, expired, or exhausted.
    Dead,
}

/// A redacted operator view of a job; excludes its payload and authorization data.
#[derive(Clone, Debug)]
pub struct Job {
    /// Internal delivery identity.
    pub id: Uuid,
    /// Stable wire message identity.
    pub message_id: Uuid,
    /// Current state.
    pub status: JobStatus,
    /// Executions or publication attempts already claimed.
    pub attempts: u32,
    /// Most recent error classification, never a formatted handler error.
    pub last_error: Option<String>,
}

/// A leased claim. Fields are private to prevent forging a valid ownership token.
#[derive(Clone)]
pub struct Claim {
    pub(crate) id: Uuid,
    pub(crate) owner: Uuid,
    pub(crate) generation: i64,
    pub(crate) envelope: Vec<u8>,
    pub(crate) tenant: String,
    pub(crate) destination: String,
    pub(crate) attempts: i32,
    pub(crate) max_attempts: i32,
}

impl std::fmt::Debug for Claim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Claim")
            .field("id", &self.id)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl Claim {
    /// Delivery identity, distinct from message identity during fan-out.
    pub fn id(&self) -> Uuid {
        self.id
    }
    /// Authenticated tenant supplied at custody, never inferred from envelope headers.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    /// Resolved destination alias.
    pub fn destination(&self) -> &str {
        &self.destination
    }
    /// Validates stored framing before dispatch.
    pub fn envelope(&self) -> Result<Envelope, Error> {
        Ok(Envelope::from_slice(&self.envelope, Default::default())?)
    }
}

/// Bounded deterministic exponential backoff and execution time budget.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    base: Duration,
    maximum: Duration,
    timeout: Duration,
}

impl RetryPolicy {
    /// Validates positive durations and a bounded scheduling horizon.
    pub fn try_new(base: Duration, maximum: Duration, timeout: Duration) -> Result<Self, Error> {
        if base.is_zero()
            || maximum < base
            || maximum > Duration::from_secs(86400 * 30)
            || timeout.is_zero()
            || timeout > Duration::from_secs(3600)
        {
            return Err(Error::Invalid("invalid retry durations"));
        }
        Ok(Self {
            base,
            maximum,
            timeout,
        })
    }

    /// Maximum duration of one execution attempt.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) fn delay(&self, claim: &Claim) -> Duration {
        let multiplier = 1u32 << (claim.attempts.saturating_sub(1).min(20) as u32);
        let base = self.base.saturating_mul(multiplier).min(self.maximum);
        let jitter = u64::from(claim.id.as_bytes()[0]) * base.as_millis() as u64 / 1024;
        base.saturating_add(Duration::from_millis(jitter))
            .min(self.maximum)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            maximum: Duration::from_secs(300),
            timeout: Duration::from_secs(30),
        }
    }
}
