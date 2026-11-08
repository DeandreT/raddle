//! Durable batches over `raddle_jobs`, not a second inbox or a claim-page alias.
//!
//! Stage canonical plaintext envelopes using `Scope::stage_received` first.
//! Reserve dedicated destinations exclusively for this adapter: ordinary workers
//! must not execute their jobs. Originals use an infinite internal job lease while
//! the separately leased batch row fences execution and recovery. This is not a
//! promise of sequencing against other destinations.

mod execution;
mod store;
mod tracking;
mod worker;

use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

pub use execution::{Execution, SettlementReport};
pub use store::BatchStore;
pub use tracking::{TrackedOutcome, TrackingSession};
pub use worker::{HandlerFuture, Isolation, RunReport};

/// Adapter failures never imply that an uncertain commit rolled back.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// SQLx statement/connection failure before commit.
    #[error("batch database operation failed")]
    Database(#[from] sqlx::Error),
    /// Commit was sent; reconcile canonical job identities before retrying effects.
    #[error("batch commit outcome is uncertain; reconcile original job identities")]
    CommitUncertain(#[source] raddle_postgres::Error),
    /// Assembly/claim commit was sent; its durable identity may already exist.
    #[error("batch custody commit outcome is uncertain; reconcile durable state")]
    CustodyUncertain(#[source] sqlx::Error),
    /// Explicit bounds, queue or persisted canonical envelope was invalid.
    #[error("invalid batch operation: {0}")]
    Invalid(&'static str),
    /// Lease expired, ownership changed, or the batch is already terminal.
    #[error("stale batch claim")]
    Stale,
    /// Pending one-time probes must finish before grouped retry resumes.
    #[error("batch requires individual probe continuation")]
    Probing,
    /// The whole assembled batch expired before execution and its remaining
    /// originals were durably marked dead. No handler ran.
    #[error("assembled batch expired before execution")]
    Expired,
    /// Explicit order-level preparation refusal; assembly quarantines only this
    /// original. Use Invalid instead for a genuinely shared configuration error.
    #[error("batch member preparation refused: {0}")]
    InvalidMember(&'static str),
    /// A resolution does not describe precisely this execution's originals.
    #[error("invalid member resolution")]
    Resolution(#[from] crate::ResolutionError),
    /// Canonical wire data is malformed; original custody remains pending.
    #[error("invalid canonical envelope")]
    Wire(#[from] raddle::wire::WireError),
    /// Waiting does not settle or cancel any original; custody is still pending.
    #[error("tracked batch settlement deadline expired")]
    TrackingTimeout,
}

/// Owned fencing token. Cloning a token never creates another execution right.
#[derive(Clone, Debug)]
pub struct BatchClaim {
    id: Uuid,
    owner: Uuid,
    generation: i64,
    tenant: String,
    attempts: u32,
}

impl BatchClaim {
    /// Stable synthetic batch identity (original identities remain separate).
    pub fn id(&self) -> Uuid {
        self.id
    }
    /// Trusted tenant carried by durable custody, not message headers.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    /// Whole-batch claim count, distinct from original member attempts.
    pub fn attempts(&self) -> u32 {
        self.attempts
    }
}

fn until(now: DateTime<Utc>, duration: Duration) -> Result<DateTime<Utc>, Error> {
    now.checked_add_signed(
        chrono::Duration::from_std(duration).map_err(|_| Error::Invalid("duration"))?,
    )
    .ok_or(Error::Invalid("timestamp overflow"))
}

fn execution_timeout(timeout: Duration) -> Result<i64, Error> {
    if !(Duration::from_millis(1)..=Duration::from_secs(3600)).contains(&timeout) {
        return Err(Error::Invalid("execution timeout must be 1ms..=1h"));
    }
    i64::try_from(timeout.as_millis()).map_err(|_| Error::Invalid("execution timeout"))
}
