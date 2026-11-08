use chrono::{DateTime, Utc};
use raddle::wire::Envelope;
use uuid::Uuid;

/// SQLite persistence and validation failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// SQLx database failure.
    #[error("SQLite operation failed")]
    Database(#[from] sqlx::Error),
    /// Versioned schema migration failed.
    #[error("SQLite migration failed")]
    Migration(#[from] sqlx::migrate::MigrateError),
    /// Invalid wire message.
    #[error("wire message validation failed")]
    Wire(#[from] raddle::wire::WireError),
    /// Invalid bounds or metadata.
    #[error("invalid SQLite messaging configuration: {0}")]
    Invalid(&'static str),
    /// Existing identity has different immutable content.
    #[error("message identity conflict")]
    IdentityConflict,
    /// Transaction encountered a staging failure.
    #[error("transaction is rollback-only")]
    RollbackOnly,
    /// Another worker took over this claim.
    #[error("claim is no longer owned")]
    StaleClaim,
    /// The configured tenant budget is full.
    #[error("durable queue capacity reached")]
    Capacity,
}

/// Whether a delivery runs locally or through a confirmed remote transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DeliveryKind {
    /// Local subscriber work.
    Local,
    /// Outgoing broker delivery.
    Remote,
}
impl DeliveryKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

/// Retry or terminal rejection, without persisting arbitrary error strings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FailureDisposition {
    /// Retry with bounded exponential backoff.
    Retry,
    /// Dead-letter immediately.
    Reject,
}

/// A pre-resolved destination and its delivery policy.
#[derive(Clone, Debug)]
pub struct Delivery {
    /// Local or remote destination.
    pub kind: DeliveryKind,
    /// Stable subscriber or endpoint alias.
    pub destination: String,
    /// Durable due time.
    pub available_at: DateTime<Utc>,
    /// Optional expiry time.
    pub expires_at: Option<DateTime<Utc>>,
    /// Optional serialized processing key.
    pub partition_key: Option<String>,
    /// Maximum attempts, including abandoned claims.
    pub max_attempts: u32,
}
impl Delivery {
    /// Constructs a delivery with five attempts.
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
            .map_err(|_| Error::Invalid("invalid destination"))?;
        if !(1..=1000).contains(&self.max_attempts)
            || self
                .partition_key
                .as_ref()
                .is_some_and(|key| key.is_empty() || key.len() > 256)
        {
            return Err(Error::Invalid("invalid delivery bounds"));
        }
        Ok(())
    }
}

/// Payload-free operator view.
#[derive(Clone, Debug)]
pub struct Job {
    /// Internal delivery ID.
    pub id: Uuid,
    /// Current lifecycle state.
    pub status: String,
    /// Number of claimed attempts.
    pub attempts: u32,
}

/// A private-generation ownership token for one delivery.
#[derive(Clone)]
pub struct Claim {
    pub(crate) id: String,
    pub(crate) generation: i64,
    pub(crate) bytes: Vec<u8>,
    pub(crate) destination: String,
    pub(crate) tenant: String,
    pub(crate) attempts: i64,
    pub(crate) max_attempts: i64,
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
    /// Internal delivery ID.
    pub fn id(&self) -> Result<Uuid, Error> {
        Uuid::parse_str(&self.id).map_err(|_| Error::Invalid("corrupt job ID"))
    }
    /// Authenticated tenant supplied at custody.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    /// Original resolved destination.
    pub fn destination(&self) -> &str {
        &self.destination
    }
    /// Validates the original stored envelope.
    pub fn envelope(&self) -> Result<Envelope, Error> {
        Ok(Envelope::from_slice(&self.bytes, Default::default())?)
    }
}
