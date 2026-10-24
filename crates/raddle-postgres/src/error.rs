/// Durable storage and ownership failures. Errors never contain message bodies.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An underlying SQLx operation failed.
    #[error("database operation failed")]
    Database(#[from] sqlx::Error),
    /// Schema migration failed.
    #[error("database migration failed")]
    Migration(#[from] sqlx::migrate::MigrateError),
    /// Wire envelope validation failed.
    #[error("wire message failed validation")]
    Wire(#[from] raddle::wire::WireError),
    /// Configuration or bounded metadata was invalid.
    #[error("invalid durable execution configuration: {0}")]
    Invalid(&'static str),
    /// A reused identity carried different immutable content.
    #[error("conflicting content for an existing message identity")]
    IdentityConflict,
    /// The worker no longer owns the claimed generation.
    #[error("claim was lost or expired")]
    StaleClaim,
    /// A failed operation made this transaction unsafe to commit.
    #[error("transaction is rollback-only")]
    RollbackOnly,
    /// Accepting more pending work would exceed the configured tenant budget.
    #[error("durable queue capacity reached")]
    Capacity,
}
