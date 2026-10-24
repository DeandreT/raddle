/// Redacted transport/control failure. Credentials and message bodies are never formatted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid local configuration or operation.
    #[error("invalid Service Bus operation: {0}")]
    Invalid(&'static str),
    /// A connection, link, or settlement operation failed or had an uncertain outcome.
    #[error("Service Bus operation failed; inspect transport health and retry policy")]
    Service,
    /// The connection was explicitly closed.
    #[error("Service Bus connection is closed")]
    Closed,
}
