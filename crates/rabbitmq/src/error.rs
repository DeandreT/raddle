/// Redacted broker or configuration failure; connection strings are never included.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid transport configuration or bounds.
    #[error("invalid RabbitMQ configuration: {0}")]
    Invalid(&'static str),
    /// Broker, network, or settlement operation failed.
    #[error("RabbitMQ operation failed")]
    Broker,
    /// Consumer stream ended.
    #[error("RabbitMQ consumer closed")]
    Closed,
}
