/// Payload-free native Kafka outcomes. Unknown delivery is never confirmed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid local limits, route or native client configuration.
    #[error("invalid Kafka configuration")]
    Configuration,
    /// The message exceeds a configured bound or is structurally invalid.
    #[error("Kafka record refused")]
    Rejected,
    /// A required pre-existing topic was not visible to this client.
    #[error("configured Kafka topic is unavailable")]
    MissingTopic,
    /// Acceptance or commit was not positively observed; retain original identity.
    #[error("Kafka outcome is uncertain")]
    Uncertain,
    /// Local admission is full. Nothing new was enqueued by this call.
    #[error("Kafka admission capacity exhausted")]
    Capacity,
    /// The original partition assignment no longer owns this delivery.
    #[error("Kafka partition ownership changed")]
    Stale,
    /// The native client or its cleanup worker did not complete normally.
    #[error("Kafka native client operation failed")]
    Native,
    /// The safe native API reported an opaque receiving failure. Recreate only
    /// after explicit recovery; later records must not commit past unknown loss.
    #[error("Kafka receiving is fenced after an opaque native failure")]
    ReceiverFenced,
}
