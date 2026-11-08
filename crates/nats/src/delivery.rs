use std::time::{Duration, Instant};

use async_nats::jetstream::message::{AckKind, Message, PublishMessage};
use raddle::wire::{Envelope, WireError};

use crate::{Error, JetStream, Receipt, Source};

/// Stable native identity; stream sequence survives redelivery independently of envelope parsing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Position {
    /// Stream name.
    pub stream: String,
    /// Durable consumer.
    pub consumer: String,
    /// Original stream sequence.
    pub sequence: u64,
    /// Broker's delivery attempt count.
    pub attempt: i64,
}

/// Private broker acknowledgement ownership retained even when decoding fails.
/// Drop does not acknowledge; broker redelivers after AckWait.
pub struct HeldMessage {
    message: Message,
    jetstream: JetStream,
    source: Source,
    position: Position,
    deadline: Instant,
    ceiling: Instant,
    ack_wait: Duration,
    settled: bool,
}
impl HeldMessage {
    pub(crate) fn new(
        message: Message,
        jetstream: JetStream,
        source: Source,
        ack_wait: Duration,
        max_extension: Duration,
        requested_at: Instant,
    ) -> Result<Self, Error> {
        let info = message
            .info()
            .map_err(|_| Error::Invalid("JetStream metadata"))?;
        let position = Position {
            stream: info.stream.to_owned(),
            consumer: info.consumer.to_owned(),
            sequence: info.stream_sequence,
            attempt: info.delivered,
        };
        // This request uses a fresh SDK inbox and collects for at most half AckWait.
        // A delivery cannot predate its request; SDK dequeue time alone is not a safe
        // lease anchor. Long empty waits use later requests, not this same timestamp.
        Ok(Self {
            message,
            jetstream,
            source,
            position,
            deadline: requested_at + ack_wait,
            ceiling: requested_at + max_extension,
            ack_wait,
            settled: false,
        })
    }
    /// Validates identity, schema, headers and byte size without surrendering the receipt.
    pub fn decode(&self) -> Result<Envelope, WireError> {
        self.owned()
            .map_err(|_| WireError::Invalid("delivery lease expired or settled"))?;
        if let Some(headers) = &self.message.headers {
            crate::config::headers(headers, self.jetstream.core.bounds.envelope.max_headers)
                .map_err(|_| WireError::Invalid("native headers"))?;
        }
        Envelope::from_slice(&self.message.payload, self.jetstream.core.bounds.envelope)
    }
    /// Bounded raw native body for an application-owned custom mapper.
    pub fn body(&self) -> Result<&[u8], WireError> {
        self.owned()
            .map_err(|_| WireError::Invalid("delivery lease expired or settled"))?;
        if self.message.payload.len() > self.jetstream.core.bounds.envelope.max_bytes {
            return Err(WireError::Invalid("body limit"));
        }
        Ok(&self.message.payload)
    }
    /// Native headers are untrusted data; only Source supplies authenticated tenant context.
    pub fn headers(&self) -> Option<&async_nats::HeaderMap> {
        self.message.headers.as_ref()
    }
    /// Invokes a custom raw/foreign mapper while preserving the held acknowledgement and
    /// validating its resulting envelope. Native position can seed a stable foreign identity.
    pub fn decode_with<F>(&self, mapper: F) -> Result<Envelope, WireError>
    where
        F: FnOnce(&[u8]) -> Result<Envelope, WireError>,
    {
        if let Some(headers) = &self.message.headers {
            crate::config::headers(headers, self.jetstream.core.bounds.envelope.max_headers)
                .map_err(|_| WireError::Invalid("native headers"))?;
        }
        let envelope = mapper(self.body()?)?;
        envelope.validate(self.jetstream.core.bounds.envelope)?;
        Ok(envelope)
    }
    /// Broker identity for diagnostics, poison handling and replay.
    pub fn position(&self) -> &Position {
        &self.position
    }
    /// Application-trusted tenant/source; no received tenant header is trusted.
    pub fn source(&self) -> &Source {
        &self.source
    }
    /// Conservative local admission check for queued work. It cannot fence a remote
    /// consumer deletion/race or arbitrary business side effects.
    pub fn is_owned(&self) -> bool {
        self.owned().is_ok()
    }
    fn owned(&self) -> Result<(), Error> {
        if self.settled || Instant::now() >= self.deadline || Instant::now() >= self.ceiling {
            return Err(Error::Lost);
        }
        Ok(())
    }
    /// Extends a queued or executing receipt using confirmed native AckProgress, up to its ceiling.
    /// The caller must invoke this before AckWait, including while work is queued.
    pub async fn progress(&mut self) -> Result<(), Error> {
        self.owned()?;
        let started = Instant::now();
        // Cancellation/uncertainty invalidates local ownership until broker redelivery.
        self.settled = true;
        self.jetstream
            .acknowledge(&self.message, AckKind::Progress)
            .await?;
        self.settled = false;
        self.deadline = (started + self.ack_wait).min(self.ceiling);
        self.owned()
    }
    /// Double-acknowledges after successful handling or committed durable inbox custody.
    /// This is not itself proof that a database transaction committed.
    pub async fn complete(&mut self) -> Result<(), Error> {
        self.settle(AckKind::Ack).await
    }
    /// Requests native delayed redelivery; stable stream/envelope identity is preserved.
    pub async fn retry(&mut self, delay: Option<Duration>) -> Result<(), Error> {
        if delay.is_some_and(|d| d > Duration::from_secs(86_400)) {
            return Err(Error::Invalid("retry delay"));
        }
        self.settle(AckKind::Nak(delay)).await
    }
    /// Explicitly discards without pretending any retained DLQ copy exists.
    pub async fn terminate_without_retention(&mut self) -> Result<(), Error> {
        self.settle(AckKind::Term).await
    }
    /// Confirms a raw copy to an allowlisted DLQ route before terminating the source.
    /// A failed copy retains source ownership; a cancellation can leave a duplicate copy.
    pub async fn dead_letter(&mut self, alias: &str) -> Result<Receipt, Error> {
        self.owned()?;
        let subject = self
            .jetstream
            .core
            .routes
            .get(alias)
            .ok_or(Error::Invalid("DLQ alias"))?;
        if subject.as_str() == self.message.subject.as_str() {
            return Err(Error::Invalid("DLQ must differ from source"));
        }
        if self.message.payload.len() > self.jetstream.core.bounds.envelope.max_bytes {
            return Err(Error::Invalid("DLQ payload too large"));
        }
        let message = PublishMessage::build()
            .payload(self.message.payload.clone())
            .message_id(crate::config::dedup_key(
                &self.source,
                "dlq",
                &[
                    subject,
                    &self.position.stream,
                    &self.position.consumer,
                    &self.position.sequence.to_string(),
                ],
            ))
            .header("Raddle-Source-Stream", self.position.stream.as_str())
            .header("Raddle-Source-Sequence", self.position.sequence.to_string())
            .header("Raddle-Delivery-Attempt", self.position.attempt.to_string());
        let receipt = self.jetstream.publish_message(subject, message).await?;
        self.settle(AckKind::Term).await?;
        Ok(receipt)
    }
    async fn settle(&mut self, kind: AckKind) -> Result<(), Error> {
        self.owned()?;
        // Invalidate before I/O: a cancelled settlement cannot be reused as certain ownership.
        self.settled = true;
        self.jetstream.acknowledge(&self.message, kind).await
    }
}
