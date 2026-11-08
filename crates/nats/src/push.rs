use std::time::Duration;

use async_nats::jetstream::consumer::{AckPolicy, Consumer, push};
use async_nats::jetstream::message::{AckKind, Message, PublishMessage};
use futures_util::StreamExt;
use raddle::wire::{Envelope, WireError};

use crate::{Error, JetStream, Position, Provision, Receipt, Source};

/// Native flow-controlled push source for buffered or durable-inbox custody.
/// This API deliberately does not expose a native execution-lease promise: a message
/// may have waited in an SDK buffer before the caller polls it. Use pull receipts
/// when admission requires a conservative local lease deadline and AckProgress.
pub struct PushReceiver {
    messages: push::Messages,
    jetstream: JetStream,
    source: Source,
}
impl JetStream {
    /// Creates/verifies a flow-controlled push consumer with explicit ack and a finite
    /// broker window. Queue push with flow control is refused; Core queue groups and
    /// JetStream pull workers have separate APIs.
    pub async fn push_receiver(
        &self,
        stream: &str,
        config: push::Config,
        mode: Provision,
        source: Source,
    ) -> Result<PushReceiver, Error> {
        crate::jetstream::stream_name(stream)?;
        source.validate()?;
        crate::config::metadata(&config.metadata)?;
        crate::config::validate_subject(&config.deliver_subject, false)?;
        if config.ack_policy != AckPolicy::Explicit
            || !config.flow_control
            || config.idle_heartbeat < Duration::from_millis(100)
            || config.ack_wait.is_zero()
            || config.ack_wait > Duration::from_secs(3600)
            || config.max_ack_pending < 1
            || config.max_ack_pending > i64::from(self.core.bounds.prefetch)
            || config.deliver_group.is_some()
            || config.filter_subjects.len() > 256
            || config.headers_only
        {
            return Err(Error::Invalid("push flow/ack bounds"));
        }
        if !config.filter_subject.is_empty() {
            crate::config::validate_subject(&config.filter_subject, true)?;
        }
        for filter in &config.filter_subjects {
            crate::config::validate_subject(filter, true)?;
        }
        let name = config.durable_name.as_deref().or(config.name.as_deref());
        if let Some(name) = name {
            crate::jetstream::stream_name(name)?;
        }
        if name.is_none() && mode != Provision::Create {
            return Err(Error::Invalid(
                "named push consumer required for verify/update",
            ));
        }
        let messages = tokio::time::timeout(self.core.bounds.operation_timeout, async {
            let stream = self
                .context
                .get_stream(stream)
                .await
                .map_err(|_| Error::Uncertain)?;
            let consumer: Consumer<push::Config> = match mode {
                Provision::Verify => stream
                    .get_consumer(name.unwrap_or_default())
                    .await
                    .map_err(|_| Error::Uncertain),
                Provision::Create if name.is_some() => stream
                    .get_or_create_consumer(name.unwrap_or_default(), config.clone())
                    .await
                    .map_err(|_| Error::Uncertain),
                _ => stream
                    .create_consumer(config.clone())
                    .await
                    .map_err(|_| Error::Uncertain),
            }?;
            let actual = &consumer.cached_info().config;
            if actual.ack_policy != config.ack_policy
                || actual.ack_wait != config.ack_wait
                || actual.max_ack_pending != config.max_ack_pending
                || actual.deliver_subject.as_deref() != Some(config.deliver_subject.as_str())
                || actual.filter_subject != config.filter_subject
                || actual.filter_subjects != config.filter_subjects
                || actual.flow_control != config.flow_control
                || actual.idle_heartbeat != config.idle_heartbeat
                || actual.deliver_group != config.deliver_group
                || actual.durable_name != config.durable_name
                || actual.headers_only != config.headers_only
                || actual.backoff != config.backoff
                || actual.deliver_policy != config.deliver_policy
                || actual.replay_policy != config.replay_policy
                || actual.rate_limit != config.rate_limit
                || actual.sample_frequency != config.sample_frequency
                || (config.max_deliver != 0 && actual.max_deliver != config.max_deliver)
                || (!config.inactive_threshold.is_zero()
                    && actual.inactive_threshold != config.inactive_threshold)
                || (config.num_replicas != 0 && actual.num_replicas != config.num_replicas)
                || (config.memory_storage && !actual.memory_storage)
                || !crate::config::metadata_matches(&config.metadata, &actual.metadata)
                || (config.description.is_some() && actual.description != config.description)
                || (config.pause_until.is_some() && actual.pause_until != config.pause_until)
            {
                return Err(Error::Drift);
            }
            consumer.messages().await.map_err(|_| Error::Uncertain)
        })
        .await
        .map_err(|_| Error::Uncertain)??;
        Ok(PushReceiver {
            messages,
            jetstream: self.clone(),
            source,
        })
    }
}
impl PushReceiver {
    /// Polls one total bounded page, counting skipped scheduler controls against its limit.
    /// The SDK responds to flow-control and idle-heartbeat traffic while being polled.
    pub async fn receive(
        &mut self,
        limit: usize,
        wait: Duration,
    ) -> Result<Vec<PushDelivery>, Error> {
        crate::config::wait(wait)?;
        if limit == 0 || limit > usize::from(self.jetstream.core.bounds.prefetch) {
            return Err(Error::Invalid("push page limit"));
        }
        let deadline = tokio::time::Instant::now() + wait;
        let mut result = Vec::with_capacity(limit);
        for _ in 0..limit {
            match tokio::time::timeout_at(deadline, self.messages.next()).await {
                Err(_) | Ok(None) => break,
                Ok(Some(Err(_))) if result.is_empty() => return Err(Error::Uncertain),
                Ok(Some(Err(_))) => break,
                Ok(Some(Ok(message))) => {
                    if message.headers.as_ref().is_some_and(|h| {
                        h.get("Nats-Schedule").is_some() && h.get("Nats-Scheduler").is_none()
                    }) {
                        let _ = tokio::time::timeout_at(
                            deadline,
                            self.jetstream.acknowledge(&message, AckKind::Ack),
                        )
                        .await;
                        continue;
                    }
                    let info = message
                        .info()
                        .map_err(|_| Error::Invalid("push metadata"))?;
                    let position = Position {
                        stream: info.stream.to_owned(),
                        consumer: info.consumer.to_owned(),
                        sequence: info.stream_sequence,
                        attempt: info.delivered,
                    };
                    result.push(PushDelivery {
                        message,
                        jetstream: self.jetstream.clone(),
                        source: self.source.clone(),
                        position,
                        settled: false,
                    });
                }
            }
        }
        Ok(result)
    }
}

/// Source-held push receipt, with no invented clock-based lease guarantee.
/// Drop leaves it unacknowledged. Only acknowledge after durable custody/handling;
/// SDK buffering means duplicates can already be running elsewhere after AckWait.
pub struct PushDelivery {
    message: Message,
    jetstream: JetStream,
    source: Source,
    position: Position,
    settled: bool,
}
impl PushDelivery {
    /// Bounded raddle decoding; invalid data retains acknowledgement ownership.
    pub fn decode(&self) -> Result<Envelope, WireError> {
        self.decode_with(|body| Envelope::from_slice(body, self.jetstream.core.bounds.envelope))
    }
    /// Custom bounded mapping preserves the push receipt after mapper failure.
    pub fn decode_with<F>(&self, mapper: F) -> Result<Envelope, WireError>
    where
        F: FnOnce(&[u8]) -> Result<Envelope, WireError>,
    {
        if self.settled {
            return Err(WireError::Invalid("settled push receipt"));
        }
        if let Some(headers) = &self.message.headers {
            crate::config::headers(headers, self.jetstream.core.bounds.envelope.max_headers)
                .map_err(|_| WireError::Invalid("native headers"))?;
        }
        if self.message.payload.len() > self.jetstream.core.bounds.envelope.max_bytes {
            return Err(WireError::Invalid("body limit"));
        }
        let envelope = mapper(&self.message.payload)?;
        envelope.validate(self.jetstream.core.bounds.envelope)?;
        Ok(envelope)
    }
    /// Native identity remains stable across redelivery.
    pub fn position(&self) -> &Position {
        &self.position
    }
    /// Trusted application identity.
    pub fn source(&self) -> &Source {
        &self.source
    }
    /// Double-acknowledges the broker source after the caller's custody boundary.
    pub async fn complete(&mut self) -> Result<(), Error> {
        self.settle(AckKind::Ack).await
    }
    /// Native NAK, not acknowledgement-plus-republish.
    pub async fn retry(&mut self, delay: Option<Duration>) -> Result<(), Error> {
        if delay.is_some_and(|d| d > Duration::from_secs(86_400)) {
            return Err(Error::Invalid("retry delay"));
        }
        self.settle(AckKind::Nak(delay)).await
    }
    /// Explicit discard, without any retained DLQ claim.
    pub async fn terminate_without_retention(&mut self) -> Result<(), Error> {
        self.settle(AckKind::Term).await
    }
    /// Confirms an allowlisted raw DLQ copy before terminating this consumer's source.
    /// Copy/TERM is not a transaction and interruption can duplicate a DLQ copy.
    pub async fn dead_letter(&mut self, alias: &str) -> Result<Receipt, Error> {
        if self.settled {
            return Err(Error::Lost);
        }
        let target = self
            .jetstream
            .core
            .routes
            .get(alias)
            .ok_or(Error::Invalid("DLQ alias"))?;
        if target.as_str() == self.message.subject.as_str()
            || self.message.payload.len() > self.jetstream.core.bounds.envelope.max_bytes
        {
            return Err(Error::Invalid("DLQ target/body"));
        }
        let message = PublishMessage::build()
            .payload(self.message.payload.clone())
            .message_id(crate::config::dedup_key(
                &self.source,
                "dlq",
                &[
                    target,
                    &self.position.stream,
                    &self.position.consumer,
                    &self.position.sequence.to_string(),
                ],
            ))
            .header("Raddle-Source-Stream", self.position.stream.as_str())
            .header("Raddle-Source-Sequence", self.position.sequence.to_string())
            .header("Raddle-Delivery-Attempt", self.position.attempt.to_string());
        let receipt = self.jetstream.publish_message(target, message).await?;
        self.settle(AckKind::Term).await?;
        Ok(receipt)
    }
    async fn settle(&mut self, kind: AckKind) -> Result<(), Error> {
        if self.settled {
            return Err(Error::Lost);
        }
        self.settled = true;
        self.jetstream.acknowledge(&self.message, kind).await
    }
}
