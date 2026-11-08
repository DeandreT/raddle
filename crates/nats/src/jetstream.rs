use std::time::Duration;

use async_nats::jetstream::consumer::{AckPolicy, Consumer, pull};
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::{Context, ContextBuilder};
use futures_util::StreamExt;
use raddle::transport::{Transport, TransportError};
use raddle::wire::Envelope;

use crate::config;
use crate::{Core, Error, HeldMessage, Source, StreamConfig};

/// Explicit resource ownership/provisioning policy. Verify never creates or updates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Provision {
    /// Create only; existing compatible resources are accepted, drift is refused.
    Create,
    /// Application owns the resource and authorizes an update.
    Update,
    /// Externally owned; verify properties without mutation.
    Verify,
}

/// Optional broker conditional publication. Deduplication requires an explicit trusted scope.
#[derive(Clone, Debug, Default)]
pub struct PublishOptions {
    /// Bounded application/native headers. Explicit Nats-Msg-Id takes dedup precedence.
    /// These are application-trusted options, never automatically propagated from inbound data.
    pub headers: async_nats::HeaderMap,
    /// Application-owned logical deduplication override, scoped by configured Source and
    /// publication subject. A publisher without a trusted scope refuses explicit dedup keys.
    pub deduplication_id: Option<String>,
    /// Refuse a different matching stream.
    pub expected_stream: Option<String>,
    /// Optimistic stream position.
    pub expected_last_sequence: Option<u64>,
    /// Optimistic subject position.
    pub expected_last_subject_sequence: Option<u64>,
}

/// Facts from a JetStream PubAck, not consumer/business completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Receipt {
    /// Stream accepting the publication.
    pub stream: String,
    /// Broker stream position.
    pub sequence: u64,
    /// Account/domain of the acknowledgement.
    pub domain: String,
    /// Broker recognized a duplicate inside its configured deduplication window.
    pub duplicate: bool,
}

/// A scheduling capability decision. Fallback is an instruction to the caller, not a send.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ScheduleResult {
    /// Broker stored a native scheduling entry.
    Native(Receipt),
    /// Caller must persist a schedule in its durable scheduler before reporting success.
    RequiresDurableScheduler,
}

/// Application-trusted native scheduling subject layout.
#[derive(Clone, Debug)]
pub struct ScheduleOptions {
    /// Dot-prefixed suffix before the envelope ID, default `.scheduled`. Both the
    /// resulting control subject and target must be captured by the same stream.
    pub subject_suffix: String,
}
impl Default for ScheduleOptions {
    fn default() -> Self {
        Self {
            subject_suffix: ".scheduled".to_owned(),
        }
    }
}

/// Confirmed JetStream sender; safe for a durable outbox's publication boundary.
#[derive(Clone)]
pub struct JetStream {
    pub(crate) core: Core,
    pub(crate) context: Context,
    dedup_scope: Option<Source>,
}
impl JetStream {
    /// Uses the same socket and a caller-trusted optional JetStream domain.
    pub fn new(core: Core, domain: Option<&str>) -> Result<Self, Error> {
        if let Some(domain) = domain {
            config::name(domain)?;
        }
        let builder = ContextBuilder::new()
            .timeout(core.bounds.operation_timeout)
            .ack_timeout(core.bounds.operation_timeout)
            .max_ack_inflight(usize::from(core.bounds.prefetch));
        let context = match domain {
            Some(domain) => builder.domain(domain).build(core.client.clone()),
            None => builder.build(core.client.clone()),
        };
        Ok(Self {
            core,
            context,
            dedup_scope: None,
        })
    }

    /// Opts into broker deduplication within a frozen application-owned namespace.
    /// Source is configuration, never inferred from Envelope UUID, payload or headers.
    pub fn with_dedup_scope(mut self, source: Source) -> Result<Self, Error> {
        source.validate()?;
        self.dedup_scope = Some(source);
        Ok(self)
    }
    /// Trusted deduplication namespace, absent for ordinary unscoped publishers.
    pub fn dedup_scope(&self) -> Option<&Source> {
        self.dedup_scope.as_ref()
    }

    /// Creates, updates or verifies a stream. Full native retention, replicas, sources,
    /// mirrors and placement configuration are preserved rather than abstracted away.
    pub async fn provision_stream(
        &self,
        config: StreamConfig,
        mode: Provision,
    ) -> Result<(), Error> {
        stream_config(&config)?;
        tokio::time::timeout(self.core.bounds.operation_timeout, async {
            match mode {
                Provision::Update => {
                    self.context
                        .create_or_update_stream(config.clone())
                        .await
                        .map_err(|_| Error::Uncertain)?;
                }
                Provision::Create => {
                    self.context
                        .get_or_create_stream(config.clone())
                        .await
                        .map_err(|_| Error::Uncertain)?;
                }
                Provision::Verify => {}
            }
            let actual = self
                .context
                .get_stream(&config.name)
                .await
                .map_err(|_| Error::Uncertain)?;
            if !stream_matches(&config, &actual.cached_info().config) {
                return Err(Error::Drift);
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::Uncertain)?
    }

    /// Publishes using real PubAck correlation. Cancellation or timeout is uncertain,
    /// and the caller must retry the same envelope identity rather than inventing success.
    pub async fn send(
        &self,
        alias: &str,
        envelope: &Envelope,
        options: PublishOptions,
    ) -> Result<Receipt, Error> {
        let subject = self
            .core
            .routes
            .get(alias)
            .ok_or(Error::Invalid("unknown alias"))?;
        self.send_subject(subject, envelope, options).await
    }

    /// Explicit application-trusted dynamic subject, with the same real PubAck and
    /// deduplication contract as named routes. Never derive this from untrusted wire data.
    pub async fn send_subject(
        &self,
        subject: &str,
        envelope: &Envelope,
        options: PublishOptions,
    ) -> Result<Receipt, Error> {
        config::validate_subject(subject, false)?;
        envelope
            .validate(self.core.bounds.envelope)
            .map_err(|_| Error::Invalid("envelope"))?;
        config::headers(&options.headers, self.core.bounds.envelope.max_headers)?;
        let mut header_dedup = None;
        let mut headers = async_nats::HeaderMap::new();
        for (name, values) in options.headers.iter() {
            let text: &str = name.as_ref();
            if text.eq_ignore_ascii_case("Nats-Msg-Id") {
                for value in values {
                    if header_dedup.replace(value.as_str().to_owned()).is_some() {
                        return Err(Error::Invalid("ambiguous deduplication header"));
                    }
                }
            } else {
                for value in values {
                    headers.append(name.clone(), value.clone());
                }
            }
        }
        let requested_key = header_dedup.or(options.deduplication_id);
        if self.dedup_scope.is_none() && requested_key.is_some() {
            return Err(Error::Invalid("trusted deduplication scope required"));
        }
        if let Some(scope) = &self.dedup_scope {
            let logical_key = requested_key.unwrap_or_else(|| envelope.id().to_string());
            if logical_key.is_empty()
                || logical_key.len() > 128
                || logical_key.bytes().any(|b| b < 32 || b == 127)
            {
                return Err(Error::Invalid("deduplication id"));
            }
            headers.insert(
                async_nats::header::NATS_MESSAGE_ID,
                config::dedup_key(scope, "publish", &[subject, &logical_key]),
            );
        }
        if let Some(stream) = options.expected_stream {
            stream_name(&stream)?;
            headers.insert(async_nats::header::NATS_EXPECTED_STREAM, stream);
        }
        if let Some(seq) = options.expected_last_sequence {
            headers.insert(
                async_nats::header::NATS_EXPECTED_LAST_SEQUENCE,
                seq.to_string(),
            );
        }
        if let Some(seq) = options.expected_last_subject_sequence {
            headers.insert(
                async_nats::header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE,
                seq.to_string(),
            );
        }
        config::headers(&headers, self.core.bounds.envelope.max_headers)?;
        let message = PublishMessage::build().headers(headers).payload(
            envelope
                .to_vec()
                .map_err(|_| Error::Invalid("envelope"))?
                .into(),
        );
        self.publish_message(subject, message).await
    }

    pub(crate) async fn publish_message(
        &self,
        subject: &str,
        message: PublishMessage,
    ) -> Result<Receipt, Error> {
        tokio::time::timeout(self.core.bounds.operation_timeout, async {
            let ack = self
                .context
                .send_publish(subject.to_owned(), message)
                .await
                .map_err(publish_error)?
                .await
                .map_err(publish_error)?;
            if ack.sequence == 0 || ack.stream.is_empty() {
                return Err(Error::Uncertain);
            }
            Ok(Receipt {
                stream: ack.stream,
                sequence: ack.sequence,
                domain: ack.domain,
                duplicate: ack.duplicate,
            })
        })
        .await
        .map_err(|_| Error::Uncertain)?
    }

    pub(crate) async fn acknowledge(
        &self,
        message: &async_nats::jetstream::Message,
        kind: async_nats::jetstream::AckKind,
    ) -> Result<(), Error> {
        let reply = message.reply.clone().ok_or(Error::Lost)?;
        tokio::time::timeout(self.core.bounds.operation_timeout, async {
            // Unlike a blind "some reply arrived" double-ack, this rejects a 503
            // no-responder/status response after a consumer has disappeared.
            let response = self
                .core
                .client
                .request(reply, kind.into())
                .await
                .map_err(|error| match error.kind() {
                    async_nats::RequestErrorKind::NoResponders => Error::Lost,
                    _ => Error::Uncertain,
                })?;
            if response.status.is_some_and(|status| !status.is_success())
                || !response.payload.is_empty()
            {
                return Err(Error::Uncertain);
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::Uncertain)?
    }

    /// Creates or resumes a finite-window pull consumer. Replay/initial positions are
    /// explicit native options; Verify does not mutate an externally owned consumer.
    pub async fn receiver(
        &self,
        stream: &str,
        config: pull::Config,
        mode: Provision,
        source: Source,
        max_extension: Duration,
    ) -> Result<Receiver, Error> {
        stream_name(stream)?;
        source.validate()?;
        config::metadata(&config.metadata)?;
        let name = config.durable_name.as_deref().or(config.name.as_deref());
        if let Some(name) = name {
            stream_name(name)?;
        }
        if name.is_none() && mode != Provision::Create {
            return Err(Error::Invalid("named consumer required for verify/update"));
        }
        if config.ack_policy != AckPolicy::Explicit
            || config.ack_wait.is_zero()
            || config.ack_wait > Duration::from_secs(3600)
            || config.max_ack_pending < 1
            || config.max_ack_pending > i64::from(self.core.bounds.prefetch)
            || max_extension < config.ack_wait
            || max_extension > Duration::from_secs(86_400)
            || !config.backoff.is_empty()
            || config.headers_only
        {
            return Err(Error::Invalid("consumer acknowledgement bounds"));
        }
        if !config.filter_subject.is_empty() {
            config::validate_subject(&config.filter_subject, true)?;
        }
        if config.filter_subjects.len() > 256 {
            return Err(Error::Invalid("filter count"));
        }
        for filter in &config.filter_subjects {
            config::validate_subject(filter, true)?;
        }
        let consumer = tokio::time::timeout(self.core.bounds.operation_timeout, async {
            let stream = self
                .context
                .get_stream(stream)
                .await
                .map_err(|_| Error::Uncertain)?;
            let consumer: Consumer<pull::Config> = match mode {
                Provision::Create if name.is_some() => stream
                    .get_or_create_consumer(name.unwrap_or_default(), config.clone())
                    .await
                    .map_err(|_| Error::Uncertain),
                Provision::Create => stream
                    .create_consumer(config.clone())
                    .await
                    .map_err(|_| Error::Uncertain),
                Provision::Update => stream
                    .create_consumer(config.clone())
                    .await
                    .map_err(|_| Error::Uncertain),
                Provision::Verify => stream
                    .get_consumer(name.unwrap_or_default())
                    .await
                    .map_err(|_| Error::Uncertain),
            }?;
            let actual = &consumer.cached_info().config;
            if actual.ack_policy != config.ack_policy
                || actual.ack_wait != config.ack_wait
                || actual.max_ack_pending != config.max_ack_pending
                || actual.filter_subject != config.filter_subject
                || actual.durable_name != config.durable_name
                || actual.filter_subjects != config.filter_subjects
                || actual.deliver_policy != config.deliver_policy
                || actual.replay_policy != config.replay_policy
                || (config.max_deliver != 0 && actual.max_deliver != config.max_deliver)
                || actual.headers_only != config.headers_only
                || actual.backoff != config.backoff
                || actual.rate_limit != config.rate_limit
                || actual.sample_frequency != config.sample_frequency
                || (config.max_waiting != 0 && actual.max_waiting != config.max_waiting)
                || (config.max_batch != 0 && actual.max_batch != config.max_batch)
                || (config.max_bytes != 0 && actual.max_bytes != config.max_bytes)
                || (!config.max_expires.is_zero() && actual.max_expires != config.max_expires)
                || (!config.inactive_threshold.is_zero()
                    && actual.inactive_threshold != config.inactive_threshold)
                || (config.num_replicas != 0 && actual.num_replicas != config.num_replicas)
                || (config.memory_storage && !actual.memory_storage)
                || !config::metadata_matches(&config.metadata, &actual.metadata)
                || actual.priority_policy != config.priority_policy
                || actual.priority_groups != config.priority_groups
                || (config.description.is_some() && actual.description != config.description)
                || (config.pause_until.is_some() && actual.pause_until != config.pause_until)
            {
                return Err(Error::Drift);
            }
            Ok(consumer)
        })
        .await
        .map_err(|_| Error::Uncertain)??;
        Ok(Receiver {
            consumer,
            jetstream: self.clone(),
            source,
            ack_wait: config.ack_wait,
            max_extension,
        })
    }

    /// Returns an explicit fallback until the server supports schedules AND the stream
    /// uses Limits retention, enables schedules, and captures both subjects.
    /// Native entry syntax is broker-defined `@at` with an RFC3339 UTC timestamp.
    pub async fn schedule(
        &self,
        stream: &str,
        alias: &str,
        envelope: &Envelope,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<ScheduleResult, Error> {
        self.schedule_with(stream, alias, envelope, at, ScheduleOptions::default())
            .await
    }

    /// Native scheduling with a validated per-endpoint subject suffix. Fallback does
    /// not publish a control or pretend the caller's durable scheduler persisted anything.
    pub async fn schedule_with(
        &self,
        stream: &str,
        alias: &str,
        envelope: &Envelope,
        at: chrono::DateTime<chrono::Utc>,
        options: ScheduleOptions,
    ) -> Result<ScheduleResult, Error> {
        stream_name(stream)?;
        if !options.subject_suffix.starts_with('.') || options.subject_suffix.len() > 64 {
            return Err(Error::Invalid("schedule suffix"));
        }
        config::validate_subject(&format!("x{}", options.subject_suffix), false)?;
        let subject = self
            .core
            .routes
            .get(alias)
            .ok_or(Error::Invalid("unknown alias"))?;
        envelope
            .validate(self.core.bounds.envelope)
            .map_err(|_| Error::Invalid("envelope"))?;
        if at <= chrono::Utc::now() {
            return Err(Error::Invalid("schedule must be in future"));
        }
        let version = self.core.client.server_info().version;
        let mut parts = version
            .trim_start_matches('v')
            .split('.')
            .filter_map(|v| v.parse::<u32>().ok());
        if (parts.next().unwrap_or(0), parts.next().unwrap_or(0)) < (2, 12) {
            return Ok(ScheduleResult::RequiresDurableScheduler);
        }
        let info = tokio::time::timeout(
            self.core.bounds.operation_timeout,
            self.context.get_stream(stream),
        )
        .await
        .map_err(|_| Error::Uncertain)?
        .map_err(|_| Error::Uncertain)?;
        let config = &info.cached_info().config;
        let identity = envelope.id().to_string();
        let control_key = match &self.dedup_scope {
            Some(scope) => config::dedup_key(scope, "schedule-control", &[subject, &identity]),
            None => format!("{identity}-{}", uuid::Uuid::new_v4()),
        };
        let schedule_subject = format!("{subject}{}.{control_key}", options.subject_suffix);
        config::validate_subject(&schedule_subject, false)?;
        if config.retention != crate::RetentionPolicy::Limits
            || !config.allow_message_schedules
            || !config.subjects.iter().any(|s| captures(s, subject))
            || !config
                .subjects
                .iter()
                .any(|s| captures(s, &schedule_subject))
        {
            return Ok(ScheduleResult::RequiresDurableScheduler);
        }
        let mut message = PublishMessage::build()
            .payload(
                envelope
                    .to_vec()
                    .map_err(|_| Error::Invalid("envelope"))?
                    .into(),
            )
            .expected_stream(stream)
            .header("Nats-Schedule", format!("@at {}", at.to_rfc3339()))
            .header("Nats-Schedule-Target", subject.as_str());
        if let Some(scope) = &self.dedup_scope {
            message =
                message.message_id(config::dedup_key(scope, "schedule", &[subject, &identity]));
        }
        Ok(ScheduleResult::Native(
            self.publish_message(&schedule_subject, message).await?,
        ))
    }
}
impl Transport for JetStream {
    async fn publish(&self, destination: &str, envelope: &Envelope) -> Result<(), TransportError> {
        self.send(destination, envelope, PublishOptions::default())
            .await
            .map(|_| ())
            .map_err(|e| match e {
                Error::Invalid(_) | Error::Unsupported | Error::Drift | Error::Refused => {
                    TransportError::Rejected
                }
                _ => TransportError::Transient,
            })
    }
}

/// Caller-polled pull consumer; owns no receiver/handler worker.
pub struct Receiver {
    consumer: Consumer<pull::Config>,
    jetstream: JetStream,
    source: Source,
    ack_wait: Duration,
    max_extension: Duration,
}
impl Receiver {
    /// Pulls at most `limit` members within one total deadline, including broker wait.
    /// Bad wire data remains in a held handle and never prevents later members arriving.
    pub async fn receive(
        &mut self,
        limit: usize,
        wait: Duration,
    ) -> Result<Vec<HeldMessage>, Error> {
        config::wait(wait)?;
        if limit == 0 || limit > usize::from(self.jetstream.core.bounds.prefetch) {
            return Err(Error::Invalid("batch limit"));
        }
        let deadline = tokio::time::Instant::now() + wait;
        let requested_at = std::time::Instant::now();
        let mut batch = tokio::time::timeout_at(
            deadline,
            self.consumer
                .batch()
                .max_messages(limit)
                .expires(wait)
                .messages(),
        )
        .await
        .map_err(|_| Error::Uncertain)?
        .map_err(|_| Error::Uncertain)?;
        let mut result = Vec::with_capacity(limit);
        let mut inspected = 0;
        while inspected < limit {
            match tokio::time::timeout_at(deadline, batch.next()).await {
                Err(_) | Ok(None) => break,
                Ok(Some(Err(_))) => {
                    if result.is_empty() {
                        return Err(Error::Uncertain);
                    }
                    break;
                }
                Ok(Some(Ok(message))) => {
                    inspected += 1;
                    if message.headers.as_ref().is_some_and(|h| {
                        h.get("Nats-Schedule").is_some() && h.get("Nats-Scheduler").is_none()
                    }) {
                        // A scheduling control is not an application delivery. It counts
                        // against this bounded pull page and is never dispatched early.
                        let _ = tokio::time::timeout_at(
                            deadline,
                            self.jetstream
                                .acknowledge(&message, async_nats::jetstream::AckKind::Ack),
                        )
                        .await;
                        continue;
                    }
                    result.push(HeldMessage::new(
                        message,
                        self.jetstream.clone(),
                        self.source.clone(),
                        self.ack_wait,
                        self.max_extension,
                        requested_at,
                    )?);
                }
            }
        }
        Ok(result)
    }
}

pub(crate) fn stream_name(name: &str) -> Result<(), Error> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(Error::Invalid("stream or consumer name"));
    }
    Ok(())
}
fn stream_config(config: &StreamConfig) -> Result<(), Error> {
    stream_name(&config.name)?;
    config::metadata(&config.metadata)?;
    if config.no_ack
        || config.subjects.len() > 256
        || config.num_replicas > 5
        || config.max_message_size > 1_048_576
    {
        return Err(Error::Invalid("stream configuration"));
    }
    for subject in &config.subjects {
        config::validate_subject(subject, true)?;
    }
    Ok(())
}
fn stream_matches(expected: &StreamConfig, actual: &StreamConfig) -> bool {
    // Server fills defaults for unlimited limits and replica count; compare explicit intent.
    expected.name == actual.name
        && expected.subjects == actual.subjects
        && expected.retention == actual.retention
        && expected.storage == actual.storage
        && (expected.num_replicas == 0 || expected.num_replicas == actual.num_replicas)
        && (expected.max_messages == 0 || expected.max_messages == actual.max_messages)
        && (expected.max_bytes == 0 || expected.max_bytes == actual.max_bytes)
        && expected.max_age == actual.max_age
        && expected.mirror == actual.mirror
        && expected.sources == actual.sources
        && expected.placement == actual.placement
        && expected.allow_message_schedules == actual.allow_message_schedules
        && expected.no_ack == actual.no_ack
        && expected.discard == actual.discard
        && expected.discard_new_per_subject == actual.discard_new_per_subject
        && (expected.max_messages_per_subject == 0
            || expected.max_messages_per_subject == actual.max_messages_per_subject)
        && (expected.max_message_size == 0 || expected.max_message_size == actual.max_message_size)
        && (expected.max_consumers == 0 || expected.max_consumers == actual.max_consumers)
        && (expected.duplicate_window.is_zero()
            || expected.duplicate_window == actual.duplicate_window)
        && expected.deny_delete == actual.deny_delete
        && expected.deny_purge == actual.deny_purge
        // Native schedules enable rollup on the server as a prerequisite.
        && (expected.allow_rollup || expected.allow_message_schedules) == actual.allow_rollup
        && expected.allow_direct == actual.allow_direct
        && expected.mirror_direct == actual.mirror_direct
        && expected.sealed == actual.sealed
        && expected.republish == actual.republish
        && expected.subject_transform == actual.subject_transform
        && config::metadata_matches(&expected.metadata, &actual.metadata)
        && expected
            .compression
            .as_ref()
            .unwrap_or(&async_nats::jetstream::stream::Compression::None)
            == actual
                .compression
                .as_ref()
                .unwrap_or(&async_nats::jetstream::stream::Compression::None)
        && expected.allow_message_ttl == actual.allow_message_ttl
        && expected.allow_atomic_publish == actual.allow_atomic_publish
        && expected.allow_message_counter == actual.allow_message_counter
        && expected.allow_batch_publish == actual.allow_batch_publish
        && (expected.description.is_none() || expected.description == actual.description)
        && (expected.consumer_limits.is_none()
            || expected.consumer_limits == actual.consumer_limits)
        && (expected.first_sequence.is_none() || expected.first_sequence == actual.first_sequence)
        && (expected.subject_delete_marker_ttl.is_none()
            || expected.subject_delete_marker_ttl == actual.subject_delete_marker_ttl)
        && (expected.pause_until.is_none() || expected.pause_until == actual.pause_until)
        && (expected.persist_mode.is_none() || expected.persist_mode == actual.persist_mode)
        && (expected.template_owner.is_empty() || expected.template_owner == actual.template_owner)
}

fn publish_error(error: async_nats::jetstream::context::PublishError) -> Error {
    use async_nats::jetstream::context::PublishErrorKind;
    if let Some(server) = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<async_nats::jetstream::Error>())
        && server.error_code() == async_nats::jetstream::ErrorCode::STREAM_LIMITS
    {
        return Error::Refused;
    }
    match error.kind() {
        PublishErrorKind::StreamNotFound
        | PublishErrorKind::WrongLastMessageId
        | PublishErrorKind::WrongLastSequence
        | PublishErrorKind::MaxPayloadExceeded => Error::Refused,
        _ => Error::Uncertain,
    }
}
fn captures(filter: &str, subject: &str) -> bool {
    let mut subjects = subject.split('.');
    for token in filter.split('.') {
        let Some(actual) = subjects.next() else {
            return false;
        };
        if token == ">" {
            return true;
        }
        if token != "*" && token != actual {
            return false;
        }
    }
    subjects.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::{StreamConfig, stream_config, stream_matches};
    use async_nats::jetstream::stream::Compression;
    #[test]
    fn server_metadata_and_compression_defaults_are_not_application_drift() {
        let expected = StreamConfig {
            name: "ORDERS".into(),
            subjects: vec!["orders.>".into()],
            ..StreamConfig::default()
        };
        let mut actual = expected.clone();
        actual.compression = Some(Compression::None);
        actual.metadata.insert("_nats.req.level".into(), "0".into());
        assert!(stream_matches(&expected, &actual));
        actual
            .metadata
            .insert("application-owner".into(), "different".into());
        assert!(!stream_matches(&expected, &actual));
        let mut invalid = expected;
        invalid
            .metadata
            .insert("_nats.req.level".into(), "0".into());
        assert!(stream_config(&invalid).is_err());
    }
}
