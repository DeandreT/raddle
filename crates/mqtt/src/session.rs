use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use raddle::wire::Envelope;
use rumqttc::v5::mqttbytes::v5::{
    Packet, PubAckReason, PubCompReason, PubRecReason, Publish, PublishProperties,
    SubscribeReasonCode,
};
use rumqttc::v5::{AsyncClient, Event, EventLoop, MqttOptions};
use rumqttc::{Outgoing, Transport};

use crate::config;
use crate::{Bounds, Error, HeldMessage, PublishOptions, QoS, Route, Source};

#[cfg(test)]
mod tests;

/// MQTT publication outcome. Only QoS1 PubAck and QoS2 PubComp are confirmed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Acceptance {
    /// QoS0 bytes written; no broker acknowledgement exists.
    Unconfirmed,
    /// Positive MQTT5 PubAck; no matching subscribers is still broker acceptance.
    PubAck,
    /// Positive MQTT5 PubComp completes the QoS2 protocol exchange.
    PubComp,
}

/// Application-owned MQTT5 event loop. No internal thread/task is spawned.
/// Each native exchange polls it directly with a finite deadline; quarantine composes
/// one publication and one source acknowledgement, each with that deadline. A cancelled
/// write poisons this session so stale packet IDs cannot confirm a later publication.
pub struct Session {
    client: AsyncClient,
    eventloop: EventLoop,
    // Validated application configuration, independent of SDK connection state.
    options: MqttOptions,
    routes: BTreeMap<String, String>,
    bounds: Bounds,
    source: Source,
    id: uuid::Uuid,
    incoming: VecDeque<Publish>,
    outstanding: Arc<AtomicUsize>,
    unresolved: bool,
    connected: bool,
}
impl Session {
    /// Builds a caller-polled client. TLS/WebSocket/auth properties stay on the caller's
    /// MqttOptions. Plaintext TCP/WebSocket is restricted to numeric loopback.
    /// A stable unique client ID and persistent session are required for redelivery.
    pub fn new(
        mut options: MqttOptions,
        source: Source,
        routes: Vec<Route>,
        bounds: Bounds,
    ) -> Result<Self, Error> {
        let bounds = bounds.validate()?;
        source.validate()?;
        #[cfg(unix)]
        if matches!(options.transport(), Transport::Unix) {
            return Err(Error::Unsupported);
        }
        if options.client_id().is_empty() || options.client_id().len() > 128 {
            return Err(Error::Invalid("client id"));
        }
        let (host, _) = options.broker_address();
        let host = if matches!(options.transport(), Transport::Ws | Transport::Wss(_)) {
            url::Url::parse(&host)
                .ok()
                .and_then(|u| u.host_str().map(str::to_owned))
                .ok_or(Error::Invalid("websocket URL"))?
        } else {
            host
        };
        let local = host
            .trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
        if !local && matches!(options.transport(), Transport::Tcp | Transport::Ws) {
            return Err(Error::Invalid("TLS required outside numeric loopback"));
        }
        if routes.is_empty() || routes.len() > 256 {
            return Err(Error::Invalid("route count"));
        }
        let mut configured = BTreeMap::new();
        for route in routes {
            config::name(&route.alias)?;
            config::validate_topic(&route.topic, false)?;
            if configured.insert(route.alias, route.topic).is_some() {
                return Err(Error::Invalid("duplicate alias"));
            }
        }
        options
            .set_manual_acks(true)
            .set_receive_maximum(Some(bounds.prefetch))
            .set_outgoing_inflight_upper_limit(1)
            .set_max_packet_size(Some((bounds.envelope.max_bytes + 131_072) as u32))
            .set_connection_timeout(bounds.timeout.as_secs().max(1));
        let (client, eventloop) = AsyncClient::new(options.clone(), usize::from(bounds.prefetch));
        Ok(Self {
            client,
            eventloop,
            options,
            routes: configured,
            bounds,
            source,
            id: uuid::Uuid::new_v4(),
            incoming: VecDeque::new(),
            outstanding: Arc::new(AtomicUsize::new(0)),
            unresolved: false,
            connected: false,
        })
    }
    /// Current client ID without exposing passwords, certificates or token material.
    pub fn client_id(&self) -> String {
        self.eventloop.options.client_id()
    }
    pub(crate) fn source(&self) -> &Source {
        &self.source
    }
    /// Whether a cancelled/failed write requires session replacement.
    pub fn is_unresolved(&self) -> bool {
        self.unresolved
    }

    /// Publishes to an allowlisted route and polls until the matching protocol outcome.
    /// No success is synthesized from enqueueing a rumqttc request.
    pub async fn publish(
        &mut self,
        alias: &str,
        envelope: &Envelope,
        qos: QoS,
        options: PublishOptions,
    ) -> Result<Acceptance, Error> {
        let topic = self
            .routes
            .get(alias)
            .ok_or(Error::Invalid("unknown alias"))?
            .clone();
        self.publish_topic(&topic, envelope, qos, options).await
    }
    /// Explicit dynamic topic publication. This method is for application-trusted topic
    /// selectors; never pass a received topic/header/URI without a separate authorization check.
    pub async fn publish_topic(
        &mut self,
        topic: &str,
        envelope: &Envelope,
        qos: QoS,
        options: PublishOptions,
    ) -> Result<Acceptance, Error> {
        envelope
            .validate(self.bounds.envelope)
            .map_err(|_| Error::Invalid("envelope"))?;
        let bytes = envelope.to_vec().map_err(|_| Error::Invalid("envelope"))?;
        let retain = options.retain;
        let mut properties = self.properties(options)?;
        properties
            .user_properties
            .push(("raddle-id".to_owned(), envelope.id().to_string()));
        self.send(topic, bytes, qos, properties, retain).await
    }
    /// Deletes the retained snapshot using a zero-length retained MQTT publication.
    /// Requires an acknowledged QoS rather than misreporting a QoS0 clear as confirmed.
    pub async fn clear_retained(&mut self, alias: &str, qos: QoS) -> Result<Acceptance, Error> {
        if qos == QoS::AtMostOnce {
            return Err(Error::Unsupported);
        }
        let topic = self
            .routes
            .get(alias)
            .ok_or(Error::Invalid("unknown alias"))?
            .clone();
        self.clear_retained_topic(&topic, qos).await
    }
    /// Clears an explicitly application-trusted dynamic retained topic, including a
    /// quarantine receipt returned by this adapter. Never authorize a received topic implicitly.
    pub async fn clear_retained_topic(
        &mut self,
        topic: &str,
        qos: QoS,
    ) -> Result<Acceptance, Error> {
        if qos == QoS::AtMostOnce {
            return Err(Error::Unsupported);
        }
        self.send(topic, Vec::new(), qos, PublishProperties::default(), true)
            .await
    }
    fn properties(&self, options: PublishOptions) -> Result<PublishProperties, Error> {
        if options
            .user_properties
            .iter()
            .any(|(key, _)| key == "raddle-id")
        {
            return Err(Error::Invalid("reserved identity property"));
        }
        config::properties(
            &options.user_properties,
            self.bounds.envelope.max_headers.saturating_sub(1),
        )?;
        if options.correlation.as_ref().is_some_and(|v| v.len() > 128) {
            return Err(Error::Invalid("correlation"));
        }
        let response_topic = options
            .reply_alias
            .map(|alias| {
                self.routes
                    .get(&alias)
                    .cloned()
                    .ok_or(Error::Invalid("reply alias"))
            })
            .transpose()?;
        // Retain is applied by send's explicit parameter; see publish_topic below.
        Ok(PublishProperties {
            response_topic,
            correlation_data: options.correlation.map(Into::into),
            user_properties: options.user_properties,
            content_type: Some("application/json".to_owned()),
            payload_format_indicator: Some(1),
            message_expiry_interval: options.expiry_seconds,
            ..PublishProperties::default()
        })
    }
    async fn send(
        &mut self,
        topic: &str,
        bytes: Vec<u8>,
        qos: QoS,
        properties: PublishProperties,
        retain: bool,
    ) -> Result<Acceptance, Error> {
        self.ready()?;
        config::validate_topic(topic, false)?;
        config::properties(
            &properties.user_properties,
            self.bounds.envelope.max_headers,
        )?;
        self.unresolved = true;
        self.client
            .try_publish_with_properties(topic, qos, retain, bytes, properties)
            .map_err(|_| Error::Uncertain)?;
        let deadline = tokio::time::Instant::now() + self.bounds.timeout;
        let mut packet = None;
        loop {
            let event = self.poll(deadline).await?;
            match event {
                Event::Outgoing(Outgoing::Publish(id)) if packet.is_none() => {
                    packet = Some(id);
                    if qos == QoS::AtMostOnce {
                        self.unresolved = false;
                        return Ok(Acceptance::Unconfirmed);
                    }
                }
                Event::Incoming(Packet::PubAck(ack))
                    if qos == QoS::AtLeastOnce && packet == Some(ack.pkid) =>
                {
                    if !matches!(
                        ack.reason,
                        PubAckReason::Success | PubAckReason::NoMatchingSubscribers
                    ) {
                        // The SDK has removed this QoS1 packet from its in-flight map;
                        // a known rejection must not block unrelated later publications.
                        self.unresolved = false;
                        return Err(Error::Refused);
                    }
                    self.unresolved = false;
                    return Ok(Acceptance::PubAck);
                }
                Event::Incoming(Packet::PubRec(ack))
                    if qos == QoS::ExactlyOnce
                        && packet == Some(ack.pkid)
                        && !matches!(
                            ack.reason,
                            PubRecReason::Success | PubRecReason::NoMatchingSubscribers
                        ) =>
                {
                    return Err(Error::Refused);
                }
                Event::Incoming(Packet::PubComp(ack))
                    if qos == QoS::ExactlyOnce && packet == Some(ack.pkid) =>
                {
                    if ack.reason != PubCompReason::Success {
                        return Err(Error::Refused);
                    }
                    self.unresolved = false;
                    return Ok(Acceptance::PubComp);
                }
                Event::Incoming(Packet::Publish(publish)) => self.buffer(publish)?,
                Event::Incoming(Packet::Disconnect(_)) => return Err(Error::Uncertain),
                _ => {}
            }
        }
    }
    /// Confirms a wildcard subscription with its matching SubAck reason codes.
    pub async fn subscribe(&mut self, filter: &str, qos: QoS) -> Result<(), Error> {
        self.ready()?;
        config::validate_topic(filter, true)?;
        self.unresolved = true;
        self.client
            .try_subscribe(filter, qos)
            .map_err(|_| Error::Uncertain)?;
        let deadline = tokio::time::Instant::now() + self.bounds.timeout;
        let mut packet = None;
        loop {
            match self.poll(deadline).await? {
                Event::Outgoing(Outgoing::Subscribe(id)) => packet = Some(id),
                Event::Incoming(Packet::SubAck(ack)) if packet == Some(ack.pkid) => {
                    if ack.return_codes.len() != 1
                        || !ack
                            .return_codes
                            .iter()
                            .all(|v| matches!(v, SubscribeReasonCode::Success(_)))
                    {
                        self.unresolved = false;
                        return Err(Error::Refused);
                    }
                    self.unresolved = false;
                    return Ok(());
                }
                Event::Incoming(Packet::Publish(p)) => self.buffer(p)?,
                _ => {}
            }
        }
    }
    /// Polls a finite batch. Receive does not auto-acknowledge or trust reply/tenant metadata.
    pub async fn receive(
        &mut self,
        limit: usize,
        wait: Duration,
    ) -> Result<Vec<HeldMessage>, Error> {
        self.ready()?;
        config::wait(wait)?;
        if limit == 0 || limit > usize::from(self.bounds.prefetch) {
            return Err(Error::Invalid("batch limit"));
        }
        let deadline = tokio::time::Instant::now() + wait;
        let mut result = Vec::with_capacity(limit);
        loop {
            while result.len() < limit {
                let Some(publish) = self.incoming.pop_front() else {
                    break;
                };
                result.push(HeldMessage::new(
                    publish,
                    self.id,
                    self.source.clone(),
                    self.bounds.envelope,
                    Arc::clone(&self.outstanding),
                ));
            }
            if result.len() == limit
                || !result.is_empty()
                || self.outstanding.load(Ordering::Relaxed) >= usize::from(self.bounds.prefetch)
            {
                break;
            }
            match self.poll(deadline).await {
                Ok(Event::Incoming(Packet::Publish(p))) => self.buffer(p)?,
                Ok(_) => {}
                Err(Error::Uncertain)
                    if !self.unresolved && tokio::time::Instant::now() >= deadline =>
                {
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(result)
    }
    /// Acknowledges a source-owned receipt only after the caller's handling/custody boundary.
    /// QoS0 has no such boundary and is refused. QoS2 completes the protocol handshake.
    pub async fn complete(&mut self, message: &mut HeldMessage) -> Result<(), Error> {
        self.ready()?;
        if message.session != self.id || message.settled {
            return Err(Error::Lost);
        }
        if message.publish.qos == QoS::AtMostOnce {
            return Err(Error::Unsupported);
        }
        self.unresolved = true;
        message.settled = true;
        self.client
            .try_ack(&message.publish)
            .map_err(|_| Error::Uncertain)?;
        let deadline = tokio::time::Instant::now() + self.bounds.timeout;
        loop {
            match self.poll(deadline).await? {
                Event::Outgoing(Outgoing::PubAck(id))
                    if message.publish.qos == QoS::AtLeastOnce && id == message.publish.pkid =>
                {
                    self.unresolved = false;
                    return Ok(());
                }
                Event::Outgoing(Outgoing::PubComp(id))
                    if message.publish.qos == QoS::ExactlyOnce && id == message.publish.pkid =>
                {
                    self.unresolved = false;
                    return Ok(());
                }
                Event::Incoming(Packet::Publish(p)) => self.buffer(p)?,
                _ => {}
            }
        }
    }
    /// Publishes a reply only when its received response topic exactly matches a trusted alias.
    pub async fn respond(
        &mut self,
        message: &HeldMessage,
        reply_alias: &str,
        envelope: &Envelope,
        qos: QoS,
    ) -> Result<Acceptance, Error> {
        if message.session != self.id {
            return Err(Error::Lost);
        }
        let topic = self
            .routes
            .get(reply_alias)
            .ok_or(Error::Invalid("reply alias"))?;
        let properties = message
            .publish
            .properties
            .as_ref()
            .ok_or(Error::Invalid("missing reply properties"))?;
        if properties.response_topic.as_ref() != Some(topic) {
            return Err(Error::Invalid("untrusted reply topic"));
        }
        let options = PublishOptions {
            correlation: properties.correlation_data.as_ref().map(|v| v.to_vec()),
            ..PublishOptions::default()
        };
        self.publish(reply_alias, envelope, qos, options).await
    }
    /// Retains a version-framed poison snapshot under a unique child of a trusted route before
    /// acknowledging its source. Returns the retained topic for inspection/replay/clear.
    /// This is broker-retained state, not an end-consumer completion or database commit.
    /// `QuarantineSnapshot` recovers exact bytes, including empty input. MQTT has no
    /// cross-topic transaction: crash/redelivery can create duplicate snapshots.
    pub async fn quarantine(
        &mut self,
        message: &mut HeldMessage,
        alias: &str,
        reason: &str,
    ) -> Result<String, Error> {
        if message.session != self.id || message.settled {
            return Err(Error::Lost);
        }
        config::name(reason)?;
        if message.publish.qos == QoS::AtMostOnce {
            return Err(Error::Unsupported);
        }
        if message.publish.payload.len() > self.bounds.envelope.max_bytes {
            return Err(Error::Invalid("quarantine payload limit"));
        }
        let base = self
            .routes
            .get(alias)
            .ok_or(Error::Invalid("quarantine alias"))?
            .clone();
        if base.as_bytes() == message.publish.topic {
            return Err(Error::Invalid("quarantine target must differ"));
        }
        let topic = format!("{base}/.quarantine/{}", message.quarantine_token);
        config::validate_topic(&topic, false)?;
        let properties = PublishProperties {
            user_properties: vec![("raddle-quarantine-reason".into(), reason.into())],
            content_type: Some("application/vnd.raddle.mqtt-quarantine.v1".into()),
            ..PublishProperties::default()
        };
        self.send(
            &topic,
            crate::QuarantineSnapshot::encode(&message.publish.payload),
            QoS::AtLeastOnce,
            properties,
            true,
        )
        .await?;
        self.complete(message).await?;
        Ok(topic)
    }
    /// Reconnects with a caller-refreshed JWT/OAuth password and optional auth method.
    /// No token is read, fetched, printed or persisted by this adapter. Refresh is refused
    /// with outstanding receipts or unresolved writes; drain or replace that session instead.
    /// Persistent broker session/subscriptions depend on the caller's MqttOptions.
    /// Rebuilds the entire SDK client/event loop, discarding unread SDK events without
    /// acknowledging them. A persistent broker may redeliver them as fresh owned receipts.
    pub fn refresh_credentials(
        &mut self,
        username: String,
        token: String,
        authentication_method: Option<String>,
    ) -> Result<(), Error> {
        self.ready()?;
        if !self.incoming.is_empty() || self.outstanding.load(Ordering::Relaxed) != 0 {
            return Err(Error::Invalid("drain before credential refresh"));
        }
        let mut options = self.options.clone();
        options
            .set_credentials(username, token)
            .set_authentication_method(authentication_method);
        // SDK clean() retains buffered incoming events. A new receipt namespace
        // must never adopt those old events (packet IDs can be reused after reconnect).
        let (client, eventloop) =
            AsyncClient::new(options.clone(), usize::from(self.bounds.prefetch));
        self.client = client;
        self.eventloop = eventloop;
        self.options = options;
        self.connected = false;
        self.id = uuid::Uuid::new_v4();
        Ok(())
    }
    /// Explicitly disconnects and drops the caller-owned socket/eventloop. No worker survives.
    pub async fn close(mut self) -> Result<(), Error> {
        // Dropping a poisoned session is safe; polling it to send Disconnect could
        // reconnect and flush stale queued requests into a different native session.
        self.ready()?;
        self.client.try_disconnect().map_err(|_| Error::Uncertain)?;
        let deadline = tokio::time::Instant::now() + self.bounds.timeout;
        loop {
            if matches!(
                self.poll(deadline).await?,
                Event::Outgoing(Outgoing::Disconnect)
            ) {
                return Ok(());
            }
        }
    }
    fn ready(&self) -> Result<(), Error> {
        if self.unresolved {
            Err(Error::Unresolved)
        } else {
            Ok(())
        }
    }
    async fn poll(&mut self, deadline: tokio::time::Instant) -> Result<Event, Error> {
        match tokio::time::timeout_at(deadline, self.eventloop.poll()).await {
            // An ordinary idle receive deadline does not change the native session.
            Err(_) => Err(Error::Uncertain),
            Ok(Err(_)) | Ok(Ok(Event::Incoming(Packet::Disconnect(_)))) => {
                self.unresolved = true;
                Err(Error::Uncertain)
            }
            Ok(Ok(event)) => {
                if matches!(&event, Event::Incoming(Packet::ConnAck(_))) {
                    // Packet IDs are only meaningful in one native connection/session.
                    // Even an apparently resumed reconnect is not allowed to reuse held
                    // receipts: require replacement rather than guessing continuity.
                    if self.connected {
                        self.unresolved = true;
                        return Err(Error::Uncertain);
                    }
                    self.connected = true;
                }
                Ok(event)
            }
        }
    }
    fn buffer(&mut self, publish: Publish) -> Result<(), Error> {
        if self.incoming.len() + self.outstanding.load(Ordering::Relaxed)
            >= usize::from(self.bounds.prefetch)
        {
            return Err(Error::Uncertain);
        }
        self.incoming.push_back(publish);
        Ok(())
    }
}
