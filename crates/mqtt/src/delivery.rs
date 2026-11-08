use raddle::wire::{Envelope, EnvelopeLimits, WireError};
use rumqttc::v5::mqttbytes::v5::Publish;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::{QoS, Source};

/// MQTT receipt with private session ownership. Drop does not acknowledge.
/// Retained state and protocol QoS do not prove downstream application completion.
pub struct HeldMessage {
    pub(crate) publish: Publish,
    pub(crate) session: uuid::Uuid,
    pub(crate) settled: bool,
    pub(crate) quarantine_token: uuid::Uuid,
    source: Source,
    limits: EnvelopeLimits,
    outstanding: Arc<AtomicUsize>,
}
impl HeldMessage {
    pub(crate) fn new(
        publish: Publish,
        session: uuid::Uuid,
        source: Source,
        limits: EnvelopeLimits,
        outstanding: Arc<AtomicUsize>,
    ) -> Self {
        outstanding.fetch_add(1, Ordering::Relaxed);
        Self {
            publish,
            session,
            settled: false,
            quarantine_token: uuid::Uuid::new_v4(),
            source,
            limits,
            outstanding,
        }
    }
    /// Wire validation does not consume the native receipt; malformed messages can be
    /// quarantined independently while other deliveries continue.
    pub fn decode(&self) -> Result<Envelope, WireError> {
        self.validate_native()?;
        Envelope::from_slice(&self.publish.payload, self.limits)
    }
    fn validate_native(&self) -> Result<(), WireError> {
        if self.settled {
            return Err(WireError::Invalid("settled MQTT receipt"));
        }
        if self.publish.payload.len() > self.limits.max_bytes {
            return Err(WireError::Invalid("body limit"));
        }
        if self.publish.topic.len() > 1024 {
            return Err(WireError::Invalid("MQTT topic limit"));
        }
        if let Some(properties) = &self.publish.properties {
            crate::config::properties(&properties.user_properties, self.limits.max_headers)
                .map_err(|_| WireError::Invalid("MQTT user properties"))?;
            if properties
                .correlation_data
                .as_ref()
                .is_some_and(|b| b.len() > 128)
                || properties
                    .content_type
                    .as_ref()
                    .is_some_and(|s| s.len() > 256)
                || properties.subscription_identifiers.len() > 32
                || properties
                    .response_topic
                    .as_ref()
                    .is_some_and(|s| crate::validate_topic(s, false).is_err())
            {
                return Err(WireError::Invalid("MQTT reply metadata"));
            }
        }
        Ok(())
    }
    /// Custom raw/foreign-device mapper. The caller must provide a stable device/message
    /// identity; MQTT packet IDs and DUP do not identify an application message across sessions.
    pub fn decode_with<F>(&self, mapper: F) -> Result<Envelope, WireError>
    where
        F: FnOnce(&[u8]) -> Result<Envelope, WireError>,
    {
        self.validate_native()?;
        let envelope = mapper(&self.publish.payload)?;
        envelope.validate(self.limits)?;
        Ok(envelope)
    }
    /// Trusted application tenant and listener identity.
    pub fn source(&self) -> &Source {
        &self.source
    }
    /// Actual native topic, with no authentication meaning.
    pub fn topic(&self) -> &[u8] {
        &self.publish.topic
    }
    /// Original payload for inspection or application-owned quarantine replay.
    /// These bytes are untrusted and have not passed envelope validation.
    pub fn raw(&self) -> &[u8] {
        &self.publish.payload
    }
    /// Broker-delivered quality of service.
    pub fn qos(&self) -> QoS {
        self.publish.qos
    }
    /// Broker marked this as a retained snapshot.
    pub fn retained(&self) -> bool {
        self.publish.retain
    }
    /// MQTT DUP is a hint, not an end-to-end deduplication identity.
    pub fn duplicate(&self) -> bool {
        self.publish.dup
    }
    /// Incoming MQTT application properties, not authenticated claims.
    pub fn user_properties(&self) -> &[(String, String)] {
        self.publish
            .properties
            .as_ref()
            .map_or(&[], |p| p.user_properties.as_slice())
    }
}
impl Drop for HeldMessage {
    fn drop(&mut self) {
        self.outstanding.fetch_sub(1, Ordering::Relaxed);
    }
}
