use std::time::Duration;

use rdkafka::config::ClientConfig;

use crate::Error;

/// An application-configured Kafka topic, not a URI selected by wire metadata.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Topic(String);

impl Topic {
    /// Validates Kafka's nonempty ASCII topic-name syntax and length bound.
    pub fn try_new(name: impl Into<String>) -> Result<Self, Error> {
        let name = name.into();
        if name.is_empty()
            || name.len() > 249
            || name == "."
            || name == ".."
            || name
                .bytes()
                .any(|b| !(b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-')))
        {
            return Err(Error::Configuration);
        }
        Ok(Self(name))
    }

    /// Exact configured topic name; no normalization or automatic creation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Explicit per-record, admission and native request bounds.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Application body, key and header byte limit, at most 1 MiB. Not a bound
    /// on the SDK's earlier compressed-batch decompression allocation.
    pub max_record_bytes: usize,
    /// Maximum Kafka headers; null values remain distinct from empty values.
    pub max_headers: usize,
    /// Maximum admitted calls or application input records. Receiving reserves
    /// one additional incidental record slot around native pause/rebalance.
    pub max_pending: usize,
    /// Total request deadline, between 100 ms and five minutes.
    pub deadline: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_record_bytes: 1_000_000,
            max_headers: 32,
            max_pending: 64,
            deadline: Duration::from_secs(60),
        }
    }
}

impl Limits {
    pub(crate) fn validate(self) -> Result<(), Error> {
        if !(1..=1_048_576).contains(&self.max_record_bytes)
            || self.max_headers > 256
            || !(1..=256).contains(&self.max_pending)
            || !(Duration::from_millis(100)..=Duration::from_secs(300)).contains(&self.deadline)
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }

    pub(crate) fn milliseconds(self) -> String {
        self.deadline.as_millis().to_string()
    }
}

/// App-owned native client options, inherited by producer, consumer and admin.
/// Credentials stay inside the SDK configuration and are never formatted here.
pub struct ClientSettings {
    pub(crate) native: ClientConfig,
    pub(crate) limits: Limits,
}

impl ClientSettings {
    /// Requires explicit bootstrap servers and security protocol. TLS/SASL values
    /// use the native client's documented configuration. The adapter owns bounds
    /// and disables implicit topic creation regardless of other native settings.
    pub fn try_new(mut native: ClientConfig, limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        let servers = native
            .get("bootstrap.servers")
            .ok_or(Error::Configuration)?;
        if servers.trim().is_empty() || servers.len() > 8192 {
            return Err(Error::Configuration);
        }
        if !matches!(
            native.get("security.protocol"),
            Some("plaintext" | "ssl" | "sasl_plaintext" | "sasl_ssl")
        ) {
            return Err(Error::Configuration);
        }
        for key in native.config_map().keys() {
            if key.contains('\0') || native.get(key).is_some_and(|value| value.contains('\0')) {
                return Err(Error::Configuration);
            }
        }
        native.set("allow.auto.create.topics", "false");
        native.set(
            "socket.timeout.ms",
            limits.deadline.as_millis().min(5000).to_string(),
        );
        native.set(
            "message.max.bytes",
            (limits.max_record_bytes + 4096).to_string(),
        );
        native.set(
            "receive.message.max.bytes",
            (limits.max_record_bytes + 131_072).max(4096).to_string(),
        );
        Ok(Self { native, limits })
    }

    /// Provider version, independent of a configured protocol string.
    pub fn native_version() -> (i32, String) {
        rdkafka::util::get_rdkafka_version()
    }

    pub(crate) fn producer_config(&self) -> ClientConfig {
        let mut native = self.native.clone();
        native.set("acks", "all");
        if native.get("enable.idempotence").is_none() {
            native.set("enable.idempotence", "false");
        }
        native.set("message.timeout.ms", self.limits.milliseconds());
        native.set(
            "queue.buffering.max.messages",
            (self.limits.max_pending * 2).to_string(),
        );
        native.set(
            "queue.buffering.max.kbytes",
            ((self.limits.max_record_bytes * self.limits.max_pending) / 1024)
                .max(1)
                .to_string(),
        );
        if native.get("linger.ms").is_none() {
            native.set("linger.ms", "250");
        }
        if native.get("batch.num.messages").is_none() {
            native.set("batch.num.messages", "100");
        }
        if native
            .get("max.in.flight.requests.per.connection")
            .is_none()
        {
            native.set("max.in.flight.requests.per.connection", "1");
        }
        native
    }
}

impl std::fmt::Debug for ClientSettings {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientSettings")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{ClientSettings, Limits, Topic};
    use rdkafka::config::ClientConfig;

    #[test]
    fn topic_validation_and_client_bounds_are_explicit() {
        for invalid in ["", ".", "..", "topic/path", "topic\0name"] {
            assert!(Topic::try_new(invalid).is_err());
        }
        assert!(ClientSettings::try_new(ClientConfig::new(), Limits::default()).is_err());
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", "127.0.0.1:9092");
        assert!(ClientSettings::try_new(config.clone(), Limits::default()).is_err());
        config
            .set("security.protocol", "plaintext")
            .set("sasl.password", "private-fixture-placeholder");
        let settings = ClientSettings::try_new(config, Limits::default()).unwrap();
        assert!(!format!("{settings:?}").contains("private-fixture-placeholder"));
        let native = settings.producer_config();
        assert_eq!(native.get("allow.auto.create.topics"), Some("false"));
        assert_eq!(native.get("acks"), Some("all"));
        assert_eq!(native.get("enable.idempotence"), Some("false"));
    }
}
