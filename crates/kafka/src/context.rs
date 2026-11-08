use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::error::KafkaError;

/// Native logs may echo configuration or server text. Public errors expose only
/// stable classifications; user logging never receives the native raw strings.
pub(crate) struct QuietContext;

impl ClientContext for QuietContext {
    fn log(&self, _: RDKafkaLogLevel, _: &str, _: &str) {}
    fn error(&self, _: KafkaError, _: &str) {}
}
