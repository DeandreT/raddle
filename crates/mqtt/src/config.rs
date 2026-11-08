use raddle::wire::EnvelopeLimits;
use std::time::Duration;

/// Redacted MQTT errors. Missing protocol acknowledgement is never reported as success.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid application configuration or bounded wire data.
    #[error("invalid MQTT configuration or message: {0}")]
    Invalid(&'static str),
    /// Publish may have been accepted; reconnect/retry with the same identity.
    #[error("MQTT operation failed or outcome is uncertain")]
    Uncertain,
    /// Explicit negative MQTT 5 reason code.
    #[error("MQTT broker refused the operation")]
    Refused,
    /// Cancellation/timeout left an unresolved write. This session cannot be reused.
    #[error("MQTT session has an unresolved operation; replace it")]
    Unresolved,
    /// Delivery belongs to a different session or is already settled.
    #[error("MQTT delivery ownership lost")]
    Lost,
    /// The backend has no such acknowledgement/durability guarantee.
    #[error("unsupported MQTT delivery semantics")]
    Unsupported,
}
/// Finite per-operation deadline and queue/window bounds.
#[derive(Clone, Copy, Debug)]
pub struct Bounds {
    /// Total operation deadline, including lock wait where applicable.
    pub timeout: Duration,
    /// Maximum incoming held/buffered deliveries, one through 256.
    pub prefetch: u16,
    /// Body, identity, schema and metadata bounds.
    pub envelope: EnvelopeLimits,
}
impl Default for Bounds {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            prefetch: 64,
            envelope: EnvelopeLimits::default(),
        }
    }
}
impl Bounds {
    pub(crate) fn validate(self) -> Result<Self, Error> {
        wait(self.timeout)?;
        if !(1..=256).contains(&self.prefetch)
            || self.envelope.max_bytes == 0
            || self.envelope.max_bytes > 1_048_576
            || self.envelope.max_headers > 32
        {
            return Err(Error::Invalid("bounds"));
        }
        Ok(self)
    }
}
/// Trusted listener context, not claims extracted from MQTT properties.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Source {
    /// Named broker connection.
    pub client: String,
    /// Configured/authenticated tenant.
    pub tenant: String,
    /// Listener endpoint identity.
    pub endpoint: String,
}
impl Source {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        for s in [&self.client, &self.tenant, &self.endpoint] {
            name(s)?;
        }
        Ok(())
    }
}
/// Frozen, allowlisted application route.
#[derive(Clone, Debug)]
pub struct Route {
    /// Application alias.
    pub alias: String,
    /// Concrete topic, never a wildcard subscription.
    pub topic: String,
}
/// Per-publication MQTT metadata. Reply selection uses an application alias.
#[derive(Clone, Debug, Default)]
pub struct PublishOptions {
    /// Store the last value at the broker, not an end-consumer completion guarantee.
    pub retain: bool,
    /// Trusted reply route alias; raw inbound response topics are never followed.
    pub reply_alias: Option<String>,
    /// Opaque conversation token, at most 128 bytes.
    pub correlation: Option<Vec<u8>>,
    /// Bounded application user properties; no tenant/authentication meaning.
    pub user_properties: Vec<(String, String)>,
    /// Optional broker expiry in seconds.
    pub expiry_seconds: Option<u32>,
}
pub(crate) fn name(s: &str) -> Result<(), Error> {
    raddle::SubscriptionName::try_new(s).map_err(|_| Error::Invalid("name"))?;
    Ok(())
}
pub(crate) fn wait(wait: Duration) -> Result<(), Error> {
    if wait.is_zero() || wait > Duration::from_secs(60) {
        return Err(Error::Invalid("deadline"));
    }
    Ok(())
}
/// MQTT 5 topic/filter grammar with bounded UTF-8 and no NUL/control bytes.
pub fn validate_topic(topic: &str, filter: bool) -> Result<(), Error> {
    if topic.is_empty() || topic.len() > 1024 || topic.chars().any(|c| c.is_control()) {
        return Err(Error::Invalid("topic"));
    }
    let mut tokens = topic.split('/').peekable();
    while let Some(token) = tokens.next() {
        if token.contains(['+', '#'])
            && (!filter || !(token == "+" || (token == "#" && tokens.peek().is_none())))
        {
            return Err(Error::Invalid("topic wildcard"));
        }
    }
    Ok(())
}
pub(crate) fn properties(properties: &[(String, String)], limit: usize) -> Result<(), Error> {
    if properties.len() > limit
        || properties.iter().any(|(k, v)| {
            k.is_empty()
                || k.len() > 64
                || v.len() > 2048
                || k.chars().chain(v.chars()).any(|c| c == '\0')
        })
    {
        return Err(Error::Invalid("user properties"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_topic;
    #[test]
    fn wildcard_grammar_is_checked_before_queueing() {
        for t in ["a/+", "a/#", "/a//b"] {
            assert!(validate_topic(t, true).is_ok());
        }
        for t in ["", "a/#/b", "a+", "a\0b"] {
            assert!(validate_topic(t, true).is_err());
        }
        assert!(validate_topic("a/+", false).is_err());
    }
}
