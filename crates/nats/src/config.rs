use std::collections::BTreeMap;
use std::time::Duration;

use raddle::wire::EnvelopeLimits;
use sha2::{Digest, Sha256};

/// Redacted errors. Uncertainty must not be interpreted as a negative broker acknowledgement.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid configuration or untrusted wire data.
    #[error("invalid NATS configuration or message: {0}")]
    Invalid(&'static str),
    /// A deadline, disconnect or missing acknowledgement leaves the outcome unknown.
    #[error("NATS operation failed or outcome is uncertain")]
    Uncertain,
    /// Core request received the protocol's no-responder response.
    #[error("no NATS responder")]
    NoResponders,
    /// Explicit negative broker response, such as a failed conditional publication.
    #[error("NATS broker refused the operation")]
    Refused,
    /// A delivery has expired or already been settled.
    #[error("delivery is no longer owned")]
    Lost,
    /// External resource differs from the requested properties.
    #[error("NATS resource configuration differs")]
    Drift,
    /// Required broker capability is not available.
    #[error("NATS capability unavailable")]
    Unsupported,
}

/// All I/O and receive pages are finite. A receive call has one total deadline.
#[derive(Clone, Copy, Debug)]
pub struct Bounds {
    /// Deadline per broker exchange, at most sixty seconds. Multi-step schedule/DLQ
    /// operations compose bounded exchanges; receive uses one total caller deadline.
    pub operation_timeout: Duration,
    /// Maximum held broker window, from one to 256.
    pub prefetch: u16,
    /// Envelope limits checked on both send and decode.
    pub envelope: EnvelopeLimits,
}
impl Default for Bounds {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(5),
            prefetch: 64,
            envelope: EnvelopeLimits::default(),
        }
    }
}
impl Bounds {
    pub(crate) fn validate(self) -> Result<Self, Error> {
        wait(self.operation_timeout)?;
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

/// Application-owned alias to subject. No subject is taken from received headers.
#[derive(Clone, Debug)]
pub struct Route {
    /// Stable application alias.
    pub alias: String,
    /// Concrete publication subject.
    pub subject: String,
}

/// Trusted receive context supplied by the application, not by a wire tenant header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Source {
    /// Stable logical broker/account alias, not a process or physical socket ID.
    pub client: String,
    /// Authenticated or statically configured tenant.
    pub tenant: String,
    /// Logical endpoint identity.
    pub endpoint: String,
}
impl Source {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        for value in [&self.client, &self.tenant, &self.endpoint] {
            name(value)?;
        }
        Ok(())
    }
}

pub(crate) fn name(value: &str) -> Result<(), Error> {
    raddle::SubscriptionName::try_new(value).map_err(|_| Error::Invalid("name"))?;
    Ok(())
}
pub(crate) fn wait(value: Duration) -> Result<(), Error> {
    if value.is_zero() || value > Duration::from_secs(60) {
        return Err(Error::Invalid("deadline"));
    }
    Ok(())
}

/// Checks NATS token/wildcard grammar before contacting the broker.
pub fn validate_subject(subject: &str, wildcard: bool) -> Result<(), Error> {
    if subject.is_empty() || subject.len() > 1024 || subject.bytes().any(|b| b <= 32 || b == 127) {
        return Err(Error::Invalid("subject"));
    }
    let mut tokens = subject.split('.').peekable();
    while let Some(token) = tokens.next() {
        if token.is_empty()
            || (token.contains(['*', '>'])
                && (!wildcard || !(token == "*" || (token == ">" && tokens.peek().is_none()))))
        {
            return Err(Error::Invalid("subject wildcard"));
        }
    }
    Ok(())
}

pub(crate) fn routes(routes: Vec<Route>) -> Result<BTreeMap<String, String>, Error> {
    if routes.is_empty() || routes.len() > 256 {
        return Err(Error::Invalid("route count"));
    }
    let mut result = BTreeMap::new();
    for route in routes {
        name(&route.alias)?;
        validate_subject(&route.subject, false)?;
        if result.insert(route.alias, route.subject).is_some() {
            return Err(Error::Invalid("duplicate alias"));
        }
    }
    Ok(result)
}

pub(crate) fn endpoint(uri: &str) -> Result<(), Error> {
    let parsed = url::Url::parse(uri).map_err(|_| Error::Invalid("endpoint"))?;
    let host = parsed.host_str().ok_or(Error::Invalid("endpoint"))?;
    let loopback = host
        .trim_matches(['[', ']'])
        .parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback());
    if parsed.fragment().is_some()
        || !(parsed.scheme() == "tls" || (parsed.scheme() == "nats" && loopback))
    {
        return Err(Error::Invalid("TLS required outside numeric loopback"));
    }
    Ok(())
}

pub(crate) fn headers(headers: &async_nats::HeaderMap, limit: usize) -> Result<(), Error> {
    let mut count = 0;
    for (name, values) in headers.iter() {
        count += values.len();
        let name: &str = name.as_ref();
        if name.len() > 64 || values.iter().any(|v| v.as_str().len() > 2048) {
            return Err(Error::Invalid("native headers"));
        }
    }
    if count > limit {
        return Err(Error::Invalid("native header count"));
    }
    Ok(())
}

pub(crate) fn dedup_key(source: &Source, purpose: &str, parts: &[&str]) -> String {
    let mut digest = Sha256::new();
    for part in [&source.client, &source.tenant, &source.endpoint]
        .into_iter()
        .map(String::as_str)
        .chain(std::iter::once(purpose))
        .chain(parts.iter().copied())
    {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    format!("raddle-{:x}", digest.finalize())
}

pub(crate) fn metadata(value: &std::collections::HashMap<String, String>) -> Result<(), Error> {
    if value.len() > 32
        || value.iter().any(|(key, value)| {
            key.starts_with("_nats.") || key.is_empty() || key.len() > 64 || value.len() > 2048
        })
    {
        return Err(Error::Invalid("resource metadata"));
    }
    Ok(())
}
pub(crate) fn metadata_matches(
    expected: &std::collections::HashMap<String, String>,
    actual: &std::collections::HashMap<String, String>,
) -> bool {
    expected
        .iter()
        .all(|(key, value)| actual.get(key) == Some(value))
        && actual
            .keys()
            .filter(|key| !key.starts_with("_nats."))
            .count()
            == expected.len()
}

#[cfg(test)]
mod tests {
    use super::{Bounds, Source, dedup_key, endpoint, validate_subject};
    #[test]
    fn grammar_and_bounds_are_strict() {
        for s in ["a.*", "a.>", "a.b"] {
            assert!(validate_subject(s, true).is_ok());
        }
        for s in ["", "a..b", "a.>.b", "a*b", "a b"] {
            assert!(validate_subject(s, true).is_err());
        }
        assert!(validate_subject("a.*", false).is_err());
        assert!(endpoint("nats://localhost:4222").is_err());
        assert!(endpoint("nats://127.0.0.1:4222").is_ok());
        assert!(
            Bounds {
                prefetch: 257,
                ..Bounds::default()
            }
            .validate()
            .is_err()
        );
    }
    #[test]
    fn deduplication_frames_every_trusted_namespace_component() {
        let a = Source {
            client: "fixture".into(),
            tenant: "a".into(),
            endpoint: "bc".into(),
        };
        let b = Source {
            client: "fixture".into(),
            tenant: "ab".into(),
            endpoint: "c".into(),
        };
        let key = dedup_key(&a, "publish", &["orders", "same-id"]);
        assert_eq!(key, dedup_key(&a, "publish", &["orders", "same-id"]));
        assert_ne!(key, dedup_key(&b, "publish", &["orders", "same-id"]));
        assert_ne!(key, dedup_key(&a, "schedule", &["orders", "same-id"]));
        assert_ne!(key, dedup_key(&a, "publish", &["other-orders", "same-id"]));
        assert!(key.len() < 128);
    }
}
