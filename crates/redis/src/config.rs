use std::time::Duration;

use raddle::wire::EnvelopeLimits;

/// Redacted broker errors; an I/O timeout may follow a successful Redis write.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid application configuration or wire metadata.
    #[error("invalid Redis configuration or message: {0}")]
    Invalid(&'static str),
    /// Network/server failure with potentially accepted writes.
    #[error("Redis operation failed or outcome is uncertain")]
    Uncertain,
    /// Native command or required durability policy unavailable.
    #[error("required Redis capability or durability policy is unavailable")]
    Unsupported,
    /// Another receiver owns this pending entry, or it was already settled.
    #[error("Redis delivery ownership lost")]
    Lost,
}

/// Acknowledgement is never emulated by XACK followed by XDEL.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AckMode {
    /// Remove the group's pending entry, retaining the stream body.
    Retain,
    /// Redis 8.2+ atomic XACKDEL with ACKED: delete only after every group acknowledged.
    DeleteWhenAcknowledged,
}
/// Required startup persistence policy, not a promise of failover survival.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Durability {
    /// Explicitly accepts volatile Redis broker custody.
    Volatile,
    /// Requires noeviction and AOF; everysec can still lose recent accepted writes.
    AppendOnly,
    /// Requires noeviction, AOF, and appendfsync always.
    FsyncAlways,
}
/// Whether the caller authorizes group creation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Ownership {
    /// Application-owned stream/group: XGROUP CREATE MKSTREAM is permitted.
    Owned,
    /// Verify an existing group; no creation or reset.
    External,
}
/// Trusted application context; never sourced from the message body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Source {
    /// Named broker identity.
    pub client: String,
    /// Statically configured/authenticated tenant.
    pub tenant: String,
    /// Logical listener identity.
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
/// Frozen alias-to-stream mapping, supplied by the application.
#[derive(Clone, Debug)]
pub struct Route {
    /// Stable endpoint alias.
    pub alias: String,
    /// Redis stream key, including a caller-selected hash tag for cluster scripts.
    pub stream: String,
}
/// Finite protocol and receive bounds.
#[derive(Clone, Debug)]
pub struct Config {
    /// Operation deadline, one millisecond through sixty seconds.
    pub timeout: Duration,
    /// Maximum receive page and outstanding entries per receiver, one through 256.
    pub prefetch: u16,
    /// Body/header/identity bounds on send/decode.
    pub envelope: EnvelopeLimits,
    /// Startup-validated acknowledgement policy.
    pub acknowledgement: AckMode,
    /// Startup-validated Redis persistence/eviction requirements.
    pub durability: Durability,
    /// Failed scheduled targets are deferred by this bounded delay so a page of size
    /// one cannot repeatedly select a poison entry ahead of unrelated due work.
    pub schedule_retry_delay: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            prefetch: 64,
            envelope: EnvelopeLimits::default(),
            acknowledgement: AckMode::Retain,
            durability: Durability::AppendOnly,
            schedule_retry_delay: Duration::from_secs(1),
        }
    }
}
impl Config {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        wait(self.timeout)?;
        if !(1..=256).contains(&self.prefetch)
            || self.envelope.max_bytes == 0
            || self.envelope.max_bytes > 1_048_576
            || self.envelope.max_headers > 32
            || self.schedule_retry_delay < Duration::from_millis(1)
            || self.schedule_retry_delay > Duration::from_secs(3600)
        {
            return Err(Error::Invalid("bounds"));
        }
        Ok(())
    }
}
pub(crate) fn wait(wait: Duration) -> Result<(), Error> {
    if wait < Duration::from_millis(1) || wait > Duration::from_secs(60) {
        return Err(Error::Invalid("deadline"));
    }
    Ok(())
}
pub(crate) fn name(value: &str) -> Result<(), Error> {
    raddle::SubscriptionName::try_new(value).map_err(|_| Error::Invalid("name"))?;
    Ok(())
}
pub(crate) fn key(value: &str) -> Result<(), Error> {
    if value.is_empty() || value.len() > 256 || value.bytes().any(|b| b <= 32 || b == 127) {
        return Err(Error::Invalid("key"));
    }
    Ok(())
}
pub(crate) fn endpoint(uri: &str) -> Result<(), Error> {
    let url = url::Url::parse(uri).map_err(|_| Error::Invalid("URI"))?;
    let host = url.host_str().ok_or(Error::Invalid("host"))?;
    let local = host
        .trim_matches(['[', ']'])
        .parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback());
    if url.fragment().is_some() || !(url.scheme() == "rediss" || (url.scheme() == "redis" && local))
    {
        return Err(Error::Invalid("TLS required outside numeric loopback"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Config, endpoint, key, name};
    use std::time::Duration;
    #[test]
    fn configuration_fails_before_broker_io() {
        assert!(
            Config {
                prefetch: 257,
                ..Config::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            Config {
                timeout: Duration::ZERO,
                ..Config::default()
            }
            .validate()
            .is_err()
        );
        for uri in [
            "redis://localhost/",
            "redis://192.0.2.1/",
            "redis://127.0.0.1/#bad",
        ] {
            assert!(endpoint(uri).is_err());
        }
        for uri in ["redis://127.0.0.1/1", "rediss://redis.example/2"] {
            assert!(endpoint(uri).is_ok());
        }
        assert!(key("bad key").is_err());
        assert!(name("untrusted/tenant").is_err());
    }
}
