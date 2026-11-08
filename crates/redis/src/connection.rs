use std::collections::BTreeMap;

use raddle::transport::{Transport, TransportError};
use raddle::wire::Envelope;
use redis::aio::MultiplexedConnection;
use redis::{AsyncConnectionConfig, FromRedisValue};

use crate::config;
use crate::{AckMode, Config, Durability, Error, Route};

/// Actual startup observations. Redis acknowledgement is not a database commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Capabilities {
    /// Redis reports XACKDEL through COMMAND INFO.
    pub atomic_ack_delete: bool,
    /// Effective maxmemory-policy.
    pub eviction_policy: String,
    /// Effective appendonly setting.
    pub append_only: bool,
    /// Effective appendfsync setting.
    pub append_fsync: String,
}
/// Confirmed XADD result; acceptance does not mean replica/fsync or consumer completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Receipt {
    /// Physical stream key.
    pub stream: String,
    /// Stable Redis stream entry ID.
    pub id: String,
}
/// Named/tenant connections are separate instances with separate frozen aliases.
/// External multiplexers are cloned, never closed, reauthenticated, or globally disposed.
#[derive(Clone)]
pub struct Broker {
    pub(crate) connection: MultiplexedConnection,
    pub(crate) config: Config,
    pub(crate) routes: BTreeMap<String, String>,
    capabilities: Capabilities,
}
impl Broker {
    /// Opens a bounded TLS/authenticated connection. Credentials must not be logged.
    pub async fn connect(uri: &str, config: Config, routes: Vec<Route>) -> Result<Self, Error> {
        config::endpoint(uri)?;
        config.validate()?;
        let client = redis::Client::open(uri).map_err(|_| Error::Invalid("URI"))?;
        let options = AsyncConnectionConfig::new()
            .set_connection_timeout(config.timeout)
            .set_response_timeout(config.timeout);
        let connection = tokio::time::timeout(
            config.timeout,
            client.get_multiplexed_async_connection_with_config(&options),
        )
        .await
        .map_err(|_| Error::Uncertain)?
        .map_err(|_| Error::Uncertain)?;
        Self::from_connection(connection, config, routes).await
    }
    /// Verifies capabilities/policies on an application-owned multiplexer, allowing custom
    /// token-refresh connectors without taking ownership of their lifecycle.
    pub async fn from_connection(
        mut connection: MultiplexedConnection,
        config: Config,
        routes: Vec<Route>,
    ) -> Result<Self, Error> {
        config.validate()?;
        if routes.is_empty() || routes.len() > 256 {
            return Err(Error::Invalid("route count"));
        }
        let mut configured = BTreeMap::new();
        for route in routes {
            config::name(&route.alias)?;
            config::key(&route.stream)?;
            if configured.insert(route.alias, route.stream).is_some() {
                return Err(Error::Invalid("duplicate route"));
            }
        }
        // This changes only our clone's request timeout, not another owner's credentials.
        connection.set_response_timeout(config.timeout);
        let capabilities = tokio::time::timeout(config.timeout, async {
            let commands: redis::Value = redis::cmd("COMMAND").arg("INFO").arg("XACKDEL").query_async(&mut connection).await.map_err(|_| Error::Unsupported)?;
            let atomic_ack_delete = matches!(commands, redis::Value::Array(ref a) if matches!(a.first(), Some(redis::Value::Array(_))));
            let settings: BTreeMap<String, String> = redis::cmd("CONFIG").arg("GET").arg("maxmemory-policy").arg("appendonly").arg("appendfsync")
                .query_async(&mut connection).await.map_err(|_| Error::Unsupported)?;
            let capabilities = Capabilities {
                atomic_ack_delete,
                eviction_policy: settings.get("maxmemory-policy").cloned().ok_or(Error::Unsupported)?,
                append_only: settings.get("appendonly").is_some_and(|v| v == "yes"),
                append_fsync: settings.get("appendfsync").cloned().ok_or(Error::Unsupported)?,
            };
            if config.acknowledgement == AckMode::DeleteWhenAcknowledged && !atomic_ack_delete { return Err(Error::Unsupported); }
            if config.durability != Durability::Volatile && (capabilities.eviction_policy != "noeviction" || !capabilities.append_only) { return Err(Error::Unsupported); }
            if config.durability == Durability::FsyncAlways && capabilities.append_fsync != "always" { return Err(Error::Unsupported); }
            Ok(capabilities)
        }).await.map_err(|_| Error::Uncertain)??;
        Ok(Self {
            connection,
            config,
            routes: configured,
            capabilities,
        })
    }
    /// Startup observations, safe to expose in diagnostics.
    pub fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
    /// Adds exactly one entry and waits for its actual XADD reply.
    /// Retrying an uncertain XADD can duplicate a stream entry; the envelope ID is stable.
    pub async fn send(&self, alias: &str, envelope: &Envelope) -> Result<Receipt, Error> {
        let stream = self
            .routes
            .get(alias)
            .ok_or(Error::Invalid("unknown alias"))?;
        envelope
            .validate(self.config.envelope)
            .map_err(|_| Error::Invalid("envelope"))?;
        let bytes = envelope.to_vec().map_err(|_| Error::Invalid("envelope"))?;
        let id: String = self
            .query(
                redis::cmd("XADD")
                    .arg(stream)
                    .arg("*")
                    .arg("body")
                    .arg(bytes)
                    .arg("message-id")
                    .arg(envelope.id().to_string()),
            )
            .await?;
        Ok(Receipt {
            stream: stream.clone(),
            id,
        })
    }
    pub(crate) async fn query<T: FromRedisValue>(&self, cmd: &redis::Cmd) -> Result<T, Error> {
        tokio::time::timeout(
            self.config.timeout,
            cmd.query_async(&mut self.connection.clone()),
        )
        .await
        .map_err(|_| Error::Uncertain)?
        .map_err(|_| Error::Uncertain)
    }
    pub(crate) async fn script<T: FromRedisValue>(
        &self,
        script: &redis::ScriptInvocation<'_>,
    ) -> Result<T, Error> {
        tokio::time::timeout(
            self.config.timeout,
            script.invoke_async(&mut self.connection.clone()),
        )
        .await
        .map_err(|_| Error::Uncertain)?
        .map_err(|_| Error::Uncertain)
    }
}
impl Transport for Broker {
    async fn publish(&self, destination: &str, envelope: &Envelope) -> Result<(), TransportError> {
        self.send(destination, envelope)
            .await
            .map(|_| ())
            .map_err(|e| match e {
                Error::Invalid(_) | Error::Unsupported => TransportError::Rejected,
                _ => TransportError::Transient,
            })
    }
}
