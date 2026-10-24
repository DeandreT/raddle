use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use azservicebus::core::BasicRetryPolicy;
use azservicebus::{
    ServiceBusClient, ServiceBusClientOptions, ServiceBusSenderOptions, ServiceBusTransportType,
};
use azure_core::credentials::TokenCredential;
use tokio::sync::Mutex;

use crate::Error;
use crate::publisher::Publisher;
use crate::receiver::{Receiver, Source};

pub(crate) type SharedClient = Arc<Mutex<Option<ServiceBusClient<BasicRetryPolicy>>>>;

/// Encrypted connection transport. Both modes preserve AMQP 1.0 semantics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum NetworkTransport {
    /// AMQP over TLS, normally port 5671.
    #[default]
    Tls,
    /// AMQP over secure WebSockets, normally port 443.
    WebSocket,
}

/// A shared connection factory. Construct bounded endpoint sets at startup.
#[derive(Clone)]
pub struct Connection {
    pub(crate) client: SharedClient,
}

fn options(network: NetworkTransport) -> ServiceBusClientOptions {
    ServiceBusClientOptions {
        transport_type: match network {
            NetworkTransport::Tls => ServiceBusTransportType::AmqpTcp,
            NetworkTransport::WebSocket => ServiceBusTransportType::AmqpWebSocket,
        },
        retry_options: azservicebus::ServiceBusRetryOptions {
            try_timeout: Duration::from_secs(30),
            ..Default::default()
        },
        ..Default::default()
    }
}

impl Connection {
    /// Opens an authenticated TLS connection. Never logs or retains a separate copy of credentials.
    pub async fn connect(
        connection_string: &str,
        network: NetworkTransport,
    ) -> Result<Self, Error> {
        let client = Box::pin(ServiceBusClient::new_from_connection_string(
            connection_string,
            options(network),
        ))
        .await
        .map_err(|_| Error::Service)?;
        Ok(Self {
            client: Arc::new(Mutex::new(Some(client))),
        })
    }

    /// Uses an Azure token credential, including managed identity implementations.
    pub async fn with_credential(
        namespace: &str,
        credential: Arc<dyn TokenCredential>,
        network: NetworkTransport,
    ) -> Result<Self, Error> {
        let client = Box::pin(ServiceBusClient::new_from_token_credential(
            namespace,
            credential,
            options(network),
        ))
        .await
        .map_err(|_| Error::Service)?;
        Ok(Self {
            client: Arc::new(Mutex::new(Some(client))),
        })
    }

    /// Opens plaintext only to an explicit loopback endpoint for emulator tests.
    #[cfg(feature = "emulator")]
    pub async fn emulator(
        connection_string: &str,
        endpoint: std::net::SocketAddr,
    ) -> Result<Self, Error> {
        if !endpoint.ip().is_loopback() {
            return Err(Error::Invalid("plaintext requires loopback"));
        }
        let mut options = options(NetworkTransport::Tls);
        options.custom_endpoint_address = Some(
            url::Url::parse(&format!("amqp://{endpoint}"))
                .map_err(|_| Error::Invalid("invalid emulator endpoint"))?,
        );
        options.retry_options.try_timeout = Duration::from_secs(5);
        options.retry_options.max_retries = 0;
        let client = Box::pin(
            ServiceBusClient::unsecured().new_from_connection_string(connection_string, options),
        )
        .await
        .map_err(|_| Error::Service)?;
        Ok(Self {
            client: Arc::new(Mutex::new(Some(client))),
        })
    }

    /// Opens a finite set of named outgoing routes; unknown aliases are rejected.
    pub async fn publisher(
        &self,
        routes: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Publisher, Error> {
        let routes: Vec<_> = routes.into_iter().take(257).collect();
        if routes.is_empty() || routes.len() > 256 {
            return Err(Error::Invalid("publisher needs 1..=256 routes"));
        }
        let mut unique = std::collections::HashSet::new();
        for (alias, entity) in &routes {
            raddle::SubscriptionName::try_new(alias.clone())
                .map_err(|_| Error::Invalid("invalid route alias"))?;
            validate_entity(entity)?;
            if !unique.insert(alias) {
                return Err(Error::Invalid("duplicate route alias"));
            }
        }
        let mut client = self.client.lock().await;
        let client = client.as_mut().ok_or(Error::Closed)?;
        let mut senders = HashMap::new();
        for (alias, entity) in routes {
            match Box::pin(client.create_sender(entity, ServiceBusSenderOptions::default())).await {
                Ok(sender) => {
                    senders.insert(alias, Mutex::new(sender));
                }
                Err(_) => {
                    for sender in senders.into_values() {
                        let _ = sender.into_inner().dispose().await;
                    }
                    return Err(Error::Service);
                }
            }
        }
        Ok(Publisher {
            senders,
            _connection: Arc::clone(&self.client),
        })
    }

    /// Opens a peek-lock receiver with bounded prefetch and optional session ownership.
    pub async fn receiver(
        &self,
        source: Source,
        session: Option<&str>,
        prefetch: u32,
    ) -> Result<Receiver, Error> {
        Box::pin(Receiver::open(self, source, session, prefetch)).await
    }

    /// Closes the shared connection. Dispose receivers/publishers first to settle gracefully.
    pub async fn close(&self) -> Result<(), Error> {
        if let Some(client) = self.client.lock().await.take() {
            Box::pin(client.dispose())
                .await
                .map_err(|_| Error::Service)?;
        }
        Ok(())
    }
}

pub(crate) fn validate_entity(entity: &str) -> Result<(), Error> {
    if entity.is_empty()
        || entity.len() > 260
        || entity
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        || !entity
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'-' | b'_' | b'.'))
    {
        return Err(Error::Invalid("invalid entity path"));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::{Connection, NetworkTransport, validate_entity};
    use crate::Source;

    #[test]
    fn entity_paths_cannot_smuggle_urls_or_control_segments() {
        for value in [
            "",
            "amqps://foreign/queue",
            "../queue",
            "a//b",
            "a\n",
            "a?b",
        ] {
            assert!(validate_entity(value).is_err());
        }
        assert!(validate_entity("orders/europe-1").is_ok());
    }

    #[test]
    fn public_futures_do_not_embed_large_sdk_state_machines() {
        let connection = Connection {
            client: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
        };
        assert!(std::mem::size_of_val(&Connection::connect("", NetworkTransport::Tls)) < 65536);
        assert!(std::mem::size_of_val(&connection.publisher(std::iter::empty())) < 65536);
        assert!(
            std::mem::size_of_val(&connection.receiver(Source::Queue("orders".into()), None, 0))
                < 65536
        );
    }

    #[tokio::test]
    #[cfg(feature = "emulator")]
    async fn plaintext_refuses_non_loopback_endpoints_before_connecting() {
        let endpoint = "192.0.2.1:5672".parse().expect("literal test address");
        assert!(Connection::emulator("", endpoint).await.is_err());
    }
}
