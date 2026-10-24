use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use lapin::options::{
    BasicCancelOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Confirmation, ConnectionProperties, Consumer};
use raddle::transport::{Transport, TransportError};
use raddle::wire::Envelope;

use crate::{Error, HeldMessage};

/// A configured remote route; queue routing may use the empty default exchange.
#[derive(Clone, Debug)]
pub struct Route {
    /// Stable application alias.
    pub alias: String,
    /// Existing exchange name.
    pub exchange: String,
    /// Binding/routing key.
    pub routing_key: String,
}

/// Shared broker connection; production URIs must use verified TLS.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<lapin::Connection>,
}
impl Connection {
    /// Connects with automatic recovery. Plaintext is restricted to numeric loopback.
    pub async fn connect(uri: &str) -> Result<Self, Error> {
        let parsed = url::Url::parse(uri).map_err(|_| Error::Invalid("invalid URI"))?;
        validate_endpoint(&parsed)?;
        let inner =
            lapin::Connection::connect(uri, ConnectionProperties::default().enable_auto_recover())
                .await
                .map_err(|_| Error::Broker)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }
    /// Enables publisher confirms on a dedicated channel and freezes named routes.
    pub async fn publisher(
        &self,
        routes: impl IntoIterator<Item = Route>,
    ) -> Result<Publisher, Error> {
        let mut configured = HashMap::new();
        for route in routes.into_iter().take(257) {
            raddle::SubscriptionName::try_new(route.alias.clone())
                .map_err(|_| Error::Invalid("invalid route alias"))?;
            if route.exchange.len() > 255
                || route.routing_key.len() > 255
                || configured.len() >= 256
            {
                return Err(Error::Invalid("route exceeds limits"));
            }
            if configured.insert(route.alias.clone(), route).is_some() {
                return Err(Error::Invalid("duplicate route alias"));
            }
        }
        if configured.is_empty() {
            return Err(Error::Invalid("no routes"));
        }
        let channel = self
            .inner
            .create_channel()
            .await
            .map_err(|_| Error::Broker)?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|_| Error::Broker)?;
        Ok(Publisher {
            channel,
            routes: configured,
            _connection: Arc::clone(&self.inner),
        })
    }
    /// Creates an explicit-ack consumer with a finite broker prefetch window.
    pub async fn receiver(&self, queue: &str, prefetch: u16) -> Result<Receiver, Error> {
        if queue.is_empty() || queue.len() > 255 || !(1..=256).contains(&prefetch) {
            return Err(Error::Invalid("invalid consumer bounds"));
        }
        let channel = self
            .inner
            .create_channel()
            .await
            .map_err(|_| Error::Broker)?;
        channel
            .basic_qos(prefetch, BasicQosOptions::default())
            .await
            .map_err(|_| Error::Broker)?;
        let tag = format!("raddle-{}", uuid::Uuid::new_v4());
        let consumer = channel
            .basic_consume(
                queue.into(),
                tag.clone().into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|_| Error::Broker)?;
        Ok(Receiver {
            channel,
            consumer,
            tag,
            _connection: Arc::clone(&self.inner),
        })
    }
    /// Closes the shared socket after application-owned work has drained.
    pub async fn close(&self) -> Result<(), Error> {
        self.inner
            .close(200, "shutdown".into())
            .await
            .map_err(|_| Error::Broker)
    }
}

/// A confirmed publisher. No route is derived from untrusted reply URLs.
pub struct Publisher {
    channel: Channel,
    routes: HashMap<String, Route>,
    _connection: Arc<lapin::Connection>,
}
impl Transport for Publisher {
    async fn publish(&self, destination: &str, envelope: &Envelope) -> Result<(), TransportError> {
        let route = self
            .routes
            .get(destination)
            .ok_or(TransportError::Rejected)?;
        let bytes = envelope.to_vec().map_err(|_| TransportError::Rejected)?;
        if bytes.len() > 1_048_576 {
            return Err(TransportError::Rejected);
        }
        let confirm = self
            .channel
            .basic_publish(
                route.exchange.clone().into(),
                route.routing_key.clone().into(),
                BasicPublishOptions {
                    mandatory: true,
                    ..Default::default()
                },
                &bytes,
                BasicProperties::default()
                    .with_delivery_mode(2)
                    .with_content_type("application/json".into())
                    .with_message_id(envelope.id().to_string().into()),
            )
            .await
            .map_err(|_| TransportError::Transient)?
            .await
            .map_err(|_| TransportError::Transient)?;
        match confirm {
            Confirmation::Ack(None) => Ok(()),
            Confirmation::Ack(Some(_)) => Err(TransportError::Rejected),
            Confirmation::Nack(_) => Err(TransportError::Transient),
            Confirmation::NotRequested => Err(TransportError::Rejected),
        }
    }
}
impl Publisher {
    /// Closes the channel. Join concurrent publications before calling this.
    pub async fn close(self) -> Result<(), Error> {
        self.channel
            .close(200, "shutdown".into())
            .await
            .map_err(|_| Error::Broker)
    }
}

/// One finite-prefetch consumer stream.
pub struct Receiver {
    channel: Channel,
    consumer: Consumer,
    tag: String,
    _connection: Arc<lapin::Connection>,
}
impl Receiver {
    /// Receives one delivery or returns `None` when the caller's wait expires.
    pub async fn receive(&mut self, wait: Duration) -> Result<Option<HeldMessage>, Error> {
        use futures_util::stream::StreamExt;
        if wait.is_zero() || wait > Duration::from_secs(60) {
            return Err(Error::Invalid("invalid receive timeout"));
        }
        match tokio::time::timeout(wait, self.consumer.next()).await {
            Err(_) => Ok(None),
            Ok(Some(Ok(delivery))) => Ok(Some(HeldMessage {
                delivery,
                settled: false,
            })),
            Ok(Some(Err(_))) => Err(Error::Broker),
            Ok(None) => Err(Error::Closed),
        }
    }
    /// Cancels consumption and closes the channel, requeuing outstanding deliveries.
    pub async fn close(self) -> Result<(), Error> {
        let cancel = self
            .channel
            .basic_cancel(self.tag.into(), BasicCancelOptions::default())
            .await;
        let close = self.channel.close(200, "shutdown".into()).await;
        cancel.and(close).map_err(|_| Error::Broker)
    }
}

fn validate_endpoint(parsed: &url::Url) -> Result<(), Error> {
    if parsed.host().is_none() || parsed.fragment().is_some() {
        return Err(Error::Invalid("invalid broker endpoint"));
    }
    let loopback = match parsed.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        // For non-special schemes URL preserves IPv4 literals as opaque domains.
        Some(url::Host::Domain(host)) => host
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|ip| ip.is_loopback()),
        _ => false,
    };
    match parsed.scheme() {
        "amqps" => Ok(()),
        "amqp" if loopback => Ok(()),
        _ => Err(Error::Invalid("TLS required outside numeric loopback")),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::validate_endpoint;

    #[test]
    fn plaintext_is_limited_to_numeric_loopback_including_ipv6() {
        for uri in [
            "amqp://127.0.0.1/",
            "amqp://[::1]/",
            "amqps://broker.example/",
        ] {
            assert!(validate_endpoint(&url::Url::parse(uri).unwrap()).is_ok());
        }
        for uri in [
            "amqp://localhost/",
            "amqp://192.0.2.1/",
            "amqp://[2001:db8::1]/",
            "https://broker.example/",
            "amqps://broker.example/#fragment",
        ] {
            assert!(validate_endpoint(&url::Url::parse(uri).unwrap()).is_err());
        }
    }
}
