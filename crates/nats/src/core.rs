use std::collections::BTreeMap;
use std::time::Duration;

use async_nats::{Client, ConnectOptions, Subscriber};
use futures_util::StreamExt;
use raddle::wire::{Envelope, EnvelopeLimits, WireError};

use crate::config;
use crate::{Bounds, Error, Route, Source};

/// Core NATS offers at-most-once delivery and flush, not durable publication confirmation.
#[derive(Clone)]
pub struct Core {
    pub(crate) client: Client,
    pub(crate) bounds: Bounds,
    pub(crate) routes: BTreeMap<String, String>,
}
impl Core {
    /// Connects using caller-owned TLS/auth options, including async JWT callbacks.
    /// The SDK owns its socket task; `close` drains the client explicitly.
    pub async fn connect(
        uri: &str,
        options: ConnectOptions,
        routes: Vec<Route>,
        bounds: Bounds,
    ) -> Result<Self, Error> {
        config::endpoint(uri)?;
        let bounds = bounds.validate()?;
        let routes = config::routes(routes)?;
        let client = tokio::time::timeout(
            bounds.operation_timeout,
            options
                .subscription_capacity(usize::from(bounds.prefetch))
                .request_timeout(Some(bounds.operation_timeout))
                .connect(uri),
        )
        .await
        .map_err(|_| Error::Uncertain)?
        .map_err(|_| Error::Uncertain)?;
        Ok(Self {
            client,
            bounds,
            routes,
        })
    }

    /// Writes then flushes. Success only proves the server processed prior socket commands.
    /// It does not prove a subscriber exists or retained anything.
    pub async fn send(&self, alias: &str, envelope: &Envelope) -> Result<(), Error> {
        let subject = self
            .routes
            .get(alias)
            .ok_or(Error::Invalid("unknown alias"))?;
        self.send_subject(subject, envelope).await
    }

    /// Explicit dynamic subject selected by application code. Received subjects/reply
    /// headers must never be forwarded here without a separate authorization decision.
    /// Like `send`, success is only a Core socket-command flush.
    pub async fn send_subject(&self, subject: &str, envelope: &Envelope) -> Result<(), Error> {
        config::validate_subject(subject, false)?;
        envelope
            .validate(self.bounds.envelope)
            .map_err(|_| Error::Invalid("envelope"))?;
        let bytes = envelope.to_vec().map_err(|_| Error::Invalid("envelope"))?;
        tokio::time::timeout(self.bounds.operation_timeout, async {
            self.client
                .publish(subject.to_owned(), bytes.into())
                .await
                .map_err(|_| Error::Uncertain)?;
            self.client.flush().await.map_err(|_| Error::Uncertain)
        })
        .await
        .map_err(|_| Error::Uncertain)?
    }

    /// Bounded Core request/reply, including a distinct no-responder result.
    /// Response bytes are decoded before returning; remote headers never select a route.
    pub async fn request(&self, alias: &str, envelope: &Envelope) -> Result<Envelope, Error> {
        let subject = self
            .routes
            .get(alias)
            .ok_or(Error::Invalid("unknown alias"))?;
        envelope
            .validate(self.bounds.envelope)
            .map_err(|_| Error::Invalid("envelope"))?;
        let bytes = envelope.to_vec().map_err(|_| Error::Invalid("envelope"))?;
        let message = tokio::time::timeout(
            self.bounds.operation_timeout,
            self.client.request(subject.clone(), bytes.into()),
        )
        .await
        .map_err(|_| Error::Uncertain)?
        .map_err(|e| match e.kind() {
            async_nats::RequestErrorKind::NoResponders => Error::NoResponders,
            _ => Error::Uncertain,
        })?;
        Envelope::from_slice(&message.payload, self.bounds.envelope)
            .map_err(|_| Error::Invalid("response"))
    }

    /// Creates a wildcard/queue subscription. This is not an inbox or a durable consumer.
    pub async fn subscribe(
        &self,
        subject: &str,
        queue: Option<&str>,
        source: Source,
    ) -> Result<CoreReceiver, Error> {
        config::validate_subject(subject, true)?;
        source.validate()?;
        if let Some(queue) = queue {
            config::name(queue)?;
        }
        let subscriber = tokio::time::timeout(self.bounds.operation_timeout, async {
            let subscriber = match queue {
                Some(queue) => {
                    self.client
                        .queue_subscribe(subject.to_owned(), queue.to_owned())
                        .await
                }
                None => self.client.subscribe(subject.to_owned()).await,
            }
            .map_err(|_| Error::Uncertain)?;
            self.client.flush().await.map_err(|_| Error::Uncertain)?;
            Ok(subscriber)
        })
        .await
        .map_err(|_| Error::Uncertain)??;
        Ok(CoreReceiver {
            subscriber,
            source,
            limits: self.bounds.envelope,
            client: self.client.clone(),
            timeout: self.bounds.operation_timeout,
        })
    }

    /// Requests SDK drain with a finite enqueue deadline. The SDK finishes socket cleanup
    /// asynchronously; observe its Closed event through caller-supplied ConnectOptions
    /// diagnostics when shutdown must wait for that final boundary. This is not a task join.
    pub async fn close(self) -> Result<(), Error> {
        tokio::time::timeout(self.bounds.operation_timeout, self.client.drain())
            .await
            .map_err(|_| Error::Uncertain)?
            .map_err(|_| Error::Uncertain)
    }
}

/// Bounded Core subscription. Dropping it unsubscribes; unreceived messages are lost.
pub struct CoreReceiver {
    subscriber: Subscriber,
    source: Source,
    limits: EnvelopeLimits,
    client: Client,
    timeout: Duration,
}
impl CoreReceiver {
    /// Waits at most the supplied total duration for one native member. A skipped
    /// scheduler control consumes this call's one-member page and returns None.
    pub async fn receive(&mut self, wait: Duration) -> Result<Option<CoreMessage>, Error> {
        config::wait(wait)?;
        match tokio::time::timeout(wait, self.subscriber.next()).await {
            Err(_) => Ok(None),
            Ok(None) => Err(Error::Uncertain),
            Ok(Some(message)) => {
                if message.headers.as_ref().is_some_and(|h| {
                    h.get("Nats-Schedule").is_some() && h.get("Nats-Scheduler").is_none()
                }) {
                    return Ok(None);
                }
                Ok(Some(CoreMessage {
                    message,
                    source: self.source.clone(),
                    limits: self.limits,
                    client: self.client.clone(),
                    timeout: self.timeout,
                }))
            }
        }
    }
}

/// A non-settleable Core message. Tenant and source are caller supplied.
pub struct CoreMessage {
    message: async_nats::Message,
    source: Source,
    limits: EnvelopeLimits,
    client: Client,
    timeout: Duration,
}
impl CoreMessage {
    /// Decodes bounded metadata; a malformed message does not end the subscription.
    pub fn decode(&self) -> Result<Envelope, WireError> {
        if let Some(headers) = &self.message.headers {
            config::headers(headers, self.limits.max_headers)
                .map_err(|_| WireError::Invalid("native headers"))?;
        }
        Envelope::from_slice(&self.message.payload, self.limits)
    }
    /// Custom inbound mapping retains Core source context, but cannot add native custody.
    pub fn decode_with<F>(&self, mapper: F) -> Result<Envelope, WireError>
    where
        F: FnOnce(&[u8]) -> Result<Envelope, WireError>,
    {
        if self.message.payload.len() > self.limits.max_bytes {
            return Err(WireError::Invalid("body limit"));
        }
        if let Some(headers) = &self.message.headers {
            config::headers(headers, self.limits.max_headers)
                .map_err(|_| WireError::Invalid("native headers"))?;
        }
        let envelope = mapper(&self.message.payload)?;
        envelope.validate(self.limits)?;
        Ok(envelope)
    }
    /// Trusted listener identity.
    pub fn source(&self) -> &Source {
        &self.source
    }
    /// Actual broker subject, not a trusted tenant identity.
    pub fn subject(&self) -> &str {
        &self.message.subject
    }
    /// Responds only to the original NATS inbox. Arbitrary received reply URIs are refused.
    pub async fn respond(&self, envelope: &Envelope) -> Result<(), Error> {
        let reply = self
            .message
            .reply
            .as_ref()
            .ok_or(Error::Invalid("no reply inbox"))?;
        if !reply.as_str().starts_with("_INBOX.") {
            return Err(Error::Invalid("untrusted reply subject"));
        }
        config::validate_subject(reply, false)?;
        envelope
            .validate(self.limits)
            .map_err(|_| Error::Invalid("envelope"))?;
        let bytes = envelope.to_vec().map_err(|_| Error::Invalid("envelope"))?;
        tokio::time::timeout(self.timeout, async {
            self.client
                .publish(reply.clone(), bytes.into())
                .await
                .map_err(|_| Error::Uncertain)?;
            self.client.flush().await.map_err(|_| Error::Uncertain)
        })
        .await
        .map_err(|_| Error::Uncertain)?
    }
}
