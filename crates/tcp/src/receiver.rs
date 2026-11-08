use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use raddle::wire::Envelope;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::codec::Codec;
use crate::config::{Error, Limits};
use crate::framing;

/// Network origin, not an authenticated application or tenant identity.
#[derive(Clone, Copy, Debug)]
pub struct Peer {
    /// Observed remote socket address. Do not trust envelope headers as claims.
    pub address: SocketAddr,
}

/// Consumer-owned disposition. A refusal never prevents later framed messages.
#[derive(Debug)]
#[non_exhaustive]
pub enum Delivery {
    /// Custody has completed; return this only after any required durable commit.
    Accepted,
    /// No positive custody acknowledgement; sender retains retry responsibility.
    Retry,
    /// Permanently refused. No raw error details are sent to the peer.
    Rejected,
    /// Custody completed with a reply correlated and caused by the request ID.
    Reply(Envelope),
}

/// Typed custody boundary. Rust handler failures must become an explicit
/// disposition; a panic or cancellation drops the connection without confirming.
pub trait Consumer: Send + Sync + 'static {
    /// Accepts one validated envelope. Configure tenant and authorization through
    /// independently trusted application state, never the untrusted peer payload.
    fn accept(&self, peer: Peer, envelope: Envelope) -> impl Future<Output = Delivery> + Send;
}

/// A listener with no background tasks until the caller runs it.
pub struct Receiver {
    listener: TcpListener,
    limits: Limits,
    codec: Codec,
}

impl Receiver {
    /// Binds an explicit address. Raw TCP supplies no TLS or authentication.
    pub async fn bind(address: SocketAddr, limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        let listener = TcpListener::bind(address)
            .await
            .map_err(|_| Error::Configuration)?;
        Ok(Self {
            listener,
            limits,
            codec: Codec::default(),
        })
    }

    /// Enables explicit RDTC2 selected-body framing while retaining RDTC1 JSON
    /// clients. No content sniffing changes the legacy default. Canonical decode
    /// completes before the consumer can acknowledge custody.
    #[cfg(feature = "serialization")]
    pub fn with_codecs(mut self, codecs: Arc<raddle_serialization::EndpointCodecs>) -> Self {
        self.codec = Codec {
            selected: Some(codecs),
        };
        self
    }

    /// Bound local address, including an OS-assigned port when binding port zero.
    pub fn local_addr(&self) -> Result<SocketAddr, Error> {
        self.listener.local_addr().map_err(|_| Error::Configuration)
    }

    /// Runs bounded connections until cancellation. Shutdown aborts and joins
    /// every in-flight custody future; it does not imply business work rolled back.
    /// Dropping this future also aborts its owned tasks through `JoinSet` drop.
    pub async fn run<C: Consumer>(
        self,
        consumer: Arc<C>,
        cancellation: CancellationToken,
    ) -> Result<(), Error> {
        let mut connections = JoinSet::new();
        let result = loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => break Ok(()),
                _ = connections.join_next(), if !connections.is_empty() => {},
                accepted = self.listener.accept(), if connections.len() < self.limits.connections => {
                    let (stream, address) = match accepted {
                        Ok(accepted) => accepted,
                        Err(_) => break Err(Error::Uncertain),
                    };
                    let consumer = Arc::clone(&consumer);
                    let limits = self.limits;
                    let codec = self.codec.clone();
                    let deadline = tokio::time::Instant::now() + limits.deadline;
                    connections.spawn(async move {
                        let _ = tokio::time::timeout_at(
                            deadline,
                            serve(stream, Peer { address }, limits, codec, consumer),
                        ).await;
                    });
                }
            }
        };
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        result
    }
}

async fn serve<C: Consumer>(
    mut stream: TcpStream,
    peer: Peer,
    limits: Limits,
    codec: Codec,
    consumer: Arc<C>,
) -> Result<(), Error> {
    stream.set_nodelay(true).map_err(|_| Error::Uncertain)?;
    let mut magic = [0; 5];
    stream
        .read_exact(&mut magic)
        .await
        .map_err(|_| Error::Uncertain)?;
    let selected = magic == *framing::SELECTED_MAGIC;
    if magic != *framing::MAGIC && !(selected && codec.selected()) {
        return Err(Error::Rejected);
    }
    let count = usize::from(stream.read_u16().await.map_err(|_| Error::Uncertain)?);
    if count == 0 || count > limits.batch_messages {
        return Err(Error::Rejected);
    }
    let mut remaining = limits.batch_bytes;
    for _ in 0..count {
        let bytes =
            framing::read_frame(&mut stream, limits.envelope.max_bytes.min(remaining)).await?;
        remaining -= bytes.len();
        let envelope = match codec.decode(&bytes, limits.envelope, selected) {
            Ok(envelope) => envelope,
            Err(_) => {
                stream
                    .write_u8(framing::REJECTED)
                    .await
                    .map_err(|_| Error::Uncertain)?;
                continue;
            }
        };
        let request_id = envelope.id();
        let disposition = consumer.accept(peer, envelope).await;
        let (status, reply) = match disposition {
            Delivery::Accepted => (framing::ACCEPTED, None),
            Delivery::Retry => (framing::RETRY, None),
            Delivery::Rejected => (framing::REJECTED, None),
            Delivery::Reply(reply) => {
                if reply.correlation_id() != Some(request_id)
                    || reply.causation_id() != Some(request_id)
                    || reply.validate(limits.envelope).is_err()
                {
                    (framing::RETRY, None)
                } else {
                    match codec.encode(&reply, limits.envelope, selected) {
                        Ok(bytes) if bytes.len() <= remaining => {
                            remaining -= bytes.len();
                            (framing::REPLY, Some(bytes))
                        }
                        Ok(_) => (framing::RETRY, None),
                        Err(_) => (framing::RETRY, None),
                    }
                }
            }
        };
        stream
            .write_u8(status)
            .await
            .map_err(|_| Error::Uncertain)?;
        if let Some(bytes) = reply {
            framing::write_frame(&mut stream, &bytes).await?;
        }
    }
    Ok(())
}
