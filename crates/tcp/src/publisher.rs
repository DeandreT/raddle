use std::collections::BTreeMap;

use raddle::SubscriptionName;
use raddle::transport::{Transport, TransportError};
use raddle::wire::Envelope;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

use crate::config::{Endpoint, Error, Limits};
use crate::framing;

/// One positively observed receiver decision, independent of other batch items.
#[derive(Debug)]
#[non_exhaustive]
pub enum Receipt {
    /// The consumer declared custody. Application completion is not implied.
    Accepted,
    /// The consumer declared custody and returned a validated correlated reply.
    Reply(Envelope),
}

/// Confirmed publisher to finite configured aliases. Each batch reconnects;
/// uncertain work is never automatically replayed, even on a fresh connection.
pub struct Publisher {
    endpoints: BTreeMap<String, Endpoint>,
    limits: Limits,
    admission: Semaphore,
}

impl Publisher {
    /// Builds a finite route table. Callers own DNS and network trust decisions.
    pub fn try_new(endpoints: BTreeMap<String, Endpoint>, limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        if endpoints.is_empty()
            || endpoints.len() > 1024
            || endpoints
                .keys()
                .any(|name| SubscriptionName::try_new(name).is_err())
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            endpoints,
            limits,
            admission: Semaphore::new(limits.connections),
        })
    }

    /// Sends a finite batch with per-item custody results. A refused or malformed
    /// item does not prevent later valid items from being sent. Observed successes
    /// survive later disconnects; retry only failed items with their original IDs.
    /// The batch uses one connection and sequential bounded request/ack exchanges
    /// to prevent mutual socket backpressure from large replies.
    pub async fn publish_batch(
        &self,
        destination: &str,
        envelopes: &[Envelope],
    ) -> Result<Vec<Result<Receipt, Error>>, Error> {
        let endpoint = self
            .endpoints
            .get(destination)
            .ok_or(Error::Configuration)?;
        if envelopes.is_empty() || envelopes.len() > self.limits.batch_messages {
            return Err(Error::Configuration);
        }
        let deadline = tokio::time::Instant::now() + self.limits.deadline;
        // Admission precedes serialization, so excess callers cannot each retain
        // an encoded batch while the finite socket capacity is already occupied.
        let _permit = self.admission.try_acquire().map_err(|_| Error::Uncertain)?;
        let mut results: Vec<_> = envelopes.iter().map(|_| Err(Error::Uncertain)).collect();
        let mut frames = Vec::new();
        let mut remaining = self.limits.batch_bytes;
        for (index, envelope) in envelopes.iter().enumerate() {
            tokio::task::yield_now().await;
            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Uncertain);
            }
            match envelope.validate(self.limits.envelope) {
                Ok(()) => {
                    let length = envelope.to_vec().map_err(|_| Error::Rejected)?.len();
                    remaining = remaining.checked_sub(length).ok_or(Error::Configuration)?;
                    frames.push((index, envelope));
                }
                Err(_) => results[index] = Err(Error::Rejected),
            }
        }
        if frames.is_empty() {
            return Ok(results);
        }
        let exchange = async {
            let mut stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port))
                .await
                .map_err(|_| Error::Uncertain)?;
            stream.set_nodelay(true).map_err(|_| Error::Uncertain)?;
            stream
                .write_all(framing::MAGIC)
                .await
                .map_err(|_| Error::Uncertain)?;
            stream
                .write_u16(frames.len() as u16)
                .await
                .map_err(|_| Error::Uncertain)?;
            for (index, envelope) in &frames {
                // Only one encoded item is retained per admitted connection.
                let bytes = envelope.to_vec().map_err(|_| Error::Rejected)?;
                framing::write_frame(&mut stream, &bytes).await?;
                let status = stream.read_u8().await.map_err(|_| Error::Uncertain)?;
                results[*index] = match status {
                    framing::ACCEPTED => Ok(Receipt::Accepted),
                    framing::RETRY => Err(Error::Uncertain),
                    framing::REJECTED => Err(Error::Rejected),
                    framing::REPLY => {
                        let bytes = framing::read_frame(
                            &mut stream,
                            self.limits.envelope.max_bytes.min(remaining),
                        )
                        .await?;
                        remaining -= bytes.len();
                        let reply = Envelope::from_slice(&bytes, self.limits.envelope)
                            .map_err(|_| Error::Uncertain)?;
                        if reply.correlation_id() != Some(envelopes[*index].id())
                            || reply.causation_id() != Some(envelopes[*index].id())
                        {
                            return Err(Error::Uncertain);
                        }
                        Ok(Receipt::Reply(reply))
                    }
                    _ => return Err(Error::Uncertain),
                };
            }
            Ok::<_, Error>(())
        };
        // Dropping the future closes the socket but cannot undo peer custody.
        let _ = tokio::time::timeout_at(deadline, exchange).await;
        Ok(results)
    }

    /// Performs a single real-socket request/reply, validating the reply contract
    /// and correlation. A deadline or disconnect cannot undo remote side effects.
    /// `Error::Reply` means custody was acknowledged without the expected reply;
    /// use batch receipts when the application needs to inspect alternate replies.
    pub async fn request(
        &self,
        destination: &str,
        envelope: &Envelope,
        reply_contract: &str,
    ) -> Result<Envelope, Error> {
        SubscriptionName::try_new(reply_contract).map_err(|_| Error::Configuration)?;
        let result = self
            .publish_batch(destination, std::slice::from_ref(envelope))
            .await?;
        match result.into_iter().next() {
            Some(Ok(Receipt::Reply(reply))) if reply.contract() == reply_contract => Ok(reply),
            Some(Err(error)) => Err(error),
            _ => Err(Error::Reply),
        }
    }
}

impl Transport for Publisher {
    async fn publish(&self, destination: &str, envelope: &Envelope) -> Result<(), TransportError> {
        match self
            .publish_batch(destination, std::slice::from_ref(envelope))
            .await
        {
            Ok(results) => match results.into_iter().next() {
                Some(Ok(_)) => Ok(()),
                Some(Err(Error::Rejected | Error::Configuration)) => Err(TransportError::Rejected),
                _ => Err(TransportError::Transient),
            },
            Err(Error::Configuration | Error::Rejected) => Err(TransportError::Rejected),
            Err(_) => Err(TransportError::Transient),
        }
    }
}
