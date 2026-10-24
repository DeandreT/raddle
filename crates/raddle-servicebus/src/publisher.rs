use std::collections::HashMap;

use azservicebus::{ServiceBusMessage, ServiceBusSender};
use raddle::transport::{Transport, TransportError};
use raddle::wire::Envelope;
use tokio::sync::Mutex;

use crate::Error;
use crate::connection::SharedClient;

/// Per-endpoint serialized senders sharing a connection, without a global send lock.
pub struct Publisher {
    pub(crate) senders: HashMap<String, Mutex<ServiceBusSender>>,
    pub(crate) _connection: SharedClient,
}

impl Transport for Publisher {
    async fn publish(&self, destination: &str, envelope: &Envelope) -> Result<(), TransportError> {
        let sender = self
            .senders
            .get(destination)
            .ok_or(TransportError::Rejected)?;
        let bytes = envelope.to_vec().map_err(|_| TransportError::Rejected)?;
        // Leave room for AMQP framing and properties in a Standard-tier 256 KiB message.
        if bytes.len() > 240 * 1024 {
            return Err(TransportError::Rejected);
        }
        let mut message = ServiceBusMessage::new(bytes);
        message
            .set_message_id(envelope.id().to_string())
            .map_err(|_| TransportError::Rejected)?;
        message.set_content_type(Some("application/json".into()));
        if let Some(session) = envelope.headers().get("session-id") {
            message
                .set_session_id(Some(session.clone()))
                .map_err(|_| TransportError::Rejected)?;
        }
        Box::pin(sender.lock().await.send_message(message))
            .await
            .map_err(|_| TransportError::Transient)
    }
}

impl Publisher {
    /// Closes each sender and reports a failure after attempting all of them.
    pub async fn close(self) -> Result<(), Error> {
        let mut failed = false;
        for sender in self.senders.into_values() {
            failed |= sender.into_inner().dispose().await.is_err();
        }
        if failed { Err(Error::Service) } else { Ok(()) }
    }
}
