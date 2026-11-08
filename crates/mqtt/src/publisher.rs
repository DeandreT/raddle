use raddle::transport::{Transport, TransportError};
use raddle::wire::Envelope;
use tokio::sync::Mutex;

use crate::{Acceptance, Error, PublishOptions, QoS, Session};

/// Serialized confirmed publisher. The caller's publish future owns all event-loop work;
/// no spawned worker is required, and lock acquisition shares the operation deadline.
pub struct Publisher {
    session: Mutex<Session>,
    qos: QoS,
    timeout: std::time::Duration,
}
impl Publisher {
    /// QoS0 is explicitly refused because it cannot implement confirmed Transport.
    pub fn new(session: Session, qos: QoS, timeout: std::time::Duration) -> Result<Self, Error> {
        crate::config::wait(timeout)?;
        if qos == QoS::AtMostOnce {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            session: Mutex::new(session),
            qos,
            timeout,
        })
    }
    /// Drains the serialized protocol session explicitly; drop also stops all polling.
    pub async fn close(self) -> Result<(), Error> {
        self.session.into_inner().close().await
    }
}
impl Transport for Publisher {
    async fn publish(&self, destination: &str, envelope: &Envelope) -> Result<(), TransportError> {
        let result = tokio::time::timeout(self.timeout, async {
            let mut session = self.session.lock().await;
            session
                .publish(destination, envelope, self.qos, PublishOptions::default())
                .await
        })
        .await
        .map_err(|_| TransportError::Transient)?;
        match result {
            Ok(Acceptance::PubAck | Acceptance::PubComp) => Ok(()),
            Ok(Acceptance::Unconfirmed) => Err(TransportError::Rejected),
            Err(Error::Invalid(_) | Error::Refused | Error::Unsupported) => {
                Err(TransportError::Rejected)
            }
            Err(_) => Err(TransportError::Transient),
        }
    }
}
