use lapin::options::{BasicAckOptions, BasicNackOptions};
use raddle::wire::{Envelope, WireError};

use crate::Error;

/// Explicit rejection semantics. Dead-lettering depends on the broker's queue policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Requeue {
    /// Return to the queue for another consumer.
    Yes,
    /// Apply the configured dead-letter policy, or discard when none exists.
    No,
}

/// An unsettled broker delivery. Decode failures retain its settlement capability.
pub struct HeldMessage {
    pub(crate) delivery: lapin::message::Delivery,
    pub(crate) settled: bool,
}
impl HeldMessage {
    /// Validates the bounded raddle envelope.
    pub fn decode(&self) -> Result<Envelope, WireError> {
        Envelope::from_slice(&self.delivery.data, Default::default())
    }
    /// Acknowledges only after the caller commits durable custody or effects.
    pub async fn complete(&mut self) -> Result<(), Error> {
        if self.settled {
            return Err(Error::Invalid("already settled"));
        }
        self.delivery
            .ack(BasicAckOptions::default())
            .await
            .map_err(|_| Error::Broker)?;
        self.settled = true;
        Ok(())
    }
    /// Rejects using an explicit requeue policy. It does not promise a DLQ exists.
    pub async fn reject(&mut self, requeue: Requeue) -> Result<(), Error> {
        if self.settled {
            return Err(Error::Invalid("already settled"));
        }
        self.delivery
            .nack(BasicNackOptions {
                requeue: requeue == Requeue::Yes,
                ..Default::default()
            })
            .await
            .map_err(|_| Error::Broker)?;
        self.settled = true;
        Ok(())
    }
}
