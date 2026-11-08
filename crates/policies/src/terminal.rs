use raddle::transport::Transport;
use raddle::wire::{Envelope, WireMessage};

use crate::{
    ExceptionNode, FailureDisposition, FailureOrigin, Fault, FaultOptions, MemoryDelivery,
    MemoryStatus,
};

/// Best-effort fault outcome, intentionally independent of terminal custody.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultPublication {
    /// Disabled, bypassed, recursive or not terminal.
    Skipped,
    /// Confirmed by the selected transport, not necessarily consumed.
    Published,
    /// Encoding or publication failed. Terminal original remains settled.
    Failed,
}

/// Executable post-settlement adapter. A successful sibling is never replayed
/// when fault publication fails, and a failed fault does not synthesize a fault.
pub struct TerminalFaultPublisher<T> {
    transport: T,
    options: FaultOptions,
    destination: String,
}
impl<T: Transport> TerminalFaultPublisher<T> {
    /// Validates an application-configured destination before startup freezes.
    pub fn new(
        transport: T,
        options: FaultOptions,
        destination: String,
    ) -> Result<Self, crate::PolicyErrorCode> {
        raddle::SubscriptionName::try_new(destination.clone())
            .map_err(|_| crate::PolicyErrorCode)?;
        Ok(Self {
            transport,
            options,
            destination,
        })
    }

    /// Emits at most once per explicit call, only for a settled memory delivery.
    /// Driver owners must invoke this once AFTER saving terminal state. It is
    /// best-effort, not transactional with DLQ, and intentionally not retried.
    #[allow(clippy::too_many_arguments)]
    pub async fn publish<M: WireMessage>(
        &self,
        delivery: &MemoryDelivery,
        origin: FailureOrigin,
        is_fault: bool,
        failed_at_millis: i64,
        source: String,
        tenant: Option<String>,
        exception: ExceptionNode,
    ) -> FaultPublication {
        let disposition = match delivery.status {
            MemoryStatus::DeadLetter { .. } => FailureDisposition::DeadLetter,
            MemoryStatus::Discarded => FailureDisposition::Discard,
            _ => return FaultPublication::Skipped,
        };
        if !self
            .options
            .should_emit(disposition, origin, is_fault, &delivery.envelope)
        {
            return FaultPublication::Skipped;
        }
        let fault = Fault::<M>::from_envelope(
            &delivery.envelope,
            delivery.attempts,
            failed_at_millis,
            source,
            tenant,
            disposition,
            exception,
        )
        .and_then(|fault| Envelope::encode(&fault));
        let result = match fault {
            Ok(envelope) => self
                .transport
                .publish(&self.destination, &envelope)
                .await
                .is_ok(),
            Err(_) => false,
        };
        if result {
            FaultPublication::Published
        } else {
            tracing::warn!(message_id = %delivery.envelope.id(), "terminal fault publication failed; original terminal custody retained");
            FaultPublication::Failed
        }
    }
}
