use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;

use raddle::wire::{Envelope, EnvelopeLimits};

use crate::{
    DriverError, FailureDisposition, PolicyErrorCode, PolicyExecutorDriver, Transition,
    UnknownMessageOptions,
};

/// An observer may fail without preventing later missing-handler observers.
pub type MissingHandlerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), PolicyErrorCode>> + Send + 'a>>;
type Observer = Box<dyn for<'a> Fn(&'a Envelope) -> MissingHandlerFuture<'a> + Send + Sync>;

/// Bounded original received bytes; Debug reveals only message identity.
pub struct UnknownRecord {
    envelope: Envelope,
    original: Vec<u8>,
}
impl UnknownRecord {
    /// Validated metadata; not an authorization source.
    pub fn envelope(&self) -> &Envelope {
        &self.envelope
    }
    /// Exact original bytes, including whitespace, retained only by opt-in bound.
    pub fn original_bytes(&self) -> &[u8] {
        &self.original
    }
}
impl std::fmt::Debug for UnknownRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnknownRecord")
            .field("message_id", &self.envelope.id())
            .finish_non_exhaustive()
    }
}

/// Payload-free missing-handler outcome, not a typed local invocation error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnknownMessageReport {
    /// Effective terminal outcome.
    pub disposition: FailureDisposition,
    /// Independent observer failures; all observers were attempted.
    pub observer_failures: usize,
}

/// Ordered additive unknown-message observers plus real driver settlement.
pub struct UnknownMessageHandler {
    options: UnknownMessageOptions,
    limits: EnvelopeLimits,
    observers: Vec<Observer>,
    records: VecDeque<UnknownRecord>,
}
impl UnknownMessageHandler {
    /// Retains at most 4096 original envelopes; zero disables tracking. Total
    /// retained bytes are also bounded by capacity times EnvelopeLimits.
    pub fn new(
        options: UnknownMessageOptions,
        limits: EnvelopeLimits,
    ) -> Result<Self, PolicyErrorCode> {
        if options.tracking_capacity > 4096
            || !matches!(
                options.disposition,
                FailureDisposition::Discard | FailureDisposition::DeadLetter
            )
        {
            return Err(PolicyErrorCode);
        }
        Ok(Self {
            options,
            limits,
            observers: Vec::new(),
            records: VecDeque::new(),
        })
    }
    /// Registers at most 64 independent observers. Observers should be
    /// idempotent: a custody I/O error can leave the original safely retryable.
    pub fn observe<F>(&mut self, observer: F) -> Result<(), PolicyErrorCode>
    where
        F: for<'a> Fn(&'a Envelope) -> MissingHandlerFuture<'a> + Send + Sync + 'static,
    {
        if self.observers.len() == 64 {
            return Err(PolicyErrorCode);
        }
        self.observers.push(Box::new(observer));
        Ok(())
    }
    /// Tracks validated unknown envelopes without exposing payload in Debug.
    pub fn records(&self) -> &VecDeque<UnknownRecord> {
        &self.records
    }
    /// Runs against the driver's active claim for these exact bytes. Does not
    /// retry a handler, synthesize a typed fault or interpret message headers.
    pub async fn handle<D: PolicyExecutorDriver>(
        &mut self,
        driver: &mut D,
        original: &[u8],
    ) -> Result<UnknownMessageReport, DriverError> {
        let envelope =
            Envelope::from_slice(original, self.limits).map_err(|_| DriverError::Invalid)?;
        let mut observer_failures = 0;
        for observer in &self.observers {
            if observer(&envelope).await.is_err() {
                observer_failures += 1;
            }
        }
        let transition = match self.options.disposition {
            FailureDisposition::Discard => Transition::Discard,
            FailureDisposition::DeadLetter => Transition::DeadLetter {
                replay_denied: false,
            },
            _ => return Err(DriverError::Invalid),
        };
        driver.transition(transition).await?;
        tracing::warn!(message_id = %envelope.id(), "received wire message has no handler");
        if self.options.tracking_capacity > 0 {
            if self.records.len() == self.options.tracking_capacity {
                self.records.pop_front();
            }
            self.records.push_back(UnknownRecord {
                envelope,
                original: original.to_vec(),
            });
        }
        Ok(UnknownMessageReport {
            disposition: self.options.disposition,
            observer_failures,
        })
    }
}
