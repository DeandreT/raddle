use std::collections::BTreeMap;
use std::error::Error;

use raddle::wire::{Envelope, WireError, WireMessage};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::AttemptCounters;

/// Terminal failure semantics, independent from provider-specific job enums.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum FailureDisposition {
    /// No terminal settlement yet.
    Retry,
    /// Terminal without dead-letter retention.
    Discard,
    /// Terminal quarantine.
    DeadLetter,
    /// Terminal deadline expiration.
    Expired,
}

/// Fault bypass categories cannot be inferred from attacker-controlled headers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureOrigin {
    /// A known handler failed after deserialization.
    Handler,
    /// No handler/contract exists. Never synthesizes a typed fault.
    UnknownMessage,
    /// Authentication/decryption failed. Never runs handler retry policy.
    Cryptography,
    /// Publication failed. Never recursively publishes a failure event.
    Sending,
}

/// Payload-only bounded exception graph. A caller-supplied redactor owns what
/// error text is safe to disclose; unredacted errors are never auto-persisted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExceptionNode {
    /// Redacted summary, bounded to 1024 UTF-8 bytes.
    pub summary: String,
    /// Optional causal node. Total capture depth is at most ten.
    pub cause: Option<Box<ExceptionNode>>,
}
impl ExceptionNode {
    fn bounded(mut self, depth: usize) -> Self {
        let mut end = self.summary.len().min(1024);
        while !self.summary.is_char_boundary(end) {
            end -= 1;
        }
        self.summary.truncate(end);
        self.cause = if depth < 10 {
            self.cause.map(|cause| Box::new(cause.bounded(depth + 1)))
        } else {
            None
        };
        self
    }
    /// Captures a source chain with cycle-safe depth and output-size bounds.
    pub fn capture(
        error: &(dyn Error + 'static),
        redact: &impl Fn(&(dyn Error + 'static)) -> String,
    ) -> Self {
        fn capture(
            error: &(dyn Error + 'static),
            redact: &impl Fn(&(dyn Error + 'static)) -> String,
            depth: usize,
        ) -> ExceptionNode {
            let mut summary = redact(error);
            let mut end = summary.len().min(1024);
            while !summary.is_char_boundary(end) {
                end -= 1;
            }
            summary.truncate(end);
            ExceptionNode {
                summary,
                cause: if depth < 10 {
                    error
                        .source()
                        .map(|source| Box::new(capture(source, redact, depth + 1)))
                } else {
                    None
                },
            }
        }
        capture(error, redact, 1)
    }
}

/// Native typed terminal fault. This is not a .NET serializer identity claim.
/// Fault publication is best-effort and must occur AFTER terminal custody is
/// saved; its failure must never undo dead-lettering or retry the original.
#[derive(Clone, Serialize, Deserialize)]
pub struct Fault<M> {
    /// Original message, not an encrypted wire copy.
    pub message: M,
    /// Original contract for policy pairing and diagnostics.
    pub source_contract: String,
    /// Original identity.
    pub message_id: Uuid,
    /// Independent observed counters.
    pub attempts: AttemptCountersDto,
    /// UTC Unix milliseconds supplied by the execution clock.
    pub failed_at_millis: i64,
    /// Configured source endpoint.
    pub source: String,
    /// Application-provided tenant attribution; never trusted from headers.
    pub tenant: Option<String>,
    /// Original untrusted application headers; not authorization claims.
    pub headers: BTreeMap<String, String>,
    /// Terminal category.
    pub disposition: FailureDisposition,
    /// Recursively redacted bounded exception graph.
    pub exception: ExceptionNode,
}
impl<M> std::fmt::Debug for Fault<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fault")
            .field("message_id", &self.message_id)
            .field("disposition", &self.disposition)
            .finish_non_exhaustive()
    }
}
impl<M: WireMessage> WireMessage for Fault<M> {
    const NAME: &'static str = "raddle.fault";
    const VERSION: u32 = 1;
}

/// Serializable attempt snapshot without coupling the executor to serde.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct AttemptCountersDto {
    /// Actual handler invocations, including the original.
    pub handling: u64,
    /// Actual publication calls, independently counted.
    pub sending: u64,
}
impl From<AttemptCounters> for AttemptCountersDto {
    fn from(value: AttemptCounters) -> Self {
        Self {
            handling: value.handling,
            sending: value.sending,
        }
    }
}

/// Fault generation is opt-in and startup-owned. Drivers carry the bypass tag
/// out-of-band; a message cannot clear it through an application header.
#[derive(Clone, Copy, Debug, Default)]
pub struct FaultOptions {
    /// Enables terminal faults for DLQ and expiry.
    pub enabled: bool,
    /// Also emit on deliberate discard, off by default.
    pub include_discard: bool,
}
impl FaultOptions {
    /// Determines emission without side effects. `is_fault` is trusted local
    /// dispatch metadata; the reserved native fault contract is also suppressed.
    pub fn should_emit(
        self,
        disposition: FailureDisposition,
        origin: FailureOrigin,
        is_fault: bool,
        envelope: &Envelope,
    ) -> bool {
        self.enabled
            && origin == FailureOrigin::Handler
            && !is_fault
            && envelope.contract() != "raddle.fault"
            && match disposition {
                FailureDisposition::DeadLetter | FailureDisposition::Expired => true,
                FailureDisposition::Discard => self.include_discard,
                FailureDisposition::Retry => false,
            }
    }
}
impl<M: WireMessage> Fault<M> {
    /// Builds a typed body from an original envelope. Caller first checks
    /// `should_emit`, persists terminal state, then attempts publication once.
    #[allow(clippy::too_many_arguments)]
    pub fn from_envelope(
        envelope: &Envelope,
        attempts: AttemptCounters,
        failed_at_millis: i64,
        source: String,
        tenant: Option<String>,
        disposition: FailureDisposition,
        exception: ExceptionNode,
    ) -> Result<Self, WireError> {
        if source.is_empty() || source.len() > 256 || tenant.as_ref().is_some_and(|t| t.len() > 256)
        {
            return Err(WireError::Invalid("fault attribution exceeds limits"));
        }
        Ok(Self {
            message: envelope.decode()?,
            source_contract: envelope.contract().to_owned(),
            message_id: envelope.id(),
            attempts: attempts.into(),
            failed_at_millis,
            source,
            tenant,
            headers: envelope.headers().clone(),
            disposition,
            exception: exception.bounded(1),
        })
    }
}

/// Unknown received wire messages default to discard, not a retry loop.
/// A missing typed local Request remains an unrelated registration error.
#[derive(Clone, Copy, Debug)]
pub struct UnknownMessageOptions {
    /// Terminal unknown-message settlement.
    pub disposition: FailureDisposition,
    /// Maximum original envelopes an implementing driver tracks in memory.
    pub tracking_capacity: usize,
}
impl Default for UnknownMessageOptions {
    fn default() -> Self {
        Self {
            disposition: FailureDisposition::Discard,
            tracking_capacity: 1000,
        }
    }
}
