use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use raddle::transport::{Transport, TransportError};
use raddle::wire::Envelope;
use uuid::Uuid;

/// A deterministic publication outcome chosen before the test starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SendOutcome {
    /// Broker accepts the publication and confirms it.
    Accept,
    /// Permanent refusal without accepting the publication.
    Reject,
    /// Temporary failure before acceptance.
    FailBeforeAcceptance,
    /// Acceptance occurs but the confirmation is lost.
    FailAfterAcceptance,
}

/// Payload-free record of simulated broker acceptance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Receipt {
    /// Original stable message identity.
    pub message_id: Uuid,
    /// Configured destination alias.
    pub destination: String,
}

/// Invalid script bounds or poisoned test state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid or unavailable transport script")]
pub struct ScriptError;

struct State {
    outcomes: VecDeque<SendOutcome>,
    receipts: Vec<Receipt>,
}

/// A bounded, cloneable transport script shared by workers in a test.
#[derive(Clone)]
pub struct ScriptedTransport {
    state: Arc<Mutex<State>>,
}

impl ScriptedTransport {
    /// Accepts at most 4096 outcomes; script exhaustion is a permanent refusal.
    pub fn new(outcomes: impl IntoIterator<Item = SendOutcome>) -> Result<Self, ScriptError> {
        let outcomes: VecDeque<_> = outcomes.into_iter().take(4097).collect();
        if outcomes.is_empty() || outcomes.len() > 4096 {
            return Err(ScriptError);
        }
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                outcomes,
                receipts: Vec::new(),
            })),
        })
    }
    /// Inspects accepted identities without retaining message bodies or headers.
    pub fn receipts(&self) -> Result<Vec<Receipt>, ScriptError> {
        Ok(self.state.lock().map_err(|_| ScriptError)?.receipts.clone())
    }
}

impl Transport for ScriptedTransport {
    async fn publish(&self, destination: &str, envelope: &Envelope) -> Result<(), TransportError> {
        raddle::SubscriptionName::try_new(destination).map_err(|_| TransportError::Rejected)?;
        envelope
            .validate(Default::default())
            .map_err(|_| TransportError::Rejected)?;
        let mut state = self.state.lock().map_err(|_| TransportError::Transient)?;
        let outcome = state.outcomes.pop_front().unwrap_or(SendOutcome::Reject);
        if matches!(
            outcome,
            SendOutcome::Accept | SendOutcome::FailAfterAcceptance
        ) {
            state.receipts.push(Receipt {
                message_id: envelope.id(),
                destination: destination.to_owned(),
            });
        }
        match outcome {
            SendOutcome::Accept => Ok(()),
            SendOutcome::Reject => Err(TransportError::Rejected),
            SendOutcome::FailBeforeAcceptance | SendOutcome::FailAfterAcceptance => {
                Err(TransportError::Transient)
            }
        }
    }
}
