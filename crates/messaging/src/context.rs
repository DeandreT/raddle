use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use raddle::wire::{Envelope, WireError};
use tokio::sync::watch;
use uuid::Uuid;

/// A clonable cooperative cancellation signal. It carries no authorization.
#[derive(Clone, Debug)]
pub struct Cancellation {
    signal: watch::Sender<bool>,
    parent: Option<Arc<Cancellation>>,
}

impl Cancellation {
    /// Creates an uncancelled signal.
    pub fn new() -> Self {
        Self {
            signal: watch::channel(false).0,
            parent: None,
        }
    }
    /// Cancels every observer, idempotently.
    pub fn cancel(&self) {
        self.signal.send_replace(true);
    }
    /// Whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        *self.signal.borrow()
            || self
                .parent
                .as_ref()
                .is_some_and(|parent| parent.is_cancelled())
    }
    /// Creates an independently cancellable execution linked to host shutdown.
    /// Cancelling/timing out this child never cancels its parent or healthy siblings.
    pub fn child(&self) -> Self {
        Self {
            signal: watch::channel(false).0,
            parent: Some(Arc::new(self.clone())),
        }
    }
    /// Waits without a lost-notification race.
    pub fn cancelled(&self) -> crate::BoxFuture<'_, ()> {
        Box::pin(async move {
            let mut receiver = self.signal.subscribe();
            let local = async {
                while !*receiver.borrow_and_update() {
                    if receiver.changed().await.is_err() {
                        return;
                    }
                }
            };
            match &self.parent {
                Some(parent) => tokio::select! { () = local => {}, () = parent.cancelled() => {} },
                None => local.await,
            }
        })
    }
}

impl Default for Cancellation {
    fn default() -> Self {
        Self::new()
    }
}

/// Identity supplied by the application's authenticated ingress, not envelope headers.
///
/// Constructing this type is an explicit trust assertion by application code. It
/// does not authenticate credentials or authorize tenant switching itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedOrigin {
    tenant: String,
    source: String,
    username: Option<String>,
}

impl TrustedOrigin {
    /// Validates bounded, nonempty custody identities. Never call with raw headers.
    pub fn authenticated(
        tenant: impl Into<String>,
        source: impl Into<String>,
        username: Option<String>,
    ) -> Result<Self, WireError> {
        let tenant = tenant.into();
        let source = source.into();
        if [&tenant, &source]
            .iter()
            .any(|s| s.is_empty() || s.len() > 128)
            || username
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 256)
        {
            return Err(WireError::Invalid("invalid authenticated origin"));
        }
        Ok(Self {
            tenant,
            source,
            username,
        })
    }
    /// Authenticated tenant; not inferred from the payload or diagnostics.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    /// Authenticated ingress/source identity.
    pub fn source(&self) -> &str {
        &self.source
    }
    /// Optional authenticated caller name, not an authorization decision.
    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }
}

/// Header binding failure, without disclosing the value.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HeaderError {
    /// No value was supplied.
    #[error("required message header is absent")]
    Missing,
    /// The requested explicit conversion failed.
    #[error("message header has an invalid value")]
    Invalid,
}

/// Explicit immutable execution metadata plus a cooperative cancellation signal.
/// No conversion exists from diagnostic `ExecutionContext` to authenticated origin.
#[derive(Clone, Debug)]
pub struct MessageContext {
    origin: TrustedOrigin,
    envelope: Envelope,
    now: DateTime<Utc>,
    cancellation: Cancellation,
    bus: Option<crate::MessageBus>,
}

impl MessageContext {
    /// Combines independently authenticated origin with validated untrusted metadata.
    /// `now` is explicit so the application can provide a deterministic clock.
    pub fn new(origin: TrustedOrigin, envelope: Envelope, now: DateTime<Utc>) -> Self {
        Self {
            origin,
            envelope,
            now,
            cancellation: Cancellation::new(),
            bus: None,
        }
    }
    /// Authenticated custody identity.
    pub fn origin(&self) -> &TrustedOrigin {
        &self.origin
    }
    /// Original message, including untrusted application headers.
    pub fn envelope(&self) -> &Envelope {
        &self.envelope
    }
    /// Time sampled by the ingress for this execution.
    pub fn now(&self) -> DateTime<Utc> {
        self.now
    }
    /// Original correlation, or this message's identity for a new conversation.
    pub fn correlation_id(&self) -> Uuid {
        self.envelope.correlation_id().unwrap_or(self.envelope.id())
    }
    /// Cancellation observed by handlers, middleware and inline effects.
    pub fn cancellation(&self) -> &Cancellation {
        &self.cancellation
    }
    /// Uses an explicitly owned caller/host cancellation signal.
    pub fn with_cancellation(mut self, cancellation: Cancellation) -> Self {
        self.cancellation = cancellation;
        self
    }
    /// Actual contextual application bus, when executing through a bound bus or
    /// durable endpoint. This is an explicit typed dependency, not a service locator.
    pub fn bus(&self) -> Option<&crate::MessageBus> {
        self.bus.as_ref()
    }

    /// Resolves a returned child using this execution's real application routes,
    /// authenticated origin and configured header propagation. It does not send
    /// early; return the intent so the root transaction captures it atomically.
    pub fn returned<M: raddle::wire::WireMessage>(
        &self,
        message: &M,
        required: bool,
    ) -> Result<crate::effects::Outgoing, crate::BusError> {
        self.bus
            .as_ref()
            .ok_or(crate::BusError::Invalid("contextual bus is not bound"))?
            .intent(message, self, required)
    }

    /// Resolves every payload branch of returned work without sending before
    /// commit. Wrap each intent in `Effect::Outgoing` in the normal effect budget.
    pub fn returned_many<M: raddle::wire::WireMessage>(
        &self,
        message: &M,
        required: bool,
    ) -> Result<Vec<crate::effects::Outgoing>, crate::BusError> {
        self.bus
            .as_ref()
            .ok_or(crate::BusError::Invalid("contextual bus is not bound"))?
            .intents(message, self, required)
    }

    pub(crate) fn with_envelope(mut self, envelope: Envelope) -> Self {
        self.envelope = envelope;
        self
    }

    pub(crate) fn with_bus(mut self, bus: crate::MessageBus) -> Self {
        self.bus = Some(bus);
        self
    }
    /// Binds one explicitly requested application header; never upgrades its trust.
    pub fn header<T: FromStr>(&self, name: &str) -> Result<T, HeaderError> {
        self.envelope
            .headers()
            .get(name)
            .ok_or(HeaderError::Missing)?
            .parse()
            .map_err(|_| HeaderError::Invalid)
    }
    /// Correlates a child and copies only allowlisted headers, preserving explicit overrides.
    /// Identity fields remain separate, even if `tenant` or `authorization` is allowlisted.
    pub fn outgoing(
        &self,
        mut envelope: Envelope,
        headers: &BTreeSet<String>,
    ) -> Result<Envelope, WireError> {
        for name in headers {
            if !envelope.headers().contains_key(name)
                && let Some(value) = self.envelope.headers().get(name)
            {
                envelope = envelope.with_header(name.clone(), value.clone())?;
            }
        }
        envelope.correlated(self.correlation_id(), self.envelope.id())
    }
}
