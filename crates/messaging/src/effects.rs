//! Typed return interpretation. Outgoing messages and inline effects are distinct.

use std::sync::Arc;

use futures_util::stream::{Stream, StreamExt};
use raddle::wire::Envelope;
use raddle_postgres::{Delivery, Scope};

use crate::{BoxFuture, MessageContext};

/// An outgoing intent resolved before commit. Every delivery must use durable custody.
#[derive(Clone, Debug)]
pub struct Outgoing {
    /// Message identity and body; retries retain these bytes.
    pub envelope: Envelope,
    /// Independent local inbox/outbox deliveries.
    pub deliveries: Vec<Delivery>,
}

/// Failure of an inline action, without converting it into an independent child retry.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EffectError {
    /// A SQL/staging operation failed; the root transaction is rolled back.
    #[error("transactional effect failed")]
    Storage(#[from] raddle_postgres::Error),
    /// An application-provided action failed. Use a fixed classification, not data.
    #[error("inline effect failed: {0}")]
    Failed(&'static str),
    /// A return wrapper exceeded the finite effect budget.
    #[error("returned effect capacity exceeded")]
    Capacity,
    /// Enumeration or execution was cancelled.
    #[error("returned effects cancelled")]
    Cancelled,
}

type Inline<S> = Box<
    dyn for<'a> FnOnce(
            &'a mut S,
            &'a mut Scope<'static>,
            &'a MessageContext,
        ) -> BoxFuture<'a, Result<(), EffectError>>
        + Send,
>;

/// An explicit return item, interpreted without object reflection or downcasting.
#[non_exhaustive]
pub enum Effect<S> {
    /// Persist in the root transaction; execute/retry as an independent child later.
    Outgoing(Outgoing),
    /// Execute before commit using exactly the root's SQL session and resources.
    Inline(Inline<S>),
    /// Execute inline, but acknowledge that file/network actions cannot be rolled back.
    External(Inline<S>),
    /// Nullable/empty storage or message decision.
    Nothing,
}

impl<S: Send + 'static> Effect<S> {
    /// Builds an inline transactional action. SQL must use the provided scope.
    pub fn inline<F>(action: F) -> Self
    where
        F: for<'a> FnOnce(
                &'a mut S,
                &'a mut Scope<'static>,
                &'a MessageContext,
            ) -> BoxFuture<'a, Result<(), EffectError>>
            + Send
            + 'static,
    {
        Self::Inline(Box::new(action))
    }

    /// Marks an external inline action explicitly. Failure fails the root, but
    /// rollback cannot reverse external actions already performed. Supply idempotency.
    pub fn external<F>(action: F) -> Self
    where
        F: for<'a> FnOnce(
                &'a mut S,
                &'a mut Scope<'static>,
                &'a MessageContext,
            ) -> BoxFuture<'a, Result<(), EffectError>>
            + Send
            + 'static,
    {
        Self::External(Box::new(action))
    }

    /// Adapts a pure storage decision using an explicit typed repository, not a locator.
    pub fn storage<T: Send + 'static, R: Storage<T, S> + 'static>(
        action: StorageAction<T>,
        repository: Arc<R>,
    ) -> Self {
        if matches!(action, StorageAction::Nothing) {
            return Self::Nothing;
        }
        Self::inline(move |resources, scope, context| {
            Box::pin(async move { repository.apply(action, resources, scope, context).await })
        })
    }
}

/// Pure typed storage decisions. `Store` means update-existing, not implicit upsert.
/// A repository must implement `Upsert` separately or reject it explicitly.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum StorageAction<T> {
    /// Require absence.
    Insert(T),
    /// Require an existing record.
    Update(T),
    /// Store/update an existing record (never silently reinterpret as upsert).
    Store(T),
    /// Explicit insert-or-update, if supported by this repository.
    Upsert(T),
    /// Delete the supplied record identity.
    Delete(T),
    /// Do not perform storage I/O.
    Nothing,
}

/// Typed repository boundary; implementations use only the enrolled SQL session.
pub trait Storage<T, S>: Send + Sync {
    /// Executes one decision inline. Rejected decisions must return an error.
    fn apply<'a>(
        &'a self,
        action: StorageAction<T>,
        resources: &'a mut S,
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), EffectError>>;
}

/// A finite ordered unit of work. No effect runs while return values are being collected.
pub struct Effects<S>(Vec<Effect<S>>);

impl<S> Default for Effects<S> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<S> Effects<S> {
    /// Converts explicit single, optional, tuple or collection return wrappers.
    pub fn returned(value: impl IntoEffects<S>) -> Result<Self, EffectError> {
        let mut effects = Self::default();
        value.append(&mut effects)?;
        Ok(effects)
    }
    /// Pushes one item, rejecting more than 256 items before any execution.
    pub fn push(&mut self, effect: Effect<S>) -> Result<(), EffectError> {
        if self.0.len() >= 256 {
            return Err(EffectError::Capacity);
        }
        self.0.push(effect);
        Ok(())
    }
    /// Collects an explicit async stream under cancellation and the same budget.
    /// Nothing is persisted until the complete stream succeeds, so cancellation
    /// rolls back the root rather than losing a partially acknowledged return.
    pub async fn from_stream<T: Stream<Item = Result<Effect<S>, EffectError>>>(
        stream: T,
        context: &MessageContext,
    ) -> Result<Self, EffectError> {
        futures_util::pin_mut!(stream);
        let mut effects = Self::default();
        loop {
            tokio::select! {
                biased;
                () = context.cancellation().cancelled() => return Err(EffectError::Cancelled),
                item = stream.next() => match item {
                    Some(item) => effects.push(item?)?,
                    None => return Ok(effects),
                }
            }
        }
    }
    pub(crate) fn into_items(self) -> Vec<Effect<S>> {
        self.0
    }
}

/// Explicit composable return shapes. No blanket `Any` or object reflection exists.
pub trait IntoEffects<S> {
    /// Flattens without executing any effect.
    fn append(self, effects: &mut Effects<S>) -> Result<(), EffectError>;
}
impl<S> IntoEffects<S> for Effect<S> {
    fn append(self, effects: &mut Effects<S>) -> Result<(), EffectError> {
        effects.push(self)
    }
}
impl<S> IntoEffects<S> for () {
    fn append(self, _: &mut Effects<S>) -> Result<(), EffectError> {
        Ok(())
    }
}
impl<S, T: IntoEffects<S>> IntoEffects<S> for Option<T> {
    fn append(self, effects: &mut Effects<S>) -> Result<(), EffectError> {
        match self {
            Some(value) => value.append(effects),
            None => Ok(()),
        }
    }
}
impl<S, T: IntoEffects<S>> IntoEffects<S> for Vec<T> {
    fn append(self, effects: &mut Effects<S>) -> Result<(), EffectError> {
        for value in self {
            value.append(effects)?;
        }
        Ok(())
    }
}
impl<S, A: IntoEffects<S>, B: IntoEffects<S>> IntoEffects<S> for (A, B) {
    fn append(self, effects: &mut Effects<S>) -> Result<(), EffectError> {
        self.0.append(effects)?;
        self.1.append(effects)
    }
}
impl<S, A: IntoEffects<S>, B: IntoEffects<S>, C: IntoEffects<S>> IntoEffects<S> for (A, B, C) {
    fn append(self, effects: &mut Effects<S>) -> Result<(), EffectError> {
        self.0.append(effects)?;
        self.1.append(effects)?;
        self.2.append(effects)
    }
}

/// Opts a synchronous iterator into effect interpretation.
pub struct EffectIter<I>(pub I);
impl<S, I: IntoIterator, T: IntoEffects<S>> IntoEffects<S> for EffectIter<I>
where
    I: IntoIterator<Item = T>,
{
    fn append(self, effects: &mut Effects<S>) -> Result<(), EffectError> {
        for item in self.0 {
            item.append(effects)?;
        }
        Ok(())
    }
}

/// Reply and outgoing work have distinct fields; the reply is never auto-published.
pub struct HandlerOutput<R, S> {
    /// Value returned exactly once to an invoking caller.
    pub reply: R,
    /// Work interpreted automatically by the application bus.
    pub effects: Effects<S>,
}

impl<R, S> HandlerOutput<R, S> {
    /// Constructs a successful response without effects.
    pub fn reply(reply: R) -> Self {
        Self {
            reply,
            effects: Effects::default(),
        }
    }
}
