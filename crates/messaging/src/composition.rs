//! Typed ordered groups executed within one scoped SQL transaction.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures_util::FutureExt;
use raddle_postgres::Scope;

use crate::effects::{Effect, Effects, HandlerOutput, Outgoing};
use crate::scoped::{CommitReceipt, Completion, Failure, HandleFuture, Phase, ScopedHandler};
use crate::{BoxFuture, MessageContext};

mod sealed {
    pub trait Sealed {}
}

/// Preparation result at the typed recursive composition boundary.
pub type PrepareFuture<'a, R, E> = BoxFuture<'a, Result<(R, Vec<Outgoing>), Failure<E>>>;

/// Typed recursive handler chain, constructed with [`Group`] and [`Then`].
pub trait Chain<M>: sealed::Sealed + Send + Sync + 'static {
    /// Retained, per-attempt resource tree.
    type Resources: Default + Send + 'static;
    /// Ordered replies; unselected groups return None.
    type Reply: Send + 'static;
    /// Concrete member error tree.
    type Error: std::error::Error + Send + Sync + 'static;
    #[doc(hidden)]
    fn identities(&self, groups: &mut Vec<String>);
    #[doc(hidden)]
    fn prepare<'a>(
        &'a self,
        selected: &'a [String],
        message: &'a M,
        resources: &'a mut Self::Resources,
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> PrepareFuture<'a, Self::Reply, Self::Error>;
    #[doc(hidden)]
    fn exception<'a>(
        &'a self,
        failure: &'a Failure<Self::Error>,
        resources: &'a mut Self::Resources,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, ()>;
    #[doc(hidden)]
    fn committed<'a>(
        &'a self,
        resources: &'a mut Self::Resources,
        receipt: &'a CommitReceipt,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;
    #[doc(hidden)]
    fn finish<'a>(
        &'a self,
        completion: Completion,
        resources: &'a mut Self::Resources,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;
    #[doc(hidden)]
    fn dispose<'a>(
        &'a self,
        resources: Self::Resources,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;
}

/// One explicit stable native identity, regardless of its number of phases.
pub struct Group<H> {
    id: String,
    handler: Arc<H>,
}
impl<H> Group<H> {
    /// Identities are validated before registration. A saga Start/Handle pair or
    /// several methods of one native handler belong in one group.
    pub fn new(id: impl Into<String>, handler: Arc<H>) -> Self {
        Self {
            id: id.into(),
            handler,
        }
    }
}
impl<H> sealed::Sealed for Group<H> {}
impl<M: Send + Sync + 'static, H: ScopedHandler<M>> Chain<M> for Group<H> {
    type Resources = Option<H::Resources>;
    type Reply = Option<H::Reply>;
    type Error = H::Error;
    fn identities(&self, groups: &mut Vec<String>) {
        groups.push(self.id.clone());
    }
    fn prepare<'a>(
        &'a self,
        selected: &'a [String],
        message: &'a M,
        resources: &'a mut Self::Resources,
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> PrepareFuture<'a, Self::Reply, Self::Error> {
        Box::pin(async move {
            if !selected.contains(&self.id) {
                return Ok((None, Vec::new()));
            }
            // The outer resource tree already exists. Cancellation constructing a
            // later group cannot abandon previously returned scoped resources.
            *resources = Some(
                self.handler
                    .create(scope, context)
                    .await
                    .map_err(Failure::Handler)?,
            );
            let Some(resources) = resources.as_mut() else {
                return Err(Failure::Panicked);
            };
            for phase in [Phase::Before, Phase::Load, Phase::Validate] {
                self.handler
                    .phase(phase, message, resources, scope, context)
                    .await
                    .map_err(Failure::Handler)?;
            }
            let output = self
                .handler
                .handle(message, resources, scope, context)
                .await
                .map_err(Failure::Handler)?;
            let mut outgoing = Vec::new();
            // Project onto the *actual* member resources, before the next member.
            // Outgoing intents alone are resource-independent and can be deferred.
            for effect in output.effects.into_items() {
                match effect {
                    Effect::Inline(action) | Effect::External(action) => {
                        action(resources, scope, context).await?
                    }
                    Effect::Outgoing(intent) => outgoing.push(intent),
                    Effect::Nothing => {}
                }
            }
            for phase in [Phase::After, Phase::PostProcess] {
                self.handler
                    .phase(phase, message, resources, scope, context)
                    .await
                    .map_err(Failure::Handler)?;
            }
            Ok((Some(output.reply), outgoing))
        })
    }
    fn exception<'a>(
        &'a self,
        failure: &'a Failure<Self::Error>,
        resources: &'a mut Self::Resources,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Some(resources) = resources {
                self.handler.on_exception(failure, resources, context).await;
            }
        })
    }
    fn committed<'a>(
        &'a self,
        resources: &'a mut Self::Resources,
        receipt: &'a CommitReceipt,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            if let Some(resources) = resources {
                self.handler
                    .after_commit(resources, receipt, context)
                    .await?;
            }
            Ok(())
        })
    }
    fn finish<'a>(
        &'a self,
        completion: Completion,
        resources: &'a mut Self::Resources,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            if let Some(resources) = resources {
                self.handler.finally(completion, resources, context).await?;
            }
            Ok(())
        })
    }
    fn dispose<'a>(
        &'a self,
        resources: Self::Resources,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            if let Some(resources) = resources {
                self.handler.dispose(resources, context).await?;
            }
            Ok(())
        })
    }
}

/// The failing member retains its original typed failure, without cloning errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ChainError<A: std::error::Error + 'static, B: std::error::Error + 'static> {
    /// Earlier group/chain failed.
    #[error("earlier handler group failed: {0}")]
    First(#[source] Failure<A>),
    /// Later group/chain failed.
    #[error("later handler group failed: {0}")]
    Second(#[source] Failure<B>),
    /// A lifecycle callback panicked; remaining cleanup still runs.
    #[error("handler group lifecycle panicked")]
    Panicked,
}

/// Entire first chain, then entire second chain, with no intervening commit.
pub struct Then<A, B> {
    first: A,
    second: B,
}
impl<A, B> Then<A, B> {
    /// Composes without erasing resources, replies or errors.
    pub fn new(first: A, second: B) -> Self {
        Self { first, second }
    }
}
impl<A, B> sealed::Sealed for Then<A, B> {}
impl<M: Send + Sync + 'static, A: Chain<M>, B: Chain<M>> Chain<M> for Then<A, B> {
    type Resources = (A::Resources, B::Resources);
    type Reply = (A::Reply, B::Reply);
    type Error = ChainError<A::Error, B::Error>;
    fn identities(&self, groups: &mut Vec<String>) {
        self.first.identities(groups);
        self.second.identities(groups);
    }
    fn prepare<'a>(
        &'a self,
        selected: &'a [String],
        message: &'a M,
        resources: &'a mut Self::Resources,
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> PrepareFuture<'a, Self::Reply, Self::Error> {
        Box::pin(async move {
            let (a, mut outgoing) = self
                .first
                .prepare(selected, message, &mut resources.0, scope, context)
                .await
                .map_err(|e| Failure::Handler(ChainError::First(e)))?;
            let (b, other) = self
                .second
                .prepare(selected, message, &mut resources.1, scope, context)
                .await
                .map_err(|e| Failure::Handler(ChainError::Second(e)))?;
            outgoing.extend(other);
            Ok(((a, b), outgoing))
        })
    }
    fn exception<'a>(
        &'a self,
        failure: &'a Failure<Self::Error>,
        resources: &'a mut Self::Resources,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            match failure {
                Failure::Handler(ChainError::First(e)) => {
                    self.first.exception(e, &mut resources.0, context).await
                }
                Failure::Handler(ChainError::Second(e)) => {
                    self.second.exception(e, &mut resources.1, context).await
                }
                Failure::Cancelled | Failure::TimedOut | Failure::Panicked | Failure::Closed => {
                    let a = signal(failure);
                    let b = signal(failure);
                    let _ = AssertUnwindSafe(self.first.exception(&a, &mut resources.0, context))
                        .catch_unwind()
                        .await;
                    self.second.exception(&b, &mut resources.1, context).await;
                }
                // Root staging/commit failures have no artificial member owner.
                // Every constructed group's Finally still observes Failed.
                _ => {}
            }
        })
    }
    fn committed<'a>(
        &'a self,
        resources: &'a mut Self::Resources,
        receipt: &'a CommitReceipt,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            let a = AssertUnwindSafe(self.first.committed(&mut resources.0, receipt, context))
                .catch_unwind()
                .await;
            let b = AssertUnwindSafe(self.second.committed(&mut resources.1, receipt, context))
                .catch_unwind()
                .await;
            combine(a, b)
        })
    }
    fn finish<'a>(
        &'a self,
        completion: Completion,
        resources: &'a mut Self::Resources,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            let b = AssertUnwindSafe(self.second.finish(completion, &mut resources.1, context))
                .catch_unwind()
                .await;
            let a = AssertUnwindSafe(self.first.finish(completion, &mut resources.0, context))
                .catch_unwind()
                .await;
            combine(a, b)
        })
    }
    fn dispose<'a>(
        &'a self,
        resources: Self::Resources,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            let b = AssertUnwindSafe(self.second.dispose(resources.1, context))
                .catch_unwind()
                .await;
            let a = AssertUnwindSafe(self.first.dispose(resources.0, context))
                .catch_unwind()
                .await;
            combine(a, b)
        })
    }
}

fn signal<E: std::error::Error, T: std::error::Error>(failure: &Failure<E>) -> Failure<T> {
    match failure {
        Failure::Cancelled => Failure::Cancelled,
        Failure::TimedOut => Failure::TimedOut,
        Failure::Closed => Failure::Closed,
        _ => Failure::Panicked,
    }
}
type Caught<E> = std::thread::Result<Result<(), E>>;
fn combine<A: std::error::Error, B: std::error::Error>(
    a: Caught<A>,
    b: Caught<B>,
) -> Result<(), ChainError<A, B>> {
    match (a, b) {
        (Ok(Ok(())), Ok(Ok(()))) => Ok(()),
        (Ok(Err(e)), _) => Err(ChainError::First(Failure::Handler(e))),
        (_, Ok(Err(e))) => Err(ChainError::Second(Failure::Handler(e))),
        _ => Err(ChainError::Panicked),
    }
}

/// A composed preparation failure, preserving both domain and effect errors.
#[derive(Debug, thiserror::Error)]
#[error("composed handler failed: {0}")]
pub struct CompositionFailure<E: std::error::Error + 'static>(
    /// Original failure, retaining the member error tree or effect error.
    #[source]
    pub Failure<E>,
);

/// Selected chain executed once by the normal scoped runner; one root commit.
pub struct Composed<C> {
    chain: Arc<C>,
    selected: Vec<String>,
}
impl<C> Composed<C> {
    pub(crate) fn new(chain: Arc<C>, selected: Vec<String>) -> Self {
        Self { chain, selected }
    }
}
impl<M: Send + Sync + 'static, C: Chain<M>> ScopedHandler<M> for Composed<C> {
    type Resources = C::Resources;
    type Reply = C::Reply;
    type Error = CompositionFailure<C::Error>;
    fn create<'a>(
        &'a self,
        _: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<Self::Resources, Self::Error>> {
        Box::pin(async { Ok(Self::Resources::default()) })
    }
    fn handle<'a>(
        &'a self,
        message: &'a M,
        resources: &'a mut Self::Resources,
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> HandleFuture<'a, Self::Reply, Self::Resources, Self::Error> {
        Box::pin(async move {
            let (reply, outgoing) = self
                .chain
                .prepare(&self.selected, message, resources, scope, context)
                .await
                .map_err(CompositionFailure)?;
            let mut effects = Effects::default();
            for intent in outgoing {
                effects
                    .push(Effect::Outgoing(intent))
                    .map_err(|e| CompositionFailure(Failure::Effect(e)))?;
            }
            Ok(HandlerOutput { reply, effects })
        })
    }
    fn on_exception<'a>(
        &'a self,
        error: &'a Failure<Self::Error>,
        resources: &'a mut Self::Resources,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            match error {
                Failure::Handler(CompositionFailure(failure)) => {
                    self.chain.exception(failure, resources, context).await
                }
                Failure::Cancelled | Failure::TimedOut | Failure::Panicked | Failure::Closed => {
                    self.chain
                        .exception(&signal(error), resources, context)
                        .await
                }
                _ => {}
            }
        })
    }
    fn after_commit<'a>(
        &'a self,
        resources: &'a mut Self::Resources,
        receipt: &'a CommitReceipt,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            self.chain
                .committed(resources, receipt, context)
                .await
                .map_err(|e| CompositionFailure(Failure::Handler(e)))
        })
    }
    fn finally<'a>(
        &'a self,
        completion: Completion,
        resources: &'a mut Self::Resources,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            self.chain
                .finish(completion, resources, context)
                .await
                .map_err(|e| CompositionFailure(Failure::Handler(e)))
        })
    }
    fn dispose<'a>(
        &'a self,
        resources: Self::Resources,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            self.chain
                .dispose(resources, context)
                .await
                .map_err(|e| CompositionFailure(Failure::Handler(e)))
        })
    }
}
