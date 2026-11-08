//! Typed per-execution resources and transaction phases owned by the message host.

use std::error::Error as StdError;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use chrono::Utc;
use futures_util::FutureExt;
use raddle::wire::WireMessage;
use raddle_postgres::{Scope, Store};
use tokio::time::Instant;
use uuid::Uuid;

use crate::effects::{Effect, EffectError, HandlerOutput};
use crate::{BoxFuture, MessageContext};

/// Ordered pre-commit hooks. Loaded values live in the handler's typed Resources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Phase {
    /// First hook after scoped construction.
    Before,
    /// Populate typed state for validation and handling.
    Load,
    /// Validate loaded state and message before calling the handler.
    Validate,
    /// Runs after the handler and inline effects, before transaction commit.
    After,
    /// Last pre-commit hook; can use the same transaction session.
    PostProcess,
}

/// The root has or has not committed; cleanup must not confuse these outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Completion {
    /// All root SQL and outgoing intent committed together.
    Committed,
    /// Root failed; SQL rollback was attempted before cleanup.
    Failed,
}

/// Root execution failure. A post-commit error is deliberately NOT a root failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Failure<E: StdError + 'static> {
    /// An application phase or handler failed, retaining the concrete error type.
    #[error("scoped handler failed: {0}")]
    Handler(E),
    /// Inline action failed, causing root rollback.
    #[error("inline action failed")]
    Effect(#[from] EffectError),
    /// Commit/staging failed. A commit I/O failure can have an uncertain outcome.
    #[error("root transaction failed")]
    Database(#[from] raddle_postgres::Error),
    /// Strict managed execution could not establish commit/rollback completion.
    /// Bypasses ordinary retry rules until custody is reconciled.
    #[error("strict transaction completion or rollback is uncertain")]
    UncertainDatabase(#[source] raddle_postgres::Error),
    /// Wire input did not match the explicitly registered type.
    #[error("input contract is invalid")]
    Wire(#[from] raddle::wire::WireError),
    /// Cooperative caller cancellation interrupted a pre-commit phase.
    #[error("scoped execution cancelled")]
    Cancelled,
    /// The total pre-commit execution budget expired.
    #[error("scoped execution deadline expired")]
    TimedOut,
    /// Managed queue closed before custody, or before delivering its response.
    #[error("managed execution queue closed")]
    Closed,
    /// A Rust panic interrupted construction, a phase, or an inline effect.
    #[error("scoped execution panicked")]
    Panicked,
}

/// The committed identities passed to the explicit confirmed-outbox flush boundary.
#[derive(Clone, Debug, Default)]
pub struct CommitReceipt {
    /// Tenant under which all these jobs were atomically staged.
    pub tenant: String,
    /// All child jobs, including local jobs with independent retry loops.
    pub jobs: Vec<Uuid>,
    /// Remote jobs that must be confirmed before AfterCommit is called.
    pub remote: Vec<(Uuid, String)>,
}

/// Post-commit callback/flush/cleanup failures never roll back or replay the parent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum PostCommitError {
    /// Root is committed, but the confirmed flush did not finish. Outbox rows remain retryable.
    #[error("committed outbox flush is pending or failed")]
    Flush,
    /// AfterCommit failed outside the completed root transaction.
    #[error("after-commit callback failed")]
    Callback,
    /// Finally or resource disposal failed after commit.
    #[error("committed resource cleanup failed")]
    Cleanup,
}

/// A successful SQL commit, even when subsequent external work fails.
#[derive(Debug)]
pub struct Committed<R> {
    /// The root response; never published as a cascade.
    pub reply: R,
    /// Durable outgoing identities, useful for reconciliation.
    pub receipt: CommitReceipt,
    /// Independent post-commit outcome. Do not retry the root on this error.
    pub post_commit: Result<(), PostCommitError>,
}

/// Explicit outbox flush boundary. Success MUST mean confirmation of every remote
/// receipt job, not merely scheduling or beginning a send.
pub trait Flush: Send + Sync + 'static {
    /// Called only after successful SQL commit. Local children are not awaited.
    fn flush<'a>(
        &'a self,
        receipt: &'a CommitReceipt,
    ) -> BoxFuture<'a, Result<(), PostCommitError>>;
}

/// Safe local-only flush: refuses to pretend remote jobs have been confirmed.
pub struct LocalOnlyFlush;
impl Flush for LocalOnlyFlush {
    fn flush<'a>(
        &'a self,
        receipt: &'a CommitReceipt,
    ) -> BoxFuture<'a, Result<(), PostCommitError>> {
        Box::pin(async move {
            if receipt.remote.is_empty() {
                Ok(())
            } else {
                Err(PostCommitError::Flush)
            }
        })
    }
}

/// Borrowed asynchronous typed handler result at the explicit registration boundary.
pub type HandleFuture<'a, R, S, E> = BoxFuture<'a, Result<HandlerOutput<R, S>, E>>;

/// Explicit functional analogue of per-message scoped handlers and compound phases.
/// Singleton dependencies are ordinary fields (usually Arcs); Resources is freshly
/// constructed per attempt and asynchronously disposed after success/error/cancellation.
pub trait ScopedHandler<M>: Send + Sync + 'static {
    /// Typed loaded values, transient services, and scoped repositories.
    type Resources: Send + 'static;
    /// The successful invocation response, independent of effects.
    type Reply: Send + 'static;
    /// Concrete application error, not an untyped exception payload.
    type Error: StdError + Send + Sync + 'static;

    /// Builds a fresh scope against the exact transaction later used by the handler.
    /// If construction is cancelled before returning, construction owns its cleanup.
    fn create<'a>(
        &'a self,
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<Self::Resources, Self::Error>>;

    /// Runs Before/Load/Validate/After/PostProcess in documented order.
    fn phase<'a>(
        &'a self,
        _phase: Phase,
        _message: &'a M,
        _resources: &'a mut Self::Resources,
        _scope: &'a mut Scope<'static>,
        _context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }

    /// Pure decisions may return storage actions, outgoing intents and inline actions.
    fn handle<'a>(
        &'a self,
        message: &'a M,
        resources: &'a mut Self::Resources,
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> HandleFuture<'a, Self::Reply, Self::Resources, Self::Error>;

    /// Observes root failure before Finally; cannot silently swallow retry state.
    /// Typed matching/recovery can be explicit application code; this first-wave
    /// hook is observational, not a full exception-policy/recovery graph.
    fn on_exception<'a>(
        &'a self,
        _error: &'a Failure<Self::Error>,
        _resources: &'a mut Self::Resources,
        _context: &'a MessageContext,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }

    /// Runs only after commit AND successful confirmed flush, without access to
    /// the completed transaction. New SQL/outgoing work needs a fresh transaction.
    fn after_commit<'a>(
        &'a self,
        _resources: &'a mut Self::Resources,
        _receipt: &'a CommitReceipt,
        _context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }

    /// Runs once after success, failure or cooperative cancellation, before disposal.
    fn finally<'a>(
        &'a self,
        _completion: Completion,
        _resources: &'a mut Self::Resources,
        _context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }

    /// Deterministic async disposal, including after a dropped caller. The host
    /// must be joined; forced host abort/process death can only provide Rust Drop.
    fn dispose<'a>(
        &'a self,
        resources: Self::Resources,
        _context: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            drop(resources);
            Ok(())
        })
    }
}

pub(crate) async fn run<M, H>(
    handler: Arc<H>,
    scope: Scope<'static>,
    context: MessageContext,
    deadline: Instant,
    flush: Arc<dyn Flush>,
) -> Result<Committed<H::Reply>, Failure<H::Error>>
where
    M: WireMessage,
    H: ScopedHandler<M>,
{
    run_inner::<M, H>(handler, scope, context, deadline, flush, false).await
}

// Policy retries must never hide a rollback failure as an ordinary domain error.
pub(crate) async fn run_strict<M, H>(
    handler: Arc<H>,
    scope: Scope<'static>,
    context: MessageContext,
    deadline: Instant,
    flush: Arc<dyn Flush>,
) -> Result<Committed<H::Reply>, Failure<H::Error>>
where
    M: WireMessage,
    H: ScopedHandler<M>,
{
    run_inner::<M, H>(handler, scope, context, deadline, flush, true).await
}

async fn run_inner<M, H>(
    handler: Arc<H>,
    scope: Scope<'static>,
    context: MessageContext,
    deadline: Instant,
    flush: Arc<dyn Flush>,
    strict_rollback: bool,
) -> Result<Committed<H::Reply>, Failure<H::Error>>
where
    M: WireMessage,
    H: ScopedHandler<M>,
{
    let message = context.envelope().decode::<M>()?;
    let mut scope = scope;
    let mut resources = match bounded(deadline, &context, async {
        handler
            .create(&mut scope, &context)
            .await
            .map_err(Failure::Handler)
    })
    .await
    {
        Ok(resources) => resources,
        Err(error) => {
            if let Err(rollback) = scope.rollback().await {
                return Err(if strict_rollback {
                    Failure::UncertainDatabase(rollback)
                } else {
                    Failure::Database(rollback)
                });
            }
            return Err(error);
        }
    };
    let result = bounded(
        deadline,
        &context,
        prepare(&*handler, &message, &mut resources, &mut scope, &context),
    )
    .await;
    let result = match result {
        Ok((reply, receipt)) => {
            // Never drop an in-progress commit on caller cancellation. Commit I/O
            // failure has an uncertain outcome and needs identity reconciliation.
            match scope.commit(Utc::now()).await {
                Ok(()) => {
                    let post_commit = match AssertUnwindSafe(async { flush.flush(&receipt).await })
                        .catch_unwind()
                        .await
                    {
                        Ok(Ok(())) => match AssertUnwindSafe(async {
                            handler
                                .after_commit(&mut resources, &receipt, &context)
                                .await
                        })
                        .catch_unwind()
                        .await
                        {
                            Ok(Ok(())) => Ok(()),
                            _ => Err(PostCommitError::Callback),
                        },
                        Ok(Err(error)) => Err(error),
                        Err(_) => Err(PostCommitError::Flush),
                    };
                    Ok(Committed {
                        reply,
                        receipt,
                        post_commit,
                    })
                }
                Err(error) => Err(if strict_rollback {
                    Failure::UncertainDatabase(error)
                } else {
                    Failure::Database(error)
                }),
            }
        }
        Err(error) => {
            // Keep the original failure if rollback also fails; the connection's
            // drop still prevents it being reused as a successful transaction.
            match scope.rollback().await {
                Err(rollback) if strict_rollback => Err(Failure::UncertainDatabase(rollback)),
                _ => Err(error),
            }
        }
    };
    if let Err(error) = &result {
        let _ =
            AssertUnwindSafe(async { handler.on_exception(error, &mut resources, &context).await })
                .catch_unwind()
                .await;
    }
    let completion = if result.is_ok() {
        Completion::Committed
    } else {
        Completion::Failed
    };
    let final_result =
        AssertUnwindSafe(async { handler.finally(completion, &mut resources, &context).await })
            .catch_unwind()
            .await;
    let dispose_result = AssertUnwindSafe(async { handler.dispose(resources, &context).await })
        .catch_unwind()
        .await;
    match result {
        Ok(mut committed) => {
            if committed.post_commit.is_ok()
                && (!matches!(final_result, Ok(Ok(()))) || !matches!(dispose_result, Ok(Ok(()))))
            {
                committed.post_commit = Err(PostCommitError::Cleanup);
            }
            Ok(committed)
        }
        Err(error) => Err(error),
    }
}

async fn bounded<T, E: StdError + 'static>(
    deadline: Instant,
    context: &MessageContext,
    future: impl std::future::Future<Output = Result<T, Failure<E>>>,
) -> Result<T, Failure<E>> {
    tokio::select! {
        biased;
        () = context.cancellation().cancelled() => Err(Failure::Cancelled),
        _ = tokio::time::sleep_until(deadline) => { context.cancellation().cancel(); Err(Failure::TimedOut) },
        result = AssertUnwindSafe(future).catch_unwind() => result.unwrap_or(Err(Failure::Panicked)),
    }
}

async fn prepare<M, H>(
    handler: &H,
    message: &M,
    resources: &mut H::Resources,
    scope: &mut Scope<'static>,
    context: &MessageContext,
) -> Result<(H::Reply, CommitReceipt), Failure<H::Error>>
where
    M: WireMessage,
    H: ScopedHandler<M>,
{
    for phase in [Phase::Before, Phase::Load, Phase::Validate] {
        handler
            .phase(phase, message, resources, scope, context)
            .await
            .map_err(Failure::Handler)?;
    }
    let output = handler
        .handle(message, resources, scope, context)
        .await
        .map_err(Failure::Handler)?;
    let mut outgoing = Vec::new();
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
        handler
            .phase(phase, message, resources, scope, context)
            .await
            .map_err(Failure::Handler)?;
    }
    let mut receipt = CommitReceipt {
        tenant: context.origin().tenant().to_owned(),
        ..Default::default()
    };
    for outgoing in outgoing {
        let envelope = outgoing
            .envelope
            .correlated(context.correlation_id(), context.envelope().id())?;
        let jobs = scope
            .stage(
                context.origin().source(),
                context.origin().tenant(),
                &envelope,
                &outgoing.deliveries,
            )
            .await?;
        for (id, delivery) in jobs.iter().zip(&outgoing.deliveries) {
            if delivery.kind == raddle_postgres::DeliveryKind::Remote {
                receipt.remote.push((*id, delivery.destination.clone()));
            }
        }
        receipt.jobs.extend(jobs);
    }
    Ok((output.reply, receipt))
}

/// Confirmed outbox flush using the existing worker and real fenced job settlement.
/// One bounded send pass is attempted per destination. Pending/failing jobs remain
/// recoverable; no parent retry is manufactured and AfterCommit is not called early.
pub struct ConfirmedFlush<T> {
    store: Store,
    worker: raddle_postgres::OutboxWorker<T>,
}
impl<T: raddle::transport::Transport> ConfirmedFlush<T> {
    /// Uses the same store as the handler transaction and normal outbox policy.
    pub fn new(store: Store, transport: T, policy: raddle_postgres::RetryPolicy) -> Self {
        Self {
            worker: raddle_postgres::OutboxWorker::new(store.clone(), transport, policy),
            store,
        }
    }
}
impl<T: raddle::transport::Transport + 'static> Flush for ConfirmedFlush<T> {
    fn flush<'a>(
        &'a self,
        receipt: &'a CommitReceipt,
    ) -> BoxFuture<'a, Result<(), PostCommitError>> {
        Box::pin(async move {
            let destinations: std::collections::BTreeSet<_> = receipt
                .remote
                .iter()
                .map(|(_, destination)| destination)
                .collect();
            for destination in destinations {
                self.worker
                    .run_once_for(destination, 256)
                    .await
                    .map_err(|_| PostCommitError::Flush)?;
            }
            for (id, _) in &receipt.remote {
                if !self
                    .store
                    .inspect(&receipt.tenant, *id)
                    .await
                    .map_err(|_| PostCommitError::Flush)?
                    .is_some_and(|job| job.status == raddle_postgres::JobStatus::Completed)
                {
                    return Err(PostCommitError::Flush);
                }
            }
            Ok(())
        })
    }
}
