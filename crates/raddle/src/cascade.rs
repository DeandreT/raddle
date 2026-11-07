//! Bounded, breadth-first local effects. No recursive dispatch or implicit retry.

use std::any::{self, Any};
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

use futures_util::future::BoxFuture;

use crate::{Event, ExecutionContext, ExecutionError, PublishReport, Request, Runtime};

/// A typed handler reply with explicitly owned, not-yet-executed follow-up work.
///
/// Ordinary `Runtime::invoke` returns this value without executing its effects.
/// Use `Runtime::invoke_cascade` to execute them after releasing root admission.
#[derive(Debug)]
pub struct Cascade<R> {
    reply: R,
    effects: Effects,
}

impl<R> Cascade<R> {
    /// Couples a typed reply with a bounded effect batch.
    pub fn new(reply: R, effects: Effects) -> Self {
        Self { reply, effects }
    }

    /// Constructs a successful reply with no follow-up effects.
    pub fn reply(reply: R) -> Self {
        Self::new(reply, Effects::default())
    }

    /// Takes the reply and pending effects without executing anything.
    pub fn into_parts(self) -> (R, Effects) {
        (self.reply, self.effects)
    }
}

/// An owned, capacity-bounded effect batch. Its default capacity is 64.
///
/// A rejected append returns the effect to the caller; it is never silently lost.
#[derive(Debug)]
pub struct Effects {
    effects: Vec<Effect>,
    capacity: NonZeroUsize,
}

impl Effects {
    /// Creates an empty batch with an explicit finite maximum.
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            effects: Vec::new(),
            capacity,
        }
    }

    /// Appends in execution order, or returns ownership when the batch is full.
    pub fn push(&mut self, effect: Effect) -> Result<(), Effect> {
        if self.effects.len() == self.capacity.get() {
            return Err(effect);
        }
        self.effects.push(effect);
        Ok(())
    }

    /// Number of pending effects.
    pub fn len(&self) -> usize {
        self.effects.len()
    }

    /// Whether this batch contains no effects.
    pub fn is_empty(&self) -> bool {
        self.effects.is_empty()
    }
}

impl Default for Effects {
    fn default() -> Self {
        Self {
            effects: Vec::new(),
            capacity: NonZeroUsize::MIN.saturating_add(63),
        }
    }
}

type EffectOperation =
    Box<dyn FnOnce(Runtime, ExecutionContext) -> BoxFuture<'static, Completion> + Send>;

/// A typed command or event erased only after its dispatch function is assembled.
///
/// Effects are consumed exactly once. No message cloning, task spawning, runtime
/// reflection, URI dispatch, or automatic handler retries are involved.
pub struct Effect {
    message: &'static str,
    operation: EffectOperation,
}

impl fmt::Debug for Effect {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Effect")
            .field("message", &self.message)
            .finish_non_exhaustive()
    }
}

impl Effect {
    /// Invokes an ordinary request and retains its typed reply or execution error.
    pub fn command<M: Request>(message: M) -> Self {
        Self {
            message: any::type_name::<M>(),
            operation: Box::new(move |runtime, context| {
                Box::pin(async move {
                    match runtime.invoke_with_context(message, context).await {
                        Ok(reply) => Completion::success(reply, Effects::default()),
                        Err(error) => Completion::failure(error),
                    }
                })
            }),
        }
    }

    /// Invokes a request whose typed reply explicitly supplies further effects.
    pub fn cascading<M, R>(message: M) -> Self
    where
        M: Request<Reply = Cascade<R>>,
        R: Send + 'static,
    {
        Self {
            message: any::type_name::<M>(),
            operation: Box::new(move |runtime, context| {
                Box::pin(async move {
                    match runtime.invoke_with_context(message, context).await {
                        Ok(cascade) => Completion::success(cascade.reply, cascade.effects),
                        Err(error) => Completion::failure(error),
                    }
                })
            }),
        }
    }

    /// Publishes once and retains every typed subscriber result, including failures.
    pub fn event<E: Event>(event: E) -> Self {
        Self::event_then(event, |_| Effects::default())
    }

    /// Publishes once, then derives children from its successful typed report.
    ///
    /// The continuation runs only if every subscriber succeeds, after all event
    /// permits have been released. A partial failure retains the entire report
    /// and never retries successful subscriptions or creates follow-up effects.
    pub fn event_then<E, F>(event: E, continuation: F) -> Self
    where
        E: Event,
        F: FnOnce(&PublishReport<E::Error>) -> Effects + Send + 'static,
    {
        Self {
            message: any::type_name::<E>(),
            operation: Box::new(move |runtime, context| {
                Box::pin(async move {
                    let report = runtime.publish_with_context(event, context).await;
                    if report.is_success() {
                        let effects = continuation(&report);
                        Completion::success(report, effects)
                    } else {
                        Completion {
                            result: EffectResult::Failed(EffectFailure::Subscribers(
                                EffectValue::new(report),
                            )),
                            effects: Effects::default(),
                        }
                    }
                })
            }),
        }
    }
}

struct Completion {
    result: EffectResult,
    effects: Effects,
}

impl Completion {
    fn success<T: Send + 'static>(value: T, effects: Effects) -> Self {
        Self {
            result: EffectResult::Succeeded(EffectValue::new(value)),
            effects,
        }
    }

    fn failure<E: Error + Send + Sync + 'static>(error: E) -> Self {
        Self {
            result: EffectResult::Failed(EffectFailure::Command(Box::new(error))),
            effects: Effects::default(),
        }
    }
}

/// An owned heterogeneous reply or subscriber report, inspectable by concrete type.
pub struct EffectValue {
    value: Box<dyn Any + Send>,
}

impl EffectValue {
    fn new<T: Send + 'static>(value: T) -> Self {
        Self {
            value: Box::new(value),
        }
    }

    /// Borrows a typed reply or `PublishReport<E::Error>` without copying it.
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.value.downcast_ref()
    }

    /// Takes a typed reply, returning the original value if the type differs.
    pub fn downcast<T: Send + 'static>(self) -> Result<T, Self> {
        self.value
            .downcast()
            .map(|value| *value)
            .map_err(|value| Self { value })
    }
}

impl fmt::Debug for EffectValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EffectValue")
            .finish_non_exhaustive()
    }
}

/// A command failure or a complete partially failed event report.
#[derive(Debug)]
#[non_exhaustive]
pub enum EffectFailure {
    /// The concrete `ExecutionError<M::Error>`, available through error downcasting.
    Command(Box<dyn Error + Send + Sync>),
    /// The concrete `PublishReport<E::Error>`, preserving successful siblings too.
    Subscribers(EffectValue),
}

/// Why a returned effect was not executed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CascadeLimit {
    /// The finite total effect-admission budget was exhausted.
    Effects,
    /// The effect would exceed the maximum number of edges from the root.
    Depth,
}

/// A single effect's independent outcome; a limit rejection never invokes a handler.
#[derive(Debug)]
#[non_exhaustive]
pub enum EffectResult {
    /// The command reply or event report.
    Succeeded(EffectValue),
    /// Execution failed; unrelated queued effects still run.
    Failed(EffectFailure),
    /// This work was never started because a configured bound was reached.
    Rejected {
        /// Which configured bound prevented admission.
        reason: CascadeLimit,
        /// Never-started work returned to the caller for explicit recovery.
        effect: Effect,
    },
}

/// One effect outcome with its breadth-first sequence and execution lineage.
#[derive(Debug)]
pub struct EffectOutcome {
    sequence: usize,
    parent: Option<usize>,
    context: ExecutionContext,
    message: &'static str,
    result: EffectResult,
}

impl EffectOutcome {
    /// Stable sequence within this run; rejected effects also have a sequence.
    pub fn sequence(&self) -> usize {
        self.sequence
    }
    /// Parent effect sequence, absent for immediate children of the root.
    pub fn parent(&self) -> Option<usize> {
        self.parent
    }
    /// Local correlation, causation, and depth.
    pub fn context(&self) -> ExecutionContext {
        self.context
    }
    /// Rust type name for diagnostics, not wire routing.
    pub fn message(&self) -> &'static str {
        self.message
    }
    /// Independent typed result or explicit limit rejection.
    pub fn result(&self) -> &EffectResult {
        &self.result
    }
    /// Takes the outcome's typed value or error.
    pub fn into_result(self) -> EffectResult {
        self.result
    }
}

/// Finite bounds for a run, counting effects but not the root handler.
#[derive(Clone, Copy, Debug)]
pub struct CascadeLimits {
    max_effects: NonZeroUsize,
    max_depth: NonZeroUsize,
}

impl CascadeLimits {
    /// Sets total admitted effects and maximum child depth. Both must be positive.
    pub fn new(max_effects: NonZeroUsize, max_depth: NonZeroUsize) -> Self {
        Self {
            max_effects,
            max_depth,
        }
    }
}

impl Default for CascadeLimits {
    fn default() -> Self {
        Self {
            max_effects: NonZeroUsize::MIN.saturating_add(255),
            max_depth: NonZeroUsize::MIN.saturating_add(15),
        }
    }
}

/// Typed root reply plus every produced effect outcome, in admission sequence.
#[derive(Debug)]
pub struct CascadeReport<R> {
    reply: R,
    outcomes: Vec<EffectOutcome>,
}

impl<R> CascadeReport<R> {
    /// Borrows the root's reply independently of downstream failures.
    pub fn reply(&self) -> &R {
        &self.reply
    }
    /// Includes successful, failed, and rejected effects; no sibling is hidden.
    pub fn outcomes(&self) -> impl ExactSizeIterator<Item = &EffectOutcome> {
        self.outcomes.iter()
    }
    /// Whether all effects completed successfully, with no limit rejection.
    pub fn is_success(&self) -> bool {
        self.outcomes
            .iter()
            .all(|outcome| matches!(outcome.result, EffectResult::Succeeded(_)))
    }
    /// Takes the root reply and all effect outcomes.
    pub fn into_parts(self) -> (R, Vec<EffectOutcome>) {
        (self.reply, self.outcomes)
    }
}

struct Queued {
    sequence: usize,
    parent: Option<usize>,
    context: ExecutionContext,
    effect: Effect,
}

impl Runtime {
    /// Invokes a cascading root, releases its permit, then drains bounded effects.
    ///
    /// Root failure starts no effects. Downstream failure is reported per effect
    /// and does not roll back the successful root or cancel unrelated effects.
    /// Dropping this future cancels unfinished work; it cannot undo completed
    /// effects. No resumable receipt or exactly-once guarantee is implied.
    pub async fn invoke_cascade<M, R>(
        &self,
        message: M,
        limits: CascadeLimits,
    ) -> Result<CascadeReport<R>, ExecutionError<M::Error>>
    where
        M: Request<Reply = Cascade<R>>,
        R: Send + 'static,
    {
        let context = ExecutionContext::new();
        let cascade = self.invoke_with_context(message, context).await?;
        let outcomes = self.drain_effects(cascade.effects, context, limits).await;
        Ok(CascadeReport {
            reply: cascade.reply,
            outcomes,
        })
    }

    /// Runs an explicitly owned batch without a root handler or held permit.
    pub async fn cascade(&self, effects: Effects, limits: CascadeLimits) -> CascadeReport<()> {
        let outcomes = self
            .drain_effects(effects, ExecutionContext::new(), limits)
            .await;
        CascadeReport {
            reply: (),
            outcomes,
        }
    }

    async fn drain_effects(
        &self,
        effects: Effects,
        context: ExecutionContext,
        limits: CascadeLimits,
    ) -> Vec<EffectOutcome> {
        let mut runner = Runner {
            limits,
            admitted: 0,
            sequence: 0,
            pending: VecDeque::new(),
            outcomes: Vec::new(),
        };
        runner.enqueue(effects, context, None);
        while let Some(queued) = runner.pending.pop_front() {
            // invoke/publish has returned and released admission before children
            // enter this queue, including with a concurrency limit of one.
            let completion = (queued.effect.operation)(self.clone(), queued.context).await;
            runner.outcomes.push(EffectOutcome {
                sequence: queued.sequence,
                parent: queued.parent,
                context: queued.context,
                message: queued.effect.message,
                result: completion.result,
            });
            runner.enqueue(completion.effects, queued.context, Some(queued.sequence));
        }
        runner
            .outcomes
            .sort_unstable_by_key(|outcome| outcome.sequence);
        runner.outcomes
    }
}

struct Runner {
    limits: CascadeLimits,
    admitted: usize,
    sequence: usize,
    pending: VecDeque<Queued>,
    outcomes: Vec<EffectOutcome>,
}

impl Runner {
    fn enqueue(
        &mut self,
        effects: Effects,
        parent_context: ExecutionContext,
        parent: Option<usize>,
    ) {
        for effect in effects.effects {
            let sequence = self.sequence;
            self.sequence = self.sequence.saturating_add(1);
            let context = parent_context.child();
            let rejection = if context.depth() > self.limits.max_depth.get() {
                Some(CascadeLimit::Depth)
            } else if self.admitted >= self.limits.max_effects.get() {
                Some(CascadeLimit::Effects)
            } else {
                None
            };
            if let Some(limit) = rejection {
                self.outcomes.push(EffectOutcome {
                    sequence,
                    parent,
                    context,
                    message: effect.message,
                    result: EffectResult::Rejected {
                        reason: limit,
                        effect,
                    },
                });
            } else {
                self.admitted += 1;
                self.pending.push_back(Queued {
                    sequence,
                    parent,
                    context,
                    effect,
                });
            }
        }
    }
}
