use std::error::Error;
use std::future::Future;
use std::time::Duration;

use tokio::sync::watch;

use crate::{Action, AttemptCounters, Direction, PolicySet, ReplaySafety, Step};

/// Errors never include message content, keys, or formatted handler errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum DriverError {
    /// Driver has no active claim, is full, or cannot preserve custody.
    #[error("endpoint custody transition failed")]
    Custody,
    /// Invalid policy counters, duration overflow, or unsupported configuration.
    #[error("invalid endpoint policy state")]
    Invalid,
}

/// Inline calls cannot silently discard, queue, or dead-letter caller-owned work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionMode {
    /// Queue owns the work and applies all selected recovery actions.
    Queued,
    /// Only immediate and cooldown recovery applies; other actions stop.
    Inline,
}

/// Driver-bound recovery context. Attempt counts include the original call.
pub struct Execution<'a> {
    /// Configured endpoint, not a message-provided URI.
    pub endpoint: &'a str,
    /// Stable message contract name.
    pub contract: &'a str,
    /// Selects independent sending versus handling policies.
    pub direction: Direction,
    /// Observed attempt counts, preserved across scheduling/requeue.
    pub attempts: AttemptCounters,
    /// Explicit caller authorization for repeated effects.
    pub replay: ReplaySafety,
    /// Whether custody belongs to a queue or the inline caller.
    pub mode: ExecutionMode,
    /// Monotonic clock value for driver scheduling.
    pub now: Duration,
    /// Rule-wide jitter sample; source ownership belongs to the application.
    pub jitter_sample: u64,
}

/// Atomic custody operation performed by an actual endpoint driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Transition {
    /// Retain the current claim for another inline attempt.
    RetryInline,
    /// Release the active claim at the queue tail, due at this time.
    QueueAt(Duration),
    /// Terminal discard, without dead-letter retention.
    Discard,
    /// Terminal quarantine. Unsafe replay refusal is separately classified.
    DeadLetter {
        /// The application did not authorize repeating effects.
        replay_denied: bool,
    },
    /// Shutdown leaves work recoverable and does not advance terminal state.
    Retain,
    /// Inline policy stopped; propagate original error to the caller.
    StopInline,
}

/// Real custody boundary. Implementations must be atomic per active delivery:
/// failed transitions keep that delivery recoverable, never acknowledge it.
/// Pauses are endpoint-local and must not discard unsent/unhandled work.
pub trait PolicyExecutorDriver: Send {
    /// Atomically changes the active delivery's custody state.
    fn transition(
        &mut self,
        transition: Transition,
    ) -> impl Future<Output = Result<(), DriverError>> + Send;
    /// Pauses admission, not cancellation of unrelated endpoint tasks.
    fn pause_until(
        &mut self,
        until: Duration,
    ) -> impl Future<Output = Result<(), DriverError>> + Send;
}

/// An immutable policy selector and executable recovery driver.
/// Never invokes application handlers or silently repeats side effects itself.
pub struct PolicyExecutor {
    policies: PolicySet,
}
impl PolicyExecutor {
    /// Takes ownership so startup configuration cannot change underneath work.
    pub fn new(policies: PolicySet) -> Self {
        Self { policies }
    }

    /// Executes the selected policy. A closed stop channel is also shutdown.
    /// Cancellation interrupts cooldown and retains custody without a failure.
    pub async fn failed<D: PolicyExecutorDriver>(
        &self,
        driver: &mut D,
        context: Execution<'_>,
        error: &(dyn Error + Send + Sync + 'static),
        stop: &mut watch::Receiver<bool>,
    ) -> Result<Transition, DriverError> {
        if stopped(stop) {
            driver.transition(Transition::Retain).await?;
            return Ok(Transition::Retain);
        }
        let step =
            match self
                .policies
                .select(context.direction, context.endpoint, context.contract, error)
            {
                Some(plan) => plan
                    .step(
                        context.attempts.get(context.direction),
                        context.jitter_sample,
                    )
                    .map_err(|_| DriverError::Invalid)?,
                None => Step::new(Action::DeadLetter),
            };
        if context.mode == ExecutionMode::Inline
            && !matches!(step.action, Action::Immediate | Action::Cooldown(_))
        {
            driver.transition(Transition::StopInline).await?;
            return Ok(Transition::StopInline);
        }
        let repeats = matches!(
            step.action,
            Action::Immediate | Action::Cooldown(_) | Action::Requeue | Action::Schedule(_)
        );
        if repeats && !context.replay.permits(context.direction) {
            let transition = if context.mode == ExecutionMode::Inline {
                Transition::StopInline
            } else {
                Transition::DeadLetter {
                    replay_denied: true,
                }
            };
            driver.transition(transition).await?;
            return Ok(transition);
        }
        if let Some(pause) = step.pause
            && context.mode == ExecutionMode::Queued
        {
            driver
                .pause_until(context.now.checked_add(pause).ok_or(DriverError::Invalid)?)
                .await?;
        }
        if let Action::Cooldown(delay) = step.action {
            let deadline = tokio::time::Instant::now()
                .checked_add(delay)
                .ok_or(DriverError::Invalid)?;
            tokio::select! {
                biased;
                _ = wait_for_stop(stop) => { driver.transition(Transition::Retain).await?; return Ok(Transition::Retain); }
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }
        if stopped(stop) {
            driver.transition(Transition::Retain).await?;
            return Ok(Transition::Retain);
        }
        let transition = match step.action {
            Action::Immediate | Action::Cooldown(_) => Transition::RetryInline,
            Action::Requeue => Transition::QueueAt(context.now),
            Action::Schedule(delay) => {
                Transition::QueueAt(context.now.checked_add(delay).ok_or(DriverError::Invalid)?)
            }
            Action::Discard => Transition::Discard,
            Action::DeadLetter => Transition::DeadLetter {
                replay_denied: false,
            },
        };
        driver.transition(transition).await?;
        Ok(transition)
    }
}

pub(crate) fn stopped(stop: &watch::Receiver<bool>) -> bool {
    *stop.borrow() || stop.has_changed().is_err()
}

pub(crate) async fn wait_for_stop(stop: &mut watch::Receiver<bool>) {
    loop {
        if stopped(stop) || stop.changed().await.is_err() {
            return;
        }
    }
}
