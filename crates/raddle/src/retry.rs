//! Explicit bounded retry of owned operations, never automatic handler replay.

use std::future::Future;
use std::num::NonZeroUsize;
use std::time::Duration;

/// Application assertion explaining why replay of returned state is safe.
///
/// The framework cannot verify this assertion. Do not claim rollback if an
/// external side effect may have occurred; use a stable idempotency key instead.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RetrySafety {
    /// All failed-attempt effects have been rolled back before returning state.
    RolledBack,
    /// Re-execution uses an application-enforced idempotency boundary.
    Idempotent,
}

/// One owned attempt result. Only `Retry` authorizes another execution.
#[derive(Debug)]
#[non_exhaustive]
pub enum Attempt<T, E, S> {
    /// Completed; successful work is not executed again.
    Complete(T),
    /// A terminal failure, regardless of remaining retry budget.
    Failed(E),
    /// Explicitly returns safe owned state and the requested bounded delay.
    Retry {
        /// State returned after rollback or with retained idempotency identity.
        state: S,
        /// Failure that caused this retry request.
        error: E,
        /// Why the application permits this particular re-execution.
        safety: RetrySafety,
        /// Delay before the next attempt; no runtime-owned timer is spawned.
        after: Duration,
    },
}

/// Finite attempt and per-delay bounds. No retry is enabled by default.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    max_attempts: NonZeroUsize,
    max_delay: Duration,
}

impl RetryPolicy {
    /// Counts the initial attempt in the positive attempt budget.
    pub fn new(max_attempts: NonZeroUsize, max_delay: Duration) -> Self {
        Self {
            max_attempts,
            max_delay,
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(NonZeroUsize::MIN, Duration::ZERO)
    }
}

/// Terminal disposition retaining the final typed domain error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RetryError<E> {
    /// The operation explicitly refused further execution.
    #[error("operation failed: {0}")]
    Failed(#[source] E),
    /// The operation requested retry, but no attempts remained.
    #[error("retry attempt budget exhausted: {0}")]
    Exhausted(#[source] E),
    /// The requested delay exceeded policy; no further attempt started.
    #[error("retry delay exceeds policy: {0}")]
    DelayExceeded(#[source] E),
}

/// Final typed result and the number of attempts actually started.
#[derive(Debug)]
pub struct RetryReport<T, E> {
    attempts: usize,
    result: Result<T, RetryError<E>>,
}

impl<T, E> RetryReport<T, E> {
    /// Number of operation calls, including the initial attempt.
    pub fn attempts(&self) -> usize {
        self.attempts
    }
    /// Borrows the typed result and terminal disposition.
    pub fn result(&self) -> Result<&T, &RetryError<E>> {
        self.result.as_ref()
    }
    /// Takes the result without losing the final domain error.
    pub fn into_result(self) -> Result<T, RetryError<E>> {
        self.result
    }
}

/// Executes an owned operation under explicit application-approved retry policy.
///
/// The operation receives a one-based attempt number and must return ownership
/// on each authorized retry. No input is cloned and no arbitrary runtime handler
/// is automatically replayed. Dropping this future cancels the attempt or delay;
/// it does not undo effects. Panics unwind normally.
pub async fn retry_owned<S, T, E, F, Fut>(
    mut state: S,
    policy: RetryPolicy,
    mut operation: F,
) -> RetryReport<T, E>
where
    F: FnMut(S, usize) -> Fut,
    Fut: Future<Output = Attempt<T, E, S>>,
{
    let mut attempts = 0;
    loop {
        attempts += 1;
        let result = match operation(state, attempts).await {
            Attempt::Complete(value) => Ok(value),
            Attempt::Failed(error) => Err(RetryError::Failed(error)),
            Attempt::Retry {
                state: returned,
                error,
                safety: _,
                after,
            } => {
                if attempts >= policy.max_attempts.get() {
                    Err(RetryError::Exhausted(error))
                } else if after > policy.max_delay {
                    Err(RetryError::DelayExceeded(error))
                } else {
                    // The owned state is restored only after the application
                    // explicitly declares this attempt safe to re-execute.
                    state = returned;
                    if !after.is_zero() {
                        tokio::time::sleep(after).await;
                    }
                    continue;
                }
            }
        };
        return RetryReport { attempts, result };
    }
}
