use std::time::Duration;

use tokio::sync::watch;

use crate::HostError;

/// Sanitized failure classification chosen by a service, not inferred from text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServiceFailure {
    /// A recoverable failure eligible for a bounded restart.
    Transient,
    /// Configuration or another non-recoverable failure; stop the host.
    Permanent,
    /// A task panic observed through Tokio's join boundary.
    Panicked,
}

/// Whether a task panic may consume a bounded restart attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PanicPolicy {
    /// Stop immediately on panic.
    FailFast,
    /// Recover a panic through the join boundary, subject to restart limits.
    Restart,
}

/// Finite exponential restart policy. Successful starts never reset the budget.
#[derive(Clone, Copy, Debug)]
pub struct RestartPolicy {
    pub(crate) maximum: u8,
    initial: Duration,
    maximum_delay: Duration,
    pub(crate) panics: PanicPolicy,
}

impl RestartPolicy {
    /// No restarts and fail-fast panics.
    pub const NEVER: Self = Self {
        maximum: 0,
        initial: Duration::from_millis(1),
        maximum_delay: Duration::from_millis(1),
        panics: PanicPolicy::FailFast,
    };

    /// Allows at most 32 restarts, with delays in 1ms..=60s.
    pub fn new(
        maximum_restarts: u8,
        initial: Duration,
        maximum_delay: Duration,
        panics: PanicPolicy,
    ) -> Result<Self, HostError> {
        if maximum_restarts > 32
            || initial < Duration::from_millis(1)
            || maximum_delay > Duration::from_secs(60)
            || initial > maximum_delay
        {
            return Err(HostError::Invalid("invalid restart bounds"));
        }
        Ok(Self {
            maximum: maximum_restarts,
            initial,
            maximum_delay,
            panics,
        })
    }

    pub(crate) fn delay(self, restarts: u8) -> Duration {
        self.initial
            .saturating_mul(1u32 << u32::from(restarts.saturating_sub(1).min(31)))
            .min(self.maximum_delay)
    }
}

/// Observed lifecycle of a single named service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServiceState {
    /// Registered, not started.
    Registered,
    /// Attempt started; dependency checks have not reported readiness.
    Starting,
    /// Service explicitly reported readiness.
    Ready,
    /// Waiting for a bounded restart; not ready.
    Backoff,
    /// Cooperative shutdown requested; not ready.
    Stopping,
    /// Attempt completed or was cancelled; not ready.
    Stopped,
    /// Terminal failure; not ready.
    Failed,
}

/// One service's independently sampled state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Status {
    /// Validated stable application name.
    pub name: String,
    /// Current lifecycle state.
    pub state: ServiceState,
    /// One-based attempt number; zero before startup.
    pub attempt: u8,
}

/// A fresh context per attempt. Old contexts cannot mark a new attempt ready.
pub struct ServiceContext {
    pub(crate) shutdown: watch::Receiver<bool>,
    pub(crate) status: watch::Sender<Status>,
    pub(crate) attempt: u8,
}

impl ServiceContext {
    /// Reports actual application/dependency readiness, or withdraws it.
    /// Reports after this attempt exits or shutdown starts are ignored.
    pub fn set_ready(&self, ready: bool) {
        self.status.send_if_modified(|status| {
            if status.attempt != self.attempt
                || !matches!(status.state, ServiceState::Starting | ServiceState::Ready)
                || *self.shutdown.borrow()
            {
                return false;
            }
            status.state = if ready {
                ServiceState::Ready
            } else {
                ServiceState::Starting
            };
            true
        });
    }

    /// Clones the cooperative shutdown receiver for existing worker APIs.
    pub fn shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown.clone()
    }

    /// Waits for shutdown, including closure of its host-owned sender.
    pub async fn cancelled(&mut self) {
        while !*self.shutdown.borrow_and_update() {
            if self.shutdown.changed().await.is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use super::{PanicPolicy, RestartPolicy};

    #[test]
    fn exponential_backoff_saturates_at_the_explicit_maximum() {
        let policy = RestartPolicy::new(
            32,
            Duration::from_millis(10),
            Duration::from_millis(35),
            PanicPolicy::FailFast,
        )
        .unwrap();
        assert_eq!(policy.delay(1), Duration::from_millis(10));
        assert_eq!(policy.delay(2), Duration::from_millis(20));
        for retry in 3..=32 {
            assert_eq!(policy.delay(retry), Duration::from_millis(35));
        }
    }
}
