use std::collections::VecDeque;
use std::error::Error;
use std::time::Duration;

use crate::{ErrorFilter, PolicyErrorCode};

/// Receive circuit configuration. Windows use a caller-owned monotonic clock.
pub struct CircuitOptions {
    /// Minimum completed samples before ratio can trip the circuit.
    pub minimum_samples: usize,
    /// Failure threshold in basis points: 1500 means 15 percent.
    pub failure_basis_points: u16,
    /// Rolling observation window.
    pub window: Duration,
    /// Endpoint admission cooldown after trip.
    pub pause: Duration,
    /// Hard memory bound; the oldest samples are evicted if full.
    pub maximum_samples: usize,
    /// Errors counted as failures. Others are excluded entirely.
    pub include: ErrorFilter,
    /// Excluded errors win over included errors.
    pub exclude: Option<ErrorFilter>,
}
impl Default for CircuitOptions {
    fn default() -> Self {
        Self {
            minimum_samples: 10,
            failure_basis_points: 1500,
            window: Duration::from_secs(600),
            pause: Duration::from_secs(300),
            maximum_samples: 100_000,
            include: ErrorFilter::any(),
            exclude: None,
        }
    }
}

/// Admission is per endpoint. One probe is permitted after a pause expires.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CircuitAdmission {
    /// Normal operation.
    Open,
    /// Admission stops until the supplied monotonic time.
    Paused(Duration),
    /// Exactly one probe may run; callers must report its outcome/cancellation.
    Probe,
}

/// Bounded receive-side failure sampling; shutdown is never an observation.
pub struct CircuitBreaker {
    options: CircuitOptions,
    samples: VecDeque<(Duration, bool)>,
    paused_until: Option<Duration>,
    probing: bool,
}
impl CircuitBreaker {
    /// Validates samples, ratio and nonzero tracking/pause durations.
    pub fn new(options: CircuitOptions) -> Result<Self, PolicyErrorCode> {
        if options.minimum_samples == 0
            || options.maximum_samples < options.minimum_samples
            || options.maximum_samples > 1_000_000
            || !(1..=10000).contains(&options.failure_basis_points)
            || options.window.is_zero()
            || options.pause.is_zero()
        {
            return Err(PolicyErrorCode);
        }
        Ok(Self {
            options,
            samples: VecDeque::new(),
            paused_until: None,
            probing: false,
        })
    }
    /// Checks admission. A second concurrent probe remains blocked.
    pub fn admission(&mut self, now: Duration) -> CircuitAdmission {
        match self.paused_until {
            Some(until) if now < until || self.probing => CircuitAdmission::Paused(until),
            Some(_) => {
                self.probing = true;
                CircuitAdmission::Probe
            }
            None => CircuitAdmission::Open,
        }
    }
    /// Records a completion; errors filtered out do not dilute the ratio.
    /// `shutdown` cancels an outstanding probe without adding a sample.
    pub fn record(
        &mut self,
        now: Duration,
        error: Option<&(dyn Error + 'static)>,
        shutdown: bool,
    ) -> Result<Option<Duration>, PolicyErrorCode> {
        if shutdown {
            self.cancel_probe();
            return Ok(None);
        }
        if let Some(error) = error
            && (!self.options.include.matches(error)
                || self
                    .options
                    .exclude
                    .as_ref()
                    .is_some_and(|filter| filter.matches(error)))
        {
            self.probing = false;
            return Ok(None);
        }
        let failure = error.is_some();
        if self.probing {
            self.probing = false;
            if failure {
                return self.trip(now).map(Some);
            }
            self.paused_until = None;
            self.samples.clear();
        }
        if self.paused_until.is_some() {
            return Ok(self.paused_until);
        }
        while self
            .samples
            .front()
            .is_some_and(|(time, _)| now.saturating_sub(*time) >= self.options.window)
        {
            self.samples.pop_front();
        }
        if self.samples.len() == self.options.maximum_samples {
            self.samples.pop_front();
        }
        self.samples.push_back((now, failure));
        let failures = self.samples.iter().filter(|(_, failed)| *failed).count();
        if self.samples.len() >= self.options.minimum_samples
            && (failures as u128) * 10000
                >= (self.samples.len() as u128) * u128::from(self.options.failure_basis_points)
        {
            return self.trip(now).map(Some);
        }
        Ok(None)
    }
    fn trip(&mut self, now: Duration) -> Result<Duration, PolicyErrorCode> {
        let until = now.checked_add(self.options.pause).ok_or(PolicyErrorCode)?;
        self.samples.clear();
        self.paused_until = Some(until);
        Ok(until)
    }
    /// Number of retained observations, useful for payload-free diagnostics.
    pub fn samples(&self) -> usize {
        self.samples.len()
    }
    /// Releases an interrupted probe without success/failure observation.
    pub fn cancel_probe(&mut self) {
        self.probing = false;
    }
}
