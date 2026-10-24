use std::time::Duration;

use chrono::{DateTime, Utc};

/// An explicit test clock. Advancing it never waits for wall time.
#[derive(Clone, Copy, Debug)]
pub struct ManualClock {
    now: DateTime<Utc>,
}

/// A requested advance exceeded the representable clock range.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("clock advance is outside the supported range")]
pub struct ClockError;

impl ManualClock {
    /// Starts at a caller-selected instant.
    pub fn new(now: DateTime<Utc>) -> Self {
        Self { now }
    }
    /// Returns the current instant without consulting the operating system.
    pub fn now(&self) -> DateTime<Utc> {
        self.now
    }
    /// Advances monotonically or returns an error without changing the clock.
    pub fn advance(&mut self, elapsed: Duration) -> Result<DateTime<Utc>, ClockError> {
        let elapsed = chrono::Duration::from_std(elapsed).map_err(|_| ClockError)?;
        self.now = self.now.checked_add_signed(elapsed).ok_or(ClockError)?;
        Ok(self.now)
    }
}
