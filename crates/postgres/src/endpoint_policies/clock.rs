use chrono::{DateTime, Utc};
use std::time::Instant;

use crate::Error;

/// Preserve the caller's time domain while accounting for real admission waits.
/// Never compare an injected logical timestamp with the server's wall clock.
pub(super) struct EffectiveNow {
    base: DateTime<Utc>,
    entered: Instant,
}

impl EffectiveNow {
    pub(super) fn new(base: DateTime<Utc>) -> Self {
        Self {
            base,
            entered: Instant::now(),
        }
    }

    pub(super) fn current(&self) -> Result<DateTime<Utc>, Error> {
        let elapsed = chrono::Duration::from_std(self.entered.elapsed())
            .map_err(|_| Error::Invalid("policy clock overflow"))?;
        let now = self
            .base
            .checked_add_signed(elapsed)
            .ok_or(Error::Invalid("policy clock overflow"))?;
        // PostgreSQL timestamps and the canonical custody comparisons use micros.
        DateTime::from_timestamp_micros(now.timestamp_micros())
            .ok_or(Error::Invalid("policy clock overflow"))
    }
}
