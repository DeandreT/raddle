use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use crate::Error;

// Caller time may be logical or ahead of the physical clock. Advance that same
// clock across pool/writer waits instead of replacing it with wall-clock time.
pub(crate) struct Clock {
    initial: i64,
    fraction: u32,
    started: Instant,
}

impl Clock {
    pub(crate) fn new(now: DateTime<Utc>) -> Self {
        Self {
            initial: now.timestamp_millis(),
            fraction: now.timestamp_subsec_nanos() % 1_000_000,
            started: Instant::now(),
        }
    }

    pub(crate) fn now(&self) -> Result<i64, Error> {
        self.at_elapsed(self.started.elapsed())
    }

    fn at_elapsed(&self, elapsed: Duration) -> Result<i64, Error> {
        let carry = i64::from((self.fraction + elapsed.subsec_nanos() % 1_000_000) / 1_000_000);
        let elapsed = i64::try_from(elapsed.as_millis())
            .map_err(|_| Error::Invalid("operation clock overflow"))?;
        self.initial
            .checked_add(elapsed)
            .and_then(|now| now.checked_add(carry))
            .ok_or(Error::Invalid("operation clock overflow"))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn retains_submillisecond_carry_on_both_sides_of_epoch() {
        for seconds in [-1, 1] {
            let now = DateTime::from_timestamp(seconds, 999_500_000).unwrap();
            let clock = Clock::new(now);
            assert_eq!(
                clock.at_elapsed(Duration::from_micros(750)).unwrap(),
                now.timestamp_millis() + 1
            );
            assert_eq!(
                clock.at_elapsed(Duration::from_micros(250)).unwrap(),
                now.timestamp_millis()
            );
        }
    }

    #[test]
    fn preserves_backend_millisecond_range_without_clamping_logical_time() {
        let now = DateTime::<Utc>::MAX_UTC;
        let clock = Clock::new(now);
        assert_eq!(
            clock.at_elapsed(Duration::from_secs(1)).unwrap(),
            now.timestamp_millis() + 1000
        );
        assert!(matches!(
            clock.at_elapsed(Duration::MAX),
            Err(Error::Invalid(_))
        ));
    }
}
