use std::collections::HashMap;
use std::time::{Duration, Instant};

use axum::http::StatusCode;

use crate::{ApiError, ErrorCode, Principal};

/// Finite fixed-window ingress limits per authenticated tenant and subject.
/// Authentication runs on every request; caller-supplied identity headers are ignored.
#[derive(Clone, Copy, Debug)]
pub struct RequestLimits {
    requests: u32,
    window: Duration,
    identities: usize,
}

impl RequestLimits {
    /// Validates a positive request budget, 1ms–1h window, and 1–4096 tracked identities.
    /// A full identity table refuses new identities until old windows expire.
    pub fn try_new(requests: u32, window: Duration, identities: usize) -> Result<Self, ApiError> {
        if requests == 0
            || window < Duration::from_millis(1)
            || window > Duration::from_secs(3600)
            || !(1..=4096).contains(&identities)
        {
            return Err(ApiError::invalid());
        }
        Ok(Self {
            requests,
            window,
            identities,
        })
    }
}

pub(crate) struct Limiter {
    settings: RequestLimits,
    identities: HashMap<(String, String), (Instant, u32)>,
}

impl Limiter {
    pub(crate) fn new(settings: RequestLimits) -> Self {
        Self {
            settings,
            identities: HashMap::new(),
        }
    }

    pub(crate) fn admit(&mut self, principal: &Principal) -> Result<(), ApiError> {
        self.admit_at(principal, Instant::now())
    }

    fn admit_at(&mut self, principal: &Principal, now: Instant) -> Result<(), ApiError> {
        self.identities
            .retain(|_, (start, _)| now.duration_since(*start) < self.settings.window);
        let key = (
            principal.tenant().to_owned(),
            principal.subject().to_owned(),
        );
        if !self.identities.contains_key(&key) && self.identities.len() >= self.settings.identities
        {
            return Err(rate_limited());
        }
        let (_, count) = self.identities.entry(key).or_insert((now, 0));
        if *count >= self.settings.requests {
            return Err(rate_limited());
        }
        *count += 1;
        Ok(())
    }
}

fn rate_limited() -> ApiError {
    ApiError::new(
        StatusCode::TOO_MANY_REQUESTS,
        ErrorCode::RateLimited,
        "request rate limit exceeded",
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::{Limiter, RequestLimits};
    use crate::{Permission, Principal};
    use std::time::{Duration, Instant};

    #[test]
    fn limits_are_identity_scoped_bounded_and_expire_without_evicting_live_budgets() {
        let mut limiter =
            Limiter::new(RequestLimits::try_new(2, Duration::from_secs(1), 2).unwrap());
        let one = Principal::try_new("tenant", "one", [Permission::Invoke]).unwrap();
        let two = Principal::try_new("tenant", "two", [Permission::Invoke]).unwrap();
        let other = Principal::try_new("other", "one", [Permission::Invoke]).unwrap();
        let now = Instant::now();
        assert!(limiter.admit_at(&one, now).is_ok());
        assert!(limiter.admit_at(&one, now).is_ok());
        assert!(limiter.admit_at(&one, now).is_err());
        assert!(limiter.admit_at(&two, now).is_ok());
        assert!(limiter.admit_at(&other, now).is_err());
        assert_eq!(limiter.identities.len(), 2);
        assert!(
            limiter
                .admit_at(&other, now + Duration::from_secs(1))
                .is_ok()
        );
        assert_eq!(limiter.identities.len(), 1);
    }
}
