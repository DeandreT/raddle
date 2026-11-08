use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::StatusCode;

use super::Problem;
use crate::Principal;

/// Immediate-rejection rate algorithms. There is no hidden unbounded wait queue.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum RateAlgorithm {
    /// Fixed window beginning on a partition's first request.
    Fixed {
        /// Requests per window.
        permits: u32,
        /// Window length.
        window: Duration,
    },
    /// Exact rolling timestamp window (at most `permits` timestamps retained).
    Sliding {
        /// Requests per rolling window.
        permits: u32,
        /// Rolling window length.
        window: Duration,
    },
    /// Continuously replenished bucket, initially full.
    TokenBucket {
        /// Maximum tokens, and initial burst.
        capacity: u32,
        /// Tokens replenished per period.
        refill: u32,
        /// Replenishment period.
        period: Duration,
    },
    /// Simultaneously executing requests per verified identity.
    Concurrency {
        /// Maximum in-flight requests; permit drops on completion/cancellation.
        permits: u32,
    },
}

/// Named bounded policy, shared by the endpoints explicitly referencing it.
#[derive(Clone, Debug)]
pub struct RatePolicy {
    pub(crate) name: String,
    pub(crate) limiter: RateLimiter,
}

impl RatePolicy {
    /// Creates a named policy; route `None` explicitly exempts that endpoint.
    pub fn new(name: &str, limiter: RateLimiter) -> Result<Self, Problem> {
        if name.is_empty() || name.len() > 128 {
            return Err(Problem::invalid("rate policy name"));
        }
        Ok(Self {
            name: name.into(),
            limiter,
        })
    }
}

/// Finite partition table keyed only by verified tenant/subject or one anonymous key.
#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<Mutex<State>>,
    algorithm: RateAlgorithm,
    identities: usize,
    idle: Duration,
}
impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("algorithm", &self.algorithm)
            .field("identities", &self.identities)
            .field("idle", &self.idle)
            .finish_non_exhaustive()
    }
}
#[derive(Debug, Default)]
struct State {
    partitions: HashMap<(String, String, String), Partition>,
}
#[derive(Debug)]
struct Partition {
    started: Instant,
    last: Instant,
    count: u32,
    times: VecDeque<Instant>,
    tokens: f64,
    active: u32,
}

pub(crate) struct RateGuard {
    limiter: RateLimiter,
    key: (String, String, String),
    concurrency: bool,
}

impl RateLimiter {
    /// Rejects invalid/unbounded configurations. Idle partitions are reclaimed;
    /// active concurrency permits are never evicted, avoiding admission bypass.
    pub fn new(
        algorithm: RateAlgorithm,
        identities: usize,
        idle: Duration,
    ) -> Result<Self, Problem> {
        let valid = match algorithm {
            RateAlgorithm::Fixed { permits, window }
            | RateAlgorithm::Sliding { permits, window } => {
                permits > 0 && permits <= 100_000 && !window.is_zero() && window <= idle
            }
            RateAlgorithm::TokenBucket {
                capacity,
                refill,
                period,
            } => {
                capacity > 0
                    && capacity <= 100_000
                    && refill > 0
                    && !period.is_zero()
                    && period <= idle
                    && capacity as f64 * period.as_secs_f64() / refill as f64 <= idle.as_secs_f64()
            }
            RateAlgorithm::Concurrency { permits } => permits > 0 && permits <= 100_000,
        };
        if !valid
            || identities == 0
            || identities > 100_000
            || idle.is_zero()
            || idle > Duration::from_secs(86400)
        {
            return Err(Problem::invalid("rate policy"));
        }
        // Sliding windows retain at most one million timestamps in total.
        if matches!(algorithm,RateAlgorithm::Sliding{permits,..} if identities.saturating_mul(permits as usize)>1_000_000)
        {
            return Err(Problem::invalid("rate memory bound"));
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(State::default())),
            algorithm,
            identities,
            idle,
        })
    }

    pub(crate) fn admit(
        &self,
        name: &str,
        principal: Option<&Principal>,
    ) -> Result<RateGuard, Problem> {
        self.admit_at(name, principal, Instant::now())
    }

    fn admit_at(
        &self,
        name: &str,
        principal: Option<&Principal>,
        now: Instant,
    ) -> Result<RateGuard, Problem> {
        let key = match principal {
            Some(principal) => (
                name.into(),
                principal.tenant().into(),
                principal.subject().into(),
            ),
            None => (name.into(), String::new(), String::new()),
        };
        let mut state = self.inner.lock().map_err(|_| Problem::internal())?;
        state.partitions.retain(|_, partition| {
            partition.active > 0 || now.duration_since(partition.last) < self.idle
        });
        if !state.partitions.contains_key(&key) && state.partitions.len() >= self.identities {
            return Err(Problem::new(StatusCode::TOO_MANY_REQUESTS));
        }
        let capacity = match self.algorithm {
            RateAlgorithm::TokenBucket { capacity, .. } => capacity as f64,
            _ => 0.0,
        };
        let partition = state
            .partitions
            .entry(key.clone())
            .or_insert_with(|| Partition {
                started: now,
                last: now,
                count: 0,
                times: VecDeque::new(),
                tokens: capacity,
                active: 0,
            });
        let concurrency = matches!(self.algorithm, RateAlgorithm::Concurrency { .. });
        let allowed = match self.algorithm {
            RateAlgorithm::Fixed { permits, window } => {
                if now.duration_since(partition.started) >= window {
                    partition.started = now;
                    partition.count = 0;
                }
                if partition.count < permits {
                    partition.count += 1;
                    true
                } else {
                    false
                }
            }
            RateAlgorithm::Sliding { permits, window } => {
                while partition
                    .times
                    .front()
                    .is_some_and(|time| now.duration_since(*time) >= window)
                {
                    partition.times.pop_front();
                }
                if partition.times.len() < permits as usize {
                    partition.times.push_back(now);
                    true
                } else {
                    false
                }
            }
            RateAlgorithm::TokenBucket {
                capacity,
                refill,
                period,
            } => {
                partition.tokens = (partition.tokens
                    + now.duration_since(partition.last).as_secs_f64() * refill as f64
                        / period.as_secs_f64())
                .min(capacity as f64);
                if partition.tokens >= 1.0 {
                    partition.tokens -= 1.0;
                    true
                } else {
                    false
                }
            }
            RateAlgorithm::Concurrency { permits } => {
                if partition.active < permits {
                    partition.active += 1;
                    true
                } else {
                    false
                }
            }
        };
        partition.last = now;
        if !allowed {
            return Err(Problem::new(StatusCode::TOO_MANY_REQUESTS));
        }
        Ok(RateGuard {
            limiter: self.clone(),
            key,
            concurrency,
        })
    }
}

impl Drop for RateGuard {
    fn drop(&mut self) {
        if self.concurrency
            && let Ok(mut state) = self.limiter.inner.lock()
            && let Some(partition) = state.partitions.get_mut(&self.key)
        {
            partition.active = partition.active.saturating_sub(1);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::{RateAlgorithm, RateLimiter};
    use std::time::{Duration, Instant};

    #[test]
    fn exact_fixed_sliding_and_token_reset_boundaries_use_monotonic_time() {
        for algorithm in [
            RateAlgorithm::Fixed {
                permits: 1,
                window: Duration::from_secs(1),
            },
            RateAlgorithm::Sliding {
                permits: 1,
                window: Duration::from_secs(1),
            },
            RateAlgorithm::TokenBucket {
                capacity: 1,
                refill: 1,
                period: Duration::from_secs(1),
            },
        ] {
            let limiter = RateLimiter::new(algorithm, 1, Duration::from_secs(10)).unwrap();
            let now = Instant::now();
            assert!(limiter.admit_at("route", None, now).is_ok());
            assert!(
                limiter
                    .admit_at("route", None, now + Duration::from_millis(999))
                    .is_err()
            );
            assert!(
                limiter
                    .admit_at("route", None, now + Duration::from_secs(1))
                    .is_ok()
            );
        }
    }

    #[test]
    fn bucket_idle_reclamation_cannot_refill_faster_than_the_real_bucket() {
        assert!(
            RateLimiter::new(
                RateAlgorithm::TokenBucket {
                    capacity: 100,
                    refill: 1,
                    period: Duration::from_secs(1)
                },
                1,
                Duration::from_secs(1)
            )
            .is_err()
        );
    }
}
