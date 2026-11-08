use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::PolicyErrorCode;

/// In-process quota algorithms, not distributed admission promises.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuotaKind {
    /// Discrete refill of a bounded bucket every period.
    TokenBucket {
        /// Maximum accumulated tokens.
        capacity: u32,
        /// Tokens added per refill period.
        refill: u32,
        /// Refill period, measured from monotonic zero.
        period: Duration,
    },
    /// Aligned monotonic windows beginning at zero.
    FixedWindow {
        /// Maximum admissions per window.
        permits: u32,
        /// Window size, aligned to monotonic zero.
        window: Duration,
    },
    /// Exact sliding window with at most `permits` retained timestamps.
    SlidingWindow {
        /// Maximum admissions in any rolling window.
        permits: u32,
        /// Rolling window duration.
        window: Duration,
    },
    /// RAII permits released on success, error, or dropped future.
    Concurrency {
        /// Maximum simultaneously live permits.
        permits: u32,
    },
}
struct State {
    last: Duration,
    anchor: Duration,
    used: u32,
    tokens: u32,
    times: VecDeque<Duration>,
}

/// One shared, bounded quota state. Clones share admission atomically.
#[derive(Clone)]
pub struct Quota {
    kind: QuotaKind,
    state: Arc<Mutex<State>>,
}

/// Successful permit or a deterministic retry time (None for concurrency).
pub enum QuotaAdmission {
    /// Admission consumed one permit.
    Granted(QuotaPermit),
    /// No work should execute; a driver must retain/reschedule its delivery.
    Limited {
        /// Next known eligibility time, or None until another permit drops.
        retry_at: Option<Duration>,
    },
}

/// Concurrency admission is returned by Drop; time quotas remain consumed.
pub struct QuotaPermit {
    release: Option<Arc<Mutex<State>>>,
}
impl Drop for QuotaPermit {
    fn drop(&mut self) {
        if let Some(state) = &self.release {
            // Poisoning fails closed on future admissions rather than granting
            // capacity from state whose invariants may have been interrupted.
            if let Ok(mut state) = state.lock() {
                state.used = state.used.saturating_sub(1);
            }
        }
    }
}
impl Quota {
    /// Validates bounds. Every queue and token count is limited to one million.
    pub fn new(kind: QuotaKind) -> Result<Self, PolicyErrorCode> {
        let (capacity, duration) = match kind {
            QuotaKind::TokenBucket {
                capacity,
                refill,
                period,
            } => {
                if refill == 0 || refill > capacity {
                    return Err(PolicyErrorCode);
                }
                (capacity, Some(period))
            }
            QuotaKind::FixedWindow { permits, window }
            | QuotaKind::SlidingWindow { permits, window } => (permits, Some(window)),
            QuotaKind::Concurrency { permits } => (permits, None),
        };
        if capacity == 0 || capacity > 1_000_000 || duration.is_some_and(|d| d.is_zero()) {
            return Err(PolicyErrorCode);
        }
        Ok(Self {
            kind,
            state: Arc::new(Mutex::new(State {
                last: Duration::ZERO,
                anchor: Duration::ZERO,
                used: 0,
                tokens: capacity,
                times: VecDeque::new(),
            })),
        })
    }
    /// Rejects backwards clocks. No waiting queue is allocated on rejection.
    pub fn acquire(&self, now: Duration) -> Result<QuotaAdmission, PolicyErrorCode> {
        let mut state = self.state.lock().map_err(|_| PolicyErrorCode)?;
        if now < state.last {
            return Err(PolicyErrorCode);
        }
        state.last = now;
        let retry_at = match self.kind {
            QuotaKind::Concurrency { permits } => {
                if state.used == permits {
                    return Ok(QuotaAdmission::Limited { retry_at: None });
                }
                state.used += 1;
                return Ok(QuotaAdmission::Granted(QuotaPermit {
                    release: Some(self.state.clone()),
                }));
            }
            QuotaKind::TokenBucket {
                capacity,
                refill,
                period,
            } => {
                let periods = now.saturating_sub(state.anchor).as_nanos() / period.as_nanos();
                if periods > 0 {
                    state.tokens = (u128::from(state.tokens)
                        + periods.saturating_mul(u128::from(refill)))
                    .min(u128::from(capacity)) as u32;
                    let remainder = now.saturating_sub(state.anchor).as_nanos() % period.as_nanos();
                    state.anchor = now
                        .checked_sub(from_nanos(remainder)?)
                        .ok_or(PolicyErrorCode)?;
                }
                if state.tokens > 0 {
                    state.tokens -= 1;
                    return Ok(QuotaAdmission::Granted(QuotaPermit { release: None }));
                }
                Some(state.anchor.checked_add(period).ok_or(PolicyErrorCode)?)
            }
            QuotaKind::FixedWindow { permits, window } => {
                let anchor = now
                    .checked_sub(from_nanos(now.as_nanos() % window.as_nanos())?)
                    .ok_or(PolicyErrorCode)?;
                if anchor != state.anchor {
                    state.anchor = anchor;
                    state.used = 0;
                }
                if state.used < permits {
                    state.used += 1;
                    return Ok(QuotaAdmission::Granted(QuotaPermit { release: None }));
                }
                Some(anchor.checked_add(window).ok_or(PolicyErrorCode)?)
            }
            QuotaKind::SlidingWindow { permits, window } => {
                while state
                    .times
                    .front()
                    .is_some_and(|t| now.saturating_sub(*t) >= window)
                {
                    state.times.pop_front();
                }
                if state.times.len() < permits as usize {
                    state.times.push_back(now);
                    return Ok(QuotaAdmission::Granted(QuotaPermit { release: None }));
                }
                Some(
                    state
                        .times
                        .front()
                        .ok_or(PolicyErrorCode)?
                        .checked_add(window)
                        .ok_or(PolicyErrorCode)?,
                )
            }
        };
        Ok(QuotaAdmission::Limited { retry_at })
    }
}
fn from_nanos(nanos: u128) -> Result<Duration, PolicyErrorCode> {
    let seconds = u64::try_from(nanos / 1_000_000_000).map_err(|_| PolicyErrorCode)?;
    Ok(Duration::new(seconds, (nanos % 1_000_000_000) as u32))
}

/// Endpoint quotas override message quotas; each configured key owns bounded
/// state. No arbitrary incoming endpoint/header creates a new state entry.
#[derive(Default)]
pub struct ScopedQuotas {
    endpoints: BTreeMap<String, Quota>,
    messages: BTreeMap<String, Quota>,
}
impl ScopedQuotas {
    /// Adds a configured endpoint quota, refusing duplicates and >4096 keys.
    pub fn endpoint(&mut self, name: String, quota: Quota) -> Result<(), PolicyErrorCode> {
        insert(&mut self.endpoints, name, quota)
    }
    /// Adds a fallback contract quota shared across its configured callers.
    pub fn message(&mut self, name: String, quota: Quota) -> Result<(), PolicyErrorCode> {
        insert(&mut self.messages, name, quota)
    }
    /// Gets the existing scoped state without allocating or trusting headers.
    pub fn select(&self, endpoint: &str, contract: &str) -> Option<&Quota> {
        self.endpoints
            .get(endpoint)
            .or_else(|| self.messages.get(contract))
    }
}
fn insert(
    map: &mut BTreeMap<String, Quota>,
    name: String,
    quota: Quota,
) -> Result<(), PolicyErrorCode> {
    if name.is_empty() || name.len() > 256 || map.len() == 4096 || map.contains_key(&name) {
        return Err(PolicyErrorCode);
    }
    map.insert(name, quota);
    Ok(())
}
