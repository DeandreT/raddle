use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes, HttpBody};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use tokio::sync::Mutex as AsyncMutex;

use super::{Problem, RequestInput};
use crate::Principal;

/// Default cache authorization eligibility is intentionally conservative.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub enum CacheEligibility {
    /// Anonymous requests without an Authorization header only.
    #[default]
    Anonymous,
    /// Explicit opt-in; always partition by verified tenant and subject.
    AuthenticatedPartition,
}

/// Named endpoint output-cache policy, applied after every resource authorization.
#[derive(Clone, Debug)]
pub struct OutputCachePolicy {
    /// Bounded store shared by named policies.
    pub store: OutputCache,
    /// Expiration from successful insertion, not sliding access.
    pub ttl: Duration,
    /// Query keys which vary the representation; omitted keys are ignored.
    pub vary_query: Vec<String>,
    /// Case-insensitive header names which vary the representation.
    pub vary_headers: Vec<String>,
    /// Invalidation tags attached to each stored representation.
    pub tags: Vec<String>,
    /// Explicit authentication eligibility.
    pub eligibility: CacheEligibility,
}

impl OutputCachePolicy {
    pub(crate) fn validate(&self) -> Result<(), Problem> {
        if self.ttl.is_zero()
            || self.ttl > Duration::from_secs(86400)
            || self.tags.len() > 32
            || self.vary_headers.len() + self.vary_query.len() > 64
            || self
                .tags
                .iter()
                .chain(&self.vary_query)
                .chain(&self.vary_headers)
                .any(|name| name.is_empty() || name.len() > 128)
            || self
                .vary_headers
                .iter()
                .any(|name| axum::http::HeaderName::try_from(name).is_err())
        {
            return Err(Problem::invalid("output cache policy"));
        }
        Ok(())
    }

    pub(crate) fn key(
        &self,
        name: &str,
        input: &RequestInput,
        principal: Option<&Principal>,
    ) -> Option<String> {
        match self.eligibility {
            CacheEligibility::Anonymous
                if principal.is_some()
                    || input
                        .parameters
                        .headers()
                        .contains_key(header::AUTHORIZATION) =>
            {
                return None;
            }
            CacheEligibility::AuthenticatedPartition if principal.is_none() => return None,
            _ => {}
        }
        // Preserve nested boundaries. Sentinel-delimited flat arrays can collide
        // when repeated query/header values contain the sentinel or the next key.
        let identity = principal.map(|principal| (principal.tenant(), principal.subject()));
        let query: Vec<_> = self
            .vary_query
            .iter()
            .map(|name| (name, input.parameters.query.get(name)))
            .collect();
        let headers: Vec<_> = self
            .vary_headers
            .iter()
            .map(|name| {
                (
                    name,
                    input
                        .parameters
                        .headers()
                        .get_all(name)
                        .iter()
                        .map(|value| value.as_bytes().to_vec())
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        serde_json::to_string(&(name, input.uri.path(), identity, query, headers)).ok()
    }
}

/// Finite in-memory output cache with per-key singleflight, TTL and tag eviction.
/// At most `entries * body_bytes` response bytes are retained (maximum 64 MiB).
#[derive(Clone)]
pub struct OutputCache {
    inner: Arc<Mutex<CacheState>>,
    entries: usize,
    body_bytes: usize,
}
impl std::fmt::Debug for OutputCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutputCache")
            .field("entries", &self.entries)
            .field("body_bytes", &self.body_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct CacheState {
    slots: HashMap<String, Arc<Slot>>,
}

#[derive(Debug)]
pub(crate) struct Slot {
    pub value: AsyncMutex<Option<Cached>>,
    expires: Mutex<Instant>,
    tags: Vec<String>,
    valid: AtomicBool,
}

#[derive(Clone, Debug)]
pub(crate) struct Cached {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl OutputCache {
    /// Creates a finite cache; saturation bypasses storage, never authorization.
    pub fn new(entries: usize, body_bytes: usize) -> Result<Self, Problem> {
        if entries == 0
            || entries > 4096
            || body_bytes == 0
            || entries
                .checked_mul(body_bytes)
                .is_none_or(|bytes| bytes > 64 * 1024 * 1024)
        {
            return Err(Problem::invalid("output cache capacity"));
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(CacheState::default())),
            entries,
            body_bytes,
        })
    }

    /// Invalidates matching entries and in-flight fills. Unrelated tags stay valid.
    pub fn evict_tag(&self, tag: &str) -> Result<usize, Problem> {
        let mut state = self.inner.lock().map_err(|_| Problem::internal())?;
        let mut count = 0;
        state.slots.retain(|_, slot| {
            if slot.tags.iter().any(|value| value == tag) {
                slot.valid.store(false, Ordering::Release);
                count += 1;
                false
            } else {
                true
            }
        });
        Ok(count)
    }

    pub(crate) fn slot(
        &self,
        key: String,
        policy: &OutputCachePolicy,
    ) -> Result<Option<Arc<Slot>>, Problem> {
        // Keys are bounded too; a hostile large URI cannot fill cache metadata.
        if key.len() > 8192 {
            return Ok(None);
        }
        let mut state = self.inner.lock().map_err(|_| Problem::internal())?;
        let now = Instant::now();
        state.slots.retain(|_, slot| {
            if slot.expired(now) && Arc::strong_count(slot) == 1 {
                slot.valid.store(false, Ordering::Release);
                false
            } else {
                true
            }
        });
        if let Some(slot) = state.slots.get(&key) {
            return Ok(Some(Arc::clone(slot)));
        }
        if state.slots.len() >= self.entries {
            return Ok(None);
        }
        let slot = Arc::new(Slot {
            value: AsyncMutex::new(None),
            expires: Mutex::new(now + policy.ttl),
            tags: policy.tags.clone(),
            valid: AtomicBool::new(true),
        });
        state.slots.insert(key, Arc::clone(&slot));
        Ok(Some(slot))
    }

    pub(crate) async fn capture(
        &self,
        response: Response,
    ) -> Result<(Response, Option<Cached>), Problem> {
        // Streaming/oversized bodies bypass storage without changing live output.
        if response
            .body()
            .size_hint()
            .exact()
            .is_none_or(|bytes| bytes > self.body_bytes as u64)
            || response
                .headers()
                .iter()
                .map(|(name, value)| name.as_str().len() + value.as_bytes().len())
                .sum::<usize>()
                > 16_384
        {
            return Ok((response, None));
        }
        if response.status() != StatusCode::OK
            || response.headers().contains_key(header::SET_COOKIE)
            || response
                .headers()
                .get_all(header::CACHE_CONTROL)
                .iter()
                .any(|value| {
                    // Directive names are tokens, not case-sensitive substrings.
                    // Unreadable controls conservatively bypass storage as well.
                    value.to_str().map_or(true, |value| {
                        value.split(',').any(|directive| {
                            directive
                                .split('=')
                                .next()
                                .is_some_and(|name| name.trim().eq_ignore_ascii_case("no-store"))
                        })
                    })
                })
        {
            return Ok((response, None));
        }
        let (parts, body) = response.into_parts();
        let bytes = axum::body::to_bytes(body, self.body_bytes)
            .await
            .map_err(|_| Problem::internal())?;
        let cached = Cached {
            status: parts.status,
            headers: parts.headers.clone(),
            body: bytes.clone(),
        };
        Ok((Response::from_parts(parts, Body::from(bytes)), Some(cached)))
    }
}

impl Slot {
    pub(crate) fn valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }
    fn expired(&self, now: Instant) -> bool {
        self.expires.lock().map_or(true, |expires| *expires <= now)
    }
    pub(crate) fn fresh(&self) -> bool {
        self.valid() && !self.expired(Instant::now())
    }
    pub(crate) fn filled(&self, ttl: Duration) -> Result<(), Problem> {
        *self.expires.lock().map_err(|_| Problem::internal())? = Instant::now() + ttl;
        Ok(())
    }
}
impl Cached {
    pub(crate) fn response(&self) -> Response {
        let mut response = Response::new(Body::from(self.body.clone()));
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers.clone();
        response
    }
}

/// Response-cache headers only; this does not store an output representation.
#[derive(Clone, Debug)]
pub struct ResponseCache {
    /// `None` emits no-store; `Some` emits a max-age.
    pub max_age: Option<Duration>,
    /// Public caching is explicit; otherwise private.
    pub public: bool,
    /// Vary header names.
    pub vary: Vec<String>,
}

impl ResponseCache {
    pub(crate) fn apply(&self, response: &mut Response) -> Result<(), Problem> {
        let value = match self.max_age {
            None => "no-store".into(),
            Some(age) => format!(
                "{}, max-age={}",
                if self.public { "public" } else { "private" },
                age.as_secs()
            ),
        };
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::try_from(value).map_err(|_| Problem::internal())?,
        );
        if !self.vary.is_empty() {
            response.headers_mut().insert(
                header::VARY,
                HeaderValue::try_from(self.vary.join(", ")).map_err(|_| Problem::internal())?,
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::{CacheEligibility, OutputCache, OutputCachePolicy};
    use crate::native::{EndpointContract, HttpMethod, RequestInput};
    use axum::body::Body;
    use std::collections::BTreeMap;
    use std::time::Duration;

    #[test]
    fn repeated_vary_values_cannot_shift_between_named_query_components() {
        let policy = OutputCachePolicy {
            store: OutputCache::new(2, 1024).unwrap(),
            ttl: Duration::from_secs(1),
            vary_query: vec!["q1".into(), "q2".into()],
            vary_headers: vec![],
            tags: vec![],
            eligibility: CacheEligibility::Anonymous,
        };
        let contract = EndpointContract::new(HttpMethod::Get, "/", "test");
        let a = RequestInput::new(
            axum::extract::Request::builder()
                .uri("/?q2=&q2=q2")
                .body(Body::empty())
                .unwrap(),
            BTreeMap::new(),
            &contract,
        );
        let b = RequestInput::new(
            axum::extract::Request::builder()
                .uri("/?q1=&q1=q2")
                .body(Body::empty())
                .unwrap(),
            BTreeMap::new(),
            &contract,
        );
        assert_ne!(
            policy.key("route@neutral", &a, None),
            policy.key("route@neutral", &b, None)
        );
    }
}
