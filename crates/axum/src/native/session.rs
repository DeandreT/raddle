use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::{HeaderMap, StatusCode, header};
use uuid::Uuid;

use super::Problem;
use crate::{ApiError, Authenticator, Principal};

/// Opt-in antiforgery enforcement; disabled is the native and pinned default.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub enum CsrfPolicy {
    /// No antiforgery check, including form endpoints unless explicitly enabled.
    #[default]
    Disabled,
    /// Require the session-bound token in `X-CSRF-Token`, without reading the body.
    Session(CookieSessions),
}

/// Returned to the application at login; the token is not placed in the cookie.
pub struct CookieSession {
    /// HttpOnly, SameSite=Lax cookie header to set on the response.
    pub set_cookie: String,
    /// Secret synchronizer token to deliver through the application's safe login UI.
    pub csrf_token: String,
}

impl std::fmt::Debug for CookieSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CookieSession([REDACTED])")
    }
}

/// Bounded in-process opaque sessions, with random independent session/CSRF tokens.
/// This is not an identity issuer; applications authenticate users before issuing.
#[derive(Clone)]
pub struct CookieSessions {
    inner: Arc<Mutex<HashMap<String, Session>>>,
    capacity: usize,
    secure: bool,
}
struct Session {
    principal: Principal,
    csrf: String,
    expires: Instant,
}

impl std::fmt::Debug for CookieSessions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CookieSessions")
            .field("capacity", &self.capacity)
            .field("secure", &self.secure)
            .finish_non_exhaustive()
    }
}

impl CookieSessions {
    /// Secure cookies are required in deployment; `false` is for loopback HTTP tests.
    pub fn new(capacity: usize, secure: bool) -> Result<Self, Problem> {
        if capacity == 0 || capacity > 100_000 {
            return Err(Problem::invalid("session capacity"));
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            capacity,
            secure,
        })
    }

    /// Issues a session for an already verified principal, expiring within 24 hours.
    pub fn issue(&self, principal: Principal, ttl: Duration) -> Result<CookieSession, Problem> {
        if ttl.as_secs() == 0 || ttl > Duration::from_secs(86400) {
            return Err(Problem::invalid("session ttl"));
        }
        let mut sessions = self.inner.lock().map_err(|_| Problem::internal())?;
        sessions.retain(|_, session| session.expires > Instant::now());
        if sessions.len() >= self.capacity {
            return Err(Problem::new(StatusCode::SERVICE_UNAVAILABLE));
        }
        let id = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let csrf = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        sessions.insert(
            id.clone(),
            Session {
                principal,
                csrf: csrf.clone(),
                expires: Instant::now() + ttl,
            },
        );
        Ok(CookieSession {
            set_cookie: format!(
                "raddle-session={id}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}{}",
                ttl.as_secs(),
                if self.secure { "; Secure" } else { "" }
            ),
            csrf_token: csrf,
        })
    }

    /// Revokes the exact session cookie; output-cache authorization rechecks it.
    pub fn revoke(&self, headers: &HeaderMap) -> Result<bool, Problem> {
        let Some(id) = cookie(headers) else {
            return Ok(false);
        };
        Ok(self
            .inner
            .lock()
            .map_err(|_| Problem::internal())?
            .remove(id)
            .is_some())
    }

    pub(crate) fn check_csrf(
        &self,
        headers: &HeaderMap,
        principal: Option<&Principal>,
    ) -> Result<(), Problem> {
        let id = cookie(headers).ok_or_else(|| Problem::new(StatusCode::BAD_REQUEST))?;
        let sessions = self.inner.lock().map_err(|_| Problem::internal())?;
        let session = sessions
            .get(id)
            .filter(|session| session.expires > Instant::now())
            .ok_or_else(|| Problem::new(StatusCode::BAD_REQUEST))?;
        let token = headers
            .get("x-csrf-token")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| Problem::new(StatusCode::BAD_REQUEST))?;
        let same_principal = principal.is_some_and(|principal| {
            principal.tenant() == session.principal.tenant()
                && principal.subject() == session.principal.subject()
        });
        // Fixed token length, constant-time equality across all bytes.
        let equal = token.len() == session.csrf.len()
            && token
                .bytes()
                .zip(session.csrf.bytes())
                .fold(0u8, |diff, (a, b)| diff | (a ^ b))
                == 0;
        if !same_principal || !equal {
            return Err(Problem::new(StatusCode::BAD_REQUEST));
        }
        Ok(())
    }
}

impl Authenticator for CookieSessions {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, ApiError> {
        let id = cookie(headers).ok_or_else(ApiError::unauthorized)?;
        let sessions = self.inner.lock().map_err(|_| ApiError::unavailable())?;
        sessions
            .get(id)
            .filter(|session| session.expires > Instant::now())
            .map(|session| session.principal.clone())
            .ok_or_else(ApiError::unauthorized)
    }
}

fn cookie(headers: &HeaderMap) -> Option<&str> {
    let mut found = None;
    for header in headers.get_all(header::COOKIE) {
        for value in header.to_str().ok()?.split(';') {
            if let Some(value) = value.trim().strip_prefix("raddle-session=") {
                if found.is_some() || value.len() != 64 {
                    return None;
                }
                found = Some(value);
            }
        }
    }
    found
}
