use std::collections::HashSet;

use axum::http::HeaderMap;
use raddle::Request;

use crate::ApiError;

/// Operations available to an authenticated caller.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum Permission {
    /// Invoke commands and submit durable work.
    Invoke,
    /// Read operational status.
    Inspect,
    /// Request audited replay.
    Replay,
}

/// Identity produced by a trusted authenticator, never deserialized from a body.
#[derive(Clone, Debug)]
pub struct Principal {
    tenant: String,
    subject: String,
    permissions: HashSet<Permission>,
}

impl Principal {
    /// Validates bounded identity fields and stores explicitly granted permissions.
    pub fn try_new(
        tenant: impl Into<String>,
        subject: impl Into<String>,
        permissions: impl IntoIterator<Item = Permission>,
    ) -> Result<Self, ApiError> {
        let tenant = tenant.into();
        let subject = subject.into();
        if tenant.is_empty()
            || tenant.len() > 128
            || subject.is_empty()
            || subject.len() > 128
            || tenant.chars().any(char::is_control)
            || subject.chars().any(char::is_control)
        {
            return Err(ApiError::unauthorized());
        }
        Ok(Self {
            tenant,
            subject,
            permissions: permissions.into_iter().collect(),
        })
    }
    /// Authenticated tenant used for storage scoping.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    /// Authenticated subject used for replay attribution.
    pub fn subject(&self) -> &str {
        &self.subject
    }
    /// Rejects operations not explicitly granted to this identity.
    pub fn require(&self, permission: Permission) -> Result<(), ApiError> {
        if self.permissions.contains(&permission) {
            Ok(())
        } else {
            Err(ApiError::forbidden())
        }
    }
}

/// Pluggable bearer verification. The application verifies signatures, expiry, and issuer.
pub trait Authenticator: Send + Sync + 'static {
    /// Validates the bearer credential and returns its trusted identity.
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, ApiError>;
}

impl<F> Authenticator for F
where
    F: Fn(&HeaderMap) -> Result<Principal, ApiError> + Send + Sync + 'static,
{
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, ApiError> {
        self(headers)
    }
}

/// A typed request carrying authenticated identity through the ordinary runtime.
pub struct Authenticated<M> {
    principal: Principal,
    message: M,
}

impl<M> Authenticated<M> {
    pub(crate) fn new(principal: Principal, message: M) -> Self {
        Self { principal, message }
    }
    /// Borrows the identity for resource-level authorization inside the handler.
    pub fn principal(&self) -> &Principal {
        &self.principal
    }
    /// Borrows the typed input.
    pub fn message(&self) -> &M {
        &self.message
    }
    /// Moves identity and input into application code.
    pub fn into_parts(self) -> (Principal, M) {
        (self.principal, self.message)
    }
}

impl<M: Request> Request for Authenticated<M> {
    type Reply = M::Reply;
    type Error = M::Error;
}
