use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;

use raddle::SubscriptionName;

use crate::ConfigurationError;

/// Validated identity returned only by the application's credential verifier.
/// Permission aliases are explicit allowlist entries, never wildcards.
#[derive(Clone, Debug)]
pub struct Principal {
    tenant: String,
    subject: String,
    permissions: BTreeSet<String>,
}

impl Principal {
    /// Checks 1–128-byte printable ASCII tenant/subject identifiers and 0–128
    /// distinct permission aliases using stable wire-name syntax.
    pub fn new(
        tenant: &str,
        subject: &str,
        permissions: &[&str],
    ) -> Result<Self, ConfigurationError> {
        if !identity(tenant) || !identity(subject) || permissions.len() > 128 {
            return Err(ConfigurationError::InvalidPrincipal);
        }
        let mut allowed = BTreeSet::new();
        for permission in permissions {
            if permission.len() > 128
                || SubscriptionName::try_new(*permission).is_err()
                || !allowed.insert((*permission).to_owned())
            {
                return Err(ConfigurationError::InvalidPrincipal);
            }
        }
        Ok(Self {
            tenant: tenant.into(),
            subject: subject.into(),
            permissions: allowed,
        })
    }

    /// Trusted tenant determined by credential validation, not caller metadata.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    /// Trusted subject determined by credential validation.
    pub fn subject(&self) -> &str {
        &self.subject
    }
    /// Whether the verifier granted this exact registered permission alias.
    pub fn allows(&self, permission: &str) -> bool {
        self.permissions.contains(permission)
    }
}

fn identity(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|byte| byte.is_ascii_graphic())
}

/// Credential rejection or verifier unavailability, without secret error details.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum AuthenticationError {
    /// The bearer credential was not verified.
    #[error("credential rejected")]
    Rejected,
    /// The application's credential verifier is unavailable.
    #[error("credential verifier unavailable")]
    Unavailable,
}

/// Caller-polled asynchronous credential validation; no detached task is created.
pub type AuthenticationFuture<'authenticator> =
    Pin<Box<dyn Future<Output = Result<Principal, AuthenticationError>> + Send + 'authenticator>>;

/// Application-supplied bearer validation. Metadata alone never becomes identity.
/// Async closures taking an owned token implement this trait automatically.
pub trait Authenticator: Send + Sync + 'static {
    /// Verifies the token and returns trusted tenant, subject and permissions.
    fn authenticate(&self, bearer: String) -> AuthenticationFuture<'_>;
}

impl<F, Fut> Authenticator for F
where
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Principal, AuthenticationError>> + Send + 'static,
{
    fn authenticate(&self, bearer: String) -> AuthenticationFuture<'_> {
        Box::pin(self(bearer))
    }
}
