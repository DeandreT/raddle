//! Shared checks for independently implemented durable custody backends.

use std::future::Future;

use raddle::wire::Envelope;
use uuid::Uuid;

/// Observable durable staging outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CustodyResult {
    /// Stable backend job identity returned after commit.
    Accepted(Uuid),
    /// Reusing an identity with different content was refused.
    Conflict,
}

/// Thin adapter implemented by each backend's isolated test fixture.
pub trait CustodyProbe: Sync {
    /// Backend-specific error propagated without replacing it with a success.
    type Error;
    /// Stages and commits under a stable source, destination, and schedule.
    fn stage(
        &self,
        tenant: &str,
        envelope: &Envelope,
    ) -> impl Future<Output = Result<CustodyResult, Self::Error>> + Send;
}

/// A backend failure or a violated common custody invariant.
#[derive(Debug, thiserror::Error)]
pub enum ConformanceError<E> {
    /// Original backend error.
    #[error("backend conformance operation failed: {0}")]
    Backend(E),
    /// A named invariant was violated.
    #[error("custody invariant failed: {0}")]
    Invariant(&'static str),
}

/// Verifies stable deduplication, tenant separation, and immutable-content conflicts.
/// The envelopes must use the same identity but different content.
pub async fn verify_custody<P: CustodyProbe>(
    probe: &P,
    original: &Envelope,
    changed: &Envelope,
) -> Result<(), ConformanceError<P::Error>> {
    if original.id() != changed.id() || original.to_vec().ok() == changed.to_vec().ok() {
        return Err(ConformanceError::Invariant(
            "fixture must change content, not identity",
        ));
    }
    let first = probe
        .stage("conformance-a", original)
        .await
        .map_err(ConformanceError::Backend)?;
    let repeated = probe
        .stage("conformance-a", original)
        .await
        .map_err(ConformanceError::Backend)?;
    let other_tenant = probe
        .stage("conformance-b", original)
        .await
        .map_err(ConformanceError::Backend)?;
    let conflict = probe
        .stage("conformance-a", changed)
        .await
        .map_err(ConformanceError::Backend)?;
    if !matches!(first, CustodyResult::Accepted(_)) || repeated != first {
        return Err(ConformanceError::Invariant(
            "duplicate custody changes identity",
        ));
    }
    if !matches!(other_tenant, CustodyResult::Accepted(_)) || other_tenant == first {
        return Err(ConformanceError::Invariant(
            "tenant custody is not independent",
        ));
    }
    if conflict != CustodyResult::Conflict {
        return Err(ConformanceError::Invariant(
            "conflicting immutable content was accepted",
        ));
    }
    Ok(())
}
