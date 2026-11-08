//! Persisted HTTP response claims, distinct from durable-message acceptance.
//!
//! `claim` and `complete` use exactly the supplied scope's connection. Separate
//! claim-and-record mode is explicit at the adapter. Expired rows remain honored
//! until bounded cleanup; a committed unanswered claim must not rerun work.

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use crate::{Error, Scope, Store};

/// A bounded identity derived from explicit nonzero tenant/user/endpoint scopes.
#[derive(Clone)]
pub struct ResponseKey {
    scope: [u8; 32],
    key: String,
}
impl std::fmt::Debug for ResponseKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseKey")
            .field("key_bytes", &self.key.len())
            .finish_non_exhaustive()
    }
}

impl ResponseKey {
    /// Hashes unambiguous length-framed trusted scope values. `None` is not an
    /// empty identity; at least one scope must be selected and all selected values
    /// must be nonempty. Endpoint values are registration names, not path hints.
    pub fn new(
        tenant: Option<&str>,
        user: Option<&str>,
        endpoint: Option<&str>,
        key: &str,
    ) -> Result<Self, Error> {
        if [tenant, user, endpoint].iter().all(Option::is_none)
            || [tenant, user, endpoint]
                .into_iter()
                .flatten()
                .any(|value| value.is_empty() || value.len() > 128)
            || key.is_empty()
            || key.len() > 128
            || !key.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(Error::Invalid("invalid HTTP response identity"));
        }
        let mut hash = Sha256::new();
        for value in [tenant, user, endpoint] {
            match value {
                None => hash.update([0]),
                Some(value) => {
                    hash.update([1]);
                    hash.update((value.len() as u64).to_be_bytes());
                    hash.update(value.as_bytes());
                }
            }
        }
        Ok(Self {
            scope: hash.finalize().into(),
            key: key.into(),
        })
    }
}

/// Exact replay subset: no arbitrary original headers are persisted or replayed.
#[derive(Clone)]
pub struct StoredResponse {
    /// Original successful status.
    pub status: u16,
    /// Original serialized body, limited to 1 MiB.
    pub body: Vec<u8>,
    /// Original Location only.
    pub location: Option<String>,
}
impl StoredResponse {
    /// Validates the complete persisted subset before business work is committed.
    /// Persistence repeats this check so direct callers cannot bypass its bounds.
    pub fn validate(&self) -> Result<(), Error> {
        if !(200..400).contains(&self.status)
            || self.body.len() > 1_048_576
            || self
                .location
                .as_ref()
                .is_some_and(|value| value.len() > 8192)
        {
            return Err(Error::Invalid("invalid stored HTTP response"));
        }
        Ok(())
    }
}
impl std::fmt::Debug for StoredResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredResponse")
            .field("status", &self.status)
            .field("body_bytes", &self.body.len())
            .field("has_location", &self.location.is_some())
            .finish_non_exhaustive()
    }
}

/// Ownership fence of one unanswered claim; cannot be fabricated outside the store.
#[derive(Clone, Debug)]
pub struct ResponseClaim {
    key: ResponseKey,
    owner: Uuid,
}

/// Claim disposition; duplicates never imply that business work should execute.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ClaimResult {
    /// Caller may execute within the supplied transaction.
    Claimed(ResponseClaim),
    /// Completed matching call, safe to replay.
    Replay(StoredResponse),
    /// Matching unanswered call or concurrent transaction (409).
    InFlight,
    /// Same scope/key with different raw request bytes (422).
    Mismatch,
}

struct Operation<'a, 'connection> {
    scope: &'a mut Scope<'connection>,
    success: bool,
}
impl Drop for Operation<'_, '_> {
    fn drop(&mut self) {
        if !self.success {
            self.scope.mark_rollback_only();
        }
    }
}

/// Claims in exactly this transaction. A conflicting active transaction returns
/// InFlight immediately; the native adapter serializes before executing duplicate
/// work rather than promising exactly-once external effects.
pub async fn claim(
    scope: &mut Scope<'_>,
    key: &ResponseKey,
    fingerprint: &[u8; 32],
    expires: DateTime<Utc>,
) -> Result<ClaimResult, Error> {
    let mut operation = Operation {
        scope,
        success: false,
    };
    let result=async {
        let mut hash=Sha256::new();hash.update(key.scope);hash.update(key.key.as_bytes());
        let lock:[u8;32]=hash.finalize().into();
        let mut lock_key=[0u8;8];lock_key.copy_from_slice(&lock[..8]);
        let locked:bool=sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)").bind(i64::from_be_bytes(lock_key)).fetch_one(operation.scope.connection()).await?;
        if !locked {return Ok(ClaimResult::InFlight);}
        let row=sqlx::query("SELECT request_hash,status,body,location FROM raddle_http_responses WHERE scope_hash=$1 AND key=$2").bind(key.scope.as_slice()).bind(&key.key).fetch_optional(operation.scope.connection()).await?;
        if let Some(row)=row {
            if row.try_get::<Vec<u8>,_>("request_hash")?.as_slice() != fingerprint.as_slice() {return Ok(ClaimResult::Mismatch);}
            return Ok(match row.try_get::<Option<i16>,_>("status")? {None=>ClaimResult::InFlight,Some(status)=>ClaimResult::Replay(StoredResponse {status:status as u16,body:row.try_get::<Option<Vec<u8>>,_>("body")?.unwrap_or_default(),location:row.try_get("location")?})});
        }
        let owner=Uuid::new_v4();
        sqlx::query("INSERT INTO raddle_http_responses(scope_hash,key,request_hash,owner,expires_at) VALUES($1,$2,$3,$4,$5)").bind(key.scope.as_slice()).bind(&key.key).bind(fingerprint.as_slice()).bind(owner).bind(expires).execute(operation.scope.connection()).await?;
        Ok(ClaimResult::Claimed(ResponseClaim {key:key.clone(),owner}))
    }.await;
    operation.success = result.is_ok();
    result
}

/// Records the response on the application's exact scope before its commit.
/// Cancellation/failure poisons the scope, preventing a partial response commit.
pub async fn complete(
    scope: &mut Scope<'_>,
    claim: &ResponseClaim,
    response: &StoredResponse,
) -> Result<(), Error> {
    let mut operation = Operation {
        scope,
        success: false,
    };
    let result=async {
        response.validate()?;
        let changed=sqlx::query("UPDATE raddle_http_responses SET status=$1,body=$2,location=$3 WHERE scope_hash=$4 AND key=$5 AND owner=$6 AND status IS NULL").bind(response.status as i16).bind(&response.body).bind(&response.location).bind(claim.key.scope.as_slice()).bind(&claim.key.key).bind(claim.owner).execute(operation.scope.connection()).await?;
        if changed.rows_affected()!=1 {return Err(Error::IdentityConflict);}
        Ok(())
    }.await;
    operation.success = result.is_ok();
    result
}

impl Store {
    /// Explicit separate-transaction compensation for a failed precommit call.
    /// Never removes a completed response or a later owner's claim.
    pub async fn release_http_response(&self, claim: &ResponseClaim) -> Result<bool, Error> {
        Ok(sqlx::query("DELETE FROM raddle_http_responses WHERE scope_hash=$1 AND key=$2 AND owner=$3 AND status IS NULL").bind(claim.key.scope.as_slice()).bind(&claim.key.key).bind(claim.owner).execute(&self.pool).await?.rows_affected()==1)
    }

    /// Bounded explicit expiry cleanup. Expiry alone never permits reexecution.
    /// Future caller cutoffs are clamped to now, preserving unexpired claims.
    pub async fn prune_http_responses(
        &self,
        before: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, Error> {
        if !(1..=1000).contains(&limit) {
            return Err(Error::Invalid("invalid HTTP response cleanup bound"));
        }
        Ok(sqlx::query("WITH old AS (SELECT scope_hash,key FROM raddle_http_responses WHERE expires_at<=$1 ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED) DELETE FROM raddle_http_responses r USING old WHERE r.scope_hash=old.scope_hash AND r.key=old.key").bind(before.min(Utc::now())).bind(limit as i64).execute(&self.pool).await?.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::StoredResponse;

    #[test]
    fn response_validation_covers_each_persisted_bound_inclusively() {
        let mut response = StoredResponse {
            status: 200,
            body: vec![0; 1_048_576],
            location: Some("x".repeat(8192)),
        };
        assert!(response.validate().is_ok());
        response.status = 399;
        assert!(response.validate().is_ok());
        for status in [199, 400] {
            response.status = status;
            assert!(response.validate().is_err());
        }
        response.status = 200;
        response.body.push(0);
        assert!(response.validate().is_err());
        response.body.clear();
        response.location = Some("x".repeat(8193));
        assert!(response.validate().is_err());
    }
}
