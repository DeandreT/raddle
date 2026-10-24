use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{Claim, DeliveryKind, Error, FailureDisposition, Job, JobStatus, RetryPolicy, Scope};

/// PostgreSQL-backed inbox/outbox. Clones share only SQLx's connection pool.
#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    /// Uses an application-provided pool. Does not migrate automatically.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    /// Applies checksummed versioned migrations explicitly.
    pub async fn migrate(&self) -> Result<(), Error> {
        Ok(sqlx::migrate!().run(&self.pool).await?)
    }
    /// Starts a runtime-owned application transaction.
    pub async fn begin(&self) -> Result<Scope<'static>, Error> {
        Ok(Scope::enroll(self.pool.begin().await?))
    }

    /// Deletes a bounded page of expired idempotency records without live jobs.
    /// The caller explicitly chooses the end of the HTTP duplicate-protection window.
    pub async fn prune_idempotency(
        &self,
        tenant: &str,
        before: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, Error> {
        if !(1..=1000).contains(&limit) {
            return Err(Error::Invalid("invalid retention batch"));
        }
        let result = sqlx::query("WITH old AS (SELECT tenant,source,key FROM raddle_idempotency i WHERE tenant=$1 AND created_at < $2 AND NOT EXISTS(SELECT 1 FROM raddle_jobs j WHERE j.tenant=i.tenant AND j.message_id=i.message_id AND j.status IN ('ready','active')) ORDER BY created_at LIMIT $3 FOR UPDATE SKIP LOCKED) DELETE FROM raddle_idempotency i USING old WHERE i.tenant=old.tenant AND i.source=old.source AND i.key=old.key")
            .bind(tenant).bind(before).bind(i64::from(limit)).execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    /// Sets a finite pending-job budget. Lowering it does not delete accepted work.
    pub async fn set_capacity(&self, tenant: &str, maximum: u32) -> Result<(), Error> {
        if tenant.is_empty() || tenant.len() > 128 || !(1..=10_000_000).contains(&maximum) {
            return Err(Error::Invalid("invalid tenant capacity"));
        }
        sqlx::query("INSERT INTO raddle_quotas(tenant,max_pending) VALUES($1,$2) ON CONFLICT(tenant) DO UPDATE SET max_pending=excluded.max_pending")
            .bind(tenant).bind(maximum as i32).execute(&self.pool).await?;
        Ok(())
    }

    /// Deletes a bounded page of old completed tombstones with no retained replay audit.
    ///
    /// This ends duplicate protection for those identities. The caller chooses
    /// the retention cutoff explicitly; live or dead work is never deleted.
    pub async fn prune_completed(
        &self,
        tenant: &str,
        before: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, Error> {
        if !(1..=1000).contains(&limit) {
            return Err(Error::Invalid("invalid retention batch"));
        }
        let result = sqlx::query("WITH old AS (SELECT id FROM raddle_jobs j WHERE tenant=$1 AND status='completed' AND completed_at < $2 AND NOT EXISTS(SELECT 1 FROM raddle_replay_audit a WHERE a.job_id=j.id) ORDER BY completed_at LIMIT $3 FOR UPDATE SKIP LOCKED) DELETE FROM raddle_jobs j USING old WHERE j.id=old.id")
            .bind(tenant).bind(before).bind(i64::from(limit)).execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    /// Claims bounded due work, fencing every takeover with a new generation.
    ///
    /// For a partition key, only its earliest unfinished visible job is eligible.
    /// Database sequence order is not an ordering guarantee between uncommitted producers.
    pub async fn claim(
        &self,
        kind: DeliveryKind,
        limit: u32,
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<Vec<Claim>, Error> {
        if !(1..=256).contains(&limit) || lease.is_zero() || lease > Duration::from_secs(7200) {
            return Err(Error::Invalid("invalid claim bounds"));
        }
        let until =
            now + chrono::Duration::from_std(lease).map_err(|_| Error::Invalid("invalid lease"))?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query("WITH expired AS (SELECT id FROM raddle_jobs WHERE kind=$1 AND (expires_at <= $2 OR attempts >= max_attempts) AND (status='ready' OR (status='active' AND lease_until <= $2)) FOR UPDATE SKIP LOCKED) UPDATE raddle_jobs j SET status='dead',last_error=CASE WHEN j.expires_at <= $2 THEN 'expired' ELSE 'attempts-exhausted' END,owner=NULL,lease_until=NULL FROM expired e WHERE j.id=e.id")
            .bind(kind.as_str()).bind(now).execute(&mut *transaction).await?;
        let rows = sqlx::query(
            "WITH candidates AS (SELECT j.id FROM raddle_jobs j WHERE j.kind=$1 AND j.available_at <= $2 AND (j.expires_at IS NULL OR j.expires_at > $2) AND (j.status='ready' OR (j.status='active' AND j.lease_until <= $2)) AND NOT EXISTS (SELECT 1 FROM raddle_jobs previous WHERE j.partition_key IS NOT NULL AND previous.tenant=j.tenant AND previous.kind=j.kind AND previous.destination=j.destination AND previous.partition_key=j.partition_key AND previous.sequence < j.sequence AND previous.status NOT IN ('completed','dead')) ORDER BY j.sequence LIMIT $3 FOR UPDATE OF j SKIP LOCKED) UPDATE raddle_jobs j SET status='active',owner=$4,lease_until=$5,generation=generation+1,attempts=attempts+1 FROM candidates c WHERE j.id=c.id RETURNING j.id,j.owner,j.generation,j.envelope,j.tenant,j.destination,j.attempts,j.max_attempts"
        ).bind(kind.as_str()).bind(now).bind(i64::from(limit)).bind(Uuid::new_v4()).bind(until).fetch_all(&mut *transaction).await?;
        let claims = rows
            .into_iter()
            .map(|row| {
                Ok(Claim {
                    id: row.try_get("id")?,
                    owner: row.try_get("owner")?,
                    generation: row.try_get("generation")?,
                    envelope: row.try_get("envelope")?,
                    tenant: row.try_get("tenant")?,
                    destination: row.try_get("destination")?,
                    attempts: row.try_get("attempts")?,
                    max_attempts: row.try_get("max_attempts")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        transaction.commit().await?;
        Ok(claims)
    }

    /// Locks and verifies a claim before the application touches business data.
    /// A database row lock fences recovery while the execution transaction lives.
    pub async fn execution(
        &self,
        claim: &Claim,
        now: DateTime<Utc>,
    ) -> Result<Scope<'static>, Error> {
        let mut transaction = self.pool.begin().await?;
        let held: Option<Uuid> = sqlx::query_scalar("SELECT id FROM raddle_jobs WHERE id=$1 AND owner=$2 AND generation=$3 AND status='active' AND lease_until > $4 FOR UPDATE")
            .bind(claim.id).bind(claim.owner).bind(claim.generation).bind(now).fetch_optional(&mut *transaction).await?;
        if held.is_none() {
            return Err(Error::StaleClaim);
        }
        Ok(Scope::enroll(transaction).with_claim(claim.clone()))
    }

    /// Conditionally marks a confirmed outgoing delivery complete.
    pub async fn complete(&self, claim: &Claim, now: DateTime<Utc>) -> Result<(), Error> {
        let changed = sqlx::query("UPDATE raddle_jobs SET status='completed',completed_at=$4,owner=NULL,lease_until=NULL,last_error=NULL WHERE id=$1 AND owner=$2 AND generation=$3 AND status='active' AND lease_until > $4")
            .bind(claim.id).bind(claim.owner).bind(claim.generation).bind(now).execute(&self.pool).await?;
        if changed.rows_affected() == 1 {
            Ok(())
        } else {
            Err(Error::StaleClaim)
        }
    }

    /// Records a classified failure or reschedules with deterministic bounded jitter.
    pub async fn fail(
        &self,
        claim: &Claim,
        disposition: FailureDisposition,
        policy: RetryPolicy,
        code: &'static str,
        now: DateTime<Utc>,
    ) -> Result<(), Error> {
        let terminal =
            disposition == FailureDisposition::Reject || claim.attempts >= claim.max_attempts;
        let due = now
            + chrono::Duration::from_std(policy.delay(claim))
                .map_err(|_| Error::Invalid("invalid retry duration"))?;
        let changed = sqlx::query("UPDATE raddle_jobs SET status=$4,available_at=$5,owner=NULL,lease_until=NULL,last_error=$6 WHERE id=$1 AND owner=$2 AND generation=$3 AND status='active'")
            .bind(claim.id).bind(claim.owner).bind(claim.generation).bind(if terminal { "dead" } else { "ready" }).bind(due).bind(code).execute(&self.pool).await?;
        if changed.rows_affected() == 1 {
            Ok(())
        } else {
            Err(Error::StaleClaim)
        }
    }

    /// Reads a payload-free status view inside an authenticated tenant scope.
    pub async fn inspect(&self, tenant: &str, id: Uuid) -> Result<Option<Job>, Error> {
        let row = sqlx::query("SELECT id,message_id,status,attempts,last_error FROM raddle_jobs WHERE tenant=$1 AND id=$2")
            .bind(tenant).bind(id).fetch_optional(&self.pool).await?;
        row.map(|row| {
            let status = match row.try_get::<&str, _>("status")? {
                "ready" => JobStatus::Ready,
                "active" => JobStatus::Active,
                "completed" => JobStatus::Completed,
                "dead" => JobStatus::Dead,
                _ => return Err(Error::Invalid("corrupt persisted job status")),
            };
            Ok(Job {
                id: row.try_get("id")?,
                message_id: row.try_get("message_id")?,
                status,
                attempts: row.try_get::<i32, _>("attempts")? as u32,
                last_error: row.try_get("last_error")?,
            })
        })
        .transpose()
    }

    /// Requeues dead work with its original identity and a retained operator audit.
    pub async fn replay(
        &self,
        tenant: &str,
        id: Uuid,
        actor: &str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, Error> {
        if actor.is_empty() || actor.len() > 128 || reason.is_empty() || reason.len() > 1024 {
            return Err(Error::Invalid("invalid replay audit"));
        }
        let mut transaction = self.pool.begin().await?;
        sqlx::query("INSERT INTO raddle_quotas(tenant) VALUES($1) ON CONFLICT DO NOTHING")
            .bind(tenant)
            .execute(&mut *transaction)
            .await?;
        let capacity: i32 =
            sqlx::query_scalar("SELECT max_pending FROM raddle_quotas WHERE tenant=$1 FOR UPDATE")
                .bind(tenant)
                .fetch_one(&mut *transaction)
                .await?;
        // Never wait on a running execution while holding the admission lock.
        let candidate: Option<Uuid> = sqlx::query_scalar("SELECT id FROM raddle_jobs WHERE tenant=$1 AND id=$2 AND status='dead' AND (expires_at IS NULL OR expires_at>$3) FOR UPDATE SKIP LOCKED")
            .bind(tenant).bind(id).bind(now).fetch_optional(&mut *transaction).await?;
        if candidate.is_none() {
            return Ok(false);
        }
        let pending: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM raddle_jobs WHERE tenant=$1 AND status IN ('ready','active')",
        )
        .bind(tenant)
        .fetch_one(&mut *transaction)
        .await?;
        if pending >= i64::from(capacity) {
            return Err(Error::Capacity);
        }
        let result = sqlx::query("UPDATE raddle_jobs SET status='ready',available_at=$3,attempts=0,last_error=NULL WHERE tenant=$1 AND id=$2 AND status='dead' AND (expires_at IS NULL OR expires_at > $3)")
            .bind(tenant).bind(id).bind(now).execute(&mut *transaction).await?;
        if result.rows_affected() == 0 {
            return Ok(false);
        }
        sqlx::query(
            "INSERT INTO raddle_replay_audit(job_id,actor,reason,replayed_at) VALUES($1,$2,$3,$4)",
        )
        .bind(id)
        .bind(actor)
        .bind(reason)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(true)
    }
}
