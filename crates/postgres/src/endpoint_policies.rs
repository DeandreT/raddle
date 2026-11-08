//! Opt-in local endpoint custody operations. Never mix this owner with a legacy
//! worker for the same subscription. Policies, not `Delivery::max_attempts`, own
//! the retry budget; expiry and partition fences still apply.

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::Row;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{Claim, Error, Scope, Store};

/// Fenced post-rollback custody disposition.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum PolicySettlement {
    /// Move to the queue tail, keeping identity and policy counters.
    QueueAt(DateTime<Utc>),
    /// Retain a successful terminal tombstone without dead-lettering.
    Discard,
    /// Quarantine without exposing formatted application errors.
    DeadLetter,
    /// Quarantine because replay of handling effects was not authorized.
    ReplayDenied,
}

/// Operator assertion after reconciling an uncertain policy execution.
#[derive(Clone, Copy, Debug)]
pub enum PolicyRecovery {
    /// All failed-attempt effects were verified rolled back.
    RolledBack,
    /// Stable application idempotency guards every potentially repeated effect.
    Idempotent,
}

fn until(now: DateTime<Utc>, lease: Duration) -> Result<DateTime<Utc>, Error> {
    if !(Duration::from_millis(1)..=Duration::from_secs(7200)).contains(&lease) {
        return Err(Error::Invalid("invalid policy lease"));
    }
    now.checked_add_signed(
        chrono::Duration::from_std(lease).map_err(|_| Error::Invalid("invalid lease"))?,
    )
    .ok_or(Error::Invalid("policy lease overflow"))
}

async fn fence(
    tx: &mut Transaction<'_, Postgres>,
    claim: &Claim,
    now: DateTime<Utc>,
) -> Result<(), Error> {
    let held: Option<Uuid> = sqlx::query_scalar("SELECT id FROM raddle_jobs WHERE id=$1 AND owner=$2 AND generation=$3 AND status='active' FOR UPDATE")
        .bind(claim.id).bind(claim.owner).bind(claim.generation).fetch_optional(&mut **tx).await?;
    if held.is_none() {
        return Err(Error::StaleClaim);
    }
    // Check after any row-lock wait, not only in the predicate evaluated before it.
    let live: bool = sqlx::query_scalar(
        "SELECT lease_until>GREATEST($2,clock_timestamp()) FROM raddle_jobs WHERE id=$1",
    )
    .bind(claim.id)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;
    if live { Ok(()) } else { Err(Error::StaleClaim) }
}

impl Store {
    /// Claims only this local policy endpoint. Claim acquisition is not an actual
    /// handling attempt. Expired leases remain recoverable without sampling a
    /// fictitious handler failure. Endpoint pauses are persisted across owners.
    pub async fn claim_policy_for(
        &self,
        destination: &str,
        limit: u32,
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<Vec<Claim>, Error> {
        raddle::SubscriptionName::try_new(destination)
            .map_err(|_| Error::Invalid("invalid policy endpoint"))?;
        if !(1..=256).contains(&limit) {
            return Err(Error::Invalid("invalid policy batch"));
        }
        let lease_until = until(now, lease)?;
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO raddle_endpoint_policy_pauses(destination,paused_until) VALUES($1,$2) ON CONFLICT DO NOTHING")
            .bind(destination).bind(now).execute(&mut *tx).await?;
        let paused: DateTime<Utc> = sqlx::query_scalar("SELECT paused_until FROM raddle_endpoint_policy_pauses WHERE destination=$1 FOR UPDATE")
            .bind(destination).fetch_one(&mut *tx).await?;
        if paused > now {
            return Ok(Vec::new());
        }
        sqlx::query("WITH expired AS (SELECT id FROM raddle_jobs WHERE kind='local' AND destination=$1 AND expires_at <= $2 AND (status='ready' OR (status='active' AND lease_until <= $2)) FOR UPDATE SKIP LOCKED) UPDATE raddle_jobs j SET status='dead',last_error='expired',owner=NULL,lease_until=NULL FROM expired e WHERE j.id=e.id")
            .bind(destination).bind(now).execute(&mut *tx).await?;
        let rows = sqlx::query("WITH candidates AS (SELECT j.id FROM raddle_jobs j WHERE j.kind='local' AND j.destination=$1 AND j.available_at <= $2 AND (j.expires_at IS NULL OR j.expires_at > $2) AND (j.status='ready' OR (j.status='active' AND j.lease_until <= $2)) AND NOT EXISTS (SELECT 1 FROM raddle_policy_attempts p WHERE p.job_id=j.id AND p.uncertain) AND NOT EXISTS (SELECT 1 FROM raddle_jobs previous WHERE j.partition_key IS NOT NULL AND previous.tenant=j.tenant AND previous.kind=j.kind AND previous.destination=j.destination AND previous.partition_key=j.partition_key AND previous.sequence < j.sequence AND previous.status NOT IN ('completed','dead')) ORDER BY j.sequence LIMIT $3 FOR UPDATE OF j SKIP LOCKED) UPDATE raddle_jobs j SET status='active',owner=$4,lease_until=$5,generation=generation+1 FROM candidates c WHERE j.id=c.id RETURNING j.id,j.owner,j.generation,j.envelope,j.source,j.tenant,j.destination,j.attempts,j.max_attempts")
            .bind(destination).bind(now).bind(i64::from(limit)).bind(Uuid::new_v4()).bind(lease_until)
            .fetch_all(&mut *tx).await?;
        let claims = rows
            .into_iter()
            .map(|row| {
                Ok(Claim {
                    id: row.try_get("id")?,
                    owner: row.try_get("owner")?,
                    generation: row.try_get("generation")?,
                    envelope: row.try_get("envelope")?,
                    tenant: row.try_get("tenant")?,
                    source: row.try_get("source")?,
                    destination: row.try_get("destination")?,
                    attempts: row.try_get("attempts")?,
                    max_attempts: row.try_get("max_attempts")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        tx.commit().await?;
        Ok(claims)
    }

    /// Persists one actual attempt before entering its execution transaction.
    /// Renews only an unexpired matching lease; a stale owner cannot resurrect
    /// itself. Returns a 1-based count retained across requeue and operator replay.
    /// A crash between this call and handler entry can conservatively consume a slot.
    pub async fn begin_policy_attempt(
        &self,
        claim: &Claim,
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<u64, Error> {
        let lease_until = until(now, lease)?;
        let mut tx = self.pool.begin().await?;
        fence(&mut tx, claim, now).await?;
        let changed = sqlx::query("UPDATE raddle_jobs SET lease_until=$5,attempts=LEAST(attempts::bigint+1,2147483647)::integer WHERE id=$1 AND owner=$2 AND generation=$3 AND status='active' AND lease_until>GREATEST($4,clock_timestamp()) AND (expires_at IS NULL OR expires_at>GREATEST($4,clock_timestamp())) AND NOT EXISTS (SELECT 1 FROM raddle_policy_attempts p WHERE p.job_id=raddle_jobs.id AND p.uncertain)")
            .bind(claim.id).bind(claim.owner).bind(claim.generation).bind(now).bind(lease_until).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            return Err(Error::StaleClaim);
        }
        // Persist uncertainty before any business effect. A lost connection can
        // never make an uncertain attempt silently eligible for lease recovery.
        let count: i64 = sqlx::query_scalar("INSERT INTO raddle_policy_attempts(job_id,handling,uncertain) VALUES($1,1,true) ON CONFLICT(job_id) DO UPDATE SET handling=raddle_policy_attempts.handling+1,uncertain=true RETURNING handling")
            .bind(claim.id).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(count as u64)
    }

    /// Acquires the original scoped transaction with a database-clock expiry
    /// fence after pool admission. Waiting for a connection must not admit an
    /// execution whose lease expired while it was waiting.
    pub async fn policy_execution(
        &self,
        claim: &Claim,
        now: DateTime<Utc>,
    ) -> Result<Scope<'static>, Error> {
        let mut tx = self.pool.begin().await?;
        fence(&mut tx, claim, now).await?;
        Ok(Scope::enroll(tx).with_claim(claim.clone()))
    }

    /// Clears the conservative in-flight marker only after the complete failed
    /// scoped attempt has joined a successful rollback and its cleanup. A stale
    /// owner cannot authorize retry by a successor.
    pub async fn policy_rolled_back(&self, claim: &Claim, now: DateTime<Utc>) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;
        fence(&mut tx, claim, now).await?;
        let changed = sqlx::query("UPDATE raddle_policy_attempts SET uncertain=false WHERE job_id=$1 AND EXISTS(SELECT 1 FROM raddle_jobs j WHERE j.id=$1 AND j.lease_until>GREATEST($2,clock_timestamp()))")
            .bind(claim.id)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        if changed.rows_affected() != 1 {
            return Err(Error::StaleClaim);
        }
        tx.commit().await?;
        Ok(())
    }

    /// Atomically settles only the live fenced claim after the attempt's SQL
    /// transaction has rolled back. Database failure leaves custody recoverable.
    pub async fn settle_policy(
        &self,
        claim: &Claim,
        settlement: PolicySettlement,
        now: DateTime<Utc>,
    ) -> Result<(), Error> {
        let (status, due, code, tail) = match settlement {
            PolicySettlement::QueueAt(due) => ("ready", due, "policy-retry", true),
            PolicySettlement::Discard => ("completed", now, "policy-discard", false),
            PolicySettlement::DeadLetter => ("dead", now, "policy-dead-letter", false),
            PolicySettlement::ReplayDenied => ("dead", now, "policy-replay-denied", false),
        };
        let mut tx = self.pool.begin().await?;
        fence(&mut tx, claim, now).await?;
        let changed = sqlx::query("UPDATE raddle_jobs SET status=$4,available_at=$5,last_error=$6,sequence=CASE WHEN $7 THEN nextval('raddle_jobs_sequence_seq') ELSE sequence END,completed_at=CASE WHEN $4='completed' THEN $8 ELSE NULL END,owner=NULL,lease_until=NULL WHERE id=$1 AND owner=$2 AND generation=$3 AND status='active' AND lease_until>GREATEST($8,clock_timestamp())")
            .bind(claim.id).bind(claim.owner).bind(claim.generation).bind(status).bind(due).bind(code).bind(tail).bind(now)
            .execute(&mut *tx).await?;
        if changed.rows_affected() == 1 {
            // Authorizing recovery and changing custody are one transaction.
            // A lost terminal transition cannot open an unauthorized retry gap.
            sqlx::query("UPDATE raddle_policy_attempts SET uncertain=false WHERE job_id=$1")
                .bind(claim.id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(())
        } else {
            Err(Error::StaleClaim)
        }
    }

    /// Pauses only the configured endpoint under the active claim's fence.
    /// A failed custody transition may leave the conservative pause in place.
    pub async fn pause_policy(
        &self,
        claim: &Claim,
        paused_until: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;
        // Match claim_policy_for's pause-row -> job-row lock order.
        sqlx::query(
            "SELECT destination FROM raddle_endpoint_policy_pauses WHERE destination=$1 FOR UPDATE",
        )
        .bind(&claim.destination)
        .execute(&mut *tx)
        .await?;
        fence(&mut tx, claim, now).await?;
        let changed = sqlx::query("INSERT INTO raddle_endpoint_policy_pauses(destination,paused_until) SELECT $1,$2 WHERE EXISTS(SELECT 1 FROM raddle_jobs WHERE id=$3 AND lease_until>GREATEST($4,clock_timestamp())) ON CONFLICT(destination) DO UPDATE SET paused_until=GREATEST(raddle_endpoint_policy_pauses.paused_until,excluded.paused_until)")
            .bind(&claim.destination).bind(paused_until).bind(claim.id).bind(now).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            return Err(Error::StaleClaim);
        }
        tx.commit().await?;
        Ok(())
    }

    /// Explicit tenant-scoped, audited resolution of uncertain execution after
    /// its lease expires. Never revives expired input or resets policy counters.
    /// The caller must establish the asserted safety before authorizing replay.
    pub async fn recover_policy_uncertain(
        &self,
        tenant: &str,
        id: Uuid,
        _safety: PolicyRecovery,
        actor: &str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, Error> {
        if actor.is_empty() || actor.len() > 128 || reason.is_empty() || reason.len() > 1024 {
            return Err(Error::Invalid("invalid policy recovery audit"));
        }
        let mut tx = self.pool.begin().await?;
        let held: Option<Uuid> = sqlx::query_scalar("SELECT j.id FROM raddle_jobs j JOIN raddle_policy_attempts p ON p.job_id=j.id WHERE j.id=$1 AND j.tenant=$2 AND j.status='active' AND j.lease_until<=$3 AND (j.expires_at IS NULL OR j.expires_at>$3) AND p.uncertain FOR UPDATE OF j")
            .bind(id).bind(tenant).bind(now).fetch_optional(&mut *tx).await?;
        if held.is_none() {
            return Ok(false);
        }
        sqlx::query("UPDATE raddle_policy_attempts SET uncertain=false WHERE job_id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE raddle_jobs SET status='ready',owner=NULL,lease_until=NULL,available_at=$2,last_error='policy-reconciled' WHERE id=$1").bind(id).bind(now).execute(&mut *tx).await?;
        sqlx::query(
            "INSERT INTO raddle_replay_audit(job_id,actor,reason,replayed_at) VALUES($1,$2,$3,$4)",
        )
        .bind(id)
        .bind(actor)
        .bind(reason)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }
}
