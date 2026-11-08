use std::time::Duration;

use chrono::{DateTime, Utc};
use raddle::wire::Envelope;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use super::{BatchClaim, Error};
use crate::{BatchKey, ExecutionQueue, Options};

/// Application-provided SQLx pool; migration is owned by `raddle_postgres::Store`.
#[derive(Clone)]
pub struct BatchStore {
    pub(super) pool: PgPool,
}

impl BatchStore {
    /// Uses the same pool and migrations as the canonical PostgreSQL store.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Actually aggregates originals when size or time is reached. Does not claim
    /// an arbitrary database page as a batch. `partition_key` is the trusted group.
    ///
    /// Invoke for each authenticated tenant/group/consumer lane. A group has at
    /// most one unfinished batch. The first original's custody availability time
    /// anchors the timer, so restart does not reset accumulation. No synthetic
    /// payload is created. The key callback runs on canonical envelopes before
    /// membership is committed and must be deterministic across restarts.
    /// `InvalidMember`, malformed envelopes and invalid coalescing keys quarantine
    /// only the affected original; other errors are shared prerequisites. Failed
    /// preparation quarantine is postponed by the wait interval, never acknowledged.
    ///
    /// Pending admission is bounded by the canonical tenant quota (configure it
    /// with `Store::set_capacity`); `Options::capacity` is volatile-only. Dedicated queue use is
    /// an explicit opt-out from mixed unbatched sequencing.
    pub async fn assemble<F>(
        &self,
        queue: &ExecutionQueue,
        key: &BatchKey,
        options: Options,
        now: DateTime<Utc>,
        coalesce: F,
    ) -> Result<Option<Uuid>, Error>
    where
        F: Fn(&Envelope) -> Result<Option<String>, Error>,
    {
        let ExecutionQueue::Independent(destination) = queue;
        raddle::SubscriptionName::try_new(destination)
            .map_err(|_| Error::Invalid("destination"))?;
        let mut tx = self.pool.begin().await?;
        // A transaction-scoped lane mutex handles concurrent first use. Hash
        // collisions only serialize unrelated lanes, never combine their data.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended(json_build_array($1::text,$2::text,$3::text)::text,0))")
            .bind(key.tenant()).bind(destination).bind(key.group()).execute(&mut *tx).await?;
        let live: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raddle_batches WHERE tenant=$1 AND destination=$2 AND group_key IS NOT DISTINCT FROM $3 AND status <> 'completed')")
            .bind(key.tenant()).bind(destination).bind(key.group()).fetch_one(&mut *tx).await?;
        if live {
            return Ok(None);
        }
        // Canonical audited replay changes a dead original back to ready. Detach
        // only after its old batch is terminal, preserving failed-only replay.
        sqlx::query("DELETE FROM raddle_batch_members m USING raddle_jobs j,raddle_batches b WHERE m.job_id=j.id AND m.batch_id=b.id AND b.status='completed' AND j.status='ready' AND j.tenant=$1 AND j.destination=$2")
            .bind(key.tenant()).bind(destination).execute(&mut *tx).await?;
        let retry_at = super::until(now, options.wait())?;
        let expired = sqlx::query("SELECT j.id,CASE WHEN j.expires_at <= $4 THEN 'expired' ELSE 'attempts-exhausted' END AS code FROM raddle_jobs j WHERE j.tenant=$1 AND j.destination=$2 AND j.kind='local' AND j.partition_key IS NOT DISTINCT FROM $3 AND j.status='ready' AND (j.expires_at <= $4 OR j.attempts >= j.max_attempts) AND NOT EXISTS(SELECT 1 FROM raddle_batch_members m WHERE m.job_id=j.id) ORDER BY j.sequence LIMIT 256 FOR UPDATE OF j SKIP LOCKED")
            .bind(key.tenant()).bind(destination).bind(key.group()).bind(now).fetch_all(&mut *tx).await?;
        for row in expired {
            quarantine_preparation(&mut tx, row.try_get("id")?, row.try_get("code")?, retry_at)
                .await?;
        }
        let rows = sqlx::query("SELECT j.id,j.envelope,j.available_at,j.expires_at FROM raddle_jobs j WHERE j.tenant=$1 AND j.destination=$2 AND j.kind='local' AND j.partition_key IS NOT DISTINCT FROM $3 AND j.status='ready' AND j.available_at <= $4 AND (j.expires_at IS NULL OR j.expires_at > $4) AND j.attempts < j.max_attempts AND NOT EXISTS(SELECT 1 FROM raddle_batch_members m WHERE m.job_id=j.id) ORDER BY j.sequence LIMIT $5 FOR UPDATE OF j SKIP LOCKED")
            .bind(key.tenant()).bind(destination).bind(key.group()).bind(now).bind(options.size() as i64).fetch_all(&mut *tx).await?;
        let Some(first) = rows.first() else {
            tx.commit().await.map_err(Error::CustodyUncertain)?;
            return Ok(None);
        };
        let first_at = first.try_get("available_at")?;
        if rows.len() < options.size() && super::until(first_at, options.wait())? > now {
            tx.commit().await.map_err(Error::CustodyUncertain)?;
            return Ok(None);
        }
        let mut prepared = Vec::new();
        for row in rows {
            let job: Uuid = row.try_get("id")?;
            let preparation =
                Envelope::from_slice(&row.try_get::<Vec<u8>, _>("envelope")?, Default::default())
                    .map_err(Error::Wire)
                    .and_then(|envelope| coalesce(&envelope));
            let coalesce_key = match preparation {
                Ok(key)
                    if key
                        .as_ref()
                        .is_none_or(|key| !key.is_empty() && key.len() <= 128) =>
                {
                    key
                }
                Ok(_) => {
                    quarantine_preparation(&mut tx, job, "batch-invalid-key", retry_at).await?;
                    continue;
                }
                Err(Error::InvalidMember(_)) => {
                    quarantine_preparation(&mut tx, job, "batch-invalid-member", retry_at).await?;
                    continue;
                }
                Err(Error::Wire(_)) => {
                    quarantine_preparation(&mut tx, job, "batch-invalid-envelope", retry_at)
                        .await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            prepared.push((
                job,
                coalesce_key,
                row.try_get::<Option<DateTime<Utc>>, _>("expires_at")?,
            ));
        }
        if prepared.is_empty() {
            tx.commit().await.map_err(Error::CustodyUncertain)?;
            return Ok(None);
        }
        let expiry = prepared
            .iter()
            .map(|(_, _, expiry)| *expiry)
            .collect::<Option<Vec<_>>>()
            .and_then(|times| times.into_iter().max());
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO raddle_batches(id,tenant,destination,group_key,created_at,expires_at,status,available_at) VALUES($1,$2,$3,$4,$5,$6,'ready',$5)")
            .bind(id).bind(key.tenant()).bind(destination).bind(key.group()).bind(now).bind(expiry).execute(&mut *tx).await?;
        for (ordinal, (job, coalesce_key, _)) in prepared.into_iter().enumerate() {
            sqlx::query("INSERT INTO raddle_batch_members(job_id,batch_id,ordinal,coalesce_key) VALUES($1,$2,$3,$4)")
                .bind(job).bind(id).bind(ordinal as i32).bind(coalesce_key).execute(&mut *tx).await?;
            // Existing generic job claimers cannot steal assembled members, even
            // after process death. The batch row below is their only lease root.
            sqlx::query("UPDATE raddle_jobs SET status='active',owner=$2,lease_until='infinity',generation=generation+1 WHERE id=$1")
                .bind(job).bind(id).execute(&mut *tx).await?;
        }
        tx.commit().await.map_err(Error::CustodyUncertain)?;
        Ok(Some(id))
    }

    /// Claims one assembled batch, fencing takeover and counting attempts for
    /// still-pending originals only. Terminal originals never execute again.
    pub async fn claim(
        &self,
        queue: &ExecutionQueue,
        now: DateTime<Utc>,
        lease: Duration,
    ) -> Result<Option<BatchClaim>, Error> {
        let ExecutionQueue::Independent(destination) = queue;
        raddle::SubscriptionName::try_new(destination)
            .map_err(|_| Error::Invalid("destination"))?;
        if !(Duration::from_millis(1)..=Duration::from_secs(7200)).contains(&lease) {
            return Err(Error::Invalid("lease"));
        }
        let until = super::until(now, lease)?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT id,tenant,generation,attempts,expires_at FROM raddle_batches WHERE destination=$1 AND available_at <= $2 AND (status='ready' OR (status='active' AND lease_until <= $2)) ORDER BY created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED")
            .bind(destination).bind(now).fetch_optional(&mut *tx).await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let id: Uuid = row.try_get("id")?;
        let expiry: Option<DateTime<Utc>> = row.try_get("expires_at")?;
        // Lock originals before counting attempts or expiry. The active job lease
        // is internal infinity; only this batch generation grants execution rights.
        sqlx::query("SELECT j.id FROM raddle_jobs j JOIN raddle_batch_members m ON m.job_id=j.id WHERE m.batch_id=$1 AND j.status='active' ORDER BY j.id FOR UPDATE OF j")
            .bind(id).fetch_all(&mut *tx).await?;
        sqlx::query("UPDATE raddle_jobs j SET status='dead',owner=NULL,lease_until=NULL,last_error=CASE WHEN $2 THEN 'expired' ELSE 'attempts-exhausted' END FROM raddle_batch_members m WHERE m.job_id=j.id AND m.batch_id=$1 AND j.status='active' AND ($2 OR j.attempts >= j.max_attempts)")
            .bind(id).bind(expiry.is_some_and(|expiry| expiry <= now)).execute(&mut *tx).await?;
        let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM raddle_jobs j JOIN raddle_batch_members m ON m.job_id=j.id WHERE m.batch_id=$1 AND j.status='active'")
            .bind(id).fetch_one(&mut *tx).await?;
        if pending == 0 {
            sqlx::query("UPDATE raddle_batches SET status='completed',owner=NULL,lease_until=NULL,probing=false WHERE id=$1").bind(id).execute(&mut *tx).await?;
            tx.commit().await.map_err(Error::CustodyUncertain)?;
            return Ok(None);
        }
        let owner = Uuid::new_v4();
        let generation: i64 = row.try_get("generation")?;
        let generation = generation
            .checked_add(1)
            .ok_or(Error::Invalid("generation overflow"))?;
        let attempts: i32 = row.try_get("attempts")?;
        let attempts = attempts
            .checked_add(1)
            .ok_or(Error::Invalid("attempt overflow"))?;
        sqlx::query("UPDATE raddle_batches SET status='active',owner=$2,generation=$3,lease_until=$4,attempts=$5 WHERE id=$1")
            .bind(id).bind(owner).bind(generation).bind(until).bind(attempts).execute(&mut *tx).await?;
        sqlx::query("UPDATE raddle_jobs j SET attempts=attempts+1 FROM raddle_batch_members m WHERE m.job_id=j.id AND m.batch_id=$1 AND j.status='active'")
            .bind(id).execute(&mut *tx).await?;
        tx.commit().await.map_err(Error::CustodyUncertain)?;
        Ok(Some(BatchClaim {
            id,
            owner,
            generation,
            tenant: row.try_get("tenant")?,
            attempts: attempts as u32,
        }))
    }

    /// Tenant/destination pending originals, including accumulation, execution and retries.
    /// This is database custody, never an acknowledgement inferred from handler return.
    pub async fn pending(&self, tenant: &str, queue: &ExecutionQueue) -> Result<i64, Error> {
        let ExecutionQueue::Independent(destination) = queue;
        Ok(sqlx::query_scalar("SELECT count(*) FROM raddle_jobs WHERE tenant=$1 AND destination=$2 AND kind='local' AND status IN ('ready','active')")
            .bind(tenant).bind(destination).fetch_one(&self.pool).await?)
    }

    /// Enters one durable size-one sweep after a matching opaque failure. The
    /// caller matches its concrete domain error; no string/type erasure policy.
    /// Returns false if already used or below the bounded configured threshold.
    /// Each call records one matching failure; call once per rolled-back typed
    /// handler failure, not once per abandoned claim. `run_once` owns this step.
    pub async fn isolate_once(
        &self,
        claim: &BatchClaim,
        after: u32,
        now: DateTime<Utc>,
    ) -> Result<bool, Error> {
        if !(1..=1000).contains(&after) {
            return Err(Error::Invalid("probe threshold"));
        }
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT isolation_used,matching_failures FROM raddle_batches WHERE id=$1 AND owner=$2 AND generation=$3 AND status='active' AND lease_until > $4 FOR UPDATE")
            .bind(claim.id).bind(claim.owner).bind(claim.generation).bind(now).fetch_optional(&mut *tx).await?.ok_or(Error::Stale)?;
        if row.try_get::<bool, _>("isolation_used")? {
            return Ok(false);
        }
        let failures = row
            .try_get::<i32, _>("matching_failures")?
            .checked_add(1)
            .ok_or(Error::Invalid("failure count overflow"))?;
        sqlx::query("UPDATE raddle_batches SET matching_failures=$2 WHERE id=$1")
            .bind(claim.id)
            .bind(failures)
            .execute(&mut *tx)
            .await?;
        if failures < after as i32 {
            tx.commit().await.map_err(Error::CustodyUncertain)?;
            return Ok(false);
        }
        sqlx::query("UPDATE raddle_batches SET isolation_used=true,probing=true WHERE id=$1")
            .bind(claim.id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await.map_err(Error::CustodyUncertain)?;
        Ok(true)
    }
}

async fn quarantine_preparation(
    connection: &mut PgConnection,
    job: Uuid,
    code: &str,
    retry_at: DateTime<Utc>,
) -> Result<(), Error> {
    sqlx::query("SAVEPOINT batch_preparation")
        .execute(&mut *connection)
        .await?;
    let result = sqlx::query("UPDATE raddle_jobs SET status='dead',last_error=$2,owner=NULL,lease_until=NULL WHERE id=$1 AND status='ready'")
        .bind(job).bind(code).execute(&mut *connection).await;
    if !result.is_ok_and(|result| result.rows_affected() == 1) {
        sqlx::query("ROLLBACK TO SAVEPOINT batch_preparation")
            .execute(&mut *connection)
            .await?;
        sqlx::query("UPDATE raddle_jobs SET available_at=$2,last_error='batch-preparation-retry' WHERE id=$1 AND status='ready'")
            .bind(job).bind(retry_at).execute(&mut *connection).await?;
    }
    sqlx::query("RELEASE SAVEPOINT batch_preparation")
        .execute(&mut *connection)
        .await?;
    Ok(())
}
