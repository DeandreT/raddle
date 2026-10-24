use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

use crate::{Claim, DeliveryKind, Error, FailureDisposition, Job, Scope};

/// Single-node SQLite store. `open` uses WAL, full synchronization and one connection.
#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
}
impl Store {
    /// Opens an embedded database without silently running schema migrations.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5));
        Ok(Self {
            pool: SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await?,
        })
    }
    /// Uses an application-owned pool whose durability options it controls.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
    /// Applies versioned checksummed migrations explicitly.
    pub async fn migrate(&self) -> Result<(), Error> {
        Ok(sqlx::migrate!().run(&self.pool).await?)
    }
    /// Starts an immediate write transaction, preventing deferred upgrade races.
    pub async fn begin(&self) -> Result<Scope<'static>, Error> {
        Ok(Scope::enroll(
            self.pool.begin_with("BEGIN IMMEDIATE").await?,
        ))
    }
    /// Closes the pool and waits for its checked-out connection to return.
    pub async fn close(&self) {
        self.pool.close().await;
    }
    /// Sets a finite pending-job budget for a tenant.
    pub async fn set_capacity(&self, tenant: &str, maximum: u32) -> Result<(), Error> {
        if tenant.is_empty() || tenant.len() > 128 || !(1..=10_000_000).contains(&maximum) {
            return Err(Error::Invalid("invalid capacity"));
        }
        sqlx::query("INSERT INTO raddle_quotas(tenant,max_pending) VALUES(?,?) ON CONFLICT(tenant) DO UPDATE SET max_pending=excluded.max_pending").bind(tenant).bind(i64::from(maximum)).execute(&self.pool).await?;
        Ok(())
    }
    /// Claims due work atomically, including lease takeover and attempt exhaustion.
    pub async fn claim(
        &self,
        kind: DeliveryKind,
        limit: u32,
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<Vec<Claim>, Error> {
        if !(1..=256).contains(&limit) || lease.is_zero() || lease > Duration::from_secs(7200) {
            return Err(Error::Invalid("invalid claim limits"));
        }
        let now = now.timestamp_millis();
        let until = now
            .checked_add(lease.as_millis() as i64)
            .ok_or(Error::Invalid("lease overflow"))?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("UPDATE raddle_jobs SET status='dead',last_error='expired-or-exhausted',lease_until=NULL WHERE kind=? AND (expires_at<=? OR attempts>=max_attempts) AND (status='ready' OR (status='active' AND lease_until<=?))").bind(kind.as_str()).bind(now).bind(now).execute(&mut *transaction).await?;
        let rows=sqlx::query("UPDATE raddle_jobs SET status='active',generation=generation+1,attempts=attempts+1,lease_until=? WHERE id IN (SELECT j.id FROM raddle_jobs j WHERE j.kind=? AND j.available_at<=? AND (j.status='ready' OR (j.status='active' AND j.lease_until<=?)) AND NOT EXISTS(SELECT 1 FROM raddle_jobs previous WHERE j.partition_key IS NOT NULL AND previous.tenant=j.tenant AND previous.kind=j.kind AND previous.destination=j.destination AND previous.partition_key=j.partition_key AND previous.sequence<j.sequence AND previous.status NOT IN ('completed','dead')) ORDER BY j.sequence LIMIT ?) RETURNING id,generation,envelope,destination,tenant,attempts,max_attempts")
            .bind(until).bind(kind.as_str()).bind(now).bind(now).bind(i64::from(limit)).fetch_all(&mut *transaction).await?;
        let claims = rows
            .into_iter()
            .map(|row| {
                Ok(Claim {
                    id: row.try_get("id")?,
                    generation: row.try_get("generation")?,
                    bytes: row.try_get("envelope")?,
                    destination: row.try_get("destination")?,
                    tenant: row.try_get("tenant")?,
                    attempts: row.try_get("attempts")?,
                    max_attempts: row.try_get("max_attempts")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        transaction.commit().await?;
        Ok(claims)
    }
    /// Begins the write transaction that fences a local execution's generation.
    pub async fn execution(
        &self,
        claim: &Claim,
        now: DateTime<Utc>,
    ) -> Result<Scope<'static>, Error> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let held:Option<String>=sqlx::query_scalar("SELECT id FROM raddle_jobs WHERE id=? AND generation=? AND status='active' AND lease_until>?")
            .bind(&claim.id).bind(claim.generation).bind(now.timestamp_millis()).fetch_optional(&mut *transaction).await?;
        if held.is_none() {
            return Err(Error::StaleClaim);
        }
        Ok(Scope::enroll(transaction).with_claim(claim.clone()))
    }
    /// Completes an acknowledged outgoing delivery only while its generation is owned.
    pub async fn complete(&self, claim: &Claim, now: DateTime<Utc>) -> Result<(), Error> {
        let result=sqlx::query("UPDATE raddle_jobs SET status='completed',completed_at=?,lease_until=NULL WHERE id=? AND generation=? AND status='active' AND lease_until>?").bind(now.timestamp_millis()).bind(&claim.id).bind(claim.generation).bind(now.timestamp_millis()).execute(&self.pool).await?;
        if result.rows_affected() == 1 {
            Ok(())
        } else {
            Err(Error::StaleClaim)
        }
    }
    /// Retries with bounded backoff or dead-letters a permanent failure.
    pub async fn fail(
        &self,
        claim: &Claim,
        disposition: FailureDisposition,
        now: DateTime<Utc>,
    ) -> Result<(), Error> {
        let terminal =
            disposition == FailureDisposition::Reject || claim.attempts >= claim.max_attempts;
        let delay = 1000i64
            .saturating_mul(1 << claim.attempts.saturating_sub(1).min(18))
            .min(300000);
        let due = now
            .timestamp_millis()
            .checked_add(delay)
            .ok_or(Error::Invalid("retry overflow"))?;
        let result=sqlx::query("UPDATE raddle_jobs SET status=?,available_at=?,lease_until=NULL,last_error=? WHERE id=? AND generation=? AND status='active'")
            .bind(if terminal{"dead"}else{"ready"}).bind(due).bind(if terminal{"rejected-or-exhausted"}else{"retry"}).bind(&claim.id).bind(claim.generation).execute(&self.pool).await?;
        if result.rows_affected() == 1 {
            Ok(())
        } else {
            Err(Error::StaleClaim)
        }
    }
    /// Reads status without revealing stored payloads.
    pub async fn inspect(&self, tenant: &str, id: Uuid) -> Result<Option<Job>, Error> {
        let row = sqlx::query("SELECT id,status,attempts FROM raddle_jobs WHERE tenant=? AND id=?")
            .bind(tenant)
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| {
            Ok(Job {
                id: Uuid::parse_str(row.try_get("id")?)
                    .map_err(|_| Error::Invalid("corrupt ID"))?,
                status: row.try_get("status")?,
                attempts: row.try_get::<i64, _>("attempts")? as u32,
            })
        })
        .transpose()
    }
    /// Requeues dead work with a retained operator audit, without reviving expired work.
    pub async fn replay(
        &self,
        tenant: &str,
        id: Uuid,
        actor: &str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, Error> {
        if actor.is_empty() || actor.len() > 128 || reason.is_empty() || reason.len() > 1024 {
            return Err(Error::Invalid("invalid audit"));
        }
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let replayable: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raddle_jobs WHERE tenant=? AND id=? AND status='dead' AND (expires_at IS NULL OR expires_at>?))")
            .bind(tenant).bind(id.to_string()).bind(now.timestamp_millis()).fetch_one(&mut *transaction).await?;
        if !replayable {
            return Ok(false);
        }
        let (pending, capacity): (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM raddle_jobs WHERE tenant=? AND status IN ('ready','active')),max_pending FROM raddle_quotas WHERE tenant=?")
            .bind(tenant).bind(tenant).fetch_one(&mut *transaction).await?;
        if pending >= capacity {
            return Err(Error::Capacity);
        }
        let result=sqlx::query("UPDATE raddle_jobs SET status='ready',available_at=?,attempts=0,last_error=NULL WHERE tenant=? AND id=? AND status='dead' AND (expires_at IS NULL OR expires_at>?)")
            .bind(now.timestamp_millis()).bind(tenant).bind(id.to_string()).bind(now.timestamp_millis()).execute(&mut *transaction).await?;
        if result.rows_affected() == 0 {
            return Ok(false);
        }
        sqlx::query(
            "INSERT INTO raddle_replay_audit(job_id,actor,reason,replayed_at) VALUES(?,?,?,?)",
        )
        .bind(id.to_string())
        .bind(actor)
        .bind(reason)
        .bind(now.timestamp_millis())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(true)
    }
    /// Removes a bounded tenant-scoped page of old completed tombstones without replay audits.
    pub async fn prune_completed(
        &self,
        tenant: &str,
        before: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, Error> {
        if !(1..=1000).contains(&limit) {
            return Err(Error::Invalid("invalid retention batch"));
        }
        Ok(sqlx::query("DELETE FROM raddle_jobs WHERE id IN(SELECT j.id FROM raddle_jobs j WHERE j.tenant=? AND j.status='completed' AND j.completed_at<? AND NOT EXISTS(SELECT 1 FROM raddle_replay_audit a WHERE a.job_id=j.id) ORDER BY j.sequence LIMIT ?)").bind(tenant).bind(before.timestamp_millis()).bind(i64::from(limit)).execute(&self.pool).await?.rows_affected())
    }
}
