use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};
use uuid::Uuid;

use crate::clock::Clock;
use crate::execution::Execution;
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
    /// Workers reserve each execution connection's SQLite progress-handler slot.
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
    /// Leases must be between one millisecond and two hours to survive storage precision.
    pub async fn claim(
        &self,
        kind: DeliveryKind,
        limit: u32,
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<Vec<Claim>, Error> {
        self.claim_inner(kind, None, limit, lease, now).await
    }

    /// Claims one configured destination, including its expiry/exhaustion sweep.
    /// Other destinations' custody and attempts remain untouched.
    pub async fn claim_for(
        &self,
        kind: DeliveryKind,
        destination: &str,
        limit: u32,
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<Vec<Claim>, Error> {
        raddle::SubscriptionName::try_new(destination)
            .map_err(|_| Error::Invalid("invalid destination"))?;
        self.claim_inner(kind, Some(&[destination]), limit, lease, now)
            .await
    }

    pub(crate) async fn claim_registered(
        &self,
        destinations: &[&str],
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<Vec<Claim>, Error> {
        self.claim_inner(DeliveryKind::Local, Some(destinations), 1, lease, now)
            .await
    }

    async fn claim_inner(
        &self,
        kind: DeliveryKind,
        destinations: Option<&[&str]>,
        limit: u32,
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<Vec<Claim>, Error> {
        let clock = Clock::new(now);
        if !(1..=256).contains(&limit)
            || !(Duration::from_millis(1)..=Duration::from_secs(7200)).contains(&lease)
        {
            return Err(Error::Invalid("invalid claim limits"));
        }
        if destinations.is_some_and(<[&str]>::is_empty) {
            return Ok(Vec::new());
        }
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let now = clock.now()?;
        let until = now
            .checked_add(lease.as_millis() as i64)
            .ok_or(Error::Invalid("lease overflow"))?;
        let mut expired = QueryBuilder::<Sqlite>::new(
            "UPDATE raddle_jobs SET status='dead',last_error='expired-or-exhausted',lease_until=NULL WHERE kind=",
        );
        expired.push_bind(kind.as_str());
        restrict_destinations(&mut expired, "destination", destinations);
        expired.push(" AND (expires_at<=").push_bind(now)
            .push(" OR attempts>=max_attempts) AND (status='ready' OR (status='active' AND lease_until<=")
            .push_bind(now).push("))");
        expired.build().execute(&mut *transaction).await?;
        let mut selection = QueryBuilder::<Sqlite>::new(
            "UPDATE raddle_jobs SET status='active',generation=generation+1,attempts=attempts+1,lease_until=",
        );
        selection
            .push_bind(until)
            .push(" WHERE id IN (SELECT j.id FROM raddle_jobs j WHERE j.kind=")
            .push_bind(kind.as_str());
        restrict_destinations(&mut selection, "j.destination", destinations);
        selection.push(" AND j.available_at<=").push_bind(now)
            .push(" AND (j.status='ready' OR (j.status='active' AND j.lease_until<=")
            .push_bind(now)
            .push(")) AND NOT EXISTS(SELECT 1 FROM raddle_jobs previous WHERE j.partition_key IS NOT NULL AND previous.tenant=j.tenant AND previous.kind=j.kind AND previous.destination=j.destination AND previous.partition_key=j.partition_key AND previous.sequence<j.sequence AND previous.status NOT IN ('completed','dead')) ORDER BY j.sequence LIMIT ")
            .push_bind(i64::from(limit))
            .push(") RETURNING id,generation,envelope,destination,tenant,attempts,max_attempts");
        let rows = selection.build().fetch_all(&mut *transaction).await?;
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
        let clock = Clock::new(now);
        let scope = self.begin().await?;
        Self::fence_execution(scope, claim, clock.now()?).await
    }

    pub(crate) async fn worker_execution(
        &self,
        claim: &Claim,
        now: DateTime<Utc>,
        timeout: Duration,
    ) -> Result<Scope<'static>, Error> {
        let clock = Clock::new(now);
        let execution = Execution::begin(&self.pool).await?;
        let mut scope =
            Self::fence_execution(Scope::execution(execution), claim, clock.now()?).await?;
        scope.start_deadline(timeout).await?;
        Ok(scope)
    }

    async fn fence_execution(
        mut scope: Scope<'static>,
        claim: &Claim,
        now: i64,
    ) -> Result<Scope<'static>, Error> {
        let held:Option<String>=sqlx::query_scalar("SELECT id FROM raddle_jobs WHERE id=? AND generation=? AND status='active' AND lease_until>?")
            .bind(&claim.id).bind(claim.generation).bind(now).fetch_optional(scope.connection()).await?;
        if held.is_none() {
            return Err(Error::StaleClaim);
        }
        Ok(scope.with_claim(claim.clone()))
    }
    /// Completes an acknowledged outgoing delivery only while its generation is owned.
    pub async fn complete(&self, claim: &Claim, now: DateTime<Utc>) -> Result<(), Error> {
        let clock = Clock::new(now);
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let now = clock.now()?;
        let result=sqlx::query("UPDATE raddle_jobs SET status='completed',completed_at=?,lease_until=NULL WHERE id=? AND generation=? AND status='active' AND lease_until>?").bind(now).bind(&claim.id).bind(claim.generation).bind(now).execute(&mut *transaction).await?;
        if result.rows_affected() == 1 {
            transaction.commit().await?;
            Ok(())
        } else {
            transaction.rollback().await?;
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
        let clock = Clock::new(now);
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let now = clock.now()?;
        let terminal =
            disposition == FailureDisposition::Reject || claim.attempts >= claim.max_attempts;
        let delay = 1000i64
            .saturating_mul(1 << claim.attempts.saturating_sub(1).min(18))
            .min(300000);
        let due = now
            .checked_add(delay)
            .ok_or(Error::Invalid("retry overflow"))?;
        let result=sqlx::query("UPDATE raddle_jobs SET status=?,available_at=?,lease_until=NULL,last_error=? WHERE id=? AND generation=? AND status='active' AND lease_until>?")
            .bind(if terminal{"dead"}else{"ready"}).bind(due).bind(if terminal{"rejected-or-exhausted"}else{"retry"}).bind(&claim.id).bind(claim.generation).bind(now).execute(&mut *transaction).await?;
        if result.rows_affected() == 1 {
            transaction.commit().await?;
            Ok(())
        } else {
            transaction.rollback().await?;
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

fn restrict_destinations<'args>(
    query: &mut QueryBuilder<'args, Sqlite>,
    column: &'static str,
    destinations: Option<&'args [&'args str]>,
) {
    if let Some(destinations) = destinations {
        query.push(" AND ").push(column).push(" IN (");
        let mut values = query.separated(",");
        for destination in destinations {
            values.push_bind(*destination);
        }
        values.push_unseparated(")");
    }
}
