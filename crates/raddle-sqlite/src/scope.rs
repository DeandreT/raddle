use chrono::{DateTime, Utc};
use raddle::wire::Envelope;
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, SqliteConnection, Transaction};
use uuid::Uuid;

use crate::{Claim, Delivery, Error};

/// Owns the application's actual SQLite transaction and all staged effects.
pub struct Scope<'connection> {
    transaction: Transaction<'connection, Sqlite>,
    poisoned: bool,
    claim: Option<Claim>,
}
impl<'connection> Scope<'connection> {
    /// Enrolls an existing transaction. Prefer `BEGIN IMMEDIATE` for read/write operations.
    pub fn enroll(transaction: Transaction<'connection, Sqlite>) -> Self {
        Self {
            transaction,
            poisoned: false,
            claim: None,
        }
    }
    /// Gives application repositories the same connection.
    pub fn connection(&mut self) -> &mut SqliteConnection {
        &mut self.transaction
    }
    /// Prevents commit after an application-level error.
    pub fn mark_rollback_only(&mut self) {
        self.poisoned = true;
    }
    /// Authenticated tenant for a worker execution; absent on producer scopes.
    pub fn tenant(&self) -> Option<&str> {
        self.claim.as_ref().map(Claim::tenant)
    }

    /// Original envelope for a worker execution, absent on producer scopes.
    pub fn envelope(&self) -> Result<Option<Envelope>, Error> {
        self.claim.as_ref().map(Claim::envelope).transpose()
    }
    pub(crate) fn with_claim(mut self, claim: Claim) -> Self {
        self.claim = Some(claim);
        self
    }

    /// Stages all local/remote destinations atomically with application writes.
    pub async fn stage(
        &mut self,
        source: &str,
        tenant: &str,
        envelope: &Envelope,
        deliveries: &[Delivery],
    ) -> Result<Vec<Uuid>, Error> {
        if self.poisoned {
            return Err(Error::RollbackOnly);
        }
        let result = self.stage_inner(source, tenant, envelope, deliveries).await;
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    async fn stage_inner(
        &mut self,
        source: &str,
        tenant: &str,
        envelope: &Envelope,
        deliveries: &[Delivery],
    ) -> Result<Vec<Uuid>, Error> {
        if source.is_empty()
            || source.len() > 128
            || tenant.is_empty()
            || tenant.len() > 128
            || deliveries.len() > 256
        {
            return Err(Error::Invalid("invalid custody bounds"));
        }
        envelope.validate(Default::default())?;
        let bytes = envelope.to_vec()?;
        let mut ids = Vec::with_capacity(deliveries.len());
        let mut added = false;
        for delivery in deliveries {
            delivery.validate()?;
            let mut digest = Sha256::new();
            digest.update(&bytes);
            digest.update(
                serde_json::to_vec(&(
                    delivery.available_at,
                    delivery.expires_at,
                    &delivery.partition_key,
                    delivery.max_attempts,
                ))
                .map_err(raddle::wire::WireError::from)?,
            );
            let fingerprint = digest.finalize().to_vec();
            let existing=sqlx::query("SELECT id,fingerprint FROM raddle_jobs WHERE source=? AND tenant=? AND destination=? AND kind=? AND message_id=?")
                .bind(source).bind(tenant).bind(&delivery.destination).bind(delivery.kind.as_str()).bind(envelope.id().to_string()).fetch_optional(&mut *self.transaction).await?;
            if let Some(existing) = existing {
                if existing.try_get::<Vec<u8>, _>("fingerprint")? != fingerprint {
                    return Err(Error::IdentityConflict);
                }
                ids.push(
                    Uuid::parse_str(existing.try_get("id")?)
                        .map_err(|_| Error::Invalid("corrupt job ID"))?,
                );
                continue;
            }
            let id = Uuid::new_v4();
            sqlx::query("INSERT INTO raddle_jobs(id,source,tenant,destination,kind,message_id,envelope,fingerprint,available_at,expires_at,partition_key,max_attempts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)")
                .bind(id.to_string()).bind(source).bind(tenant).bind(&delivery.destination).bind(delivery.kind.as_str()).bind(envelope.id().to_string()).bind(&bytes).bind(fingerprint).bind(delivery.available_at.timestamp_millis()).bind(delivery.expires_at.map(|time|time.timestamp_millis())).bind(&delivery.partition_key).bind(i64::from(delivery.max_attempts)).execute(&mut *self.transaction).await?;
            ids.push(id);
            added = true;
        }
        sqlx::query("INSERT INTO raddle_quotas(tenant) VALUES(?) ON CONFLICT DO NOTHING")
            .bind(tenant)
            .execute(&mut *self.transaction)
            .await?;
        let capacity: i64 =
            sqlx::query_scalar("SELECT max_pending FROM raddle_quotas WHERE tenant=?")
                .bind(tenant)
                .fetch_one(&mut *self.transaction)
                .await?;
        let pending: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM raddle_jobs WHERE tenant=? AND status IN ('ready','active')",
        )
        .bind(tenant)
        .fetch_one(&mut *self.transaction)
        .await?;
        let completing = i64::from(
            self.claim
                .as_ref()
                .is_some_and(|claim| claim.tenant() == tenant),
        );
        if added && pending - completing > capacity {
            return Err(Error::Capacity);
        }
        Ok(ids)
    }
    /// Commits application writes, messages, and optional inbox completion together.
    pub async fn commit(mut self, now: DateTime<Utc>) -> Result<(), Error> {
        if self.poisoned {
            self.transaction.rollback().await?;
            return Err(Error::RollbackOnly);
        }
        if let Some(claim) = &self.claim {
            let result=sqlx::query("UPDATE raddle_jobs SET status='completed',completed_at=?,lease_until=NULL WHERE id=? AND generation=? AND status='active'")
                .bind(now.timestamp_millis()).bind(&claim.id).bind(claim.generation).execute(&mut *self.transaction).await?;
            if result.rows_affected() != 1 {
                self.transaction.rollback().await?;
                return Err(Error::StaleClaim);
            }
        }
        Ok(self.transaction.commit().await?)
    }
    /// Rolls back every effect explicitly; dropping the scope also rolls back.
    pub async fn rollback(self) -> Result<(), Error> {
        Ok(self.transaction.rollback().await?)
    }
}
