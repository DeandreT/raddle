use chrono::{DateTime, Utc};
use raddle::wire::Envelope;
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::{Claim, Delivery, Error};

/// An owned SQLx transaction whose durable effects share the application's connection.
///
/// No commit occurs until `commit` is called. Dropping the scope rolls back through
/// SQLx. Any failed staging operation poisons the scope, including validation
/// errors that would not themselves abort a PostgreSQL transaction.
pub struct Scope<'connection> {
    transaction: Transaction<'connection, Postgres>,
    poisoned: bool,
    claim: Option<Claim>,
}

impl<'connection> Scope<'connection> {
    /// Takes ownership of an existing application transaction; never opens another.
    pub fn enroll(transaction: Transaction<'connection, Postgres>) -> Self {
        Self {
            transaction,
            poisoned: false,
            claim: None,
        }
    }

    /// Gives application repositories the same transaction connection.
    ///
    /// Application errors must be propagated or followed by `mark_rollback_only`;
    /// swallowing a repository error cannot be detected by a wrapper.
    pub fn connection(&mut self) -> &mut PgConnection {
        &mut self.transaction
    }

    /// Prevents commit after an application-level failure, even if SQL succeeded.
    pub fn mark_rollback_only(&mut self) {
        self.poisoned = true;
    }

    /// Returns the authenticated tenant of the current delivery, if this is a worker scope.
    pub fn tenant(&self) -> Option<&str> {
        self.claim.as_ref().map(|claim| claim.tenant())
    }

    pub(crate) fn with_claim(mut self, claim: Claim) -> Self {
        self.claim = Some(claim);
        self
    }

    /// Atomically stages independent subscriber jobs and outbox deliveries.
    ///
    /// Repeated identical custody is idempotent. Conflicting immutable content,
    /// including routing/schedule metadata, is rejected and poisons the scope.
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
            return Err(Error::Invalid("invalid custody scope or fan-out size"));
        }
        envelope.validate(Default::default())?;
        sqlx::query("INSERT INTO raddle_quotas(tenant) VALUES($1) ON CONFLICT DO NOTHING")
            .bind(tenant)
            .execute(&mut *self.transaction)
            .await?;
        let capacity: i32 =
            sqlx::query_scalar("SELECT max_pending FROM raddle_quotas WHERE tenant=$1 FOR UPDATE")
                .bind(tenant)
                .fetch_one(&mut *self.transaction)
                .await?;
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
            // Fingerprints are immutable. Reading existing custody without a
            // row lock avoids a quota-lock/inbox-lock inversion with workers.
            let existing = sqlx::query("SELECT id,fingerprint FROM raddle_jobs WHERE source=$1 AND tenant=$2 AND destination=$3 AND kind=$4 AND message_id=$5")
                .bind(source).bind(tenant).bind(&delivery.destination).bind(delivery.kind.as_str()).bind(envelope.id()).fetch_optional(&mut *self.transaction).await?;
            if let Some(existing) = existing {
                if existing.try_get::<Vec<u8>, _>("fingerprint")? != fingerprint {
                    return Err(Error::IdentityConflict);
                }
                ids.push(existing.try_get("id")?);
                continue;
            }
            let id = Uuid::new_v4();
            let inserted: Option<Uuid> = sqlx::query_scalar(
                "INSERT INTO raddle_jobs (id,source,tenant,destination,kind,message_id,envelope,fingerprint,partition_key,available_at,expires_at,max_attempts) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (source,tenant,destination,kind,message_id) DO NOTHING RETURNING id"
            ).bind(id).bind(source).bind(tenant).bind(&delivery.destination).bind(delivery.kind.as_str()).bind(envelope.id()).bind(&bytes).bind(&fingerprint).bind(&delivery.partition_key).bind(delivery.available_at).bind(delivery.expires_at).bind(delivery.max_attempts as i32).fetch_optional(&mut *self.transaction).await?;
            if let Some(id) = inserted {
                added = true;
                ids.push(id);
                continue;
            }
            let existing = sqlx::query("SELECT id,fingerprint FROM raddle_jobs WHERE source=$1 AND tenant=$2 AND destination=$3 AND kind=$4 AND message_id=$5")
                .bind(source).bind(tenant).bind(&delivery.destination).bind(delivery.kind.as_str()).bind(envelope.id()).fetch_one(&mut *self.transaction).await?;
            if existing.try_get::<Vec<u8>, _>("fingerprint")? != fingerprint {
                return Err(Error::IdentityConflict);
            }
            ids.push(existing.try_get("id")?);
        }
        let pending: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM raddle_jobs WHERE tenant=$1 AND status IN ('ready','active')",
        )
        .bind(tenant)
        .fetch_one(&mut *self.transaction)
        .await?;
        let completing = i64::from(
            self.claim
                .as_ref()
                .is_some_and(|claim| claim.tenant() == tenant),
        );
        if added && pending - completing > i64::from(capacity) {
            return Err(Error::Capacity);
        }
        Ok(ids)
    }

    /// Commits business writes, staged effects, and optional inbox completion together.
    ///
    /// An I/O failure during commit has an uncertain outcome. Reconcile by message
    /// identity; never infer that it is safe to repeat external actions.
    pub async fn commit(mut self, now: DateTime<Utc>) -> Result<(), Error> {
        if self.poisoned {
            self.transaction.rollback().await?;
            return Err(Error::RollbackOnly);
        }
        if let Some(claim) = &self.claim {
            let result = sqlx::query("UPDATE raddle_jobs SET status='completed',completed_at=$4,owner=NULL,lease_until=NULL,last_error=NULL WHERE id=$1 AND owner=$2 AND generation=$3 AND status='active'")
                .bind(claim.id).bind(claim.owner).bind(claim.generation).bind(now).execute(&mut *self.transaction).await?;
            if result.rows_affected() != 1 {
                self.transaction.rollback().await?;
                return Err(Error::StaleClaim);
            }
        }
        self.transaction.commit().await?;
        Ok(())
    }

    /// Explicitly rolls back all application and messaging effects.
    pub async fn rollback(self) -> Result<(), Error> {
        Ok(self.transaction.rollback().await?)
    }
}
