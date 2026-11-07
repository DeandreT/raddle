//! Bounded, tenant-scoped operational views with no message bodies or private errors.

use std::time::Duration;

use serde::Serialize;
use sqlx::Row;

use crate::{Error, Store};

/// A single tenant's current durable queue counts, without high-cardinality labels.
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
pub struct QueueStatistics {
    /// Available or scheduled deliveries waiting for an attempt.
    pub ready: i64,
    /// Claimed deliveries, including those whose lease needs recovery.
    pub active: i64,
    /// Quarantined deliveries awaiting explicit operator action.
    pub dead: i64,
    /// Completed deduplication tombstones.
    pub completed: i64,
    /// Ready work whose due time has passed, excluding expired messages.
    pub due: i64,
}

impl Store {
    /// Verifies a database round trip within two seconds; does not claim broker health.
    ///
    /// A failure is deliberately reduced to an unavailable result. Credentials,
    /// connection details and server errors are not part of this operational view.
    pub async fn is_available(&self) -> bool {
        matches!(
            tokio::time::timeout(
                Duration::from_secs(2),
                sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&self.pool)
            )
            .await,
            Ok(Ok(1))
        )
    }

    /// Reads one trusted tenant's payload-free counts with a two-second query budget.
    ///
    /// The caller must authorize the tenant independently. No cross-tenant
    /// enumeration or per-message metrics labels are exposed by this method.
    pub async fn statistics(&self, tenant: &str) -> Result<QueueStatistics, Error> {
        tokio::time::timeout(Duration::from_secs(2), self.statistics_inner(tenant))
            .await
            .map_err(|_| Error::Database(sqlx::Error::PoolTimedOut))?
    }

    async fn statistics_inner(&self, tenant: &str) -> Result<QueueStatistics, Error> {
        if tenant.is_empty() || tenant.len() > 128 {
            return Err(Error::Invalid("invalid tenant"));
        }
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *transaction)
            .await?;
        let row = sqlx::query("SELECT count(*) FILTER(WHERE status='ready') AS ready,count(*) FILTER(WHERE status='active') AS active,count(*) FILTER(WHERE status='dead') AS dead,count(*) FILTER(WHERE status='completed') AS completed,count(*) FILTER(WHERE status='ready' AND available_at<=clock_timestamp() AND (expires_at IS NULL OR expires_at>clock_timestamp())) AS due FROM raddle_jobs WHERE tenant=$1")
            .bind(tenant).fetch_one(&mut *transaction).await?;
        let view = QueueStatistics {
            ready: row.try_get("ready")?,
            active: row.try_get("active")?,
            dead: row.try_get("dead")?,
            completed: row.try_get("completed")?,
            due: row.try_get("due")?,
        };
        transaction.commit().await?;
        Ok(view)
    }
}
