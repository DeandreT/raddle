use std::time::Duration;

use sqlx::Row;
use uuid::Uuid;

use super::{BatchStore, Error};

/// Exact canonical delivery identities a caller wants to await, across retries.
/// Owns no worker or transaction, and does not upgrade wire metadata to authority.
pub struct TrackingSession {
    tenant: String,
    ids: Vec<Uuid>,
}

/// Terminal durable outcomes. Only completed originals represent business success.
#[derive(Debug, Default)]
pub struct TrackedOutcome {
    /// Successfully committed originals.
    pub completed: Vec<Uuid>,
    /// Quarantined, expired or exhausted originals.
    pub dead: Vec<Uuid>,
    /// Originals explicitly retired by an operator, not completed or quarantined.
    /// Discard is terminal but is not successful business execution.
    pub discarded: Vec<Uuid>,
}

impl TrackingSession {
    /// Tracks 1..=256 unique delivery ids within one authenticated tenant.
    /// A missing/pruned identity is an error, never inferred success.
    pub fn new(tenant: impl Into<String>, ids: Vec<Uuid>) -> Result<Self, Error> {
        let tenant = tenant.into();
        let unique: std::collections::BTreeSet<_> = ids.iter().collect();
        if tenant.is_empty()
            || tenant.len() > 128
            || ids.is_empty()
            || ids.len() > 256
            || unique.len() != ids.len()
            || ids.iter().any(Uuid::is_nil)
        {
            return Err(Error::Invalid("tracking identities"));
        }
        Ok(Self { tenant, ids })
    }
    /// Drives bounded read-only observation until every original is durably
    /// terminal. Applications must independently drive assembly/workers. Cancel
    /// or timeout this future without changing custody or stopping those workers.
    /// Poll interval is 1ms..=1s and total budget 1ms..=1h; no task is spawned.
    pub async fn wait(
        &self,
        store: &BatchStore,
        interval: Duration,
        budget: Duration,
    ) -> Result<TrackedOutcome, Error> {
        if !(Duration::from_millis(1)..=Duration::from_secs(1)).contains(&interval)
            || !(Duration::from_millis(1)..=Duration::from_secs(3600)).contains(&budget)
        {
            return Err(Error::Invalid("tracking wait bounds"));
        }
        let wait = async {
            let mut ticks = tokio::time::interval(interval);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticks.tick().await;
                let rows =
                    sqlx::query("SELECT id,status FROM raddle_jobs WHERE tenant=$1 AND id=ANY($2)")
                        .bind(&self.tenant)
                        .bind(&self.ids)
                        .fetch_all(&store.pool)
                        .await?;
                if rows.len() != self.ids.len() {
                    return Err(Error::Invalid("tracked custody missing"));
                }
                let mut outcome = TrackedOutcome::default();
                for row in rows {
                    match row.try_get::<&str, _>("status")? {
                        "completed" => outcome.completed.push(row.try_get("id")?),
                        "dead" => outcome.dead.push(row.try_get("id")?),
                        "discarded" => outcome.discarded.push(row.try_get("id")?),
                        "ready" | "active" => {}
                        _ => return Err(Error::Invalid("persisted job status")),
                    }
                }
                if outcome.completed.len() + outcome.dead.len() + outcome.discarded.len()
                    == self.ids.len()
                {
                    return Ok(outcome);
                }
            }
        };
        tokio::time::timeout(budget, wait)
            .await
            .map_err(|_| Error::TrackingTimeout)?
    }
}
