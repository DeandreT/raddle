use std::time::Duration;

use chrono::{DateTime, Utc};
use raddle::wire::Envelope;
use raddle_postgres::Scope;
use sqlx::Row;
use uuid::Uuid;

use super::{BatchClaim, BatchStore, Error};
use crate::{BatchKey, BatchView, Decision, Member, MemberContext, Resolution, ResolutionError};

/// Atomic SQLx execution with original-member context and a held batch fence.
/// Dropping at any await rolls back; the batch remains recoverable by lease.
pub struct Execution {
    scope: Scope<'static>,
    claim: BatchClaim,
    members: Vec<Member<Envelope>>,
    key: BatchKey,
    probing: bool,
}

/// Successfully committed settlement, including quarantine writes that could not
/// persist and therefore remain pending. Never reports those originals as dead.
#[derive(Debug, Default)]
pub struct SettlementReport {
    /// Terminal committed originals in this transaction.
    pub committed: Vec<Uuid>,
    /// Terminal quarantined originals in this transaction.
    pub quarantined: Vec<Uuid>,
    /// Quarantine persistence failed; these originals remain retryable.
    pub quarantine_failed: Vec<Uuid>,
}

impl BatchStore {
    /// Fences an entire non-probing execution before application writes. SQLx row
    /// locks continue fencing takeover while the business transaction is alive,
    /// even if wall time passes the lease. Bounds SQL statement/lock waits too.
    pub async fn execution(
        &self,
        claim: &BatchClaim,
        now: DateTime<Utc>,
        timeout: Duration,
    ) -> Result<Execution, Error> {
        self.begin_execution(claim, now, timeout, false)
            .await?
            .ok_or(Error::Stale)
    }
    /// Continues the durable one-time sweep. One coalesced key is a single input
    /// unit but retains every original custody participant. None means no probes
    /// remain. Failed probes must be recorded with Retry after rolling back their
    /// business transaction; use `run_once` for that safe execution sequence.
    pub async fn probe(
        &self,
        claim: &BatchClaim,
        now: DateTime<Utc>,
        timeout: Duration,
    ) -> Result<Option<Execution>, Error> {
        self.begin_execution(claim, now, timeout, true).await
    }
    async fn begin_execution(
        &self,
        claim: &BatchClaim,
        now: DateTime<Utc>,
        timeout: Duration,
        probe: bool,
    ) -> Result<Option<Execution>, Error> {
        let clock = super::EffectiveClock::new(now);
        let millis = super::execution_timeout(timeout)?;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "SELECT set_config('statement_timeout',$1,true),set_config('lock_timeout',$1,true)",
        )
        .bind(format!("{millis}ms"))
        .execute(&mut *tx)
        .await?;
        let batch = sqlx::query("SELECT probing,group_key,expires_at,lease_until FROM raddle_batches WHERE id=$1 AND owner=$2 AND generation=$3 AND status='active' FOR UPDATE")
            .bind(claim.id).bind(claim.owner).bind(claim.generation).fetch_optional(&mut *tx).await?.ok_or(Error::Stale)?;
        let lease_until: DateTime<Utc> = batch.try_get("lease_until")?;
        if lease_until <= clock.now()? {
            return Err(Error::Stale);
        }
        let rows =
            super::custody::lock_members(&mut tx, claim.id, super::custody::MemberRead::Input)
                .await?;
        // Both the pool acquisition and either row-lock wait can consume the
        // lease. The logical UTC origin remains the caller's deterministic one.
        let now = clock.now()?;
        if lease_until <= now {
            return Err(Error::Stale);
        }
        let expiry: Option<DateTime<Utc>> = batch.try_get("expires_at")?;
        if expiry.is_some_and(|expiry| expiry <= now) {
            super::custody::quarantine(&mut tx, claim.id, &rows, now, true).await?;
            let counts = super::custody::counts(&mut tx, claim.id).await?;
            super::custody::release(&mut tx, claim.id, &counts, now).await?;
            tx.commit().await.map_err(Error::CustodyUncertain)?;
            return Err(if counts.pending == 0 {
                Error::Expired
            } else {
                Error::QuarantinePending
            });
        }
        let mut probing: bool = batch.try_get("probing")?;
        let counts = super::custody::counts(&mut tx, claim.id).await?;
        if probing && counts.unprobed == 0 && probe {
            super::custody::release(&mut tx, claim.id, &counts, now).await?;
            tx.commit().await.map_err(Error::CustodyUncertain)?;
            return Ok(None);
        }
        if probing && counts.unprobed == 0 {
            probing = false;
            sqlx::query("UPDATE raddle_batches SET probing=false WHERE id=$1")
                .bind(claim.id)
                .execute(&mut *tx)
                .await?;
        }
        if counts.executable == 0 {
            super::custody::release(&mut tx, claim.id, &counts, now).await?;
            tx.commit().await.map_err(Error::CustodyUncertain)?;
            return Err(Error::QuarantinePending);
        }
        let key = BatchKey::new(claim.tenant.clone(), batch.try_get("group_key")?)
            .map_err(Error::Invalid)?;
        if probing != probe {
            return Err(if probing {
                Error::Probing
            } else {
                Error::Invalid("not in probe mode")
            });
        }
        let rows = rows
            .into_iter()
            .map(|row| {
                let eligible = row.try_get::<&str, _>("status")? == "active"
                    && row.try_get::<Option<Uuid>, _>("owner")? == Some(claim.id)
                    && !super::custody::deferred(row.try_get("last_error")?);
                Ok(eligible.then_some(row))
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let unit = if probe {
            let Some(row) = rows
                .iter()
                .find(|row| row.try_get::<bool, _>("probed").is_ok_and(|probed| !probed))
            else {
                return Ok(None);
            };
            Some((
                row.try_get::<Uuid, _>("id")?,
                row.try_get::<Option<String>, _>("coalesce_key")?,
            ))
        } else {
            None
        };
        let mut members = Vec::new();
        for row in rows {
            let job_id: Uuid = row.try_get("id")?;
            let coalesce = row.try_get::<Option<String>, _>("coalesce_key")?;
            if let Some((id, key)) = &unit
                && job_id != *id
                && (key.is_none() || coalesce != *key)
            {
                continue;
            }
            let value =
                Envelope::from_slice(&row.try_get::<Vec<u8>, _>("envelope")?, Default::default())?;
            members.push(Member {
                context: MemberContext {
                    delivery_id: job_id,
                    id: value.id(),
                    attempt: row.try_get::<i32, _>("attempts")? as u32,
                    sent_at: value.created_at(),
                    expires_at: row.try_get("expires_at")?,
                },
                value,
                coalesce,
            });
        }
        Ok(Some(Execution {
            scope: Scope::enroll(tx),
            claim: claim.clone(),
            members,
            key,
            probing,
        }))
    }
}

impl Execution {
    /// All original contexts in this execution (not synthetic batch context).
    pub fn members(&self) -> &[Member<Envelope>] {
        &self.members
    }
    /// Coalesced inputs; every hidden original remains in `members`.
    pub fn input(&self) -> Vec<&Member<Envelope>> {
        crate::assembly::survivors(&self.members)
    }
    /// Disjoint borrows for a typed handler/custom shape and its transaction.
    /// This scope intentionally has no single incoming-envelope identity: use
    /// the original contexts instead of pretending a batch is a single message.
    pub fn parts(&mut self) -> (BatchView<'_, Envelope>, &mut Scope<'static>) {
        (BatchView::new(&self.key, &self.members), &mut self.scope)
    }
    /// Rolls back business writes before failure classification or probing.
    pub async fn rollback(self) -> Result<(), Error> {
        self.scope.rollback().await.map_err(|error| match error {
            raddle_postgres::Error::Database(error) => Error::Database(error),
            _ => Error::Invalid("rollback failed"),
        })
    }
    /// Commits business writes and terminal member states together. Unnamed retry
    /// participants retain their canonical pending jobs and membership. Poison
    /// writes use a savepoint per coalesced unit: a failed DLQ write cannot lose
    /// that unit or prevent unrelated successful units from committing.
    ///
    /// The application must return Commit only for effects performed in THIS
    /// scope, and perform no business effects for Retry/Poison originals. A named
    /// resolution is not evidence that an external side effect is transactional.
    pub async fn settle(
        mut self,
        resolution: Resolution,
        retry_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<SettlementReport, Error> {
        let checked = Resolution::named(
            &self.members,
            resolution
                .decisions()
                .iter()
                .map(|(id, decision)| (*id, decision.clone())),
        )?;
        if checked.decisions() != resolution.decisions() {
            return Err(Error::Resolution(ResolutionError::Unknown));
        }
        let mut report = SettlementReport::default();
        let mut visited = std::collections::BTreeSet::new();
        for member in &self.members {
            let id = member.context.delivery_id;
            if !visited.insert(id) {
                continue;
            }
            let unit: Vec<_> = self
                .members
                .iter()
                .filter(|other| {
                    other.context.delivery_id == id
                        || member
                            .coalesce
                            .as_ref()
                            .is_some_and(|key| other.coalesce.as_ref() == Some(key))
                })
                .map(|other| other.context.delivery_id)
                .collect();
            visited.extend(unit.iter().copied());
            let decision = resolution
                .decisions()
                .get(&id)
                .ok_or(Error::Resolution(ResolutionError::Unknown))?;
            if matches!(decision, Decision::Poison(_)) {
                sqlx::query("SAVEPOINT batch_quarantine")
                    .execute(self.scope.connection())
                    .await?;
            }
            let mut failure = None;
            for original in &unit {
                let result = match decision {
                    Decision::Commit => sqlx::query("UPDATE raddle_jobs j SET status='completed',completed_at=$3,owner=NULL,lease_until=NULL,last_error=NULL FROM raddle_batch_members m WHERE m.job_id=j.id AND m.batch_id=$1 AND j.id=$2 AND j.status='active' AND j.owner=$1")
                        .bind(self.claim.id).bind(original).bind(now).execute(self.scope.connection()).await,
                    Decision::Poison(code) => sqlx::query("UPDATE raddle_jobs j SET status='dead',owner=NULL,lease_until=NULL,last_error=$3 FROM raddle_batch_members m WHERE m.job_id=j.id AND m.batch_id=$1 AND j.id=$2 AND j.status='active' AND j.owner=$1")
                        .bind(self.claim.id).bind(original).bind(code).execute(self.scope.connection()).await,
                    Decision::Retry => sqlx::query("UPDATE raddle_jobs j SET last_error='batch-retry' FROM raddle_batch_members m WHERE m.job_id=j.id AND m.batch_id=$1 AND j.id=$2 AND j.status='active' AND j.owner=$1")
                        .bind(self.claim.id).bind(original).execute(self.scope.connection()).await,
                };
                match result {
                    Ok(result) if result.rows_affected() == 1 => {}
                    Ok(_) => {
                        failure = Some(sqlx::Error::RowNotFound);
                        break;
                    }
                    Err(error) => {
                        failure = Some(error);
                        break;
                    }
                }
            }
            if matches!(decision, Decision::Poison(_)) {
                if failure.is_some() {
                    sqlx::query("ROLLBACK TO SAVEPOINT batch_quarantine")
                        .execute(self.scope.connection())
                        .await?;
                    report.quarantine_failed.extend(&unit);
                } else {
                    report.quarantined.extend(&unit);
                }
                sqlx::query("RELEASE SAVEPOINT batch_quarantine")
                    .execute(self.scope.connection())
                    .await?;
            } else if let Some(error) = failure {
                return Err(Error::Database(error));
            } else if matches!(decision, Decision::Commit) {
                report.committed.extend(&unit);
            }
            if self.probing {
                sqlx::query("UPDATE raddle_batch_members m SET probed=true FROM raddle_jobs j WHERE j.id=m.job_id AND m.batch_id=$1 AND j.id=ANY($2)")
                    .bind(self.claim.id).bind(&unit).execute(self.scope.connection()).await?;
            }
        }
        let counts = super::custody::counts(self.scope.connection(), self.claim.id).await?;
        let continue_probe = self.probing && counts.unprobed > 0;
        if continue_probe {
            sqlx::query("UPDATE raddle_batches SET available_at=$2 WHERE id=$1")
                .bind(self.claim.id)
                .bind(retry_at)
                .execute(self.scope.connection())
                .await?;
        } else {
            super::custody::release(self.scope.connection(), self.claim.id, &counts, retry_at)
                .await?;
        }
        match self.scope.commit(now).await {
            Ok(()) => {}
            Err(raddle_postgres::Error::RollbackOnly) => {
                return Err(Error::Invalid("transaction is rollback-only"));
            }
            Err(error) => return Err(Error::CommitUncertain(error)),
        }
        Ok(report)
    }
}
