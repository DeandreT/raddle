use chrono::{DateTime, Utc};
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

use super::config::bounded;
use super::poll::Outcome;
use super::source::Input;
use super::{Error, Listener, connection};
use crate::Scope;

/// One failed source occurrence, scoped to this listener's authorized tenant.
/// Contains private data; deliberately has no payload-bearing Debug implementation.
pub struct QuarantinedInput {
    /// Stable occurrence identity for corrected replay.
    pub sequence: i64,
    /// Original external UUID, not an authorization claim.
    pub input_id: Uuid,
    /// Persisted native canonical creation time, independent of source diagnostics.
    pub first_seen: DateTime<Utc>,
    /// Bounded original diagnostic timestamp text, including null/infinity.
    pub diagnostic_time: Option<String>,
    /// Bounded classification, never a formatted application error.
    pub code: String,
    /// Complete stored JSON text within the current body budget; none for SQL null,
    /// an in-place reference, or a snapshot exceeding the current budget.
    pub body: Option<String>,
    /// Complete original alias within the current alias budget; otherwise none.
    pub alias: Option<String>,
    /// Original full stored-value fingerprint, not original producer JSON bytes.
    pub fingerprint: Vec<u8>,
    /// Original source body length, or none for SQL null.
    pub body_bytes: Option<i64>,
    /// Original source alias length, or none for SQL null/no alias column.
    pub alias_bytes: Option<i64>,
    /// The complete original remains at source; it has not been deleted/acknowledged.
    pub source_retained: bool,
    /// The complete stored snapshot fits both current transfer budgets and is not
    /// an in-place reference. False leaves the original evidence untouched and
    /// requires an explicit bounded correction for replay.
    pub snapshot_available: bool,
}

/// Bounded failed-only page. Payload access needs independent application authorization.
pub struct QuarantinePage {
    /// At most ten occurrences, with bounded complete snapshots or references.
    pub items: Vec<QuarantinedInput>,
    /// Exclusive continuation cursor; none when no further failed occurrence exists.
    pub next_after: Option<i64>,
}

/// Explicit raw-input correction, not an untyped canonical envelope.
/// Typed decoding and protected-root rejection still run before custody.
pub struct Correction {
    /// JSON application body, checked before parsing against the transfer bound.
    pub body: String,
    /// Registered external type alias; none uses the explicitly configured default.
    pub alias: Option<String>,
}

/// Successful replay outcome. An error never establishes a COMMIT outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplayOutcome {
    /// This failed occurrence gained new canonical custody and a correction audit.
    Accepted,
    /// Corrected content matched existing accepted work; no work was restaged.
    Reconciled,
    /// The occurrence was already resolved, without another staging/audit operation.
    AlreadyResolved,
    /// No such occurrence exists within the bound tenant/source.
    NotFound,
    /// The failed original is a reference or exceeds current transfer budgets.
    /// Nothing was admitted/resolved; supply an explicit bounded correction.
    Unavailable,
}

impl Listener {
    pub(super) async fn quarantine(
        &self,
        connection: &mut PgConnection,
        input: &Input,
        code: &str,
    ) -> Result<Outcome, Error> {
        // Source state serializes budget admission even across different advisory keys.
        sqlx::query(
            "SELECT tenant FROM raddle_external_sources WHERE tenant=$1 AND source=$2 FOR UPDATE",
        )
        .bind(&self.config.tenant)
        .bind(&self.config.source)
        .fetch_one(&mut *connection)
        .await?;
        let snapshot = input
            .body_bytes
            .is_none_or(|n| n <= i64::from(self.options.body_bytes))
            && input
                .alias_bytes
                .is_none_or(|n| n <= i64::from(self.options.alias_bytes));
        let bytes = if snapshot {
            input.body_bytes.unwrap_or_default() + input.alias_bytes.unwrap_or_default()
        } else {
            0
        };
        let budget=sqlx::query("SELECT count(*)::bigint AS rows,coalesce(sum(retained_bytes),0)::bigint AS bytes FROM raddle_external_inputs WHERE tenant=$1 AND source=$2 AND code IS NOT NULL")
            .bind(&self.config.tenant).bind(&self.config.source).fetch_one(&mut *connection).await?;
        if budget.try_get::<i64, _>("rows")? >= i64::from(self.options.quarantine_rows)
            || budget.try_get::<i64, _>("bytes")?.saturating_add(bytes)
                > self.options.quarantine_bytes as i64
        {
            return Ok(Outcome::Deferred);
        }
        let changed=sqlx::query("UPDATE raddle_external_inputs SET status='quarantined',code=$5,body=$6,alias=$7,source_retained=$8,retained_bytes=$9 WHERE tenant=$1 AND source=$2 AND input_id=$3 AND fingerprint=$4 AND status='observed'")
            .bind(&self.config.tenant).bind(&self.config.source).bind(input.id).bind(&input.fingerprint)
            .bind(code).bind(if snapshot { input.body.as_deref() } else { None })
            .bind(if snapshot { input.alias.as_deref() } else { None }).bind(!snapshot).bind(bytes)
            .execute(&mut *connection).await?;
        if changed.rows_affected() != 1 {
            return Err(Error::Conflict);
        }
        if snapshot {
            self.delete(connection, input.id).await?;
        }
        Ok(Outcome::Quarantined)
    }

    /// Reads only this explicitly bound tenant/source, with at most ten bounded
    /// payload snapshots. Oversized originals are references, never fetched here.
    pub async fn search_quarantine(&self, after: i64, limit: u32) -> Result<QuarantinePage, Error> {
        if after < 0 || !(1..=10).contains(&limit) {
            return Err(Error::Invalid("invalid quarantine page"));
        }
        tokio::time::timeout(self.options.operation_timeout, async {
            let mut connection=connection::acquire(&self.store.pool,&self.options).await?;
            let mut tx=connection.begin().await?;
            sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *tx).await?;
            let rows=sqlx::query("SELECT sequence,input_id,first_seen,diagnostic_time,code,CASE WHEN octet_length(convert_to(body,'UTF8'))<=$5 THEN body END AS body,CASE WHEN octet_length(convert_to(alias,'UTF8'))<=$6 THEN alias END AS alias,fingerprint,body_bytes,alias_bytes,source_retained,(NOT source_retained AND coalesce(octet_length(convert_to(body,'UTF8')),0)<=$5 AND coalesce(octet_length(convert_to(alias,'UTF8')),0)<=$6) AS snapshot_available FROM raddle_external_inputs WHERE tenant=$1 AND source=$2 AND status='quarantined' AND sequence>$3 ORDER BY sequence LIMIT $4")
                .bind(&self.config.tenant).bind(&self.config.source).bind(after).bind(i64::from(limit))
                .bind(i64::from(self.options.body_bytes)).bind(i64::from(self.options.alias_bytes)).fetch_all(&mut *tx).await?;
            let mut items=Vec::with_capacity(rows.len());
            let mut cursor=after;
            for row in rows {
                cursor=row.try_get("sequence")?;
                items.push(QuarantinedInput { sequence:cursor,input_id:row.try_get("input_id")?,first_seen:row.try_get("first_seen")?,diagnostic_time:row.try_get("diagnostic_time")?,code:row.try_get("code")?,
                    body:row.try_get("body")?,alias:row.try_get("alias")?,fingerprint:row.try_get("fingerprint")?,
                    body_bytes:row.try_get("body_bytes")?,alias_bytes:row.try_get("alias_bytes")?,source_retained:row.try_get("source_retained")?,snapshot_available:row.try_get("snapshot_available")? });
            }
            let more:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raddle_external_inputs WHERE tenant=$1 AND source=$2 AND status='quarantined' AND sequence>$3)")
                .bind(&self.config.tenant).bind(&self.config.source).bind(cursor).fetch_one(&mut *tx).await?;
            tx.commit().await?;
            Ok(QuarantinePage {items,next_after:more.then_some(cursor)})
        }).await.map_err(|_| Error::Timeout)?
    }

    /// Corrects exactly one failed raw occurrence and retains its original evidence
    /// plus actor/reason/time and corrected content hash atomically with custody.
    /// In-place oversized originals remain retained even after correction; this API
    /// does not invent a destructive retention policy. Repeated calls do not restage.
    /// Cancellation rolls back; a COMMIT transport failure requires reconciliation.
    pub async fn replay_quarantined(
        &self,
        sequence: i64,
        correction: Option<&Correction>,
        actor: &str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<ReplayOutcome, Error> {
        if sequence <= 0 || !bounded(actor, 128) || !bounded(reason, 512) {
            return Err(Error::Invalid("invalid raw replay audit"));
        }
        if correction.is_some_and(|c| {
            c.body.len() > self.options.body_bytes as usize
                || c.alias
                    .as_ref()
                    .is_some_and(|a| a.len() > self.options.alias_bytes as usize)
        }) {
            return Err(Error::Invalid("correction exceeds bound"));
        }
        tokio::time::timeout(self.options.operation_timeout, async {
            let mut connection = connection::acquire(&self.store.pool, &self.options).await?;
            let mut scope = Scope::enroll(connection.begin().await?);
            let result = self
                .replay_scoped(&mut scope, sequence, correction, actor, reason, now)
                .await;
            match result {
                Ok(outcome) => {
                    scope.commit(now).await?;
                    Ok(outcome)
                }
                Err(error) => {
                    scope.rollback().await?;
                    Err(error)
                }
            }
        })
        .await
        .map_err(|_| Error::Timeout)?
    }

    async fn replay_scoped(
        &self,
        scope: &mut Scope<'_>,
        sequence: i64,
        correction: Option<&Correction>,
        actor: &str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<ReplayOutcome, Error> {
        let row=sqlx::query("SELECT input_id,fingerprint,content_hash,first_seen,status,CASE WHEN $4::boolean AND status='quarantined' AND octet_length(convert_to(body,'UTF8'))<=$5 THEN body END AS body,CASE WHEN $4::boolean AND status='quarantined' AND octet_length(convert_to(alias,'UTF8'))<=$6 THEN alias END AS alias,body_bytes,alias_bytes,source_retained,(NOT source_retained AND coalesce(octet_length(convert_to(body,'UTF8')),0)<=$5 AND coalesce(octet_length(convert_to(alias,'UTF8')),0)<=$6) AS snapshot_available FROM raddle_external_inputs WHERE tenant=$1 AND source=$2 AND sequence=$3 FOR UPDATE")
            .bind(&self.config.tenant).bind(&self.config.source).bind(sequence).bind(correction.is_none())
            .bind(i64::from(self.options.body_bytes)).bind(i64::from(self.options.alias_bytes)).fetch_optional(scope.connection()).await?;
        let Some(row) = row else {
            return Ok(ReplayOutcome::NotFound);
        };
        // A tenant/source namespace must not be rebound to a lookalike table or
        // changed column projection during recovery, including snapshot replay.
        // This is read-only: recovery does not register a new source or issue DDL.
        self.verify_binding(scope.connection()).await?;
        let state: String = row.try_get("status")?;
        if matches!(state.as_str(), "replayed" | "resolved") {
            return Ok(ReplayOutcome::AlreadyResolved);
        }
        if state != "quarantined" {
            return Ok(ReplayOutcome::NotFound);
        }
        let id: Uuid = row.try_get("input_id")?;
        let fingerprint: Vec<u8> = row.try_get("fingerprint")?;
        if row.try_get::<bool, _>("source_retained")? {
            let Some(original) = self.select_metadata(scope.connection(), id).await? else {
                return Err(Error::Conflict);
            };
            if original.fingerprint != fingerprint {
                return Err(Error::Conflict);
            }
        }
        if correction.is_none() && !row.try_get::<bool, _>("snapshot_available")? {
            return Ok(ReplayOutcome::Unavailable);
        }
        let (body, alias) = match correction {
            Some(c) => (Some(c.body.clone()), c.alias.clone()),
            None => (row.try_get("body")?, row.try_get("alias")?),
        };
        let mut input = Input {
            id,
            fingerprint,
            body,
            alias,
            content_hash: row.try_get("content_hash")?,
            diagnostic_time: None,
            body_bytes: None,
            alias_bytes: None,
        };
        input.body_bytes = input.body.as_ref().map(|v| v.len() as i64);
        input.alias_bytes = input.alias.as_ref().map(|v| v.len() as i64);
        let envelope = self
            .decode(&input, row.try_get("first_seen")?)
            .map_err(Error::Invalid)?;
        let hash = self.canonical_hash(&envelope)?;
        // Reproduce PostgreSQL's stored JSONB representation, not Rust JSON layout.
        let content: Vec<u8> = sqlx::query_scalar(
            "SELECT sha256(convert_to(jsonb_build_array(($1::jsonb) IS NULL,$1::jsonb,$2::varchar)::text,'UTF8'))",
        )
        .bind(&input.body)
        .bind(self.config.alias_column.as_ref().and(input.alias.as_deref()))
        .fetch_one(scope.connection())
        .await?;
        let duplicate = match self.accepted(scope.connection(), id).await? {
            Some((accepted, canonical)) => {
                if accepted != content || canonical != hash {
                    return Err(Error::Conflict);
                }
                true
            }
            None => false,
        };
        if !duplicate {
            let mut delivery = self.config.delivery.clone();
            delivery.available_at = now;
            scope
                .stage_received(
                    &self.config.source,
                    &self.config.tenant,
                    &envelope,
                    &[delivery],
                )
                .await?;
        }
        let changed=sqlx::query("UPDATE raddle_external_inputs SET status=$4,canonical_hash=$5,replay_actor=$6,replay_reason=$7,replayed_at=$8,replay_hash=$9 WHERE tenant=$1 AND source=$2 AND sequence=$3 AND status='quarantined'")
            .bind(&self.config.tenant).bind(&self.config.source).bind(sequence)
            .bind(if duplicate {"resolved"} else {"replayed"}).bind(hash).bind(actor).bind(reason).bind(now).bind(content)
            .execute(scope.connection()).await?;
        if changed.rows_affected() != 1 {
            return Err(Error::Conflict);
        }
        Ok(if duplicate {
            ReplayOutcome::Reconciled
        } else {
            ReplayOutcome::Accepted
        })
    }
}
