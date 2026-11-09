use chrono::{DateTime, Utc};
use raddle::wire::Envelope;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

use super::config::quote;
use super::source::Input;
use super::{Error, Listener, connection};
use crate::Scope;

/// Observed outcomes, not business completions. Deferred rows remain at source.
#[derive(Default, Debug)]
pub struct PollReport {
    /// Candidates discovered within the finite sweep.
    pub scanned: usize,
    /// New canonical custody committed.
    pub accepted: usize,
    /// Equivalent accepted input removed without another delivery.
    pub duplicates: usize,
    /// Raw quarantine committed, including retained source references.
    pub quarantined: usize,
    /// Row-local refusal/failure; source retained for a later sweep.
    pub deferred: usize,
    /// Another poller holds the configured advisory lock.
    pub busy: bool,
}

pub(super) enum Outcome {
    Accepted,
    Duplicate,
    Quarantined,
    Deferred,
}

impl Listener {
    /// Runs a bounded round on one exclusively checked-out connection. No tasks
    /// or DDL. Cancellation closes that connection, releasing its session lock;
    /// a commit I/O error is uncertain, never evidence of successful admission.
    pub async fn poll_once(&self, now: DateTime<Utc>) -> Result<PollReport, Error> {
        tokio::time::timeout(self.options.operation_timeout, self.poll(now))
            .await
            .map_err(|_| Error::Timeout)?
    }

    async fn poll(&self, now: DateTime<Utc>) -> Result<PollReport, Error> {
        let mut connection = connection::acquire(&self.store.pool, &self.options).await?;
        self.validate_on(&mut connection).await?;
        if !connection::lock(&mut connection, self.options.advisory_lock).await? {
            return Ok(PollReport {
                busy: true,
                ..Default::default()
            });
        }
        let result = self.poll_locked(&mut connection, now).await;
        connection::unlock(&mut connection, self.options.advisory_lock).await?;
        connection.close().await?;
        result
    }

    async fn poll_locked(
        &self,
        connection: &mut PgConnection,
        now: DateTime<Utc>,
    ) -> Result<PollReport, Error> {
        // A second per-source fence also coordinates applications accidentally
        // configured with different session advisory keys.
        let ids = self.discover(connection).await?;
        let mut report = PollReport {
            scanned: ids.len(),
            ..Default::default()
        };
        for id in ids {
            let prepared = self.prepare(connection, id, now).await;
            // Cursor is independent of custody, including trigger/receipt faults.
            self.advance(connection, id).await?;
            let result = match prepared {
                Ok(true) => self.ingest(connection, id, now).await,
                Ok(false) => Ok(Outcome::Deferred),
                Err(error) => Err(error),
            };
            match result {
                Ok(Outcome::Accepted) => report.accepted += 1,
                Ok(Outcome::Duplicate) => report.duplicates += 1,
                Ok(Outcome::Quarantined) => report.quarantined += 1,
                Ok(Outcome::Deferred) => report.deferred += 1,
                Err(error) if !connection::shared(&error) => report.deferred += 1,
                Err(error) => return Err(error),
            }
        }
        Ok(report)
    }

    async fn discover(&self, connection: &mut PgConnection) -> Result<Vec<Uuid>, Error> {
        let mut tx = connection.begin().await?;
        self.bind_source(&mut tx).await?;
        let row = sqlx::query("SELECT cursor,high_water FROM raddle_external_sources WHERE tenant=$1 AND source=$2 FOR UPDATE")
            .bind(&self.config.tenant).bind(&self.config.source).fetch_one(&mut *tx).await?;
        let mut cursor: Option<Uuid> = row.try_get("cursor")?;
        let mut high: Option<Uuid> = row.try_get("high_water")?;
        let key = quote(&self.config.id_column)?;
        let table = self.config.qualified()?;
        let query = format!(
            "SELECT t.{key} FROM {table} t WHERE ($1::uuid IS NULL OR t.{key}>$1) AND t.{key}<=$2 AND NOT EXISTS(SELECT 1 FROM raddle_external_inputs i WHERE i.tenant=$3 AND i.source=$4 AND i.input_id=t.{key} AND i.source_retained) ORDER BY t.{key} LIMIT $5"
        );
        let mut ids = Vec::new();
        for _ in 0..2 {
            if high.is_none() {
                high = sqlx::query_scalar(&format!(
                    "SELECT {key} FROM {table} ORDER BY {key} DESC LIMIT 1"
                ))
                .fetch_optional(&mut *tx)
                .await?;
            }
            ids = sqlx::query_scalar(&query)
                .bind(cursor)
                .bind(high)
                .bind(&self.config.tenant)
                .bind(&self.config.source)
                .bind(i64::from(self.options.batch_size))
                .fetch_all(&mut *tx)
                .await?;
            if !ids.is_empty() {
                break;
            }
            cursor = None;
            high = None;
        }
        sqlx::query("UPDATE raddle_external_sources SET cursor=$3,high_water=$4 WHERE tenant=$1 AND source=$2")
            .bind(&self.config.tenant).bind(&self.config.source).bind(cursor).bind(high).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(ids)
    }

    async fn advance(&self, connection: &mut PgConnection, id: Uuid) -> Result<(), Error> {
        sqlx::query("UPDATE raddle_external_sources SET cursor=$3 WHERE tenant=$1 AND source=$2")
            .bind(&self.config.tenant)
            .bind(&self.config.source)
            .bind(id)
            .execute(connection)
            .await?;
        Ok(())
    }

    // First durable observation is not canonical custody or acknowledgement.
    // Retaining its time separately lets admission/quarantine failures retry
    // without inventing a different canonical creation time.
    async fn prepare(
        &self,
        connection: &mut PgConnection,
        id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<bool, Error> {
        let mut tx = connection.begin().await?;
        let Some(input) = self.select(&mut tx, id).await? else {
            return Ok(false);
        };
        self.observe(&mut tx, &input, now).await?;
        tx.commit().await?;
        Ok(true)
    }

    async fn ingest(
        &self,
        connection: &mut PgConnection,
        id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Outcome, Error> {
        let mut scope = Scope::enroll(connection.begin().await?);
        let result = self.ingest_scoped(&mut scope, id, now).await;
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
    }

    async fn ingest_scoped(
        &self,
        scope: &mut Scope<'_>,
        id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<Outcome, Error> {
        let Some(input) = self.select(scope.connection(), id).await? else {
            return Ok(Outcome::Deferred);
        };
        let row = sqlx::query("SELECT first_seen,status FROM raddle_external_inputs WHERE tenant=$1 AND source=$2 AND input_id=$3 AND fingerprint=$4 FOR UPDATE")
            .bind(&self.config.tenant).bind(&self.config.source).bind(id).bind(&input.fingerprint)
            .fetch_optional(scope.connection()).await?;
        let Some(row) = row else {
            return Ok(Outcome::Deferred);
        };
        let state: String = row.try_get("status")?;
        // A complete pending snapshot already owns this raw input. Repeated
        // producer inserts (including a changed diagnostic timestamp) neither
        // consume another quarantine slot nor bypass explicit operator replay.
        let pending_snapshot: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raddle_external_inputs WHERE tenant=$1 AND source=$2 AND input_id=$3 AND content_hash=$4 AND status='quarantined' AND NOT source_retained)")
            .bind(&self.config.tenant).bind(&self.config.source).bind(id).bind(&input.content_hash)
            .fetch_one(scope.connection()).await?;
        if pending_snapshot {
            self.delete(scope.connection(), id).await?;
            return Ok(Outcome::Quarantined);
        }
        if state == "quarantined" {
            return Ok(Outcome::Quarantined);
        }
        // A producer can redeliver the original poison after an operator has
        // corrected it. Its complete snapshot/audit already owns that input;
        // never run the correction or the successful business effects again.
        let resolved: Option<Vec<u8>> = sqlx::query_scalar("SELECT q.canonical_hash FROM raddle_external_inputs q JOIN raddle_external_inputs a ON a.tenant=q.tenant AND a.source=q.source AND a.input_id=q.input_id AND a.status IN ('accepted','replayed') AND a.canonical_hash=q.canonical_hash WHERE q.tenant=$1 AND q.source=$2 AND q.input_id=$3 AND q.content_hash=$4 AND q.status IN ('replayed','resolved') AND NOT q.source_retained LIMIT 1")
            .bind(&self.config.tenant).bind(&self.config.source).bind(id).bind(&input.content_hash)
            .fetch_optional(scope.connection()).await?;
        if let Some(hash) = resolved {
            self.delete(scope.connection(), id).await?;
            if state == "observed" {
                self.record_acceptance(scope.connection(), &input, &hash, "duplicate")
                    .await?;
            }
            return Ok(Outcome::Duplicate);
        }
        let time = row.try_get("first_seen")?;
        let envelope = match self.decode(&input, time) {
            Ok(envelope) => envelope,
            Err(code) => return self.quarantine(scope.connection(), &input, code).await,
        };
        let hash = self.canonical_hash(&envelope)?;
        let existing = self.accepted(scope.connection(), id).await?;
        if let Some((content, canonical)) = existing {
            if content != input.content_hash || canonical != hash {
                return self
                    .quarantine(scope.connection(), &input, "identity-conflict")
                    .await;
            }
            self.delete(scope.connection(), id).await?;
            if state == "observed" {
                self.record_acceptance(scope.connection(), &input, &hash, "duplicate")
                    .await?;
            }
            return Ok(Outcome::Duplicate);
        }
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
        self.record_acceptance(scope.connection(), &input, &hash, "accepted")
            .await?;
        self.delete(scope.connection(), id).await?;
        Ok(Outcome::Accepted)
    }

    pub(super) fn decode(
        &self,
        input: &Input,
        time: DateTime<Utc>,
    ) -> Result<Envelope, &'static str> {
        if input
            .body_bytes
            .is_some_and(|n| n > i64::from(self.options.body_bytes))
            || input
                .alias_bytes
                .is_some_and(|n| n > i64::from(self.options.alias_bytes))
        {
            return Err("oversized-input");
        }
        let body = input.body.as_deref().ok_or("missing-body")?;
        let alias = input
            .alias
            .as_deref()
            .filter(|a| !a.is_empty())
            .or(self.config.default_alias.as_deref())
            .ok_or("missing-alias")?;
        let decoder = self.aliases.entries.get(alias).ok_or("unknown-alias")?;
        let value: Value = serde_json::from_str(body).map_err(|_| "invalid-body")?;
        let protected = |v: &Value| {
            v.get("format")
                .and_then(Value::as_str)
                .is_some_and(|s| s.starts_with("raddle-aes"))
        };
        let protected_outer = ["id", "contract", "version", "created_at"]
            .iter()
            .all(|key| value.get(key).is_some())
            && value.get("payload").is_some_and(protected);
        if protected(&value)
            || protected_outer
            || Envelope::from_slice(body.as_bytes(), Default::default()).is_ok()
            || value.as_str().is_some_and(|s| s.starts_with("RDSB1"))
        {
            return Err("unsupported-wire-framing");
        }
        // Only the synchronous, owned pre-custody projection is inside this
        // unwind boundary. No Scope or partially committed row state is reused.
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            decoder(input.id, time, value)
        }))
        .map_err(|_| "decoder-panic")?
    }

    pub(super) fn canonical_hash(&self, envelope: &Envelope) -> Result<Vec<u8>, Error> {
        let mut hash = Sha256::new();
        hash.update(envelope.to_vec().map_err(crate::Error::Wire)?);
        hash.update(
            serde_json::to_vec(&(
                &self.config.delivery.destination,
                self.config.delivery.kind.as_str(),
                self.config.delivery.expires_at,
                &self.config.delivery.partition_key,
                self.config.delivery.max_attempts,
            ))
            .map_err(|_| Error::Invalid("invalid delivery policy"))?,
        );
        Ok(hash.finalize().to_vec())
    }

    pub(super) async fn accepted(
        &self,
        connection: &mut PgConnection,
        id: Uuid,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        let row=sqlx::query("SELECT coalesce(replay_hash,content_hash) AS content_hash,canonical_hash FROM raddle_external_inputs WHERE tenant=$1 AND source=$2 AND input_id=$3 AND status IN ('accepted','replayed')")
            .bind(&self.config.tenant).bind(&self.config.source).bind(id).fetch_optional(connection).await?;
        row.map(|r| Ok((r.try_get("content_hash")?, r.try_get("canonical_hash")?)))
            .transpose()
    }

    async fn record_acceptance(
        &self,
        connection: &mut PgConnection,
        input: &Input,
        hash: &[u8],
        state: &str,
    ) -> Result<(), Error> {
        let changed=sqlx::query("UPDATE raddle_external_inputs SET status=$5,canonical_hash=$6 WHERE tenant=$1 AND source=$2 AND input_id=$3 AND fingerprint=$4 AND status='observed'")
            .bind(&self.config.tenant).bind(&self.config.source).bind(input.id).bind(&input.fingerprint)
            .bind(state).bind(hash).execute(connection).await?;
        if changed.rows_affected() != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}
