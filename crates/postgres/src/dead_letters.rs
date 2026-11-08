//! Bounded trusted-tenant dead-letter reads and atomic audited operator discard.

use std::time::Duration;

use chrono::{DateTime, Utc};
use raddle::wire::Envelope;
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::{Error, Store};

/// A bounded cursor scan; it never enumerates other tenants or scans all payloads.
pub struct DeadLetterQuery {
    /// Number of matching envelopes to return, from one through ten.
    pub limit: u32,
    /// Exclusive sequence cursor, at least zero.
    pub after: i64,
    /// Optional exact destination alias, at most 128 bytes.
    pub destination: Option<String>,
    /// Optional exact wire contract, at most 128 bytes.
    pub contract: Option<String>,
    /// Optional exact classified error, at most 64 bytes.
    pub error_code: Option<String>,
}

/// Failed delivery data; deliberately has no derived payload-bearing Debug output.
pub struct DeadLetter {
    /// Delivery identity used by management operations.
    pub id: Uuid,
    /// Durable pagination sequence.
    pub sequence: i64,
    /// Configured destination alias.
    pub destination: String,
    /// Validated canonical failed message, including private payload and headers.
    pub envelope: Envelope,
}

/// An unreadable candidate identified only by trusted persisted custody metadata.
/// No unvalidated envelope fields, protected bytes, or parser errors are returned.
pub struct UnavailableDeadLetter {
    /// Persisted delivery identity within the caller's authorized tenant.
    pub id: Uuid,
    /// Persisted sequence; pagination advances past this candidate.
    pub sequence: i64,
}

/// Bounded matching page; follow the cursor even if this scan found no matches.
pub struct DeadLetterScan {
    /// Matching data, no more than the requested limit.
    pub items: Vec<DeadLetter>,
    /// Unreadable candidates, including protected framing and identity mismatches.
    /// Together with `items`, never exceeds the candidate limit. These entries
    /// cannot establish a contract match and remain visible with a contract filter.
    pub unavailable: Vec<UnavailableDeadLetter>,
    /// Exclusive continuation cursor or null when the bounded scan reached the end.
    pub next_after: Option<i64>,
}

fn bounded(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

fn readable_envelope(bytes: &[u8], message_id: Uuid) -> Option<Envelope> {
    let envelope = Envelope::from_slice(bytes, Default::default()).ok()?;
    if envelope.id() != message_id {
        return None;
    }
    // Native encryption keeps a canonical outer envelope, so canonical parsing
    // alone is insufficient to identify its protected body. Recognize
    // the codec's reserved framing namespace without decrypting or exposing it.
    let value: Value = serde_json::from_slice(bytes).ok()?;
    if value["payload"]["format"]
        .as_str()
        .is_some_and(|format| format.starts_with("raddle-aes"))
    {
        return None;
    }
    Some(envelope)
}

impl Store {
    /// Replays through normal custody/audit rules with two-second server statement
    /// and lock deadlines local to its transaction. Unlike `replay`, this opt-in
    /// path bounds blocked statements even when its caller drops the future. A
    /// deadline cannot undo committed work or establish a COMMIT outcome.
    pub async fn replay_dead_letter_bounded(
        &self,
        tenant: &str,
        id: Uuid,
        actor: &str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, Error> {
        self.replay_inner(tenant, id, actor, reason, now, Some(Duration::from_secs(2)))
            .await
    }

    /// Reads at most ten candidate envelopes in an explicitly read-only transaction.
    ///
    /// Destination/error filters run in SQL; contract filtering validates canonical
    /// envelopes within the bounded candidate page. This may return an empty page
    /// with a continuation cursor, avoiding an unbounded payload scan. Each envelope
    /// is limited to 1 MiB with at most 32 bounded headers. Corrupt framing,
    /// mismatched envelope identities and reserved protected payloads produce
    /// payload-free `unavailable` entries without blocking healthy candidates.
    /// The cursor advances past every candidate, even unreadable or filtered rows.
    /// Database/query failures remain errors, not per-row payload classifications.
    /// The caller must independently authorize private payload access and the tenant.
    pub async fn search_dead_letters(
        &self,
        tenant: &str,
        query: &DeadLetterQuery,
    ) -> Result<DeadLetterScan, Error> {
        if !bounded(tenant, 128)
            || !(1..=10).contains(&query.limit)
            || query.after < 0
            || query
                .destination
                .as_deref()
                .is_some_and(|v| !bounded(v, 128))
            || query.contract.as_deref().is_some_and(|v| !bounded(v, 128))
            || query.error_code.as_deref().is_some_and(|v| !bounded(v, 64))
        {
            return Err(Error::Invalid("invalid dead-letter query"));
        }
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("SET LOCAL statement_timeout='2s'")
            .execute(&mut *transaction)
            .await?;
        // Fetch only sequence for the lookahead; do not allocate another payload.
        let rows = sqlx::query("SELECT id,message_id,sequence,destination,envelope FROM raddle_jobs WHERE tenant=$1 AND status='dead' AND sequence>$2 AND ($3::text IS NULL OR destination=$3) AND ($4::text IS NULL OR last_error=$4) ORDER BY sequence LIMIT $5")
            .bind(tenant).bind(query.after).bind(&query.destination).bind(&query.error_code)
            .bind(i64::from(query.limit)).fetch_all(&mut *transaction).await?;
        let mut items = Vec::with_capacity(rows.len());
        let mut unavailable = Vec::new();
        let mut after = query.after;
        for row in rows {
            after = row.try_get("sequence")?;
            let id = row.try_get("id")?;
            let message_id = row.try_get("message_id")?;
            let bytes: Vec<u8> = row.try_get("envelope")?;
            let Some(envelope) = readable_envelope(&bytes, message_id) else {
                unavailable.push(UnavailableDeadLetter {
                    id,
                    sequence: after,
                });
                continue;
            };
            if query
                .contract
                .as_deref()
                .is_none_or(|name| envelope.contract() == name)
            {
                items.push(DeadLetter {
                    id,
                    sequence: after,
                    destination: row.try_get("destination")?,
                    envelope,
                });
            }
        }
        let more: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM raddle_jobs WHERE tenant=$1 AND status='dead' AND sequence>$2 AND ($3::text IS NULL OR destination=$3) AND ($4::text IS NULL OR last_error=$4))")
            .bind(tenant).bind(after).bind(&query.destination).bind(&query.error_code)
            .fetch_one(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(DeadLetterScan {
            items,
            unavailable,
            next_after: more.then_some(after),
        })
    }

    /// Atomically retires one dead delivery and retains an explicit discard audit.
    ///
    /// Discard is not successful business completion. Original identity, envelope,
    /// fingerprint, generation and any recovery metadata remain unchanged. This
    /// terminal tombstone prevents duplicate custody from re-admitting the message.
    /// Neither replay nor normal completed-tombstone pruning removes it. There is
    /// deliberately no automatic discard/payload retention purge in this API.
    /// Cancellation drops the unfinished transaction; a commit transport failure
    /// can have an unknown outcome and must not be reported as accepted.
    pub async fn discard_dead_letter(
        &self,
        tenant: &str,
        id: Uuid,
        actor: &str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, Error> {
        if !bounded(tenant, 128) || id.is_nil() || !bounded(actor, 128) || !bounded(reason, 512) {
            return Err(Error::Invalid("invalid discard audit"));
        }
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "SELECT set_config('statement_timeout','2s',true),set_config('lock_timeout','2s',true)",
        )
        .execute(&mut *transaction)
        .await?;
        let candidate: Option<Uuid> = sqlx::query_scalar("SELECT id FROM raddle_jobs WHERE tenant=$1 AND id=$2 AND status='dead' FOR UPDATE SKIP LOCKED")
            .bind(tenant).bind(id).fetch_optional(&mut *transaction).await?;
        if candidate.is_none() {
            return Ok(false);
        }
        let changed = sqlx::query("UPDATE raddle_jobs SET status='discarded',owner=NULL,lease_until=NULL WHERE tenant=$1 AND id=$2 AND status='dead'")
            .bind(tenant).bind(id).execute(&mut *transaction).await?;
        if changed.rows_affected() != 1 {
            return Ok(false);
        }
        let audit = sqlx::query("INSERT INTO raddle_discard_audit(job_id,actor,reason,discarded_at) VALUES($1,$2,$3,$4)")
            .bind(id).bind(actor).bind(reason).bind(now).execute(&mut *transaction).await?;
        if audit.rows_affected() != 1 {
            return Err(Error::Invalid("discard audit was not retained"));
        }
        transaction.commit().await?;
        Ok(true)
    }
}
