use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::Error;

// These are pending quarantine classifications, NOT terminal outcomes. Canonical
// jobs remain active and batch-owned until the corresponding dead write commits.
pub(super) const EXHAUSTION_PENDING: &str = "batch-exhaustion-quarantine-retry";
pub(super) const EXPIRY_PENDING: &str = "batch-expiry-quarantine-retry";
pub(super) const POSTPONE: Duration = Duration::from_millis(250);

pub(super) fn deferred(code: Option<&str>) -> bool {
    matches!(code, Some(EXHAUSTION_PENDING | EXPIRY_PENDING))
}

pub(super) enum MemberRead {
    Metadata,
    Input,
}

/// Membership, including terminal participants, defines a unit permanently.
/// Never rebuild a coalesced unit from just its surviving active originals.
pub(super) async fn lock_members(
    connection: &mut PgConnection,
    batch: Uuid,
    mode: MemberRead,
) -> Result<Vec<PgRow>, Error> {
    let rows = sqlx::query("SELECT j.id,j.status,j.owner,CASE WHEN $2 AND j.status='active' AND j.owner=$1 AND j.last_error IS DISTINCT FROM $3 AND j.last_error IS DISTINCT FROM $4 THEN j.envelope END AS envelope,j.attempts,j.max_attempts,j.last_error,j.available_at,j.expires_at,m.coalesce_key,m.ordinal,m.probed FROM raddle_batch_members m JOIN raddle_jobs j ON j.id=m.job_id WHERE m.batch_id=$1 ORDER BY j.id FOR UPDATE OF j")
        .bind(batch).bind(matches!(mode, MemberRead::Input)).bind(EXHAUSTION_PENDING).bind(EXPIRY_PENDING).fetch_all(connection).await?;
    let mut ordered = rows
        .into_iter()
        .map(|row| Ok((row.try_get::<i32, _>("ordinal")?, row)))
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    ordered.sort_by_key(|(ordinal, _)| *ordinal);
    Ok(ordered.into_iter().map(|(_, row)| row).collect())
}

struct Unit<'a> {
    key: Option<&'a str>,
    rows: Vec<&'a PgRow>,
}

/// Terminalize whole coalesced units. A rejected write is rolled back only to
/// that unit's savepoint, and its original custody is postponed, never released.
pub(super) async fn quarantine(
    connection: &mut PgConnection,
    batch: Uuid,
    rows: &[PgRow],
    now: DateTime<Utc>,
    expired: bool,
) -> Result<(), Error> {
    let mut units: Vec<Unit<'_>> = Vec::new();
    for row in rows {
        let key: Option<&str> = row.try_get("coalesce_key")?;
        if let Some(unit) = key.and_then(|key| units.iter_mut().find(|unit| unit.key == Some(key)))
        {
            unit.rows.push(row);
        } else {
            units.push(Unit {
                key,
                rows: vec![row],
            });
        }
    }
    for unit in units {
        let mut ids = Vec::new();
        let mut exhausted = false;
        let mut pending_expiry = false;
        let mut retry_due = false;
        let mut already_deferred = false;
        for row in &unit.rows {
            let status: &str = row.try_get("status")?;
            let code: Option<&str> = row.try_get("last_error")?;
            // Also repair partial exhaustion left by an older worker: the old
            // value must not become input when its persisted last winner is dead.
            exhausted |= (status == "active"
                && row.try_get::<i32, _>("attempts")? >= row.try_get::<i32, _>("max_attempts")?)
                || (status == "dead" && code == Some("attempts-exhausted"))
                || code == Some(EXHAUSTION_PENDING);
            pending_expiry |= code == Some(EXPIRY_PENDING);
            if status == "active" && row.try_get::<Option<Uuid>, _>("owner")? == Some(batch) {
                ids.push(row.try_get::<Uuid, _>("id")?);
                already_deferred |= deferred(code);
                retry_due |= row.try_get::<DateTime<Utc>, _>("available_at")? <= now;
            }
        }
        if ids.is_empty() || !(expired || pending_expiry || exhausted) {
            continue;
        }
        if already_deferred && !retry_due {
            continue;
        }
        let (code, pending) = if expired || pending_expiry {
            ("expired", EXPIRY_PENDING)
        } else {
            ("attempts-exhausted", EXHAUSTION_PENDING)
        };
        sqlx::query("SAVEPOINT batch_claim_quarantine")
            .execute(&mut *connection)
            .await?;
        let result = sqlx::query("UPDATE raddle_jobs SET status='dead',owner=NULL,lease_until=NULL,last_error=$3 WHERE id=ANY($1) AND status='active' AND owner=$2")
            .bind(&ids).bind(batch).bind(code).execute(&mut *connection).await;
        if !result.is_ok_and(|result| result.rows_affected() == ids.len() as u64) {
            sqlx::query("ROLLBACK TO SAVEPOINT batch_claim_quarantine")
                .execute(&mut *connection)
                .await?;
            let retry_at = super::until(now, POSTPONE)?;
            let changed = sqlx::query("UPDATE raddle_jobs SET last_error=$3,available_at=$4 WHERE id=ANY($1) AND status='active' AND owner=$2")
                .bind(&ids).bind(batch).bind(pending).bind(retry_at).execute(&mut *connection).await?;
            if changed.rows_affected() != ids.len() as u64 {
                return Err(Error::Stale);
            }
        }
        sqlx::query("RELEASE SAVEPOINT batch_claim_quarantine")
            .execute(&mut *connection)
            .await?;
    }
    Ok(())
}

pub(super) struct Counts {
    pub(super) pending: i64,
    pub(super) executable: i64,
    pub(super) unprobed: i64,
    pub(super) quarantine_due: Option<DateTime<Utc>>,
}

pub(super) async fn counts(connection: &mut PgConnection, batch: Uuid) -> Result<Counts, Error> {
    let row = sqlx::query("SELECT count(*) AS pending,count(*) FILTER(WHERE j.last_error IS DISTINCT FROM $2 AND j.last_error IS DISTINCT FROM $3) AS executable,count(*) FILTER(WHERE NOT m.probed AND j.last_error IS DISTINCT FROM $2 AND j.last_error IS DISTINCT FROM $3) AS unprobed,min(j.available_at) FILTER(WHERE j.last_error IN ($2,$3)) AS quarantine_due FROM raddle_batch_members m JOIN raddle_jobs j ON j.id=m.job_id WHERE m.batch_id=$1 AND j.status='active' AND j.owner=$1")
        .bind(batch).bind(EXHAUSTION_PENDING).bind(EXPIRY_PENDING).fetch_one(connection).await?;
    Ok(Counts {
        pending: row.try_get("pending")?,
        executable: row.try_get("executable")?,
        unprobed: row.try_get("unprobed")?,
        quarantine_due: row.try_get("quarantine_due")?,
    })
}

pub(super) async fn release(
    connection: &mut PgConnection,
    batch: Uuid,
    counts: &Counts,
    due: DateTime<Utc>,
) -> Result<(), Error> {
    let due = if counts.executable == 0 {
        counts.quarantine_due.unwrap_or(due)
    } else {
        due
    };
    sqlx::query("UPDATE raddle_batches SET status=$2,available_at=$3,probing=false,owner=NULL,lease_until=NULL WHERE id=$1")
        .bind(batch).bind(if counts.pending == 0 { "completed" } else { "ready" }).bind(due).execute(connection).await?;
    Ok(())
}
