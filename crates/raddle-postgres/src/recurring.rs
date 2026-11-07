//! Persistent interval scheduling with bounded catch-up and schedule-local transactions.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use raddle::wire::{Envelope, WireMessage};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use crate::{Delivery, DeliveryKind, Error, Scope, Store};

/// Tenant/source/name identity; no part is taken from message headers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ScheduleKey {
    tenant: String,
    source: String,
    name: String,
}

impl ScheduleKey {
    /// Validates bounded identity fields and the stable schedule name.
    pub fn new(tenant: &str, source: &str, name: &str) -> Result<Self, Error> {
        if tenant.is_empty() || tenant.len() > 128 || source.is_empty() || source.len() > 128 {
            return Err(Error::Invalid("invalid recurring scope"));
        }
        raddle::SubscriptionName::try_new(name)
            .map_err(|_| Error::Invalid("invalid recurring name"))?;
        Ok(Self {
            tenant: tenant.into(),
            source: source.into(),
            name: name.into(),
        })
    }

    /// Authenticated/configured owner.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    /// Configured producer namespace.
    pub fn source(&self) -> &str {
        &self.source
    }
    /// Stable schedule name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Explicit handling of elapsed interval occurrences.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MissedTickPolicy {
    /// Emits the stored due occurrence, then skips intervening missed ticks without a loop.
    SkipMissed,
    /// Emits oldest due occurrences, retaining backlog; capped per schedule per page.
    CatchUp {
        /// Between 1 and 256 occurrences.
        max_occurrences: u32,
    },
}

#[derive(Clone, Serialize, Deserialize)]
struct Route {
    kind: String,
    destination: String,
    expires_at: Option<DateTime<Utc>>,
    partition_key: Option<String>,
    max_attempts: u32,
}

/// A typed, validated persistent interval definition.
pub struct RecurringSchedule {
    key: ScheduleKey,
    first: DateTime<Utc>,
    interval_us: i64,
    catch_up: i32,
    envelope: Vec<u8>,
    deliveries: serde_json::Value,
    hash: Vec<u8>,
}

impl RecurringSchedule {
    /// Encodes a typed message once. Each occurrence gets its own stable identity.
    ///
    /// Intervals are whole microseconds from 1ms through 365 days. The first time
    /// is truncated to PostgreSQL microsecond precision. Delivery availability is
    /// replaced by the original occurrence time; all other delivery settings persist.
    pub fn new<M: WireMessage>(
        key: ScheduleKey,
        first: DateTime<Utc>,
        interval: Duration,
        policy: MissedTickPolicy,
        message: &M,
        deliveries: &[Delivery],
    ) -> Result<Self, Error> {
        let interval_us = i64::try_from(interval.as_micros())
            .map_err(|_| Error::Invalid("recurring interval overflow"))?;
        if !(1000..=31_536_000_000_000).contains(&interval_us)
            || !interval.subsec_nanos().is_multiple_of(1000)
            || deliveries.is_empty()
            || deliveries.len() > 256
        {
            return Err(Error::Invalid("invalid recurring interval or fan-out"));
        }
        let catch_up = match policy {
            MissedTickPolicy::SkipMissed => 0,
            MissedTickPolicy::CatchUp { max_occurrences }
                if (1..=256).contains(&max_occurrences) =>
            {
                max_occurrences as i32
            }
            _ => return Err(Error::Invalid("invalid recurring catch-up bound")),
        };
        let first = DateTime::from_timestamp_micros(first.timestamp_micros())
            .ok_or(Error::Invalid("invalid recurring timestamp"))?;
        first
            .checked_add_signed(TimeDelta::microseconds(interval_us))
            .ok_or(Error::Invalid("recurring timestamp overflow"))?;
        let envelope = Envelope::with_identity(Uuid::from_u128(1), first, message)?.to_vec()?;
        let routes = deliveries
            .iter()
            .map(|delivery| {
                delivery.validate()?;
                Ok(Route {
                    kind: delivery.kind.as_str().into(),
                    destination: delivery.destination.clone(),
                    expires_at: delivery.expires_at,
                    partition_key: delivery.partition_key.clone(),
                    max_attempts: delivery.max_attempts,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let deliveries = serde_json::to_value(routes).map_err(raddle::wire::WireError::from)?;
        let hash = Sha256::digest(
            serde_json::to_vec(&(first, interval_us, catch_up, &envelope, &deliveries))
                .map_err(raddle::wire::WireError::from)?,
        )
        .to_vec();
        Ok(Self {
            key,
            first,
            interval_us,
            catch_up,
            envelope,
            deliveries,
            hash,
        })
    }
}

impl Scope<'_> {
    /// Registers without resetting progress. Equivalent registration is idempotent;
    /// changed configuration conflicts. A cancelled schedule remains cancelled.
    pub async fn register_recurring(&mut self, schedule: &RecurringSchedule) -> Result<(), Error> {
        let mut operation = RecurringOperation::new(self);
        let result = async {
            sqlx::query("INSERT INTO raddle_recurring(tenant,source,name,incarnation,definition_hash,next_due,interval_us,catch_up,envelope,deliveries) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING")
                .bind(&schedule.key.tenant).bind(&schedule.key.source).bind(&schedule.key.name).bind(Uuid::new_v4())
                .bind(&schedule.hash).bind(schedule.first).bind(schedule.interval_us).bind(schedule.catch_up).bind(&schedule.envelope).bind(&schedule.deliveries)
                .execute(operation.scope.connection()).await?;
            let hash: Vec<u8> = sqlx::query_scalar("SELECT definition_hash FROM raddle_recurring WHERE tenant=$1 AND source=$2 AND name=$3 FOR UPDATE")
                .bind(&schedule.key.tenant).bind(&schedule.key.source).bind(&schedule.key.name).fetch_one(operation.scope.connection()).await?;
            if hash != schedule.hash { return Err(Error::IdentityConflict); }
            Ok(())
        }.await;
        if result.is_ok() {
            operation.armed = false;
        }
        result
    }

    /// Explicitly replaces/restarts an existing definition under the same row lock
    /// as dispatch. Already staged occurrences remain; replacement gets a new incarnation.
    pub async fn update_recurring(&mut self, schedule: &RecurringSchedule) -> Result<bool, Error> {
        let mut operation = RecurringOperation::new(self);
        let result = sqlx::query("UPDATE raddle_recurring SET incarnation=$4,definition_hash=$5,next_due=$6,interval_us=$7,catch_up=$8,envelope=$9,deliveries=$10,cancelled=false,retry_after=NULL,last_error=NULL WHERE tenant=$1 AND source=$2 AND name=$3")
            .bind(&schedule.key.tenant).bind(&schedule.key.source).bind(&schedule.key.name).bind(Uuid::new_v4())
            .bind(&schedule.hash).bind(schedule.first).bind(schedule.interval_us).bind(schedule.catch_up).bind(&schedule.envelope).bind(&schedule.deliveries)
            .execute(operation.scope.connection()).await.map(|result| result.rows_affected() == 1).map_err(Error::from);
        if result.is_ok() {
            operation.armed = false;
        }
        result
    }

    /// Cancels future scheduling, not occurrences already committed to custody.
    pub async fn cancel_recurring(&mut self, key: &ScheduleKey) -> Result<bool, Error> {
        let mut operation = RecurringOperation::new(self);
        let result = sqlx::query("UPDATE raddle_recurring SET cancelled=true WHERE tenant=$1 AND source=$2 AND name=$3 AND NOT cancelled")
            .bind(&key.tenant).bind(&key.source).bind(&key.name).execute(operation.scope.connection()).await
            .map(|result| result.rows_affected() == 1).map_err(Error::from);
        if result.is_ok() {
            operation.armed = false;
        }
        result
    }
}

// A borrowed future can disappear after SQL accepted a write but before its
// result is observed. Poison on both errors and cancellation, not just Err.
struct RecurringOperation<'borrow, 'connection> {
    scope: &'borrow mut Scope<'connection>,
    armed: bool,
}

impl<'borrow, 'connection> RecurringOperation<'borrow, 'connection> {
    fn new(scope: &'borrow mut Scope<'connection>) -> Self {
        Self { scope, armed: true }
    }
}

impl Drop for RecurringOperation<'_, '_> {
    fn drop(&mut self) {
        if self.armed {
            self.scope.mark_rollback_only();
        }
    }
}

/// A schedule-local failure; no payload or raw database error is exposed.
#[derive(Clone, Debug)]
pub struct ScheduleFailure {
    /// Failed identity; other schedules still run.
    pub key: ScheduleKey,
    /// Stable diagnostic classification.
    pub classification: &'static str,
}

/// Bounded work performed, including isolated failures.
#[derive(Default, Debug)]
pub struct SchedulePage {
    /// Schedules attempted, each in its own transaction.
    pub schedules: u32,
    /// Occurrences committed (not destination fan-out).
    pub occurrences: u32,
    /// Failures left safely retryable at their original occurrence.
    pub failures: Vec<ScheduleFailure>,
}

/// Explicit interval scheduler; starts no background tasks.
pub struct RecurringScheduler {
    store: Store,
}

impl RecurringScheduler {
    /// Shares the application's pool through its store.
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    /// Uses PostgreSQL's real clock, not the caller's process clock.
    pub async fn run_once(&self, schedules: u32, occurrences: u32) -> Result<SchedulePage, Error> {
        let mut scope = self.store.begin().await?;
        let now = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(scope.connection())
            .await?;
        scope.rollback().await?;
        self.run_at(schedules, occurrences, now).await
    }

    /// Runs against an explicitly supplied authoritative time (also useful in tests).
    /// Caps are 1..=256 schedules and 1..=1024 total occurrences per call. Each
    /// schedule is visited at most once, even when backlog or failures remain.
    pub async fn run_at(
        &self,
        schedules: u32,
        occurrences: u32,
        now: DateTime<Utc>,
    ) -> Result<SchedulePage, Error> {
        if !(1..=256).contains(&schedules) || !(1..=1024).contains(&occurrences) {
            return Err(Error::Invalid("invalid recurring page bounds"));
        }
        let retry_at = now
            .checked_add_signed(TimeDelta::seconds(30))
            .ok_or(Error::Invalid("recurring retry timestamp overflow"))?;
        let mut page = SchedulePage::default();
        let mut visited = Vec::<ScheduleKey>::new();
        for _ in 0..schedules {
            if page.occurrences == occurrences {
                break;
            }
            let mut scope = self.store.begin().await?;
            let seen = serde_json::to_value(&visited).map_err(raddle::wire::WireError::from)?;
            let row = sqlx::query("SELECT tenant,source,name,incarnation,next_due,interval_us,catch_up,envelope,deliveries FROM raddle_recurring WHERE NOT cancelled AND next_due <= $1 AND (retry_after IS NULL OR retry_after <= $1) AND NOT EXISTS (SELECT 1 FROM jsonb_to_recordset($2) AS seen(tenant text,source text,name text) WHERE seen.tenant=raddle_recurring.tenant AND seen.source=raddle_recurring.source AND seen.name=raddle_recurring.name) ORDER BY next_due,tenant,source,name LIMIT 1 FOR UPDATE SKIP LOCKED")
                .bind(now).bind(seen).fetch_optional(scope.connection()).await?;
            let Some(row) = row else {
                scope.rollback().await?;
                break;
            };
            let incarnation: Uuid = row.try_get("incarnation")?;
            let key = ScheduleKey {
                tenant: row.try_get("tenant")?,
                source: row.try_get("source")?,
                name: row.try_get("name")?,
            };
            let due: DateTime<Utc> = row.try_get("next_due")?;
            visited.push(key.clone());
            page.schedules += 1;
            let result = dispatch(
                &mut scope,
                &row,
                &key,
                incarnation,
                now,
                occurrences - page.occurrences,
            )
            .await;
            match result {
                Ok(count) => {
                    scope.commit(now).await?;
                    page.occurrences += count;
                }
                Err(error) => {
                    scope.mark_rollback_only();
                    scope.rollback().await?;
                    let classification = match error {
                        Error::Capacity => "capacity",
                        Error::Database(_) => "database",
                        _ => "invalid-schedule",
                    };
                    // New incarnation and due comparison fence concurrent updates
                    // and successful dispatches between rollback and diagnostics.
                    let mut diagnostic = self.store.begin().await?;
                    sqlx::query("WITH failed AS (SELECT tenant,source,name FROM raddle_recurring WHERE tenant=$1 AND source=$2 AND name=$3 AND incarnation=$4 AND next_due=$5 AND NOT cancelled FOR UPDATE SKIP LOCKED) UPDATE raddle_recurring r SET retry_after=$6,last_error=$7 FROM failed f WHERE r.tenant=f.tenant AND r.source=f.source AND r.name=f.name")
                        .bind(&key.tenant).bind(&key.source).bind(&key.name).bind(incarnation).bind(due).bind(retry_at).bind(classification)
                        .execute(diagnostic.connection()).await?;
                    diagnostic.commit(now).await?;
                    page.failures.push(ScheduleFailure {
                        key,
                        classification,
                    });
                }
            }
        }
        Ok(page)
    }
}

#[derive(Serialize, Deserialize)]
struct OccurrenceEnvelope {
    id: Uuid,
    contract: String,
    version: u32,
    created_at: DateTime<Utc>,
    correlation_id: Option<Uuid>,
    causation_id: Option<Uuid>,
    headers: BTreeMap<String, String>,
    payload: serde_json::Value,
}

async fn dispatch(
    scope: &mut Scope<'_>,
    row: &sqlx::postgres::PgRow,
    key: &ScheduleKey,
    incarnation: Uuid,
    now: DateTime<Utc>,
    remaining: u32,
) -> Result<u32, Error> {
    let mut due: DateTime<Utc> = row.try_get("next_due")?;
    let interval: i64 = row.try_get("interval_us")?;
    let catch_up: i32 = row.try_get("catch_up")?;
    if !(1000..=31_536_000_000_000).contains(&interval) || !(0..=256).contains(&catch_up) {
        return Err(Error::Invalid("invalid persisted recurring bounds"));
    }
    let routes: Vec<Route> = serde_json::from_value(row.try_get("deliveries")?)
        .map_err(raddle::wire::WireError::from)?;
    if routes.is_empty() || routes.len() > 256 {
        return Err(Error::Invalid("invalid recurring fan-out"));
    }
    let bytes: Vec<u8> = row.try_get("envelope")?;
    Envelope::from_slice(&bytes, Default::default())?;
    let mut template: OccurrenceEnvelope =
        serde_json::from_slice(&bytes).map_err(raddle::wire::WireError::from)?;
    let skip_next = if catch_up == 0 {
        let elapsed = now
            .signed_duration_since(due)
            .num_microseconds()
            .ok_or(Error::Invalid("recurring elapsed overflow"))?;
        let skipped = (elapsed / interval)
            .checked_add(1)
            .ok_or(Error::Invalid("recurring skip overflow"))?
            .checked_mul(interval)
            .ok_or(Error::Invalid("recurring skip overflow"))?;
        Some(
            due.checked_add_signed(TimeDelta::microseconds(skipped))
                .ok_or(Error::Invalid("recurring timestamp overflow"))?,
        )
    } else {
        None
    };
    let limit = if catch_up == 0 {
        1
    } else {
        (catch_up as u32).min(remaining)
    };
    let mut count = 0;
    while due <= now && count < limit {
        let next = match skip_next {
            Some(next) => next,
            None => due
                .checked_add_signed(TimeDelta::microseconds(interval))
                .ok_or(Error::Invalid("recurring timestamp overflow"))?,
        };
        let mut digest = Sha256::new();
        digest.update(incarnation.as_bytes());
        digest.update(due.timestamp_micros().to_be_bytes());
        let digest = digest.finalize();
        let mut id = [0; 16];
        id.copy_from_slice(&digest[..16]);
        // RFC 9562 custom version, derived from the original scheduled time.
        id[6] = (id[6] & 0x0f) | 0x80;
        id[8] = (id[8] & 0x3f) | 0x80;
        template.id = Uuid::from_bytes(id);
        template.created_at = due;
        let envelope = Envelope::from_slice(
            &serde_json::to_vec(&template).map_err(raddle::wire::WireError::from)?,
            Default::default(),
        )?;
        let deliveries = routes
            .iter()
            .map(|route| {
                let kind = match route.kind.as_str() {
                    "local" => DeliveryKind::Local,
                    "remote" => DeliveryKind::Remote,
                    _ => return Err(Error::Invalid("invalid recurring delivery kind")),
                };
                Ok(Delivery {
                    kind,
                    destination: route.destination.clone(),
                    available_at: due,
                    expires_at: route.expires_at,
                    partition_key: route.partition_key.clone(),
                    max_attempts: route.max_attempts,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        scope
            .stage(&key.source, &key.tenant, &envelope, &deliveries)
            .await?;
        count += 1;
        due = next;
    }
    sqlx::query("UPDATE raddle_recurring SET next_due=$4,retry_after=NULL,last_error=NULL WHERE tenant=$1 AND source=$2 AND name=$3")
        .bind(&key.tenant).bind(&key.source).bind(&key.name).bind(due).execute(scope.connection()).await?;
    Ok(count)
}
