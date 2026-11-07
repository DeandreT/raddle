//! Tenant-scoped event streams and transactional, stream-isolated projections.
//!
//! Event identity is not deduplication: even an identical repeated UUID conflicts.
//! Tenant positions are allocated under a transaction-held counter-row lock, not
//! a PostgreSQL sequence. There is deliberately no cross-tenant high-water mark.

use std::{collections::HashSet, future::Future, pin::Pin};

use chrono::Utc;
use raddle::wire::{Codecs, Envelope, WireError, WireMessage};
use sqlx::{Row, postgres::PgRow};
use uuid::Uuid;

use crate::{Error, Scope, Store};

/// Event persistence, optimistic concurrency, or deterministic application failure.
/// Error text never includes event bodies.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EventError {
    /// An underlying database query failed; serialization failures require a new transaction.
    #[error("event database operation failed")]
    Database(#[from] sqlx::Error),
    /// The shared transaction or outbox operation failed.
    #[error("event transaction operation failed")]
    Transaction(#[from] Error),
    /// Encoding, decoding, or an explicit upcaster failed.
    #[error("event wire conversion failed")]
    Wire(#[from] WireError),
    /// Bounded identity, revision, or page arguments were invalid.
    #[error("invalid event operation: {0}")]
    Invalid(&'static str),
    /// The stream no longer has the caller's expected revision.
    #[error("event stream revision conflict (actual revision {actual})")]
    RevisionConflict {
        /// Requested optimistic concurrency token.
        expected: ExpectedRevision,
        /// Current revision, or zero for a nonexistent stream.
        actual: u64,
    },
    /// A stream cannot change its aggregate contract or epoch.
    #[error("event stream aggregate contract or epoch differs")]
    AggregateMismatch,
    /// An event UUID was reused within this tenant, even with identical content.
    #[error("event identity already exists")]
    DuplicateEvent,
    /// Rehydration encountered an event outside its explicit typed contract.
    #[error("aggregate does not support the recorded event contract")]
    UnknownContract,
    /// Persisted revisions were not contiguous; no event was silently skipped.
    #[error("event stream has a revision gap or inconsistent metadata")]
    RevisionGap,
    /// A pure aggregate transition or projection rejected the event.
    #[error("event application rejected: {0}")]
    Rejected(&'static str),
}

/// Explicit optimistic concurrency; unconditional appends are not supported.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpectedRevision {
    /// Creates a stream only if no committed stream exists.
    NoStream,
    /// Appends only at this existing stream revision (positive and at most i64::MAX).
    Exact(u64),
}

/// A trusted tenant, non-nil stream identity, and immutable aggregate contract/epoch.
/// Tenant identity must come from authentication/configuration, not event headers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventStream {
    tenant: String,
    id: Uuid,
    aggregate: String,
    epoch: u32,
}

impl EventStream {
    /// Validates a stream identity without touching storage.
    pub fn new(tenant: &str, id: Uuid, aggregate: &str, epoch: u32) -> Result<Self, EventError> {
        tenant_valid(tenant)?;
        raddle::SubscriptionName::try_new(aggregate)
            .map_err(|_| EventError::Invalid("invalid aggregate contract"))?;
        if id.is_nil() || epoch == 0 || epoch > i32::MAX as u32 {
            return Err(EventError::Invalid("invalid stream identity or epoch"));
        }
        Ok(Self {
            tenant: tenant.into(),
            id,
            aggregate: aggregate.into(),
            epoch,
        })
    }

    /// Uses the aggregate's explicit stable identity and epoch.
    pub fn for_aggregate<A: Aggregate>(tenant: &str, id: Uuid) -> Result<Self, EventError> {
        Self::new(tenant, id, A::NAME, A::EPOCH)
    }

    /// Authenticated tenant namespace.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    /// Stream UUID, meaningful only within its tenant.
    pub fn id(&self) -> Uuid {
        self.id
    }
    /// Immutable stable aggregate contract.
    pub fn aggregate(&self) -> &str {
        &self.aggregate
    }
    /// Immutable application-selected aggregate epoch, independent of wire versions.
    pub fn epoch(&self) -> u32 {
        self.epoch
    }
}

/// One immutable persisted event, with both stream and tenant ordering tokens.
#[derive(Clone, Debug)]
pub struct RecordedEvent {
    /// Explicit tenant and aggregate identity.
    pub stream: EventStream,
    /// Contiguous positive stream revision.
    pub revision: u64,
    /// Commit-ordered position within this tenant, never a cross-tenant cursor.
    pub tenant_position: u64,
    /// Original versioned event identity, payload, and correlation metadata.
    pub envelope: Envelope,
}

/// The committed value is meaningful only after the surrounding scope commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendResult {
    /// Last appended stream revision.
    pub revision: u64,
    /// First allocated tenant position.
    pub first_position: u64,
    /// Last allocated tenant position.
    pub last_position: u64,
}

/// Typed deterministic fold. Implementations must not perform I/O or side effects.
/// Use an explicitly tagged wire enum when an aggregate has several event variants.
pub trait Aggregate: Default {
    /// Stable aggregate contract, independent of Rust paths and event wire names.
    const NAME: &'static str;
    /// Positive aggregate epoch; changing it requires an explicit new stream/migration.
    const EPOCH: u32;
    /// Supported versioned event contract; `Codecs` performs explicit upcasting.
    type Event: WireMessage;
    /// Applies one event in revision order; errors stop the fold, never skip an event.
    fn apply(&mut self, event: Self::Event) -> Result<(), EventError>;
}

/// Rehydrated application state and the exact optimistic revision it represents.
pub struct Rehydrated<A> {
    /// Deterministically folded state.
    pub state: A,
    /// Last applied persisted revision.
    pub revision: u64,
}

/// A projection callback borrows the same transaction as its checkpoint.
pub type ProjectionFuture<'scope> =
    Pin<Box<dyn Future<Output = Result<(), EventError>> + Send + 'scope>>;

/// A bounded projection page; its checkpoint commits with all callback effects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionProgress {
    /// Previous successful checkpoint for this projection/tenant/stream.
    pub previous_revision: u64,
    /// New successful checkpoint (unchanged for an empty page).
    pub revision: u64,
    /// Number of events applied in this page.
    pub applied: u32,
}

fn tenant_valid(tenant: &str) -> Result<(), EventError> {
    if tenant.is_empty() || tenant.len() > 128 {
        return Err(EventError::Invalid("invalid tenant"));
    }
    Ok(())
}

fn page_valid(after: u64, limit: u32) -> Result<(), EventError> {
    if after > i64::MAX as u64 || !(1..=256).contains(&limit) {
        return Err(EventError::Invalid(
            "invalid revision/position or page size",
        ));
    }
    Ok(())
}

fn recorded(row: PgRow) -> Result<RecordedEvent, EventError> {
    let event = RecordedEvent {
        stream: EventStream::new(
            row.try_get("tenant")?,
            row.try_get("stream_id")?,
            row.try_get("aggregate_contract")?,
            row.try_get::<i32, _>("epoch")? as u32,
        )?,
        revision: row.try_get::<i64, _>("revision")? as u64,
        tenant_position: row.try_get::<i64, _>("tenant_position")? as u64,
        envelope: Envelope::from_slice(row.try_get("envelope")?, Default::default())?,
    };
    if event.envelope.id() != row.try_get::<Uuid, _>("event_id")? {
        return Err(EventError::RevisionGap);
    }
    Ok(event)
}

// A cancelled borrowed future must not leave partially executed statements
// available for a caller to commit. This also covers unwinding in callbacks.
struct EventOperation<'scope, 'connection> {
    scope: &'scope mut Scope<'connection>,
    succeeded: bool,
}

impl<'scope, 'connection> EventOperation<'scope, 'connection> {
    fn new(scope: &'scope mut Scope<'connection>) -> Self {
        Self {
            scope,
            succeeded: false,
        }
    }

    fn finish<T>(mut self, result: Result<T, EventError>) -> Result<T, EventError> {
        self.succeeded = result.is_ok();
        result
    }
}

impl Drop for EventOperation<'_, '_> {
    fn drop(&mut self) {
        if !self.succeeded {
            self.scope.mark_rollback_only();
        }
    }
}

impl Scope<'_> {
    /// Appends 1..=256 original envelopes inside this business transaction.
    /// Stable event UUID reuse always conflicts, not idempotent success.
    /// Every error or cancellation makes the scope rollback-only, including validation conflicts.
    pub async fn append_events(
        &mut self,
        stream: &EventStream,
        expected: ExpectedRevision,
        events: &[Envelope],
    ) -> Result<AppendResult, EventError> {
        let operation = EventOperation::new(self);
        let result = operation
            .scope
            .append_events_inner(stream, expected, events)
            .await;
        operation.finish(result)
    }

    /// Encodes a typed batch and appends it; serialization failure poisons the scope.
    /// This allocates new UUIDs. For retry-stable identities, encode once using
    /// `Envelope::with_identity` and call `append_events` instead.
    pub async fn append_messages<M: WireMessage>(
        &mut self,
        stream: &EventStream,
        expected: ExpectedRevision,
        messages: &[M],
    ) -> Result<AppendResult, EventError> {
        let operation = EventOperation::new(self);
        let result = async {
            if !(1..=256).contains(&messages.len()) {
                return Err(EventError::Invalid("invalid append batch size"));
            }
            let events = messages
                .iter()
                .map(Envelope::encode)
                .collect::<Result<Vec<_>, _>>()?;
            operation
                .scope
                .append_events_inner(stream, expected, &events)
                .await
        }
        .await;
        operation.finish(result)
    }

    async fn append_events_inner(
        &mut self,
        stream: &EventStream,
        expected: ExpectedRevision,
        events: &[Envelope],
    ) -> Result<AppendResult, EventError> {
        if !(1..=256).contains(&events.len())
            || matches!(expected, ExpectedRevision::Exact(0))
            || matches!(expected, ExpectedRevision::Exact(n) if n > i64::MAX as u64)
        {
            return Err(EventError::Invalid(
                "invalid append batch or expected revision",
            ));
        }
        let mut identities = HashSet::with_capacity(events.len());
        let mut bytes = Vec::with_capacity(events.len());
        for event in events {
            event.validate(Default::default())?;
            if !identities.insert(event.id()) {
                return Err(EventError::DuplicateEvent);
            }
            bytes.push(event.to_vec()?);
        }
        sqlx::query("INSERT INTO raddle_event_tenants(tenant) VALUES($1) ON CONFLICT DO NOTHING")
            .bind(&stream.tenant)
            .execute(self.connection())
            .await?;
        // The tuple write both serializes commit order and fences stale RR/SSI
        // snapshots. Merely locking a row would not refresh a fixed snapshot.
        let last: i64 = sqlx::query_scalar("UPDATE raddle_event_tenants SET position=position+$2 WHERE tenant=$1 RETURNING position")
            .bind(&stream.tenant).bind(events.len() as i64).fetch_one(self.connection()).await?;
        let current = self.stream_revision(stream).await?;
        let actual = current.unwrap_or(0);
        let matches = match expected {
            ExpectedRevision::NoStream => current.is_none(),
            ExpectedRevision::Exact(revision) => current == Some(revision),
        };
        if !matches {
            return Err(EventError::RevisionConflict { expected, actual });
        }
        let revision = actual
            .checked_add(events.len() as u64)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(EventError::Invalid("stream revision exhausted"))?;
        if current.is_none() {
            sqlx::query("INSERT INTO raddle_event_streams(tenant,stream_id,aggregate_contract,epoch) VALUES($1,$2,$3,$4)")
                .bind(&stream.tenant).bind(stream.id).bind(&stream.aggregate).bind(stream.epoch as i32)
                .execute(self.connection()).await?;
        }
        let first = last - events.len() as i64 + 1;
        for (index, (event, bytes)) in events.iter().zip(bytes).enumerate() {
            let inserted = sqlx::query("INSERT INTO raddle_events(tenant,stream_id,revision,tenant_position,event_id,envelope) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(tenant,event_id) DO NOTHING")
                .bind(&stream.tenant).bind(stream.id).bind((actual + index as u64 + 1) as i64)
                .bind(first + index as i64).bind(event.id()).bind(bytes).execute(self.connection()).await?;
            if inserted.rows_affected() != 1 {
                return Err(EventError::DuplicateEvent);
            }
        }
        sqlx::query("UPDATE raddle_event_streams SET revision=$3 WHERE tenant=$1 AND stream_id=$2")
            .bind(&stream.tenant)
            .bind(stream.id)
            .bind(revision as i64)
            .execute(self.connection())
            .await?;
        Ok(AppendResult {
            revision,
            first_position: first as u64,
            last_position: last as u64,
        })
    }

    async fn stream_revision(&mut self, stream: &EventStream) -> Result<Option<u64>, EventError> {
        let row = sqlx::query("SELECT aggregate_contract,epoch,revision FROM raddle_event_streams WHERE tenant=$1 AND stream_id=$2")
            .bind(&stream.tenant).bind(stream.id).fetch_optional(self.connection()).await?;
        row.map(|row| {
            if row.try_get::<&str, _>("aggregate_contract")? != stream.aggregate
                || row.try_get::<i32, _>("epoch")? != stream.epoch as i32
            {
                return Err(EventError::AggregateMismatch);
            }
            Ok(row.try_get::<i64, _>("revision")? as u64)
        })
        .transpose()
    }

    /// Reads 1..=256 events after an exclusive stream revision; missing streams are empty.
    /// Aggregate contract/epoch mismatch and decode errors poison the transaction.
    pub async fn load_events(
        &mut self,
        stream: &EventStream,
        after_revision: u64,
        limit: u32,
    ) -> Result<Vec<RecordedEvent>, EventError> {
        let operation = EventOperation::new(self);
        let result = async {
            page_valid(after_revision, limit)?;
            let Some(head) = operation.scope.stream_revision(stream).await? else {
                return Ok(Vec::new());
            };
            operation
                .scope
                .event_page(stream, after_revision, head, limit)
                .await
        }
        .await;
        operation.finish(result)
    }

    async fn event_page(
        &mut self,
        stream: &EventStream,
        after: u64,
        through: u64,
        limit: u32,
    ) -> Result<Vec<RecordedEvent>, EventError> {
        let rows = sqlx::query("SELECT e.*,s.aggregate_contract,s.epoch FROM raddle_events e JOIN raddle_event_streams s USING(tenant,stream_id) WHERE e.tenant=$1 AND e.stream_id=$2 AND e.revision>$3 AND e.revision<=$4 ORDER BY e.revision LIMIT $5")
            .bind(&stream.tenant).bind(stream.id).bind(after as i64).bind(through as i64).bind(i64::from(limit))
            .fetch_all(self.connection()).await?;
        let events = rows
            .into_iter()
            .map(recorded)
            .collect::<Result<Vec<_>, _>>()?;
        let expected_len = (through.saturating_sub(after)).min(u64::from(limit));
        if events.len() as u64 != expected_len
            || events
                .iter()
                .enumerate()
                .any(|(index, event)| event.revision != after + index as u64 + 1)
        {
            return Err(EventError::RevisionGap);
        }
        Ok(events)
    }

    /// Reads a bounded commit-ordered tenant feed, with an exclusive tenant position.
    /// Never use a position from one tenant as another tenant's checkpoint.
    pub async fn tenant_events(
        &mut self,
        tenant: &str,
        after_position: u64,
        limit: u32,
    ) -> Result<Vec<RecordedEvent>, EventError> {
        let operation = EventOperation::new(self);
        let result = async {
            tenant_valid(tenant)?;
            page_valid(after_position, limit)?;
            let rows = sqlx::query("SELECT e.*,s.aggregate_contract,s.epoch FROM raddle_events e JOIN raddle_event_streams s USING(tenant,stream_id) WHERE e.tenant=$1 AND e.tenant_position>$2 ORDER BY e.tenant_position LIMIT $3")
                .bind(tenant).bind(after_position as i64).bind(i64::from(limit)).fetch_all(operation.scope.connection()).await?;
            rows.into_iter().map(recorded).collect::<Result<Vec<_>, EventError>>()
        }.await;
        operation.finish(result)
    }

    /// Folds a fixed stream head in bounded pages with registered pure upcasters.
    /// Missing contracts/versions, gaps, or failed transitions stop rehydration.
    pub async fn rehydrate<A: Aggregate>(
        &mut self,
        stream: &EventStream,
        codecs: &Codecs,
        page_size: u32,
    ) -> Result<Option<Rehydrated<A>>, EventError> {
        let operation = EventOperation::new(self);
        let result = async {
            page_valid(0, page_size)?;
            if stream.aggregate != A::NAME || stream.epoch != A::EPOCH {
                return Err(EventError::AggregateMismatch);
            }
            let Some(head) = operation.scope.stream_revision(stream).await? else {
                return Ok(None);
            };
            let mut state = A::default();
            let mut revision = 0;
            while revision < head {
                for event in operation
                    .scope
                    .event_page(stream, revision, head, page_size)
                    .await?
                {
                    if event.envelope.contract() != A::Event::NAME {
                        return Err(EventError::UnknownContract);
                    }
                    state.apply(codecs.decode::<A::Event>(&event.envelope)?)?;
                    revision = event.revision;
                }
            }
            Ok(Some(Rehydrated { state, revision }))
        }
        .await;
        operation.finish(result)
    }
}

impl<'connection> Scope<'connection> {
    /// Applies a bounded page for one named projection/tenant/stream.
    /// Callback writes, outgoing `stage` calls, and checkpoint share this scope.
    /// Failure or cancellation poisons the scope: dependent events are never skipped.
    /// Run unrelated streams in separate scopes to preserve failure isolation.
    pub async fn project_events<F>(
        &mut self,
        projection: &str,
        stream: &EventStream,
        limit: u32,
        mut apply: F,
    ) -> Result<ProjectionProgress, EventError>
    where
        F: for<'a> FnMut(&'a RecordedEvent, &'a mut Scope<'connection>) -> ProjectionFuture<'a>,
    {
        let operation = EventOperation::new(self);
        let result = async {
            raddle::SubscriptionName::try_new(projection)
                .map_err(|_| EventError::Invalid("invalid projection name"))?;
            page_valid(0, limit)?;
            if operation.scope.stream_revision(stream).await?.is_none() {
                return Ok(ProjectionProgress { previous_revision: 0, revision: 0, applied: 0 });
            }
            sqlx::query("INSERT INTO raddle_event_checkpoints(projection,tenant,stream_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
                .bind(projection).bind(&stream.tenant).bind(stream.id).execute(operation.scope.connection()).await?;
            // A tuple write fences old fixed snapshots, as with append admission.
            let previous: i64 = sqlx::query_scalar("UPDATE raddle_event_checkpoints SET revision=revision WHERE projection=$1 AND tenant=$2 AND stream_id=$3 RETURNING revision")
                .bind(projection).bind(&stream.tenant).bind(stream.id).fetch_one(operation.scope.connection()).await?;
            // Refresh the head after waiting for the checkpoint lock: another
            // projector may have advanced while this transaction was waiting.
            let head = operation.scope.stream_revision(stream).await?.ok_or(EventError::RevisionGap)?;
            if previous as u64 > head {
                return Err(EventError::RevisionGap);
            }
            let events = operation.scope.event_page(stream, previous as u64, head, limit).await?;
            for event in &events {
                apply(event, operation.scope).await?;
            }
            let revision = events.last().map_or(previous as u64, |event| event.revision);
            let changed = sqlx::query("UPDATE raddle_event_checkpoints SET revision=$4 WHERE projection=$1 AND tenant=$2 AND stream_id=$3 AND revision=$5")
                .bind(projection).bind(&stream.tenant).bind(stream.id).bind(revision as i64).bind(previous)
                .execute(operation.scope.connection()).await?;
            if changed.rows_affected() != 1 {
                return Err(EventError::RevisionGap);
            }
            Ok(ProjectionProgress { previous_revision: previous as u64, revision, applied: events.len() as u32 })
        }.await;
        operation.finish(result)
    }
}

impl Store {
    /// Opens and commits one bounded, stream-isolated projection page.
    /// The host owns iteration, retry, scheduling, and error reporting. There is
    /// no implicit skip/dead-letter policy and no checkpoint-reset operation.
    /// Replay uses a new explicit projection name and separately owned view state.
    pub async fn project_stream<F>(
        &self,
        projection: &str,
        stream: &EventStream,
        limit: u32,
        apply: F,
    ) -> Result<ProjectionProgress, EventError>
    where
        F: for<'a> FnMut(&'a RecordedEvent, &'a mut Scope<'static>) -> ProjectionFuture<'a>,
    {
        let mut scope = self.begin().await?;
        let result = scope.project_events(projection, stream, limit, apply).await;
        match result {
            Ok(progress) => {
                scope.commit(Utc::now()).await?;
                Ok(progress)
            }
            Err(error) => {
                scope.rollback().await?;
                Err(error)
            }
        }
    }
}
