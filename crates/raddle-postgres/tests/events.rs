//! Real PostgreSQL event streams and stream-isolated projection transactions.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use chrono::Utc;
use raddle::wire::{Codecs, Envelope, WireMessage};
use raddle_postgres::events::{
    Aggregate, EventError, EventStream, ExpectedRevision, ProjectionFuture, RecordedEvent,
};
use raddle_postgres::{Delivery, DeliveryKind, Error, Scope, Store};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Added {
    amount: u32,
}
impl WireMessage for Added {
    const NAME: &'static str = "counter.added";
    const VERSION: u32 = 2;
}

#[derive(Serialize, Deserialize)]
struct LegacyAdded {
    step: u32,
}
impl WireMessage for LegacyAdded {
    const NAME: &'static str = Added::NAME;
    const VERSION: u32 = 1;
}

#[derive(Serialize, Deserialize)]
struct Unrelated;
impl WireMessage for Unrelated {
    const NAME: &'static str = "unrelated.event";
    const VERSION: u32 = 1;
}

#[derive(Default)]
struct Counter(u64);
impl Aggregate for Counter {
    const NAME: &'static str = "counter";
    const EPOCH: u32 = 1;
    type Event = Added;
    fn apply(&mut self, event: Added) -> Result<(), EventError> {
        self.0 += u64::from(event.amount);
        Ok(())
    }
}

fn stream(tenant: &str) -> EventStream {
    EventStream::for_aggregate::<Counter>(tenant, Uuid::new_v4()).unwrap()
}

fn event(amount: u32) -> Envelope {
    Envelope::encode(&Added { amount }).unwrap()
}

async fn append(store: &Store, stream: &EventStream, count: usize) {
    let mut scope = store.begin().await.unwrap();
    let events = (0..count).map(|_| event(1)).collect::<Vec<_>>();
    scope
        .append_events(stream, ExpectedRevision::NoStream, &events)
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
}

#[test]
fn stream_identity_is_explicit_and_validated() {
    assert!(EventStream::new("", Uuid::new_v4(), "counter", 1).is_err());
    assert!(EventStream::new("tenant", Uuid::nil(), "counter", 1).is_err());
    assert!(EventStream::new("tenant", Uuid::new_v4(), "Bad name", 1).is_err());
    assert!(EventStream::new("tenant", Uuid::new_v4(), "counter", 0).is_err());
    let stream = stream("tenant");
    assert_eq!(stream.tenant(), "tenant");
    assert_eq!(stream.aggregate(), Counter::NAME);
    assert_eq!(stream.epoch(), 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn tenant_stream_pages_rehydrate_with_explicit_upcasters(pool: PgPool) {
    let store = Store::new(pool);
    let first = stream("first");
    let second = EventStream::for_aggregate::<Counter>("second", first.id()).unwrap();
    let legacy = Envelope::encode(&LegacyAdded { step: 7 })
        .unwrap()
        .correlated(Uuid::new_v4(), Uuid::new_v4())
        .unwrap();
    let mut scope = store.begin().await.unwrap();
    scope
        .append_events(
            &first,
            ExpectedRevision::NoStream,
            &[legacy.clone(), event(3)],
        )
        .await
        .unwrap();
    scope
        .append_messages(&first, ExpectedRevision::Exact(2), &[Added { amount: 5 }])
        .await
        .unwrap();
    scope
        .append_messages(&second, ExpectedRevision::NoStream, &[Added { amount: 99 }])
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();

    let mut codecs = Codecs::default();
    codecs
        .upcast(Added::NAME, 1, |value| {
            Ok(serde_json::json!({"amount": value["step"]}))
        })
        .unwrap();
    let mut scope = store.begin().await.unwrap();
    let loaded = scope
        .rehydrate::<Counter>(&first, &codecs, 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.state.0, 15);
    assert_eq!(loaded.revision, 3);
    let page = scope.load_events(&first, 0, 1).await.unwrap();
    assert_eq!(page[0].revision, 1);
    assert_eq!(page[0].envelope.to_vec().unwrap(), legacy.to_vec().unwrap());
    assert_eq!(scope.load_events(&first, 1, 256).await.unwrap().len(), 2);
    assert!(scope.load_events(&first, 3, 1).await.unwrap().is_empty());
    assert_eq!(scope.tenant_events("first", 0, 256).await.unwrap().len(), 3);
    assert_eq!(
        scope.tenant_events("second", 0, 256).await.unwrap().len(),
        1
    );
    assert!(
        scope
            .rehydrate::<Counter>(&stream("missing"), &codecs, 2)
            .await
            .unwrap()
            .is_none()
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn optimistic_conflicts_and_uuid_reuse_never_deduplicate(pool: PgPool) {
    let store = Store::new(pool.clone());
    let stream = stream("tenant");
    let original = event(1);
    let mut scope = store.begin().await.unwrap();
    scope
        .append_events(
            &stream,
            ExpectedRevision::NoStream,
            std::slice::from_ref(&original),
        )
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    for expected in [ExpectedRevision::NoStream, ExpectedRevision::Exact(2)] {
        let mut scope = store.begin().await.unwrap();
        assert!(matches!(
            scope.append_events(&stream, expected, &[event(1)]).await,
            Err(EventError::RevisionConflict { actual: 1, .. })
        ));
        assert!(matches!(
            scope.commit(Utc::now()).await,
            Err(Error::RollbackOnly)
        ));
    }
    for duplicate in [
        original.clone(),
        Envelope::with_identity(original.id(), original.created_at(), &Added { amount: 999 })
            .unwrap(),
    ] {
        let mut scope = store.begin().await.unwrap();
        assert!(matches!(
            scope
                .append_events(&stream, ExpectedRevision::Exact(1), &[duplicate])
                .await,
            Err(EventError::DuplicateEvent)
        ));
        assert!(matches!(
            scope.commit(Utc::now()).await,
            Err(Error::RollbackOnly)
        ));
    }
    let another = EventStream::new("tenant", Uuid::new_v4(), Counter::NAME, 1).unwrap();
    let mut scope = store.begin().await.unwrap();
    assert!(matches!(
        scope
            .append_events(
                &another,
                ExpectedRevision::NoStream,
                std::slice::from_ref(&original)
            )
            .await,
        Err(EventError::DuplicateEvent)
    ));
    assert!(matches!(
        scope.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
    // Identity is tenant-scoped, not a global UUID admission gate.
    let other_tenant = EventStream::new("other", stream.id(), Counter::NAME, 1).unwrap();
    let mut scope = store.begin().await.unwrap();
    scope
        .append_events(&other_tenant, ExpectedRevision::NoStream, &[original])
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_events")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_event_streams")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn aggregate_contract_and_epoch_are_immutable(pool: PgPool) {
    let store = Store::new(pool);
    let stream = stream("tenant");
    append(&store, &stream, 1).await;
    for changed in [
        EventStream::new("tenant", stream.id(), "other", 1).unwrap(),
        EventStream::new("tenant", stream.id(), Counter::NAME, 2).unwrap(),
    ] {
        let mut scope = store.begin().await.unwrap();
        assert!(matches!(
            scope
                .append_events(&changed, ExpectedRevision::Exact(1), &[event(1)])
                .await,
            Err(EventError::AggregateMismatch)
        ));
        assert!(matches!(
            scope.commit(Utc::now()).await,
            Err(Error::RollbackOnly)
        ));
        let mut scope = store.begin().await.unwrap();
        assert!(matches!(
            scope.load_events(&changed, 0, 1).await,
            Err(EventError::AggregateMismatch)
        ));
        assert!(matches!(
            scope.commit(Utc::now()).await,
            Err(Error::RollbackOnly)
        ));
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn append_business_and_outbox_commit_or_rollback_together(pool: PgPool) {
    let store = Store::new(pool.clone());
    sqlx::query("CREATE TABLE event_business(id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    let stream = stream("tenant");
    let envelope = event(1);
    let mut scope = Scope::enroll(pool.begin().await.unwrap());
    sqlx::query("INSERT INTO event_business VALUES(1)")
        .execute(scope.connection())
        .await
        .unwrap();
    scope
        .append_events(
            &stream,
            ExpectedRevision::NoStream,
            std::slice::from_ref(&envelope),
        )
        .await
        .unwrap();
    scope
        .stage(
            "event-tests",
            "tenant",
            &envelope,
            &[Delivery::new(DeliveryKind::Remote, "out", Utc::now())],
        )
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    sqlx::query("INSERT INTO event_business VALUES(2)")
        .execute(scope.connection())
        .await
        .unwrap();
    scope
        .stage(
            "event-tests",
            "tenant",
            &event(2),
            &[Delivery::new(DeliveryKind::Remote, "out", Utc::now())],
        )
        .await
        .unwrap();
    // A late UUID collision rolls back the earlier event in this batch as well
    // as business writes and the outbox operation that preceded the append.
    assert!(matches!(
        scope
            .append_events(&stream, ExpectedRevision::Exact(1), &[event(2), envelope])
            .await,
        Err(EventError::DuplicateEvent)
    ));
    assert!(matches!(
        scope.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM event_business")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_events")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT position FROM raddle_event_tenants WHERE tenant='tenant'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[derive(Deserialize)]
struct CannotSerialize;
impl Serialize for CannotSerialize {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("rejected"))
    }
}
impl WireMessage for CannotSerialize {
    const NAME: &'static str = "cannot.serialize";
    const VERSION: u32 = 1;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn serialization_and_batch_bounds_poison_the_transaction(pool: PgPool) {
    let store = Store::new(pool);
    let stream = stream("tenant");
    let mut scope = store.begin().await.unwrap();
    assert!(matches!(
        scope
            .append_messages(&stream, ExpectedRevision::NoStream, &[CannotSerialize])
            .await,
        Err(EventError::Wire(_))
    ));
    assert!(matches!(
        scope.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
    for count in [0, 257] {
        let mut scope = store.begin().await.unwrap();
        let events = (0..count).map(|_| event(1)).collect::<Vec<_>>();
        assert!(matches!(
            scope
                .append_events(&stream, ExpectedRevision::NoStream, &events)
                .await,
            Err(EventError::Invalid(_))
        ));
        assert!(matches!(
            scope.commit(Utc::now()).await,
            Err(Error::RollbackOnly)
        ));
    }
    append(&store, &stream, 256).await;
    for size in [0, 257] {
        let mut scope = store.begin().await.unwrap();
        assert!(matches!(
            scope.load_events(&stream, 0, size).await,
            Err(EventError::Invalid(_))
        ));
        assert!(matches!(
            scope.commit(Utc::now()).await,
            Err(Error::RollbackOnly)
        ));
    }
    let mut scope = store.begin().await.unwrap();
    assert_eq!(scope.load_events(&stream, 0, 256).await.unwrap().len(), 256);
    assert!(matches!(
        scope
            .append_events(&stream, ExpectedRevision::Exact(0), &[event(1)])
            .await,
        Err(EventError::Invalid(_))
    ));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn unknown_events_missing_upcasters_and_gaps_are_not_skipped(pool: PgPool) {
    let store = Store::new(pool.clone());
    let unknown = stream("unknown");
    let legacy = stream("legacy");
    let gap = stream("gap");
    let mut scope = store.begin().await.unwrap();
    scope
        .append_events(
            &unknown,
            ExpectedRevision::NoStream,
            &[event(1), Envelope::encode(&Unrelated).unwrap(), event(1)],
        )
        .await
        .unwrap();
    scope
        .append_messages(
            &legacy,
            ExpectedRevision::NoStream,
            &[LegacyAdded { step: 2 }],
        )
        .await
        .unwrap();
    scope
        .append_events(
            &gap,
            ExpectedRevision::NoStream,
            &[event(1), event(1), event(1)],
        )
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    assert!(matches!(
        scope
            .rehydrate::<Counter>(&unknown, &Codecs::default(), 1)
            .await,
        Err(EventError::UnknownContract)
    ));
    assert!(matches!(
        scope.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
    let mut scope = store.begin().await.unwrap();
    assert!(matches!(
        scope
            .rehydrate::<Counter>(&legacy, &Codecs::default(), 1)
            .await,
        Err(EventError::Wire(_))
    ));
    assert!(matches!(
        scope.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
    // Deliberate test-only corruption verifies the reader does not equate an
    // absent revision with a successfully processed event.
    sqlx::query("DELETE FROM raddle_events WHERE tenant='gap' AND revision=2")
        .execute(&pool)
        .await
        .unwrap();
    let mut scope = store.begin().await.unwrap();
    assert!(matches!(
        scope
            .rehydrate::<Counter>(&gap, &Codecs::default(), 2)
            .await,
        Err(EventError::RevisionGap)
    ));
    assert!(matches!(
        scope.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn uncommitted_append_cannot_be_skipped_by_a_tenant_cursor(pool: PgPool) {
    let store = Store::new(pool);
    let seed = stream("tenant");
    let early = stream("tenant");
    let later = stream("tenant");
    append(&store, &seed, 1).await;
    let mut first = store.begin().await.unwrap();
    assert_eq!(
        first
            .append_events(&early, ExpectedRevision::NoStream, &[event(1)])
            .await
            .unwrap()
            .last_position,
        2
    );
    let mut second = store.begin().await.unwrap();
    let cursor;
    {
        let events = [event(1)];
        let pending = second.append_events(&later, ExpectedRevision::NoStream, &events);
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut pending)
                .await
                .is_err()
        );
        // Another tenant can commit while the first tenant's append is uncommitted.
        append(&store, &stream("unrelated"), 1).await;
        let mut reader = store.begin().await.unwrap();
        let visible = reader.tenant_events("tenant", 0, 256).await.unwrap();
        assert_eq!(visible.len(), 1);
        cursor = visible[0].tenant_position;
        reader.rollback().await.unwrap();
        first.commit(Utc::now()).await.unwrap();
        assert_eq!(pending.await.unwrap().last_position, 3);
        // The pending future has released its mutable borrow, but the SQL scope
        // remains uncommitted: only position 2 is visible to a new reader.
        let mut reader = store.begin().await.unwrap();
        assert_eq!(
            reader
                .tenant_events("tenant", cursor, 256)
                .await
                .unwrap()
                .iter()
                .map(|e| e.tenant_position)
                .collect::<Vec<_>>(),
            vec![2]
        );
        reader.rollback().await.unwrap();
    }
    second.commit(Utc::now()).await.unwrap();
    let mut reader = store.begin().await.unwrap();
    assert_eq!(
        reader
            .tenant_events("tenant", cursor, 256)
            .await
            .unwrap()
            .iter()
            .map(|e| e.tenant_position)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn concurrent_optimistic_writers_have_only_one_winner(pool: PgPool) {
    let store = Store::new(pool);
    let stream = stream("tenant");
    append(&store, &stream, 1).await;
    async fn write(store: &Store, stream: &EventStream) -> bool {
        let mut scope = store.begin().await.unwrap();
        match scope
            .append_events(stream, ExpectedRevision::Exact(1), &[event(1)])
            .await
        {
            Ok(_) => {
                scope.commit(Utc::now()).await.unwrap();
                true
            }
            Err(EventError::RevisionConflict { actual: 2, .. }) => {
                assert!(matches!(
                    scope.commit(Utc::now()).await,
                    Err(Error::RollbackOnly)
                ));
                false
            }
            other => panic!("unexpected append result: {other:?}"),
        }
    }
    let (first, second) = tokio::join!(write(&store, &stream), write(&store, &stream));
    assert_ne!(first, second);
    let mut scope = store.begin().await.unwrap();
    assert_eq!(scope.load_events(&stream, 0, 256).await.unwrap().len(), 2);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn fixed_snapshot_append_is_fenced_and_requires_whole_transaction_retry(pool: PgPool) {
    let store = Store::new(pool.clone());
    for isolation in ["REPEATABLE READ", "SERIALIZABLE"] {
        let stream = stream("tenant");
        append(&store, &stream, 1).await;
        let mut transaction = pool.begin().await.unwrap();
        sqlx::query(&format!("SET TRANSACTION ISOLATION LEVEL {isolation}"))
            .execute(&mut *transaction)
            .await
            .unwrap();
        sqlx::query("SELECT position FROM raddle_event_tenants WHERE tenant='tenant'")
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
        let mut stale = Scope::enroll(transaction);
        let mut winner = store.begin().await.unwrap();
        winner
            .append_events(&stream, ExpectedRevision::Exact(1), &[event(1)])
            .await
            .unwrap();
        winner.commit(Utc::now()).await.unwrap();
        match stale
            .append_events(&stream, ExpectedRevision::Exact(1), &[event(1)])
            .await
        {
            Err(EventError::Database(sqlx::Error::Database(error))) => {
                assert_eq!(error.code().as_deref(), Some("40001"));
            }
            other => panic!("expected stale-snapshot serialization failure: {other:?}"),
        }
        assert!(matches!(
            stale.commit(Utc::now()).await,
            Err(Error::RollbackOnly)
        ));
        let mut fresh = store.begin().await.unwrap();
        assert_eq!(
            fresh
                .append_events(&stream, ExpectedRevision::Exact(2), &[event(1)])
                .await
                .unwrap()
                .revision,
            3
        );
        fresh.commit(Utc::now()).await.unwrap();
    }
}

async fn views(pool: &PgPool) {
    sqlx::query("CREATE TABLE event_views(projection TEXT,tenant TEXT,stream_id UUID,applied BIGINT NOT NULL,PRIMARY KEY(projection,tenant,stream_id))")
        .execute(pool).await.unwrap();
}

fn effect<'a>(
    event: &'a RecordedEvent,
    scope: &'a mut Scope<'static>,
    projection: &'static str,
    fail: Option<u64>,
) -> ProjectionFuture<'a> {
    Box::pin(async move {
        sqlx::query("INSERT INTO event_views(projection,tenant,stream_id,applied) VALUES($1,$2,$3,1) ON CONFLICT(projection,tenant,stream_id) DO UPDATE SET applied=event_views.applied+1")
            .bind(projection).bind(event.stream.tenant()).bind(event.stream.id()).execute(scope.connection()).await?;
        scope
            .stage(
                "event-projections",
                event.stream.tenant(),
                &event.envelope,
                &[Delivery::new(DeliveryKind::Remote, projection, Utc::now())],
            )
            .await?;
        if fail == Some(event.revision) {
            return Err(EventError::Rejected("test failure"));
        }
        Ok(())
    })
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn projection_failures_first_middle_last_isolate_streams_and_tenants(pool: PgPool) {
    let store = Store::new(pool.clone());
    views(&pool).await;
    let before = stream("tenant");
    let failing = stream("tenant");
    let after = stream("tenant");
    let other_tenant = stream("other-tenant");
    for stream in [&before, &failing, &after, &other_tenant] {
        append(&store, stream, 3).await;
    }
    for (projection, fail) in [
        ("failure-first", 1),
        ("failure-middle", 2),
        ("failure-last", 3),
    ] {
        // Move the failing stream through the first/middle/last positions as
        // well as failing its first/middle/last event. The host handles each
        // result independently instead of returning on the first failure.
        let ordered = match fail {
            1 => [&failing, &before, &after],
            2 => [&before, &failing, &after],
            _ => [&before, &after, &failing],
        };
        for current in ordered.into_iter().chain([&other_tenant]) {
            let failure = (current == &failing).then_some(fail);
            let result = store
                .project_stream(projection, current, 256, |e, s| {
                    effect(e, s, projection, failure)
                })
                .await;
            if failure.is_some() {
                assert!(matches!(result, Err(EventError::Rejected(_))));
            } else {
                assert_eq!(result.unwrap().applied, 3);
            }
        }
        // Failed pages retain no partial view/outbox/checkpoint. Successful
        // streams stay checkpointed while the failed stream is retried.
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM raddle_event_checkpoints WHERE projection=$1"
            )
            .bind(projection)
            .fetch_one(&pool)
            .await
            .unwrap(),
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs WHERE destination=$1")
                .bind(projection)
                .fetch_one(&pool)
                .await
                .unwrap(),
            9
        );
        assert_eq!(
            store
                .project_stream(projection, &before, 256, |e, s| effect(
                    e, s, projection, None
                ))
                .await
                .unwrap()
                .applied,
            0
        );
        let retry = store
            .project_stream(projection, &failing, 256, |e, s| {
                effect(e, s, projection, None)
            })
            .await
            .unwrap();
        assert_eq!(retry.previous_revision, 0);
        assert_eq!(retry.revision, 3);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT sum(applied)::bigint FROM event_views WHERE projection=$1"
            )
            .bind(projection)
            .fetch_one(&pool)
            .await
            .unwrap(),
            12
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs WHERE destination=$1")
                .bind(projection)
                .fetch_one(&pool)
                .await
                .unwrap(),
            12
        );
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn projection_retains_checkpoint_restarts_and_replays_with_a_new_name(pool: PgPool) {
    let store = Store::new(pool.clone());
    views(&pool).await;
    let stream = stream("tenant");
    append(&store, &stream, 3).await;
    let first = store
        .project_stream("view-v1", &stream, 1, |e, s| effect(e, s, "view-v1", None))
        .await
        .unwrap();
    assert_eq!(first.revision, 1);
    assert!(
        store
            .project_stream("view-v1", &stream, 2, |e, s| effect(
                e,
                s,
                "view-v1",
                Some(3)
            ))
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT revision FROM raddle_event_checkpoints WHERE projection='view-v1'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT applied FROM event_views WHERE projection='view-v1'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let restarted = Store::new(pool.clone());
    let resumed = restarted
        .project_stream("view-v1", &stream, 2, |e, s| effect(e, s, "view-v1", None))
        .await
        .unwrap();
    assert_eq!((resumed.previous_revision, resumed.revision), (1, 3));
    assert_eq!(
        restarted
            .project_stream("view-v2", &stream, 256, |e, s| effect(
                e, s, "view-v2", None
            ))
            .await
            .unwrap()
            .applied,
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT applied FROM event_views WHERE projection='view-v1'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT applied FROM event_views WHERE projection='view-v2'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        3
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn competing_projection_pages_do_not_apply_an_event_twice(pool: PgPool) {
    let store = Store::new(pool.clone());
    views(&pool).await;
    let stream = stream("tenant");
    append(&store, &stream, 3).await;
    let (a, b) = tokio::join!(
        store.project_stream("view", &stream, 2, |e, s| effect(e, s, "view", None)),
        store.project_stream("view", &stream, 2, |e, s| effect(e, s, "view", None))
    );
    assert_eq!(a.unwrap().applied + b.unwrap().applied, 3);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT applied FROM event_views")
            .fetch_one(&pool)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        3
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn enrolled_projection_failure_poisoned_scope_cannot_commit_business_writes(pool: PgPool) {
    let store = Store::new(pool.clone());
    views(&pool).await;
    let stream = stream("tenant");
    append(&store, &stream, 3).await;
    let mut scope = Scope::enroll(pool.begin().await.unwrap());
    scope
        .append_events(
            &EventStream::for_aggregate::<Counter>("other", Uuid::new_v4()).unwrap(),
            ExpectedRevision::NoStream,
            &[event(1)],
        )
        .await
        .unwrap();
    assert!(
        scope
            .project_events("outer", &stream, 3, |e, s| effect(e, s, "outer", Some(2)))
            .await
            .is_err()
    );
    assert!(matches!(
        scope.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_events")
            .fetch_one(&pool)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM event_views")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_event_checkpoints")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    // Even a callback that returns success cannot hide an explicitly poisoned scope.
    assert!(matches!(
        store
            .project_stream("poisoned", &stream, 1, |_event, scope| Box::pin(
                async move {
                    scope.mark_rollback_only();
                    Ok(())
                }
            ))
            .await,
        Err(EventError::Transaction(Error::RollbackOnly))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_event_checkpoints")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn fixed_snapshot_projection_cannot_reapply_a_committed_page(pool: PgPool) {
    let store = Store::new(pool.clone());
    views(&pool).await;
    let stream = stream("tenant");
    append(&store, &stream, 3).await;
    store
        .project_stream("view", &stream, 1, |e, s| effect(e, s, "view", None))
        .await
        .unwrap();
    let mut transaction = pool.begin().await.unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query("SELECT revision FROM raddle_event_checkpoints WHERE projection='view'")
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
    let mut stale = Scope::enroll(transaction);
    store
        .project_stream("view", &stream, 2, |e, s| effect(e, s, "view", None))
        .await
        .unwrap();
    match stale
        .project_events("view", &stream, 2, |e, s| effect(e, s, "view", None))
        .await
    {
        Err(EventError::Database(sqlx::Error::Database(error))) => {
            assert_eq!(error.code().as_deref(), Some("40001"))
        }
        other => panic!("expected stale-snapshot serialization failure: {other:?}"),
    }
    assert!(matches!(
        stale.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT applied FROM event_views")
            .fetch_one(&pool)
            .await
            .unwrap(),
        3
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn cancelled_append_marks_the_borrowed_scope_rollback_only(pool: PgPool) {
    let store = Store::new(pool.clone());
    let locked = stream("tenant");
    append(&store, &locked, 1).await;
    let mut blocker = pool.begin().await.unwrap();
    // The event INSERT's FK key-share check waits behind this stream row lock.
    sqlx::query(
        "SELECT stream_id FROM raddle_event_streams WHERE tenant=$1 AND stream_id=$2 FOR UPDATE",
    )
    .bind(locked.tenant())
    .bind(locked.id())
    .fetch_one(&mut *blocker)
    .await
    .unwrap();
    let mut scope = store.begin().await.unwrap();
    scope
        .append_events(
            &stream("earlier-business-effect"),
            ExpectedRevision::NoStream,
            &[event(1)],
        )
        .await
        .unwrap();
    let events = [event(2), event(3)];
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            scope.append_events(&locked, ExpectedRevision::Exact(1), &events)
        )
        .await
        .is_err()
    );
    blocker.rollback().await.unwrap();
    assert!(matches!(
        scope.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_events")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT position FROM raddle_event_tenants WHERE tenant='tenant'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn cancelled_projection_cannot_commit_partial_business_or_outbox_effects(pool: PgPool) {
    let store = Store::new(pool.clone());
    views(&pool).await;
    let stream = stream("tenant");
    append(&store, &stream, 3).await;
    let mut scope = store.begin().await.unwrap();
    let (started, observed) = tokio::sync::oneshot::channel();
    let mut started = Some(started);
    {
        let future = scope.project_events("cancelled", &stream, 3, move |event, scope| {
            let started = started.take();
            Box::pin(async move {
                effect(event, scope, "cancelled", None).await?;
                started.unwrap().send(()).unwrap();
                std::future::pending::<()>().await;
                Ok(())
            })
        });
        tokio::pin!(future);
        tokio::select! {
            result = &mut future => panic!("projection unexpectedly finished: {result:?}"),
            signal = observed => signal.unwrap(),
        }
        // Drop the pending borrowed future after its callback has staged actual
        // writes; the outer scope is still owned by the caller.
    }
    assert!(matches!(
        scope.commit(Utc::now()).await,
        Err(Error::RollbackOnly)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM event_views")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_event_checkpoints")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}
