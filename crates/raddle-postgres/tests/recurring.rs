//! Recurring custody contracts against a disposable PostgreSQL database.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use raddle::wire::{Envelope, WireMessage};
use raddle_postgres::{
    Delivery, DeliveryKind, Error, MissedTickPolicy, RecurringSchedule, RecurringScheduler,
    ScheduleKey, Scope, Store,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Tick(u32);
impl WireMessage for Tick {
    const NAME: &'static str = "test.recurring";
    const VERSION: u32 = 1;
}

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000, 0).unwrap()
}
fn key(tenant: &str, name: &str) -> ScheduleKey {
    ScheduleKey::new(tenant, "clock", name).unwrap()
}
fn schedule(
    tenant: &str,
    name: &str,
    first: DateTime<Utc>,
    policy: MissedTickPolicy,
) -> RecurringSchedule {
    RecurringSchedule::new(
        key(tenant, name),
        first,
        Duration::from_secs(1),
        policy,
        &Tick(7),
        &[Delivery::new(DeliveryKind::Local, "ticks", first)],
    )
    .unwrap()
}
async fn register(store: &Store, schedule: &RecurringSchedule) {
    let mut scope = store.begin().await.unwrap();
    scope.register_recurring(schedule).await.unwrap();
    scope.commit(now()).await.unwrap();
}
async fn envelopes(pool: &PgPool) -> Vec<Envelope> {
    sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT envelope FROM raddle_jobs ORDER BY available_at,message_id",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|bytes| Envelope::from_slice(&bytes, Default::default()).unwrap())
    .collect()
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn run_once_uses_database_clock_and_respects_future_occurrences(pool: PgPool) {
    let store = Store::new(pool.clone());
    let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    register(
        &store,
        &schedule(
            "a",
            "due",
            database_now - TimeDelta::seconds(2),
            MissedTickPolicy::SkipMissed,
        ),
    )
    .await;
    register(
        &store,
        &schedule(
            "a",
            "future",
            database_now + TimeDelta::days(1),
            MissedTickPolicy::SkipMissed,
        ),
    )
    .await;
    let page = RecurringScheduler::new(store)
        .run_once(10, 10)
        .await
        .unwrap();
    assert_eq!(page.schedules, 1);
    assert_eq!(page.occurrences, 1);
    assert!(envelopes(&pool).await[0].created_at() < database_now);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn skip_missed_jumps_arithmetically_and_preserves_original_occurrence(pool: PgPool) {
    let store = Store::new(pool.clone());
    let first = now() - TimeDelta::days(3650);
    let definition = RecurringSchedule::new(
        key("a", "fast"),
        first,
        Duration::from_millis(1),
        MissedTickPolicy::SkipMissed,
        &Tick(3),
        &[Delivery::new(DeliveryKind::Local, "ticks", first)],
    )
    .unwrap();
    register(&store, &definition).await;
    let scheduler = RecurringScheduler::new(store);
    let page = scheduler
        .run_at(10, 100, now() + TimeDelta::microseconds(500))
        .await
        .unwrap();
    assert_eq!(page.occurrences, 1);
    assert!(page.failures.is_empty());
    let messages = envelopes(&pool).await;
    assert_eq!(messages[0].created_at(), first);
    assert_eq!(messages[0].decode::<Tick>().unwrap().0, 3);
    let next: DateTime<Utc> = sqlx::query_scalar("SELECT next_due FROM raddle_recurring")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(next, now() + TimeDelta::milliseconds(1));
    assert_eq!(
        scheduler.run_at(10, 100, now()).await.unwrap().occurrences,
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn bounded_catch_up_respects_total_page_and_idempotent_registration(pool: PgPool) {
    let store = Store::new(pool.clone());
    let definition = schedule(
        "a",
        "backlog",
        now() - TimeDelta::seconds(20),
        MissedTickPolicy::CatchUp { max_occurrences: 3 },
    );
    register(&store, &definition).await;
    let scheduler = RecurringScheduler::new(store.clone());
    assert_eq!(
        scheduler.run_at(256, 2, now()).await.unwrap().occurrences,
        2
    );
    register(&store, &definition).await;
    let page = scheduler.run_at(256, 100, now()).await.unwrap();
    assert_eq!(page.schedules, 1);
    assert_eq!(page.occurrences, 3);
    let messages = envelopes(&pool).await;
    assert_eq!(messages.len(), 5);
    assert_eq!(messages[4].created_at(), now() - TimeDelta::seconds(16));
    let mut scope = store.begin().await.unwrap();
    assert!(matches!(
        scope
            .register_recurring(&schedule(
                "a",
                "backlog",
                now(),
                MissedTickPolicy::SkipMissed
            ))
            .await,
        Err(Error::IdentityConflict)
    ));
    assert!(matches!(
        scope.commit(now()).await,
        Err(Error::RollbackOnly)
    ));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn failed_schedule_rolls_back_whole_catch_up_and_other_tenants_continue(pool: PgPool) {
    let store = Store::new(pool.clone());
    store.set_capacity("blocked", 1).await.unwrap();
    register(
        &store,
        &schedule(
            "blocked",
            "blocked",
            now() - TimeDelta::seconds(2),
            MissedTickPolicy::CatchUp { max_occurrences: 3 },
        ),
    )
    .await;
    register(
        &store,
        &schedule(
            "before",
            "valid",
            now() - TimeDelta::seconds(3),
            MissedTickPolicy::SkipMissed,
        ),
    )
    .await;
    register(
        &store,
        &schedule(
            "after",
            "valid",
            now() - TimeDelta::seconds(1),
            MissedTickPolicy::SkipMissed,
        ),
    )
    .await;
    let original_incarnation: Uuid =
        sqlx::query_scalar("SELECT incarnation FROM raddle_recurring WHERE tenant='blocked'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let scheduler = RecurringScheduler::new(store.clone());
    let page = scheduler.run_at(10, 20, now()).await.unwrap();
    assert_eq!(page.schedules, 3);
    assert_eq!(page.occurrences, 2);
    assert_eq!(page.failures.len(), 1);
    assert_eq!(page.failures[0].classification, "capacity");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs WHERE tenant='blocked'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, DateTime<Utc>>(
            "SELECT next_due FROM raddle_recurring WHERE tenant='blocked'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        now() - TimeDelta::seconds(2)
    );
    store.set_capacity("blocked", 100).await.unwrap();
    let retry = scheduler
        .run_at(10, 20, now() + TimeDelta::seconds(31))
        .await
        .unwrap();
    assert!(retry.failures.is_empty());
    let bytes: Vec<u8> = sqlx::query_scalar(
        "SELECT envelope FROM raddle_jobs WHERE tenant='blocked' ORDER BY available_at LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let message = Envelope::from_slice(&bytes, Default::default()).unwrap();
    let occurrence = now() - TimeDelta::seconds(2);
    assert_eq!(message.created_at(), occurrence);
    let mut digest = Sha256::new();
    digest.update(original_incarnation.as_bytes());
    digest.update(occurrence.timestamp_micros().to_be_bytes());
    let digest = digest.finalize();
    let mut id = [0; 16];
    id.copy_from_slice(&digest[..16]);
    id[6] = (id[6] & 15) | 128;
    id[8] = (id[8] & 63) | 128;
    assert_eq!(message.id(), Uuid::from_bytes(id));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_jobs WHERE tenant='before' AND available_at=$1"
        )
        .bind(now() - TimeDelta::seconds(3))
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn skip_missed_retry_keeps_original_due_when_clock_advances(pool: PgPool) {
    let store = Store::new(pool.clone());
    store.set_capacity("a", 1).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    scope
        .stage(
            "seed",
            "a",
            &Envelope::encode(&Tick(0)).unwrap(),
            &[Delivery::new(DeliveryKind::Remote, "seed", now())],
        )
        .await
        .unwrap();
    scope.commit(now()).await.unwrap();
    let first = now() - TimeDelta::days(1);
    register(
        &store,
        &schedule("a", "skip", first, MissedTickPolicy::SkipMissed),
    )
    .await;
    let scheduler = RecurringScheduler::new(store.clone());
    assert_eq!(
        scheduler.run_at(1, 1, now()).await.unwrap().failures.len(),
        1
    );
    store.set_capacity("a", 2).await.unwrap();
    let retry = scheduler
        .run_at(1, 1, now() + TimeDelta::seconds(31))
        .await
        .unwrap();
    assert_eq!(retry.occurrences, 1);
    let bytes: Vec<u8> =
        sqlx::query_scalar("SELECT envelope FROM raddle_jobs WHERE source='clock'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        Envelope::from_slice(&bytes, Default::default())
            .unwrap()
            .created_at(),
        first
    );
    assert_eq!(
        sqlx::query_scalar::<_, DateTime<Utc>>("SELECT next_due FROM raddle_recurring")
            .fetch_one(&pool)
            .await
            .unwrap(),
        now() + TimeDelta::seconds(32)
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn malformed_schedule_is_isolated_with_retry_diagnostics(pool: PgPool) {
    let store = Store::new(pool.clone());
    for name in ["a.bad", "b.good", "c.bad"] {
        register(
            &store,
            &schedule("same", name, now(), MissedTickPolicy::SkipMissed),
        )
        .await;
    }
    sqlx::query("UPDATE raddle_recurring SET deliveries='{}' WHERE name <> 'b.good'")
        .execute(&pool)
        .await
        .unwrap();
    let scheduler = RecurringScheduler::new(store.clone());
    let page = scheduler.run_at(3, 3, now()).await.unwrap();
    assert_eq!(page.schedules, 3);
    assert_eq!(page.occurrences, 1);
    assert_eq!(page.failures.len(), 2);
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_recurring WHERE last_error='invalid-schedule' AND retry_after IS NOT NULL").fetch_one(&pool).await.unwrap(), 2);
    // Fixing only failed definitions never resets the successful occurrence.
    let mut scope = store.begin().await.unwrap();
    for name in ["a.bad", "c.bad"] {
        assert!(
            scope
                .update_recurring(&schedule("same", name, now(), MissedTickPolicy::SkipMissed))
                .await
                .unwrap()
        );
    }
    scope.commit(now()).await.unwrap();
    assert_eq!(scheduler.run_at(3, 3, now()).await.unwrap().occurrences, 2);
    assert_eq!(envelopes(&pool).await.len(), 3);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn cancellation_replacement_and_registration_share_application_transaction(pool: PgPool) {
    let store = Store::new(pool.clone());
    let definition = schedule("a", "clock", now(), MissedTickPolicy::SkipMissed);
    let mut scope = store.begin().await.unwrap();
    scope.register_recurring(&definition).await.unwrap();
    scope.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_recurring")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    register(&store, &definition).await;
    let scheduler = RecurringScheduler::new(store.clone());
    assert_eq!(scheduler.run_at(1, 1, now()).await.unwrap().occurrences, 1);
    let mut scope = store.begin().await.unwrap();
    assert!(scope.cancel_recurring(&key("a", "clock")).await.unwrap());
    scope.commit(now()).await.unwrap();
    register(&store, &definition).await;
    assert_eq!(
        scheduler
            .run_at(1, 1, now() + TimeDelta::seconds(10))
            .await
            .unwrap()
            .occurrences,
        0
    );
    let mut scope = store.begin().await.unwrap();
    assert!(scope.update_recurring(&definition).await.unwrap());
    scope.commit(now()).await.unwrap();
    assert_eq!(scheduler.run_at(1, 1, now()).await.unwrap().occurrences, 1);
    let messages = envelopes(&pool).await;
    assert_eq!(messages.len(), 2);
    assert_ne!(messages[0].id(), messages[1].id());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn concurrent_schedulers_and_locked_updates_do_not_duplicate_occurrences(pool: PgPool) {
    let store = Store::new(pool.clone());
    register(
        &store,
        &schedule("a", "clock", now(), MissedTickPolicy::SkipMissed),
    )
    .await;
    let scheduler = RecurringScheduler::new(store.clone());
    let (a, b) = tokio::join!(scheduler.run_at(1, 1, now()), scheduler.run_at(1, 1, now()));
    assert_eq!(a.unwrap().occurrences + b.unwrap().occurrences, 1);
    let mut update = store.begin().await.unwrap();
    update
        .update_recurring(&schedule(
            "a",
            "clock",
            now() + TimeDelta::seconds(1),
            MissedTickPolicy::SkipMissed,
        ))
        .await
        .unwrap();
    // A dispatch page never waits behind configuration locks.
    assert_eq!(
        scheduler
            .run_at(10, 10, now() + TimeDelta::seconds(2))
            .await
            .unwrap()
            .schedules,
        0
    );
    update.cancel_recurring(&key("a", "clock")).await.unwrap();
    update.commit(now()).await.unwrap();
    assert_eq!(
        scheduler
            .run_at(10, 10, now() + TimeDelta::seconds(2))
            .await
            .unwrap()
            .schedules,
        0
    );
    assert_eq!(envelopes(&pool).await.len(), 1);
}

#[test]
fn invalid_intervals_policies_and_overflow_fail_closed() {
    let routes = [Delivery::new(DeliveryKind::Local, "ticks", now())];
    for interval in [
        Duration::ZERO,
        Duration::from_nanos(1_000_001),
        Duration::MAX,
    ] {
        assert!(
            RecurringSchedule::new(
                key("a", "clock"),
                now(),
                interval,
                MissedTickPolicy::SkipMissed,
                &Tick(1),
                &routes
            )
            .is_err()
        );
    }
    assert!(
        RecurringSchedule::new(
            key("a", "clock"),
            DateTime::<Utc>::MAX_UTC,
            Duration::from_secs(1),
            MissedTickPolicy::SkipMissed,
            &Tick(1),
            &routes
        )
        .is_err()
    );
    assert!(
        RecurringSchedule::new(
            key("a", "clock"),
            now(),
            Duration::from_secs(1),
            MissedTickPolicy::CatchUp { max_occurrences: 0 },
            &Tick(1),
            &routes
        )
        .is_err()
    );
    assert!(
        RecurringSchedule::new(
            key("a", "clock"),
            now(),
            Duration::from_secs(1),
            MissedTickPolicy::SkipMissed,
            &Tick(1),
            &[]
        )
        .is_err()
    );
    assert!(ScheduleKey::new("", "clock", "x").is_err());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn cancelling_registration_during_hash_verification_poisoned_the_borrowed_scope(
    pool: PgPool,
) {
    let store = Store::new(pool.clone());
    let definition = schedule("a", "clock", now(), MissedTickPolicy::SkipMissed);
    register(&store, &definition).await;
    sqlx::query("CREATE TABLE recurring_business_effects(value integer)")
        .execute(&pool)
        .await
        .unwrap();
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("SELECT name FROM raddle_recurring WHERE tenant='a' FOR UPDATE")
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let mut scope = store.begin().await.unwrap();
    let backend: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(scope.connection())
        .await
        .unwrap();
    sqlx::query("INSERT INTO recurring_business_effects VALUES(1)")
        .execute(scope.connection())
        .await
        .unwrap();
    // ON CONFLICT DO NOTHING completes against the committed tuple. The second
    // statement, SELECT definition_hash ... FOR UPDATE, waits on our row lock.
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            scope.register_recurring(&definition)
        )
        .await
        .is_err()
    );
    let (query, waiting): (String, Option<String>) =
        sqlx::query_as("SELECT query,wait_event_type FROM pg_stat_activity WHERE pid=$1")
            .bind(backend)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        query.starts_with("SELECT definition_hash"),
        "cancellation must reach the hash-verification statement"
    );
    assert_eq!(waiting.as_deref(), Some("Lock"));
    blocker.rollback().await.unwrap();
    assert!(matches!(
        scope.commit(now()).await,
        Err(Error::RollbackOnly)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM recurring_business_effects")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_recurring")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

fn assert_serialization_failure(error: Error) {
    match error {
        Error::Database(sqlx::Error::Database(error)) => {
            assert_eq!(error.code().as_deref(), Some("40001"));
        }
        error => panic!("expected PostgreSQL serialization failure, got {error:?}"),
    }
}

async fn configuration(pool: &PgPool, key: &ScheduleKey) -> (Uuid, DateTime<Utc>, bool, Vec<u8>) {
    sqlx::query_as("SELECT incarnation,next_due,cancelled,definition_hash FROM raddle_recurring WHERE tenant=$1 AND source=$2 AND name=$3")
        .bind(key.tenant()).bind(key.source()).bind(key.name())
        .fetch_one(pool).await.unwrap()
}

async fn register_read_committed(pool: &PgPool, definition: &RecurringSchedule) {
    let transaction = pool
        .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
        .await
        .unwrap();
    let mut retry = Scope::enroll(transaction);
    retry.register_recurring(definition).await.unwrap();
    retry.commit(now()).await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn repeatable_read_registration_rejects_an_identity_inserted_after_its_snapshot(
    pool: PgPool,
) {
    let store = Store::new(pool.clone());
    let definition = schedule(
        "a",
        "clock",
        now() - TimeDelta::seconds(5),
        MissedTickPolicy::SkipMissed,
    );
    let transaction = pool
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .unwrap();
    let mut stale = Scope::enroll(transaction);
    let existed: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM raddle_recurring WHERE tenant='a' AND name='clock')",
    )
    .fetch_one(stale.connection())
    .await
    .unwrap();
    assert!(!existed);

    register(&store, &definition).await;
    let scheduler = RecurringScheduler::new(store.clone());
    assert_eq!(scheduler.run_at(1, 1, now()).await.unwrap().occurrences, 1);
    let advanced = configuration(&pool, &key("a", "clock")).await;
    assert_eq!(advanced.1, now() + TimeDelta::seconds(1));
    register(
        &store,
        &schedule("other", "healthy", now(), MissedTickPolicy::SkipMissed),
    )
    .await;

    assert_serialization_failure(stale.register_recurring(&definition).await.unwrap_err());
    assert!(matches!(
        stale.commit(now()).await,
        Err(Error::RollbackOnly)
    ));
    assert_eq!(configuration(&pool, &key("a", "clock")).await, advanced);
    // Retry the whole transaction at Read Committed, not the failed statement.
    register_read_committed(&pool, &definition).await;
    assert_eq!(configuration(&pool, &key("a", "clock")).await, advanced);
    let page = scheduler.run_at(10, 10, now()).await.unwrap();
    assert_eq!(page.schedules, 1);
    assert_eq!(page.occurrences, 1);
    assert!(page.failures.is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs WHERE tenant='a'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs WHERE tenant='other'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn repeatable_read_stale_configuration_commands_cannot_mutate_a_new_incarnation(
    pool: PgPool,
) {
    let store = Store::new(pool.clone());
    let scheduler = RecurringScheduler::new(store.clone());
    sqlx::query("CREATE TABLE recurring_configuration_effects(operation text)")
        .execute(&pool)
        .await
        .unwrap();
    for (index, operation) in ["register", "replace", "cancel"].into_iter().enumerate() {
        let schedule_key = key("a", operation);
        let original = schedule(
            "a",
            operation,
            now() - TimeDelta::seconds(30),
            MissedTickPolicy::CatchUp { max_occurrences: 2 },
        );
        register(&store, &original).await;
        let transaction = pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
            .await
            .unwrap();
        let mut stale = Scope::enroll(transaction);
        // This read fixes the snapshot but deliberately takes no row lock.
        let original_incarnation: Uuid = sqlx::query_scalar(
            "SELECT incarnation FROM raddle_recurring WHERE tenant='a' AND name=$1",
        )
        .bind(operation)
        .fetch_one(stale.connection())
        .await
        .unwrap();
        sqlx::query("INSERT INTO recurring_configuration_effects VALUES($1)")
            .bind(operation)
            .execute(stale.connection())
            .await
            .unwrap();

        let replacement = schedule(
            "a",
            operation,
            now() - TimeDelta::seconds(5),
            MissedTickPolicy::SkipMissed,
        );
        let mut concurrent = store.begin().await.unwrap();
        assert!(concurrent.update_recurring(&replacement).await.unwrap());
        concurrent.commit(now()).await.unwrap();
        assert_eq!(
            scheduler.run_at(10, 10, now()).await.unwrap().occurrences,
            1
        );
        let advanced = configuration(&pool, &schedule_key).await;
        assert_ne!(advanced.0, original_incarnation);
        assert_eq!(advanced.1, now() + TimeDelta::seconds(1));
        assert!(!advanced.2);
        register(
            &store,
            &schedule("other", operation, now(), MissedTickPolicy::SkipMissed),
        )
        .await;

        let failure = match operation {
            "register" => stale.register_recurring(&original).await.unwrap_err(),
            "replace" => stale.update_recurring(&original).await.unwrap_err(),
            "cancel" => stale.cancel_recurring(&schedule_key).await.unwrap_err(),
            _ => unreachable!(),
        };
        assert_serialization_failure(failure);
        assert!(matches!(
            stale.commit(now()).await,
            Err(Error::RollbackOnly)
        ));
        assert_eq!(configuration(&pool, &schedule_key).await, advanced);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM recurring_configuration_effects")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        // Equivalent fresh registration preserves the new definition's progress;
        // a caller must not blindly replay the stale replacement/cancellation.
        register_read_committed(&pool, &replacement).await;
        assert_eq!(configuration(&pool, &schedule_key).await, advanced);
        let page = scheduler.run_at(10, 10, now()).await.unwrap();
        assert_eq!(page.schedules, 1);
        assert_eq!(page.occurrences, 1);
        assert!(page.failures.is_empty());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs WHERE tenant='a'")
                .fetch_one(&pool)
                .await
                .unwrap(),
            (index + 1) as i64
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs WHERE tenant='other'")
                .fetch_one(&pool)
                .await
                .unwrap(),
            (index + 1) as i64
        );
    }
}
