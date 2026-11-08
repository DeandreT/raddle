//! Tracking observes authoritative discard without settling healthy batch siblings.
#![cfg(feature = "postgres")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeSet;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use raddle::wire::{Envelope, WireMessage};
use raddle_batching::postgres::{BatchStore, Error, TrackedOutcome, TrackingSession};
use raddle_batching::{BatchKey, Decision, ExecutionQueue, Options, Resolution};
use raddle_postgres::{Delivery, DeliveryKind, JobStatus, Store};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Message {
    value: u32,
}
impl WireMessage for Message {
    const NAME: &'static str = "tracking.discard";
    const VERSION: u32 = 1;
}

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000, 0).unwrap()
}
fn queue() -> ExecutionQueue {
    ExecutionQueue::Independent("tracked-batch".into())
}
fn interval() -> Duration {
    Duration::from_millis(1)
}
fn timeout() -> Duration {
    Duration::from_secs(30)
}

async fn setup(pool: &PgPool) {
    Store::new(pool.clone()).migrate().await.unwrap();
    sqlx::query("CREATE TABLE effects(delivery_id uuid PRIMARY KEY,value integer NOT NULL)")
        .execute(pool)
        .await
        .unwrap();
}

async fn admit(pool: &PgPool, values: &[u32]) -> Vec<Uuid> {
    let mut scope = Store::new(pool.clone()).begin().await.unwrap();
    let mut ids = Vec::new();
    for value in values {
        let envelope = Envelope::encode(&Message { value: *value }).unwrap();
        let delivery = Delivery::new(DeliveryKind::Local, "tracked-batch", now());
        ids.extend(
            scope
                .stage_received("tracking-test", "tenant", &envelope, &[delivery])
                .await
                .unwrap(),
        );
    }
    scope.commit(now()).await.unwrap();
    ids
}

async fn assemble(batches: &BatchStore, count: usize, at: DateTime<Utc>) {
    batches
        .assemble(
            &queue(),
            &BatchKey::new("tenant", None).unwrap(),
            Options::new(count, Duration::from_millis(250), 256).unwrap(),
            at,
            |_| Ok(None),
        )
        .await
        .unwrap()
        .unwrap();
}

async fn settle(batches: &BatchStore, at: DateTime<Utc>, decisions: &[(Uuid, Decision)]) {
    let claim = batches
        .claim(&queue(), at, Duration::from_secs(600))
        .await
        .unwrap()
        .unwrap();
    let mut execution = batches.execution(&claim, at, timeout()).await.unwrap();
    let resolution = Resolution::named(execution.members(), decisions.iter().cloned()).unwrap();
    let (view, scope) = execution.parts();
    for member in view.input() {
        if matches!(
            resolution.decisions().get(&member.context.delivery_id),
            Some(Decision::Commit)
        ) {
            sqlx::query("INSERT INTO effects VALUES($1,$2)")
                .bind(member.context.delivery_id)
                .bind(member.value.decode::<Message>().unwrap().value as i32)
                .execute(scope.connection())
                .await
                .unwrap();
        }
    }
    execution.settle(resolution, at, at).await.unwrap();
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn discard_does_not_abort_observation_of_retryable_batch_sibling(pool: PgPool) {
    setup(&pool).await;
    let batches = BatchStore::new(pool.clone());
    let canonical = Store::new(pool.clone());
    let ids = admit(&pool, &[0, 1]).await;
    let session = TrackingSession::new("tenant", ids.clone()).unwrap();
    assemble(&batches, 2, now()).await;
    settle(
        &batches,
        now(),
        &[
            (ids[0], Decision::Poison("invalid-order".into())),
            (ids[1], Decision::Retry),
        ],
    )
    .await;
    assert!(
        canonical
            .discard_dead_letter(
                "tenant",
                ids[0],
                "operator",
                "retire poison original",
                now()
            )
            .await
            .unwrap()
    );

    let pending = session
        .wait(&batches, interval(), Duration::from_secs(1))
        .await;
    assert!(
        matches!(pending, Err(Error::TrackingTimeout)),
        "healthy retryable work must remain observed; got {pending:?}"
    );
    settle(
        &batches,
        now() + TimeDelta::seconds(1),
        &[(ids[1], Decision::Commit)],
    )
    .await;
    let outcome = session.wait(&batches, interval(), timeout()).await.unwrap();
    assert_eq!(outcome.completed, [ids[1]]);
    assert!(outcome.dead.is_empty());
    assert_eq!(outcome.discarded, [ids[0]]);
}

#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    jobs: Vec<String>,
    batches: Vec<String>,
    members: Vec<String>,
    discards: Vec<String>,
    replays: Vec<String>,
    effects: Vec<(Uuid, i32)>,
}

async fn snapshot(pool: &PgPool) -> Snapshot {
    Snapshot {
        jobs: sqlx::query_scalar("SELECT to_jsonb(j)::text FROM raddle_jobs j ORDER BY sequence")
            .fetch_all(pool)
            .await
            .unwrap(),
        batches: sqlx::query_scalar("SELECT to_jsonb(b)::text FROM raddle_batches b ORDER BY id")
            .fetch_all(pool)
            .await
            .unwrap(),
        members: sqlx::query_scalar(
            "SELECT to_jsonb(m)::text FROM raddle_batch_members m ORDER BY batch_id,ordinal,job_id",
        )
        .fetch_all(pool)
        .await
        .unwrap(),
        discards: sqlx::query_scalar(
            "SELECT to_jsonb(a)::text FROM raddle_discard_audit a ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .unwrap(),
        replays: sqlx::query_scalar(
            "SELECT to_jsonb(a)::text FROM raddle_replay_audit a ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .unwrap(),
        effects: sqlx::query_as("SELECT delivery_id,value FROM effects ORDER BY value,delivery_id")
            .fetch_all(pool)
            .await
            .unwrap(),
    }
}

async fn original(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT to_jsonb(j)::text FROM raddle_jobs j WHERE id=$1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn pending_read_only(session: &TrackingSession, batches: &BatchStore, pool: &PgPool) {
    let before = snapshot(pool).await;
    let result = session
        .wait(batches, interval(), Duration::from_millis(100))
        .await;
    assert!(
        matches!(result, Err(Error::TrackingTimeout)),
        "healthy work is still pending: {result:?}"
    );
    assert_eq!(snapshot(pool).await, before);
}

fn outcomes(outcome: &TrackedOutcome, completed: &[Uuid], dead: &[Uuid], discarded: &[Uuid]) {
    for (actual, expected) in [
        (&outcome.completed, completed),
        (&outcome.dead, dead),
        (&outcome.discarded, discarded),
    ] {
        assert_eq!(actual.len(), expected.len());
        assert_eq!(
            actual.iter().copied().collect::<BTreeSet<_>>(),
            expected.iter().copied().collect::<BTreeSet<_>>()
        );
    }
}

#[test]
fn outcome_default_and_struct_update_leave_discarded_empty() {
    let empty = TrackedOutcome::default();
    outcomes(&empty, &[], &[], &[]);
    let id = Uuid::new_v4();
    let extended = TrackedOutcome {
        completed: vec![id],
        ..TrackedOutcome::default()
    };
    outcomes(&extended, &[id], &[], &[]);
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn discard_first_middle_last_keeps_all_healthy_originals_retryable_until_commit(
    pool: PgPool,
) {
    setup(&pool).await;
    let batches = BatchStore::new(pool.clone());
    let canonical = Store::new(pool.clone());
    for discarded in 0..3 {
        let base = discarded as u32 * 10;
        let ids = admit(&pool, &[base, base + 1, base + 2]).await;
        let session = TrackingSession::new("tenant", ids.clone()).unwrap();
        assemble(&batches, 3, now()).await;
        let decisions = ids
            .iter()
            .enumerate()
            .map(|(index, id)| {
                (
                    *id,
                    if index == discarded {
                        Decision::Poison("invalid-order".into())
                    } else {
                        Decision::Retry
                    },
                )
            })
            .collect::<Vec<_>>();
        settle(&batches, now(), &decisions).await;
        assert!(
            canonical
                .discard_dead_letter(
                    "tenant",
                    ids[discarded],
                    "operator",
                    "retire poison original",
                    now()
                )
                .await
                .unwrap()
        );
        let retired = original(&pool, ids[discarded]).await;
        pending_read_only(&session, &batches, &pool).await;
        let healthy = ids
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != discarded)
            .map(|(_, id)| *id)
            .collect::<Vec<_>>();
        let commits = healthy
            .iter()
            .map(|id| (*id, Decision::Commit))
            .collect::<Vec<_>>();
        settle(&batches, now() + TimeDelta::seconds(1), &commits).await;
        let before = snapshot(&pool).await;
        let result = session.wait(&batches, interval(), timeout()).await.unwrap();
        outcomes(&result, &healthy, &[], &[ids[discarded]]);
        assert_eq!(snapshot(&pool).await, before);
        assert_eq!(original(&pool, ids[discarded]).await, retired);
        for id in healthy {
            let job = canonical.inspect("tenant", id).await.unwrap().unwrap();
            assert_eq!(job.status, JobStatus::Completed);
            assert_eq!(job.attempts, 2);
        }
        let job = canonical
            .inspect("tenant", ids[discarded])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.status, JobStatus::Discarded);
        assert_eq!(job.attempts, 1);
        assert_eq!(before.discards.len(), discarded + 1);
        assert!(before.replays.is_empty());
        assert_eq!(before.effects.len(), (discarded + 1) * 2);
    }
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn mixed_terminal_current_and_new_work_survive_cancel_repoll_and_missing_checks(
    pool: PgPool,
) {
    setup(&pool).await;
    let batches = BatchStore::new(pool.clone());
    let canonical = Store::new(pool.clone());
    let ids = admit(&pool, &[0, 1, 2, 3]).await;
    let current = TrackingSession::new("tenant", ids.clone()).unwrap();
    assemble(&batches, 4, now()).await;
    settle(
        &batches,
        now(),
        &[
            (ids[0], Decision::Commit),
            (ids[1], Decision::Poison("quarantine".into())),
            (ids[2], Decision::Poison("discard-me".into())),
            (ids[3], Decision::Retry),
        ],
    )
    .await;
    assert!(
        canonical
            .discard_dead_letter(
                "tenant",
                ids[2],
                "operator",
                "retire selected original",
                now()
            )
            .await
            .unwrap()
    );
    let successful = original(&pool, ids[0]).await;
    let retired = original(&pool, ids[2]).await;
    let new = admit(&pool, &[4]).await[0];
    assert_eq!(
        canonical
            .inspect("tenant", new)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Ready
    );
    let mut all = ids.clone();
    all.push(new);
    let combined = TrackingSession::new("tenant", all.clone()).unwrap();
    pending_read_only(&current, &batches, &pool).await;
    pending_read_only(&combined, &batches, &pool).await;
    let before = snapshot(&pool).await;
    {
        let observing = combined.wait(&batches, interval(), timeout());
        tokio::pin!(observing);
        tokio::select! {
            result=&mut observing => panic!("observation must stay pending before cancellation: {result:?}"),
            ()=tokio::time::sleep(Duration::from_millis(25)) => {}
        }
    }
    assert_eq!(snapshot(&pool).await, before);
    pending_read_only(&combined, &batches, &pool).await;
    settle(
        &batches,
        now() + TimeDelta::seconds(1),
        &[(ids[3], Decision::Commit)],
    )
    .await;
    pending_read_only(&combined, &batches, &pool).await; // New admitted work is still ready.
    let old_result = current.wait(&batches, interval(), timeout()).await.unwrap();
    outcomes(&old_result, &[ids[0], ids[3]], &[ids[1]], &[ids[2]]);
    assemble(&batches, 1, now() + TimeDelta::seconds(2)).await;
    settle(
        &batches,
        now() + TimeDelta::seconds(2),
        &[(new, Decision::Commit)],
    )
    .await;
    let before = snapshot(&pool).await;
    let result = combined
        .wait(&batches, interval(), timeout())
        .await
        .unwrap();
    outcomes(&result, &[ids[0], ids[3], new], &[ids[1]], &[ids[2]]);
    assert_eq!(snapshot(&pool).await, before);
    assert_eq!(original(&pool, ids[0]).await, successful);
    assert_eq!(original(&pool, ids[2]).await, retired);
    assert_eq!(
        canonical
            .inspect("tenant", ids[0])
            .await
            .unwrap()
            .unwrap()
            .attempts,
        1
    );
    assert_eq!(
        canonical
            .inspect("tenant", ids[3])
            .await
            .unwrap()
            .unwrap()
            .attempts,
        2
    );
    assert_eq!(
        canonical
            .inspect("tenant", new)
            .await
            .unwrap()
            .unwrap()
            .attempts,
        1
    );
    assert_eq!(before.discards.len(), 1);
    assert!(before.replays.is_empty());
    assert_eq!(before.effects.len(), 3);

    for (tenant, tracked) in [
        ("other-tenant", all.clone()),
        ("tenant", vec![ids[2], Uuid::new_v4()]),
    ] {
        let invalid = TrackingSession::new(tenant, tracked).unwrap();
        assert!(matches!(
            invalid.wait(&batches, interval(), timeout()).await,
            Err(Error::Invalid("tracked custody missing"))
        ));
        assert_eq!(snapshot(&pool).await, before);
    }
    assert_eq!(
        canonical
            .prune_completed("tenant", now() + TimeDelta::hours(1), 1)
            .await
            .unwrap(),
        1
    );
    let pruned = snapshot(&pool).await;
    assert!(matches!(
        combined.wait(&batches, interval(), timeout()).await,
        Err(Error::Invalid("tracked custody missing"))
    ));
    assert_eq!(snapshot(&pool).await, pruned);
    assert_eq!(original(&pool, ids[2]).await, retired);
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn query_error_propagates_without_custody_or_audit_mutation(pool: PgPool) {
    setup(&pool).await;
    let batches = BatchStore::new(pool.clone());
    let canonical = Store::new(pool.clone());
    let ids = admit(&pool, &[0, 1]).await;
    assemble(&batches, 2, now()).await;
    settle(
        &batches,
        now(),
        &[
            (ids[0], Decision::Poison("discard-me".into())),
            (ids[1], Decision::Retry),
        ],
    )
    .await;
    assert!(
        canonical
            .discard_dead_letter("tenant", ids[0], "operator", "retire poison", now())
            .await
            .unwrap()
    );
    let session = TrackingSession::new("tenant", ids).unwrap();
    let before = snapshot(&pool).await;
    // Session-local configuration makes the real SELECT fail. No canonical
    // table, permission, role, shared server or connection URL is changed.
    let observer = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET search_path TO pg_catalog")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let result = session
        .wait(&BatchStore::new(observer.clone()), interval(), timeout())
        .await;
    match result {
        Err(Error::Database(sqlx::Error::Database(error))) => {
            assert_eq!(error.code().as_deref(), Some("42P01"))
        }
        other => panic!("expected the actual SQLx undefined-table error, got {other:?}"),
    }
    observer.close().await;
    assert_eq!(snapshot(&pool).await, before);
    pending_read_only(&session, &batches, &pool).await;
}
