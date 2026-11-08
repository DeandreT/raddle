//! Real PostgreSQL isolation and fencing regressions; each test owns its database.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use raddle::wire::{Envelope, WireMessage};
use raddle_host::{Host, HostError};
use raddle_postgres::{
    Claim, Delivery, DeliveryKind, Error, FailureDisposition, RetryPolicy, Scope, Store,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::{ClaimOutcome, DurableEndpoint};
use crate::effects::HandlerOutput;
use crate::scoped::{LocalOnlyFlush, ScopedHandler};
use crate::{BoxFuture, Cancellation, MessageContext};

#[derive(Serialize, Deserialize)]
struct Work(i32);
impl WireMessage for Work {
    const NAME: &'static str = "test.durable-isolation";
    const VERSION: u32 = 1;
}

#[derive(Debug, thiserror::Error)]
#[error("scripted handler failure")]
struct HandlerError;

struct Handler(AtomicUsize);
impl ScopedHandler<Work> for Handler {
    type Resources = ();
    type Reply = ();
    type Error = HandlerError;

    fn create<'a>(
        &'a self,
        _: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), HandlerError>> {
        Box::pin(async { Ok(()) })
    }

    fn handle<'a>(
        &'a self,
        work: &'a Work,
        _: &'a mut (),
        scope: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<HandlerOutput<(), ()>, HandlerError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            sqlx::query("INSERT INTO business(id) VALUES($1)")
                .bind(work.0)
                .execute(scope.connection())
                .await
                .map_err(|_| HandlerError)?;
            Ok(HandlerOutput::reply(()))
        })
    }
}

async fn endpoint(pool: &PgPool, parallelism: u32) -> (Store, DurableEndpoint, Arc<Handler>) {
    sqlx::query("CREATE TABLE business(id integer PRIMARY KEY)")
        .execute(pool)
        .await
        .unwrap();
    let store = Store::new(pool.clone());
    let mut endpoint = DurableEndpoint::new(
        store.clone(),
        "isolation",
        parallelism,
        RetryPolicy::default(),
    )
    .unwrap();
    let handler = Arc::new(Handler(AtomicUsize::new(0)));
    endpoint
        .handle::<Work, _>(Arc::clone(&handler), Arc::new(LocalOnlyFlush))
        .unwrap();
    (store, endpoint, handler)
}

async fn stage(store: &Store, value: i32) -> (Uuid, Vec<u8>) {
    let envelope = Envelope::encode(&Work(value)).unwrap();
    let mut scope = store.begin().await.unwrap();
    let ids = scope
        .stage(
            "source",
            "tenant",
            &envelope,
            &[Delivery::new(DeliveryKind::Local, "isolation", Utc::now())],
        )
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    (ids[0], envelope.to_vec().unwrap())
}

async fn snapshot(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT row_to_json(j)::text FROM raddle_jobs j WHERE id=$1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn listener(pool: &PgPool) -> PgListener {
    sqlx::raw_sql("CREATE FUNCTION notify_state() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_notify('job_state', NEW.id::text); RETURN NEW; END $$; CREATE TRIGGER notify_state AFTER UPDATE ON raddle_jobs FOR EACH ROW EXECUTE FUNCTION notify_state();")
        .execute(pool).await.unwrap();
    let mut listener = PgListener::connect_with(pool).await.unwrap();
    listener.listen("job_state").await.unwrap();
    listener
}

async fn wait_states(
    pool: &PgPool,
    listener: &mut PgListener,
    host: &mut JoinHandle<Result<(), HostError>>,
    expected: &[(Uuid, &str)],
) {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let mut matched = true;
            for (id, status) in expected {
                let actual: String = sqlx::query_scalar("SELECT status FROM raddle_jobs WHERE id=$1")
                    .bind(id).fetch_one(pool).await.unwrap();
                matched &= actual == *status;
            }
            if matched { break; }
            tokio::select! {
                result = &mut *host => panic!("durable host stopped before independent work finished: {result:?}"),
                result = listener.recv() => { result.unwrap(); },
            }
        }
    }).await.unwrap();
    assert!(
        !host.is_finished(),
        "durable host must remain available for later arrivals"
    );
}

async fn malformed_position(pool: PgPool, bad: usize, invalid_origin: bool) {
    // Parallelism one makes first/middle/last refer to actual receiver order,
    // rather than relying on UPDATE RETURNING ordering within a claim batch.
    let (store, endpoint, handler) = endpoint(&pool, 1).await;
    let mut listener = listener(&pool).await;
    let mut jobs = Vec::new();
    for value in 0..3 {
        jobs.push(stage(&store, value).await);
    }
    let ordered: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM raddle_jobs ORDER BY sequence")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(ordered, jobs.iter().map(|job| job.0).collect::<Vec<_>>());
    if invalid_origin {
        sqlx::query("UPDATE raddle_jobs SET source='' WHERE id=$1")
            .bind(jobs[bad].0)
            .execute(&pool)
            .await
            .unwrap();
    } else {
        sqlx::query("UPDATE raddle_jobs SET envelope=$2 WHERE id=$1")
            .bind(jobs[bad].0)
            .bind(b"bad persisted envelope".as_slice())
            .execute(&pool)
            .await
            .unwrap();
    }
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    endpoint
        .attach(&mut host, Duration::from_millis(5))
        .unwrap();
    let shutdown = host.handle();
    let mut task = tokio::spawn(host.run());
    let expected = jobs
        .iter()
        .enumerate()
        .map(|(i, job)| (job.0, if i == bad { "dead" } else { "completed" }))
        .collect::<Vec<_>>();
    wait_states(&pool, &mut listener, &mut task, &expected).await;
    assert_eq!(handler.0.load(Ordering::SeqCst), 2);
    let failed = store.inspect("tenant", jobs[bad].0).await.unwrap().unwrap();
    assert_eq!(
        failed.last_error.as_deref(),
        Some(if invalid_origin {
            "invalid-origin"
        } else {
            "invalid-envelope"
        })
    );
    assert_eq!(failed.attempts, 1);
    let mut successes = Vec::new();
    for (i, job) in jobs.iter().enumerate() {
        if i != bad {
            successes.push((job.0, snapshot(&pool, job.0).await));
        }
    }
    // Restore exactly the original custody data; do not alter its fingerprint
    // or relax the store's immutable identity/schedule comparison.
    sqlx::query("UPDATE raddle_jobs SET envelope=$2,source='source' WHERE id=$1")
        .bind(jobs[bad].0)
        .bind(&jobs[bad].1)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        store
            .replay(
                "tenant",
                jobs[bad].0,
                "operator",
                "correct persisted record",
                Utc::now()
            )
            .await
            .unwrap()
    );
    wait_states(
        &pool,
        &mut listener,
        &mut task,
        &[(jobs[bad].0, "completed")],
    )
    .await;
    assert_eq!(handler.0.load(Ordering::SeqCst), 3);
    assert_eq!(
        store
            .inspect("tenant", jobs[bad].0)
            .await
            .unwrap()
            .unwrap()
            .attempts,
        1
    );
    let later = stage(&store, 3).await;
    wait_states(&pool, &mut listener, &mut task, &[(later.0, "completed")]).await;
    assert_eq!(handler.0.load(Ordering::SeqCst), 4);
    for (id, before) in successes {
        assert_eq!(snapshot(&pool, id).await, before);
    }
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT id FROM business ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap(),
        [0, 1, 2, 3]
    );
    shutdown.shutdown();
    task.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn malformed_first_is_quarantined_and_host_processes_corrected_retry_and_later_arrival(
    pool: PgPool,
) {
    malformed_position(pool, 0, false).await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn malformed_middle_is_quarantined_and_host_processes_corrected_retry_and_later_arrival(
    pool: PgPool,
) {
    malformed_position(pool, 1, false).await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn malformed_last_is_quarantined_and_host_processes_corrected_retry_and_later_arrival(
    pool: PgPool,
) {
    malformed_position(pool, 2, false).await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn invalid_persisted_origin_is_quarantined_without_stopping_host(pool: PgPool) {
    malformed_position(pool, 1, true).await;
}

async fn claim(store: &Store, count: u32) -> Vec<Claim> {
    store
        .claim_for(
            DeliveryKind::Local,
            "isolation",
            count,
            Duration::from_secs(60),
            Utc::now(),
        )
        .await
        .unwrap()
}

async fn expire(pool: &PgPool, id: Uuid) {
    sqlx::query("UPDATE raddle_jobs SET lease_until=$2 WHERE id=$1")
        .bind(id)
        .bind(Utc::now() - chrono::Duration::seconds(1))
        .execute(pool)
        .await
        .unwrap();
}

async fn assert_fail_fenced(store: &Store, pool: &PgPool, old: &Claim) {
    let before = snapshot(pool, old.id()).await;
    for disposition in [FailureDisposition::Retry, FailureDisposition::Reject] {
        assert!(matches!(
            store
                .fail(
                    old,
                    disposition,
                    RetryPolicy::default(),
                    "stale-test",
                    Utc::now()
                )
                .await,
            Err(Error::StaleClaim)
        ));
        assert_eq!(snapshot(pool, old.id()).await, before);
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn expired_and_reclaimed_valid_and_malformed_claims_cannot_settle_or_release_successors(
    pool: PgPool,
) {
    let (store, endpoint, handler) = endpoint(&pool, 2).await;
    let valid = stage(&store, 1).await;
    let malformed = stage(&store, 2).await;
    sqlx::query("UPDATE raddle_jobs SET envelope=$2 WHERE id=$1")
        .bind(malformed.0)
        .bind(b"corrupt".as_slice())
        .execute(&pool)
        .await
        .unwrap();
    let old = claim(&store, 2).await;
    assert_eq!(old.len(), 2);
    for stale in &old {
        expire(&pool, stale.id()).await;
        assert_fail_fenced(&store, &pool, stale).await;
        let before = snapshot(&pool, stale.id()).await;
        assert_eq!(
            endpoint
                .process_claim(stale.clone(), Cancellation::new())
                .await
                .unwrap(),
            ClaimOutcome::OwnershipLost
        );
        assert_eq!(snapshot(&pool, stale.id()).await, before);
    }
    assert_eq!(handler.0.load(Ordering::SeqCst), 0);
    sqlx::query("UPDATE raddle_jobs SET envelope=$2 WHERE id=$1")
        .bind(malformed.0)
        .bind(&malformed.1)
        .execute(&pool)
        .await
        .unwrap();
    let successors = claim(&store, 2).await;
    assert_eq!(successors.len(), 2);
    for stale in &old {
        assert_fail_fenced(&store, &pool, stale).await;
        let before = snapshot(&pool, stale.id()).await;
        assert_eq!(
            endpoint
                .process_claim(stale.clone(), Cancellation::new())
                .await
                .unwrap(),
            ClaimOutcome::OwnershipLost
        );
        assert_eq!(snapshot(&pool, stale.id()).await, before);
    }
    assert_eq!(handler.0.load(Ordering::SeqCst), 0);
    for successor in successors {
        assert_eq!(
            endpoint
                .process_claim(successor, Cancellation::new())
                .await
                .unwrap(),
            ClaimOutcome::Completed
        );
    }
    assert_eq!(handler.0.load(Ordering::SeqCst), 2);
    for id in [valid.0, malformed.0] {
        assert_eq!(
            store.inspect("tenant", id).await.unwrap().unwrap().attempts,
            2
        );
    }
    assert_eq!(endpoint.run_once().await.unwrap(), 0);
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn real_database_failure_is_returned_only_after_healthy_current_batch_finishes(pool: PgPool) {
    let (store, endpoint, handler) = endpoint(&pool, 3).await;
    let first = stage(&store, 1).await;
    let bad = stage(&store, 2).await;
    let last = stage(&store, 3).await;
    sqlx::query("UPDATE raddle_jobs SET envelope=$2 WHERE id=$1")
        .bind(bad.0)
        .bind(b"corrupt".as_slice())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql("CREATE FUNCTION fail_quarantine() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.last_error='invalid-envelope' THEN RAISE EXCEPTION 'injected storage failure' USING ERRCODE='53100'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_quarantine BEFORE UPDATE ON raddle_jobs FOR EACH ROW EXECUTE FUNCTION fail_quarantine();")
        .execute(&pool).await.unwrap();
    let error = endpoint.run_once().await.unwrap_err();
    match error {
        Error::Database(sqlx::Error::Database(error)) => {
            assert_eq!(error.code().as_deref(), Some("53100"))
        }
        error => panic!("expected actual PostgreSQL error, got {error:?}"),
    }
    assert_eq!(handler.0.load(Ordering::SeqCst), 2);
    for id in [first.0, last.0] {
        assert_eq!(
            store.inspect("tenant", id).await.unwrap().unwrap().status,
            raddle_postgres::JobStatus::Completed
        );
    }
    let failed = store.inspect("tenant", bad.0).await.unwrap().unwrap();
    assert_eq!(failed.status, raddle_postgres::JobStatus::Active);
    assert_eq!(failed.last_error, None);
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT id FROM business ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap(),
        [1, 3]
    );
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn ownership_lost_during_handler_failure_is_not_a_host_error(pool: PgPool) {
    let (store, mut endpoint, handler) = endpoint(&pool, 1).await;
    let job = stage(&store, 1).await;
    let failure_pool = pool.clone();
    endpoint.handlers.insert(
        (Work::NAME.to_owned(), Work::VERSION),
        Box::new(move |scope, _| {
            let pool = failure_pool.clone();
            Box::pin(async move {
                scope.rollback().await.unwrap();
                // Lose ownership between rollback and failure settlement,
                // without depending on a timed lease race.
                expire(&pool, job.0).await;
                Err(FailureDisposition::Reject)
            })
        }),
    );
    let old = claim(&store, 1).await.pop().unwrap();
    assert_eq!(
        endpoint
            .process_claim(old.clone(), Cancellation::new())
            .await
            .unwrap(),
        ClaimOutcome::OwnershipLost
    );
    let before = snapshot(&pool, job.0).await;
    assert_fail_fenced(&store, &pool, &old).await;
    assert_eq!(snapshot(&pool, job.0).await, before);
    assert_eq!(
        store
            .inspect("tenant", job.0)
            .await
            .unwrap()
            .unwrap()
            .status,
        raddle_postgres::JobStatus::Active
    );
    assert_eq!(handler.0.load(Ordering::SeqCst), 0);
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn shutdown_retains_recoverable_claim_without_fabricating_completion_or_failure(
    pool: PgPool,
) {
    let (store, mut endpoint, handler) = endpoint(&pool, 1).await;
    let job = stage(&store, 1).await;
    let shutdown = Cancellation::new();
    let signal = shutdown.clone();
    endpoint.handlers.insert(
        (Work::NAME.to_owned(), Work::VERSION),
        Box::new(move |scope, _| {
            let shutdown = signal.clone();
            Box::pin(async move {
                scope.rollback().await.unwrap();
                shutdown.cancel();
                Err(FailureDisposition::Retry)
            })
        }),
    );
    let old = claim(&store, 1).await.pop().unwrap();
    let before = snapshot(&pool, job.0).await;
    assert_eq!(
        endpoint.process_claim(old, shutdown).await.unwrap(),
        ClaimOutcome::Recoverable
    );
    assert_eq!(snapshot(&pool, job.0).await, before);
    assert_eq!(handler.0.load(Ordering::SeqCst), 0);
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn retry_disposition_can_dead_letter_exhausted_attempts_without_claiming_completion(
    pool: PgPool,
) {
    let (store, endpoint, _) = endpoint(&pool, 1).await;
    let job = stage(&store, 1).await;
    sqlx::query("UPDATE raddle_jobs SET max_attempts=1 WHERE id=$1")
        .bind(job.0)
        .execute(&pool)
        .await
        .unwrap();
    let claimed = claim(&store, 1).await.pop().unwrap();
    assert_eq!(
        endpoint
            .record_failure(&claimed, FailureDisposition::Retry, "handler-failed")
            .await
            .unwrap(),
        ClaimOutcome::FailureRecorded
    );
    assert_eq!(
        store
            .inspect("tenant", job.0)
            .await
            .unwrap()
            .unwrap()
            .status,
        raddle_postgres::JobStatus::Dead
    );
}
