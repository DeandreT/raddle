//! Real server COMMIT rejection must not take down a NEVER-restart Host.
use super::{Confirmed, FailedFlush, Handler, Work, endpoint, policies, schema, stage};
use chrono::Utc;
use raddle::wire::Envelope;
use raddle_host::Host;
use raddle_messaging::scoped::LocalOnlyFlush;
use raddle_messaging::{EndpointPolicies, MessageContext};
use raddle_messaging::{PolicyQueue, PolicyQueueBuilder, TrustedOrigin};
use raddle_policies::MemoryStatus;
use raddle_policies::{Action, PolicySet, ReplaySafety};
use raddle_postgres::{JobStatus, RetryPolicy, Store};
use sqlx::PgPool;
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;
use uuid::Uuid;

async fn submit(queue: &PolicyQueue, value: i32) -> Uuid {
    let envelope = Envelope::encode(&Work(value)).unwrap();
    let id = envelope.id();
    queue
        .submit(MessageContext::new(
            TrustedOrigin::authenticated("tenant", "source", None).unwrap(),
            envelope,
            Utc::now(),
        ))
        .await
        .unwrap();
    id
}

async fn wait_settled(
    store: &Store,
    ids: &[Uuid],
    queue: &PolicyQueue,
    count: usize,
    allow_dead: bool,
) {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let mut settled = true;
            for id in ids {
                let status = store.inspect("tenant", *id).await.unwrap().unwrap().status;
                settled &=
                    status == JobStatus::Completed || (allow_dead && status == JobStatus::Dead);
            }
            let states = queue.deliveries().unwrap();
            if settled
                && states.len() == count
                && states.iter().all(|state| {
                    state.status == MemoryStatus::Completed
                        || (allow_dead && matches!(state.status, MemoryStatus::DeadLetter { .. }))
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn deferred_unique_rejection_is_order_local_for_both_host_runners_and_later_arrivals(
    pool: PgPool,
) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    for failed in 0..3 {
        let base = failed * 100;
        let name = format!("commit-durable-{failed}");
        let mut h = Handler::new(base + failed);
        h.duplicate_at_commit = true;
        let durable_handler = Arc::new(h);
        let durable = endpoint(
            &store,
            &name,
            3,
            Arc::clone(&durable_handler),
            EndpointPolicies::new(PolicySet::default()),
            Arc::new(LocalOnlyFlush),
        );

        let mut h = Handler::new(base + 10 + failed);
        h.duplicate_at_commit = true;
        let local_handler = Arc::new(h);
        let mut builder = PolicyQueueBuilder::new(
            &format!("commit-local-{failed}"),
            8,
            Duration::from_secs(30),
            EndpointPolicies::new(PolicySet::default()),
        )
        .unwrap();
        builder
            .scoped::<Work, _>(
                Arc::clone(&local_handler),
                store.clone(),
                Arc::new(LocalOnlyFlush),
                ReplaySafety::Transactional,
            )
            .unwrap();
        let (queue, runner) = builder.build().unwrap();
        let sibling_name = format!("commit-sibling-{failed}");
        let sibling_handler = Arc::new(Handler::new(-1));
        let sibling = endpoint(
            &store,
            &sibling_name,
            1,
            Arc::clone(&sibling_handler),
            EndpointPolicies::new(PolicySet::default()),
            Arc::new(LocalOnlyFlush),
        );
        let mut ids = Vec::new();
        let mut local_ids = Vec::new();
        for index in 0..3 {
            ids.push(stage(&store, &name, base + index).await);
            local_ids.push(submit(&queue, base + 10 + index).await);
        }
        let mut host = Host::new(Duration::from_secs(10)).unwrap();
        // All three attach APIs use RestartPolicy::NEVER. No restart masks loss.
        durable.attach(&mut host, Duration::from_millis(5)).unwrap();
        runner
            .attach(&mut host, &format!("commit-local-{failed}"))
            .unwrap();
        sibling.attach(&mut host, Duration::from_millis(5)).unwrap();
        let stop = host.handle();
        let task = tokio::spawn(host.run());
        wait_settled(&store, &ids, &queue, 3, true).await;
        assert!(!task.is_finished());
        let states = queue.deliveries().unwrap();
        for index in 0..3 {
            assert_eq!(
                store
                    .inspect("tenant", ids[index])
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                if index == failed as usize {
                    JobStatus::Dead
                } else {
                    JobStatus::Completed
                }
            );
            if index == failed as usize {
                assert!(matches!(
                    states[index].status,
                    MemoryStatus::DeadLetter { .. }
                ));
            } else {
                assert_eq!(states[index].status, MemoryStatus::Completed);
            }
        }

        // New work arrives only AFTER both failures have settled, on each
        // original runner and a separately registered sibling service.
        ids.push(stage(&store, &name, base + 3).await);
        let sibling_id = stage(&store, &sibling_name, base + 20).await;
        submit(&queue, base + 13).await;
        let mut with_sibling = ids.clone();
        with_sibling.push(sibling_id);
        wait_settled(&store, &with_sibling, &queue, 4, true).await;
        assert_eq!(
            store
                .inspect("tenant", ids[3])
                .await
                .unwrap()
                .unwrap()
                .status,
            JobStatus::Completed
        );
        assert_eq!(
            store
                .inspect("tenant", sibling_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            JobStatus::Completed
        );
        assert_eq!(
            queue.deliveries().unwrap()[3].status,
            MemoryStatus::Completed
        );
        assert!(!task.is_finished());

        durable_handler.corrected.store(true, Ordering::SeqCst);
        local_handler.corrected.store(true, Ordering::SeqCst);
        let rejected: (i64, bool) =
            sqlx::query_as("SELECT handling,uncertain FROM raddle_policy_attempts WHERE job_id=$1")
                .bind(ids[failed as usize])
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(rejected, (1, false));
        assert!(
            store
                .replay(
                    "tenant",
                    ids[failed as usize],
                    "operator",
                    "corrected constraint input",
                    Utc::now()
                )
                .await
                .unwrap()
        );
        queue
            .retry_dead_letter(local_ids[failed as usize])
            .await
            .unwrap();
        wait_settled(&store, &with_sibling, &queue, 4, false).await;
        assert_eq!(
            store
                .inspect("tenant", ids[failed as usize])
                .await
                .unwrap()
                .unwrap()
                .status,
            JobStatus::Completed
        );
        assert!(
            queue
                .deliveries()
                .unwrap()
                .iter()
                .all(|s| s.status == MemoryStatus::Completed)
        );
        assert!(!task.is_finished());
        stop.shutdown();
        task.await.unwrap().unwrap();
        for (index, state) in queue.deliveries().unwrap().iter().enumerate() {
            let expected = if index == failed as usize { 2 } else { 1 };
            assert_eq!(state.attempts.handling, expected);
            let persisted: i64 =
                sqlx::query_scalar("SELECT handling FROM raddle_policy_attempts WHERE job_id=$1")
                    .bind(ids[index])
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(persisted, expected as i64);
        }
        for handler in [&durable_handler, &local_handler] {
            assert_eq!(handler.calls.load(Ordering::SeqCst), 5);
            assert_eq!(handler.finalized.load(Ordering::SeqCst), 5);
            assert_eq!(handler.disposed.load(Ordering::SeqCst), 5);
            assert_eq!(handler.commit_rejections.load(Ordering::SeqCst), 1);
            assert_eq!(handler.uncertain_completions.load(Ordering::SeqCst), 0);
            assert_eq!(handler.after_commit.load(Ordering::SeqCst), 4);
        }
        assert_eq!(sibling_handler.calls.load(Ordering::SeqCst), 1);
        let rows: Vec<i32> =
            sqlx::query_scalar("SELECT id FROM business WHERE id BETWEEN $1 AND $1+20 ORDER BY id")
                .bind(base)
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            rows,
            [
                base,
                base + 1,
                base + 2,
                base + 3,
                base + 10,
                base + 11,
                base + 12,
                base + 13,
                base + 20
            ]
        );
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn postgres_unknown_completion_sqlstate_is_not_a_known_commit_rejection(pool: PgPool) {
    schema(&pool).await;
    sqlx::raw_sql("CREATE FUNCTION unknown_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='40003'; END $$; CREATE CONSTRAINT TRIGGER unknown_commit AFTER INSERT ON business DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION unknown_commit();").execute(&pool).await.unwrap();
    let store = Store::new(pool.clone());
    let handler = Arc::new(Handler::new(-1));
    let endpoint = endpoint(
        &store,
        "unknown-commit",
        1,
        Arc::clone(&handler),
        policies(Action::Immediate),
        Arc::new(LocalOnlyFlush),
    );
    let id = stage(&store, "unknown-commit", 42).await;
    assert_eq!(endpoint.run_once().await.unwrap(), 1);
    assert_eq!(*handler.uncertain_sqlstates.lock().await, ["40003"]);
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    assert_eq!(handler.uncertain_completions.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Active
    );
    assert!(
        store
            .claim_policy_for(
                "unknown-commit",
                1,
                Duration::from_secs(15),
                Utc::now() + chrono::Duration::minutes(5)
            )
            .await
            .unwrap()
            .is_empty()
    );
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn local_host_postcommit_flush_failure_never_replays_committed_root(pool: PgPool) {
    schema(&pool).await;
    let store = Store::new(pool.clone());
    let mut h = Handler::new(-1);
    h.outgoing = true;
    let handler = Arc::new(h);
    let mut builder = PolicyQueueBuilder::new(
        "local-postcommit",
        8,
        Duration::from_secs(30),
        policies(Action::Immediate),
    )
    .unwrap();
    builder
        .scoped::<Work, _>(
            Arc::clone(&handler),
            store.clone(),
            Arc::new(FailedFlush),
            ReplaySafety::Transactional,
        )
        .unwrap();
    let (queue, runner) = builder.build().unwrap();
    submit(&queue, 1).await;
    let mut host = Host::new(Duration::from_secs(10)).unwrap();
    runner.attach(&mut host, "local-postcommit").unwrap();
    let stop = host.handle();
    let task = tokio::spawn(host.run());
    wait_settled(&store, &[], &queue, 1, false).await;
    submit(&queue, 2).await;
    wait_settled(&store, &[], &queue, 2, false).await;
    let worker =
        raddle_postgres::OutboxWorker::new(store.clone(), Confirmed, RetryPolicy::default());
    assert_eq!(worker.run_once_for("remote", 8).await.unwrap(), 2);
    assert!(!task.is_finished());
    stop.shutdown();
    task.await.unwrap().unwrap();
    assert!(
        queue
            .deliveries()
            .unwrap()
            .iter()
            .all(|state| state.status == MemoryStatus::Completed && state.attempts.handling == 1)
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 2);
    assert_eq!(handler.after_commit.load(Ordering::SeqCst), 0);
    assert_eq!(handler.disposed.load(Ordering::SeqCst), 2);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM business")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
}
