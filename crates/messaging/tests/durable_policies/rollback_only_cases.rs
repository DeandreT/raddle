use super::{Handler, Owner, Poison, Running, TrackedFlush, effects, setup};
use raddle_policies::{Action, MemoryStatus, ReplaySafety};
use sqlx::PgPool;
use std::sync::{Arc, atomic::Ordering};

async fn matrix(pool: PgPool, poison: Poison) {
    let store = setup(&pool).await;
    let mut base = 0;
    for owner in [Owner::Durable, Owner::Local] {
        for returned in [false, true] {
            for position in 0..3 {
                base += 10;
                let name = format!("rollback-{base}");
                let handler = Arc::new(Handler::new(
                    &pool,
                    base + position as i32,
                    poison,
                    returned,
                ));
                let flush = Arc::new(TrackedFlush::default());
                let mut running = Running::prepare(
                    owner,
                    &store,
                    &name,
                    Arc::clone(&handler),
                    Arc::clone(&flush),
                    Action::Immediate,
                    ReplaySafety::Transactional,
                );
                let mut ids = Vec::new();
                for value in base..base + 3 {
                    ids.push(running.admit(&store, &name, value).await);
                }
                running.start(&handler).await;
                let mut expected = ids
                    .iter()
                    .enumerate()
                    .map(|(index, id)| {
                        (
                            *id,
                            if index == position {
                                MemoryStatus::DeadLetter {
                                    replay_denied: false,
                                }
                            } else {
                                MemoryStatus::Completed
                            },
                        )
                    })
                    .collect::<Vec<_>>();
                running.wait(&store, &expected).await;
                running.wait_cleanup(&handler, 4).await;
                assert_eq!(handler.rolled_back.load(Ordering::SeqCst), 2);
                assert_eq!(handler.uncertain.load(Ordering::SeqCst), 0);
                assert_eq!(running.attempts(&pool, ids[position]).await, 2);
                if matches!(poison, Poison::CancelIdempotent | Poison::CancelBatch) {
                    assert_eq!(handler.partial.load(Ordering::SeqCst), 2);
                }
                if matches!(owner, Owner::Durable) {
                    let uncertain: bool = sqlx::query_scalar(
                        "SELECT uncertain FROM raddle_policy_attempts WHERE job_id=$1",
                    )
                    .bind(ids[position])
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                    assert!(!uncertain);
                }
                // A genuinely new identity arrives only after the failed attempt
                // settled and both original healthy identities completed.
                ids.push(running.admit(&store, &name, base + 3).await);
                expected.push((ids[3], MemoryStatus::Completed));
                running.wait(&store, &expected).await;
                let good = (base..base + 4)
                    .filter(|value| *value != handler.victim)
                    .collect::<Vec<_>>();
                effects(&pool, base, &good, returned).await;
                running.wait_cleanup(&handler, 5).await;
                handler.assert_cleanup(5, 2, 3);
                assert_eq!(flush.0.load(Ordering::SeqCst), 3);

                handler.corrected.store(true, Ordering::SeqCst);
                running.retry(&store, ids[position]).await;
                expected[position].1 = MemoryStatus::Completed;
                running.wait(&store, &expected).await;
                // State settles before disposal for committed roots. Joining the
                // Host also joins the final cleanup before checking exact counts.
                running.alive();
                for (index, id) in ids.iter().enumerate() {
                    assert_eq!(
                        running.attempts(&pool, *id).await,
                        if index == position { 3 } else { 1 }
                    );
                }
                running.wait_cleanup(&handler, 6).await;
                running.stop().await;
                let calls = handler.calls.lock().await;
                for index in 0..4 {
                    assert_eq!(
                        calls[&(base + index as i32)],
                        if index == position { 3 } else { 1 }
                    );
                }
                handler.assert_cleanup(6, 2, 4);
                assert_eq!(flush.0.load(Ordering::SeqCst), 4);
                effects(&pool, base, &(base..base + 4).collect::<Vec<_>>(), returned).await;
            }
        }
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn validation_poison_success_obeys_typed_bounded_policy_in_both_owners(pool: PgPool) {
    matrix(pool, Poison::Invalid).await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn cancelled_idempotent_inner_stage_success_is_local_in_both_owners_and_completion_paths(
    pool: PgPool,
) {
    matrix(pool, Poison::CancelIdempotent).await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn cancelled_multi_delivery_inner_stage_success_is_local_in_both_owners_and_completion_paths(
    pool: PgPool,
) {
    matrix(pool, Poison::CancelBatch).await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn exact_rollback_only_terminal_policy_can_discard_without_committing_root(pool: PgPool) {
    let store = setup(&pool).await;
    let mut base = 0;
    for owner in [Owner::Durable, Owner::Local] {
        for returned in [false, true] {
            base += 10;
            let name = format!("discard-{base}");
            let handler = Arc::new(Handler::new(&pool, base, Poison::Invalid, returned));
            let flush = Arc::new(TrackedFlush::default());
            let mut running = Running::prepare(
                owner,
                &store,
                &name,
                Arc::clone(&handler),
                Arc::clone(&flush),
                Action::Discard,
                ReplaySafety::Transactional,
            );
            let failed = running.admit(&store, &name, base).await;
            running.start(&handler).await;
            running
                .wait(
                    &store,
                    &[(
                        failed,
                        if matches!(owner, Owner::Durable) {
                            MemoryStatus::Completed
                        } else {
                            MemoryStatus::Discarded
                        },
                    )],
                )
                .await;
            if matches!(owner, Owner::Durable) {
                let state: (Option<String>, bool) = sqlx::query_as("SELECT j.last_error,p.uncertain FROM raddle_jobs j JOIN raddle_policy_attempts p ON j.id=p.job_id WHERE j.id=$1").bind(failed).fetch_one(&pool).await.unwrap();
                assert_eq!(state, (Some("policy-discard".to_owned()), false));
            }
            assert_eq!(running.attempts(&pool, failed).await, 1);
            running.alive();
            running.stop().await;
            assert_eq!(handler.rolled_back.load(Ordering::SeqCst), 1);
            assert_eq!(handler.uncertain.load(Ordering::SeqCst), 0);
            handler.assert_cleanup(1, 1, 0);
            assert_eq!(flush.0.load(Ordering::SeqCst), 0);
            effects(&pool, base, &[], returned).await;
        }
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn confirmed_abort_does_not_override_never_replay_authorization(pool: PgPool) {
    let store = setup(&pool).await;
    let mut base = 0;
    for owner in [Owner::Durable, Owner::Local] {
        for returned in [false, true] {
            base += 10;
            let name = format!("never-{base}");
            let handler = Arc::new(Handler::new(&pool, base, Poison::Invalid, returned));
            let flush = Arc::new(TrackedFlush::default());
            let mut running = Running::prepare(
                owner,
                &store,
                &name,
                Arc::clone(&handler),
                Arc::clone(&flush),
                Action::Immediate,
                ReplaySafety::Never,
            );
            let failed = running.admit(&store, &name, base).await;
            running.start(&handler).await;
            running
                .wait(
                    &store,
                    &[(
                        failed,
                        MemoryStatus::DeadLetter {
                            replay_denied: true,
                        },
                    )],
                )
                .await;
            assert_eq!(running.attempts(&pool, failed).await, 1);
            running.stop().await;
            handler.assert_cleanup(1, 1, 0);
            assert_eq!(handler.rolled_back.load(Ordering::SeqCst), 1);
            assert_eq!(handler.calls.lock().await[&base], 1);
            assert_eq!(flush.0.load(Ordering::SeqCst), 0);
            effects(&pool, base, &[], returned).await;
        }
    }
}
