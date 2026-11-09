use super::{Handler, Owner, Poison, Running, TrackedFlush, effects, setup};
use chrono::Utc;
use raddle_policies::{Action, MemoryStatus, ReplaySafety};
use sqlx::PgPool;
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn poisoned_connection_rollback_failure_stays_uncertain_in_both_owners_and_completion_paths(
    pool: PgPool,
) {
    let store = setup(&pool).await;
    let mut base = 0;
    for owner in [Owner::Durable, Owner::Local] {
        for returned in [false, true] {
            base += 10;
            let name = format!("lost-rollback-{base}");
            let handler = Arc::new(Handler::new(
                &pool,
                base + 1,
                Poison::LostConnection,
                returned,
            ));
            let flush = Arc::new(TrackedFlush::default());
            let running = Running::start(
                owner,
                &store,
                &name,
                Arc::clone(&handler),
                Arc::clone(&flush),
                Action::Immediate,
                ReplaySafety::Transactional,
            );
            let first = running.admit(&store, &name, base).await;
            let failed = running.admit(&store, &name, base + 1).await;
            let last = running.admit(&store, &name, base + 2).await;
            let failed_state = if matches!(owner, Owner::Durable) {
                MemoryStatus::Active
            } else {
                MemoryStatus::Interrupted
            };
            running
                .wait(
                    &store,
                    &[
                        (first, MemoryStatus::Completed),
                        (last, MemoryStatus::Completed),
                        (failed, failed_state),
                    ],
                )
                .await;
            // Admission can observe Active before the failed attempt has joined.
            tokio::time::timeout(Duration::from_secs(30), async {
                while handler.disposed.load(Ordering::SeqCst) != 3 {
                    running.alive();
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(handler.uncertain.load(Ordering::SeqCst), 1);
            assert_eq!(handler.rolled_back.load(Ordering::SeqCst), 0);
            assert_eq!(running.attempts(&pool, failed).await, 1);
            if matches!(owner, Owner::Durable) {
                let uncertain: bool = sqlx::query_scalar(
                    "SELECT uncertain FROM raddle_policy_attempts WHERE job_id=$1",
                )
                .bind(failed)
                .fetch_one(&pool)
                .await
                .unwrap();
                assert!(uncertain);
                assert!(
                    store
                        .claim_policy_for(
                            &name,
                            4,
                            Duration::from_secs(30),
                            Utc::now() + chrono::Duration::minutes(5)
                        )
                        .await
                        .unwrap()
                        .is_empty()
                );
            } else {
                let markers: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM raddle_policy_attempts WHERE job_id=$1",
                )
                .bind(failed)
                .fetch_one(&pool)
                .await
                .unwrap();
                assert_eq!(markers, 0, "memory custody must not invent durable audit");
            }
            let later = running.admit(&store, &name, base + 3).await;
            running
                .wait(
                    &store,
                    &[(later, MemoryStatus::Completed), (failed, failed_state)],
                )
                .await;
            assert_eq!(running.attempts(&pool, failed).await, 1);
            running.alive();
            running.wait_cleanup(&handler, 4).await;
            running.stop().await;
            assert_eq!(handler.calls.lock().await[&(base + 1)], 1);
            handler.assert_cleanup(4, 1, 3);
            assert_eq!(flush.0.load(Ordering::SeqCst), 3);
            effects(&pool, base, &[base, base + 2, base + 3], returned).await;
        }
    }
}
