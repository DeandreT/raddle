//! Actual pool/row waits in caller clock domains, with distinct lease/input TTLs.
use super::Work;
use chrono::Utc;
use raddle::wire::Envelope;
use raddle_postgres::endpoint_policies::PolicyRecovery;
use raddle_postgres::{Delivery, DeliveryKind, JobStatus, Store};
use sqlx::PgPool;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
enum Operation {
    BeginAttempt,
    Execution,
}
#[derive(Clone, Copy, Debug)]
enum Wait {
    Pool,
    Row,
}
#[derive(Clone, Copy, Debug)]
enum Deadline {
    Lease,
    Message,
    Neither,
}

async fn operation(
    store: &Store,
    claim: &raddle_postgres::Claim,
    op: Operation,
    now: chrono::DateTime<Utc>,
) -> Result<(), raddle_postgres::Error> {
    match op {
        Operation::BeginAttempt => {
            store
                .begin_policy_attempt(claim, Duration::from_secs(15), now)
                .await?;
        }
        Operation::Execution => {
            let scope = store.policy_execution(claim, now).await?;
            // Acquiring a scope is already permission to execute business work.
            // No test business effect is performed after a refused admission.
            scope.rollback().await?;
        }
    }
    Ok(())
}

async fn case(pool: &PgPool, shift: i64, op: Operation, wait: Wait, deadline: Deadline) {
    let store = Store::new(pool.clone());
    let destination = "clock-case";
    let logical = Utc::now() + chrono::Duration::minutes(shift);
    let mut scope = store.begin().await.unwrap();
    let mut delivery = Delivery::new(
        DeliveryKind::Local,
        destination,
        logical - chrono::Duration::seconds(1),
    );
    delivery.expires_at = Some(logical + chrono::Duration::minutes(1));
    let id = scope
        .stage(
            "source",
            "tenant",
            &Envelope::encode(&Work(1)).unwrap(),
            &[delivery],
        )
        .await
        .unwrap()[0];
    scope.commit(logical).await.unwrap();
    let claim = store
        .claim_policy_for(destination, 1, Duration::from_secs(15), logical)
        .await
        .unwrap()
        .remove(0);
    if matches!(op, Operation::Execution) {
        assert_eq!(
            store
                .begin_policy_attempt(&claim, Duration::from_secs(15), logical)
                .await
                .unwrap(),
            1
        );
    }
    let single = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let single_store = Store::new(single.clone());
    // Set the test's immutable clock boundary only once all fixture setup is done.
    // Both clocks are deliberately far from PostgreSQL wall time.
    let entered = std::time::Instant::now();
    let now = Utc::now() + chrono::Duration::minutes(shift);
    let lease = now
        + chrono::Duration::seconds(if matches!(deadline, Deadline::Lease) {
            2
        } else {
            15
        });
    let expiry = now
        + chrono::Duration::seconds(if matches!(deadline, Deadline::Message) {
            2
        } else {
            15
        });
    sqlx::query("UPDATE raddle_jobs SET lease_until=$2,expires_at=$3 WHERE id=$1")
        .bind(id)
        .bind(lease)
        .bind(expiry)
        .execute(pool)
        .await
        .unwrap();
    let result = match wait {
        Wait::Pool => {
            let mut held = single.acquire().await.unwrap();
            let pending = operation(&single_store, &claim, op, now);
            tokio::pin!(pending);
            tokio::select! { biased; _ = &mut pending => panic!("entered despite exhausted pool"), () = tokio::task::yield_now() => {} }
            sqlx::query("SELECT pg_sleep(2.2)")
                .execute(&mut *held)
                .await
                .unwrap();
            drop(held);
            pending.await
        }
        Wait::Row => {
            let mut held = pool.begin().await.unwrap();
            sqlx::query("SELECT id FROM raddle_jobs WHERE id=$1 FOR UPDATE")
                .bind(id)
                .execute(&mut *held)
                .await
                .unwrap();
            let (claim, store) = (claim.clone(), store.clone());
            let task = tokio::spawn(async move { operation(&store, &claim, op, now).await });
            tokio::time::timeout(Duration::from_secs(30), async { loop {
                let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT lease_until,expires_at FROM raddle_jobs%FOR UPDATE')").fetch_one(pool).await.unwrap();
                if waiting { break; } tokio::task::yield_now().await;
            }}).await.unwrap();
            sqlx::query("SELECT pg_sleep(2.2)")
                .execute(&mut *held)
                .await
                .unwrap();
            held.rollback().await.unwrap();
            task.await.unwrap()
        }
    };
    let accepted = matches!(deadline, Deadline::Neither);
    if accepted {
        result.unwrap();
    } else {
        assert!(
            matches!(result, Err(raddle_postgres::Error::StaleClaim)),
            "{shift} {op:?} {wait:?} {deadline:?}"
        );
    }
    let state = store.inspect("tenant", id).await.unwrap().unwrap();
    assert_eq!(state.status, JobStatus::Active);
    assert_eq!(
        state.attempts,
        u32::from(matches!(op, Operation::Execution) || accepted)
    );
    let attempts: Option<(i64, bool)> =
        sqlx::query_as("SELECT handling,uncertain FROM raddle_policy_attempts WHERE job_id=$1")
            .bind(id)
            .fetch_optional(pool)
            .await
            .unwrap();
    if matches!(op, Operation::Execution) || accepted {
        assert_eq!(attempts, Some((1, true)));
    } else {
        assert_eq!(attempts, None);
    }
    let completed: bool =
        sqlx::query_scalar("SELECT completed_at IS NOT NULL FROM raddle_jobs WHERE id=$1")
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap();
    assert!(!completed);
    let observed = now + chrono::Duration::from_std(entered.elapsed()).unwrap();
    match deadline {
        Deadline::Message => {
            assert!(expiry <= observed);
            assert!(
                lease > observed,
                "whole message expired while its lease was still live"
            );
        }
        Deadline::Lease => {
            assert!(lease <= observed);
            assert!(
                expiry > observed,
                "lease expired independently of whole-message expiry"
            );
        }
        Deadline::Neither => {}
    }
    // Once both TTLs have passed, normal expiry quarantine is allowed, never a
    // success acknowledgement or an automatic recovery of uncertain execution.
    let later = now + chrono::Duration::minutes(1);
    assert!(
        store
            .claim_policy_for(destination, 1, Duration::from_secs(15), later)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Dead
    );
    assert!(
        !store
            .recover_policy_uncertain(
                "tenant",
                id,
                PolicyRecovery::RolledBack,
                "operator",
                "expired input",
                later
            )
            .await
            .unwrap()
    );
    single.close().await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn supplied_future_and_historical_clocks_advance_through_actual_pool_and_row_waits(
    pool: PgPool,
) {
    for shift in [-5, 5] {
        for op in [Operation::BeginAttempt, Operation::Execution] {
            for wait in [Wait::Pool, Wait::Row] {
                for deadline in [Deadline::Lease, Deadline::Message, Deadline::Neither] {
                    case(&pool, shift, op, wait, deadline).await;
                }
            }
        }
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn message_expiry_after_attempt_entry_is_rechecked_after_pool_and_row_waits_with_live_lease(
    pool: PgPool,
) {
    for wait in [Wait::Pool, Wait::Row] {
        case(&pool, 0, Operation::Execution, wait, Deadline::Message).await;
    }
}
