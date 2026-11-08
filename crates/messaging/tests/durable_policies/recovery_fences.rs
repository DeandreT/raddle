//! Reconciliation must recheck actual post-wait caller time before any mutation.
use super::Work;
use chrono::{DateTime, Utc};
use raddle::wire::Envelope;
use raddle_postgres::endpoint_policies::PolicyRecovery;
use raddle_postgres::{Delivery, DeliveryKind, Store};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
enum Wait {
    Pool,
    Row,
}
type State = (
    String,
    Option<Uuid>,
    i64,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    DateTime<Utc>,
    i64,
    bool,
    i64,
    Option<String>,
    Option<DateTime<Utc>>,
);
async fn state(pool: &PgPool, id: Uuid) -> State {
    sqlx::query_as("SELECT j.status,j.owner,j.generation,j.lease_until,j.expires_at,j.available_at,p.handling,p.uncertain,(SELECT count(*) FROM raddle_replay_audit a WHERE a.job_id=j.id),j.last_error,j.completed_at FROM raddle_jobs j JOIN raddle_policy_attempts p ON p.job_id=j.id WHERE j.id=$1").bind(id).fetch_one(pool).await.unwrap()
}
async fn recover(
    store: &Store,
    id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, raddle_postgres::Error> {
    store
        .recover_policy_uncertain(
            "tenant",
            id,
            PolicyRecovery::RolledBack,
            "operator",
            "verified failed transaction",
            now,
        )
        .await
}
async fn case(pool: &PgPool, shift: i64, wait: Wait, expires: bool, renew: bool) {
    let store = Store::new(pool.clone());
    let destination = format!("recovery-{}", Uuid::new_v4());
    let initial = Utc::now() + chrono::Duration::minutes(shift);
    let mut scope = store.begin().await.unwrap();
    let mut delivery = Delivery::new(
        DeliveryKind::Local,
        &destination,
        initial - chrono::Duration::seconds(1),
    );
    delivery.expires_at = Some(initial + chrono::Duration::minutes(1));
    let id = scope
        .stage(
            "source",
            "tenant",
            &Envelope::encode(&Work(1)).unwrap(),
            &[delivery],
        )
        .await
        .unwrap()[0];
    scope.commit(initial).await.unwrap();
    let claim = store
        .claim_policy_for(&destination, 1, Duration::from_secs(15), initial)
        .await
        .unwrap()
        .remove(0);
    store
        .begin_policy_attempt(&claim, Duration::from_secs(15), initial)
        .await
        .unwrap();
    let single = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let recovering = Store::new(single.clone());
    let entered = std::time::Instant::now();
    let now = Utc::now() + chrono::Duration::minutes(shift);
    sqlx::query("UPDATE raddle_jobs SET lease_until=$2,expires_at=$3 WHERE id=$1")
        .bind(id)
        .bind(now - chrono::Duration::seconds(1))
        .bind(now + chrono::Duration::seconds(if expires { 4 } else { 60 }))
        .execute(pool)
        .await
        .unwrap();
    let before = state(pool, id).await;
    assert!(
        store
            .recover_policy_uncertain(
                "tenant",
                id,
                PolicyRecovery::RolledBack,
                "",
                "bad actor",
                now
            )
            .await
            .is_err()
    );
    assert!(
        !store
            .recover_policy_uncertain(
                "other-tenant",
                id,
                PolicyRecovery::RolledBack,
                "operator",
                "wrong tenant",
                now
            )
            .await
            .unwrap()
    );
    assert_eq!(state(pool, id).await, before);
    let renewed = now + chrono::Duration::seconds(60);
    let mut expected = before.clone();
    if renew {
        expected.3 = Some(DateTime::from_timestamp_micros(renewed.timestamp_micros()).unwrap());
    }
    let result = match wait {
        Wait::Pool => {
            let mut held = single.acquire().await.unwrap();
            let pending = recover(&recovering, id, now);
            tokio::pin!(pending);
            tokio::select! {biased; _=&mut pending=>panic!("recovery entered exhausted pool"),()=tokio::task::yield_now()=>{}}
            assert!(
                entered.elapsed() < Duration::from_secs(4),
                "fixture expired before the admission wait began"
            );
            if renew {
                sqlx::query("UPDATE raddle_jobs SET lease_until=$2 WHERE id=$1")
                    .bind(id)
                    .bind(renewed)
                    .execute(pool)
                    .await
                    .unwrap();
            }
            sqlx::query("SELECT pg_sleep($1)")
                .bind(if expires { 4.2f64 } else { 2.2f64 })
                .execute(&mut *held)
                .await
                .unwrap();
            drop(held);
            pending.await.unwrap()
        }
        Wait::Row => {
            let mut held = pool.begin().await.unwrap();
            sqlx::query("SELECT id FROM raddle_jobs WHERE id=$1 FOR UPDATE")
                .bind(id)
                .execute(&mut *held)
                .await
                .unwrap();
            let task = tokio::spawn(async move { recover(&recovering, id, now).await });
            tokio::time::timeout(Duration::from_secs(30),async {loop {
                let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%FROM raddle_jobs j%FOR UPDATE%')").fetch_one(pool).await.unwrap();
                if waiting {break;} tokio::task::yield_now().await;
            }}).await.unwrap();
            assert!(
                entered.elapsed() < Duration::from_secs(4),
                "fixture expired before the actual row wait began"
            );
            if renew {
                sqlx::query("UPDATE raddle_jobs SET lease_until=$2 WHERE id=$1")
                    .bind(id)
                    .bind(renewed)
                    .execute(&mut *held)
                    .await
                    .unwrap();
            }
            sqlx::query("SELECT pg_sleep($1)")
                .bind(if expires { 4.2f64 } else { 2.2f64 })
                .execute(&mut *held)
                .await
                .unwrap();
            held.commit().await.unwrap();
            task.await.unwrap().unwrap()
        }
    };
    assert_eq!(
        result,
        !expires && !renew,
        "{shift} {wait:?}: recovery crossed expiry during a real admission wait"
    );
    if expires || renew {
        assert_eq!(
            state(pool, id).await,
            expected,
            "denied recovery changed custody, uncertainty or audit"
        );
    } else {
        let after = state(pool, id).await;
        assert_eq!(after.0, "ready");
        assert_eq!(after.1, None);
        assert_eq!(after.2, before.2);
        assert_eq!(after.3, None);
        assert_eq!(after.6, 1);
        assert!(!after.7);
        assert_eq!(after.8, 1);
        assert_eq!(after.9.as_deref(), Some("policy-reconciled"));
        assert_eq!(after.10, None);
        let audited: DateTime<Utc> =
            sqlx::query_scalar("SELECT replayed_at FROM raddle_replay_audit WHERE job_id=$1")
                .bind(id)
                .fetch_one(pool)
                .await
                .unwrap();
        assert!(audited >= now + chrono::Duration::seconds(2));
        assert_eq!(audited, after.5);
        assert_eq!(audited.timestamp_subsec_nanos() % 1000, 0);
    }
    single.close().await;
}
#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn expired_recovery_after_pool_or_row_wait_never_mutates_taint_custody_or_audit(
    pool: PgPool,
) {
    for shift in [5, 0, -5] {
        for wait in [Wait::Row, Wait::Pool] {
            case(&pool, shift, wait, true, false).await;
        }
    }
}
#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn valid_recovery_advances_supplied_clock_after_pool_and_row_waits(pool: PgPool) {
    for shift in [-5, 0, 5] {
        for wait in [Wait::Pool, Wait::Row] {
            case(&pool, shift, wait, false, false).await;
        }
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn renewed_lease_during_recovery_wait_refuses_mutation_in_both_clock_domains(pool: PgPool) {
    for shift in [-5, 5] {
        for wait in [Wait::Row, Wait::Pool] {
            case(&pool, shift, wait, false, true).await;
        }
    }
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn recovery_shared_pool_failure_propagates_instead_of_false_denial(pool: PgPool) {
    let own = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let store = Store::new(own.clone());
    own.close().await;
    assert!(matches!(
        recover(&store, Uuid::new_v4(), Utc::now()).await,
        Err(raddle_postgres::Error::Database(sqlx::Error::PoolClosed))
    ));
}
