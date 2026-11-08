//! Policy uncertainty and terminal discard share one durable custody history.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use chrono::{DateTime, Utc};
use raddle::wire::{Envelope, WireMessage};
use raddle_postgres::endpoint_policies::{PolicyRecovery, PolicySettlement};
use raddle_postgres::{Delivery, DeliveryKind, Error, JobStatus, Store};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
struct Work(u32);

impl WireMessage for Work {
    const NAME: &'static str = "test.policy-discard";
    const VERSION: u32 = 1;
}

async fn stage(
    store: &Store,
    endpoint: &str,
    envelope: &Envelope,
    now: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
) -> Uuid {
    let mut delivery = Delivery::new(DeliveryKind::Local, endpoint, now);
    delivery.partition_key = Some("ordered".into());
    delivery.expires_at = expires_at;
    let mut scope = store.begin().await.unwrap();
    let ids = scope
        .stage("source", "tenant", envelope, &[delivery])
        .await
        .unwrap();
    scope.commit(now).await.unwrap();
    ids[0]
}

async fn uncertain(pool: &PgPool, id: Uuid) -> bool {
    sqlx::query_scalar("SELECT uncertain FROM raddle_policy_attempts WHERE job_id=$1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn discarded_policy_predecessors_release_ordered_neighbors_without_replaying_success(
    pool: PgPool,
) {
    let store = Store::new(pool);
    let now = Utc::now();
    for failed in 0..3 {
        let endpoint = format!("discard-position-{failed}");
        let mut jobs = Vec::new();
        for value in 0..3 {
            let envelope = Envelope::encode(&Work(value)).unwrap();
            let id = stage(&store, &endpoint, &envelope, now, None).await;
            jobs.push((id, envelope));
        }
        for (position, (id, _)) in jobs.iter().enumerate() {
            let claims = store
                .claim_policy_for(&endpoint, 8, Duration::from_secs(30), now)
                .await
                .unwrap();
            assert_eq!(claims.len(), 1);
            let claim = &claims[0];
            assert_eq!(claim.id(), *id);
            assert_eq!(
                store
                    .begin_policy_attempt(claim, Duration::from_secs(30), now)
                    .await
                    .unwrap(),
                1
            );
            if position == failed {
                store
                    .settle_policy(claim, PolicySettlement::DeadLetter, now)
                    .await
                    .unwrap();
                assert!(
                    store
                        .discard_dead_letter("tenant", *id, "operator", "retire failed work", now)
                        .await
                        .unwrap()
                );
            } else {
                store
                    .policy_execution(claim, now)
                    .await
                    .unwrap()
                    .commit(now)
                    .await
                    .unwrap();
            }
        }
        for (position, (id, envelope)) in jobs.iter().enumerate() {
            let job = store.inspect("tenant", *id).await.unwrap().unwrap();
            assert_eq!(
                job.status,
                if position == failed {
                    JobStatus::Discarded
                } else {
                    JobStatus::Completed
                }
            );
            // Duplicate ingress retains each original tombstone and attempt count.
            assert_eq!(stage(&store, &endpoint, envelope, now, None).await, *id);
            assert_eq!(
                store
                    .inspect("tenant", *id)
                    .await
                    .unwrap()
                    .unwrap()
                    .attempts,
                job.attempts
            );
            assert_eq!(job.attempts, 1);
        }
        let fresh = stage(
            &store,
            &endpoint,
            &Envelope::encode(&Work(10)).unwrap(),
            now,
            None,
        )
        .await;
        let claims = store
            .claim_policy_for(&endpoint, 8, Duration::from_secs(30), now)
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].id(), fresh);
        store.complete(&claims[0], now).await.unwrap();
    }
    let statistics = store.statistics("tenant").await.unwrap();
    assert_eq!(statistics.completed, 9);
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn expired_uncertainty_survives_discard_and_cannot_reopen_its_tombstone(pool: PgPool) {
    let store = Store::new(pool.clone());
    let now = Utc::now();
    let expired = now + chrono::Duration::seconds(60);
    let id = stage(
        &store,
        "uncertain-discard",
        &Envelope::encode(&Work(1)).unwrap(),
        now,
        Some(now + chrono::Duration::seconds(40)),
    )
    .await;
    let claim = store
        .claim_policy_for("uncertain-discard", 1, Duration::from_secs(30), now)
        .await
        .unwrap()
        .remove(0);
    store
        .begin_policy_attempt(&claim, Duration::from_secs(30), now)
        .await
        .unwrap();
    assert!(uncertain(&pool, id).await);
    // The ordinary expiry sweep makes the input terminal, not safely replayable.
    assert!(
        store
            .claim_policy_for("uncertain-discard", 1, Duration::from_secs(30), expired)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Dead
    );
    assert!(uncertain(&pool, id).await);
    assert!(
        store
            .discard_dead_letter("tenant", id, "operator", "retire expired input", expired)
            .await
            .unwrap()
    );
    assert!(uncertain(&pool, id).await);
    for tenant in ["tenant", "other-tenant"] {
        assert!(
            !store
                .recover_policy_uncertain(
                    tenant,
                    id,
                    PolicyRecovery::RolledBack,
                    "operator",
                    "old recovery request",
                    expired,
                )
                .await
                .unwrap()
        );
        assert!(
            !store
                .replay(tenant, id, "operator", "old replay request", expired)
                .await
                .unwrap()
        );
        assert!(
            !store
                .replay_dead_letter_bounded(tenant, id, "operator", "HTTP replay", expired)
                .await
                .unwrap()
        );
    }
    assert!(matches!(
        store
            .settle_policy(&claim, PolicySettlement::QueueAt(expired), expired)
            .await,
        Err(Error::StaleClaim)
    ));
    assert!(matches!(
        store.policy_rolled_back(&claim, expired).await,
        Err(Error::StaleClaim)
    ));
    assert!(uncertain(&pool, id).await);
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Discarded
    );
    let audits: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM raddle_discard_audit WHERE job_id=$1),\
                (SELECT count(*) FROM raddle_replay_audit WHERE job_id=$1)",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(audits, (1, 0));
    assert_eq!(store.statistics("tenant").await.unwrap().completed, 0);
    let fresh = stage(
        &store,
        "uncertain-discard",
        &Envelope::encode(&Work(2)).unwrap(),
        expired,
        None,
    )
    .await;
    let claims = store
        .claim_policy_for("uncertain-discard", 8, Duration::from_secs(30), expired)
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].id(), fresh);
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn replayed_historical_uncertainty_remains_fenced_in_the_policy_claim_path(pool: PgPool) {
    let store = Store::new(pool.clone());
    let now = Utc::now();
    for bounded in [false, true] {
        let endpoint = if bounded {
            "bounded-replay"
        } else {
            "legacy-replay"
        };
        let id = stage(
            &store,
            endpoint,
            &Envelope::encode(&Work(1)).unwrap(),
            now,
            None,
        )
        .await;
        // Seed a historical dead row with conservative recovery metadata. Replay
        // owns the job state, but intentionally does not authorize policy recovery.
        sqlx::query("UPDATE raddle_jobs SET status='dead' WHERE id=$1")
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO raddle_policy_attempts(job_id,handling,uncertain) VALUES($1,1,true)",
        )
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
        let accepted = if bounded {
            store
                .replay_dead_letter_bounded("tenant", id, "operator", "retry request", now)
                .await
        } else {
            store
                .replay("tenant", id, "operator", "retry request", now)
                .await
        };
        assert!(accepted.unwrap());
        assert!(uncertain(&pool, id).await);
        assert!(
            store
                .claim_policy_for(endpoint, 8, Duration::from_secs(30), now)
                .await
                .unwrap()
                .is_empty()
        );
        let healthy_endpoint = format!("{endpoint}-healthy");
        let healthy = stage(
            &store,
            &healthy_endpoint,
            &Envelope::encode(&Work(2)).unwrap(),
            now,
            None,
        )
        .await;
        let claims = store
            .claim_policy_for(&healthy_endpoint, 8, Duration::from_secs(30), now)
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].id(), healthy);
        store.complete(&claims[0], now).await.unwrap();
    }
}
