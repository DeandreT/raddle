use std::process::{self, Command};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use raddle::wire::Envelope;
use raddle_batching::{
    Resolution,
    postgres::{BatchStore, Error, Isolation},
};
use raddle_postgres::{Delivery, DeliveryKind, JobStatus, Store};
use sqlx::{
    ConnectOptions, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use super::{Message, effects, key, lease, now, options, queue, setup, timeout};

async fn stage(
    pool: &PgPool,
    destination: &str,
    group: &str,
    values: &[(u32, &str, u32, Option<DateTime<Utc>>)],
) -> Vec<Uuid> {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let mut scope = store.begin().await.unwrap();
    let mut ids = Vec::new();
    for (value, coalesce, budget, expiry) in values {
        let envelope = Envelope::encode(&Message {
            value: *value,
            key: coalesce.to_string(),
        })
        .unwrap();
        let mut delivery = Delivery::new(DeliveryKind::Local, destination, now());
        delivery.partition_key = Some(group.into());
        delivery.max_attempts = *budget;
        delivery.expires_at = *expiry;
        ids.extend(
            scope
                .stage_received("review", "a", &envelope, &[delivery])
                .await
                .unwrap(),
        );
    }
    scope.commit(now()).await.unwrap();
    ids
}

async fn aggregate(
    store: &BatchStore,
    destination: &str,
    group: &str,
    size: usize,
    at: DateTime<Utc>,
) -> Uuid {
    store
        .assemble(
            &queue(destination),
            &key("a", Some(group)),
            options(size),
            at,
            |envelope| Ok(Some(envelope.decode::<Message>()?.key)),
        )
        .await
        .unwrap()
        .unwrap()
}

async fn retry(store: &BatchStore, destination: &str) {
    let claim = store
        .claim(&queue(destination), now(), lease())
        .await
        .unwrap()
        .unwrap();
    let execution = store.execution(&claim, now(), timeout()).await.unwrap();
    let resolution = Resolution::named(execution.members(), []).unwrap();
    execution.settle(resolution, now(), now()).await.unwrap();
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn mixed_budgets_exhaust_entire_persisted_coalesced_unit_without_old_promotion(pool: PgPool) {
    let batches = BatchStore::new(pool.clone());
    let canonical = Store::new(pool.clone());
    for legacy_partial in [false, true] {
        let destination = if legacy_partial {
            "legacy-unit"
        } else {
            "mixed-unit"
        };
        let ids = stage(
            &pool,
            destination,
            "group",
            &[(1, "A", 5, None), (2, "A", 1, None)],
        )
        .await;
        aggregate(&batches, destination, "group", 2, now()).await;
        retry(&batches, destination).await;
        if legacy_partial {
            sqlx::query("UPDATE raddle_jobs SET status='dead',last_error='attempts-exhausted',owner=NULL,lease_until=NULL WHERE id=$1")
                .bind(ids[1]).execute(&pool).await.unwrap();
        }
        let called = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&called);
        let report = batches
            .run_once(
                &queue(destination),
                now(),
                lease(),
                timeout(),
                now(),
                Isolation::Disabled,
                move |view, _| {
                    observed.store(true, Ordering::SeqCst);
                    Box::pin(async move { Ok::<_, ()>(Resolution::committed(view.originals())) })
                },
                |_| false,
            )
            .await
            .unwrap();
        assert!(!report.claimed);
        assert!(!called.load(Ordering::SeqCst));
        for id in ids {
            let job = canonical.inspect("a", id).await.unwrap().unwrap();
            assert_eq!(job.status, JobStatus::Dead);
            assert_eq!(job.last_error.as_deref(), Some("attempts-exhausted"));
            assert_eq!(job.attempts, 1);
        }
        assert_eq!(batches.pending("a", &queue(destination)).await.unwrap(), 0);
    }
}

#[tokio::test]
async fn subprocess_review_probe() {
    let Ok(url) = std::env::var("RADDLE_BATCH_REVIEW_URL") else {
        return;
    };
    let options: PgConnectOptions = url.parse().unwrap();
    let pool = PgPool::connect_with(options.disable_statement_logging())
        .await
        .unwrap();
    let batches = BatchStore::new(pool);
    let claim = batches
        .claim(&queue("probe-gap"), now(), lease())
        .await
        .unwrap()
        .unwrap();
    assert!(batches.isolate_once(&claim, 1, now()).await.unwrap());
    let execution = batches
        .probe(&claim, now(), timeout())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        execution.members()[0]
            .value
            .decode::<Message>()
            .unwrap()
            .value,
        1
    );
    // Failed A probe: rollback business work, then persist its retry/probe progress.
    execution.rollback().await.unwrap();
    let execution = batches
        .probe(&claim, now(), timeout())
        .await
        .unwrap()
        .unwrap();
    let resolution = Resolution::named(execution.members(), []).unwrap();
    execution.settle(resolution, now(), now()).await.unwrap();
    process::exit(44); // B has not been probed and its one claim consumed its budget.
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn crash_with_last_unprobed_exhausted_resumes_already_probed_reduced_retry(pool: PgPool) {
    setup(&pool).await;
    let batches = BatchStore::new(pool.clone());
    let ids = stage(
        &pool,
        "probe-gap",
        "group",
        &[(1, "A", 5, None), (2, "B", 1, None)],
    )
    .await;
    let batch = aggregate(&batches, "probe-gap", "group", 2, now()).await;
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "review::subprocess_review_probe", "--nocapture"])
        .env(
            "RADDLE_BATCH_REVIEW_URL",
            pool.connect_options().to_url_lossy().as_str(),
        )
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(44),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut barrier = pool.begin().await.unwrap();
    sqlx::query("SET LOCAL statement_timeout='60s'")
        .execute(&mut *barrier)
        .await
        .unwrap();
    sqlx::query("SELECT id FROM raddle_jobs WHERE id=ANY($1) ORDER BY id FOR UPDATE")
        .bind(&ids)
        .fetch_all(&mut *barrier)
        .await
        .unwrap();
    barrier.commit().await.unwrap();
    let at = now() + TimeDelta::minutes(1);
    let report = batches
        .run_once(
            &queue("probe-gap"),
            at,
            lease(),
            timeout(),
            at,
            Isolation::After(1),
            |view, scope| {
                Box::pin(async move {
                    assert_eq!(view.originals().len(), 1);
                    let member = view.originals().first().unwrap();
                    assert_eq!(member.value.decode::<Message>().unwrap().value, 1);
                    assert_eq!(member.context.attempt, 2);
                    sqlx::query("INSERT INTO effects VALUES($1,1)")
                        .bind(member.context.delivery_id)
                        .execute(scope.connection())
                        .await
                        .unwrap();
                    Ok::<_, ()>(Resolution::committed(view.originals()))
                })
            },
            |_| true,
        )
        .await
        .unwrap();
    assert!(report.claimed);
    assert_eq!(report.probes, 0);
    assert_eq!(effects(&pool).await, [1]);
    let canonical = Store::new(pool.clone());
    assert_eq!(
        canonical
            .inspect("a", ids[0])
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Completed
    );
    let exhausted = canonical.inspect("a", ids[1]).await.unwrap().unwrap();
    assert_eq!(exhausted.status, JobStatus::Dead);
    assert_eq!(exhausted.attempts, 1);
    let row = sqlx::query("SELECT status,probing,owner FROM raddle_batches WHERE id=$1")
        .bind(batch)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<&str, _>("status"), "completed");
    assert!(!row.get::<bool, _>("probing"));
    assert!(row.get::<Option<Uuid>, _>("owner").is_none());
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn claim_quarantine_fault_first_middle_last_preserves_unit_and_fair_progress(pool: PgPool) {
    setup(&pool).await;
    sqlx::query("CREATE TABLE claim_faults(id uuid PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE FUNCTION reject_claim_quarantine() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='dead' AND EXISTS(SELECT 1 FROM claim_faults WHERE id=NEW.id) THEN RAISE EXCEPTION 'injected row-specific quarantine fault'; END IF; RETURN NEW; END $$").execute(&pool).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_claim_quarantine BEFORE UPDATE ON raddle_jobs FOR EACH ROW EXECUTE FUNCTION reject_claim_quarantine()").execute(&pool).await.unwrap();
    let batches = BatchStore::new(pool.clone());
    let canonical = Store::new(pool.clone());
    for poison in 0..3 {
        let destination = format!("fair-{poison}");
        let mut values = Vec::new();
        let mut poisoned = Vec::new();
        for position in 0..3 {
            if position == poison {
                poisoned.push(values.len());
                values.push((100, "P", 5, None));
                poisoned.push(values.len());
                values.push((101, "P", 1, None));
            } else {
                values.push((position, "healthy", 5, None));
            }
        }
        // Healthy keys must remain independent from one another.
        for (index, value) in values.iter_mut().enumerate() {
            if value.1 == "healthy" {
                value.1 = if index == 0 {
                    "H0"
                } else if index == 1 {
                    "H1"
                } else if index == 2 {
                    "H2"
                } else {
                    "H3"
                };
            }
        }
        let ids = stage(&pool, &destination, "group", &values).await;
        aggregate(&batches, &destination, "group", 4, now()).await;
        retry(&batches, &destination).await;
        sqlx::query("INSERT INTO claim_faults VALUES($1)")
            .bind(ids[poisoned[1]])
            .execute(&pool)
            .await
            .unwrap();
        stage(&pool, &destination, "later", &[(999, "later", 5, None)]).await;
        aggregate(
            &batches,
            &destination,
            "later",
            1,
            now() + TimeDelta::milliseconds(1),
        )
        .await;
        let first = batches
            .run_once(
                &queue(&destination),
                now(),
                lease(),
                timeout(),
                now(),
                Isolation::Disabled,
                |view, scope| {
                    Box::pin(async move {
                        assert_eq!(view.input().len(), 2);
                        for member in view.input() {
                            let value = member.value.decode::<Message>().unwrap().value;
                            assert!(value < 100);
                            sqlx::query("INSERT INTO effects VALUES($1,$2)")
                                .bind(member.context.delivery_id)
                                .bind(value as i32)
                                .execute(scope.connection())
                                .await
                                .unwrap();
                        }
                        Ok::<_, ()>(Resolution::committed(view.originals()))
                    })
                },
                |_| false,
            )
            .await
            .unwrap();
        assert_eq!(first.settlements[0].committed.len(), 2);
        for index in &poisoned {
            let job = canonical.inspect("a", ids[*index]).await.unwrap().unwrap();
            assert_eq!(job.status, JobStatus::Active);
            assert_eq!(job.attempts, 1);
            assert_eq!(
                job.last_error.as_deref(),
                Some("batch-exhaustion-quarantine-retry")
            );
        }
        // Even when the postponed oldest batch is due again, available-time
        // ordering must select the untouched later batch first.
        let later = now() + TimeDelta::seconds(1);
        let report = batches
            .run_once(
                &queue(&destination),
                later,
                lease(),
                timeout(),
                later,
                Isolation::Disabled,
                |view, scope| {
                    Box::pin(async move {
                        assert_eq!(view.input().len(), 1);
                        let member = view.input()[0];
                        assert_eq!(member.value.decode::<Message>().unwrap().value, 999);
                        sqlx::query("INSERT INTO effects VALUES($1,999)")
                            .bind(member.context.delivery_id)
                            .execute(scope.connection())
                            .await
                            .unwrap();
                        Ok::<_, ()>(Resolution::committed(view.originals()))
                    })
                },
                |_| false,
            )
            .await
            .unwrap();
        assert!(report.claimed);
        assert!(
            batches
                .claim(&queue(&destination), later, lease())
                .await
                .unwrap()
                .is_none()
        );
        for index in &poisoned {
            assert_eq!(
                canonical
                    .inspect("a", ids[*index])
                    .await
                    .unwrap()
                    .unwrap()
                    .attempts,
                1
            );
        }
        sqlx::query("DELETE FROM claim_faults WHERE id=$1")
            .bind(ids[poisoned[1]])
            .execute(&pool)
            .await
            .unwrap();
        let corrected = now() + TimeDelta::seconds(2);
        assert!(
            batches
                .claim(&queue(&destination), corrected, lease())
                .await
                .unwrap()
                .is_none()
        );
        for index in &poisoned {
            let job = canonical.inspect("a", ids[*index]).await.unwrap().unwrap();
            assert_eq!(job.status, JobStatus::Dead);
            assert_eq!(job.last_error.as_deref(), Some("attempts-exhausted"));
            assert!(
                canonical
                    .replay(
                        "a",
                        ids[*index],
                        "operator",
                        "quarantine restored; corrected retry",
                        corrected
                    )
                    .await
                    .unwrap()
            );
        }
        aggregate(&batches, &destination, "group", 2, corrected).await;
        batches
            .run_once(
                &queue(&destination),
                corrected,
                lease(),
                timeout(),
                corrected,
                Isolation::Disabled,
                |view, scope| {
                    Box::pin(async move {
                        assert_eq!(view.originals().len(), 2);
                        assert_eq!(view.input().len(), 1);
                        let member = view.input()[0];
                        assert_eq!(member.value.decode::<Message>().unwrap().value, 101);
                        sqlx::query("INSERT INTO effects VALUES($1,101)")
                            .bind(member.context.delivery_id)
                            .execute(scope.connection())
                            .await
                            .unwrap();
                        Ok::<_, ()>(Resolution::committed(view.originals()))
                    })
                },
                |_| false,
            )
            .await
            .unwrap();
        assert_eq!(batches.pending("a", &queue(&destination)).await.unwrap(), 0);
        assert_eq!(effects(&pool).await.len(), (poison as usize + 1) * 4);
    }
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn execution_refreshes_logical_time_after_pool_batch_and_original_lock_waits(pool: PgPool) {
    setup(&pool).await;
    let batches = BatchStore::new(pool.clone());
    for wait_kind in ["pool", "batch-row", "original-row"] {
        for expires in [false, true] {
            let destination = format!("{wait_kind}-{}", if expires { "expiry" } else { "lease" });
            let expiry = expires.then_some(now() + TimeDelta::minutes(5));
            let ids = stage(&pool, &destination, "group", &[(1, "A", 5, expiry)]).await;
            let batch = aggregate(&batches, &destination, "group", 1, now()).await;
            let claim = batches
                .claim(&queue(&destination), now(), Duration::from_secs(600))
                .await
                .unwrap()
                .unwrap();
            let end: DateTime<Utc> =
                sqlx::query_scalar("SELECT lease_until FROM raddle_batches WHERE id=$1")
                    .bind(batch)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let origin = expiry.unwrap_or(end) - TimeDelta::milliseconds(250);
            let small = PgPoolOptions::new()
                .max_connections(1)
                .connect_with((*pool.connect_options()).clone())
                .await
                .unwrap();
            // An already connected dedicated pool keeps the row-wait cases
            // independent of a new connection handshake consuming the deadline.
            let execution_store = BatchStore::new(small.clone());
            let held_connection = if wait_kind == "pool" {
                Some(small.acquire().await.unwrap())
            } else {
                None
            };
            let mut held_lock = if wait_kind != "pool" {
                Some(pool.begin().await.unwrap())
            } else {
                None
            };
            if let Some(lock) = held_lock.as_mut() {
                let (query, id) = if wait_kind == "batch-row" {
                    (
                        "SELECT id FROM raddle_batches WHERE id=$1 FOR UPDATE",
                        batch,
                    )
                } else {
                    ("SELECT id FROM raddle_jobs WHERE id=$1 FOR UPDATE", ids[0])
                };
                sqlx::query(query)
                    .bind(id)
                    .execute(&mut **lock)
                    .await
                    .unwrap();
            }
            let waiting = execution_store.execution(&claim, origin, timeout());
            tokio::pin!(waiting);
            tokio::select! {
                result=&mut waiting => panic!("{wait_kind}: unexpectedly completed before release; success={}",result.is_ok()),
                ()=tokio::time::sleep(Duration::from_millis(500)) => {}
            }
            drop(held_connection);
            if let Some(lock) = held_lock {
                lock.rollback().await.unwrap();
            }
            let result = waiting.await;
            if expires {
                assert!(
                    matches!(result, Err(Error::Expired)),
                    "{wait_kind}: expiry must be checked after waiting"
                );
                assert_eq!(
                    Store::new(pool.clone())
                        .inspect("a", ids[0])
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    JobStatus::Dead
                );
            } else {
                assert!(
                    matches!(result, Err(Error::Stale)),
                    "{wait_kind}: lease must be checked after waiting"
                );
                assert_eq!(
                    Store::new(pool.clone())
                        .inspect("a", ids[0])
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    JobStatus::Active
                );
            }
            let generation: i64 =
                sqlx::query_scalar("SELECT generation FROM raddle_batches WHERE id=$1")
                    .bind(batch)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(generation, 1); // No takeover made the old pre-wait check stale.
            small.close().await;
        }
    }
    assert!(effects(&pool).await.is_empty());
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn empty_probe_completion_releases_fence_for_normal_reduced_retry(pool: PgPool) {
    setup(&pool).await;
    let batches = BatchStore::new(pool.clone());
    let ids = stage(
        &pool,
        "empty-probe",
        "group",
        &[(1, "A", 5, None), (2, "B", 1, None)],
    )
    .await;
    let batch = aggregate(&batches, "empty-probe", "group", 2, now()).await;
    let claim = batches
        .claim(&queue("empty-probe"), now(), lease())
        .await
        .unwrap()
        .unwrap();
    assert!(batches.isolate_once(&claim, 1, now()).await.unwrap());
    let execution = batches
        .probe(&claim, now(), timeout())
        .await
        .unwrap()
        .unwrap();
    let resolution = Resolution::named(execution.members(), []).unwrap();
    execution.settle(resolution, now(), now()).await.unwrap();
    // A is already probed. Removing the last unprobed participant must close
    // this pass durably even for a caller using the lower-level probe API.
    sqlx::query("UPDATE raddle_jobs SET status='dead',last_error='attempts-exhausted',owner=NULL,lease_until=NULL WHERE id=$1")
        .bind(ids[1]).execute(&pool).await.unwrap();
    assert!(
        batches
            .probe(&claim, now(), timeout())
            .await
            .unwrap()
            .is_none()
    );
    let row = sqlx::query("SELECT status,probing,owner FROM raddle_batches WHERE id=$1")
        .bind(batch)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<&str, _>("status"), "ready");
    assert!(!row.get::<bool, _>("probing"));
    assert!(row.get::<Option<Uuid>, _>("owner").is_none());
    let at = now() + TimeDelta::seconds(1);
    let report = batches
        .run_once(
            &queue("empty-probe"),
            at,
            lease(),
            timeout(),
            at,
            Isolation::After(1),
            |view, scope| {
                Box::pin(async move {
                    assert_eq!(view.originals().len(), 1);
                    let member = view.originals().first().unwrap();
                    assert_eq!(member.context.attempt, 2);
                    sqlx::query("INSERT INTO effects VALUES($1,1)")
                        .bind(member.context.delivery_id)
                        .execute(scope.connection())
                        .await
                        .unwrap();
                    Ok::<_, ()>(Resolution::committed(view.originals()))
                })
            },
            |_| true,
        )
        .await
        .unwrap();
    assert!(report.claimed);
    assert_eq!(report.probes, 0);
    assert_eq!(effects(&pool).await, [1]);
    assert_eq!(
        batches.pending("a", &queue("empty-probe")).await.unwrap(),
        0
    );
}
