//! Real SQLx custody, fencing, poison isolation and subprocess recovery contracts.
#![cfg(feature = "postgres")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::{self, Command};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use raddle::wire::{Envelope, WireMessage};
use raddle_batching::postgres::{BatchStore, Error, Isolation};
use raddle_batching::{BatchKey, Decision, ExecutionQueue, Options, Resolution};
use raddle_postgres::{Delivery, DeliveryKind, JobStatus, Store};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, PgPool, Row};
use uuid::Uuid;

#[path = "postgres/preparation.rs"]
mod preparation;

#[derive(Serialize, Deserialize)]
struct Message {
    value: u32,
    key: String,
}
impl WireMessage for Message {
    const NAME: &'static str = "batch.test";
    const VERSION: u32 = 1;
}

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000, 0).unwrap()
}
fn queue(name: &str) -> ExecutionQueue {
    ExecutionQueue::Independent(name.into())
}
fn key(tenant: &str, group: Option<&str>) -> BatchKey {
    BatchKey::new(tenant, group.map(str::to_owned)).unwrap()
}
fn options(size: usize) -> Options {
    Options::new(size, Duration::from_millis(250), 1000).unwrap()
}
fn lease() -> Duration {
    Duration::from_secs(30)
}
fn timeout() -> Duration {
    Duration::from_secs(30)
}

async fn setup(pool: &PgPool) {
    Store::new(pool.clone()).migrate().await.unwrap();
    sqlx::query("CREATE TABLE effects(delivery_id uuid PRIMARY KEY, value integer NOT NULL)")
        .execute(pool)
        .await
        .unwrap();
}

async fn seed(
    pool: &PgPool,
    tenant: &str,
    group: Option<&str>,
    destination: &str,
    values: &[(u32, &str, Option<DateTime<Utc>>)],
) -> Vec<Uuid> {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let mut scope = store.begin().await.unwrap();
    let mut ids = Vec::new();
    for (value, coalesce, expiry) in values {
        let envelope = Envelope::with_identity(
            Uuid::new_v4(),
            now() - TimeDelta::seconds(1),
            &Message {
                value: *value,
                key: coalesce.to_string(),
            },
        )
        .unwrap();
        let mut delivery = Delivery::new(DeliveryKind::Local, destination, now());
        delivery.partition_key = group.map(str::to_owned);
        delivery.expires_at = *expiry;
        ids.extend(
            scope
                .stage_received("source", tenant, &envelope, &[delivery])
                .await
                .unwrap(),
        );
    }
    scope.commit(now()).await.unwrap();
    ids
}

async fn assemble(store: &BatchStore, destination: &str, size: usize, coalesce: bool) -> Uuid {
    store
        .assemble(
            &queue(destination),
            &key("a", Some("group")),
            options(size),
            now(),
            |envelope| Ok(coalesce.then(|| envelope.decode::<Message>().unwrap().key)),
        )
        .await
        .unwrap()
        .unwrap()
}

async fn effects(pool: &PgPool) -> Vec<i32> {
    sqlx::query_scalar("SELECT value FROM effects ORDER BY value")
        .fetch_all(pool)
        .await
        .unwrap()
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn size_time_tenant_group_concurrent_first_use_and_canonical_context(pool: PgPool) {
    let batches = BatchStore::new(pool.clone());
    seed(
        &pool,
        "a",
        Some("group"),
        "batch",
        &[(1, "A", None), (2, "B", None)],
    )
    .await;
    seed(&pool, "a", Some("other"), "batch", &[(3, "C", None)]).await;
    seed(&pool, "b", Some("group"), "batch", &[(4, "D", None)]).await;
    assert!(
        batches
            .assemble(
                &queue("batch"),
                &key("a", Some("group")),
                Options::default(),
                now(),
                |_| Ok(None)
            )
            .await
            .unwrap()
            .is_none()
    );
    let q = queue("batch");
    let k = key("a", Some("group"));
    let (first, second) = tokio::join!(
        batches.assemble(&q, &k, options(2), now(), |_| Ok(None)),
        batches.assemble(&q, &k, options(2), now(), |_| Ok(None)),
    );
    assert_eq!(
        usize::from(first.unwrap().is_some()) + usize::from(second.unwrap().is_some()),
        1
    );
    assert_eq!(batches.pending("a", &q).await.unwrap(), 3);
    // The old generic worker must not claim the two assembled originals.
    let ordinary = Store::new(pool.clone())
        .claim_for(DeliveryKind::Local, "batch", 10, lease(), now())
        .await
        .unwrap();
    assert_eq!(ordinary.len(), 2); // Other tenant/group only.
    let claim = batches.claim(&q, now(), lease()).await.unwrap().unwrap();
    let execution = batches.execution(&claim, now(), timeout()).await.unwrap();
    assert_eq!(execution.members().len(), 2);
    assert!(
        execution
            .members()
            .iter()
            .all(|member| member.context.attempt == 1
                && member.context.sent_at == now() - TimeDelta::seconds(1))
    );
    let resolution = Resolution::committed(execution.members());
    execution.settle(resolution, now(), now()).await.unwrap();
    assert_eq!(batches.pending("a", &q).await.unwrap(), 1);

    seed(&pool, "c", None, "timer", &[(5, "E", None)]).await;
    assert!(
        batches
            .assemble(
                &queue("timer"),
                &key("c", None),
                Options::default(),
                now() + TimeDelta::milliseconds(249),
                |_| Ok(None)
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        batches
            .assemble(
                &queue("timer"),
                &key("c", None),
                Options::default(),
                now() + TimeDelta::milliseconds(250),
                |_| Ok(None)
            )
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        batches
            .assemble(
                &queue("empty"),
                &key("a", None),
                Options::default(),
                now(),
                |_| Ok(None)
            )
            .await
            .unwrap()
            .is_none()
    );
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn poison_first_middle_last_one_time_probe_and_failed_only_corrected_retry(pool: PgPool) {
    setup(&pool).await;
    let batches = BatchStore::new(pool.clone());
    for poison in 0..3 {
        let destination = format!("batch-{poison}");
        let base = poison * 10;
        let ids = seed(
            &pool,
            "a",
            Some("group"),
            &destination,
            &[
                (base, "A", None),
                (base + 1, "B", None),
                (base + 2, "C", None),
            ],
        )
        .await;
        assemble(&batches, &destination, 3, false).await;
        let bad = base + poison;
        let first =
            batches
                .run_once(
                    &queue(&destination),
                    now(),
                    lease(),
                    timeout(),
                    now(),
                    Isolation::After(1),
                    move |members, scope| {
                        Box::pin(async move {
                            if members.input().iter().any(|member| {
                                member.value.decode::<Message>().unwrap().value == bad
                            }) {
                                return Err("poison");
                            }
                            for member in members.input() {
                                sqlx::query("INSERT INTO effects VALUES($1,$2)")
                                    .bind(member.context.delivery_id)
                                    .bind(member.value.decode::<Message>().unwrap().value as i32)
                                    .execute(scope.connection())
                                    .await
                                    .unwrap();
                            }
                            Ok(Resolution::committed(members.originals()))
                        })
                    },
                    |_| true,
                )
                .await
                .unwrap();
        assert_eq!(first.probes, 3);
        assert_eq!(first.handler_failures, 2); // Original batch and poison singleton.
        assert_eq!(batches.pending("a", &queue(&destination)).await.unwrap(), 1);
        assert_eq!(effects(&pool).await.len(), poison as usize * 3 + 2);
        for (position, id) in ids.iter().enumerate() {
            let status = Store::new(pool.clone())
                .inspect("a", *id)
                .await
                .unwrap()
                .unwrap()
                .status;
            assert_eq!(
                status,
                if position == poison as usize {
                    JobStatus::Active
                } else {
                    JobStatus::Completed
                }
            );
        }
        let second = batches
            .run_once(
                &queue(&destination),
                now(),
                lease(),
                timeout(),
                now(),
                Isolation::After(1),
                |_, _| Box::pin(async { Err::<Resolution, _>("still-poison") }),
                |_| true,
            )
            .await
            .unwrap();
        assert_eq!(second.probes, 0); // No recursive or second isolation sweep.
        let corrected = batches
            .run_once(
                &queue(&destination),
                now(),
                lease(),
                timeout(),
                now(),
                Isolation::After(1),
                move |members, scope| {
                    Box::pin(async move {
                        assert_eq!(members.input().len(), 1);
                        assert_eq!(
                            members.input()[0].value.decode::<Message>().unwrap().value,
                            bad
                        );
                        sqlx::query("INSERT INTO effects VALUES($1,$2)")
                            .bind(members.input()[0].context.delivery_id)
                            .bind(bad as i32)
                            .execute(scope.connection())
                            .await
                            .unwrap();
                        Ok::<_, ()>(Resolution::committed(members.originals()))
                    })
                },
                |_| true,
            )
            .await
            .unwrap();
        assert_eq!(corrected.probes, 0);
        assert_eq!(batches.pending("a", &queue(&destination)).await.unwrap(), 0);
        assert_eq!(effects(&pool).await.len(), (poison as usize + 1) * 3);
    }
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn coalesced_poison_dlq_failure_keeps_all_originals_retryable_healthy_commits(pool: PgPool) {
    setup(&pool).await;
    sqlx::query("CREATE FUNCTION reject_dlq() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='dead' AND NEW.last_error='poison' THEN RAISE EXCEPTION 'injected quarantine outage'; END IF; RETURN NEW; END $$").execute(&pool).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_dlq BEFORE UPDATE ON raddle_jobs FOR EACH ROW EXECUTE FUNCTION reject_dlq()").execute(&pool).await.unwrap();
    let batches = BatchStore::new(pool.clone());
    let ids = seed(
        &pool,
        "a",
        Some("group"),
        "batch",
        &[(1, "A", None), (10, "B", None), (2, "A", None)],
    )
    .await;
    assemble(&batches, "batch", 3, true).await;
    let claim = batches
        .claim(&queue("batch"), now(), lease())
        .await
        .unwrap()
        .unwrap();
    let mut execution = batches.execution(&claim, now(), timeout()).await.unwrap();
    assert_eq!(
        execution
            .input()
            .iter()
            .map(|member| member.value.decode::<Message>().unwrap().value)
            .collect::<Vec<_>>(),
        [10, 2]
    );
    assert_eq!(execution.members().len(), 3);
    let resolution = Resolution::named(
        execution.members(),
        [
            (ids[2], Decision::Poison("poison".into())),
            (ids[1], Decision::Commit),
        ],
    )
    .unwrap();
    let (_, scope) = execution.parts();
    sqlx::query("INSERT INTO effects VALUES($1,10)")
        .bind(ids[1])
        .execute(scope.connection())
        .await
        .unwrap();
    assert_eq!(batches.pending("a", &queue("batch")).await.unwrap(), 3);
    let report = execution.settle(resolution, now(), now()).await.unwrap();
    assert_eq!(report.quarantine_failed, [ids[0], ids[2]]);
    assert_eq!(effects(&pool).await, [10]);
    assert_eq!(batches.pending("a", &queue("batch")).await.unwrap(), 2);
    sqlx::query("DROP TRIGGER reject_dlq ON raddle_jobs")
        .execute(&pool)
        .await
        .unwrap();
    let claim = batches
        .claim(&queue("batch"), now(), lease())
        .await
        .unwrap()
        .unwrap();
    let execution = batches.execution(&claim, now(), timeout()).await.unwrap();
    assert_eq!(execution.members().len(), 2);
    assert_eq!(execution.input().len(), 1);
    assert_eq!(
        execution.input()[0]
            .value
            .decode::<Message>()
            .unwrap()
            .value,
        2
    );
    let resolution = Resolution::named(
        execution.members(),
        [(ids[2], Decision::Poison("poison".into()))],
    )
    .unwrap();
    let report = execution.settle(resolution, now(), now()).await.unwrap();
    assert_eq!(report.quarantined, [ids[0], ids[2]]);
    assert_eq!(batches.pending("a", &queue("batch")).await.unwrap(), 0);
    assert_eq!(effects(&pool).await, [10]);
    // Canonical audited replay reassembles only the failed originals; successful
    // B remains a tombstone and can never appear in the new batch.
    let canonical = Store::new(pool.clone());
    assert!(
        canonical
            .replay("a", ids[0], "operator", "corrected rule", now())
            .await
            .unwrap()
    );
    assert!(
        canonical
            .replay("a", ids[2], "operator", "corrected rule", now())
            .await
            .unwrap()
    );
    assemble(&batches, "batch", 2, true).await;
    let claim = batches
        .claim(&queue("batch"), now(), lease())
        .await
        .unwrap()
        .unwrap();
    let execution = batches.execution(&claim, now(), timeout()).await.unwrap();
    assert_eq!(execution.members().len(), 2);
    assert!(
        execution
            .members()
            .iter()
            .all(|member| member.context.delivery_id != ids[1])
    );
    let resolution = Resolution::committed(execution.members());
    execution.settle(resolution, now(), now()).await.unwrap();
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn expiry_before_assembly_and_partial_after_assembly_preserve_live_and_unbounded(
    pool: PgPool,
) {
    let batches = BatchStore::new(pool.clone());
    let ids = seed(
        &pool,
        "a",
        Some("group"),
        "pre",
        &[(0, "X", Some(now())), (1, "A", None)],
    )
    .await;
    assemble(&batches, "pre", 1, false).await;
    assert_eq!(
        Store::new(pool.clone())
            .inspect("a", ids[0])
            .await
            .unwrap()
            .unwrap()
            .last_error
            .as_deref(),
        Some("expired")
    );
    for destination in ["bounded", "unbounded", "expired", "between"] {
        seed(
            &pool,
            "a",
            Some("group"),
            destination,
            &[
                (1, "A", Some(now() + TimeDelta::seconds(1))),
                (
                    2,
                    "B",
                    if destination == "unbounded" {
                        None
                    } else {
                        Some(now() + TimeDelta::seconds(3))
                    },
                ),
            ],
        )
        .await;
        assemble(&batches, destination, 2, false).await;
    }
    for destination in ["bounded", "unbounded"] {
        let at = now() + TimeDelta::seconds(2);
        let claim = batches
            .claim(&queue(destination), at, lease())
            .await
            .unwrap()
            .unwrap();
        let execution = batches.execution(&claim, at, timeout()).await.unwrap();
        assert_eq!(execution.members().len(), 2);
        assert!(execution.members()[0].context.expires_at.unwrap() < at);
        let resolution = Resolution::committed(execution.members());
        execution.settle(resolution, at, at).await.unwrap();
    }
    assert!(
        batches
            .claim(&queue("expired"), now() + TimeDelta::seconds(4), lease())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(batches.pending("a", &queue("expired")).await.unwrap(), 0);
    let claim = batches
        .claim(&queue("between"), now() + TimeDelta::seconds(2), lease())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        batches
            .execution(&claim, now() + TimeDelta::seconds(4), timeout())
            .await,
        Err(Error::Expired)
    ));
    assert_eq!(batches.pending("a", &queue("between")).await.unwrap(), 0);
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn execution_lock_fences_takeover_timeout_and_cancellation_keep_custody(pool: PgPool) {
    setup(&pool).await;
    let batches = BatchStore::new(pool.clone());
    seed(&pool, "a", Some("group"), "batch", &[(1, "A", None)]).await;
    assemble(&batches, "batch", 1, false).await;
    let claim = batches
        .claim(&queue("batch"), now(), Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    let mut execution = batches.execution(&claim, now(), timeout()).await.unwrap();
    let (members, scope) = execution.parts();
    sqlx::query("INSERT INTO effects VALUES($1,1)")
        .bind(members.input()[0].context.delivery_id)
        .execute(scope.connection())
        .await
        .unwrap();
    assert!(
        batches
            .claim(&queue("batch"), now() + TimeDelta::seconds(2), lease())
            .await
            .unwrap()
            .is_none()
    );
    execution.rollback().await.unwrap();
    let later = batches
        .claim(
            &queue("batch"),
            now() + TimeDelta::seconds(2),
            Duration::from_secs(1),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        batches
            .execution(&claim, now() + TimeDelta::seconds(2), timeout())
            .await,
        Err(Error::Stale)
    ));
    drop(
        batches
            .execution(&later, now() + TimeDelta::seconds(2), timeout())
            .await
            .unwrap(),
    );
    assert!(effects(&pool).await.is_empty());
    let report = batches
        .run_once(
            &queue("batch"),
            now() + TimeDelta::seconds(4),
            lease(),
            Duration::from_millis(100),
            now(),
            Isolation::After(1),
            |members, scope| {
                Box::pin(async move {
                    sqlx::query("INSERT INTO effects VALUES($1,1)")
                        .bind(members.input()[0].context.delivery_id)
                        .execute(scope.connection())
                        .await
                        .unwrap();
                    std::future::pending::<()>().await;
                    Ok::<_, ()>(Resolution::committed(members.originals()))
                })
            },
            |_| true,
        )
        .await
        .unwrap();
    assert_eq!(report.timeouts, 1);
    assert_eq!(report.probes, 0);
    assert!(effects(&pool).await.is_empty());
    assert_eq!(batches.pending("a", &queue("batch")).await.unwrap(), 1);
}

// Explicit private child mode; ordinary test execution performs no I/O here.
#[tokio::test]
async fn subprocess_probe() {
    let Ok(url) = std::env::var("RADDLE_BATCH_CRASH_URL") else {
        return;
    };
    let options: PgConnectOptions = url.parse().unwrap();
    let pool = PgPool::connect_with(options.disable_statement_logging())
        .await
        .unwrap();
    let batches = BatchStore::new(pool.clone());
    let phase = std::env::var("RADDLE_BATCH_CRASH_PHASE").unwrap();
    if phase == "accumulating" {
        assert!(
            batches
                .assemble(
                    &queue("batch"),
                    &key("a", Some("group")),
                    Options::default(),
                    now(),
                    |_| Ok(None)
                )
                .await
                .unwrap()
                .is_none()
        );
        process::exit(31);
    }
    let claim = batches
        .claim(&queue("batch"), now(), Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    let mut execution = batches.execution(&claim, now(), timeout()).await.unwrap();
    let first = execution.members()[0].context.delivery_id;
    let resolution = Resolution::named(execution.members(), [(first, Decision::Commit)]).unwrap();
    sqlx::query("INSERT INTO effects VALUES($1,0)")
        .bind(first)
        .execute(execution.parts().1.connection())
        .await
        .unwrap();
    if phase == "executing" {
        process::exit(32);
    }
    execution.settle(resolution, now(), now()).await.unwrap();
    let claim = batches
        .claim(&queue("batch"), now(), Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    if phase == "probing" {
        assert!(batches.isolate_once(&claim, 1, now()).await.unwrap());
        let mut execution = batches
            .probe(&claim, now(), timeout())
            .await
            .unwrap()
            .unwrap();
        let member = &execution.members()[0];
        sqlx::query("INSERT INTO effects VALUES($1,1)")
            .bind(member.context.delivery_id)
            .execute(execution.parts().1.connection())
            .await
            .unwrap();
        let resolution = Resolution::committed(execution.members());
        execution.settle(resolution, now(), now()).await.unwrap();
        let mut execution = batches
            .probe(&claim, now(), timeout())
            .await
            .unwrap()
            .unwrap();
        let id = execution.members()[0].context.delivery_id;
        sqlx::query("INSERT INTO effects VALUES($1,99)")
            .bind(id)
            .execute(execution.parts().1.connection())
            .await
            .unwrap();
        process::exit(34);
    }
    let mut execution = batches.execution(&claim, now(), timeout()).await.unwrap();
    let id = execution.members()[0].context.delivery_id;
    sqlx::query("INSERT INTO effects VALUES($1,99)")
        .bind(id)
        .execute(execution.parts().1.connection())
        .await
        .unwrap();
    process::exit(33);
}

fn crash(pool: &PgPool, phase: &str, code: i32) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "subprocess_probe", "--nocapture"])
        .env(
            "RADDLE_BATCH_CRASH_URL",
            pool.connect_options().to_url_lossy().as_str(),
        )
        .env("RADDLE_BATCH_CRASH_PHASE", phase)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(code),
        "child did not reach its deliberate exit: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn subprocess_crashes_accumulating_executing_reduced_replay_and_probing(pool: PgPool) {
    setup(&pool).await;
    let batches = BatchStore::new(pool.clone());
    for phase in ["accumulating", "executing", "reduced", "probing"] {
        sqlx::query("TRUNCATE raddle_batch_members,raddle_batches,raddle_jobs,raddle_replay_audit,effects CASCADE").execute(&pool).await.unwrap();
        let ids = seed(
            &pool,
            "a",
            Some("group"),
            "batch",
            &[(0, "A", None), (1, "B", None), (2, "C", None)],
        )
        .await;
        if phase != "accumulating" {
            assemble(&batches, "batch", 3, false).await;
        }
        crash(
            &pool,
            phase,
            match phase {
                "accumulating" => 31,
                "executing" => 32,
                "reduced" => 33,
                _ => 34,
            },
        );
        // Process exit closes the socket, but PostgreSQL rolls back on its own
        // scheduler. Observe release of the dead execution's original row locks
        // before asking a SKIP LOCKED recovery claimant to find available work.
        // This is a bounded database barrier, not a timing sleep or claim poll.
        let mut rollback_barrier = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL statement_timeout='60s'")
            .execute(&mut *rollback_barrier)
            .await
            .unwrap();
        sqlx::query("SELECT id FROM raddle_jobs WHERE id=ANY($1) ORDER BY id FOR UPDATE")
            .bind(&ids)
            .fetch_all(&mut *rollback_barrier)
            .await
            .unwrap();
        rollback_barrier.commit().await.unwrap();
        let terminal = if phase == "reduced" {
            1
        } else if phase == "probing" {
            2
        } else {
            0
        };
        assert_eq!(effects(&pool).await.len(), terminal);
        assert_eq!(
            batches.pending("a", &queue("batch")).await.unwrap(),
            3 - terminal as i64
        );
        let at = now() + TimeDelta::seconds(3);
        if phase == "accumulating" {
            assert!(
                batches
                    .assemble(
                        &queue("batch"),
                        &key("a", Some("group")),
                        Options::default(),
                        at,
                        |_| Ok(None)
                    )
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        let expected = 3 - terminal;
        let probing = phase == "probing";
        let report = batches
            .run_once(
                &queue("batch"),
                at,
                lease(),
                timeout(),
                at,
                Isolation::After(1),
                move |members, scope| {
                    Box::pin(async move {
                        assert_eq!(members.input().len(), if probing { 1 } else { expected });
                        assert_eq!(members.key().tenant(), "a");
                        assert_eq!(members.key().group(), Some("group"));
                        for member in members.input() {
                            let value = member.value.decode::<Message>().unwrap().value;
                            assert_eq!(member.context.sent_at, now() - TimeDelta::seconds(1));
                            sqlx::query("INSERT INTO effects VALUES($1,$2)")
                                .bind(member.context.delivery_id)
                                .bind(value as i32)
                                .execute(scope.connection())
                                .await
                                .unwrap();
                        }
                        Ok::<_, ()>(Resolution::committed(members.originals()))
                    })
                },
                |_| true,
            )
            .await
            .unwrap();
        assert_eq!(report.probes, usize::from(probing));
        assert!(report.claimed, "{phase}: {report:?}");
        assert_eq!(report.timeouts, 0, "{phase}: {report:?}");
        assert_eq!(report.handler_failures, 0, "{phase}: {report:?}");
        assert_eq!(effects(&pool).await, [0, 1, 2]);
        assert_eq!(batches.pending("a", &queue("batch")).await.unwrap(), 0);
        let retained: Vec<Uuid> = sqlx::query("SELECT id FROM raddle_jobs ORDER BY sequence")
            .fetch_all(&pool)
            .await
            .unwrap()
            .iter()
            .map(|row| row.get("id"))
            .collect();
        assert_eq!(retained, ids);
    }
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn separated_fanout_retains_direct_and_successful_batch_siblings(pool: PgPool) {
    use raddle_batching::{Combination, RoutePlan};
    setup(&pool).await;
    let canonical = Store::new(pool.clone());
    let batches = BatchStore::new(pool.clone());
    let plan = RoutePlan::build(
        Combination::Separated,
        Some("direct".into()),
        vec![queue("first-batch"), queue("second-batch")],
        true,
        false,
    )
    .unwrap();
    let mut scope = canonical.begin().await.unwrap();
    for value in 0..2 {
        let envelope = Envelope::encode(&Message {
            value,
            key: value.to_string(),
        })
        .unwrap();
        let deliveries: Vec<_> = plan
            .destinations()
            .iter()
            .map(|destination| {
                let mut delivery = Delivery::new(DeliveryKind::Local, destination, now());
                delivery.partition_key = Some("group".into());
                delivery
            })
            .collect();
        scope
            .stage_received("source", "a", &envelope, &deliveries)
            .await
            .unwrap();
    }
    scope.commit(now()).await.unwrap();
    for _ in 0..2 {
        let claim = canonical
            .claim_for(DeliveryKind::Local, "direct", 2, lease(), now())
            .await
            .unwrap()
            .remove(0);
        let mut scope = canonical.execution(&claim, now()).await.unwrap();
        sqlx::query("INSERT INTO effects VALUES($1,$2)")
            .bind(claim.id())
            .bind(100 + claim.envelope().unwrap().decode::<Message>().unwrap().value as i32)
            .execute(scope.connection())
            .await
            .unwrap();
        scope.commit(now()).await.unwrap();
    }
    assemble(&batches, "first-batch", 2, false).await;
    assemble(&batches, "second-batch", 2, false).await;
    batches
        .run_once(
            &queue("first-batch"),
            now(),
            lease(),
            timeout(),
            now(),
            Isolation::Disabled,
            |view, scope| {
                Box::pin(async move {
                    for member in view.input() {
                        sqlx::query("INSERT INTO effects VALUES($1,$2)")
                            .bind(member.context.delivery_id)
                            .bind(member.value.decode::<Message>().unwrap().value as i32)
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
    batches
        .run_once(
            &queue("second-batch"),
            now(),
            lease(),
            timeout(),
            now(),
            Isolation::Disabled,
            |_, _| Box::pin(async { Err::<Resolution, _>("second-consumer-only") }),
            |_| false,
        )
        .await
        .unwrap();
    assert_eq!(effects(&pool).await, [0, 1, 100, 101]);
    assert_eq!(batches.pending("a", &queue("direct")).await.unwrap(), 0);
    assert_eq!(
        batches.pending("a", &queue("first-batch")).await.unwrap(),
        0
    );
    assert_eq!(
        batches.pending("a", &queue("second-batch")).await.unwrap(),
        2
    );
    let first_attempts: Vec<i32> =
        sqlx::query_scalar("SELECT attempts FROM raddle_jobs WHERE destination='first-batch'")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(first_attempts, [1, 1]);
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn same_wire_identity_distinct_custody_and_matching_failure_threshold(pool: PgPool) {
    let canonical = Store::new(pool.clone());
    canonical.migrate().await.unwrap();
    let batches = BatchStore::new(pool.clone());
    let envelope = Envelope::encode(&Message {
        value: 1,
        key: "A".into(),
    })
    .unwrap();
    let mut scope = canonical.begin().await.unwrap();
    let mut delivery = Delivery::new(DeliveryKind::Local, "batch", now());
    delivery.partition_key = Some("group".into());
    let first = scope
        .stage_received("first-source", "a", &envelope, &[delivery.clone()])
        .await
        .unwrap()[0];
    let second = scope
        .stage_received("second-source", "a", &envelope, &[delivery])
        .await
        .unwrap()[0];
    scope.commit(now()).await.unwrap();
    assemble(&batches, "batch", 2, false).await;
    let claim = batches
        .claim(&queue("batch"), now(), Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    let execution = batches.execution(&claim, now(), timeout()).await.unwrap();
    assert_eq!(
        execution.members()[0].context.id,
        execution.members()[1].context.id
    );
    assert_ne!(first, second);
    let resolution = Resolution::named(execution.members(), [(first, Decision::Commit)]).unwrap();
    execution.settle(resolution, now(), now()).await.unwrap();
    let claim = batches
        .claim(&queue("batch"), now(), lease())
        .await
        .unwrap()
        .unwrap();
    let execution = batches.execution(&claim, now(), timeout()).await.unwrap();
    assert_eq!(execution.members().len(), 1);
    assert_eq!(execution.members()[0].context.delivery_id, second);
    execution.rollback().await.unwrap();
    // Two prior claims are NOT two matching domain failures.
    assert!(!batches.isolate_once(&claim, 2, now()).await.unwrap());
    let execution = batches.execution(&claim, now(), timeout()).await.unwrap();
    let resolution = Resolution::named(execution.members(), []).unwrap();
    execution.settle(resolution, now(), now()).await.unwrap();
    let report = batches
        .run_once(
            &queue("batch"),
            now(),
            lease(),
            timeout(),
            now(),
            Isolation::After(2),
            |_, _| Box::pin(async { Err::<Resolution, _>("matching") }),
            |_| true,
        )
        .await
        .unwrap();
    assert_eq!(report.probes, 1);
    assert_eq!(
        canonical.inspect("a", first).await.unwrap().unwrap().status,
        JobStatus::Completed
    );
    assert_eq!(
        canonical
            .inspect("a", second)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Active
    );
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn tracked_session_waits_through_retry_and_uncommitted_settlement(pool: PgPool) {
    use raddle_batching::postgres::TrackingSession;
    let batches = BatchStore::new(pool.clone());
    let ids = seed(
        &pool,
        "a",
        Some("group"),
        "batch",
        &[(1, "A", None), (2, "B", None)],
    )
    .await;
    let session = TrackingSession::new("a", ids.clone()).unwrap();
    assemble(&batches, "batch", 2, false).await;
    let claim = batches
        .claim(&queue("batch"), now(), lease())
        .await
        .unwrap()
        .unwrap();
    let execution = batches.execution(&claim, now(), timeout()).await.unwrap();
    let resolution = Resolution::named(execution.members(), [(ids[0], Decision::Commit)]).unwrap();
    assert!(matches!(
        session
            .wait(
                &batches,
                Duration::from_millis(1),
                Duration::from_millis(20)
            )
            .await,
        Err(Error::TrackingTimeout)
    ));
    execution.settle(resolution, now(), now()).await.unwrap();
    assert!(matches!(
        session
            .wait(
                &batches,
                Duration::from_millis(1),
                Duration::from_millis(20)
            )
            .await,
        Err(Error::TrackingTimeout)
    ));
    let claim = batches
        .claim(&queue("batch"), now(), lease())
        .await
        .unwrap()
        .unwrap();
    let execution = batches.execution(&claim, now(), timeout()).await.unwrap();
    let resolution = Resolution::named(
        execution.members(),
        [(ids[1], Decision::Poison("invalid".into()))],
    )
    .unwrap();
    execution.settle(resolution, now(), now()).await.unwrap();
    let outcome = session
        .wait(&batches, Duration::from_millis(1), timeout())
        .await
        .unwrap();
    assert_eq!(outcome.completed, [ids[0]]);
    assert_eq!(outcome.dead, [ids[1]]);
    let missing = TrackingSession::new("other-tenant", ids).unwrap();
    assert!(matches!(
        missing
            .wait(&batches, Duration::from_millis(1), timeout())
            .await,
        Err(Error::Invalid("tracked custody missing"))
    ));
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn typed_custom_shape_coalesces_two_tenants_but_settles_all_originals(pool: PgPool) {
    setup(&pool).await;
    let batches = BatchStore::new(pool.clone());
    for (tenant, base) in [("a", 0), ("b", 100)] {
        seed(
            &pool,
            tenant,
            Some("group"),
            "batch",
            &[
                (base + 1, "A", None),
                (base + 10, "B", None),
                (base + 2, "A", None),
            ],
        )
        .await;
        batches
            .assemble(
                &queue("batch"),
                &key(tenant, Some("group")),
                options(3),
                now(),
                |envelope| Ok(Some(envelope.decode::<Message>().unwrap().key)),
            )
            .await
            .unwrap()
            .unwrap();
    }
    for _ in 0..2 {
        batches
            .run_once(
                &queue("batch"),
                now(),
                lease(),
                timeout(),
                now(),
                Isolation::Disabled,
                |view, scope| {
                    Box::pin(async move {
                        let decoded = view.decode::<Message>().unwrap();
                        let typed = decoded.view();
                        assert_eq!(typed.originals().len(), 3);
                        let input = typed.input();
                        assert_eq!(input.len(), 2);
                        struct Shape {
                            latest_a: u32,
                            b: u32,
                        }
                        let shape = Shape {
                            latest_a: input[1].value.value,
                            b: input[0].value.value,
                        };
                        let base = if typed.key().tenant() == "a" { 0 } else { 100 };
                        assert_eq!(shape.latest_a, base + 2);
                        assert_eq!(shape.b, base + 10);
                        for member in input {
                            sqlx::query("INSERT INTO effects VALUES($1,$2)")
                                .bind(member.context.delivery_id)
                                .bind(member.value.value as i32)
                                .execute(scope.connection())
                                .await
                                .unwrap();
                        }
                        Ok::<_, ()>(Resolution::committed(typed.originals()))
                    })
                },
                |_| false,
            )
            .await
            .unwrap();
    }
    assert_eq!(effects(&pool).await, [2, 10, 102, 110]);
    assert_eq!(batches.pending("a", &queue("batch")).await.unwrap(), 0);
    assert_eq!(batches.pending("b", &queue("batch")).await.unwrap(), 0);
    let completed: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raddle_jobs WHERE status='completed'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(completed, 6);
}
