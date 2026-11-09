//! Caller-owned external source supervision against a disposable PostgreSQL DB.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use raddle::wire::{Codecs, WireMessage};
use raddle_host::{Host, PollSettings, RestartPolicy};
use raddle_postgres::external_tables::{Aliases, Config, Listener, Options};
use raddle_postgres::{RetryPolicy, Store, Worker};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, PgPool};

#[derive(Serialize, Deserialize)]
struct Order {
    value: i32,
}
impl WireMessage for Order {
    const NAME: &'static str = "external.host";
    const VERSION: u32 = 1;
}

fn listener(pool: &PgPool) -> Listener {
    let mut config = Config::new("tenant", "host-external", "ext", "items", "host-external");
    config.default_alias = Some("order".into());
    let mut aliases = Aliases::default();
    aliases.register::<Order>("order").unwrap();
    Listener::new(
        Store::new(pool.clone()),
        config,
        aliases,
        Options {
            interval: Duration::from_millis(5),
            ..Default::default()
        },
    )
    .unwrap()
}
async fn insert(pool: &PgPool, id: u128, body: &str) {
    sqlx::query("INSERT INTO ext.items VALUES($1,$2::jsonb,clock_timestamp())")
        .bind(uuid::Uuid::from_u128(id))
        .bind(body)
        .execute(pool)
        .await
        .unwrap();
}
async fn count(pool: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(pool)
        .await
        .unwrap()
}
async fn blocked(pool: &PgPool) -> bool {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE 'DELETE FROM%')")
        .fetch_one(pool).await.unwrap()
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_host_registration_readiness_and_row_errors_are_not_host_failures(
    pool: PgPool,
) {
    let listener = Arc::new(listener(&pool));
    listener.provision_source().await.unwrap();
    sqlx::query("CREATE TABLE external_host_effects(value integer PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    insert(&pool, 1, "{\"value\":\"bad\"}").await;
    insert(&pool, 2, "{\"value\":2}").await;
    let mut worker = Worker::new(Store::new(pool.clone()), RetryPolicy::default());
    worker
        .handle::<Order, _>("host-external", Codecs::default(), |m, scope| {
            Box::pin(async move {
                sqlx::query("INSERT INTO external_host_effects VALUES($1)")
                    .bind(m.value)
                    .execute(scope.connection())
                    .await
                    .unwrap();
                Ok(())
            })
        })
        .unwrap();
    let worker = Arc::new(worker);
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    let source = Arc::clone(&listener);
    host.service("external-source", RestartPolicy::NEVER, move |context| {
        let source = Arc::clone(&source);
        async move { raddle_host::postgres_external_table(&source, context).await }
    })
    .unwrap();
    host.service("external-worker", RestartPolicy::NEVER, move |context| {
        let worker = Arc::clone(&worker);
        async move {
            raddle_host::postgres_worker(
                &worker,
                PollSettings::new(10, Duration::from_millis(5)).unwrap(),
                context,
            )
            .await
        }
    })
    .unwrap();
    let handle = host.handle();
    assert!(!handle.snapshot().ready);
    assert_eq!(count(&pool, "ext.items").await, 2);
    assert_eq!(
        count(&pool, "raddle_external_sources").await,
        0,
        "registration performed SQL"
    );
    let run = tokio::spawn(host.run());
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if handle.snapshot().ready && count(&pool, "external_host_effects").await == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        listener.search_quarantine(0, 10).await.unwrap().items.len(),
        1
    );
    insert(&pool, 3, "{\"value\":3}").await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while count(&pool, "external_host_effects").await != 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    handle.shutdown();
    run.await.unwrap().unwrap();
    assert!(!handle.snapshot().ready);
    assert_eq!(count(&pool, "ext.items").await, 0);
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_host_cooperative_shutdown_drains_current_row(pool: PgPool) {
    shutdown_case(pool, false).await;
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_host_forced_abort_rolls_back_and_releases_advisory_lease(pool: PgPool) {
    shutdown_case(pool, true).await;
}

async fn shutdown_case(pool: PgPool, forced: bool) {
    let listener = Arc::new(listener(&pool));
    listener.provision_source().await.unwrap();
    insert(&pool, 1, "{\"value\":1}").await;
    sqlx::raw_sql("CREATE FUNCTION external_host_block() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(2700060); RETURN OLD; END $$; CREATE TRIGGER external_host_block BEFORE DELETE ON ext.items FOR EACH ROW EXECUTE FUNCTION external_host_block()")
        .execute(&pool).await.unwrap();
    let mut blocker = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(2700060)")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let mut host = Host::new(if forced {
        Duration::from_millis(10)
    } else {
        Duration::from_secs(5)
    })
    .unwrap();
    let source = Arc::clone(&listener);
    host.service("external-source", RestartPolicy::NEVER, move |context| {
        let source = Arc::clone(&source);
        async move { raddle_host::postgres_external_table(&source, context).await }
    })
    .unwrap();
    let handle = host.handle();
    let run = tokio::spawn(host.run());
    tokio::time::timeout(Duration::from_secs(10), async {
        while !blocked(&pool).await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !handle.snapshot().ready,
        "uncommitted first row reported readiness"
    );
    handle.shutdown();
    let result = if forced {
        let result = run.await.unwrap();
        sqlx::query("SELECT pg_advisory_unlock(2700060)")
            .execute(&mut *blocker)
            .await
            .unwrap();
        result
    } else {
        sqlx::query("SELECT pg_advisory_unlock(2700060)")
            .execute(&mut *blocker)
            .await
            .unwrap();
        run.await.unwrap()
    };
    assert_eq!(result.is_err(), forced);
    let mut tx = blocker.begin().await.unwrap();
    sqlx::query("SET LOCAL statement_timeout='5s'")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(12000)")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    drop(blocker);
    assert_eq!(count(&pool, "ext.items").await, i64::from(forced));
    assert_eq!(count(&pool, "raddle_jobs").await, i64::from(!forced));
    if forced {
        sqlx::query("DROP TRIGGER external_host_block ON ext.items")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            listener
                .poll_once(chrono::Utc::now())
                .await
                .unwrap()
                .accepted,
            1
        );
    }
}
