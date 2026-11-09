use std::process::{Child, Command};
use std::time::Duration;

use chrono::Utc;
use raddle_postgres::external_tables::{Correction, Options, ReplayOutcome};
use serde_json::json;
use sqlx::{
    ConnectOptions, Connection, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use super::support;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_competing_pollers_one_connection_and_identity_versions(pool: PgPool) {
    let listener = support::setup(&pool, Default::default()).await;
    let options: PgConnectOptions = pool.connect_options().as_ref().clone();
    let single = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.disable_statement_logging())
        .await
        .unwrap();
    let other = support::listener(
        &single,
        support::config(),
        Options {
            advisory_lock: 12001,
            ..Default::default()
        },
    );
    for id in 1..=30 {
        support::insert(&pool, &support::config(), id, json!({"value":id}), None).await;
    }
    let (a, b) = tokio::join!(listener.poll_once(Utc::now()), other.poll_once(Utc::now()));
    a.unwrap();
    b.unwrap();
    for _ in 0..3 {
        listener.poll_once(Utc::now()).await.unwrap();
    }
    assert_eq!(support::count(&pool, "raddle_jobs").await, 30);
    support::worker(&pool).run_once(100).await.unwrap();
    support::insert(&pool, &support::config(), 1, json!({"value":1}), None).await;
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().duplicates, 1);
    support::insert(&pool, &support::config(), 1, json!({"value":999}), None).await;
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().quarantined, 1);
    let item = listener
        .search_quarantine(0, 10)
        .await
        .unwrap()
        .items
        .remove(0);
    let wrong = Correction {
        body: "{\"value\":2}".into(),
        alias: None,
    };
    assert!(
        listener
            .replay_quarantined(
                item.sequence,
                Some(&wrong),
                "op",
                "cannot replace success",
                Utc::now()
            )
            .await
            .is_err()
    );
    let restore = Correction {
        body: "{\"value\":1}".into(),
        alias: None,
    };
    assert_eq!(
        listener
            .replay_quarantined(
                item.sequence,
                Some(&restore),
                "op",
                "restore equivalent",
                Utc::now()
            )
            .await
            .unwrap(),
        ReplayOutcome::Reconciled
    );
    assert_eq!(support::count(&pool, "raddle_jobs").await, 30);
    assert_eq!(support::count(&pool, "external_effects").await, 30);
    single.close().await;
}

struct OwnedChild(Child);

impl std::ops::Deref for OwnedChild {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}
impl std::ops::DerefMut for OwnedChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Ok(None) = self.0.try_wait() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn child(pool: &PgPool, phase: &str) -> OwnedChild {
    OwnedChild(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "external_native_child", "--nocapture"])
            .env(
                "RADDLE_EXTERNAL_CHILD_URL",
                pool.connect_options().to_url_lossy().as_str(),
            )
            .env("RADDLE_EXTERNAL_CHILD_PHASE", phase)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    )
}

async fn wait_for_blocked(pool: &PgPool, application: &str) {
    tokio::time::timeout(Duration::from_secs(15),async {
        loop {
            let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND application_name=$1 AND wait_event='advisory')")
                .bind(application).fetch_one(pool).await.unwrap();
            if blocked {break;}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_two_real_polling_processes_coordinate_advisory_ownership(pool: PgPool) {
    let listener = support::setup(&pool, Default::default()).await;
    for id in 1..=3 {
        support::insert(&pool, &support::config(), id, json!({"value":id}), None).await;
    }
    sqlx::raw_sql("CREATE FUNCTION external_process_block() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(2700061); RETURN OLD; END $$; CREATE TRIGGER external_process_block BEFORE DELETE ON ext.items FOR EACH ROW EXECUTE FUNCTION external_process_block()")
        .execute(&pool).await.unwrap();
    let mut blocker = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(2700061)")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let mut first = child(&pool, "compete-first");
    wait_for_blocked(&pool, "external-native-compete-first").await;
    let mut second = child(&pool, "compete-second");
    assert_ne!(first.id(), second.id());
    assert_ne!(first.id(), std::process::id());
    assert_ne!(second.id(), std::process::id());
    assert_eq!(
        second.wait().unwrap().code(),
        Some(25),
        "second process did not observe advisory contention"
    );
    sqlx::query("SELECT pg_advisory_unlock(2700061)")
        .execute(&mut *blocker)
        .await
        .unwrap();
    assert_eq!(first.wait().unwrap().code(), Some(24));
    drop(blocker);
    assert_eq!(support::count(&pool, "ext.items").await, 0);
    assert_eq!(support::count(&pool, "raddle_jobs").await, 3);
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().accepted, 0);
    support::worker(&pool).run_once(10).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 3);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_real_child_exit_before_commit_and_after_unobserved_commit(pool: PgPool) {
    let listener = support::setup(&pool, Default::default()).await;
    support::insert(&pool, &support::config(), 1, json!({"value":1}), None).await;
    sqlx::raw_sql("CREATE FUNCTION external_crash_block() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(2700057); RETURN OLD; END $$; CREATE TRIGGER external_crash_block BEFORE DELETE ON ext.items FOR EACH ROW EXECUTE FUNCTION external_crash_block()")
        .execute(&pool).await.unwrap();
    let mut blocker = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(2700057)")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let mut process = child(&pool, "before");
    wait_for_blocked(&pool, "external-native-before").await;
    process.kill().unwrap();
    assert!(!process.wait().unwrap().success());
    sqlx::query("SELECT pg_advisory_unlock(2700057)")
        .execute(&mut *blocker)
        .await
        .unwrap();
    // A fresh transaction waits for server-side disconnect rollback to finish.
    let mut tx = blocker.begin().await.unwrap();
    sqlx::query("SET LOCAL statement_timeout='5s'")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT id FROM ext.items FOR UPDATE")
        .fetch_all(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    drop(blocker);
    assert_eq!(support::count(&pool, "ext.items").await, 1);
    assert_eq!(support::count(&pool, "raddle_jobs").await, 0);
    let accepted: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raddle_external_inputs WHERE status='accepted'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(accepted, 0);
    sqlx::query("DROP TRIGGER external_crash_block ON ext.items")
        .execute(&pool)
        .await
        .unwrap();
    let result = child(&pool, "after").wait().unwrap();
    assert_eq!(result.code(), Some(24));
    assert_eq!(support::count(&pool, "ext.items").await, 0);
    assert_eq!(support::count(&pool, "raddle_jobs").await, 1);
    support::insert(&pool, &support::config(), 1, json!({"value":1}), None).await;
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().duplicates, 1);
    support::worker(&pool).run_once(100).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_cancelled_poll_releases_session_lock_and_rolls_back(pool: PgPool) {
    let listener = support::setup(
        &pool,
        Options {
            operation_timeout: Duration::from_secs(1),
            ..Default::default()
        },
    )
    .await;
    support::insert(&pool, &support::config(), 1, json!({"value":1}), None).await;
    sqlx::raw_sql("CREATE FUNCTION external_cancel_block() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(2700058); RETURN OLD; END $$; CREATE TRIGGER external_cancel_block BEFORE DELETE ON ext.items FOR EACH ROW EXECUTE FUNCTION external_cancel_block()")
        .execute(&pool).await.unwrap();
    let mut blocker = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(2700058)")
        .execute(&mut *blocker)
        .await
        .unwrap();
    assert!(matches!(
        listener.poll_once(Utc::now()).await,
        Err(raddle_postgres::external_tables::Error::Timeout)
    ));
    sqlx::query("SELECT pg_advisory_unlock(2700058)")
        .execute(&mut *blocker)
        .await
        .unwrap();
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
    assert_eq!(support::count(&pool, "ext.items").await, 1);
    assert_eq!(support::count(&pool, "raddle_jobs").await, 0);
    sqlx::query("DROP TRIGGER external_cancel_block ON ext.items")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().accepted, 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_cancelled_raw_replay_leaves_no_partial_audit_or_jobs(pool: PgPool) {
    let listener = support::setup(
        &pool,
        Options {
            operation_timeout: Duration::from_secs(1),
            ..Default::default()
        },
    )
    .await;
    support::insert(&pool, &support::config(), 1, json!({"value":"bad"}), None).await;
    listener.poll_once(Utc::now()).await.unwrap();
    let sequence = listener.search_quarantine(0, 10).await.unwrap().items[0].sequence;
    sqlx::raw_sql("CREATE FUNCTION external_replay_cancel_block() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='replayed' THEN PERFORM pg_advisory_xact_lock(2700059); END IF; RETURN NEW; END $$; CREATE TRIGGER external_replay_cancel_block BEFORE UPDATE ON raddle_external_inputs FOR EACH ROW EXECUTE FUNCTION external_replay_cancel_block()")
        .execute(&pool).await.unwrap();
    let mut blocker = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(2700059)")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let correction = Correction {
        body: "{\"value\":1}".into(),
        alias: None,
    };
    assert!(matches!(
        listener
            .replay_quarantined(
                sequence,
                Some(&correction),
                "op",
                "cancelled correction",
                Utc::now()
            )
            .await,
        Err(raddle_postgres::external_tables::Error::Timeout)
    ));
    sqlx::query("SELECT pg_advisory_unlock(2700059)")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let mut tx = blocker.begin().await.unwrap();
    sqlx::query("SET LOCAL statement_timeout='5s'")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT sequence FROM raddle_external_inputs FOR UPDATE")
        .fetch_all(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    drop(blocker);
    assert_eq!(support::count(&pool, "raddle_jobs").await, 0);
    assert_eq!(
        listener.search_quarantine(0, 10).await.unwrap().items.len(),
        1
    );
    assert_eq!(
        listener
            .replay_quarantined(
                sequence,
                Some(&correction),
                "op",
                "corrected retry",
                Utc::now()
            )
            .await
            .unwrap(),
        ReplayOutcome::Accepted
    );
}
