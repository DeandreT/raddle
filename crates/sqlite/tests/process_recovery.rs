//! Real process exits exercise SQLite rollback and committed WAL recovery.
#![allow(clippy::unwrap_used)]

use chrono::Utc;
use raddle::wire::{Envelope, WireMessage};
use raddle_sqlite::{Delivery, DeliveryKind, Store};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Message;
impl WireMessage for Message {
    const NAME: &'static str = "test.process";
    const VERSION: u32 = 1;
}

#[tokio::test]
async fn subprocess_probe() {
    let Ok(path) = std::env::var("RADDLE_SQLITE_CRASH_FILE") else {
        return;
    };
    let store = Store::open(path).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    sqlx::query("INSERT INTO business VALUES(1)")
        .execute(scope.connection())
        .await
        .unwrap();
    scope
        .stage(
            "test",
            "tenant",
            &Envelope::encode(&Message).unwrap(),
            &[Delivery::new(DeliveryKind::Local, "audit", Utc::now())],
        )
        .await
        .unwrap();
    if std::env::var("RADDLE_SQLITE_CRASH_PHASE").as_deref() == Ok("after-commit") {
        scope.commit(Utc::now()).await.unwrap();
        std::process::exit(62);
    }
    std::process::exit(61);
}

#[tokio::test]
async fn process_exit_keeps_only_committed_business_and_message_rows() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("crash.sqlite");
    let store = Store::open(&path).await.unwrap();
    store.migrate().await.unwrap();
    let mut scope = store.begin().await.unwrap();
    sqlx::query("CREATE TABLE business(value INTEGER)")
        .execute(scope.connection())
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    store.close().await;
    for (phase, exit, expected) in [("before-commit", 61, 0i64), ("after-commit", 62, 1)] {
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "subprocess_probe", "--nocapture"])
            .env("RADDLE_SQLITE_CRASH_FILE", &path)
            .env("RADDLE_SQLITE_CRASH_PHASE", phase)
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(exit),
            "child must reach the injected exit"
        );
        let reopened = Store::open(&path).await.unwrap();
        let mut scope = reopened.begin().await.unwrap();
        for table in ["business", "raddle_jobs"] {
            let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(scope.connection())
                .await
                .unwrap();
            assert_eq!(count, expected);
        }
        scope.rollback().await.unwrap();
        reopened.close().await;
    }
}
