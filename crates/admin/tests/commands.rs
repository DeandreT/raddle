//! Exercise the compiled CLI against an isolated migrated database.
#![allow(clippy::unwrap_used)]

use std::process::{Command, Output};

use chrono::Utc;
use raddle_postgres::Store;
use sqlx::{ConnectOptions, PgPool};

fn cli(pool: &PgPool, args: &[&str]) -> Output {
    let url = pool.connect_options().to_url_lossy().to_string();
    Command::new(env!("CARGO_BIN_EXE_raddle-admin"))
        .args(args)
        .env("DATABASE_URL", url)
        .output()
        .unwrap()
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn diagnostics_are_read_only_and_replay_is_explicit_audited_and_scoped(pool: PgPool) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let output = cli(&pool, &["check"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["database"],
        "available"
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["read_only"],
        true
    );
    let output = cli(&pool, &["stats", "tenant"]);
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["ready"],
        0
    );
    let id = uuid::Uuid::new_v4();
    // Sanitized quarantine fixture: operational commands must never load its bytes.
    let envelope = r#"{"private":"not-readable-by-cli"}"#;
    sqlx::query("INSERT INTO raddle_jobs(id,source,tenant,destination,kind,message_id,envelope,fingerprint,available_at,status,max_attempts) VALUES($1,'ops','tenant','handler','local',$2,$3,$4,$5,'dead',8)")
        .bind(id).bind(uuid::Uuid::new_v4()).bind(envelope.as_bytes()).bind(vec![0u8;32]).bind(Utc::now()).execute(&pool).await.unwrap();
    let output = cli(&pool, &["inspect", "tenant", &id.to_string()]);
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("private"));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_replay_audit")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert!(
        !cli(
            &pool,
            &["replay", "tenant", &id.to_string(), "operator", "reason"]
        )
        .status
        .success()
    );
    assert!(
        !cli(
            &pool,
            &[
                "replay",
                "other",
                &id.to_string(),
                "operator",
                "reason",
                "--confirm-replay"
            ]
        )
        .status
        .success()
    );
    assert!(
        cli(
            &pool,
            &[
                "replay",
                "tenant",
                &id.to_string(),
                "operator",
                "reason",
                "--confirm-replay"
            ]
        )
        .status
        .success()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM raddle_replay_audit WHERE actor='operator' AND reason='reason'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM raddle_jobs WHERE id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "ready"
    );
    assert!(
        !cli(
            &pool,
            &[
                "replay",
                "tenant",
                &id.to_string(),
                "operator",
                "reason",
                "--confirm-replay"
            ]
        )
        .status
        .success()
    );
}
