//! Server-side deadlines release actual blocked PostgreSQL statements after cancellation.

use std::time::Duration;

use axum::http::HeaderMap;
use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle_axum::dead_letters::DeadLetterAction;
use raddle_axum::{ApiBuilder, Permission, Principal};
use raddle_postgres::{JobStatus, Store};
use serde_json::json;
use sqlx::PgPool;
use tokio::task::JoinSet;

use super::{Failed, Policy, authenticate, checked, client, mutation, serve, stage};

async fn blocked_audit_backend(pool: &PgPool) -> i32 {
    // Bounded fixture coordination, matching the existing PostgreSQL cancellation
    // tests: observe a real server lock, never infer it from a client-side sleep.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND wait_event_type='Lock' AND query LIKE 'INSERT INTO raddle_replay_audit%' LIMIT 1")
                .fetch_optional(pool).await.unwrap();
            if let Some(pid) = pid { return pid; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.unwrap()
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn dropped_http_future_after_quota_lock_does_not_poison_later_tenant_acceptance(
    pool: PgPool,
) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let (id, _, _) = stage(&store, "tenant", "client-cancel", "PRIVATE CANCELED DATA").await;
    let mut builder = ApiBuilder::new(RuntimeBuilder::default().build(), |headers: &HeaderMap| {
        let identity = authenticate(headers)?;
        Principal::try_new(identity.tenant(), identity.subject(), [Permission::Invoke])
    });
    builder.dead_letters(store.clone(), Policy).unwrap();
    builder
        .durable::<Failed>("/healthy", store.clone(), "healthy")
        .unwrap();
    let api = builder.build();
    let mut tasks = JoinSet::new();
    let base = serve(&api, &mut tasks).await;
    let client = client();
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE raddle_replay_audit IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let request = client
        .post(format!("{base}/dead-letters/replay"))
        .bearer_auth("admin")
        .json(&mutation(&[id.to_string()], DeadLetterAction::Replay));
    let mut pending = tokio::spawn(async move { request.send().await });
    let pid = tokio::select! {
        result = &mut pending => panic!("request completed before actual blocked audit observation: {result:?}"),
        pid = blocked_audit_backend(&pool) => pid,
    };
    assert!(pid > 0);
    // The audit statement is reached only after the tenant's tuple-version fence.
    // Independently prove that the request really owns that quota lock now.
    let mut probe = pool.begin().await.unwrap();
    let error =
        sqlx::query("SELECT tenant FROM raddle_quotas WHERE tenant='tenant' FOR UPDATE NOWAIT")
            .execute(&mut *probe)
            .await
            .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("55P03")
    );
    probe.rollback().await.unwrap();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    // Keep the audit blocker held. Only the transaction-local server deadline,
    // not unlocking the fixture, can release replay's tenant admission lock.
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        client
            .post(format!("{base}/healthy"))
            .bearer_auth("admin")
            .header("Idempotency-Key", "after-cancel")
            .json(&json!({"private":"fresh healthy request"}))
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    checked(&api, "/healthy", "post", 202, response).await;
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Dead
    );
    blocker.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_replay_audit WHERE job_id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    let response = client
        .post(format!("{base}/dead-letters/replay"))
        .bearer_auth("admin")
        .json(&mutation(&[id.to_string()], DeadLetterAction::Replay))
        .send()
        .await
        .unwrap();
    assert_eq!(
        checked(&api, "/dead-letters/replay", "post", 200, response).await["results"][0]["outcome"],
        "replayed"
    );
    tasks.shutdown().await;
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn legacy_replay_none_path_keeps_application_timeout_and_transaction_settings(pool: PgPool) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let (id, _, _) = stage(&store, "tenant", "legacy-budget", "legacy replay").await;
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE raddle_replay_audit IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let store_for_task = store.clone();
    let mut pending = tokio::spawn(async move {
        store_for_task
            .replay(
                "tenant",
                id,
                "operator",
                "legacy caller owns its budget",
                Utc::now(),
            )
            .await
    });
    tokio::select! {
        result = &mut pending => panic!("legacy replay completed before its blocked audit: {result:?}"),
        _pid = blocked_audit_backend(&pool) => {},
    }
    // The unchanged default replay path must not silently inherit the new HTTP
    // server deadline. Applications/CLI retain their original budget ownership.
    tokio::select! {
        result = &mut pending => panic!("legacy replay acquired an implicit two-second deadline: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(2200)) => {},
    }
    blocker.rollback().await.unwrap();
    assert!(pending.await.unwrap().unwrap());
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Ready
    );
}
