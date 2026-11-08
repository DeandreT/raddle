//! Unreadable stored rows cannot strand healthy dead letters behind a failed page.

use raddle::wire::Envelope;
use raddle_postgres::{JobStatus, Store};
use reqwest::Client;
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::task::JoinSet;
use uuid::Uuid;

use super::{api, checked, client, mutation, serve, stage};
use raddle_axum::Api;
use raddle_axum::dead_letters::DeadLetterAction;

const SECRET: &str = "SENSITIVE-UNREADABLE-PAYLOAD";

#[derive(Clone, Copy, Debug)]
enum Unreadable {
    Invalid,
    Identity,
    ProtectedFrame,
    ProtectedEnvelope,
}

async fn damage(pool: &PgPool, id: Uuid, envelope: &Envelope, kind: Unreadable) -> (Uuid, Vec<u8>) {
    let claimed = Uuid::new_v4();
    let protected = json!({"format":"raddle-aes256gcm-v2", "key_id":SECRET,
        "inner_content_type":"application/json", "nonce":SECRET,
        "ciphertext":SECRET, "fault_source":null});
    let bytes = match kind {
        Unreadable::Invalid => format!("{{not-json:{SECRET}").into_bytes(),
        Unreadable::ProtectedFrame => serde_json::to_vec(&protected).unwrap(),
        Unreadable::Identity | Unreadable::ProtectedEnvelope => {
            let mut value = serde_json::to_value(envelope).unwrap();
            if matches!(kind, Unreadable::Identity) {
                value["id"] = json!(claimed);
                value["payload"] = json!({"private":SECRET});
            } else {
                value["payload"] = protected;
            }
            serde_json::to_vec(&value).unwrap()
        }
    };
    sqlx::query("UPDATE raddle_jobs SET envelope=$2 WHERE id=$1")
        .bind(id)
        .bind(&bytes)
        .execute(pool)
        .await
        .unwrap();
    (claimed, bytes)
}

async fn search(api: &Api, client: &Client, base: &str, body: Value) -> Value {
    let response = client
        .post(format!("{base}/dead-letters/"))
        .bearer_auth("reader")
        .json(&body)
        .send()
        .await
        .unwrap();
    checked(api, "/dead-letters/", "post", 200, response).await
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn unreadable_middle_candidate_keeps_healthy_neighbors_available(pool: PgPool) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let mut ids = Vec::new();
    for n in 0..3 {
        ids.push(stage(&store, "tenant", "negative-search", &format!("healthy-{n}")).await);
    }
    damage(&pool, ids[1].0, &ids[1].1, Unreadable::Invalid).await;
    let api = api(store, None);
    let mut tasks = JoinSet::new();
    let base = serve(&api, &mut tasks).await;
    let page = search(
        &api,
        &client(),
        &base,
        json!({"destination":"negative-search","limit":3}),
    )
    .await;
    assert_eq!(page["items"].as_array().unwrap().len(), 2);
    assert_eq!(page["unavailable"].as_array().unwrap().len(), 1);
    assert_eq!(page["unavailable"][0]["id"], ids[1].0.to_string());
    assert!(page["next_after"].is_null());
    tasks.shutdown().await;
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn first_middle_last_unreadable_rows_keep_bounded_filter_pages_and_custody_isolated(
    pool: PgPool,
) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let api = api(store.clone(), None);
    let mut tasks = JoinSet::new();
    let base = serve(&api, &mut tasks).await;
    let client = client();
    for kind in [
        Unreadable::Invalid,
        Unreadable::Identity,
        Unreadable::ProtectedFrame,
        Unreadable::ProtectedEnvelope,
    ] {
        for bad in 0..3 {
            let destination = format!("isolation-{kind:?}-{bad}").to_ascii_lowercase();
            let mut rows = Vec::new();
            for n in 0..3 {
                rows.push(stage(&store, "tenant", &destination, &format!("healthy-{n}")).await);
            }
            let id = rows[bad].0;
            let (claimed, bytes) = damage(&pool, id, &rows[bad].1, kind).await;
            let sequence: i64 = sqlx::query_scalar("SELECT sequence FROM raddle_jobs WHERE id=$1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
            // An unreadable other-tenant row must not appear even as a marker or
            // extend this tenant's continuation. Same destination avoids masking
            // a missing tenant predicate with a destination mismatch.
            if matches!(kind, Unreadable::Invalid) && bad == 0 {
                let (other, envelope, _) = stage(&store, "other", &destination, SECRET).await;
                damage(&pool, other, &envelope, Unreadable::Identity).await;
            }
            let snapshot: Value = sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(j) ORDER BY sequence) FROM raddle_jobs j WHERE tenant='tenant' AND destination=$1")
                .bind(&destination).fetch_one(&pool).await.unwrap();
            for limit in [1, 3] {
                for contract in [None, Some("test.failed"), Some("absent.contract")] {
                    let mut after = 0i64;
                    let mut healthy = Vec::new();
                    let mut unavailable = Vec::new();
                    let mut pages = 0;
                    loop {
                        pages += 1;
                        assert!(
                            pages <= 3,
                            "continuation must not strand the reader on a candidate"
                        );
                        let page = search(&api,&client,&base,json!({"destination":destination,
                            "error_code":"fixture-rejection","contract":contract,"limit":limit,"after":after})).await;
                        let text = page.to_string();
                        assert!(
                            !text.contains(SECRET) && !text.contains(&claimed.to_string()),
                            "unvalidated bytes/identity must be withheld"
                        );
                        assert!(
                            !text.contains("ciphertext")
                                && !text.contains("key_id")
                                && !text.contains("expected value")
                        );
                        let items = page["items"].as_array().unwrap();
                        let missing = page["unavailable"].as_array().unwrap();
                        assert!(items.len() + missing.len() <= limit as usize);
                        healthy.extend(
                            items
                                .iter()
                                .map(|item| item["id"].as_str().unwrap().parse::<Uuid>().unwrap()),
                        );
                        unavailable.extend(missing.iter().cloned());
                        match page["next_after"].as_i64() {
                            Some(next) => {
                                assert!(
                                    next > after,
                                    "cursor must advance past unreadable or filtered rows"
                                );
                                after = next;
                            }
                            None => break,
                        }
                    }
                    let expected: Vec<_> = if contract == Some("absent.contract") {
                        Vec::new()
                    } else {
                        rows.iter()
                            .enumerate()
                            .filter(|(n, _)| *n != bad)
                            .map(|(_, row)| row.0)
                            .collect()
                    };
                    assert_eq!(healthy, expected);
                    assert_eq!(
                        unavailable,
                        json!([{"id":id,"sequence":sequence,"outcome":"payload_unavailable"}])
                            .as_array()
                            .unwrap()
                            .clone()
                    );
                }
            }
            let after_search: Value = sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(j) ORDER BY sequence) FROM raddle_jobs j WHERE tenant='tenant' AND destination=$1")
                .bind(&destination).fetch_one(&pool).await.unwrap();
            assert!(
                snapshot == after_search,
                "search must not alter custody, fingerprints, fencing, or stored bytes"
            );
            let audits: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM raddle_replay_audit a JOIN raddle_jobs j ON a.job_id=j.id WHERE j.destination=$1)+(SELECT count(*) FROM raddle_discard_audit a JOIN raddle_jobs j ON a.job_id=j.id WHERE j.destination=$1)")
                .bind(&destination).fetch_one(&pool).await.unwrap();
            assert_eq!(
                audits, 0,
                "search must not acknowledge, publish, replay, or discard work"
            );
            let fresh = search(
                &api,
                &client,
                &base,
                json!({"destination":destination,"limit":10}),
            )
            .await;
            assert_eq!(fresh["items"].as_array().unwrap().len(), 2);
            let healthy_id = rows[(bad + 1) % 3].0;
            let response = client
                .delete(format!("{base}/dead-letters/"))
                .bearer_auth("admin")
                .json(&mutation(
                    &[healthy_id.to_string()],
                    DeadLetterAction::Discard,
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(
                checked(&api, "/dead-letters/", "delete", 200, response).await["results"][0]["outcome"],
                "discarded"
            );
            let remaining = search(
                &api,
                &client,
                &base,
                json!({"destination":destination,"limit":3}),
            )
            .await;
            assert_eq!(remaining["items"].as_array().unwrap().len(), 1);
            assert_eq!(remaining["unavailable"][0]["id"], id.to_string());
            assert_eq!(
                store.inspect("tenant", id).await.unwrap().unwrap().status,
                JobStatus::Dead
            );
            assert!(
                sqlx::query_scalar::<_, Vec<u8>>("SELECT envelope FROM raddle_jobs WHERE id=$1")
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
                    == bytes
            );
        }
    }
    tasks.shutdown().await;
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn shared_database_failure_stays_an_error_and_next_healthy_search_recovers(pool: PgPool) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    stage(&store, "tenant", "database-failure", "healthy").await;
    let api = api(store, None);
    let mut tasks = JoinSet::new();
    let base = serve(&api, &mut tasks).await;
    let client = client();
    sqlx::query("ALTER TABLE raddle_jobs RENAME TO temporarily_unavailable_jobs")
        .execute(&pool)
        .await
        .unwrap();
    let response = client
        .post(format!("{base}/dead-letters/"))
        .bearer_auth("reader")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let body = checked(&api, "/dead-letters/", "post", 503, response).await;
    assert_eq!(
        body,
        json!({"code":"unavailable","message":"service unavailable"})
    );
    sqlx::query("ALTER TABLE temporarily_unavailable_jobs RENAME TO raddle_jobs")
        .execute(&pool)
        .await
        .unwrap();
    let page = search(
        &api,
        &client,
        &base,
        json!({"destination":"database-failure"}),
    )
    .await;
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(page["unavailable"], json!([]));
    tasks.shutdown().await;
}
