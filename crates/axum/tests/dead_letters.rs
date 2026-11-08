//! Actual socket and disposable PostgreSQL management contracts, not tower-only proof.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use axum::http::HeaderMap;
use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::wire::{Envelope, WireMessage};
use raddle_axum::dead_letters::{DeadLetterAction, DeadLetterAuthorizer, DeadLetterMutation};
use raddle_axum::{Api, ApiBuilder, ApiError, Principal};
use raddle_postgres::{Delivery, DeliveryKind, FailureDisposition, JobStatus, RetryPolicy, Store};
use reqwest::{Client, Method, Response};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::task::JoinSet;
use uuid::Uuid;

#[path = "dead_letters/cancellation.rs"]
mod cancellation;
#[path = "dead_letters/isolation.rs"]
mod isolation;

#[derive(Serialize, Deserialize, JsonSchema)]
struct Failed {
    private: String,
}

impl WireMessage for Failed {
    const NAME: &'static str = "test.failed";
    const VERSION: u32 = 1;
}

fn authenticate(headers: &HeaderMap) -> Result<Principal, ApiError> {
    let token = headers.get("Authorization").and_then(|v| v.to_str().ok());
    let (tenant, subject) = match token {
        Some("Bearer admin") => ("tenant", "admin"),
        Some("Bearer other") => ("other", "admin"),
        Some("Bearer reader") => ("tenant", "reader"),
        Some("Bearer replayer") => ("tenant", "replayer"),
        Some("Bearer discarder") => ("tenant", "discarder"),
        Some("Bearer wrong") => ("tenant", "wrong"),
        _ => return Err(ApiError::unauthorized()),
    };
    // No ordinary Invoke/Inspect/Replay permissions accidentally authorize this slice.
    Principal::try_new(tenant, subject, [])
}

struct Policy;

impl DeadLetterAuthorizer for Policy {
    fn authorize(&self, principal: &Principal, action: DeadLetterAction) -> Result<(), ApiError> {
        let allowed = principal.subject() == "admin"
            || matches!(
                (principal.subject(), action),
                ("reader", DeadLetterAction::Search)
                    | ("replayer", DeadLetterAction::Replay)
                    | ("discarder", DeadLetterAction::Discard)
            );
        if allowed {
            Ok(())
        } else {
            Err(ApiError::forbidden())
        }
    }

    fn confirm(
        &self,
        principal: &Principal,
        action: DeadLetterAction,
        request: &DeadLetterMutation,
    ) -> Result<(), ApiError> {
        // Test approval only. Real applications verify a signed challenge or an
        // independent approval store; a caller-controlled boolean is not approval.
        let expected = format!("fixture-approval:{}:{action:?}", principal.tenant());
        if request.confirmation == expected {
            Ok(())
        } else {
            Err(ApiError::forbidden())
        }
    }
}

fn api(store: Store, prefix: Option<&str>) -> Api {
    let mut builder = ApiBuilder::new(RuntimeBuilder::default().build(), authenticate);
    match prefix {
        Some(prefix) => builder.dead_letters_at(prefix, store, Policy).unwrap(),
        None => builder.dead_letters(store, Policy).unwrap(),
    };
    builder.build()
}

async fn serve(api: &Api, tasks: &mut JoinSet<()>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = api.router();
    tasks.spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{address}")
}

fn client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

fn mutation(ids: &[String], action: DeadLetterAction) -> Value {
    json!({"ids": ids, "reason":"operator approved recovery", "confirmation":format!("fixture-approval:tenant:{action:?}")})
}

async fn checked(api: &Api, path: &str, method: &str, status: u16, response: Response) -> Value {
    assert_eq!(response.status().as_u16(), status, "{method} {path}");
    assert_eq!(response.headers()["Content-Type"], "application/json");
    let body: Value = response.json().await.unwrap();
    let mut schema = api.specification()["paths"][path][method]["responses"][status.to_string()]["content"]["application/json"]["schema"].clone();
    schema["components"] = api.specification()["components"].clone();
    assert!(
        jsonschema::validator_for(&schema).unwrap().is_valid(&body),
        "{body}"
    );
    body
}

async fn stage(
    store: &Store,
    tenant: &str,
    destination: &str,
    private: &str,
) -> (Uuid, Envelope, Delivery) {
    let envelope = Envelope::encode(&Failed {
        private: private.into(),
    })
    .unwrap()
    .with_header("test-metadata".into(), "canonical".into())
    .unwrap();
    let mut delivery = Delivery::new(DeliveryKind::Local, destination, Utc::now());
    delivery.partition_key = Some("ordered".into());
    let mut scope = store.begin().await.unwrap();
    scope
        .stage(
            "dlq-tests",
            tenant,
            &envelope,
            std::slice::from_ref(&delivery),
        )
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    // Exercise real claim/fail custody instead of manufacturing the failed row.
    let claim = store
        .claim_for(
            DeliveryKind::Local,
            destination,
            1,
            Duration::from_secs(60),
            Utc::now(),
        )
        .await
        .unwrap()
        .remove(0);
    let id = claim.id();
    store
        .fail(
            &claim,
            FailureDisposition::Reject,
            RetryPolicy::default(),
            "fixture-rejection",
            Utc::now(),
        )
        .await
        .unwrap();
    (id, envelope, delivery)
}

#[tokio::test]
async fn invalid_prefixes_and_duplicate_registration_leave_router_usable() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://localhost/unused")
        .unwrap();
    let store = Store::new(pool);
    let mut builder = ApiBuilder::new(RuntimeBuilder::default().build(), authenticate);
    for prefix in [
        "",
        "/",
        "relative",
        "/tail/",
        "/double//slash",
        "/{tenant}",
        "/bad?query",
        "/bad#fragment",
        "/private/../dlq",
        "/private/./dlq",
        "/has space",
        "/é",
    ] {
        assert!(
            builder
                .dead_letters_at(prefix, store.clone(), Policy)
                .is_err(),
            "{prefix}"
        );
    }
    builder.dead_letters(store.clone(), Policy).unwrap();
    assert!(builder.dead_letters(store.clone(), Policy).is_err());
    builder
        .dead_letters_at("/private/dlq", store, Policy)
        .unwrap();
    let api = builder.build();
    for root in ["/dead-letters/", "/private/dlq/"] {
        assert!(api.specification()["paths"][root]["post"]["requestBody"].is_object());
        assert!(api.specification()["paths"][root]["delete"]["requestBody"].is_object());
    }
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn sockets_enforce_auth_roles_tenant_bounds_methods_and_registered_contracts(pool: PgPool) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let (id, envelope, _) = stage(&store, "tenant", "alpha", "PRIVATE FAILED DATA").await;
    stage(&store, "other", "beta", "OTHER TENANT DATA").await;
    let mut tasks = JoinSet::new();
    let client = client();
    for prefix in [None, Some("/private/dlq")] {
        let api = api(store.clone(), prefix);
        let root = format!("{}/", prefix.unwrap_or("/dead-letters"));
        let replay = format!("{}replay", root);
        let base = serve(&api, &mut tasks).await;
        for (path, method, body) in [
            (&root, Method::POST, json!({})),
            (
                &replay,
                Method::POST,
                mutation(&[id.to_string()], DeadLetterAction::Replay),
            ),
            (
                &root,
                Method::DELETE,
                mutation(&[id.to_string()], DeadLetterAction::Discard),
            ),
        ] {
            for token in [None, Some("invalid"), Some("wrong")] {
                let mut request = client
                    .request(method.clone(), format!("{base}{path}"))
                    .json(&body);
                if let Some(token) = token {
                    request = request.bearer_auth(token);
                }
                let response = request.send().await.unwrap();
                let status = if token == Some("wrong") { 403 } else { 401 };
                if status == 401 {
                    assert_eq!(response.headers()["WWW-Authenticate"], "Bearer");
                }
                let error = checked(
                    &api,
                    path,
                    method.as_str().to_ascii_lowercase().as_str(),
                    status,
                    response,
                )
                .await;
                assert!(!error.to_string().contains("PRIVATE"));
            }
        }
        for (method, path, token, body) in [
            (Method::POST, &root, "replayer", json!({})),
            (Method::POST, &root, "discarder", json!({})),
            (
                Method::POST,
                &replay,
                "reader",
                mutation(&[id.to_string()], DeadLetterAction::Replay),
            ),
            (
                Method::DELETE,
                &root,
                "replayer",
                mutation(&[id.to_string()], DeadLetterAction::Discard),
            ),
        ] {
            assert_eq!(
                client
                    .request(method, format!("{base}{path}"))
                    .bearer_auth(token)
                    .json(&body)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                403
            );
        }
        let response = client.post(format!("{base}{root}")).bearer_auth("reader")
            .header("X-Tenant", "other").json(&json!({"destination":"alpha", "contract":"test.failed", "error_code":"fixture-rejection", "limit":1})).send().await.unwrap();
        let page = checked(&api, &root, "post", 200, response).await;
        assert_eq!(page["items"].as_array().unwrap().len(), 1);
        assert_eq!(page["items"][0]["id"], id.to_string());
        assert_eq!(page["items"][0]["message_id"], envelope.id().to_string());
        assert_eq!(
            page["items"][0]["envelope"],
            serde_json::to_value(&envelope).unwrap()
        );
        assert!(!page.to_string().contains("OTHER TENANT"));
        let response = client
            .post(format!("{base}{root}"))
            .bearer_auth("other")
            .json(&json!({"destination":"alpha"}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            checked(&api, &root, "post", 200, response).await["items"],
            json!([])
        );
        for (path, method, action) in [
            (&replay, Method::POST, DeadLetterAction::Replay),
            (&root, Method::DELETE, DeadLetterAction::Discard),
        ] {
            let mut body = mutation(&[id.to_string()], action);
            body["confirmation"] = json!(format!("fixture-approval:other:{action:?}"));
            let response = client
                .request(method.clone(), format!("{base}{path}"))
                .bearer_auth("other")
                .json(&body)
                .send()
                .await
                .unwrap();
            assert_eq!(
                checked(
                    &api,
                    path,
                    &method.as_str().to_ascii_lowercase(),
                    200,
                    response
                )
                .await["results"][0]["outcome"],
                "not_eligible"
            );
            body["confirmation"] = json!("true");
            assert_eq!(
                client
                    .request(method.clone(), format!("{base}{path}"))
                    .bearer_auth("admin")
                    .json(&body)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                403
            );
            body["confirmation"] = json!(format!("fixture-approval:tenant:{action:?}"));
            body["tenant"] = json!("other");
            assert_eq!(
                client
                    .request(method, format!("{base}{path}"))
                    .bearer_auth("admin")
                    .json(&body)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                400
            );
        }
        for body in [
            json!({"limit":0}),
            json!({"limit":11}),
            json!({"after":-1}),
            json!({"tenant":"other"}),
            json!({"TenantId":"other"}),
            json!({"contract":"x".repeat(129)}),
            json!({"destination":""}),
            json!({"error_code":"x".repeat(65)}),
        ] {
            let response = client
                .post(format!("{base}{root}"))
                .bearer_auth("admin")
                .json(&body)
                .send()
                .await
                .unwrap();
            checked(&api, &root, "post", 400, response).await;
        }
        for body in [
            json!({"ids":[],"reason":"why","confirmation":"x"}),
            json!({"ids":vec![id.to_string();33],"reason":"why","confirmation":"x"}),
            json!({"ids":[id],"reason":"x".repeat(513),"confirmation":"x"}),
            json!({"ids":[id],"reason":"why","confirmation":"x".repeat(129)}),
        ] {
            checked(
                &api,
                &replay,
                "post",
                400,
                client
                    .post(format!("{base}{replay}"))
                    .bearer_auth("admin")
                    .json(&body)
                    .send()
                    .await
                    .unwrap(),
            )
            .await;
        }
        for (request, status) in [
            (
                client
                    .post(format!("{base}{root}"))
                    .header("Content-Type", "text/plain")
                    .body("{}"),
                415,
            ),
            (
                client
                    .post(format!("{base}{root}"))
                    .header("Content-Type", "application/json")
                    .body("{"),
                400,
            ),
            (
                client
                    .post(format!("{base}{root}"))
                    .header("Content-Type", "application/json")
                    .body(" ".repeat(16_385)),
                413,
            ),
        ] {
            checked(
                &api,
                &root,
                "post",
                status,
                request.bearer_auth("admin").send().await.unwrap(),
            )
            .await;
        }
        let response = client
            .get(format!("{base}{root}"))
            .bearer_auth("admin")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 405);
        let error: Value = response.json().await.unwrap();
        assert_eq!(error["code"], "method_not_allowed");
        let spec = client
            .get(format!("{base}/openapi.json"))
            .bearer_auth("admin")
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        assert_eq!(spec, *api.specification());
        assert_eq!(
            store.inspect("tenant", id).await.unwrap().unwrap().status,
            JobStatus::Dead
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_discard_audit")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_replay_audit")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    tasks.shutdown().await;
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn cursor_pages_filters_and_empty_filtered_pages_return_actual_failed_data(pool: PgPool) {
    let store = Store::new(pool);
    store.migrate().await.unwrap();
    let mut ids = Vec::new();
    for n in 0..4 {
        ids.push(
            stage(&store, "tenant", "pages", &format!("private-{n}"))
                .await
                .0,
        );
    }
    let api = api(store, None);
    let mut tasks = JoinSet::new();
    let base = serve(&api, &mut tasks).await;
    let client = client();
    let mut after = json!(0);
    let mut seen = Vec::new();
    loop {
        let response = client
            .post(format!("{base}/dead-letters/"))
            .bearer_auth("admin")
            .json(&json!({"limit":1,"after":after,"destination":"pages"}))
            .send()
            .await
            .unwrap();
        let page = checked(&api, "/dead-letters/", "post", 200, response).await;
        seen.push(
            page["items"][0]["id"]
                .as_str()
                .unwrap()
                .parse::<Uuid>()
                .unwrap(),
        );
        after = page["next_after"].clone();
        if after.is_null() {
            break;
        }
    }
    assert_eq!(seen, ids);
    let response = client
        .post(format!("{base}/dead-letters/"))
        .bearer_auth("admin")
        .json(&json!({"limit":1,"contract":"absent.contract"}))
        .send()
        .await
        .unwrap();
    let page = checked(&api, "/dead-letters/", "post", 200, response).await;
    assert_eq!(page["items"], json!([]));
    assert!(page["next_after"].is_i64());
    tasks.shutdown().await;
}

async fn audit_failure(pool: &PgPool, table: &str, id: Uuid) {
    sqlx::query(&format!("CREATE FUNCTION reject_selected_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_id='{id}'::uuid THEN RAISE EXCEPTION 'PRIVATE FAILED DATA'; END IF; RETURN NEW; END $$"))
        .execute(pool).await.unwrap();
    sqlx::query(&format!("CREATE TRIGGER reject_selected BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION reject_selected_audit()"))
        .execute(pool).await.unwrap();
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn per_id_invalid_stale_audit_failure_neighbors_and_failed_only_retries(pool: PgPool) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let api = api(store.clone(), None);
    let mut tasks = JoinSet::new();
    let base = serve(&api, &mut tasks).await;
    let client = client();
    for action in [DeadLetterAction::Replay, DeadLetterAction::Discard] {
        let (method, path, table, success, expected_status) = match action {
            DeadLetterAction::Replay => (
                Method::POST,
                "/dead-letters/replay",
                "raddle_replay_audit",
                "replayed",
                JobStatus::Ready,
            ),
            _ => (
                Method::DELETE,
                "/dead-letters/",
                "raddle_discard_audit",
                "discarded",
                JobStatus::Discarded,
            ),
        };
        for position in 0..3 {
            for kind in ["invalid", "stale", "audit"] {
                let destination =
                    format!("neighbors-{action:?}-{position}-{kind}").to_ascii_lowercase();
                let mut ids = Vec::new();
                for n in 0..3 {
                    ids.push(
                        stage(
                            &store,
                            "tenant",
                            &format!("{destination}-{n}"),
                            "PRIVATE FAILED DATA",
                        )
                        .await
                        .0
                        .to_string(),
                    );
                }
                let failed = ids[position].parse::<Uuid>().unwrap();
                match kind {
                    "invalid" => ids[position] = "PRIVATE INVALID ID".into(),
                    "stale" => {
                        let stale = stage(
                            &store,
                            "tenant",
                            &format!("{destination}-stale"),
                            "already requeued",
                        )
                        .await
                        .0;
                        assert!(
                            store
                                .replay("tenant", stale, "fixture", "already requeued", Utc::now())
                                .await
                                .unwrap()
                        );
                        ids[position] = stale.to_string();
                    }
                    _ => audit_failure(&pool, table, failed).await,
                }
                let response = client
                    .request(method.clone(), format!("{base}{path}"))
                    .bearer_auth("admin")
                    .json(&mutation(&ids, action))
                    .send()
                    .await
                    .unwrap();
                let body = checked(
                    &api,
                    path,
                    &method.as_str().to_ascii_lowercase(),
                    200,
                    response,
                )
                .await;
                assert!(!body.to_string().contains("PRIVATE"));
                for n in 0..3 {
                    let expected = if n != position {
                        success
                    } else {
                        match kind {
                            "invalid" => "invalid",
                            "stale" => "not_eligible",
                            _ => "unknown",
                        }
                    };
                    assert_eq!(body["results"][n], json!({"index":n,"outcome":expected}));
                }
                assert_eq!(
                    store
                        .inspect("tenant", failed)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    JobStatus::Dead
                );
                if kind == "audit" {
                    sqlx::query(&format!("DROP TRIGGER reject_selected ON {table}"))
                        .execute(&pool)
                        .await
                        .unwrap();
                    sqlx::query("DROP FUNCTION reject_selected_audit()")
                        .execute(&pool)
                        .await
                        .unwrap();
                }
                // Correct only the failed ID; never resubmit its successful neighbors.
                let corrected = mutation(&[failed.to_string()], action);
                let response = client
                    .request(method.clone(), format!("{base}{path}"))
                    .bearer_auth("admin")
                    .json(&corrected)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(
                    checked(
                        &api,
                        path,
                        &method.as_str().to_ascii_lowercase(),
                        200,
                        response
                    )
                    .await["results"][0]["outcome"],
                    success
                );
                assert_eq!(
                    store
                        .inspect("tenant", failed)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    expected_status
                );
                for input in ids
                    .iter()
                    .enumerate()
                    .filter(|(n, _)| *n != position)
                    .map(|(_, v)| v)
                {
                    let count: i64 = sqlx::query_scalar(&format!(
                        "SELECT count(*) FROM {table} WHERE job_id=$1"
                    ))
                    .bind(input.parse::<Uuid>().unwrap())
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                    assert_eq!(
                        count, 1,
                        "corrected retry duplicated a successful neighbor's audit"
                    );
                }
                let repeated = client
                    .request(method.clone(), format!("{base}{path}"))
                    .bearer_auth("admin")
                    .json(&corrected)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(
                    checked(
                        &api,
                        path,
                        &method.as_str().to_ascii_lowercase(),
                        200,
                        repeated
                    )
                    .await["results"][0]["outcome"],
                    "not_eligible"
                );
            }
        }
    }
    tasks.shutdown().await;
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn deadline_cancellation_rolls_back_unfinished_mutation_and_same_listener_recovers(
    pool: PgPool,
) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let api = api(store.clone(), None);
    let mut tasks = JoinSet::new();
    let base = serve(&api, &mut tasks).await;
    let client = client();
    for action in [DeadLetterAction::Replay, DeadLetterAction::Discard] {
        let (method, path, table) = if action == DeadLetterAction::Replay {
            (Method::POST, "/dead-letters/replay", "raddle_replay_audit")
        } else {
            (Method::DELETE, "/dead-letters/", "raddle_discard_audit")
        };
        let (id, _, _) = stage(
            &store,
            "tenant",
            &format!("cancel-{action:?}").to_ascii_lowercase(),
            "PRIVATE CANCELED DATA",
        )
        .await;
        let mut lock = pool.begin().await.unwrap();
        sqlx::query(&format!("LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))
            .execute(&mut *lock)
            .await
            .unwrap();
        let body = mutation(&[id.to_string()], action);
        let response = client
            .request(method.clone(), format!("{base}{path}"))
            .bearer_auth("admin")
            .json(&body)
            .send()
            .await
            .unwrap();
        let result = checked(
            &api,
            path,
            &method.as_str().to_ascii_lowercase(),
            200,
            response,
        )
        .await;
        assert_eq!(result["results"][0]["outcome"], "unknown");
        lock.rollback().await.unwrap();
        assert_eq!(
            store.inspect("tenant", id).await.unwrap().unwrap().status,
            JobStatus::Dead
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table} WHERE job_id=$1"))
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        let response = client
            .request(method.clone(), format!("{base}{path}"))
            .bearer_auth("admin")
            .json(&body)
            .send()
            .await
            .unwrap();
        let result = checked(
            &api,
            path,
            &method.as_str().to_ascii_lowercase(),
            200,
            response,
        )
        .await;
        assert_eq!(
            result["results"][0]["outcome"],
            if action == DeadLetterAction::Replay {
                "replayed"
            } else {
                "discarded"
            }
        );
    }
    tasks.shutdown().await;
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn discard_retains_typed_tombstone_audit_identity_fencing_dedup_and_partition_progress(
    pool: PgPool,
) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let (id, envelope, delivery) =
        stage(&store, "tenant", "retention", "PRIVATE RETAINED DATA").await;
    let original: (Uuid, i64, Vec<u8>, Vec<u8>) = sqlx::query_as(
        "SELECT message_id,generation,fingerprint,envelope FROM raddle_jobs WHERE id=$1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        store
            .discard_dead_letter("tenant", id, "operator", "retire dead delivery", Utc::now())
            .await
            .unwrap()
    );
    assert!(
        !store
            .discard_dead_letter("tenant", id, "operator", "again", Utc::now())
            .await
            .unwrap()
    );
    assert!(
        !store
            .replay(
                "tenant",
                id,
                "operator",
                "cannot replay discard",
                Utc::now()
            )
            .await
            .unwrap()
    );
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Discarded
    );
    let retained: (Uuid, i64, Vec<u8>, Vec<u8>) = sqlx::query_as(
        "SELECT message_id,generation,fingerprint,envelope FROM raddle_jobs WHERE id=$1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(retained, original);
    let (actor, reason): (String, String) =
        sqlx::query_as("SELECT actor,reason FROM raddle_discard_audit WHERE job_id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        (actor.as_str(), reason.as_str()),
        ("operator", "retire dead delivery")
    );
    let mut duplicate = store.begin().await.unwrap();
    duplicate
        .stage(
            "dlq-tests",
            "tenant",
            &envelope,
            std::slice::from_ref(&delivery),
        )
        .await
        .unwrap();
    duplicate.commit(Utc::now()).await.unwrap();
    let changed = Envelope::with_identity(
        envelope.id(),
        envelope.created_at(),
        &Failed {
            private: "different".into(),
        },
    )
    .unwrap()
    .with_header("test-metadata".into(), "canonical".into())
    .unwrap();
    let mut conflicting = store.begin().await.unwrap();
    assert!(matches!(
        conflicting
            .stage(
                "dlq-tests",
                "tenant",
                &changed,
                std::slice::from_ref(&delivery)
            )
            .await,
        Err(raddle_postgres::Error::IdentityConflict)
    ));
    drop(conflicting);
    assert_eq!(
        store
            .prune_completed("tenant", Utc::now() + chrono::Duration::days(1), 100)
            .await
            .unwrap(),
        0
    );
    assert!(
        sqlx::query("DELETE FROM raddle_jobs WHERE id=$1")
            .bind(id)
            .execute(&pool)
            .await
            .is_err(),
        "audit FK must protect the retained tombstone"
    );
    let next = Envelope::encode(&Failed {
        private: "next independent message".into(),
    })
    .unwrap();
    let mut scope = store.begin().await.unwrap();
    scope
        .stage("dlq-tests", "tenant", &next, &[delivery])
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    let claims = store
        .claim_for(
            DeliveryKind::Local,
            "retention",
            10,
            Duration::from_secs(60),
            Utc::now(),
        )
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].envelope().unwrap().id(), next.id());
    let statistics = store.statistics("tenant").await.unwrap();
    assert_eq!(statistics.completed, 0);
    assert_eq!(statistics.dead, 0);
    assert_eq!(statistics.active, 1);
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn successful_jobs_expired_replays_nil_duplicate_and_oversized_ids_are_not_resubmitted(
    pool: PgPool,
) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let (completed, _, _) = stage(&store, "tenant", "completed", "successful job").await;
    assert!(
        store
            .replay(
                "tenant",
                completed,
                "fixture",
                "prepare success",
                Utc::now()
            )
            .await
            .unwrap()
    );
    let claim = store
        .claim_for(
            DeliveryKind::Local,
            "completed",
            1,
            Duration::from_secs(60),
            Utc::now(),
        )
        .await
        .unwrap()
        .remove(0);
    store.complete(&claim, Utc::now()).await.unwrap();
    let (expired, _, _) = stage(&store, "tenant", "expired", "expired job").await;
    sqlx::query("UPDATE raddle_jobs SET expires_at=clock_timestamp()-interval '1 day' WHERE id=$1")
        .bind(expired)
        .execute(&pool)
        .await
        .unwrap();
    let api = api(store.clone(), None);
    let mut tasks = JoinSet::new();
    let base = serve(&api, &mut tasks).await;
    let client = client();
    // Cookies do not establish bearer authentication or trusted tenant scope.
    assert_eq!(
        client
            .post(format!("{base}/dead-letters/"))
            .header("Cookie", "Authorization=Bearer admin; tenant=tenant")
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    for action in [DeadLetterAction::Replay, DeadLetterAction::Discard] {
        let (id, _, _) = stage(
            &store,
            "tenant",
            &format!("valid-{action:?}").to_ascii_lowercase(),
            "valid neighbor",
        )
        .await;
        let (method, path) = if action == DeadLetterAction::Replay {
            (Method::POST, "/dead-letters/replay")
        } else {
            (Method::DELETE, "/dead-letters/")
        };
        let ids = vec![
            Uuid::nil().to_string(),
            completed.to_string(),
            "x".repeat(128),
            id.to_string(),
            id.to_string(),
        ];
        let response = client
            .request(method.clone(), format!("{base}{path}"))
            .bearer_auth("admin")
            .json(&mutation(&ids, action))
            .send()
            .await
            .unwrap();
        let body = checked(
            &api,
            path,
            &method.as_str().to_ascii_lowercase(),
            200,
            response,
        )
        .await;
        for (n, expected) in [
            (0, "invalid"),
            (1, "not_eligible"),
            (2, "invalid"),
            (4, "invalid"),
        ] {
            assert_eq!(body["results"][n]["outcome"], expected);
        }
        assert_eq!(
            body["results"][3]["outcome"],
            if action == DeadLetterAction::Replay {
                "replayed"
            } else {
                "discarded"
            }
        );
        assert_eq!(
            store
                .inspect("tenant", completed)
                .await
                .unwrap()
                .unwrap()
                .status,
            JobStatus::Completed
        );
    }
    let response = client
        .post(format!("{base}/dead-letters/replay"))
        .bearer_auth("admin")
        .json(&mutation(&[expired.to_string()], DeadLetterAction::Replay))
        .send()
        .await
        .unwrap();
    assert_eq!(
        checked(&api, "/dead-letters/replay", "post", 200, response).await["results"][0]["outcome"],
        "not_eligible"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs WHERE id=$1")
            .bind(completed)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_replay_audit WHERE job_id=$1")
            .bind(completed)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "only the fixture preparation replay is audited"
    );
    tasks.shutdown().await;
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn locked_dead_neighbor_does_not_block_discard_and_old_claim_cannot_settle_discard(
    pool: PgPool,
) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let (locked, _, _) = stage(&store, "tenant", "locked", "locked failed data").await;
    let (neighbor, _, _) = stage(&store, "tenant", "neighbor", "independent failed data").await;
    let api = api(store.clone(), None);
    let mut tasks = JoinSet::new();
    let base = serve(&api, &mut tasks).await;
    let client = client();
    let mut lock = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM raddle_jobs WHERE id=$1 FOR UPDATE")
        .bind(locked)
        .execute(&mut *lock)
        .await
        .unwrap();
    let response = client
        .delete(format!("{base}/dead-letters/"))
        .bearer_auth("admin")
        .json(&mutation(
            &[locked.to_string(), neighbor.to_string()],
            DeadLetterAction::Discard,
        ))
        .send()
        .await
        .unwrap();
    let results = checked(&api, "/dead-letters/", "delete", 200, response).await;
    assert_eq!(results["results"][0]["outcome"], "not_eligible");
    assert_eq!(results["results"][1]["outcome"], "discarded");
    lock.rollback().await.unwrap();
    let response = client
        .delete(format!("{base}/dead-letters/"))
        .bearer_auth("admin")
        .json(&mutation(&[locked.to_string()], DeadLetterAction::Discard))
        .send()
        .await
        .unwrap();
    assert_eq!(
        checked(&api, "/dead-letters/", "delete", 200, response).await["results"][0]["outcome"],
        "discarded"
    );
    // A real old claim cannot settle or regain execution after explicit discard.
    let (id, _, _) = stage(&store, "tenant", "stale-fence", "failed again").await;
    assert!(
        store
            .replay("tenant", id, "fixture", "prepare claim", Utc::now())
            .await
            .unwrap()
    );
    let claim = store
        .claim_for(
            DeliveryKind::Local,
            "stale-fence",
            1,
            Duration::from_secs(60),
            Utc::now(),
        )
        .await
        .unwrap()
        .remove(0);
    store
        .fail(
            &claim,
            FailureDisposition::Reject,
            RetryPolicy::default(),
            "fixture-rejection",
            Utc::now(),
        )
        .await
        .unwrap();
    assert!(
        store
            .discard_dead_letter("tenant", id, "operator", "retire", Utc::now())
            .await
            .unwrap()
    );
    assert!(matches!(
        store.complete(&claim, Utc::now()).await,
        Err(raddle_postgres::Error::StaleClaim)
    ));
    assert!(matches!(
        store.execution(&claim, Utc::now()).await,
        Err(raddle_postgres::Error::StaleClaim)
    ));
    assert_eq!(
        store.inspect("tenant", id).await.unwrap().unwrap().status,
        JobStatus::Discarded
    );
    tasks.shutdown().await;
}
