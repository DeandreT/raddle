//! Authenticated HTTP contracts exercised against committed PostgreSQL state.
#![allow(clippy::unwrap_used)]

use std::future::{self, Future};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use axum::http::HeaderMap;
use chrono::Utc;
use raddle::wire::{Envelope, WireMessage};
use raddle::{Request, RuntimeBuilder};
use raddle_axum::{Api, ApiBuilder, ApiError, Authenticated, Permission, Principal};
use raddle_postgres::{Delivery, DeliveryKind, Store};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::sync::Notify;
use tokio::task::JoinSet;

#[derive(Deserialize, Serialize, JsonSchema)]
struct Write {
    id: i32,
    parent: i32,
}

#[derive(Serialize, JsonSchema)]
struct Written {
    id: i32,
}

impl Request for Write {
    type Reply = Written;
    type Error = raddle_postgres::Error;
}

impl WireMessage for Write {
    const NAME: &'static str = "test.http.write";
    const VERSION: u32 = 1;
}

fn authenticate(headers: &HeaderMap) -> Result<Principal, ApiError> {
    let token = headers
        .get("Authorization")
        .and_then(|value| value.to_str().ok());
    let tenant = match token {
        Some("Bearer allowed") => "tenant",
        Some("Bearer other") => "other",
        _ => return Err(ApiError::unauthorized()),
    };
    Principal::try_new(
        tenant,
        "operator",
        [Permission::Invoke, Permission::Inspect, Permission::Replay],
    )
}

fn validate_response(api: &Api, path: &str, method: &str, status: u16, body: &Value) {
    let mut schema = api.specification()["paths"][path][method]["responses"][status.to_string()]
        ["content"]["application/json"]["schema"]
        .clone();
    assert!(
        schema.is_object(),
        "missing response contract for {method} {path}"
    );
    schema["components"] = api.specification()["components"].clone();
    assert!(jsonschema::validator_for(&schema).unwrap().is_valid(body));
}

async fn serve(api: &Api, tasks: &mut JoinSet<()>) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = api.router();
    tasks.spawn(async move { axum::serve(listener, router).await.unwrap() });
    address
}

#[sqlx::test(migrations = "../raddle-postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn synchronous_http_reply_follows_commit_and_failed_commit_is_not_success(pool: PgPool) {
    sqlx::query("CREATE TABLE http_parents(id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO http_parents VALUES(1)")
        .execute(&pool)
        .await
        .unwrap();
    // This fails at COMMIT, not during the handler's business write or staging.
    sqlx::query("CREATE TABLE http_effects(id INTEGER PRIMARY KEY,parent INTEGER REFERENCES http_parents(id) DEFERRABLE INITIALLY DEFERRED)")
        .execute(&pool).await.unwrap();
    let staged = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let commit_attempts = Arc::new(AtomicUsize::new(0));
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle({
            let pool = pool.clone();
            let staged = Arc::clone(&staged);
            let release = Arc::clone(&release);
            let attempts = Arc::clone(&commit_attempts);
            move |request: Authenticated<Write>| {
                let pool = pool.clone();
                let staged = Arc::clone(&staged);
                let release = Arc::clone(&release);
                let attempts = Arc::clone(&attempts);
                async move {
                    let (principal, command) = request.into_parts();
                    let mut scope = Store::new(pool).begin().await?;
                    sqlx::query("INSERT INTO http_effects(id,parent) VALUES($1,$2)")
                        .bind(command.id)
                        .bind(command.parent)
                        .execute(scope.connection())
                        .await?;
                    scope
                        .stage(
                            "http-sync",
                            principal.tenant(),
                            &Envelope::encode(&command)?,
                            &[Delivery::new(DeliveryKind::Local, "follow-up", Utc::now())],
                        )
                        .await?;
                    if command.id == 1 {
                        staged.notify_one();
                        release.notified().await;
                    }
                    attempts.fetch_add(1, Ordering::SeqCst);
                    scope.commit(Utc::now()).await?;
                    Ok(Written { id: command.id })
                }
            }
        })
        .unwrap();
    let mut builder = ApiBuilder::new(runtime.build(), authenticate);
    builder.request::<Write>("/write").unwrap();
    let api = builder.build();
    let mut tasks = JoinSet::new();
    let address = serve(&api, &mut tasks).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let pending = client
        .post(format!("http://{address}/write"))
        .bearer_auth("allowed")
        .json(&json!({"id":1,"parent":1}))
        .send();
    tokio::pin!(pending);
    tokio::select! {
        response = &mut pending => panic!("HTTP completed before the commit gate: {response:?}"),
        () = staged.notified() => {},
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM http_effects")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    future::poll_fn(|context| {
        assert!(
            pending.as_mut().poll(context).is_pending(),
            "response escaped an uncommitted handler"
        );
        Poll::Ready(())
    })
    .await;
    release.notify_one();
    let response = pending.await.unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body, json!({"id":1}));
    validate_response(&api, "/write", "post", 200, &body);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM http_effects WHERE id=1")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );

    let response = client
        .post(format!("http://{address}/write"))
        .bearer_auth("allowed")
        .json(&json!({"id":2,"parent":999}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 422);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(
        body,
        json!({"code":"rejected","message":"command rejected"})
    );
    validate_response(&api, "/write", "post", 422, &body);
    assert_eq!(
        commit_attempts.load(Ordering::SeqCst),
        2,
        "both handlers reached commit"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM http_effects WHERE id=2")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    tasks.shutdown().await;
}

#[sqlx::test(migrations = "../raddle-postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn authenticated_operations_and_acceptance_match_every_advertised_route(pool: PgPool) {
    let store = Store::new(pool.clone());
    let mut builder = ApiBuilder::new(RuntimeBuilder::default().build(), authenticate);
    builder
        .durable::<Write>("/commands", store.clone(), "commands")
        .unwrap();
    builder.operations("/ops", store).unwrap();
    let api = builder.build();
    let mut tasks = JoinSet::new();
    let address = serve(&api, &mut tasks).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let response = client
        .post(format!("http://{address}/commands"))
        .bearer_auth("allowed")
        .header("Idempotency-Key", "first")
        .json(&json!({"id":1,"parent":1}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 202);
    let accepted = response.json::<Value>().await.unwrap();
    validate_response(&api, "/commands", "post", 202, &accepted);
    let (job, message): (uuid::Uuid, uuid::Uuid) =
        sqlx::query_as("SELECT id,message_id FROM raddle_jobs WHERE tenant='tenant'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(accepted["message_id"], message.to_string());
    sqlx::query("UPDATE raddle_jobs SET status='dead' WHERE id=$1")
        .bind(job)
        .execute(&pool)
        .await
        .unwrap();

    for (path, operations) in api.specification()["paths"].as_object().unwrap() {
        for method in operations.as_object().unwrap().keys() {
            let url = format!("http://{address}{}", path.replace("{id}", &job.to_string()));
            let mut request = client
                .request(method.to_uppercase().parse().unwrap(), url)
                .bearer_auth("allowed");
            if path == "/commands" {
                request = request
                    .header("Idempotency-Key", "first")
                    .json(&json!({"id":1,"parent":1}));
            } else if path.ends_with("/replay") {
                request = request.json(&json!({"reason":"operator-approved retry"}));
            }
            let response = request.send().await.unwrap();
            let expected = if path == "/commands" { 202 } else { 200 };
            assert_eq!(response.status().as_u16(), expected, "{method} {path}");
            if path.ends_with("/metrics") {
                assert_eq!(response.headers()["Content-Type"], "text/plain");
                assert!(
                    response
                        .text()
                        .await
                        .unwrap()
                        .contains("raddle_http_requests_total")
                );
            } else {
                let body = response.json::<Value>().await.unwrap();
                validate_response(&api, path, method, expected, &body);
                if path == "/openapi.json" {
                    assert_eq!(body, *api.specification());
                }
            }
        }
    }
    let (status, actor, reason): (String, String, String) = sqlx::query_as("SELECT j.status,a.actor,a.reason FROM raddle_jobs j JOIN raddle_replay_audit a ON a.job_id=j.id WHERE j.id=$1")
        .bind(job).fetch_one(&pool).await.unwrap();
    assert_eq!(
        (status.as_str(), actor.as_str(), reason.as_str()),
        ("ready", "operator", "operator-approved retry")
    );
    let response = client
        .get(format!("http://{address}/ops/jobs/{job}"))
        .bearer_auth("other")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 404);
    // A ready job would refuse replay for either tenant, masking a missing
    // tenant predicate. Keep this job eligible while checking isolation.
    sqlx::query("UPDATE raddle_jobs SET status='dead' WHERE id=$1")
        .bind(job)
        .execute(&pool)
        .await
        .unwrap();
    let response = client
        .post(format!("http://{address}/ops/jobs/{job}/replay"))
        .bearer_auth("other")
        .json(&json!({"reason":"cannot cross tenant"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 409);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_replay_audit")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM raddle_jobs WHERE id=$1")
            .bind(job)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "dead"
    );
    let response = client
        .post(format!("http://{address}/ops/jobs/{job}/replay"))
        .bearer_auth("allowed")
        .json(&json!({"reason":"owner retry"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status().as_u16(),
        200,
        "the same job must remain replayable by its owner"
    );
    let body = response.json::<Value>().await.unwrap();
    validate_response(&api, "/ops/jobs/{id}/replay", "post", 200, &body);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_replay_audit")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    Store::new(pool.clone())
        .set_capacity("tenant", 1)
        .await
        .unwrap();
    let response = client
        .post(format!("http://{address}/commands"))
        .bearer_auth("allowed")
        .header("Idempotency-Key", "refused")
        .json(&json!({"id":2,"parent":1}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 503);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(
        body,
        json!({"code":"unavailable","message":"service unavailable"})
    );
    validate_response(&api, "/commands", "post", 503, &body);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_idempotency WHERE key='refused'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    tasks.shutdown().await;
}
