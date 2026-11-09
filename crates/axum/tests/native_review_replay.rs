//! Review regressions through the actual request future and PostgreSQL scope.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::Response;
use raddle::RuntimeBuilder;
use raddle_axum::native::{
    BodyMode, Endpoint, EndpointContext, EndpointContract, EndpointOutput, EndpointRegistration,
    EndpointResponse, HttpMethod, NativeApiBuilder, Problem, ReplayMode, ReplayPolicy, ReplayScope,
    RequestInput, ResponseKind,
};
use raddle_axum::{Api, ApiError, Permission, Principal};
use raddle_postgres::Store;
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::sync::Notify;
use tower::ServiceExt;

#[derive(Deserialize)]
struct Input {
    item: String,
    status: u16,
}
struct Work {
    invalid: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    pause: Option<(bool, Arc<Notify>)>,
}
impl Endpoint for Work {
    type Input = Input;
    type Resource = Value;
    type Error = Problem;
    async fn bind(&self, input: &mut RequestInput) -> Result<Input, Problem> {
        input.json().await?.ok_or_else(|| Problem::invalid("body"))
    }
    async fn handle(
        &self,
        context: &mut EndpointContext,
        input: Input,
    ) -> Result<EndpointOutput<Value>, Problem> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        sqlx::query("INSERT INTO review_work(item) VALUES($1)")
            .bind(&input.item)
            .execute(context.scope()?.connection())
            .await
            .map_err(|_| Problem::new(StatusCode::INTERNAL_SERVER_ERROR))?;
        self.pause_at(false).await;
        let response = if input.status == 201 {
            let invalid = self.invalid.load(Ordering::SeqCst);
            EndpointResponse::Created {
                resource: Some(
                    json!({"item":input.item,"value":if invalid==2 {"x".repeat(1_048_576)}else{"ok".into()}}),
                ),
                location: if invalid == 1 {
                    format!("/{}", "x".repeat(8192))
                } else {
                    "/ok".into()
                },
            }
        } else {
            EndpointResponse::Status(StatusCode::from_u16(input.status).unwrap())
        };
        let effect: raddle_axum::native::EndpointEffect = Box::new(move |context| {
            Box::pin(async move {
                sqlx::query("INSERT INTO review_effects(item) VALUES($1)")
                    .bind(input.item)
                    .execute(context.scope()?.connection())
                    .await
                    .map_err(|_| Problem::new(StatusCode::INTERNAL_SERVER_ERROR))?;
                Ok(())
            })
        });
        Ok((response, vec![effect]).into())
    }
    fn map_error(&self, error: Problem) -> Problem {
        error
    }
    async fn after_commit(&self, _context: &mut EndpointContext) -> Result<(), Problem> {
        self.pause_at(true).await;
        Ok(())
    }
}
impl Work {
    async fn pause_at(&self, postcommit: bool) {
        if let Some((stage, entered)) = &self.pause
            && *stage == postcommit
        {
            entered.notify_one();
            std::future::pending::<()>().await;
        }
    }
}
fn app(pool: &PgPool, mode: ReplayMode, required: bool, work: Work) -> Api {
    let mut runtime = RuntimeBuilder::default();
    let mut contract = EndpointContract::new(HttpMethod::Post, "/work", "review_work");
    contract.body = BodyMode::Json {
        optional: false,
        max_bytes: 4096,
    };
    contract.responses = vec![ResponseKind::Created, ResponseKind::Status];
    contract.replay = Some(ReplayPolicy {
        store: Store::new(pool.clone()),
        mode,
        required,
        ttl: Duration::from_secs(60),
        scope: ReplayScope {
            tenant: false,
            user: false,
            endpoint: true,
        },
    });
    let registration = EndpointRegistration::prepare(&mut runtime, contract, work).unwrap();
    let mut builder = NativeApiBuilder::new(
        runtime.build(),
        |_: &HeaderMap| -> Result<Principal, ApiError> {
            Principal::try_new("tenant", "subject", [Permission::Invoke])
        },
    );
    builder.endpoint(registration).unwrap();
    builder.build()
}
fn work(invalid: &Arc<AtomicUsize>, calls: &Arc<AtomicUsize>) -> Work {
    Work {
        invalid: Arc::clone(invalid),
        calls: Arc::clone(calls),
        pause: None,
    }
}
fn request(key: Option<&str>, item: &str, status: u16) -> Request<Body> {
    let mut request = Request::builder()
        .method("POST")
        .uri("/work")
        .header("content-type", "application/json");
    if let Some(key) = key {
        request = request.header("idempotency-key", key);
    }
    request
        .body(Body::from(json!({"item":item,"status":status}).to_string()))
        .unwrap()
}
async fn send(api: &Api, key: Option<&str>, item: &str, status: u16) -> Response {
    api.router()
        .oneshot(request(key, item, status))
        .await
        .unwrap()
}
async fn setup(pool: &PgPool) {
    Store::new(pool.clone()).migrate().await.unwrap();
    sqlx::query("CREATE TABLE review_work(item text NOT NULL)")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE review_effects(item text NOT NULL)")
        .execute(pool)
        .await
        .unwrap();
}
async fn count(pool: &PgPool, item: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM review_work WHERE item=$1")
        .bind(item)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn effect_count(pool: &PgPool, item: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM review_effects WHERE item=$1")
        .bind(item)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[sqlx::test(migrations = false)]
async fn optional_key_redirects_commit_but_400_plus_refusals_rollback(pool: PgPool) {
    setup(&pool).await;
    for mode in [ReplayMode::SeparateClaim, ReplayMode::SameTransaction] {
        let invalid = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let api = app(&pool, mode, false, work(&invalid, &calls));
        for keyed in [false, true] {
            for status in [303, 400, 503] {
                let item = format!("{mode:?}-{keyed}-{status}");
                let key = keyed.then_some(item.as_str());
                assert_eq!(
                    send(&api, key, &item, status).await.status().as_u16(),
                    status
                );
                assert_eq!(count(&pool, &item).await, i64::from(status < 400));
                assert_eq!(effect_count(&pool, &item).await, i64::from(status < 400));
                if keyed {
                    let before = calls.load(Ordering::SeqCst);
                    assert_eq!(
                        send(&api, key, &item, status).await.status().as_u16(),
                        status
                    );
                    assert_eq!(
                        calls.load(Ordering::SeqCst) - before,
                        usize::from(status >= 400)
                    );
                }
            }
        }
    }
}

#[sqlx::test(migrations = false)]
async fn all_stored_response_bounds_are_checked_before_business_commit_and_allow_corrected_retry(
    pool: PgPool,
) {
    setup(&pool).await;
    for mode in [ReplayMode::SeparateClaim, ReplayMode::SameTransaction] {
        for invalid_value in [1, 2] {
            let invalid = Arc::new(AtomicUsize::new(invalid_value));
            let calls = Arc::new(AtomicUsize::new(0));
            let api = app(&pool, mode, true, work(&invalid, &calls));
            let key = format!("{mode:?}-{invalid_value}");
            assert!(
                send(&api, Some(&key), &key, 201)
                    .await
                    .status()
                    .is_server_error()
            );
            assert_eq!(
                count(&pool, &key).await,
                0,
                "oversized Location/body must roll back work"
            );
            assert_eq!(effect_count(&pool, &key).await, 0);
            invalid.store(0, Ordering::SeqCst);
            let response = send(&api, Some(&key), &key, 201).await;
            assert_eq!(response.status(), 201);
            assert_eq!(response.headers()["location"], "/ok");
            assert_eq!(count(&pool, &key).await, 1);
            assert_eq!(effect_count(&pool, &key).await, 1);
            assert_eq!(send(&api, Some(&key), &key, 201).await.status(), 201);
            assert_eq!(
                calls.load(Ordering::SeqCst),
                2,
                "corrected retry runs once, then replays"
            );
        }
    }
}

#[sqlx::test(migrations = false)]
async fn aborting_the_actual_request_future_preserves_pre_and_postcommit_replay_boundaries(
    pool: PgPool,
) {
    setup(&pool).await;
    for mode in [ReplayMode::SeparateClaim, ReplayMode::SameTransaction] {
        for postcommit in [false, true] {
            let key = format!("cancel-{mode:?}-{postcommit}");
            let invalid = Arc::new(AtomicUsize::new(0));
            let calls = Arc::new(AtomicUsize::new(0));
            let entered = Arc::new(Notify::new());
            let mut paused = work(&invalid, &calls);
            paused.pause = Some((postcommit, Arc::clone(&entered)));
            let api = app(&pool, mode, true, paused);
            let router = api.router();
            let pending = tokio::spawn(router.oneshot(request(Some(&key), &key, 201)));
            tokio::time::timeout(Duration::from_secs(10), entered.notified())
                .await
                .unwrap();
            pending.abort();
            assert!(pending.await.unwrap_err().is_cancelled());
            // Check every pooled connection back out to flush SQLx's queued
            // rollback, without sleeps or assuming which connection was used.
            let mut connections = Vec::new();
            for _ in 0..pool.options().get_max_connections() {
                let mut connection = pool.acquire().await.unwrap();
                sqlx::query("SELECT 1")
                    .execute(&mut *connection)
                    .await
                    .unwrap();
                connections.push(connection);
            }
            drop(connections);
            assert_eq!(count(&pool, &key).await, i64::from(postcommit));
            assert_eq!(effect_count(&pool, &key).await, i64::from(postcommit));
            let retry = app(&pool, mode, true, work(&invalid, &calls));
            let response = send(&retry, Some(&key), &key, 201).await;
            assert_eq!(
                response.status(),
                if mode == ReplayMode::SeparateClaim {
                    409
                } else {
                    201
                }
            );
            assert_eq!(
                calls.load(Ordering::SeqCst),
                if !postcommit && mode == ReplayMode::SameTransaction {
                    2
                } else {
                    1
                }
            );
            assert_eq!(
                count(&pool, &key).await,
                if mode == ReplayMode::SameTransaction {
                    1
                } else {
                    i64::from(postcommit)
                }
            );
        }
    }
}
