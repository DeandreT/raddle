//! Real PostgreSQL commits/concurrent duplicate handling and owned-process crash windows.
#![allow(clippy::unwrap_used)]

use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::http::{HeaderMap, StatusCode};
use raddle::RuntimeBuilder;
use raddle_axum::native::{
    BodyMode, Endpoint, EndpointContext, EndpointContract, EndpointEffect, EndpointOutput,
    EndpointRegistration, EndpointResponse, HttpMethod, NativeApiBuilder, Problem, ReplayMode,
    ReplayPolicy, ReplayScope, RequestInput, ResponseKind,
};
use raddle_axum::{Api, ApiError, Permission, Principal};
use raddle_postgres::Store;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Notify;

fn auth(headers: &HeaderMap) -> Result<Principal, ApiError> {
    match headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
    {
        Some("Bearer allowed") => {
            Principal::try_new("verified-tenant", "alice", [Permission::Invoke])
        }
        Some("Bearer bob") => Principal::try_new("verified-tenant", "bob", [Permission::Invoke]),
        Some("Bearer other") => Principal::try_new("other-tenant", "alice", [Permission::Invoke]),
        _ => Err(ApiError::unauthorized()),
    }
}

#[derive(Deserialize, Serialize)]
struct Command {
    item: String,
    #[serde(default)]
    fail: bool,
    #[serde(default)]
    rollback: bool,
}
struct Write<const N: usize> {
    calls: Arc<AtomicUsize>,
    entered: Option<Arc<Notify>>,
    release: Option<Arc<Notify>>,
    crash: Option<String>,
}
impl<const N: usize> Write<N> {
    fn new(calls: Arc<AtomicUsize>) -> Self {
        Self {
            calls,
            entered: None,
            release: None,
            crash: None,
        }
    }
}
impl<const N: usize> Endpoint for Write<N> {
    type Input = Command;
    type Resource = Value;
    type Error = Problem;
    async fn bind(&self, input: &mut RequestInput) -> Result<Command, Problem> {
        input.json().await?.ok_or_else(|| Problem::invalid("body"))
    }
    async fn handle(
        &self,
        context: &mut EndpointContext,
        input: Command,
    ) -> Result<EndpointOutput<Value>, Problem> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        context
            .response_headers
            .insert("x-first-only", "not-replayed".parse().unwrap());
        let tenant = context.principal().unwrap().tenant().to_owned();
        let subject = context.principal().unwrap().subject().to_owned();
        let transaction: i64 = sqlx::query_scalar("SELECT txid_current()::bigint")
            .fetch_one(context.scope()?.connection())
            .await
            .map_err(database_error)?;
        sqlx::query("INSERT INTO http_business(tenant,subject,item) VALUES($1,$2,$3)")
            .bind(&tenant)
            .bind(&subject)
            .bind(&input.item)
            .execute(context.scope()?.connection())
            .await
            .map_err(database_error)?;
        if self.crash.as_deref() == Some("precommit") {
            println!("NATIVE_HTTP_STAGE=precommit");
            std::future::pending::<()>().await;
        }
        if let Some(entered) = &self.entered {
            entered.notify_one();
        }
        if let Some(release) = &self.release {
            release.notified().await;
        }
        if input.fail {
            return Err(Problem::invalid("item"));
        }
        if input.rollback {
            context.scope()?.mark_rollback_only();
        }
        let effect: EndpointEffect = Box::new(move |context| {
            Box::pin(async move {
                sqlx::query(
                    "INSERT INTO http_effects(transaction_id) VALUES(txid_current()::bigint)",
                )
                .execute(context.scope()?.connection())
                .await
                .map_err(database_error)?;
                Ok(())
            })
        });
        Ok((EndpointResponse::Created {resource:Some(json!({"item":input.item,"tenant":tenant,"subject":subject,"transaction":transaction})),location:"/committed/items".into()},vec![effect]).into())
    }
    fn map_error(&self, error: Problem) -> Problem {
        error
    }
    async fn after_commit(&self, _context: &mut EndpointContext) -> Result<(), Problem> {
        if self.crash.as_deref() == Some("postcommit") {
            println!("NATIVE_HTTP_STAGE=postcommit");
            std::future::pending::<()>().await;
        }
        Ok(())
    }
    fn body_schema(&self) -> Value {
        json!({"type":"object","required":["item"],"properties":{"item":{"type":"string"},"fail":{"type":"boolean","default":false},"rollback":{"type":"boolean","default":false}}})
    }
}

fn database_error(_: sqlx::Error) -> Problem {
    Problem::new(StatusCode::INTERNAL_SERVER_ERROR)
}

fn registration<const N: usize>(
    runtime: &mut RuntimeBuilder,
    store: Store,
    mode: ReplayMode,
    path: &str,
    endpoint: Write<N>,
) -> EndpointRegistration<Write<N>> {
    let mut contract = EndpointContract::new(HttpMethod::Post, path, &format!("write{N}"));
    contract.body = BodyMode::Json {
        optional: false,
        max_bytes: 4096,
    };
    contract.responses = vec![ResponseKind::Created];
    contract.replay = Some(ReplayPolicy {
        store,
        scope: ReplayScope {
            tenant: true,
            user: true,
            endpoint: true,
        },
        mode,
        required: true,
        ttl: Duration::from_secs(60),
    });
    EndpointRegistration::prepare(runtime, contract, endpoint).unwrap()
}

fn api<const N: usize>(store: Store, mode: ReplayMode, endpoint: Write<N>) -> Api {
    let mut runtime = RuntimeBuilder::default();
    let registration = registration(&mut runtime, store, mode, "/write", endpoint);
    let mut builder = NativeApiBuilder::new(runtime.build(), auth);
    builder.endpoint(registration).unwrap();
    builder.build()
}

async fn serve(api: &Api) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = api.router();
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (url, task)
}

async fn setup(pool: &PgPool) {
    Store::new(pool.clone()).migrate().await.unwrap();
    sqlx::query(
        "CREATE TABLE http_business(tenant text NOT NULL,subject text NOT NULL,item text NOT NULL)",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("CREATE TABLE http_effects(transaction_id bigint NOT NULL)")
        .execute(pool)
        .await
        .unwrap();
}
async fn send(
    client: &reqwest::Client,
    url: &str,
    key: &str,
    body: &str,
    identity: &str,
) -> reqwest::Response {
    client
        .post(format!("{url}/write"))
        .bearer_auth(identity)
        .header("idempotency-key", key)
        .header("content-type", "application/json")
        .body(body.to_owned())
        .send()
        .await
        .unwrap()
}

#[sqlx::test(migrations = false)]
async fn persisted_status_body_location_replays_after_restart_without_other_headers(pool: PgPool) {
    setup(&pool).await;
    for mode in [ReplayMode::SeparateClaim, ReplayMode::SameTransaction] {
        let calls = Arc::new(AtomicUsize::new(0));
        let store = Store::new(pool.clone());
        let api = api(store.clone(), mode, Write::<0>::new(Arc::clone(&calls)));
        let (url, task) = serve(&api).await;
        let client = reqwest::Client::new();
        let key = format!("{mode:?}");
        let body = r#"{"item":"one","tenant":"untrusted-tenant","subject":"admin"}"#;
        assert_eq!(
            client
                .post(format!("{url}/write"))
                .bearer_auth("allowed")
                .json(&json!({"item":"one"}))
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        let first = send(&client, &url, &key, body, "allowed").await;
        assert_eq!(first.status(), 201);
        assert_eq!(first.headers()["location"], "/committed/items");
        assert_eq!(first.headers()["x-first-only"], "not-replayed");
        let first_body = first.bytes().await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&first_body).unwrap()["tenant"],
            "verified-tenant"
        );
        let work_transaction = serde_json::from_slice::<Value>(&first_body).unwrap()["transaction"]
            .as_i64()
            .unwrap();
        let response_transaction: i64 =
            sqlx::query_scalar("SELECT xmin::text::bigint FROM raddle_http_responses WHERE key=$1")
                .bind(&key)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            work_transaction == response_transaction,
            mode == ReplayMode::SameTransaction,
            "same-scope recording must use the actual business transaction"
        );
        let repeated = send(&client, &url, &key, body, "allowed").await;
        assert_eq!(repeated.status(), 201);
        assert_eq!(repeated.headers()["location"], "/committed/items");
        assert!(!repeated.headers().contains_key("x-first-only"));
        assert_eq!(repeated.bytes().await.unwrap(), first_body);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            send(&client, &url, &key, &format!(" {body}"), "allowed")
                .await
                .status(),
            422,
            "raw whitespace changes fingerprint"
        );
        task.abort();
        let _ = task.await;
        let restarted = api_builder_after_restart(store, mode, Arc::clone(&calls));
        let (url, task) = serve(&restarted).await;
        assert_eq!(
            send(&client, &url, &key, body, "allowed")
                .await
                .bytes()
                .await
                .unwrap(),
            first_body
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(send(&client, &url, &key, body, "bob").await.status(), 201);
        assert_eq!(send(&client, &url, &key, body, "other").await.status(), 201);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let transaction: i64 = serde_json::from_slice::<Value>(&first_body).unwrap()["transaction"]
            .as_i64()
            .unwrap();
        let effects: i64 =
            sqlx::query_scalar("SELECT count(*) FROM http_effects WHERE transaction_id=$1")
                .bind(transaction)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(effects, 1, "secondary effect shares exact transaction");
        task.abort();
        let _ = task.await;
    }
}

fn api_builder_after_restart(store: Store, mode: ReplayMode, calls: Arc<AtomicUsize>) -> Api {
    api(store, mode, Write::<0>::new(calls))
}

#[sqlx::test(migrations = false)]
async fn concurrent_duplicates_failures_and_corrected_retry_only_repeat_failed_work(pool: PgPool) {
    setup(&pool).await;
    let client = reqwest::Client::new();
    for mode in [ReplayMode::SeparateClaim, ReplayMode::SameTransaction] {
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut endpoint = Write::<1>::new(Arc::clone(&calls));
        endpoint.entered = Some(Arc::clone(&entered));
        endpoint.release = Some(Arc::clone(&release));
        let app = api(Store::new(pool.clone()), mode, endpoint);
        let (url, task) = serve(&app).await;
        let key = format!("concurrent-{mode:?}");
        let first_key = key.clone();
        let client_clone = client.clone();
        let url_clone = url.clone();
        let first = tokio::spawn(async move {
            send(
                &client_clone,
                &url_clone,
                &first_key,
                r#"{"item":"concurrent"}"#,
                "allowed",
            )
            .await
        });
        entered.notified().await;
        assert_eq!(
            send(&client, &url, &key, r#"{"item":"concurrent"}"#, "allowed")
                .await
                .status(),
            409
        );
        release.notify_one();
        assert_eq!(first.await.unwrap().status(), 201);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            send(&client, &url, &key, r#"{"item":"different"}"#, "allowed")
                .await
                .status(),
            422
        );
        task.abort();
        let _ = task.await;
        // A fresh endpoint type/name and keys isolate each poison position.
        let counts = Arc::new(AtomicUsize::new(0));
        let api = api(
            Store::new(pool.clone()),
            mode,
            Write::<2>::new(Arc::clone(&counts)),
        );
        let (url, task) = serve(&api).await;
        for failing in 0..3 {
            for index in 0..3 {
                let key = format!("{mode:?}-{failing}-{index}");
                let body = json!({"item":key,"fail":index==failing}).to_string();
                let response = send(&client, &url, &key, &body, "allowed").await;
                assert_eq!(response.status(), if index == failing { 400 } else { 201 });
            }
            let before = counts.load(Ordering::SeqCst);
            for index in 0..3 {
                let key = format!("{mode:?}-{failing}-{index}");
                let body = json!({"item":key,"fail":false}).to_string();
                assert_eq!(
                    send(&client, &url, &key, &body, "allowed").await.status(),
                    201
                );
            }
            assert_eq!(
                counts.load(Ordering::SeqCst),
                before + 1,
                "successful neighbors must never reexecute"
            );
        }
        assert_eq!(counts.load(Ordering::SeqCst), 12);
        task.abort();
        let _ = task.await;
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM http_business")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 20);
}

#[sqlx::test(migrations = false)]
async fn owned_process_crashes_before_and_after_commit_observe_explicit_boundaries(pool: PgPool) {
    setup(&pool).await;
    for (mode, stage, expected_status, expected_writes) in [
        (ReplayMode::SeparateClaim, "precommit", 409, 0),
        (ReplayMode::SeparateClaim, "postcommit", 409, 1),
        (ReplayMode::SameTransaction, "precommit", 201, 1),
        (ReplayMode::SameTransaction, "postcommit", 201, 1),
    ] {
        let key = format!("{mode:?}-{stage}");
        let body = json!({"item":key}).to_string();
        let mut url = url::Url::parse(&std::env::var("DATABASE_URL").unwrap()).unwrap();
        url.set_path(pool.connect_options().get_database().unwrap());
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "replay_crash_child", "--nocapture"])
            .env("DATABASE_URL", url.as_str())
            .env("RADDLE_HTTP_CRASH_CHILD", stage)
            .env(
                "RADDLE_HTTP_CRASH_MODE",
                if mode == ReplayMode::SeparateClaim {
                    "separate"
                } else {
                    "same"
                },
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let child_url = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let line = lines.next_line().await.unwrap().unwrap();
                if let Some(url) = line.strip_prefix("NATIVE_HTTP_ADDR=") {
                    break url.to_owned();
                }
            }
        })
        .await
        .unwrap();
        let key_clone = key.clone();
        let body_clone = body.clone();
        let request = tokio::spawn(async move {
            send(
                &reqwest::Client::new(),
                &child_url,
                &key_clone,
                &body_clone,
                "allowed",
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let line = lines.next_line().await.unwrap().unwrap();
                if line == format!("NATIVE_HTTP_STAGE={stage}") {
                    break;
                }
            }
        })
        .await
        .unwrap();
        child.kill().await.unwrap();
        child.wait().await.unwrap();
        request.abort();
        let _ = request.await;
        let calls = Arc::new(AtomicUsize::new(0));
        let app = api(
            Store::new(pool.clone()),
            mode,
            Write::<0>::new(Arc::clone(&calls)),
        );
        let (url, task) = serve(&app).await;
        assert_eq!(
            send(&reqwest::Client::new(), &url, &key, &body, "allowed")
                .await
                .status(),
            expected_status
        );
        let writes: i64 = sqlx::query_scalar("SELECT count(*) FROM http_business WHERE item=$1")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(writes, expected_writes);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            usize::from(mode == ReplayMode::SameTransaction && stage == "precommit")
        );
        task.abort();
        let _ = task.await;
    }
}

#[test]
fn replay_crash_child() {
    let Ok(stage) = std::env::var("RADDLE_HTTP_CRASH_CHILD") else {
        return;
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let pool = PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
                .await
                .unwrap();
            let mode = if std::env::var("RADDLE_HTTP_CRASH_MODE").unwrap() == "same" {
                ReplayMode::SameTransaction
            } else {
                ReplayMode::SeparateClaim
            };
            let mut endpoint = Write::<0>::new(Arc::new(AtomicUsize::new(0)));
            endpoint.crash = Some(stage);
            let app = api(Store::new(pool), mode, endpoint);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            println!("NATIVE_HTTP_ADDR=http://{}", listener.local_addr().unwrap());
            axum::serve(listener, app.router()).await.unwrap();
        });
}

#[sqlx::test(migrations = false)]
async fn confirmed_rollback_only_commit_failure_releases_key_without_repeating_neighbors(
    pool: PgPool,
) {
    setup(&pool).await;
    let client = reqwest::Client::new();
    for mode in [ReplayMode::SeparateClaim, ReplayMode::SameTransaction] {
        let counts = Arc::new(AtomicUsize::new(0));
        let app = api(
            Store::new(pool.clone()),
            mode,
            Write::<4>::new(Arc::clone(&counts)),
        );
        let (url, task) = serve(&app).await;
        let key = format!("rollback-only-{mode:?}");
        assert_eq!(
            send(
                &client,
                &url,
                &key,
                &json!({"item":key,"rollback":true}).to_string(),
                "allowed"
            )
            .await
            .status(),
            503
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM http_business WHERE item=$1")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
        assert_eq!(
            send(
                &client,
                &url,
                &key,
                &json!({"item":key,"rollback":false}).to_string(),
                "allowed"
            )
            .await
            .status(),
            201
        );
        assert_eq!(counts.load(Ordering::SeqCst), 2);
        task.abort();
        let _ = task.await;
    }
}

#[sqlx::test(migrations = false)]
async fn expired_rows_require_explicit_cleanup_and_optional_keys_still_commit_business_work(
    pool: PgPool,
) {
    use chrono::Utc;
    use raddle_postgres::http_responses::{self, ClaimResult, ResponseKey, StoredResponse};
    setup(&pool).await;
    let store = Store::new(pool.clone());
    assert!(ResponseKey::new(None, None, None, "key").is_err());
    let key = ResponseKey::new(
        Some("verified-tenant"),
        Some("alice"),
        Some("expiry"),
        "private-key",
    )
    .unwrap();
    assert!(!format!("{key:?}").contains("private-key"));
    let mut scope = store.begin().await.unwrap();
    let claim = match http_responses::claim(
        &mut scope,
        &key,
        &[1u8; 32],
        Utc::now() - chrono::Duration::hours(1),
    )
    .await
    .unwrap()
    {
        ClaimResult::Claimed(claim) => claim,
        _ => panic!("expected fresh claim"),
    };
    let response = StoredResponse {
        status: 202,
        body: Vec::new(),
        location: Some("/jobs/1".into()),
    };
    http_responses::complete(&mut scope, &claim, &response)
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    assert!(
        !store.release_http_response(&claim).await.unwrap(),
        "completed rows cannot be compensated"
    );
    let mut scope = store.begin().await.unwrap();
    assert!(matches!(
        http_responses::claim(&mut scope, &key, &[1u8; 32], Utc::now())
            .await
            .unwrap(),
        ClaimResult::Replay(_)
    ));
    drop(scope);
    assert_eq!(store.prune_http_responses(Utc::now(), 1).await.unwrap(), 1);
    let mut scope = store.begin().await.unwrap();
    assert!(matches!(
        http_responses::claim(&mut scope, &key, &[2u8; 32], Utc::now())
            .await
            .unwrap(),
        ClaimResult::Claimed(_)
    ));
    drop(scope);
    let counts = Arc::new(AtomicUsize::new(0));
    let mut runtime = RuntimeBuilder::default();
    let mut contract = EndpointContract::new(HttpMethod::Post, "/optional", "optional");
    contract.body = BodyMode::Json {
        optional: false,
        max_bytes: 4096,
    };
    contract.responses = vec![ResponseKind::Created];
    contract.replay = Some(ReplayPolicy {
        store,
        scope: ReplayScope {
            tenant: true,
            user: true,
            endpoint: true,
        },
        mode: ReplayMode::SameTransaction,
        required: false,
        ttl: Duration::from_secs(60),
    });
    let reg =
        EndpointRegistration::prepare(&mut runtime, contract, Write::<3>::new(Arc::clone(&counts)))
            .unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), auth);
    builder.endpoint(reg).unwrap();
    let app = builder.build();
    let (url, task) = serve(&app).await;
    let client = reqwest::Client::new();
    for _ in 0..2 {
        assert_eq!(
            client
                .post(format!("{url}/optional"))
                .bearer_auth("allowed")
                .json(&json!({"item":"no-key"}))
                .send()
                .await
                .unwrap()
                .status(),
            201
        );
    }
    assert_eq!(counts.load(Ordering::SeqCst), 2);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM http_business")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
    task.abort();
    let _ = task.await;
}

#[sqlx::test(migrations = false)]
async fn response_fences_immutability_unicode_null_fields_and_batched_cleanup_preserve_live_claims(
    pool: PgPool,
) {
    use chrono::Utc;
    use raddle_postgres::http_responses::{self, ClaimResult, ResponseKey, StoredResponse};
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let expired = Utc::now() - chrono::Duration::hours(1);
    let future = Utc::now() + chrono::Duration::hours(1);
    let key = ResponseKey::new(Some("tenant"), None, Some("corpus"), "reused").unwrap();
    let mut scope = store.begin().await.unwrap();
    let stale = match http_responses::claim(&mut scope, &key, &[1; 32], expired)
        .await
        .unwrap()
    {
        ClaimResult::Claimed(claim) => claim,
        _ => panic!("expected claim"),
    };
    scope.commit(Utc::now()).await.unwrap();
    for index in 0..2 {
        let key = ResponseKey::new(
            Some("tenant"),
            None,
            Some("corpus"),
            &format!("expired-{index}"),
        )
        .unwrap();
        let mut scope = store.begin().await.unwrap();
        assert!(matches!(
            http_responses::claim(&mut scope, &key, &[1; 32], expired)
                .await
                .unwrap(),
            ClaimResult::Claimed(_)
        ));
        scope.commit(Utc::now()).await.unwrap();
    }
    let live_key = ResponseKey::new(Some("tenant"), None, Some("corpus"), "live").unwrap();
    let mut scope = store.begin().await.unwrap();
    let live = match http_responses::claim(&mut scope, &live_key, &[1; 32], future)
        .await
        .unwrap()
    {
        ClaimResult::Claimed(claim) => claim,
        _ => panic!("expected claim"),
    };
    scope.commit(Utc::now()).await.unwrap();
    for _ in 0..3 {
        assert_eq!(store.prune_http_responses(Utc::now(), 1).await.unwrap(), 1);
    }
    assert_eq!(
        store
            .prune_http_responses(Utc::now() + chrono::Duration::days(1), 1)
            .await
            .unwrap(),
        0,
        "future caller cutoffs cannot delete unexpired claims"
    );
    let mut scope = store.begin().await.unwrap();
    assert!(matches!(
        http_responses::claim(&mut scope, &live_key, &[1; 32], future)
            .await
            .unwrap(),
        ClaimResult::InFlight
    ));
    drop(scope);
    let mut scope = store.begin().await.unwrap();
    let current = match http_responses::claim(&mut scope, &key, &[2; 32], future)
        .await
        .unwrap()
    {
        ClaimResult::Claimed(claim) => claim,
        _ => panic!("expected successor"),
    };
    scope.commit(Utc::now()).await.unwrap();
    let response = StoredResponse {
        status: 202,
        body: serde_json::to_vec(&json!({"text":"網🧵".repeat(70_000),"optional":null})).unwrap(),
        location: None,
    };
    assert!(response.body.len() > 256_000);
    assert!(!format!("{response:?}").contains('網'));
    let mut scope = store.begin().await.unwrap();
    assert!(matches!(
        http_responses::complete(&mut scope, &stale, &response).await,
        Err(raddle_postgres::Error::IdentityConflict)
    ));
    assert!(scope.commit(Utc::now()).await.is_err());
    assert!(!store.release_http_response(&stale).await.unwrap());
    let mut scope = store.begin().await.unwrap();
    http_responses::complete(&mut scope, &current, &response)
        .await
        .unwrap();
    scope.commit(Utc::now()).await.unwrap();
    let mut scope = store.begin().await.unwrap();
    assert!(
        http_responses::complete(
            &mut scope,
            &current,
            &StoredResponse {
                status: 201,
                body: Vec::new(),
                location: Some("/changed".into())
            }
        )
        .await
        .is_err()
    );
    drop(scope);
    assert!(!store.release_http_response(&current).await.unwrap());
    let mut scope = store.begin().await.unwrap();
    match http_responses::claim(&mut scope, &key, &[2; 32], future)
        .await
        .unwrap()
    {
        ClaimResult::Replay(stored) => {
            assert_eq!(stored.status, 202);
            assert_eq!(stored.body, response.body);
            assert_eq!(stored.location, None);
            assert!(serde_json::from_slice::<Value>(&stored.body).unwrap()["optional"].is_null());
        }
        _ => panic!("expected immutable answered response"),
    };
    drop(scope);
    assert!(store.release_http_response(&live).await.unwrap());
}
