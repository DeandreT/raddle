//! Actual router/socket checks for typed binding and bounded streaming.
#![allow(clippy::unwrap_used)]

use std::convert::Infallible;
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use http_body_util::BodyExt;
use raddle::{Request as Message, RuntimeBuilder};
use raddle_axum::{
    ApiBuilder, ApiError, Authenticated, EventStream, HeaderParameter, HttpRequest, Permission,
    Principal, StreamOptions,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tower::ServiceExt;

#[derive(Deserialize, JsonSchema)]
struct Id {
    id: u32,
}
#[derive(Deserialize, JsonSchema)]
struct Options {
    #[serde(default)]
    detailed: bool,
}
#[derive(Deserialize, JsonSchema)]
struct Empty {}
#[derive(Serialize, JsonSchema)]
struct Reply {
    id: u32,
    detailed: bool,
    locale: String,
    tenant: String,
}
struct Lookup {
    id: u32,
    detailed: bool,
    locale: String,
}
impl Message for Lookup {
    type Reply = Reply;
    type Error = Infallible;
}
impl HttpRequest for Lookup {
    type Path = Id;
    type Query = Options;
    type Body = ();
    const HEADERS: &'static [HeaderParameter] = &[HeaderParameter {
        name: "x-locale",
        required: true,
    }];
    fn bind(path: Id, query: Options, headers: &HeaderMap, (): ()) -> Result<Self, ApiError> {
        if path.id == 0 {
            return Err(ApiError::invalid());
        }
        let locale = headers
            .get("x-locale")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(ApiError::invalid)?;
        if locale.len() > 8 {
            return Err(ApiError::invalid());
        }
        Ok(Self {
            id: path.id,
            detailed: query.detailed,
            locale: locale.to_owned(),
        })
    }
}

fn authenticate(_: &HeaderMap) -> Result<Principal, ApiError> {
    Principal::try_new("trusted", "test", [Permission::Invoke, Permission::Inspect])
}

fn request(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header("Authorization", "Bearer fixture")
        .header("x-locale", "en")
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn path_query_and_headers_bind_with_matching_parameters_and_validation() {
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(|request: Authenticated<Lookup>| async move {
            let (principal, message) = request.into_parts();
            Ok(Reply {
                id: message.id,
                detailed: message.detailed,
                locale: message.locale,
                tenant: principal.tenant().to_owned(),
            })
        })
        .unwrap();
    let mut builder = ApiBuilder::new(runtime.build(), authenticate);
    builder.query::<Lookup>("/items/{id}").unwrap();
    let api = builder.build();
    let parameters = api.specification()["paths"]["/items/{id}"]["get"]["parameters"]
        .as_array()
        .unwrap();
    assert!(
        parameters
            .iter()
            .any(|p| p["name"] == "id" && p["in"] == "path" && p["required"] == true)
    );
    assert!(
        parameters
            .iter()
            .any(|p| p["name"] == "detailed" && p["in"] == "query" && p["required"] == false)
    );
    assert!(
        parameters
            .iter()
            .any(|p| p["name"] == "x-locale" && p["in"] == "header" && p["required"] == true)
    );
    let response = api
        .router()
        .oneshot(request("/items/42?detailed=true"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(
        body,
        json!({"id":42,"detailed":true,"locale":"en","tenant":"trusted"})
    );
    for uri in ["/items/0", "/items/no", "/items/42?detailed=no"] {
        assert_eq!(
            api.router().oneshot(request(uri)).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
    }
    let mut missing = request("/items/42");
    missing.headers_mut().remove("x-locale");
    assert_eq!(
        api.router().oneshot(missing).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    let anonymous = Request::builder()
        .uri("/items/42")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        api.router().oneshot(anonymous).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn invalid_templates_and_schema_names_fail_without_poisoning_registration() {
    let mut builder = ApiBuilder::new(RuntimeBuilder::default().build(), authenticate);
    for path in [
        "/items/{other}",
        "/items/{id}/{id}",
        "/items/{*id}",
        "/items/{id",
        "/items/{id}/?bad",
    ] {
        assert!(builder.query::<Lookup>(path).is_err(), "{path}");
    }
    builder.query::<Lookup>("/items/{id}").unwrap();
    assert!(builder.query::<Lookup>("/items/{id}").is_err());
    assert!(builder.query::<Lookup>("/items/{other}").is_err());
}

#[derive(Deserialize, JsonSchema)]
struct Watch {
    count: u32,
}
impl Message for Watch {
    type Reply = EventStream<u32>;
    type Error = Infallible;
}

#[tokio::test]
async fn sse_uses_actual_http_socket_has_finite_bounds_and_closes_producer() {
    let (producer, stream) = EventStream::channel(
        2,
        StreamOptions::try_new(2, Duration::from_secs(3)).unwrap(),
    )
    .unwrap();
    producer.send(7).await.unwrap();
    producer.send(8).await.unwrap();
    let stream = std::sync::Arc::new(std::sync::Mutex::new(Some(stream)));
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(move |request: Authenticated<Watch>| {
            assert_eq!(request.message().count, 2);
            let stream = stream.lock().unwrap().take().unwrap();
            async move { Ok(stream) }
        })
        .unwrap();
    let mut builder = ApiBuilder::new(runtime.build(), authenticate);
    builder.events::<Watch, u32>("/events").unwrap();
    let api = builder.build();
    assert!(api.specification()["paths"]["/events"]["get"]["requestBody"].is_null());
    assert!(api.specification()["paths"]["/events"]["get"]["responses"]["200"]["content"]["text/event-stream"].is_object());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut servers = tokio::task::JoinSet::new();
    servers.spawn(async move { axum::serve(listener, api.router()).await.unwrap() });
    let response = reqwest::Client::new()
        .get(format!("http://{address}/events?count=2"))
        .bearer_auth("fixture")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(
        response.headers()["Content-Type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    let body = response.text().await.unwrap();
    assert!(
        body.contains("data: 7") && body.contains("data: 8"),
        "{body}"
    );
    assert_eq!(body.matches("data:").count(), 2);
    assert!(
        producer.send(9).await.is_err(),
        "finite stream must close its producer"
    );
    servers.shutdown().await;
}

#[tokio::test]
async fn stream_drop_cancels_backpressure_and_lifetime_is_finite_without_events() {
    let options = StreamOptions::try_new(1, Duration::from_millis(20)).unwrap();
    assert!(EventStream::<u32>::channel(0, options).is_err());
    assert!(StreamOptions::try_new(0, Duration::from_secs(1)).is_err());
    let (producer, stream) = EventStream::<u32>::channel(1, options).unwrap();
    producer.send(1).await.unwrap();
    let blocked = producer.send(2);
    tokio::pin!(blocked);
    tokio::select! { result=&mut blocked => panic!("bounded producer escaped: {result:?}"), ()=tokio::time::sleep(Duration::from_millis(5)) => {} }
    drop(stream);
    assert!(blocked.await.is_err());
}

struct Create {
    id: u32,
    text: String,
}
#[derive(Deserialize, JsonSchema)]
struct Text {
    text: String,
}
impl Message for Create {
    type Reply = String;
    type Error = Infallible;
}
impl HttpRequest for Create {
    type Path = Id;
    type Query = Empty;
    type Body = Text;
    fn bind(path: Id, _: Empty, _: &HeaderMap, body: Text) -> Result<Self, ApiError> {
        if body.text.len() > 16 {
            return Err(ApiError::invalid());
        }
        Ok(Self {
            id: path.id,
            text: body.text,
        })
    }
}

#[tokio::test]
async fn post_binds_typed_json_with_path_and_query() {
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(|request: Authenticated<Create>| async move {
            let (_, m) = request.into_parts();
            Ok(format!("{}:{}", m.id, m.text))
        })
        .unwrap();
    let mut builder = ApiBuilder::new(runtime.build(), authenticate);
    builder.bound_request::<Create>("/items/{id}").unwrap();
    let api = builder.build();
    let response = api
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/items/4")
                .header("Authorization", "Bearer fixture")
                .header("Content-Type", "application/json")
                .body(Body::from("{\"text\":\"hello\"}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "\"4:hello\""
    );
}

#[tokio::test]
async fn conditional_get_revalidates_through_authentication_and_handler_without_shared_cache() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle({
            let calls = Arc::clone(&calls);
            move |request: Authenticated<Lookup>| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    let (p, m) = request.into_parts();
                    Ok(Reply {
                        id: m.id,
                        detailed: m.detailed,
                        locale: m.locale,
                        tenant: p.tenant().to_owned(),
                    })
                }
            }
        })
        .unwrap();
    let mut builder = ApiBuilder::new(runtime.build(), authenticate);
    assert!(builder.conditional_get("/items/{id}").is_err());
    builder
        .query::<Lookup>("/items/{id}")
        .unwrap()
        .conditional_get("/items/{id}")
        .unwrap();
    assert!(builder.conditional_get("/items/{id}").is_err());
    let api = builder.build();
    let response = api.router().oneshot(request("/items/4")).await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["Cache-Control"], "private, no-cache");
    let tag = response.headers()["ETag"].clone();
    let mut again = request("/items/4");
    again.headers_mut().insert("If-None-Match", tag.clone());
    let response = api.router().oneshot(again).await.unwrap();
    assert_eq!(response.status(), 304);
    assert!(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty()
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "conditional requests must not bypass current handler authorization"
    );
    let mut anonymous = request("/items/4");
    anonymous.headers_mut().remove("Authorization");
    anonymous.headers_mut().insert("If-None-Match", tag.clone());
    let response = api.router().oneshot(anonymous).await.unwrap();
    assert_eq!(response.status(), 401);
    assert!(!response.headers().contains_key("ETag"));
    let mut changed = request("/items/5");
    changed.headers_mut().insert("If-None-Match", tag);
    assert_eq!(api.router().oneshot(changed).await.unwrap().status(), 200);
    assert!(
        api.specification()["paths"]["/items/{id}"]["get"]["responses"]["304"]["content"].is_null()
    );
}

#[tokio::test]
async fn ingress_limits_apply_after_authentication_and_return_documented_safe_429() {
    use raddle_axum::RequestLimits;
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(|request: Authenticated<Lookup>| async move {
            let (p, m) = request.into_parts();
            Ok(Reply {
                id: m.id,
                detailed: m.detailed,
                locale: m.locale,
                tenant: p.tenant().to_owned(),
            })
        })
        .unwrap();
    let mut builder = ApiBuilder::new(runtime.build(), authenticate);
    builder.query::<Lookup>("/items/{id}").unwrap();
    builder
        .limits(RequestLimits::try_new(1, Duration::from_secs(60), 1).unwrap())
        .unwrap();
    let api = builder.build();
    assert_eq!(
        api.router()
            .oneshot(request("/items/1"))
            .await
            .unwrap()
            .status(),
        200
    );
    let mut spoof = request("/items/2");
    spoof
        .headers_mut()
        .insert("x-tenant", "different".parse().unwrap());
    let response = api.router().oneshot(spoof).await.unwrap();
    assert_eq!(response.status(), 429);
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(
        body,
        json!({"code":"rate_limited","message":"request rate limit exceeded"})
    );
    let mut schema=api.specification()["paths"]["/items/{id}"]["get"]["responses"]["429"]["content"]["application/json"]["schema"].clone();
    schema["components"] = api.specification()["components"].clone();
    assert!(jsonschema::validator_for(&schema).unwrap().is_valid(&body));
    let mut anonymous = request("/items/3");
    anonymous.headers_mut().remove("Authorization");
    assert_eq!(api.router().oneshot(anonymous).await.unwrap().status(), 401);
}

#[tokio::test]
async fn idle_sse_reaches_its_lifetime_and_closes_the_real_http_channel() {
    let (producer, stream) = EventStream::<u32>::channel(
        1,
        StreamOptions::try_new(1, Duration::from_millis(30)).unwrap(),
    )
    .unwrap();
    let stream = std::sync::Arc::new(std::sync::Mutex::new(Some(stream)));
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(move |_: Authenticated<Watch>| {
            let stream = stream.lock().unwrap().take().unwrap();
            async move { Ok(stream) }
        })
        .unwrap();
    let mut builder = ApiBuilder::new(runtime.build(), authenticate);
    builder.events::<Watch, u32>("/idle").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let api = builder.build();
    let mut servers = tokio::task::JoinSet::new();
    servers.spawn(async move { axum::serve(listener, api.router()).await.unwrap() });
    let response = reqwest::Client::new()
        .get(format!("http://{address}/idle?count=0"))
        .bearer_auth("fixture")
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.text().await.unwrap().is_empty());
    assert!(producer.send(1).await.is_err());
    servers.shutdown().await;
}

#[derive(JsonSchema)]
struct BrokenEvent;
impl Serialize for BrokenEvent {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("private serializer diagnostic"))
    }
}
#[derive(Deserialize, JsonSchema)]
struct BrokenWatch {}
impl Message for BrokenWatch {
    type Reply = EventStream<BrokenEvent>;
    type Error = Infallible;
}

#[tokio::test]
async fn sse_serialization_failure_closes_stream_without_exposing_error_text() {
    let (producer, stream) = EventStream::channel(
        1,
        StreamOptions::try_new(1, Duration::from_secs(1)).unwrap(),
    )
    .unwrap();
    producer.send(BrokenEvent).await.unwrap();
    let stream = std::sync::Arc::new(std::sync::Mutex::new(Some(stream)));
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(move |_: Authenticated<BrokenWatch>| {
            let stream = stream.lock().unwrap().take().unwrap();
            async move { Ok(stream) }
        })
        .unwrap();
    let mut builder = ApiBuilder::new(runtime.build(), authenticate);
    builder
        .events::<BrokenWatch, BrokenEvent>("/broken")
        .unwrap();
    let response = builder
        .build()
        .router()
        .oneshot(request("/broken"))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert!(bytes.is_empty());
    assert!(!String::from_utf8_lossy(&bytes).contains("private"));
    assert!(producer.send(BrokenEvent).await.is_err());
}

#[derive(Deserialize, JsonSchema)]
struct JobPath {
    job: uuid::Uuid,
}
struct ReadJob;
impl Message for ReadJob {
    type Reply = u32;
    type Error = Infallible;
}
impl HttpRequest for ReadJob {
    type Path = JobPath;
    type Query = Empty;
    type Body = ();
    fn bind(path: JobPath, _: Empty, _: &HeaderMap, (): ()) -> Result<Self, ApiError> {
        let _id = path.job;
        Ok(Self)
    }
}

#[tokio::test]
async fn operations_template_conflicts_return_errors_without_panicking_or_partial_registration() {
    let mut builder = ApiBuilder::new(RuntimeBuilder::default().build(), authenticate);
    builder.query::<ReadJob>("/ops/jobs/{job}").unwrap();
    let store = raddle_postgres::Store::new(
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://localhost/unused")
            .unwrap(),
    );
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        builder.operations("/ops", store).map(|_| ())
    }));
    assert!(
        result.is_ok(),
        "conflicting route shapes must return Err, not panic"
    );
    assert!(result.unwrap().is_err());
    builder
        .operations(
            "/other",
            raddle_postgres::Store::new(
                sqlx::postgres::PgPoolOptions::new()
                    .connect_lazy("postgresql://localhost/unused")
                    .unwrap(),
            ),
        )
        .unwrap();
    let api = builder.build();
    assert!(api.specification()["paths"]["/ops/jobs/{job}"]["get"].is_object());
    assert!(api.specification()["paths"]["/ops/jobs/{id}"].is_null());
}

#[tokio::test]
async fn expired_sse_does_not_emit_even_when_its_channel_is_already_buffered() {
    let (producer, stream) = EventStream::<u32>::channel(
        1,
        StreamOptions::try_new(1, Duration::from_millis(10)).unwrap(),
    )
    .unwrap();
    producer.send(42).await.unwrap();
    let stream = std::sync::Arc::new(std::sync::Mutex::new(Some(stream)));
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(move |_: Authenticated<Watch>| {
            let stream = stream.lock().unwrap().take().unwrap();
            async move { Ok(stream) }
        })
        .unwrap();
    let mut builder = ApiBuilder::new(runtime.build(), authenticate);
    builder.events::<Watch, u32>("/buffered").unwrap();
    let response = builder
        .build()
        .router()
        .oneshot(request("/buffered?count=1"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty(),
        "an immediately-ready buffered value cannot bypass the absolute deadline"
    );
    assert!(producer.send(43).await.is_err());
}

#[tokio::test]
async fn distinct_sibling_routes_can_use_different_capture_names() {
    let mut builder = ApiBuilder::new(RuntimeBuilder::default().build(), authenticate);
    builder.query::<Lookup>("/items/{id}/details").unwrap();
    builder.query::<ReadJob>("/items/{job}/history").unwrap();
    let api = builder.build();
    for path in [
        "/items/4/details",
        "/items/00000000-0000-0000-0000-000000000001/history",
    ] {
        // Binding must succeed with each route's actual capture name, then reach
        // the intentionally empty runtime (503), rather than fallback or a 400.
        assert_eq!(
            api.router().oneshot(request(path)).await.unwrap().status(),
            503
        );
    }
}
