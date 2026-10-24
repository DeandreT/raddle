//! Exercise the real router against its generated OpenAPI contract.
#![allow(clippy::unwrap_used)]

use std::convert::Infallible;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use http_body_util::BodyExt;
use raddle::{Request as Message, RuntimeBuilder};
use raddle_axum::{Api, ApiBuilder, ApiError, Authenticated, Permission, Principal};
use raddle_postgres::Store;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

#[derive(Deserialize, Serialize, JsonSchema)]
struct Echo {
    value: String,
}
#[derive(Serialize, Deserialize, JsonSchema)]
struct EchoReply {
    value: String,
    tenant: String,
}
impl Message for Echo {
    type Reply = EchoReply;
    type Error = Infallible;
}

fn authenticate(headers: &HeaderMap) -> Result<Principal, ApiError> {
    match headers
        .get("Authorization")
        .and_then(|value| value.to_str().ok())
    {
        Some("Bearer allowed") => Principal::try_new(
            "tenant-a",
            "test-subject",
            [Permission::Invoke, Permission::Inspect, Permission::Replay],
        ),
        Some("Bearer no-role") => Principal::try_new("tenant-a", "test-subject", []),
        _ => Err(ApiError::unauthorized()),
    }
}

fn api() -> Api {
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(|request: Authenticated<Echo>| async move {
            let (principal, message) = request.into_parts();
            Ok(EchoReply {
                value: message.value,
                tenant: principal.tenant().into(),
            })
        })
        .unwrap();
    let mut api = ApiBuilder::new(runtime.build(), authenticate);
    api.request::<Echo>("/echo").unwrap();
    // The metrics route does not connect to this deliberately unused pool.
    api.operations(
        "/ops",
        Store::new(
            PgPoolOptions::new()
                .connect_lazy("postgresql://localhost/unused")
                .unwrap(),
        ),
    )
    .unwrap();
    api.build()
}

fn request(path: &str, method: &str, authorization: Option<&str>, body: &str) -> Request<Body> {
    let mut request = Request::builder()
        .uri(path)
        .method(method)
        .header("Content-Type", "application/json");
    if let Some(authorization) = authorization {
        request = request.header("Authorization", authorization);
    }
    request.body(Body::from(body.to_owned())).unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

fn validate(schema: &Value, spec: &Value, body: &Value) {
    let mut schema = schema.clone();
    schema["components"] = spec["components"].clone();
    let validator = jsonschema::validator_for(&schema).unwrap();
    assert!(
        validator.is_valid(body),
        "body differs from the generated schema"
    );
}

#[tokio::test]
async fn typed_handler_and_errors_match_openapi() {
    let api = api();
    let response = api
        .router()
        .oneshot(request(
            "/echo",
            "POST",
            Some("Bearer allowed"),
            r#"{"value":"hello"}"#,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body, json!({"value":"hello","tenant":"tenant-a"}));
    validate(
        &api.specification()["paths"]["/echo"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"],
        api.specification(),
        &body,
    );
    for (path, method, auth, payload, status, code) in [
        ("/echo", "POST", None, "not json", 401, "unauthorized"),
        (
            "/echo",
            "POST",
            Some("Bearer no-role"),
            r#"{"value":"x"}"#,
            403,
            "forbidden",
        ),
        (
            "/echo",
            "POST",
            Some("Bearer allowed"),
            "not json",
            400,
            "invalid_request",
        ),
        (
            "/missing",
            "GET",
            Some("Bearer allowed"),
            "",
            404,
            "not_found",
        ),
        (
            "/echo",
            "GET",
            Some("Bearer allowed"),
            "",
            405,
            "method_not_allowed",
        ),
    ] {
        let response = api
            .router()
            .oneshot(request(path, method, auth, payload))
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status);
        if status == 401 {
            assert_eq!(response.headers()["WWW-Authenticate"], "Bearer");
        }
        let body = json_body(response).await;
        assert_eq!(body["code"], code);
        validate(
            &json!({"$ref":"#/components/schemas/Error"}),
            api.specification(),
            &body,
        );
    }
}

#[tokio::test]
async fn advertised_operations_and_headers_are_served_by_the_router() {
    let api = api();
    let response = api
        .router()
        .oneshot(request("/openapi.json", "GET", Some("Bearer allowed"), ""))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await, *api.specification());
    for (path, methods) in api.specification()["paths"].as_object().unwrap() {
        for (method, operation) in methods.as_object().unwrap() {
            assert_eq!(operation["security"], json!([{"bearerAuth":[]}]));
            assert!(operation["responses"]["401"]["headers"]["WWW-Authenticate"].is_object());
            // Anonymous calls reach the common authentication layer, not a missing route.
            let path = path.replace("{id}", "00000000-0000-0000-0000-000000000001");
            let response = api
                .router()
                .oneshot(request(&path, &method.to_uppercase(), None, ""))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
    }
    let response = api
        .router()
        .oneshot(request("/ops/metrics", "GET", Some("Bearer allowed"), ""))
        .await
        .unwrap();
    assert_eq!(response.headers()["Content-Type"], "text/plain");
    let text = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(text.contains("raddle_http_requests_total"));
}

#[tokio::test]
async fn route_registration_rejects_invalid_and_duplicate_paths() {
    let mut builder = ApiBuilder::new(RuntimeBuilder::default().build(), authenticate);
    assert!(builder.request::<Echo>("/openapi.json").is_err());
    assert!(builder.request::<Echo>("/:parameter").is_err());
    builder.request::<Echo>("/echo").unwrap();
    assert!(builder.request::<Echo>("/echo").is_err());
}

#[tokio::test]
async fn spec_and_response_are_checked_over_an_actual_http_socket() {
    let api = api();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = api.router();
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{address}/echo"))
        .bearer_auth("allowed")
        .json(&json!({"value":"socket"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let body = response.json::<Value>().await.unwrap();
    validate(
        &api.specification()["paths"]["/echo"]["post"]["responses"]["200"]["content"]["application/json"]
            ["schema"],
        api.specification(),
        &body,
    );
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn malformed_path_and_large_body_keep_the_json_error_contract() {
    let api = api();
    let response = api
        .router()
        .oneshot(request("/ops/jobs/%FF", "GET", Some("Bearer allowed"), ""))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json_body(response).await["code"], "invalid_request");
    let response = api
        .router()
        .oneshot(request(
            "/echo",
            "POST",
            Some("Bearer allowed"),
            &"a".repeat(1_048_577),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(json_body(response).await["code"], "payload_too_large");
}

#[tokio::test]
async fn overlapping_methods_are_preserved_in_the_specification() {
    let mut builder = ApiBuilder::new(RuntimeBuilder::default().build(), authenticate);
    builder.request::<Echo>("/ops/metrics").unwrap();
    builder
        .operations(
            "/ops",
            Store::new(
                PgPoolOptions::new()
                    .connect_lazy("postgresql://localhost/unused")
                    .unwrap(),
            ),
        )
        .unwrap();
    let api = builder.build();
    assert!(api.specification()["paths"]["/ops/metrics"]["get"].is_object());
    assert!(api.specification()["paths"]["/ops/metrics"]["post"].is_object());
    let mut ids = std::collections::HashSet::new();
    for operations in api.specification()["paths"].as_object().unwrap().values() {
        for operation in operations.as_object().unwrap().values() {
            assert!(ids.insert(operation["operationId"].as_str().unwrap()));
        }
    }
}
