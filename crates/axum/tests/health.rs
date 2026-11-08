//! Real database health with authorization and OpenAPI agreement.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::body::Body;
use axum::http::{HeaderMap, Request};
use http_body_util::BodyExt;
use raddle::RuntimeBuilder;
use raddle_axum::{ApiBuilder, ApiError, Permission, Principal};
use raddle_postgres::Store;
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;

fn auth(headers: &HeaderMap) -> Result<Principal, ApiError> {
    let permissions = if headers["Authorization"] == "Bearer inspect" {
        vec![Permission::Inspect]
    } else {
        vec![Permission::Invoke]
    };
    Principal::try_new("tenant", "operator", permissions)
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn readiness_tracks_workers_and_database_without_claiming_broker_health(pool: PgPool) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let workers = Arc::new(AtomicBool::new(false));
    let mut builder = ApiBuilder::new(RuntimeBuilder::default().build(), auth);
    builder
        .health("/health", store, {
            let workers = Arc::clone(&workers);
            move || workers.load(Ordering::SeqCst)
        })
        .unwrap();
    let api = builder.build();
    for (ready, expected) in [(false, 503), (true, 200)] {
        workers.store(ready, Ordering::SeqCst);
        let response = api
            .router()
            .oneshot(
                Request::builder()
                    .uri("/health/ready")
                    .header("Authorization", "Bearer inspect")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body, json!({"ready":ready,"database":true,"workers":ready}));
        let mut schema = api.specification()["paths"]["/health/ready"]["get"]["responses"]
            [expected.to_string()]["content"]["application/json"]["schema"]
            .clone();
        schema["components"] = api.specification()["components"].clone();
        assert!(jsonschema::validator_for(&schema).unwrap().is_valid(&body));
    }
    for path in ["/health/live", "/health/ready", "/health/queues"] {
        let response = api
            .router()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("Authorization", "Bearer invoke")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 403);
        let response = api
            .router()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("Authorization", "Bearer inspect")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
    pool.close().await;
    let response = api
        .router()
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .header("Authorization", "Bearer inspect")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
}
