//! Cache-Control field/directive parsing without changing handler or eviction behavior.
#![allow(clippy::unwrap_used)]

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Request, header};
use raddle::RuntimeBuilder;
use raddle_axum::native::{
    CacheEligibility, Endpoint, EndpointContext, EndpointContract, EndpointOutput,
    EndpointRegistration, EndpointResponse, HttpMethod, NativeApiBuilder, OutputCache,
    OutputCachePolicy, Problem, RequestInput,
};
use raddle_axum::{ApiError, Principal};
use serde_json::{Value, json};
use tower::ServiceExt;

struct Read {
    calls: Arc<AtomicUsize>,
    controls: Vec<&'static str>,
}
impl Endpoint for Read {
    type Input = ();
    type Resource = Value;
    type Error = Infallible;
    async fn bind(&self, _input: &mut RequestInput) -> Result<(), Problem> {
        Ok(())
    }
    async fn handle(
        &self,
        context: &mut EndpointContext,
        (): (),
    ) -> Result<EndpointOutput<Value>, Infallible> {
        let count = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        for value in &self.controls {
            context
                .response_headers
                .append(header::CACHE_CONTROL, HeaderValue::from_static(value));
        }
        Ok(EndpointResponse::Json(json!({"count":count})).into())
    }
}

#[tokio::test]
async fn no_store_is_case_insensitive_across_all_fields_without_substring_false_positives() {
    for (controls, cacheable) in [
        (vec!["No-Store"], false),
        (vec!["private, max-age=60", "no-store"], false),
        (vec!["private, NO-STORE = ignored"], false),
        (vec!["private, x-no-store=1"], true),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let cache = OutputCache::new(4, 4096).unwrap();
        let mut contract = EndpointContract::new(HttpMethod::Get, "/cached", "cached");
        contract.anonymous = true;
        contract.output_cache = Some(OutputCachePolicy {
            store: cache.clone(),
            ttl: Duration::from_secs(60),
            vary_query: vec![],
            vary_headers: vec![],
            tags: vec!["items".into()],
            eligibility: CacheEligibility::Anonymous,
        });
        let mut runtime = RuntimeBuilder::default();
        let registration = EndpointRegistration::prepare(
            &mut runtime,
            contract,
            Read {
                calls: Arc::clone(&calls),
                controls: controls.clone(),
            },
        )
        .unwrap();
        let mut builder = NativeApiBuilder::new(
            runtime.build(),
            |_: &HeaderMap| -> Result<Principal, ApiError> { Err(ApiError::unauthorized()) },
        );
        builder.endpoint(registration).unwrap();
        let api = builder.build();
        for index in 1..=3 {
            let response = api
                .router()
                .oneshot(
                    Request::builder()
                        .uri("/cached")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(
                response
                    .headers()
                    .get_all(header::CACHE_CONTROL)
                    .iter()
                    .count(),
                controls.len()
            );
            let body: Value = serde_json::from_slice(
                &axum::body::to_bytes(response.into_body(), 4096)
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(body["count"], if cacheable { 1 } else { index });
        }
        assert_eq!(calls.load(Ordering::SeqCst), if cacheable { 1 } else { 3 });
        cache.evict_tag("items").unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            if cacheable { 1 } else { 3 },
            "invalidation itself must not execute handlers"
        );
        let response = api
            .router()
            .oneshot(
                Request::builder()
                    .uri("/cached")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(calls.load(Ordering::SeqCst), if cacheable { 2 } else { 4 });
    }
}
