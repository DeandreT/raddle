//! Typed domain mapping and asynchronous validation use the actual endpoint pipeline.
#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use http_body_util::BodyExt;
use raddle::RuntimeBuilder;
use raddle_axum::native::{
    Endpoint, EndpointContext, EndpointContract, EndpointOutput, EndpointRegistration,
    EndpointResponse, HttpMethod, NativeApiBuilder, Problem, RequestInput, Validation,
};
use raddle_axum::{ApiError, Permission, Principal};
use serde_json::{Value, json};
use tower::ServiceExt;

#[derive(Debug)]
enum Failure {
    Concurrent,
    Locked,
    Unknown,
    Group(Vec<Failure>),
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("private database exception never exposed")
    }
}
impl std::error::Error for Failure {}
fn concurrency(error: &Failure) -> bool {
    matches!(error, Failure::Concurrent | Failure::Locked)
}

struct Checked {
    phases: Arc<Mutex<Vec<&'static str>>>,
}
impl Endpoint for Checked {
    type Input = String;
    type Resource = Value;
    type Error = Failure;
    async fn bind(&self, input: &mut RequestInput) -> Result<String, Problem> {
        self.phases.lock().unwrap().push("bind");
        input.parameters.query("case", String::from("ok"))
    }
    async fn validate(&self, _context: &mut EndpointContext, input: &String) -> Validation {
        self.phases.lock().unwrap().push("validate");
        tokio::task::yield_now().await;
        if input == "invalid" {
            let mut problem = Problem::invalid("name");
            problem.extensions.insert("trace".into(), json!("safe-id"));
            Validation::Reject(problem)
        } else {
            Validation::NoProblems
        }
    }
    async fn handle(
        &self,
        _context: &mut EndpointContext,
        input: String,
    ) -> Result<EndpointOutput<Value>, Failure> {
        self.phases.lock().unwrap().push("handle");
        match input.as_str() {
            "conflict" => Err(Failure::Concurrent),
            "all" => Err(Failure::Group(vec![Failure::Concurrent, Failure::Locked])),
            "mixed" => Err(Failure::Group(vec![Failure::Concurrent, Failure::Unknown])),
            _ => Ok(EndpointResponse::Json(json!({"ok":true})).into()),
        }
    }
    async fn after(
        &self,
        _context: &mut EndpointContext,
        _output: &mut EndpointOutput<Value>,
    ) -> Result<(), Problem> {
        self.phases.lock().unwrap().push("after");
        Ok(())
    }
    async fn finally(&self, _context: &mut EndpointContext, _status: StatusCode) {
        self.phases.lock().unwrap().push("finally");
    }
    fn map_error(&self, error: Failure) -> Problem {
        // Explicit typed filter: a mixed aggregate must not be swallowed as 409.
        if concurrency(&error)
            || matches!(&error,Failure::Group(errors) if !errors.is_empty() && errors.iter().all(concurrency))
        {
            let mut problem =
                Problem::new(StatusCode::CONFLICT).detail("The resource changed; reload and retry");
            problem.kind = "urn:test:concurrency".into();
            problem
        } else {
            Problem::new(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[tokio::test]
async fn typed_all_concurrency_filter_validation_sentinel_and_finally_preserve_safe_problems() {
    let phases = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = RuntimeBuilder::default();
    let reg = EndpointRegistration::prepare(
        &mut runtime,
        EndpointContract::new(HttpMethod::Get, "/checked", "checked"),
        Checked {
            phases: Arc::clone(&phases),
        },
    )
    .unwrap();
    let mut builder = NativeApiBuilder::new(
        runtime.build(),
        |_: &HeaderMap| -> Result<Principal, ApiError> {
            Principal::try_new("tenant", "subject", [Permission::Invoke])
        },
    );
    builder.endpoint(reg).unwrap();
    let api = builder.build();
    for (case, status, expected) in [
        (
            "ok",
            200,
            vec!["bind", "validate", "handle", "after", "finally"],
        ),
        ("invalid", 400, vec!["bind", "validate", "finally"]),
        (
            "conflict",
            409,
            vec!["bind", "validate", "handle", "finally"],
        ),
        ("all", 409, vec!["bind", "validate", "handle", "finally"]),
        ("mixed", 500, vec!["bind", "validate", "handle", "finally"]),
        (
            "ok",
            200,
            vec!["bind", "validate", "handle", "after", "finally"],
        ),
    ] {
        phases.lock().unwrap().clear();
        let request = Request::builder()
            .uri(format!("/checked?case={case}"))
            .body(Body::empty())
            .unwrap();
        let response = api.router().oneshot(request).await.unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(*phases.lock().unwrap(), expected);
        if status != 200 {
            assert_eq!(
                response.headers()["content-type"],
                "application/problem+json"
            );
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert!(!String::from_utf8_lossy(&body).contains("private database"));
            let value = serde_json::from_slice::<Value>(&body).unwrap();
            assert_eq!(value["status"], status);
            let mut schema = json!({"$ref":"#/components/schemas/Problem"});
            schema["components"] = api.specification()["components"].clone();
            assert!(jsonschema::validator_for(&schema).unwrap().is_valid(&value));
        }
    }
}
