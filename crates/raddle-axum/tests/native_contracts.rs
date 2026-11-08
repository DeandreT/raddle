//! Real socket contracts for the explicit native registration path.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use futures_util::stream;
use raddle::RuntimeBuilder;
use raddle_axum::native::{
    ApiVersion, BindingPolicy, BodyMode, Endpoint, EndpointContext, EndpointContract,
    EndpointEffect, EndpointOutput, EndpointRegistration, EndpointResponse, HttpMethod,
    MissingResponse, NativeApiBuilder, Negotiation, Parameter, ParameterSource, Problem,
    RequestInput, ResourceWriter, ResponseKind, RoutePrefixes, Validation, VersionReader,
    Versioning,
};
use raddle_axum::{Api, ApiError, Permission, Principal};
use serde_json::{Value, json};

fn authenticate(headers: &HeaderMap) -> Result<Principal, ApiError> {
    match headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
    {
        Some("Bearer allowed") => {
            Principal::try_new("verified-tenant", "alice", [Permission::Invoke])
        }
        Some("Bearer other") => Principal::try_new("other-tenant", "bob", [Permission::Invoke]),
        Some("Bearer denied") => Principal::try_new("verified-tenant", "alice", []),
        _ => Err(ApiError::unauthorized()),
    }
}

struct Probe<const N: usize> {
    json: bool,
    calls: Arc<AtomicUsize>,
    effects: Arc<AtomicUsize>,
    allowed: Arc<AtomicBool>,
}
impl<const N: usize> Probe<N> {
    fn new(json: bool) -> Self {
        Self {
            json,
            calls: Arc::new(AtomicUsize::new(0)),
            effects: Arc::new(AtomicUsize::new(0)),
            allowed: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl<const N: usize> Endpoint for Probe<N> {
    type Input = Value;
    type Resource = Value;
    type Error = Infallible;
    async fn bind(&self, input: &mut RequestInput) -> Result<Value, Problem> {
        let id = input.parameters.path::<u32>("id")?;
        let query = input.parameters.query("aliased-count", 9u32)?;
        let many = input.parameters.query_many::<u32>("values[]")?;
        let header = input.parameters.header("x-count", 7u32)?;
        let raw_header = input.parameters.header("x-text", String::from("default"))?;
        let response = input.parameters.query("response", String::from("json"))?;
        let rest = input.parameters.path::<String>("rest")?;
        let body = if self.json {
            input.json::<Value>().await?
        } else {
            None
        };
        Ok(
            json!({"id":id,"count":query,"values":many,"header":header,"raw_header":raw_header,"body":body,"response":response,"rest":rest,"protocol":format!("{:?}",input.version)}),
        )
    }
    async fn authorize(
        &self,
        _context: &mut EndpointContext,
        _input: &Value,
    ) -> Result<(), Problem> {
        if !self.allowed.load(Ordering::SeqCst) {
            return Err(Problem::new(StatusCode::FORBIDDEN));
        }
        Ok(())
    }
    async fn validate(&self, _context: &mut EndpointContext, input: &Value) -> Validation {
        if input["count"] == 0 {
            Validation::Reject(Problem::invalid("aliased-count"))
        } else {
            Validation::NoProblems
        }
    }
    async fn handle(
        &self,
        context: &mut EndpointContext,
        mut input: Value,
    ) -> Result<EndpointOutput<Value>, Infallible> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        input["tenant"] = json!(context.principal().map(Principal::tenant));
        input["api_version"] = json!(context.api_version);
        let effect = Arc::clone(&self.effects);
        let effects: Vec<EndpointEffect> = vec![Box::new(move |_| {
            Box::pin(async move {
                effect.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        })];
        let response = match input["response"].as_str().unwrap() {
            "text" => EndpointResponse::Text("plain text".into()),
            "empty" => EndpointResponse::Empty,
            "missing" => EndpointResponse::Missing,
            "created" => EndpointResponse::Created {
                resource: Some(input),
                location: "/items/3".into(),
            },
            "accepted" => EndpointResponse::Accepted {
                resource: None,
                location: "/jobs/4".into(),
            },
            "status" => EndpointResponse::Status(StatusCode::IM_A_TEAPOT),
            "started" => {
                EndpointResponse::Started((StatusCode::OK, "already started").into_response())
            }
            _ => EndpointResponse::Json(input),
        };
        Ok((response, effects).into())
    }
    fn writers(&self) -> Vec<ResourceWriter<Value>> {
        vec![
            ResourceWriter {
                media_type: "text/csv",
                write: |_| Ok(b"first,csv".to_vec()),
            },
            ResourceWriter {
                media_type: "application/custom",
                write: |_| Ok(b"second".to_vec()),
            },
        ]
    }
    fn body_schema(&self) -> Value {
        json!({"type":"object"})
    }
}

fn contract(method: HttpMethod, path: &str, name: &str, json_body: bool) -> EndpointContract {
    let mut contract = EndpointContract::new(method, path, name);
    contract.responses = vec![
        ResponseKind::Json,
        ResponseKind::Text,
        ResponseKind::Status,
        ResponseKind::Empty,
        ResponseKind::Missing,
        ResponseKind::Created,
        ResponseKind::Accepted,
        ResponseKind::Started,
    ];
    contract.response_metadata.insert("200".into(),json!({"description":"JSON, custom writer, text or explicit started response","content":{"application/json":{"schema":{}},"text/plain":{"schema":{"type":"string"}},"text/csv":{"schema":{"type":"string"}},"application/custom":{"schema":{"type":"string"}}}}));
    contract.parameters = vec![
        Parameter::typed::<u32>("aliased-count", ParameterSource::Query, false).default(9),
        Parameter::typed::<Vec<u32>>("values[]", ParameterSource::Query, false),
        Parameter::typed::<u32>("x-count", ParameterSource::Header, false).default(7),
        Parameter::typed::<String>("x-text", ParameterSource::Header, false),
    ];
    if path.contains("{id") {
        contract
            .parameters
            .push(Parameter::typed::<u32>("id", ParameterSource::Path, true));
    }
    if path.contains("{*rest}") {
        contract.parameters.push(Parameter::typed::<String>(
            "rest",
            ParameterSource::Path,
            true,
        ));
    }
    if json_body {
        contract.body = BodyMode::Json {
            optional: true,
            max_bytes: 1024,
        };
    }
    contract
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

#[tokio::test]
async fn every_method_uses_the_same_contract_and_head_has_no_body() {
    let mut runtime = RuntimeBuilder::default();
    let get = EndpointRegistration::prepare(
        &mut runtime,
        contract(HttpMethod::Get, "/method", "get", false),
        Probe::<0>::new(false),
    )
    .unwrap();
    let post = EndpointRegistration::prepare(
        &mut runtime,
        contract(HttpMethod::Post, "/method", "post", true),
        Probe::<1>::new(true),
    )
    .unwrap();
    let put = EndpointRegistration::prepare(
        &mut runtime,
        contract(HttpMethod::Put, "/method", "put", true),
        Probe::<2>::new(true),
    )
    .unwrap();
    let delete = EndpointRegistration::prepare(
        &mut runtime,
        contract(HttpMethod::Delete, "/method", "delete", true),
        Probe::<3>::new(true),
    )
    .unwrap();
    let patch = EndpointRegistration::prepare(
        &mut runtime,
        contract(HttpMethod::Patch, "/method", "patch", true),
        Probe::<4>::new(true),
    )
    .unwrap();
    let options = EndpointRegistration::prepare(
        &mut runtime,
        contract(HttpMethod::Options, "/method", "options", false),
        Probe::<5>::new(false),
    )
    .unwrap();
    let head = EndpointRegistration::prepare(
        &mut runtime,
        contract(HttpMethod::Head, "/method", "head", false),
        Probe::<6>::new(false),
    )
    .unwrap();
    let query = EndpointRegistration::prepare(
        &mut runtime,
        contract(HttpMethod::Query, "/method", "query", true),
        Probe::<7>::new(true),
    )
    .unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), authenticate);
    builder
        .endpoint(get)
        .unwrap()
        .endpoint(post)
        .unwrap()
        .endpoint(put)
        .unwrap()
        .endpoint(delete)
        .unwrap()
        .endpoint(patch)
        .unwrap()
        .endpoint(options)
        .unwrap()
        .endpoint(head)
        .unwrap()
        .endpoint(query)
        .unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    let client = reqwest::Client::new();
    for method in [
        "GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD", "QUERY",
    ] {
        let mut request = client
            .request(method.parse().unwrap(), format!("{url}/method"))
            .bearer_auth("allowed");
        if matches!(method, "POST" | "PUT" | "DELETE" | "PATCH" | "QUERY") {
            request = request.json(&json!({"actual":"body"}));
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), 200, "{method}");
        if method == "HEAD" {
            assert_eq!(response.bytes().await.unwrap().len(), 0);
        } else {
            let body = response.json::<Value>().await.unwrap();
            assert_eq!(body["tenant"], "verified-tenant");
            if method == "QUERY" {
                assert_eq!(body["body"]["actual"], "body");
            }
        }
    }
    let trace = client
        .request("TRACE".parse().unwrap(), format!("{url}/method"))
        .send()
        .await
        .unwrap();
    assert_eq!(trace.status(), 405);
    assert!(trace.headers()["allow"].to_str().unwrap().contains("QUERY"));
    assert!(api.specification()["paths"]["/method"]["query"].is_null());
    assert!(
        api.specification()["paths"]["/method"]["head"]["responses"]["200"]["content"].is_null()
    );
    assert!(api.specification()["paths"]["/method"]["x-http-query-operation"].is_object());
    assert_eq!(
        api.specification()["paths"]["/method"]["post"]["requestBody"]["required"],
        false
    );
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn route_ids_aliases_collections_defaults_typed_headers_and_named_problems() {
    let mut runtime = RuntimeBuilder::default();
    let lenient = EndpointRegistration::prepare(
        &mut runtime,
        contract(HttpMethod::Get, "/items/{id?}", "items", false),
        Probe::<10>::new(false),
    )
    .unwrap();
    let mut strict = contract(HttpMethod::Get, "/strict/{id}", "strict", false);
    strict.query_policy = BindingPolicy::Strict;
    strict.header_policy = BindingPolicy::Strict;
    let strict =
        EndpointRegistration::prepare(&mut runtime, strict, Probe::<11>::new(false)).unwrap();
    let wild = EndpointRegistration::prepare(
        &mut runtime,
        contract(HttpMethod::Get, "/files/{*rest}", "files", false),
        Probe::<12>::new(false),
    )
    .unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), authenticate);
    builder
        .endpoint(lenient)
        .unwrap()
        .endpoint(strict)
        .unwrap()
        .endpoint(wild)
        .unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    let client = reqwest::Client::new();
    let body = client
        .get(format!(
            "{url}/items?aliased-count=bad&values[]=1&values[]=bad&values[]=3"
        ))
        .bearer_auth("allowed")
        .header("x-count", "bad")
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(body["id"], Value::Null);
    assert_eq!(body["count"], 9);
    assert_eq!(body["values"], json!([1, 3]));
    assert_eq!(body["header"], 7);
    for path in ["/items/bad", "/strict/bad"] {
        assert_eq!(
            client
                .get(format!("{url}{path}"))
                .bearer_auth("allowed")
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
    }
    for query in ["aliased-count=bad", "values[]=1&values[]=bad"] {
        let response = client
            .get(format!("{url}/strict/3?{query}"))
            .bearer_auth("allowed")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        assert_eq!(
            response.headers()["content-type"],
            "application/problem+json"
        );
        assert!(
            response.json::<Value>().await.unwrap()["errors"]
                .as_object()
                .unwrap()
                .contains_key(if query.starts_with("values") {
                    "values[]"
                } else {
                    "aliased-count"
                })
        );
    }
    let repeated = client
        .get(format!("{url}/strict/3"))
        .bearer_auth("allowed")
        .header("x-count", "1")
        .header("x-count", "2")
        .send()
        .await
        .unwrap();
    assert_eq!(repeated.status(), 400);
    let strings = client
        .get(format!("{url}/strict/3"))
        .bearer_auth("allowed")
        .header("x-text", "a")
        .header("x-text", "b")
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(strings["raw_header"], "a,b");
    let wildcard = client
        .get(format!("{url}/files/a/b/c"))
        .bearer_auth("allowed")
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(wildcard["rest"], "a/b/c");
    let wildcard_spec = &api.specification()["paths"]["/files/{rest}"]["get"];
    assert_eq!(wildcard_spec["x-native-route-template"], "/files/{*rest}");
    assert_eq!(
        wildcard_spec["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["name"] == "rest")
            .unwrap()["x-catch-all"],
        true
    );
    let parameters = api.specification()["paths"]["/strict/{id}"]["get"]["parameters"]
        .as_array()
        .unwrap();
    assert_eq!(
        api.specification()
            .pointer(
                parameters.iter().find(|p| p["name"] == "values[]").unwrap()["schema"]["$ref"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches('#')
            )
            .unwrap()["type"],
        "array"
    );
    assert_eq!(
        api.specification()
            .pointer(
                parameters.iter().find(|p| p["name"] == "x-count").unwrap()["schema"]["$ref"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches('#')
            )
            .unwrap()["type"],
        "integer"
    );
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn return_categories_validation_and_configured_writer_negotiation_match_wire() {
    let mut runtime = RuntimeBuilder::default();
    let probe = Probe::<20>::new(false);
    let effects = Arc::clone(&probe.effects);
    let calls = Arc::clone(&probe.calls);
    let reg = EndpointRegistration::prepare(
        &mut runtime,
        contract(HttpMethod::Get, "/responses", "responses", false),
        probe,
    )
    .unwrap();
    let mut strict = contract(HttpMethod::Get, "/negotiated", "negotiated", false);
    strict.negotiation = Negotiation::Strict;
    let strict =
        EndpointRegistration::prepare(&mut runtime, strict, Probe::<21>::new(false)).unwrap();
    let mut missing = contract(HttpMethod::Query, "/missing", "missing", false);
    missing.missing = MissingResponse::NoContent;
    let missing =
        EndpointRegistration::prepare(&mut runtime, missing, Probe::<22>::new(false)).unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), authenticate);
    builder
        .endpoint(reg)
        .unwrap()
        .endpoint(strict)
        .unwrap()
        .endpoint(missing)
        .unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    let client = reqwest::Client::new();
    for (shape, status, body) in [
        ("text", 200, "plain text"),
        ("empty", 204, ""),
        ("missing", 404, ""),
        ("status", 418, ""),
        ("started", 200, "already started"),
        ("accepted", 202, ""),
    ] {
        let response = client
            .get(format!("{url}/responses?response={shape}"))
            .bearer_auth("allowed")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        if shape == "accepted" {
            assert_eq!(response.headers()["location"], "/jobs/4");
        }
        if shape == "text" {
            assert!(
                response.headers()["content-type"]
                    .to_str()
                    .unwrap()
                    .starts_with("text/plain")
            );
        }
        assert_eq!(response.text().await.unwrap(), body);
    }
    let response = client
        .get(format!("{url}/responses?response=created"))
        .bearer_auth("allowed")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    assert_eq!(response.headers()["location"], "/items/3");
    assert!(response.json::<Value>().await.unwrap().is_object());
    let denied = client
        .get(format!("{url}/responses?aliased-count=0"))
        .bearer_auth("allowed")
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 400);
    assert_eq!(calls.load(Ordering::SeqCst), 7);
    assert_eq!(effects.load(Ordering::SeqCst), 7);
    for (accept, status, expected) in [
        ("text/csv;q=0,application/custom", 200, "first,csv"),
        ("TEXT/CSV", 406, ""),
        ("*/*", 406, ""),
    ] {
        let response = client
            .get(format!("{url}/negotiated"))
            .bearer_auth("allowed")
            .header("accept", accept)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.text().await.unwrap(), expected);
    }
    assert_eq!(
        client
            .get(format!("{url}/negotiated"))
            .bearer_auth("allowed")
            .send()
            .await
            .unwrap()
            .status(),
        406
    );
    assert_eq!(
        client
            .request(
                "QUERY".parse().unwrap(),
                format!("{url}/missing?response=missing")
            )
            .bearer_auth("allowed")
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn optional_json_transport_presence_over_chunked_http1_and_real_http2() {
    let mut runtime = RuntimeBuilder::default();
    let reg = EndpointRegistration::prepare(
        &mut runtime,
        contract(HttpMethod::Post, "/body", "body", true),
        Probe::<30>::new(true),
    )
    .unwrap();
    let mut required = contract(HttpMethod::Post, "/required", "required", true);
    required.body = BodyMode::Json {
        optional: false,
        max_bytes: 1024,
    };
    let required =
        EndpointRegistration::prepare(&mut runtime, required, Probe::<31>::new(true)).unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), authenticate);
    builder.endpoint(reg).unwrap().endpoint(required).unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    for h2 in [false, true] {
        let client = if h2 {
            reqwest::Client::builder()
                .http2_prior_knowledge()
                .build()
                .unwrap()
        } else {
            reqwest::Client::builder().http1_only().build().unwrap()
        };
        let chunks = stream::iter(vec![Ok::<_, std::io::Error>("{\"chunked\":"), Ok("true}")]);
        let response = client
            .post(format!("{url}/body"))
            .bearer_auth("allowed")
            .header("content-type", "application/json")
            .body(reqwest::Body::wrap_stream(chunks))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        if h2 {
            assert_eq!(response.version(), reqwest::Version::HTTP_2);
        }
        let body = response.json::<Value>().await.unwrap();
        assert_eq!(body["body"], json!({"chunked":true}));
        assert_eq!(body["protocol"], if h2 { "HTTP/2.0" } else { "HTTP/1.1" });
        let response = client
            .post(format!("{url}/body"))
            .bearer_auth("allowed")
            .header("content-length", "0")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.json::<Value>().await.unwrap()["body"], Value::Null);
        assert_eq!(
            client
                .post(format!("{url}/required"))
                .bearer_auth("allowed")
                .header("content-length", "0")
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        assert_eq!(
            client
                .post(format!("{url}/body"))
                .bearer_auth("allowed")
                .header("content-type", "text/plain")
                .body("{}")
                .send()
                .await
                .unwrap()
                .status(),
            415
        );
        assert_eq!(
            client
                .post(format!("{url}/body"))
                .bearer_auth("allowed")
                .header("content-type", "application/json")
                .body("not json")
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    let client = reqwest::Client::builder().http1_only().build().unwrap();
    let empty = stream::empty::<Result<&str, std::io::Error>>();
    assert_eq!(
        client
            .post(format!("{url}/body"))
            .bearer_auth("allowed")
            .header("content-type", "application/json")
            .body(reqwest::Body::wrap_stream(empty))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn version_expansion_header_query_media_metadata_and_headers_on_validation() {
    let mut runtime = RuntimeBuilder::default();
    let mut config = contract(HttpMethod::Get, "/v/{version}/items", "versioned", false);
    let mut v1 = ApiVersion::new("1");
    v1.sunset = Some("Sun, 01 Nov 2026 00:00:00 GMT".into());
    v1.deprecation = Some("@1793491200".into());
    v1.link = Some("</migration>; rel=\"sunset\"".into());
    config.versioning = Some(Versioning {
        supported: vec![v1.clone(), ApiVersion::new("2")],
        mapped: vec![],
        readers: vec![VersionReader::UrlSegment],
        default: None,
        neutral: false,
    });
    let url_reg =
        EndpointRegistration::prepare(&mut runtime, config, Probe::<40>::new(false)).unwrap();
    let mut config = contract(HttpMethod::Get, "/selected", "selected", false);
    config.output_cache = Some(raddle_axum::native::OutputCachePolicy {
        store: raddle_axum::native::OutputCache::new(4, 4096).unwrap(),
        ttl: std::time::Duration::from_secs(60),
        vary_query: vec![],
        vary_headers: vec![],
        tags: vec![],
        eligibility: raddle_axum::native::CacheEligibility::AuthenticatedPartition,
    });
    config.versioning = Some(Versioning {
        supported: vec![v1, ApiVersion::new("2")],
        mapped: vec![],
        readers: vec![
            VersionReader::Header("x-api-version".into()),
            VersionReader::Query("api-version".into()),
            VersionReader::MediaType("v".into()),
        ],
        default: Some("1".into()),
        neutral: false,
    });
    let selected =
        EndpointRegistration::prepare(&mut runtime, config, Probe::<41>::new(false)).unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), authenticate);
    builder
        .endpoint(url_reg)
        .unwrap()
        .endpoint(selected)
        .unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    let client = reqwest::Client::new();
    for suffix in ["", "?aliased-count=0"] {
        let response = client
            .get(format!("{url}/v/1/items{suffix}"))
            .bearer_auth("allowed")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), if suffix.is_empty() { 200 } else { 400 });
        assert!(response.headers().contains_key("sunset"));
        assert!(response.headers().contains_key("deprecation"));
        assert!(response.headers().contains_key("link"));
    }
    let default = client
        .get(format!("{url}/selected"))
        .bearer_auth("allowed")
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(default["api_version"], "1");
    for (query, header, media) in [
        ("?api-version=2", None, None),
        ("", Some("2"), None),
        ("", None, Some("application/json; v=2")),
    ] {
        let mut request = client
            .get(format!("{url}/selected{query}"))
            .bearer_auth("allowed");
        if let Some(header) = header {
            request = request.header("x-api-version", header);
        }
        if let Some(media) = media {
            request = request.header("content-type", media);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), 200);
        assert!(!response.headers().contains_key("sunset"));
        assert_eq!(
            response.json::<Value>().await.unwrap()["api_version"],
            "2",
            "resolved versions must partition cache entries even with no vary selectors"
        );
    }
    assert_eq!(
        client
            .get(format!("{url}/selected?api-version=1"))
            .bearer_auth("allowed")
            .header("x-api-version", "2")
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert!(api.specification()["paths"]["/v/1/items"]["get"].is_object());
    assert!(api.specification()["paths"]["/v/2/items"]["get"].is_object());
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn exact_request_media_dispatch_and_bootstrap_guards_are_atomic() {
    let mut runtime = RuntimeBuilder::default();
    let mut json_contract = contract(HttpMethod::Post, "/media", "media-json", true);
    json_contract.accepts = Some("application/json".into());
    let json_reg =
        EndpointRegistration::prepare(&mut runtime, json_contract, Probe::<50>::new(true)).unwrap();
    let mut vendor_contract = contract(HttpMethod::Post, "/media", "media-vendor", true);
    vendor_contract.accepts = Some("application/vnd.example+json".into());
    let vendor_reg =
        EndpointRegistration::prepare(&mut runtime, vendor_contract, Probe::<51>::new(true))
            .unwrap();
    let mut invalid = contract(HttpMethod::Get, "/bad", "bad", true);
    assert!(
        EndpointRegistration::prepare(&mut runtime, invalid.clone(), Probe::<52>::new(true))
            .is_err()
    );
    invalid.body = BodyMode::None;
    invalid.missing = MissingResponse::NoContent;
    invalid.method = HttpMethod::Post;
    assert!(EndpointRegistration::prepare(&mut runtime, invalid, Probe::<52>::new(false)).is_err());
    let mut builder = NativeApiBuilder::new(runtime.build(), authenticate);
    builder
        .endpoint(json_reg)
        .unwrap()
        .endpoint(vendor_reg)
        .unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    let client = reqwest::Client::new();
    for (media, status) in [
        ("application/json; charset=utf-8", 200),
        ("application/vnd.example+json", 200),
        ("application/other+json", 415),
        ("*/*", 415),
    ] {
        assert_eq!(
            client
                .post(format!("{url}/media"))
                .bearer_auth("allowed")
                .header("content-type", media)
                .body("{}")
                .send()
                .await
                .unwrap()
                .status(),
            status
        );
    }
    assert_eq!(
        client
            .post(format!("{url}/media"))
            .bearer_auth("allowed")
            .send()
            .await
            .unwrap()
            .status(),
        415
    );
    assert!(api.specification()["paths"]["/media"]["post"]["requestBody"]["content"]["application/json"].is_object());
    assert!(api.specification()["paths"]["/media"]["post"]["requestBody"]["content"]["application/vnd.example+json"].is_object());
    task.abort();
    let _ = task.await;
}

#[test]
fn prefix_precedence_and_named_links_do_not_stack() {
    let mut contract = contract(HttpMethod::Get, "/items/{id?}", "items", false);
    contract.prefixes = RoutePrefixes {
        global: Some("/global".into()),
        namespace: vec![(1, "/less".into()), (4, "/specific".into())],
        endpoint: Some("/endpoint".into()),
    };
    assert_eq!(contract.link(&BTreeMap::new()).unwrap(), "/endpoint/items");
    assert_eq!(
        contract.link(&BTreeMap::from([("id", "a b")])).unwrap(),
        "/endpoint/items/a%20b"
    );
    contract.prefixes.endpoint = None;
    assert_eq!(contract.link(&BTreeMap::new()).unwrap(), "/specific/items");
    contract.prefixes.namespace.clear();
    assert_eq!(contract.link(&BTreeMap::new()).unwrap(), "/global/items");
}

struct FrozenWriters {
    config_calls: Arc<AtomicUsize>,
}
impl Endpoint for FrozenWriters {
    type Input = ();
    type Resource = Value;
    type Error = Infallible;
    async fn bind(&self, _input: &mut RequestInput) -> Result<(), Problem> {
        Ok(())
    }
    async fn handle(
        &self,
        _context: &mut EndpointContext,
        _input: (),
    ) -> Result<EndpointOutput<Value>, Infallible> {
        Ok(EndpointResponse::Json(json!({"value":1})).into())
    }
    fn writers(&self) -> Vec<ResourceWriter<Value>> {
        assert_eq!(
            self.config_calls.fetch_add(1, Ordering::SeqCst),
            0,
            "writer configuration must be frozen at preparation"
        );
        vec![ResourceWriter {
            media_type: "text/csv",
            write: |_| Ok(b"frozen".to_vec()),
        }]
    }
}

#[tokio::test]
async fn writer_registration_is_frozen_before_runtime_and_schema_share_the_same_branches() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = RuntimeBuilder::default();
    let mut contract = EndpointContract::new(HttpMethod::Get, "/frozen", "frozen");
    contract.negotiation = Negotiation::Strict;
    let registration = EndpointRegistration::prepare(
        &mut runtime,
        contract,
        FrozenWriters {
            config_calls: Arc::clone(&calls),
        },
    )
    .unwrap();
    let mut builder = NativeApiBuilder::new(runtime.build(), authenticate);
    builder.endpoint(registration).unwrap();
    let api = builder.build();
    let (url, task) = serve(&api).await;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{url}/frozen"))
        .bearer_auth("allowed")
        .header("accept", "text/csv")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-type"], "text/csv");
    assert_eq!(response.text().await.unwrap(), "frozen");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        api.specification()["paths"]["/frozen"]["get"]["responses"]["200"]["content"]["text/csv"]
            .is_object()
    );
    task.abort();
    let _ = task.await;
}
