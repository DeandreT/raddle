use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::Utc;
use raddle::wire::{Envelope, WireMessage};
use raddle::{ExecutionError, Request, Runtime};
use raddle_postgres::{Delivery, DeliveryKind, Store};
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::specification;
use crate::{ApiError, Authenticated, Authenticator, ErrorBody, ErrorCode, Permission, Principal};

struct Shared {
    auth: Arc<dyn Authenticator>,
    requests: AtomicU64,
    failures: AtomicU64,
}

/// Durable acceptance response. Acceptance does not mean handler completion.
#[derive(Debug, Serialize, JsonSchema)]
pub struct Accepted {
    /// Original message identity, retained across equivalent idempotent submissions.
    pub message_id: Uuid,
}

/// Router and OpenAPI document created together from the same registrations.
pub struct Api {
    router: Router,
    specification: Value,
}

impl Api {
    /// Clones the Axum service handle, not its application state.
    pub fn router(&self) -> Router {
        self.router.clone()
    }
    /// Returns the generated OpenAPI 3.1 contract.
    pub fn specification(&self) -> &Value {
        &self.specification
    }
}

/// Registers authenticated JSON ingress and its matching OpenAPI operations.
pub struct ApiBuilder {
    router: Router,
    runtime: Runtime,
    shared: Arc<Shared>,
    paths: Map<String, Value>,
    schemas: Map<String, Value>,
    registered: HashSet<(String, String)>,
}

impl ApiBuilder {
    /// Requires application-supplied bearer validation; there is no anonymous default.
    pub fn new(runtime: Runtime, authenticator: impl Authenticator) -> Self {
        let mut schemas = Map::new();
        specification::schema::<ErrorBody>("Error", &mut schemas);
        Self {
            router: Router::new(),
            runtime,
            shared: Arc::new(Shared {
                auth: Arc::new(authenticator),
                requests: AtomicU64::new(0),
                failures: AtomicU64::new(0),
            }),
            paths: Map::new(),
            schemas,
            registered: HashSet::new(),
        }
    }

    fn reserve(&mut self, path: &str, method: &str) -> Result<usize, ApiError> {
        if !path.starts_with('/')
            || path.len() > 120
            || path.contains("//")
            || !path.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.')
            })
            || path == "/openapi.json"
        {
            return Err(ApiError::invalid());
        }
        if !self.registered.insert((path.into(), method.into())) {
            return Err(ApiError::invalid());
        }
        Ok(self.registered.len())
    }

    /// Mounts a typed request through `Runtime::invoke`, including trusted identity.
    /// Applications register a handler for `Authenticated<M>` in that runtime.
    pub fn request<M>(&mut self, path: &str) -> Result<&mut Self, ApiError>
    where
        M: Request + DeserializeOwned + JsonSchema,
        M::Reply: Serialize + JsonSchema,
    {
        let index = self.reserve(path, "post")?;
        let request = specification::schema::<M>(&format!("Request{index}"), &mut self.schemas);
        let response =
            specification::schema::<M::Reply>(&format!("Reply{index}"), &mut self.schemas);
        self.paths
            .entry(path.to_owned())
            .or_insert_with(|| json!({}))["post"] =
            specification::operation("post", index, Some(request), response, 200);
        let runtime = self.runtime.clone();
        let route = post(
            move |Extension(principal): Extension<Principal>,
                  body: Result<Json<M>, JsonRejection>| {
                let runtime = runtime.clone();
                async move {
                    principal.require(Permission::Invoke)?;
                    let Json(message) = body.map_err(json_error)?;
                    runtime
                        .invoke(Authenticated::new(principal, message))
                        .await
                        .map(Json)
                        .map_err(|error| match error {
                            ExecutionError::Handler(_) => ApiError::new(
                                StatusCode::UNPROCESSABLE_ENTITY,
                                ErrorCode::Rejected,
                                "command rejected",
                            ),
                            ExecutionError::Closed | ExecutionError::Unregistered { .. } => {
                                ApiError::unavailable()
                            }
                            _ => ApiError::new(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                ErrorCode::Internal,
                                "internal execution failure",
                            ),
                        })
                }
            },
        );
        self.router = self.router.clone().route(path, route);
        Ok(self)
    }

    /// Mounts idempotent durable acceptance with one configured local subscription.
    pub fn durable<M: WireMessage + JsonSchema>(
        &mut self,
        path: &str,
        store: Store,
        subscription: &str,
    ) -> Result<&mut Self, ApiError> {
        raddle::SubscriptionName::try_new(subscription).map_err(|_| ApiError::invalid())?;
        let index = self.reserve(path, "post")?;
        let request = specification::schema::<M>(&format!("Request{index}"), &mut self.schemas);
        let response = specification::schema::<Accepted>("Accepted", &mut self.schemas);
        let mut operation = specification::operation("post", index, Some(request), response, 202);
        operation["parameters"] = json!([{ "in": "header", "name": "Idempotency-Key", "required": true, "schema": { "type": "string", "minLength": 1, "maxLength": 128 }, "description": "Tenant- and route-scoped idempotency key" }]);
        self.paths
            .entry(path.to_owned())
            .or_insert_with(|| json!({}))["post"] = operation;
        let source = format!("http:{path}");
        let subscription = subscription.to_owned();
        let route = post(
            move |Extension(principal): Extension<Principal>,
                  headers: HeaderMap,
                  body: Result<Json<M>, JsonRejection>| {
                let (store, source, subscription) =
                    (store.clone(), source.clone(), subscription.clone());
                async move {
                    principal.require(Permission::Invoke)?;
                    let key = headers
                        .get("Idempotency-Key")
                        .and_then(|value| value.to_str().ok())
                        .ok_or_else(ApiError::invalid)?;
                    let Json(message) = body.map_err(json_error)?;
                    let envelope = Envelope::encode(&message).map_err(|_| ApiError::invalid())?;
                    let mut scope = store.begin().await?;
                    let id = scope
                        .stage_idempotent(
                            &source,
                            principal.tenant(),
                            key,
                            &envelope,
                            &[Delivery::new(
                                DeliveryKind::Local,
                                &subscription,
                                Utc::now(),
                            )],
                        )
                        .await?;
                    scope.commit(Utc::now()).await?;
                    Ok::<_, ApiError>((StatusCode::ACCEPTED, Json(Accepted { message_id: id })))
                }
            },
        );
        self.router = self.router.clone().route(path, route);
        Ok(self)
    }

    /// Adds explicitly authorized operational inspection, replay, and aggregate counters.
    pub fn operations(&mut self, prefix: &str, store: Store) -> Result<&mut Self, ApiError> {
        if prefix.ends_with('/') {
            return Err(ApiError::invalid());
        }
        self.reserve(prefix, "operations")?;
        self.registered
            .remove(&(prefix.to_owned(), "operations".to_owned()));
        let inspect_path = format!("{prefix}/jobs/{{id}}");
        let replay_path = format!("{prefix}/jobs/{{id}}/replay");
        let metrics_path = format!("{prefix}/metrics");
        let keys = [
            (inspect_path.clone(), "get".to_owned()),
            (replay_path.clone(), "post".to_owned()),
            (metrics_path.clone(), "get".to_owned()),
        ];
        if keys.iter().any(|key| self.registered.contains(key)) {
            return Err(ApiError::invalid());
        }
        let index = self.registered.len() + 1;
        self.registered.extend(keys);
        let view = specification::schema::<JobView>("Job", &mut self.schemas);
        let request = specification::schema::<ReplayRequest>("ReplayRequest", &mut self.schemas);
        let response = specification::schema::<ReplayResponse>("ReplayResponse", &mut self.schemas);
        let parameter = json!([{ "name":"id", "in":"path", "required":true, "schema":{"type":"string","format":"uuid"} }]);
        let mut inspect_op = specification::operation("get", index, None, view, 200);
        inspect_op["parameters"] = parameter.clone();
        self.paths
            .entry(inspect_path.clone())
            .or_insert_with(|| json!({}))["get"] = inspect_op;
        let mut replay_op =
            specification::operation("post", index + 1, Some(request), response, 200);
        replay_op["parameters"] = parameter;
        self.paths
            .entry(replay_path.clone())
            .or_insert_with(|| json!({}))["post"] = replay_op;
        let mut metrics_op =
            specification::operation("get", index + 2, None, json!({"type":"string"}), 200);
        metrics_op["responses"]["200"]["content"] =
            json!({"text/plain":{"schema":{"type":"string"}}});
        self.paths
            .entry(metrics_path.clone())
            .or_insert_with(|| json!({}))["get"] = metrics_op;
        let inspect_store = store.clone();
        self.router = self
            .router
            .clone()
            .route(
                &inspect_path,
                get(
                    move |Extension(principal): Extension<Principal>,
                          path: Result<Path<String>, PathRejection>| {
                        let store = inspect_store.clone();
                        async move {
                            principal.require(Permission::Inspect)?;
                            let Path(id) = path.map_err(|_| ApiError::invalid())?;
                            let id = id.parse::<Uuid>().map_err(|_| ApiError::invalid())?;
                            let job = store
                                .inspect(principal.tenant(), id)
                                .await?
                                .ok_or_else(ApiError::not_found)?;
                            Ok::<_, ApiError>(Json(JobView {
                                id: job.id,
                                message_id: job.message_id,
                                status: format!("{:?}", job.status).to_lowercase(),
                                attempts: job.attempts,
                            }))
                        }
                    },
                ),
            )
            .route(
                &replay_path,
                post(
                    move |Extension(principal): Extension<Principal>,
                          path: Result<Path<String>, PathRejection>,
                          body: Result<Json<ReplayRequest>, JsonRejection>| {
                        let store = store.clone();
                        async move {
                            principal.require(Permission::Replay)?;
                            let Path(id) = path.map_err(|_| ApiError::invalid())?;
                            let id = id.parse::<Uuid>().map_err(|_| ApiError::invalid())?;
                            let Json(request) = body.map_err(json_error)?;
                            if !store
                                .replay(
                                    principal.tenant(),
                                    id,
                                    principal.subject(),
                                    &request.reason,
                                    Utc::now(),
                                )
                                .await?
                            {
                                return Err(ApiError::new(
                                    StatusCode::CONFLICT,
                                    ErrorCode::Conflict,
                                    "job is not replayable",
                                ));
                            }
                            Ok(Json(ReplayResponse { replayed: true }))
                        }
                    },
                ),
            );
        let shared = Arc::clone(&self.shared);
        self.router = self.router.clone().route(
            &metrics_path,
            get(move |Extension(principal): Extension<Principal>| {
                let shared = Arc::clone(&shared);
                async move {
                    principal.require(Permission::Inspect)?;
                    Ok::<_, ApiError>((
                        [(header::CONTENT_TYPE, "text/plain")],
                        format!(
                            "raddle_http_requests_total {}\nraddle_http_failures_total {}\n",
                            shared.requests.load(Ordering::Relaxed),
                            shared.failures.load(Ordering::Relaxed)
                        ),
                    ))
                }
            }),
        );
        Ok(self)
    }

    /// Freezes routes and their specification. No listening socket is opened automatically.
    pub fn build(mut self) -> Api {
        self.paths.insert(
            "/openapi.json".into(),
            json!({"get":specification::operation("get",0,None,json!({"type":"object"}),200)}),
        );
        let document = json!({"openapi":"3.1.0","info":{"title":"raddle API","version":"1.0.0"},"paths":self.paths,"components":{"schemas":self.schemas,"securitySchemes":{"bearerAuth":{"type":"http","scheme":"bearer"}}}});
        let spec = document.clone();
        let router = self
            .router
            .route(
                "/openapi.json",
                get(move || {
                    let spec = spec.clone();
                    async move { Json(spec) }
                }),
            )
            .fallback(|| async { ApiError::not_found() })
            .method_not_allowed_fallback(|| async {
                ApiError::new(
                    StatusCode::METHOD_NOT_ALLOWED,
                    ErrorCode::MethodNotAllowed,
                    "method not allowed",
                )
            })
            .layer(DefaultBodyLimit::max(1_048_576))
            .layer(middleware::from_fn_with_state(self.shared, authenticate));
        Api {
            router,
            specification: document,
        }
    }
}

#[derive(Serialize, JsonSchema)]
struct JobView {
    id: Uuid,
    message_id: Uuid,
    status: String,
    attempts: u32,
}
#[derive(Deserialize, JsonSchema)]
struct ReplayRequest {
    reason: String,
}
#[derive(Serialize, JsonSchema)]
struct ReplayResponse {
    replayed: bool,
}

fn json_error(error: JsonRejection) -> ApiError {
    match error.status() {
        StatusCode::PAYLOAD_TOO_LARGE => ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::PayloadTooLarge,
            "payload too large",
        ),
        StatusCode::UNSUPPORTED_MEDIA_TYPE => ApiError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ErrorCode::UnsupportedMediaType,
            "JSON content type required",
        ),
        _ => ApiError::invalid(),
    }
}

async fn authenticate(
    State(shared): State<Arc<Shared>>,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    let started = std::time::Instant::now();
    shared.requests.fetch_add(1, Ordering::Relaxed);
    let identity = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .filter(|value| {
            value
                .strip_prefix("Bearer ")
                .is_some_and(|token| !token.is_empty())
        });
    let principal = if identity.is_some() {
        shared.auth.authenticate(request.headers())
    } else {
        Err(ApiError::unauthorized())
    };
    let response = match principal {
        Ok(principal) => {
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Err(_) => ApiError::unauthorized().into_response(),
    };
    if response.status().is_client_error() || response.status().is_server_error() {
        shared.failures.fetch_add(1, Ordering::Relaxed);
    }
    tracing::debug!(
        status = response.status().as_u16(),
        elapsed_micros = started.elapsed().as_micros() as u64,
        "HTTP operation completed"
    );
    response
}
