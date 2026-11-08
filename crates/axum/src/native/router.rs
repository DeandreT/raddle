use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use axum::body::{Body, HttpBody};
use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use futures_util::future::BoxFuture;
use raddle::{ExecutionError, Runtime};
use schemars::generate::Contract as SchemaContract;
use serde_json::{Map, Value, json};

use super::endpoint::NativeCall;
use super::{
    BodyMode, Endpoint, EndpointContract, EndpointRegistration, HttpMethod, MissingResponse,
    Problem, ResponseKind, VersionReader,
};
use crate::{Api, Authenticator};

type Action = Arc<
    dyn Fn(Request, Option<String>) -> BoxFuture<'static, Result<Response, Problem>> + Send + Sync,
>;
struct Entry {
    contract: Arc<EndpointContract>,
    version: Option<String>,
    action: Action,
}

/// Native registration router and OpenAPI producer; never creates another runtime.
/// All per-request application callbacks run inside the prepared core admission permit.
pub struct NativeApiBuilder {
    runtime: Runtime,
    auth: Arc<dyn Authenticator>,
    routes: BTreeMap<String, Vec<Entry>>,
    names: HashSet<String>,
    paths: Map<String, Value>,
    schemas: Map<String, Value>,
    cookie_auth: bool,
}

impl NativeApiBuilder {
    /// Uses the runtime into which `EndpointRegistration::prepare` registered.
    /// Publishes bearer authentication metadata; use `cookie_sessions` for the
    /// native cookie-only verifier and matching cookie metadata/challenges.
    pub fn new(runtime: Runtime, auth: impl Authenticator) -> Self {
        let mut schemas = Map::new();
        crate::specification::schema::<Problem>("Problem", SchemaContract::Serialize, &mut schemas);
        Self {
            runtime,
            auth: Arc::new(auth),
            routes: BTreeMap::new(),
            names: HashSet::new(),
            paths: Map::new(),
            schemas,
            cookie_auth: false,
        }
    }

    /// Uses the actual native cookie-session verifier and publishes cookie security
    /// metadata. A cookie-only 401 does not advertise a nonexistent bearer scheme.
    pub fn cookie_sessions(runtime: Runtime, sessions: super::CookieSessions) -> Self {
        let mut builder = Self::new(runtime, sessions);
        builder.cookie_auth = true;
        builder
    }

    /// Mounts prepared routes with deterministic duplicate/conflict rejection.
    /// The router and schema are updated together, never partially on failure.
    pub fn endpoint<E: Endpoint>(
        &mut self,
        registration: EndpointRegistration<E>,
    ) -> Result<&mut Self, Problem> {
        let EndpointRegistration {
            endpoint,
            contract,
            writers,
        } = registration;
        let paths = contract.paths()?;
        let url_reader = contract.versioning.as_ref().is_some_and(|config| {
            !config.neutral
                && config
                    .readers
                    .iter()
                    .any(|reader| matches!(reader, VersionReader::UrlSegment))
        });
        let versions: Vec<Option<String>> = match &contract.versioning {
            Some(config) if !config.neutral => config.values().into_iter().map(Some).collect(),
            _ => vec![None],
        };
        let mut pending = Vec::new();
        for path in paths {
            for version in &versions {
                let path = if url_reader {
                    path.replace("{version}", version.as_deref().unwrap_or(""))
                } else {
                    path.clone()
                };
                pending.push((path, version.clone()));
            }
        }
        if self.names.contains(&contract.name) {
            return Err(Problem::invalid("duplicate route name"));
        }
        // Validate with Axum's pinned matcher before invoking its panic-on-invalid
        // route API. Optional expansion/catch-all conflicts must be ordinary
        // configuration errors, not partially mounted routes or startup panics.
        let mut matcher = matchit::Router::new();
        for path in self.routes.keys() {
            matcher
                .insert(path, ())
                .map_err(|_| Problem::invalid("route template conflict"))?;
        }
        let mut new_paths = HashSet::new();
        for (path, _) in &pending {
            if !self.routes.contains_key(path) && new_paths.insert(path) {
                matcher
                    .insert(path, ())
                    .map_err(|_| Problem::invalid("route template conflict"))?;
            }
        }
        for (path, version) in &pending {
            for (existing, entries) in &self.routes {
                if existing != path && shape(existing) == shape(path) {
                    return Err(Problem::invalid("conflicting route template"));
                }
                if existing == path
                    && entries.iter().any(|entry| {
                        entry.contract.method == contract.method
                            && (entry.contract.anonymous != contract.anonymous
                                || entry.contract.parameters != contract.parameters
                                || matches!(entry.contract.csrf, super::CsrfPolicy::Session(_))
                                    != matches!(contract.csrf, super::CsrfPolicy::Session(_)))
                    })
                {
                    return Err(Problem::invalid(
                        "shared operation binding/security metadata must agree",
                    ));
                }
                if existing == path
                    && entries.iter().any(|entry| {
                        entry.contract.method == contract.method
                            && ((entry.contract.accepts == contract.accepts
                                && (&entry.version == version
                                    || entry.version.is_none()
                                    || version.is_none()))
                                || entry.contract.accepts.is_none() != contract.accepts.is_none())
                    })
                {
                    return Err(Problem::invalid("ambiguous route/method/media/version"));
                }
            }
        }
        let mut schemas = self.schemas.clone();
        let operation = operation(
            endpoint.as_ref(),
            &contract,
            self.cookie_auth,
            &mut schemas,
            writers.as_slice(),
        );
        let runtime = self.runtime.clone();
        let auth = Arc::clone(&self.auth);
        let action_contract = Arc::clone(&contract);
        let cookie_auth = self.cookie_auth;
        let action: Action = Arc::new(move |request, url_version| {
            let (runtime, auth, contract, endpoint, writers) = (
                runtime.clone(),
                Arc::clone(&auth),
                Arc::clone(&action_contract),
                Arc::clone(&endpoint),
                Arc::clone(&writers),
            );
            Box::pin(async move {
                let mut response = runtime
                    .invoke(NativeCall {
                        endpoint,
                        contract,
                        request,
                        auth,
                        url_version,
                        writers,
                    })
                    .await
                    .map_err(|error| match error {
                        ExecutionError::Handler(problem) => problem,
                        ExecutionError::Closed | ExecutionError::Unregistered { .. } => {
                            Problem::new(StatusCode::SERVICE_UNAVAILABLE)
                        }
                        _ => Problem::internal(),
                    })?;
                if cookie_auth && response.status() == StatusCode::UNAUTHORIZED {
                    response.headers_mut().remove(header::WWW_AUTHENTICATE);
                }
                Ok(response)
            })
        });
        for (path, version) in pending {
            let mut operation = operation.clone();
            operation["x-native-route-template"] = json!(path);
            // Optional route expansion removes absent path members from metadata.
            if let Some(parameters) = operation["parameters"].as_array_mut() {
                parameters.retain(|parameter| {
                    parameter["in"] != "path"
                        || parameter["name"].as_str().is_some_and(|name| {
                            path.contains(&format!("{{{name}}}"))
                                || path.contains(&format!("{{*{name}}}"))
                        })
                });
                for parameter in parameters.iter_mut() {
                    if parameter["in"] == "path"
                        && parameter["name"]
                            .as_str()
                            .is_some_and(|name| path.contains(&format!("{{*{name}}}")))
                    {
                        parameter["x-catch-all"] = json!(true);
                    }
                }
            }
            if let Some(version) = &version
                && url_reader
            {
                operation["x-api-version"] = json!(version);
                operation["operationId"] = json!(format!("{}.v{}", contract.name, version));
            }
            let key = if contract.method == HttpMethod::Query {
                "x-http-query-operation".into()
            } else {
                contract.method.as_str().to_ascii_lowercase()
            };
            let path_item = self
                .paths
                .entry(path.replace("{*", "{"))
                .or_insert_with(|| json!({}));
            if path_item.get(&key).is_some() {
                merge_operation(&mut path_item[&key], operation);
            } else {
                path_item[&key] = operation;
            }
            self.routes.entry(path).or_default().push(Entry {
                contract: Arc::clone(&contract),
                version,
                action: Arc::clone(&action),
            });
        }
        self.names.insert(contract.name.clone());
        self.schemas = schemas;
        Ok(self)
    }

    /// Produces routes and an offline OpenAPI 3.1 document without binding sockets
    /// or connecting to databases. QUERY is carried as an explicit extension.
    pub fn build(mut self) -> Api {
        let mut router = Router::new();
        for (path, entries) in self.routes {
            let entries = Arc::new(entries);
            router = router.route(
                &path,
                any(move |request: Request| {
                    let entries = Arc::clone(&entries);
                    async move { dispatch(entries, request).await }
                }),
            );
        }
        self.paths.insert("/openapi.json".into(),json!({"get":{"operationId":"openapi","responses":{"200":{"description":"Offline generated contract","content":{"application/json":{"schema":{"type":"object"}}}}}},"head":{"operationId":"openapi-head","responses":{"200":{"description":"Contract headers without a body"}}}}));
        let security_schemes = if self.cookie_auth {
            json!({"sessionCookie":{"type":"apiKey","in":"cookie","name":"raddle-session"}})
        } else {
            json!({"applicationAuth":{"type":"http","scheme":"bearer"}})
        };
        let specification = json!({"openapi":"3.1.0","info":{"title":"raddle native HTTP contracts","version":"1.0.0"},"paths":self.paths,"components":{"schemas":self.schemas,"securitySchemes":security_schemes}});
        let spec = specification.clone();
        router = router
            .route(
                "/openapi.json",
                get(move || {
                    let spec = spec.clone();
                    async move { Json(spec) }
                }),
            )
            .fallback(|| async { Problem::new(StatusCode::NOT_FOUND) });
        Api {
            router,
            specification,
        }
    }
}

async fn dispatch(entries: Arc<Vec<Entry>>, request: Request) -> Response {
    let head = request.method() == axum::http::Method::HEAD;
    let mut matching: Vec<_> = entries
        .iter()
        .filter(|entry| entry.contract.method.matches(request.method()))
        .collect();
    let result = if matching.is_empty() {
        let mut response = Problem::new(StatusCode::METHOD_NOT_ALLOWED).into_response();
        let mut methods: Vec<_> = entries
            .iter()
            .map(|entry| entry.contract.method.as_str())
            .collect();
        methods.sort_unstable();
        methods.dedup();
        if let Ok(allow) = HeaderValue::try_from(methods.join(", ")) {
            response.headers_mut().insert(header::ALLOW, allow);
        }
        return response;
    } else {
        let media = super::input::media_type(request.headers());
        // Explicit media registrations have precedence over an unspecified fallback.
        let has_media = matching
            .iter()
            .any(|entry| entry.contract.accepts.is_some());
        if has_media {
            matching.retain(|entry| {
                entry.contract.accepts.as_deref() == media
                    && !entry
                        .contract
                        .accepts
                        .as_deref()
                        .is_some_and(|media| media.contains('*'))
            });
        }
        if matching.is_empty() {
            Err(Problem::new(StatusCode::UNSUPPORTED_MEDIA_TYPE))
        } else {
            let mut selection = None;
            let mut error = None;
            for entry in matching {
                let selected = match &entry.contract.versioning {
                    Some(config) => super::version::select(
                        config,
                        request.uri(),
                        request.headers(),
                        entry.version.as_deref().filter(|_| {
                            config
                                .readers
                                .iter()
                                .any(|reader| matches!(reader, VersionReader::UrlSegment))
                        }),
                    ),
                    None => Ok(None),
                };
                match selected {
                    Ok(version) if entry.version == version => {
                        selection = Some(entry);
                        break;
                    }
                    Ok(_) => {}
                    Err(problem) => error = Some(problem),
                }
            }
            match selection {
                Some(entry) => (entry.action)(request, entry.version.clone()).await,
                None => Err(error.unwrap_or_else(|| Problem::invalid("api version"))),
            }
        }
    };
    let mut response = match result {
        Ok(response) => response,
        Err(problem) => problem.into_response(),
    };
    if head {
        if response.status() != StatusCode::NO_CONTENT
            && !response.headers().contains_key(header::CONTENT_LENGTH)
            && let Some(length) = response.body().size_hint().exact()
            && let Ok(value) = HeaderValue::try_from(length.to_string())
        {
            response.headers_mut().insert(header::CONTENT_LENGTH, value);
        }
        *response.body_mut() = Body::empty();
    }
    response
}

fn operation<E: Endpoint>(
    endpoint: &E,
    contract: &EndpointContract,
    cookie_auth: bool,
    schemas: &mut Map<String, Value>,
    writers: &[super::ResourceWriter<E::Resource>],
) -> Value {
    let schema = crate::specification::schema::<E::Resource>(
        &format!("Resource_{}", contract.name),
        SchemaContract::Serialize,
        schemas,
    );
    let body_schema = crate::specification::value_schema(
        endpoint.body_schema(),
        &format!("Body_{}", contract.name),
        schemas,
    );
    let mut responses = Map::new();
    for code in [400, 401, 403, 404, 405, 409, 413, 415, 422, 429, 500, 503] {
        responses.insert(code.to_string(),json!({"description":StatusCode::from_u16(code).ok().and_then(|status|status.canonical_reason()).unwrap_or("Problem"),"content":{"application/problem+json":{"schema":{"$ref":"#/components/schemas/Problem"}}}}));
    }
    if !cookie_auth {
        responses["401"]["headers"] = json!({"WWW-Authenticate":{"schema":{"type":"string"}}});
    }
    responses["405"]["headers"] = json!({"Allow":{"schema":{"type":"string"}}});
    for kind in &contract.responses {
        let (code, content) = match kind {
            ResponseKind::Json => ("200", Some(json!({"application/json":{"schema":schema}}))),
            ResponseKind::Text => (
                "200",
                Some(json!({"text/plain":{"schema":{"type":"string"}}})),
            ),
            ResponseKind::Empty => ("204", None),
            ResponseKind::Missing => (
                if contract.missing == MissingResponse::NoContent {
                    "204"
                } else {
                    "404"
                },
                None,
            ),
            ResponseKind::Created => ("201", Some(json!({"application/json":{"schema":schema}}))),
            ResponseKind::Accepted => ("202", Some(json!({"application/json":{"schema":schema}}))),
            ResponseKind::Status | ResponseKind::Started => ("default", None),
        };
        let value = responses
            .entry(code)
            .or_insert_with(|| json!({"description":"Endpoint response"}));
        if let Some(content) = content {
            if !value["content"].is_object() {
                value["content"] = json!({});
            }
            for (media, schema) in content.as_object().into_iter().flatten() {
                value["content"][media] = schema.clone();
            }
        }
        if matches!(kind, ResponseKind::Created | ResponseKind::Accepted) {
            value["headers"] = json!({"Location":{"schema":{"type":"string"}}});
        }
    }
    for writer in writers {
        responses["200"]["content"][writer.media_type] = json!({"schema":{"type":"string"}});
    }
    if matches!(contract.negotiation, super::Negotiation::Strict) {
        responses.insert(
            "406".into(),
            json!({"description":"No configured writer matched Accept"}),
        );
    }
    for (code, metadata) in &contract.response_metadata {
        responses.insert(code.clone(), metadata.clone());
    }
    if let Some(cache) = &contract.response_cache {
        for response in responses.values_mut() {
            if !response["headers"].is_object() {
                response["headers"] = json!({});
            }
            response["headers"]["Cache-Control"] = json!({"schema":{"type":"string"}});
            if !cache.vary.is_empty() {
                response["headers"]["Vary"] = json!({"schema":{"type":"string"}});
            }
        }
    }
    let mut parameters: Vec<_> = contract
        .parameters
        .iter()
        .filter(|p| p.source != super::ParameterSource::Form)
        .enumerate()
        .map(|(index, p)| {
            let mut metadata = p.metadata();
            metadata["schema"] = crate::specification::value_schema(
                p.schema.clone(),
                &format!("Parameter_{}_{index}", contract.name),
                schemas,
            );
            metadata
        })
        .collect();
    if contract.replay.is_some() {
        parameters.push(json!({"name":"Idempotency-Key","in":"header","required":contract.replay.as_ref().is_some_and(|policy|policy.required),"schema":{"type":"string","minLength":1,"maxLength":128}}));
    }
    if matches!(contract.csrf, super::CsrfPolicy::Session(_)) {
        parameters.push(
            json!({"name":"X-CSRF-Token","in":"header","required":true,"schema":{"type":"string"}}),
        );
    }
    if let Some(config) = &contract.versioning {
        for reader in &config.readers {
            let (name, location) = match reader {
                VersionReader::Query(name) => (name, "query"),
                VersionReader::Header(name) => (name, "header"),
                _ => continue,
            };
            parameters.push(json!({"name":name,"in":location,"required":false,"schema":{"type":"string","enum":config.values()}}));
        }
        for (_, response) in responses.iter_mut() {
            if !response["headers"].is_object() {
                response["headers"] = json!({});
            }
            for name in ["api-supported-versions", "sunset", "deprecation", "link"] {
                response["headers"][name] = json!({"schema":{"type":"string"}});
            }
        }
    }
    let mut operation = json!({"operationId":contract.name,"x-route-name":contract.name,"parameters":parameters,"responses":responses,"security":if contract.anonymous {json!([])}else if cookie_auth {json!([{"sessionCookie":[]}])}else{json!([{"applicationAuth":[]}])}});
    if let Some(version) = &contract.versioning {
        operation["x-api-versions"] = json!(version.values());
        operation["x-api-version-neutral"] = json!(version.neutral);
    }
    let body = match &contract.body {
        BodyMode::None => None,
        BodyMode::Json { optional, .. } => Some(
            json!({"required":!optional,"content":{contract.accepts.as_deref().unwrap_or("application/json"):{"schema":if *optional {json!({"anyOf":[body_schema,{"type":"null"}]})}else{body_schema.clone()}}}}),
        ),
        BodyMode::Form(_) => Some(
            json!({"required":true,"content":{"application/x-www-form-urlencoded":{"schema":body_schema},"multipart/form-data":{"schema":body_schema}}}),
        ),
        BodyMode::Multipart(_) => {
            Some(json!({"required":true,"content":{"multipart/form-data":{"schema":body_schema}}}))
        }
    };
    if let Some(body) = body {
        operation["requestBody"] = body;
    }
    if let Some(cache) = &contract.response_cache {
        operation["x-response-cache"] = json!({"maxAge":cache.max_age.map(|age|age.as_secs()),"public":cache.public,"vary":cache.vary});
    }
    if let Some(policy) = &contract.rate {
        operation["x-rate-policy"] = json!(policy.name);
    }
    if contract.method == HttpMethod::Head
        && let Some(responses) = operation["responses"].as_object_mut()
    {
        for response in responses.values_mut() {
            if let Some(response) = response.as_object_mut() {
                response.remove("content");
            }
        }
    }
    operation
}

fn shape(path: &str) -> String {
    path.split('/')
        .map(|segment| {
            if segment.starts_with("{*") {
                "{*}"
            } else if segment.starts_with('{') {
                "{}"
            } else {
                segment
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn merge_operation(existing: &mut Value, other: Value) {
    if let Some(parameters) = existing["parameters"].as_array_mut() {
        let other = other["parameters"].as_array().cloned().unwrap_or_default();
        for parameter in parameters.iter_mut() {
            if let Some(next) = other
                .iter()
                .find(|next| next["in"] == parameter["in"] && next["name"] == parameter["name"])
            {
                parameter["required"] =
                    json!(parameter["required"] == true && next["required"] == true);
                if parameter["schema"] != next["schema"] {
                    parameter["schema"] = json!({"anyOf":[parameter["schema"],next["schema"]]});
                }
            } else {
                parameter["required"] = json!(false);
            }
        }
        for mut parameter in other {
            if !parameters.iter().any(|current| {
                current["in"] == parameter["in"] && current["name"] == parameter["name"]
            }) {
                parameter["required"] = json!(false);
                parameters.push(parameter);
            }
        }
    }
    if let Some(responses) = other["responses"].as_object() {
        for (status, response) in responses {
            if existing["responses"][status].is_null() {
                existing["responses"][status] = response.clone();
            } else {
                merge_representation(&mut existing["responses"][status], response);
            }
        }
    }
    if other["requestBody"].is_object() {
        if existing["requestBody"].is_null() {
            existing["requestBody"] = other["requestBody"].clone();
        } else {
            existing["requestBody"]["required"] = json!(
                existing["requestBody"]["required"] == true
                    && other["requestBody"]["required"] == true
            );
            merge_representation(&mut existing["requestBody"], &other["requestBody"]);
        }
    }
    if let Some(versions) = existing["x-api-versions"].as_array().cloned() {
        let mut versions = versions;
        for version in other["x-api-versions"].as_array().into_iter().flatten() {
            if !versions.contains(version) {
                versions.push(version.clone());
            }
        }
        existing["x-api-versions"] = json!(versions);
    }
}

fn merge_representation(existing: &mut Value, other: &Value) {
    if let Some(content) = other["content"].as_object() {
        if !existing["content"].is_object() {
            existing["content"] = json!({});
        }
        for (media, representation) in content {
            if existing["content"][media].is_null() {
                existing["content"][media] = representation.clone();
            } else if existing["content"][media]["schema"] != representation["schema"] {
                let old = existing["content"][media]["schema"].clone();
                existing["content"][media]["schema"] =
                    json!({"anyOf":[old,representation["schema"]]});
            }
        }
    }
    if let Some(headers) = other["headers"].as_object() {
        if !existing["headers"].is_object() {
            existing["headers"] = json!({});
        }
        for (name, metadata) in headers {
            existing["headers"][name] = metadata.clone();
        }
    }
}
