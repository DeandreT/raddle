use axum::extract::rejection::{JsonRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Extension, Json};
use raddle::{ExecutionError, Request};
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::{ApiBuilder, ApiError, Authenticated, ErrorCode, Permission, Principal, specification};

/// A documented scalar header supplied by the caller, not an authenticated identity.
#[derive(Clone, Copy, Debug)]
pub struct HeaderParameter {
    /// Case-insensitive HTTP header name. Authorization is managed by the authenticator.
    pub name: &'static str,
    /// Whether the binder must receive this header.
    pub required: bool,
}

/// Explicit binding of path, query, headers and JSON into an application request.
///
/// The authenticated principal remains separate in `Authenticated<Self>`; never
/// derive tenant ownership from untrusted path, query or header fields.
pub trait HttpRequest: Request + Sized {
    /// Named scalar path fields matching the route's template exactly.
    type Path: DeserializeOwned + JsonSchema + Send + 'static;
    /// Named scalar query fields; Serde controls defaults and required fields.
    type Query: DeserializeOwned + JsonSchema + Send + 'static;
    /// JSON body for POST, or `()` for a body-free GET.
    type Body: DeserializeOwned + JsonSchema + Send + 'static;
    /// Extra scalar header inputs to document and check before invoking the binder.
    const HEADERS: &'static [HeaderParameter] = &[];
    /// Binds and validates application inputs. Errors must not include private values.
    fn bind(
        path: Self::Path,
        query: Self::Query,
        headers: &HeaderMap,
        body: Self::Body,
    ) -> Result<Self, ApiError>;
}

impl ApiBuilder {
    /// Registers a typed body-free GET with path, query and header binding.
    pub fn query<M>(&mut self, path: &str) -> Result<&mut Self, ApiError>
    where
        M: HttpRequest<Body = ()>,
        M::Reply: Serialize + JsonSchema,
    {
        self.bind_operation::<M>(path, "get", false)?;
        let runtime = self.runtime.clone();
        self.router = self.router.clone().route(
            path,
            get(
                move |Extension(principal): Extension<Principal>,
                      path: Result<Path<M::Path>, PathRejection>,
                      query: Result<Query<M::Query>, QueryRejection>,
                      headers: HeaderMap| {
                    let runtime = runtime.clone();
                    async move {
                        principal.require(Permission::Invoke)?;
                        let Path(path) = path.map_err(|_| ApiError::invalid())?;
                        let Query(query) = query.map_err(|_| ApiError::invalid())?;
                        check_headers::<M>(&headers)?;
                        let message = M::bind(path, query, &headers, ())?;
                        json_reply(runtime.invoke(Authenticated::new(principal, message)).await)
                    }
                },
            ),
        );
        Ok(self)
    }

    /// Registers a POST binding named path/query fields, headers and a typed JSON body.
    pub fn bound_request<M>(&mut self, path: &str) -> Result<&mut Self, ApiError>
    where
        M: HttpRequest,
        M::Reply: Serialize + JsonSchema,
    {
        self.bind_operation::<M>(path, "post", true)?;
        let runtime = self.runtime.clone();
        self.router = self.router.clone().route(
            path,
            post(
                move |Extension(principal): Extension<Principal>,
                      path: Result<Path<M::Path>, PathRejection>,
                      query: Result<Query<M::Query>, QueryRejection>,
                      headers: HeaderMap,
                      body: Result<Json<M::Body>, JsonRejection>| {
                    let runtime = runtime.clone();
                    async move {
                        principal.require(Permission::Invoke)?;
                        let Path(path) = path.map_err(|_| ApiError::invalid())?;
                        let Query(query) = query.map_err(|_| ApiError::invalid())?;
                        let Json(body) = body.map_err(crate::api::json_error)?;
                        check_headers::<M>(&headers)?;
                        let message = M::bind(path, query, &headers, body)?;
                        json_reply(runtime.invoke(Authenticated::new(principal, message)).await)
                    }
                },
            ),
        );
        Ok(self)
    }

    fn bind_operation<M: HttpRequest>(
        &mut self,
        path: &str,
        method: &str,
        body: bool,
    ) -> Result<(), ApiError>
    where
        M::Reply: JsonSchema,
    {
        // Validate in temporary schema storage: a rejected registration changes nothing.
        let mut schemas = self.schemas.clone();
        let index = self.registered.len() + 1;
        let path_schema = specification::schema::<M::Path>(
            &format!("Path{index}"),
            Contract::Deserialize,
            &mut schemas,
        );
        let query_schema = specification::schema::<M::Query>(
            &format!("Query{index}"),
            Contract::Deserialize,
            &mut schemas,
        );
        let mut parameters = parameters(&path_schema, &schemas, "path")?;
        let captures = captures(path)?;
        let names: std::collections::HashSet<_> = parameters
            .iter()
            .filter_map(|v| v["name"].as_str())
            .collect();
        if names != captures.iter().map(String::as_str).collect() {
            return Err(ApiError::invalid());
        }
        parameters.extend(parameters_for_query(&query_schema, &schemas)?);
        let mut headers = std::collections::HashSet::new();
        for header in M::HEADERS {
            let name =
                axum::http::HeaderName::try_from(header.name).map_err(|_| ApiError::invalid())?;
            if name == axum::http::header::AUTHORIZATION || !headers.insert(name.clone()) {
                return Err(ApiError::invalid());
            }
            parameters.push(json!({"name":name.as_str(),"in":"header","required":header.required,"schema":{"type":"string"}}));
        }
        let request = if body {
            Some(specification::schema::<M::Body>(
                &format!("Body{index}"),
                Contract::Deserialize,
                &mut schemas,
            ))
        } else {
            None
        };
        let response = specification::schema::<M::Reply>(
            &format!("Reply{index}"),
            Contract::Serialize,
            &mut schemas,
        );
        self.reserve_template(path, method)?;
        let mut operation = specification::operation(method, index, request, response, 200);
        operation["parameters"] = Value::Array(parameters);
        self.paths
            .entry(path.to_owned())
            .or_insert_with(|| json!({}))[method] = operation;
        self.schemas = schemas;
        Ok(())
    }

    pub(crate) fn reserve_template(&mut self, path: &str, method: &str) -> Result<(), ApiError> {
        captures(path)?;
        if path == "/openapi.json"
            || self
                .registered
                .iter()
                .any(|(registered, _)| registered != path && template_conflict(registered, path))
            || !self.registered.insert((path.to_owned(), method.to_owned()))
        {
            return Err(ApiError::invalid());
        }
        Ok(())
    }
}

fn template_conflict(left: &str, right: &str) -> bool {
    // Matchit remaps capture names on distinct sibling routes. Only complete
    // equivalent patterns conflict; a shared wildcard prefix is not enough.
    let shape = |path: &str| {
        path.split('/')
            .map(|segment| {
                if segment.starts_with('{') {
                    "{}"
                } else {
                    segment
                }
            })
            .collect::<Vec<_>>()
            .join("/")
    };
    shape(left) == shape(right)
}

fn captures(path: &str) -> Result<Vec<String>, ApiError> {
    if !path.starts_with('/') || path.len() > 120 || path.contains("//") {
        return Err(ApiError::invalid());
    }
    let mut captures = Vec::new();
    for segment in path.split('/').skip(1) {
        let name = segment.strip_prefix('{').and_then(|s| s.strip_suffix('}'));
        let value = name.unwrap_or(segment);
        if !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
            || name.is_some_and(|s| s.is_empty() || s.starts_with('*'))
        {
            return Err(ApiError::invalid());
        }
        if let Some(name) = name {
            if captures.iter().any(|s| s == name) {
                return Err(ApiError::invalid());
            }
            captures.push(name.to_owned());
        }
    }
    Ok(captures)
}

fn parameters(
    reference: &Value,
    schemas: &serde_json::Map<String, Value>,
    location: &str,
) -> Result<Vec<Value>, ApiError> {
    let name = reference["$ref"]
        .as_str()
        .and_then(|s| s.strip_prefix("#/components/schemas/"))
        .ok_or_else(ApiError::invalid)?;
    let schema = &schemas[name];
    let empty = serde_json::Map::new();
    let properties = match schema["properties"].as_object() {
        Some(properties) => properties,
        None if schema["type"] == "object"
            && schema["additionalProperties"].as_bool() != Some(true) =>
        {
            &empty
        }
        None => return Err(ApiError::invalid()),
    };
    let required = schema["required"].as_array();
    properties.iter().map(|(name, schema)| {
        // Axum path/query binding accepts scalar values, not nested JSON objects.
        let valid_type = |kind: &str| matches!(kind, "string" | "integer" | "number" | "boolean" | "null");
        let valid = match &schema["type"] {
            Value::String(kind) => valid_type(kind),
            Value::Array(kinds) => kinds.iter().all(|v| v.as_str().is_some_and(valid_type)),
            _ => false,
        };
        if !valid { return Err(ApiError::invalid()); }
        Ok(json!({"name":name,"in":location,"required":location=="path" || required.is_some_and(|a| a.contains(&Value::String(name.clone()))),"schema":schema}))
    }).collect()
}

pub(crate) fn parameters_for_query(
    reference: &Value,
    schemas: &serde_json::Map<String, Value>,
) -> Result<Vec<Value>, ApiError> {
    parameters(reference, schemas, "query")
}

fn check_headers<M: HttpRequest>(headers: &HeaderMap) -> Result<(), ApiError> {
    for header in M::HEADERS {
        if header.required && !headers.contains_key(header.name) {
            return Err(ApiError::invalid());
        }
    }
    Ok(())
}

pub(crate) fn json_reply<R: Serialize, E>(
    result: Result<R, ExecutionError<E>>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    let reply = result.map_err(execution_error)?;
    let bytes = serde_json::to_vec(&reply).map_err(|_| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::Internal,
            "internal response failure",
        )
    })?;
    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        bytes,
    ))
}

pub(crate) fn execution_error<E>(error: ExecutionError<E>) -> ApiError {
    match error {
        ExecutionError::Handler(_) => ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            ErrorCode::Rejected,
            "command rejected",
        ),
        ExecutionError::Closed | ExecutionError::Unregistered { .. } => ApiError::unavailable(),
        _ => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::Internal,
            "internal execution failure",
        ),
    }
}
