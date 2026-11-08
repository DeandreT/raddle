use std::collections::BTreeMap;
use std::fmt;

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

/// Safe RFC 9457 problem details. Do not put secrets or exception text in details.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct Problem {
    /// Stable problem type URI; `about:blank` uses the HTTP status meaning.
    #[serde(rename = "type")]
    pub kind: Box<str>,
    /// Short safe title.
    pub title: Box<str>,
    /// HTTP status code, identical to the response status.
    pub status: u16,
    /// Safe description, when explicitly supplied by the application.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Named validation errors, never the rejected private values.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub errors: BTreeMap<String, Vec<String>>,
    /// Application-defined safe extensions.
    #[serde(flatten)]
    pub extensions: BTreeMap<String, Value>,
}

impl Problem {
    /// Creates a safe problem using the standard status reason.
    pub fn new(status: StatusCode) -> Self {
        Self {
            kind: "about:blank".into(),
            title: status.canonical_reason().unwrap_or("HTTP failure").into(),
            status: status.as_u16(),
            detail: None,
            errors: BTreeMap::new(),
            extensions: BTreeMap::new(),
        }
    }

    /// Adds a named validation failure without including its rejected value.
    pub fn invalid(name: &str) -> Self {
        let mut problem = Self::new(StatusCode::BAD_REQUEST);
        problem
            .errors
            .insert(name.into(), vec!["Invalid value".into()]);
        problem
    }

    /// Adds an application-selected safe detail.
    pub fn detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub(crate) fn internal() -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR)
    }
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.status, self.title)
    }
}

impl std::error::Error for Problem {}

impl IntoResponse for Problem {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status)
            .ok()
            .filter(|status| status.is_client_error() || status.is_server_error())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        // Normalize public mutable fields so the wire status cannot disagree.
        let mut problem = self;
        problem.status = status.as_u16();
        problem.extensions.retain(|name, _| {
            !matches!(
                name.as_str(),
                "type" | "title" | "status" | "detail" | "errors"
            )
        });
        let body = serde_json::to_vec(&problem).unwrap_or_default();
        let mut response = (
            status,
            [(header::CONTENT_TYPE, "application/problem+json")],
            body,
        )
            .into_response();
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                axum::http::HeaderValue::from_static("Bearer"),
            );
        }
        response
    }
}

/// An explicit continuation sentinel; an empty error document is not a sentinel.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Validation {
    /// Continue to the endpoint.
    NoProblems,
    /// Stop before handler/effects and write this problem.
    Reject(Problem),
}
