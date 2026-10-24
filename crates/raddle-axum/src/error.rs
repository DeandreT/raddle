use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use schemars::JsonSchema;
use serde::Serialize;

/// Stable HTTP failure codes; exception strings and payloads are never returned.
#[derive(Clone, Copy, Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorCode {
    /// Invalid JSON, headers, or arguments.
    InvalidRequest,
    /// No valid authenticated identity.
    Unauthorized,
    /// Identity lacks permission.
    Forbidden,
    /// Missing resource or route.
    NotFound,
    /// HTTP method is not registered.
    MethodNotAllowed,
    /// Idempotency key conflicts with earlier input.
    Conflict,
    /// Body exceeds the configured limit.
    PayloadTooLarge,
    /// Request is not JSON.
    UnsupportedMediaType,
    /// Application rejected the command.
    Rejected,
    /// Runtime, queue, or backend is unavailable.
    Unavailable,
    /// Unexpected server failure.
    Internal,
}

/// JSON error contract shared by all routes, middleware and fallbacks.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ErrorBody {
    /// Stable machine-readable classification.
    pub code: ErrorCode,
    /// Safe, fixed human-readable explanation.
    pub message: &'static str,
}

/// An HTTP error with a bounded, non-sensitive response body.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    body: ErrorBody,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, code: ErrorCode, message: &'static str) -> Self {
        Self {
            status,
            body: ErrorBody { code, message },
        }
    }
    /// Authentication failed. Do not include credentials in the response.
    pub fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            ErrorCode::Unauthorized,
            "authentication required",
        )
    }
    /// The caller has no authority for this operation.
    pub fn forbidden() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            ErrorCode::Forbidden,
            "permission denied",
        )
    }
    pub(crate) fn invalid() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            "invalid request",
        )
    }
    pub(crate) fn unavailable() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Unavailable,
            "service unavailable",
        )
    }
    pub(crate) fn not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            ErrorCode::NotFound,
            "resource not found",
        )
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.body.message)
    }
}
impl std::error::Error for ApiError {}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (self.status, Json(self.body)).into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}

impl From<raddle_postgres::Error> for ApiError {
    fn from(error: raddle_postgres::Error) -> Self {
        match error {
            raddle_postgres::Error::IdentityConflict => Self::new(
                StatusCode::CONFLICT,
                ErrorCode::Conflict,
                "idempotency key was used with different input",
            ),
            raddle_postgres::Error::Invalid(_) | raddle_postgres::Error::Wire(_) => Self::invalid(),
            _ => Self::unavailable(),
        }
    }
}
