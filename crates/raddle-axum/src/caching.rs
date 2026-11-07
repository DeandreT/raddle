use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use sha2::{Digest, Sha256};

use crate::{ApiError, ErrorCode};

pub(crate) async fn conditional(response: Response, condition: Option<HeaderValue>) -> Response {
    if response.status() != StatusCode::OK
        || response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            != Some("application/json")
    {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let bytes = match tokio::time::timeout(
        Duration::from_secs(2),
        axum::body::to_bytes(body, 1_048_576),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        _ => {
            return ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::Internal,
                "conditional response exceeds configured bounds",
            )
            .into_response();
        }
    };
    let digest = Sha256::digest(&bytes);
    let tag = format!("\"{digest:x}\"");
    let Ok(value) = HeaderValue::from_str(&tag) else {
        return ApiError::unavailable().into_response();
    };
    parts.headers.insert(header::ETAG, value);
    // Every reuse must revalidate through authentication and the application handler.
    parts.headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-cache"),
    );
    parts
        .headers
        .append(header::VARY, HeaderValue::from_static("Authorization"));
    let unchanged = condition
        .and_then(|h| h.to_str().ok().map(str::to_owned))
        .is_some_and(|condition| {
            condition.split(',').any(|candidate| {
                let candidate = candidate.trim();
                candidate == "*" || candidate.strip_prefix("W/").unwrap_or(candidate) == tag
            })
        });
    if unchanged {
        parts.status = StatusCode::NOT_MODIFIED;
        parts.headers.remove(header::CONTENT_TYPE);
        parts.headers.remove(header::CONTENT_LENGTH);
        parts.headers.remove(header::TRANSFER_ENCODING);
        Response::from_parts(parts, Body::empty())
    } else {
        Response::from_parts(parts, Body::from(bytes))
    }
}
