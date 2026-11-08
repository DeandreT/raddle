use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use super::Problem;

/// Null-resource policy; selected explicitly by endpoint scope.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum MissingResponse {
    /// Empty 404.
    #[default]
    NotFound,
    /// Empty 204, permitted only for GET/QUERY at registration.
    NoContent,
}

/// Released writer selection semantics, not RFC Accept preference negotiation.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub enum Negotiation {
    /// Fall back to JSON if no configured writer's media type appears in Accept.
    #[default]
    Loose,
    /// Return empty 406 if no configured writer matches.
    Strict,
}

/// Declared return alternatives used by startup checks and OpenAPI.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResponseKind {
    /// JSON 200 resource.
    Json,
    /// text/plain 200.
    Text,
    /// An explicit status without body (documented as a default response).
    Status,
    /// Empty 204.
    Empty,
    /// Missing resource (404 or GET/QUERY-only 204).
    Missing,
    /// 201 with optional JSON resource and Location.
    Created,
    /// 202 with optional JSON resource and Location.
    Accepted,
    /// Application-owned already-started/custom response; requires explicit metadata.
    Started,
}

/// A typed response, never an accidentally serialized tuple or status integer.
#[non_exhaustive]
pub enum EndpointResponse<R> {
    /// JSON resource, or a configured writer selected from Accept.
    Json(R),
    /// A plain text response.
    Text(String),
    /// An explicit body-free status.
    Status(StatusCode),
    /// No resource: 204, not empty 200.
    Empty,
    /// Null resource using the endpoint's missing policy.
    Missing,
    /// 201 and Location, with an optional resource.
    Created {
        /// Created resource, when any.
        resource: Option<R>,
        /// Validated Location header value.
        location: String,
    },
    /// 202 and Location, with an optional resource.
    Accepted {
        /// Accepted resource, when any.
        resource: Option<R>,
        /// Validated Location header value.
        location: String,
    },
    /// Preserve the application's status/body, including an explicitly started 200.
    /// No replay support is implied; arbitrary streams remain application-owned.
    Started(Response),
}

/// An explicitly configured custom resource writer. Earlier entries win.
pub struct ResourceWriter<R> {
    /// Exact case-sensitive response media type used in Accept substring matching.
    pub media_type: &'static str,
    /// Serialization function, evaluated before any transaction commit.
    pub write: fn(&R) -> Result<Vec<u8>, Problem>,
}

impl<R> EndpointResponse<R> {
    pub(crate) fn kind(&self) -> ResponseKind {
        match self {
            Self::Json(_) => ResponseKind::Json,
            Self::Text(_) => ResponseKind::Text,
            Self::Status(_) => ResponseKind::Status,
            Self::Empty => ResponseKind::Empty,
            Self::Missing => ResponseKind::Missing,
            Self::Created { .. } => ResponseKind::Created,
            Self::Accepted { .. } => ResponseKind::Accepted,
            Self::Started(_) => ResponseKind::Started,
        }
    }
}

pub(crate) struct Rendered {
    pub response: Response,
    pub replayable: bool,
}

impl<R: Serialize> EndpointResponse<R> {
    pub(crate) fn render(
        self,
        headers: &HeaderMap,
        missing: MissingResponse,
        negotiation: Negotiation,
        writers: &[ResourceWriter<R>],
    ) -> Result<Rendered, Problem> {
        let mut replayable = true;
        let response = match self {
            Self::Json(resource) => {
                let accept = headers
                    .get(header::ACCEPT)
                    .and_then(|header| header.to_str().ok())
                    .unwrap_or("");
                if let Some(writer) = writers
                    .iter()
                    .find(|writer| accept.contains(writer.media_type))
                {
                    replayable = false;
                    (
                        [(header::CONTENT_TYPE, writer.media_type)],
                        (writer.write)(&resource)?,
                    )
                        .into_response()
                } else if matches!(negotiation, Negotiation::Strict) && !writers.is_empty() {
                    StatusCode::NOT_ACCEPTABLE.into_response()
                } else {
                    json_response(StatusCode::OK, Some(resource), None)?
                }
            }
            Self::Text(text) => {
                replayable = false;
                ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], text).into_response()
            }
            Self::Status(status) => status.into_response(),
            Self::Empty => StatusCode::NO_CONTENT.into_response(),
            Self::Missing => match missing {
                MissingResponse::NotFound => StatusCode::NOT_FOUND,
                MissingResponse::NoContent => StatusCode::NO_CONTENT,
            }
            .into_response(),
            Self::Created { resource, location } => {
                json_response(StatusCode::CREATED, resource, Some(location))?
            }
            Self::Accepted { resource, location } => {
                json_response(StatusCode::ACCEPTED, resource, Some(location))?
            }
            Self::Started(response) => {
                replayable = false;
                response
            }
        };
        Ok(Rendered {
            response,
            replayable,
        })
    }
}

fn json_response<R: Serialize>(
    status: StatusCode,
    resource: Option<R>,
    location: Option<String>,
) -> Result<Response, Problem> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    if let Some(resource) = resource {
        *response.body_mut() =
            Body::from(serde_json::to_vec(&resource).map_err(|_| Problem::internal())?);
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
    }
    if let Some(location) = location {
        response.headers_mut().insert(
            header::LOCATION,
            HeaderValue::try_from(location).map_err(|_| Problem::internal())?,
        );
    }
    Ok(response)
}
