//! Authenticated HTTP ingress over the existing executor and SQLx durable store.

mod api;
mod authentication;
mod error;
mod specification;

pub use api::{Accepted, Api, ApiBuilder};
pub use authentication::{Authenticated, Authenticator, Permission, Principal};
pub use error::{ApiError, ErrorBody, ErrorCode};
