//! Authenticated, typed local requests over the standard protobuf gRPC protocol.
//!
//! Applications own listeners, TLS, credential verification and task lifetimes.
//! Only explicitly registered wire contracts reach the normal bounded runtime.

mod authenticated;
mod client;
mod error;
mod json;
mod principal;
mod registry;
mod rpc_context;
mod server;

/// Generated protobuf messages and interoperable Tonic clients/servers.
#[allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]
pub mod protocol;

pub use authenticated::Authenticated;
pub use client::GrpcClient;
pub use error::{ClientError, ConfigurationError};
pub use principal::{AuthenticationError, AuthenticationFuture, Authenticator, Principal};
pub use registry::GrpcBuilder;
pub use rpc_context::RpcContext;
pub use server::{
    GrpcService, INVOKE_PATH, MAX_HEADER_BYTES, MAX_HEADERS, MAX_JSON_BYTES, MAX_MESSAGE_BYTES,
};
