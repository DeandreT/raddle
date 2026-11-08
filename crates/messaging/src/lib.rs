//! Application messaging with explicit trust, task ownership and transaction boundaries.
//!
//! The low-level `raddle::Runtime` remains caller-polled and does not interpret
//! returned effects. This crate opts into managed application behavior explicitly.

mod bus;
pub mod composition;
mod context;
mod durable;
pub mod effects;
pub mod handler_routing;
mod ingress;
mod queue;
mod request_reply;
pub mod scoped;

pub use bus::ScopedRegistration;
pub use bus::{BusBuilder, BusError, InvokeError, InvokeOptions, MessageBus, RemoteInvoker};
pub use bus::{InputStream, LocalStream, StreamError, StreamOptions};
pub use context::{Cancellation, HeaderError, MessageContext, TrustedOrigin};
pub use durable::{DurableControl, DurableEndpoint};
pub use ingress::{IngressDecoder, IngressError, JsonIngress};
pub use queue::{LocalQueue, QueueError, QueueMode, QueueRunner, QueueSettings, QueueSnapshot};
pub use request_reply::{RemoteResponder, ReplyHubInvoker};

/// Boxed only at a reusable asynchronous extension boundary, never through `Any`.
pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
