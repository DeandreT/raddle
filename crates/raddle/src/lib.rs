//! Typed, bounded, caller-owned local execution.
//!
//! Requests have one handler and a typed reply; events have named independent
//! subscriptions. No handler is spawned in the background. Local execution is
//! volatile and performs no automatic retries or persistence.
//!
//! ```
//! use std::convert::Infallible;
//!
//! use raddle::{Request, RuntimeBuilder};
//!
//! struct Double(u32);
//! impl Request for Double {
//!     type Reply = u32;
//!     type Error = Infallible;
//! }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut builder = RuntimeBuilder::default();
//! builder.handle(|request: Double| async move { Ok(request.0 * 2) })?;
//! let runtime = builder.build();
//! assert_eq!(runtime.invoke(Double(21)).await?, 42);
//! # Ok(())
//! # }
//! ```

mod builder;
mod delivery;
mod error;
mod handler;
mod message;
mod publication;
mod runtime;
mod subscription_name;
pub mod transport;
pub mod wire;

pub use builder::RuntimeBuilder;
pub use delivery::EventDelivery;
pub use error::{ConfigurationError, ExecutionError, RegistrationError};
pub use message::{Event, Request};
pub use publication::{PublishReport, SubscriptionOutcome};
pub use runtime::Runtime;
pub use subscription_name::{InvalidSubscriptionName, SubscriptionName};
