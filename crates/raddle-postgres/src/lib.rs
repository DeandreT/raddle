//! SQLx transaction participation and durable at-least-once execution.

mod error;
mod job;
mod scope;
mod store;
mod worker;

pub use error::Error;
pub use job::{Claim, Delivery, DeliveryKind, FailureDisposition, Job, JobStatus, RetryPolicy};
pub use scope::Scope;
pub use store::Store;
pub use worker::{HandlerFailure, HandlerFuture, OutboxWorker, Worker};
