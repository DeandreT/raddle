//! Durable inbox/outbox for embedded single-node deployments.
//!
//! SQLite serializes writers. Execution transactions use `BEGIN IMMEDIATE`,
//! rather than pretending to have PostgreSQL's independent row-level locks.

mod model;
mod scope;
mod store;
mod worker;

pub use model::{Claim, Delivery, DeliveryKind, Error, FailureDisposition, Job};
pub use scope::Scope;
pub use store::Store;
pub use worker::{HandlerFuture, OutboxWorker, Worker};
