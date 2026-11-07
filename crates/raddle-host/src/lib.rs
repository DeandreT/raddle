//! Caller-polled application supervision with bounded restart and joined shutdown.
//!
//! Nothing starts until [`Host::run`] is polled. Services explicitly report their
//! own readiness; task creation is never evidence of dependency readiness.

mod host;
mod postgres;
mod service;

pub use host::{Host, HostError, HostHandle, Snapshot};
pub use postgres::{PollSettings, postgres_outbox, postgres_worker};
pub use service::{
    PanicPolicy, RestartPolicy, ServiceContext, ServiceFailure, ServiceState, Status,
};
