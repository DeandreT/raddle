//! Opt-in failure handling with explicit retry ownership and endpoint custody.
//!
//! This crate does not change legacy `Runtime` or PostgreSQL workers. The
//! `raddle-messaging` opt-in managed policy queues and durable endpoints bind
//! these drivers to actual application execution with explicit replay safety.

mod circuit;
mod driver;
mod fault;
mod filter;
mod memory;
mod policy;
mod quota;
mod terminal;
mod unknown;

pub use circuit::{CircuitAdmission, CircuitBreaker, CircuitOptions};
pub use driver::{
    DriverError, Execution, ExecutionMode, PolicyExecutor, PolicyExecutorDriver, Transition,
};
pub use fault::{
    AttemptCountersDto, ExceptionNode, FailureDisposition, FailureOrigin, Fault, FaultOptions,
    UnknownMessageOptions,
};
pub use filter::{ErrorFilter, PolicyError};
pub use memory::{InMemoryEndpoint, MemoryDelivery, MemoryStatus, ReplaySafety};
pub use policy::{
    Action, AttemptCounters, Direction, Jitter, Plan, PolicyErrorCode, PolicySet, Rule, Step, Tail,
};
pub use quota::{Quota, QuotaAdmission, QuotaKind, QuotaPermit, ScopedQuotas};
pub use terminal::{FaultPublication, TerminalFaultPublisher};
pub use unknown::{
    MissingHandlerFuture, UnknownMessageHandler, UnknownMessageReport, UnknownRecord,
};
