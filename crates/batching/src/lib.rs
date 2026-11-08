//! Caller-driven receive-side aggregation, with explicit original-member custody.
//!
//! No tasks, clocks, handlers or transport acknowledgements are started implicitly.
//! The optional `postgres` feature adds fenced execution over canonical inbox jobs.

mod assembly;
mod observation;
mod recovery;
mod routing;

#[cfg(feature = "postgres")]
pub mod postgres;

pub use assembly::{
    AdmissionError, Assembler, Batch, BatchKey, BatchView, DecodedBatch, Expired, Member,
    MemberContext, Options,
};
pub use observation::{PendingSnapshot, Registry, TypePending};
pub use recovery::{Decision, Resolution, ResolutionError};
pub use routing::{Combination, Diagnostic, ExecutionQueue, RoutePlan, RoutingError};
