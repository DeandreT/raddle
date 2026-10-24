//! Deterministic fault injection without background tasks, sleeps, or real brokers.

mod clock;
pub mod conformance;
mod transport;

pub use clock::{ClockError, ManualClock};
pub use transport::{Receipt, ScriptError, ScriptedTransport, SendOutcome};
