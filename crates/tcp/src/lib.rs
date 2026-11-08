//! Native TCP framing with explicit receiver custody and caller-owned lifetimes.
//!
//! Raw TCP provides neither encryption nor peer authentication. Only use this
//! adapter on an independently secured network. Its wire format is raddle-native,
//! not the Wolverine wire protocol. Acknowledgements attest only to the receiver's
//! declared custody, not durable storage unless its consumer actually commits it.

mod codec;
mod config;
mod framing;
mod publisher;
mod receiver;

pub use config::{Endpoint, Error, Limits};
pub use publisher::{Publisher, Receipt};
pub use receiver::{Consumer, Delivery, Peer, Receiver};
