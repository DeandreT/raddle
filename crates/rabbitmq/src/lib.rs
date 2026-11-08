//! AMQP 0-9-1 with mandatory publication, publisher confirms and explicit settlement.
//!
//! This is a separate adapter from Service Bus's AMQP 1.0 protocol. The library
//! uses existing configured exchanges/queues and never provisions them implicitly.
//! Applications can explicitly declare a validated [`Topology`] before starting
//! endpoints. This does not provision cloud resources.

mod connection;
mod delivery;
mod error;
mod topology;

pub use connection::{Connection, Publisher, Receiver, Route};
pub use delivery::{HeldMessage, Requeue};
pub use error::Error;
pub use topology::{Binding, Exchange, ExchangeKind, Queue, Topology};
