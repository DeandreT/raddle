//! Confirmed AMQP 1.0 publishing, peek-lock custody, and explicit session control.
//!
//! Production connections use TLS. Plaintext is available only with the explicit
//! `emulator` feature and is restricted to a numeric loopback address.

mod connection;
mod error;
mod publisher;
mod receiver;

pub use connection::{Connection, NetworkTransport};
pub use error::Error;
pub use publisher::Publisher;
pub use receiver::{HeldMessage, Receiver, Source};
