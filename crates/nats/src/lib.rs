//! Protocol-specific NATS APIs; Core NATS is deliberately not a confirmed Transport.

mod clients;
mod config;
mod core;
mod delivery;
mod jetstream;
mod push;

pub use async_nats::jetstream::consumer::pull::Config as ConsumerConfig;
pub use async_nats::jetstream::consumer::push::Config as PushConfig;
pub use async_nats::jetstream::consumer::{DeliverPolicy, ReplayPolicy};
pub use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy, StorageType};
pub use async_nats::{ConnectOptions, HeaderMap};
pub use clients::Clients;
pub use config::{Bounds, Error, Route, Source, validate_subject};
pub use core::{Core, CoreMessage, CoreReceiver};
pub use delivery::{HeldMessage, Position};
pub use jetstream::{
    JetStream, Provision, PublishOptions, Receipt, Receiver, ScheduleOptions, ScheduleResult,
};
pub use push::{PushDelivery, PushReceiver};
