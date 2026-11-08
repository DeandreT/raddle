//! Native Kafka delivery with explicit broker acceptance and custody boundaries.
//!
//! Positive production receipts are Kafka delivery reports, not merely native
//! queue admission. Kafka idempotent production does not deduplicate separate
//! application resubmissions. Durable consumers must retain original identities.

mod config;
mod consumer_config;
mod context;
mod error;
mod offsets;
mod publisher;
mod receiver;
mod record;
mod topology;

pub use config::{ClientSettings, Limits, Topic};
pub use consumer_config::{CommitStrategy, ConsumerSettings, Membership};
pub use error::Error;
pub use publisher::{Publication, Publisher};
pub use receiver::{Received, Receiver, Settlement};
pub use record::{Header, Record};
pub use topology::{Admin, TopicSpec, TopologyOutcome, TopologyResult};
