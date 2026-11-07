use std::collections::{HashMap, HashSet};

use lapin::Channel;
use lapin::options::{ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::types::FieldTable;

use crate::Error;

/// Supported application-owned exchange routing semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExchangeKind {
    /// Exact binding-key match.
    Direct,
    /// Every bound queue receives a copy; bindings use an empty key.
    Fanout,
    /// Dot-separated binding patterns with `*` and `#` wildcard segments.
    Topic,
}

/// An explicit named exchange declaration; automatic deletion is disabled.
#[derive(Clone, Debug)]
pub struct Exchange {
    /// Application-owned safe ASCII name; `amq.` is reserved.
    pub name: String,
    /// Broker routing semantics.
    pub kind: ExchangeKind,
    /// Whether the exchange survives a broker restart.
    pub durable: bool,
}

/// An explicit named shared queue; non-exclusive, without automatic deletion.
#[derive(Clone, Debug)]
pub struct Queue {
    /// Application-owned safe ASCII name; `amq.` is reserved.
    pub name: String,
    /// Whether the queue survives a broker restart. Messages must also persist.
    pub durable: bool,
}

/// Binds one declared queue to one declared exchange.
#[derive(Clone, Debug)]
pub struct Binding {
    /// Queue declared in the same topology.
    pub queue: String,
    /// Exchange declared in the same topology.
    pub exchange: String,
    /// Explicit key; empty for fanout, exact for direct, pattern for topic.
    pub routing_key: String,
}

/// Fully validated topology with at most 256 total declarations and bindings.
/// No broker side effects occur during construction. All binding references
/// must point into this topology; implicit/default exchange declarations,
/// server-generated names, arbitrary arguments, and cloud provisioning are absent.
#[derive(Clone, Debug)]
pub struct Topology {
    exchanges: Vec<Exchange>,
    queues: Vec<Queue>,
    bindings: Vec<Binding>,
}

impl Topology {
    /// Validates the entire finite plan before it can reach a broker. Iteration
    /// stops after the first over-limit element, including unbounded iterators.
    pub fn new(
        exchanges: impl IntoIterator<Item = Exchange>,
        queues: impl IntoIterator<Item = Queue>,
        bindings: impl IntoIterator<Item = Binding>,
    ) -> Result<Self, Error> {
        let exchanges: Vec<_> = exchanges.into_iter().take(257).collect();
        let queues: Vec<_> = queues.into_iter().take(257).collect();
        let bindings: Vec<_> = bindings.into_iter().take(257).collect();
        if exchanges.len() + queues.len() + bindings.len() > 256
            || exchanges.is_empty() && queues.is_empty()
        {
            return Err(Error::Invalid("invalid topology declaration count"));
        }
        let mut exchange_names = HashMap::new();
        for exchange in &exchanges {
            validate_name(&exchange.name)?;
            if exchange_names
                .insert(exchange.name.as_str(), exchange.kind)
                .is_some()
            {
                return Err(Error::Invalid("duplicate exchange"));
            }
        }
        let mut queue_names = HashSet::new();
        for queue in &queues {
            validate_name(&queue.name)?;
            if !queue_names.insert(queue.name.as_str()) {
                return Err(Error::Invalid("duplicate queue"));
            }
        }
        let mut unique_bindings = HashSet::new();
        for binding in &bindings {
            let Some(kind) = exchange_names.get(binding.exchange.as_str()) else {
                return Err(Error::Invalid("binding references undeclared exchange"));
            };
            if !queue_names.contains(binding.queue.as_str()) {
                return Err(Error::Invalid("binding references undeclared queue"));
            }
            if !unique_bindings.insert((&binding.queue, &binding.exchange, &binding.routing_key)) {
                return Err(Error::Invalid("duplicate binding"));
            }
            if binding.routing_key.len() > 255
                || !binding
                    .routing_key
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-*#".contains(&b))
                || *kind == ExchangeKind::Fanout && !binding.routing_key.is_empty()
                || *kind != ExchangeKind::Topic && binding.routing_key.contains(['*', '#'])
                || *kind == ExchangeKind::Topic
                    && (binding.routing_key.is_empty()
                        || binding.routing_key.split('.').any(|part| {
                            part.is_empty()
                                || (part.contains(['*', '#']) && part != "*" && part != "#")
                        }))
            {
                return Err(Error::Invalid("invalid binding key"));
            }
        }
        Ok(Self {
            exchanges,
            queues,
            bindings,
        })
    }

    pub(crate) async fn declare(&self, channel: &Channel) -> Result<(), Error> {
        for exchange in &self.exchanges {
            let kind = match exchange.kind {
                ExchangeKind::Direct => lapin::ExchangeKind::Direct,
                ExchangeKind::Fanout => lapin::ExchangeKind::Fanout,
                ExchangeKind::Topic => lapin::ExchangeKind::Topic,
            };
            channel
                .exchange_declare(
                    exchange.name.clone().into(),
                    kind,
                    ExchangeDeclareOptions {
                        durable: exchange.durable,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|_| Error::Broker)?;
        }
        for queue in &self.queues {
            channel
                .queue_declare(
                    queue.name.clone().into(),
                    QueueDeclareOptions {
                        durable: queue.durable,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|_| Error::Broker)?;
        }
        for binding in &self.bindings {
            channel
                .queue_bind(
                    binding.queue.clone().into(),
                    binding.exchange.clone().into(),
                    binding.routing_key.clone().into(),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|_| Error::Broker)?;
        }
        Ok(())
    }
}

fn validate_name(name: &str) -> Result<(), Error> {
    if name.is_empty()
        || name.len() > 255
        || name.starts_with("amq.")
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return Err(Error::Invalid("invalid topology name"));
    }
    Ok(())
}
