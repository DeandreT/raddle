//! Explicit topology validation and disposable local-broker routing regressions.
#![allow(clippy::unwrap_used)]

use std::error::Error;
use std::time::Duration;

use lapin::options::{ExchangeDeclareOptions, ExchangeDeleteOptions, QueueDeleteOptions};
use lapin::types::FieldTable;
use lapin::{Connection as AmqpConnection, ConnectionProperties};
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle_rabbitmq::{Binding, Connection, Exchange, ExchangeKind, Queue, Route, Topology};
use serde::{Deserialize, Serialize};

fn exchange(name: &str, kind: ExchangeKind) -> Exchange {
    Exchange {
        name: name.into(),
        kind,
        durable: true,
    }
}
fn queue(name: &str) -> Queue {
    Queue {
        name: name.into(),
        durable: true,
    }
}
fn binding(exchange: &str, queue: &str, key: &str) -> Binding {
    Binding {
        exchange: exchange.into(),
        queue: queue.into(),
        routing_key: key.into(),
    }
}

#[test]
fn validation_rejects_invalid_names_duplicates_references_and_keys() {
    for name in [
        "",
        "amq.reserved",
        "contains space",
        "bad\nname",
        "ü",
        &"a".repeat(256),
    ] {
        assert!(Topology::new([exchange(name, ExchangeKind::Direct)], [], []).is_err());
        assert!(Topology::new([], [queue(name)], []).is_err());
    }
    assert!(Topology::new([], [], []).is_err());
    assert!(
        Topology::new(
            [
                exchange("same", ExchangeKind::Direct),
                exchange("same", ExchangeKind::Topic)
            ],
            [],
            []
        )
        .is_err()
    );
    assert!(Topology::new([], [queue("same"), queue("same")], []).is_err());
    assert!(
        Topology::new(
            [exchange("e", ExchangeKind::Direct)],
            [queue("q")],
            [binding("absent", "q", "key")]
        )
        .is_err()
    );
    assert!(
        Topology::new(
            [exchange("e", ExchangeKind::Direct)],
            [queue("q")],
            [binding("e", "absent", "key")]
        )
        .is_err()
    );
    assert!(
        Topology::new(
            [exchange("e", ExchangeKind::Direct)],
            [queue("q")],
            [binding("e", "q", "key"), binding("e", "q", "key")]
        )
        .is_err()
    );
    for (kind, bad_keys) in [
        (ExchangeKind::Direct, vec!["*", "#", "bad space"]),
        (ExchangeKind::Fanout, vec!["ignored-but-misleading"]),
        (ExchangeKind::Topic, vec!["", "a..b", "foo*", "a.#b", "a."]),
    ] {
        for key in bad_keys {
            assert!(
                Topology::new(
                    [exchange("e", kind)],
                    [queue("q")],
                    [binding("e", "q", key)]
                )
                .is_err()
            );
        }
    }
    assert!(
        Topology::new(
            [exchange("e", ExchangeKind::Topic)],
            [queue("q")],
            [binding("e", "q", &"a".repeat(256))]
        )
        .is_err()
    );
    for (kind, key) in [
        (ExchangeKind::Direct, ""),
        (ExchangeKind::Fanout, ""),
        (ExchangeKind::Topic, "orders.*.#"),
    ] {
        assert!(
            Topology::new(
                [exchange("e", kind)],
                [queue("q")],
                [binding("e", "q", key)]
            )
            .is_ok()
        );
    }
}

#[test]
fn declaration_bound_is_global_and_unbounded_input_is_cut_off() {
    assert!(Topology::new([], (0..256).map(|i| queue(&format!("q-{i}"))), []).is_ok());
    assert!(
        Topology::new(
            [exchange("e", ExchangeKind::Direct)],
            (0..256).map(|i| queue(&format!("q-{i}"))),
            []
        )
        .is_err()
    );
    assert!(Topology::new([], (0..).map(|i| queue(&format!("q-{i}"))), []).is_err());
}

#[derive(Serialize, Deserialize)]
struct Probe;
impl WireMessage for Probe {
    const NAME: &'static str = "test.topology";
    const VERSION: u32 = 1;
}

#[tokio::test]
#[ignore = "requires RADDLE_RABBITMQ_TEST_URI for a disposable loopback broker"]
async fn declares_idempotently_routes_all_kinds_and_returns_unroutable()
-> Result<(), Box<dyn Error>> {
    let uri = std::env::var("RADDLE_RABBITMQ_TEST_URI")?;
    let connection = Connection::connect(&uri).await?;
    let admin = AmqpConnection::connect(&uri, ConnectionProperties::default()).await?;
    let channel = admin.create_channel().await?;
    let prefix = format!("raddle-topology-{}", uuid::Uuid::new_v4());
    let kinds = [
        ExchangeKind::Direct,
        ExchangeKind::Fanout,
        ExchangeKind::Topic,
    ];
    let exchanges: Vec<_> = kinds
        .into_iter()
        .enumerate()
        .map(|(i, kind)| exchange(&format!("{prefix}-e{i}"), kind))
        .collect();
    let queues: Vec<_> = (0..3).map(|i| queue(&format!("{prefix}-q{i}"))).collect();
    let keys = ["orders.created", "", "orders.*"];
    let bindings: Vec<_> = (0..3)
        .map(|i| binding(&exchanges[i].name, &queues[i].name, keys[i]))
        .collect();
    let topology = Topology::new(exchanges.clone(), queues.clone(), bindings)?;
    let result = async {
        connection.declare_topology(&topology).await?;
        connection.declare_topology(&topology).await?;
        let incompatible_exchange = Topology::new(
            [Exchange {
                durable: false,
                ..exchanges[0].clone()
            }],
            [],
            [],
        )?;
        if connection
            .declare_topology(&incompatible_exchange)
            .await
            .is_ok()
        {
            return Err("incompatible exchange durability was silently accepted".into());
        }
        let incompatible_queue = Topology::new(
            [],
            [Queue {
                durable: false,
                ..queues[0].clone()
            }],
            [],
        )?;
        if connection
            .declare_topology(&incompatible_queue)
            .await
            .is_ok()
        {
            return Err("incompatible queue durability was silently accepted".into());
        }
        connection.declare_topology(&topology).await?;
        for i in 0..3 {
            let publisher = connection
                .publisher([
                    Route {
                        alias: "matching".into(),
                        exchange: exchanges[i].name.clone(),
                        routing_key: "orders.created".into(),
                    },
                    Route {
                        alias: "other".into(),
                        exchange: exchanges[i].name.clone(),
                        routing_key: "inventory.created".into(),
                    },
                ])
                .await?;
            let message = Envelope::encode(&Probe)?;
            publisher.publish("matching", &message).await?;
            let mut receiver = connection.receiver(&queues[i].name, 1).await?;
            let mut delivery = receiver
                .receive(Duration::from_secs(5))
                .await?
                .ok_or("missing matched delivery")?;
            if delivery.decode()?.id() != message.id() {
                return Err("wrong routed message".into());
            }
            delivery.complete().await?;
            let other = publisher.publish("other", &message).await;
            if i == 1 {
                other?;
                let mut delivery = receiver
                    .receive(Duration::from_secs(5))
                    .await?
                    .ok_or("fanout did not route arbitrary key")?;
                delivery.complete().await?;
            } else if other != Err(TransportError::Rejected) {
                return Err("mandatory unroutable publication was not rejected".into());
            }
            if receiver.receive(Duration::from_millis(20)).await?.is_some() {
                return Err("duplicate declaration duplicated routing".into());
            }
            receiver.close().await?;
            publisher.close().await?;
        }
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    let mut cleanup_error = None;
    for queue in queues {
        if let Err(error) = channel
            .queue_delete(queue.name.into(), QueueDeleteOptions::default())
            .await
        {
            cleanup_error = Some(error);
        }
    }
    for exchange in exchanges {
        if let Err(error) = channel
            .exchange_delete(exchange.name.into(), ExchangeDeleteOptions::default())
            .await
        {
            cleanup_error = Some(error);
        }
    }
    let adapter_close = connection.close().await;
    let admin_close = admin.close(200, "finished".into()).await;
    result?;
    if let Some(error) = cleanup_error {
        return Err(error.into());
    }
    adapter_close?;
    admin_close?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires RADDLE_RABBITMQ_TEST_URI for a disposable loopback broker"]
async fn invalid_plan_has_no_broker_side_effects() -> Result<(), Box<dyn Error>> {
    let uri = std::env::var("RADDLE_RABBITMQ_TEST_URI")?;
    let name = format!("raddle-invalid-plan-{}", uuid::Uuid::new_v4());
    // An invalid late binding rejects the whole plan, not just that binding.
    assert!(
        Topology::new(
            [exchange(&name, ExchangeKind::Direct)],
            [],
            [binding(&name, "undeclared", "key")]
        )
        .is_err()
    );
    let admin = AmqpConnection::connect(&uri, ConnectionProperties::default()).await?;
    let channel = admin.create_channel().await?;
    let exists = channel
        .exchange_declare(
            name.clone().into(),
            lapin::ExchangeKind::Direct,
            ExchangeDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await;
    if exists.is_ok() {
        channel
            .exchange_delete(name.into(), ExchangeDeleteOptions::default())
            .await?;
    }
    admin.close(200, "finished".into()).await?;
    if exists.is_ok() {
        return Err("invalid topology created an exchange".into());
    }
    Ok(())
}
