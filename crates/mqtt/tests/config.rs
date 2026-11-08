//! Configuration checks open no sockets and start no event-loop worker.
#![allow(clippy::unwrap_used)]
use raddle_mqtt::{Bounds, Clients, MqttOptions, Publisher, QoS, Route, Session, Source};
use std::time::Duration;

fn session(id: &str, tenant: &str) -> Session {
    Session::new(
        MqttOptions::new(id, "127.0.0.1", 1883),
        Source {
            client: "fixture".into(),
            tenant: tenant.into(),
            endpoint: "orders".into(),
        },
        vec![Route {
            alias: "orders".into(),
            topic: format!("{tenant}/orders"),
        }],
        Bounds::default(),
    )
    .unwrap()
}
#[test]
fn named_clients_bind_source_and_never_reuse_a_protocol_id() {
    assert!(
        Clients::new(vec![
            ("fixture".into(), "a".into(), session("duplicate", "a")),
            ("fixture".into(), "b".into(), session("duplicate", "b"))
        ])
        .is_err()
    );
    assert!(Clients::new(vec![("other".into(), "a".into(), session("a", "a"))]).is_err());
    let mut clients = Clients::new(vec![
        ("fixture".into(), "a".into(), session("a", "a")),
        ("fixture".into(), "b".into(), session("b", "b")),
    ])
    .unwrap();
    assert_eq!(clients.get_mut("fixture", "a").unwrap().client_id(), "a");
    assert_eq!(clients.get_mut("fixture", "b").unwrap().client_id(), "b");
    assert!(clients.get_mut("fixture", "untrusted").is_err());
}
#[test]
fn qos0_can_never_construct_a_confirmed_transport() {
    assert!(
        Publisher::new(
            session("publisher", "a"),
            QoS::AtMostOnce,
            Duration::from_secs(1)
        )
        .is_err()
    );
}
#[test]
fn nonloopback_plaintext_is_refused_without_opening_a_socket() {
    let result = Session::new(
        MqttOptions::new("id", "broker.example", 1883),
        Source {
            client: "fixture".into(),
            tenant: "a".into(),
            endpoint: "orders".into(),
        },
        vec![Route {
            alias: "orders".into(),
            topic: "orders".into(),
        }],
        Bounds::default(),
    );
    assert!(result.is_err());
}
