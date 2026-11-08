//! Controlled wire peers exercise SDK-buffered events, not simulated adapter queues.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::{Event, MqttOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::Session;
use crate::{Bounds, QoS, Route, Source};

async fn packet(socket: &mut TcpStream) -> (u8, Vec<u8>) {
    let header = socket.read_u8().await.unwrap();
    let mut remaining = 0usize;
    let mut multiplier = 1;
    for _ in 0..4 {
        let byte = socket.read_u8().await.unwrap();
        remaining += usize::from(byte & 127) * multiplier;
        if byte & 128 == 0 {
            break;
        }
        multiplier *= 128;
    }
    assert!(remaining <= 1024);
    let mut body = vec![0; remaining];
    socket.read_exact(&mut body).await.unwrap();
    (header, body)
}

async fn subscribed_packet_one(listener: &TcpListener, payload: &[u8]) -> TcpStream {
    let (mut socket, _) = listener.accept().await.unwrap();
    assert_eq!(packet(&mut socket).await.0 >> 4, 1);
    // Both connections explicitly start without broker session continuity.
    socket.write_all(&[0x20, 3, 0, 0, 0]).await.unwrap();
    let (header, subscribe) = packet(&mut socket).await;
    assert_eq!(header, 0x82);
    let mut bytes = vec![0x90, 4, subscribe[0], subscribe[1], 0, 1];
    let mut publish = vec![0, 6];
    publish.extend_from_slice(b"orders");
    publish.extend_from_slice(&[0, 1, 0]); // QoS1 packet ID 1, no properties
    publish.extend_from_slice(payload);
    assert!(publish.len() < 128);
    bytes.extend_from_slice(&[0x32, publish.len() as u8]);
    bytes.extend_from_slice(&publish);
    // One write puts SubAck followed by Publish in the SDK's bulk-read path.
    socket.write_all(&bytes).await.unwrap();
    socket
}

#[tokio::test]
async fn credential_refresh_discards_coalesced_sdk_publish_before_packet_id_reuse() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (check_ack, check_ack_rx) = tokio::sync::oneshot::channel();
        let (no_stale_ack, no_stale_ack_rx) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut before = subscribed_packet_one(&listener, b"before-refresh").await;
            let mut byte = [0];
            match before.read(&mut byte).await {
                Ok(0) => {},
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {},
                _ => panic!("pre-refresh unread publication was acknowledged"),
            }
            let mut fresh = subscribed_packet_one(&listener, b"fresh-work").await;
            check_ack_rx.await.unwrap();
            assert!(tokio::time::timeout(Duration::from_millis(100), packet(&mut fresh)).await.is_err(),
                "fresh unprocessed packet was acknowledged by stale state");
            no_stale_ack.send(()).unwrap();
            let (header, ack) = packet(&mut fresh).await;
            assert_eq!(header, 0x40);
            assert_eq!(&ack[..2], &[0, 1]);
        });
        let mut options = MqttOptions::new("refresh-client", "127.0.0.1", port);
        options.set_clean_start(false).set_session_expiry_interval(Some(60));
        let mut session = Session::new(options, Source {
            client: "peer".into(), tenant: "trusted".into(), endpoint: "orders".into(),
        }, vec![Route { alias: "orders".into(), topic: "orders".into() }], Bounds {
            timeout: Duration::from_secs(3), prefetch: 4, ..Bounds::default()
        }).unwrap();
        session.subscribe("orders", QoS::AtLeastOnce).await.unwrap();
        assert!(session.incoming.is_empty());
        assert_eq!(session.outstanding.load(std::sync::atomic::Ordering::Relaxed), 0);
        // Prove the old payload is really in rumqttc's queue when the adapter's
        // drain check passes; lack of TCP coalescing must not yield a false pass.
        assert!(session.eventloop.state.events.iter().any(|event|
            matches!(event, Event::Incoming(Packet::Publish(p)) if p.pkid == 1 && p.payload.as_ref() == b"before-refresh")));
        session.refresh_credentials("test-user".into(), "test-token".into(), None).unwrap();
        assert!(session.eventloop.state.events.is_empty());
        assert!(session.eventloop.pending.is_empty());
        assert_eq!(session.client_id(), "refresh-client");
        assert!(!session.eventloop.options.clean_start());
        assert_eq!(session.eventloop.options.session_expiry_interval(), Some(60));
        session.subscribe("orders", QoS::AtLeastOnce).await.unwrap();
        let mut held = session.receive(1, Duration::from_secs(3)).await.unwrap().pop().unwrap();
        if held.raw() == b"before-refresh" {
            let completion = session.complete(&mut held).await;
            assert!(completion.is_err(), "pre-refresh payload acknowledged a fresh broker session's packet 1");
            panic!("pre-refresh SDK event was re-owned by the refreshed session");
        }
        assert_eq!(held.raw(), b"fresh-work");
        check_ack.send(()).unwrap();
        no_stale_ack_rx.await.unwrap();
        session.complete(&mut held).await.unwrap();
        peer.await.unwrap();
        drop(held); drop(session);
    }).await.unwrap();
}

#[test]
fn credential_refresh_preserves_application_tls_session_and_adapter_limits() {
    use rumqttc::{TlsConfiguration, Transport};
    use std::sync::Arc;

    let tls = Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth(),
    );
    let mut options = MqttOptions::new("tls-client", "broker.invalid", 8883);
    options
        .set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(
            Arc::clone(&tls),
        )))
        .set_clean_start(false)
        .set_session_expiry_interval(Some(42))
        .set_keep_alive(Duration::from_secs(17))
        .set_topic_alias_max(Some(8));
    let mut bounds = Bounds {
        prefetch: 7,
        timeout: Duration::from_secs(2),
        ..Bounds::default()
    };
    bounds.envelope.max_bytes = 4096;
    let mut session = Session::new(
        options,
        Source {
            client: "configured".into(),
            tenant: "trusted".into(),
            endpoint: "orders".into(),
        },
        vec![Route {
            alias: "orders".into(),
            topic: "orders".into(),
        }],
        bounds,
    )
    .unwrap();
    let previous_id = session.id;
    session
        .refresh_credentials(
            "test-user".into(),
            "test-token".into(),
            Some("custom-method".into()),
        )
        .unwrap();
    assert_ne!(session.id, previous_id);
    let options = &session.eventloop.options;
    match options.transport() {
        Transport::Tls(TlsConfiguration::Rustls(actual)) => assert!(Arc::ptr_eq(&actual, &tls)),
        _ => panic!("configured TLS transport was replaced"),
    }
    assert_eq!(options.broker_address(), ("broker.invalid".into(), 8883));
    assert_eq!(options.client_id(), "tls-client");
    assert!(!options.clean_start());
    assert_eq!(options.session_expiry_interval(), Some(42));
    assert_eq!(options.keep_alive(), Duration::from_secs(17));
    assert_eq!(options.topic_alias_max(), Some(8));
    assert_eq!(
        options.authentication_method().as_deref(),
        Some("custom-method")
    );
    assert!(options.manual_acks());
    assert_eq!(options.receive_maximum(), Some(7));
    assert_eq!(options.get_outgoing_inflight_upper_limit(), Some(1));
    assert_eq!(options.max_packet_size(), Some(4096 + 131_072));
    assert_eq!(options.connection_timeout(), 2);
}
