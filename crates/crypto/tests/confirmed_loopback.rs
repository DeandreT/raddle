//! Confirmed loopback publication and isolated endpoint execution.
#![allow(clippy::unwrap_used)]

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, EnvelopeLimits, WireMessage};
use raddle_crypto::{
    EncryptedTransport, EncryptionCodec, EncryptionPolicy, KeyId, RotatingKeys, SecretKey,
    Selection,
};
use raddle_policies::{
    Direction, InMemoryEndpoint, MemoryDelivery, MemoryStatus, PolicyError, PolicyExecutor,
    PolicySet,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

#[derive(Serialize, Deserialize)]
struct Work(u32);
impl WireMessage for Work {
    const NAME: &'static str = "loopback.work";
    const VERSION: u32 = 1;
}

// A real bounded TCP byte transport: acceptance is explicit after receiver
// codec succeeds. It is a test protocol, not a new production broker adapter.
struct ConfirmedTcp {
    address: SocketAddr,
}
impl Transport for ConfirmedTcp {
    async fn publish(&self, _: &str, envelope: &Envelope) -> Result<(), TransportError> {
        let bytes = envelope.to_vec().map_err(|_| TransportError::Rejected)?;
        let mut socket = TcpStream::connect(self.address)
            .await
            .map_err(|_| TransportError::Transient)?;
        socket
            .write_u32(u32::try_from(bytes.len()).map_err(|_| TransportError::Rejected)?)
            .await
            .map_err(|_| TransportError::Transient)?;
        socket
            .write_all(&bytes)
            .await
            .map_err(|_| TransportError::Transient)?;
        match socket.read_u8().await {
            Ok(1) => Ok(()),
            Ok(_) => Err(TransportError::Rejected),
            Err(_) => Err(TransportError::Transient),
        }
    }
}

fn keys() -> RotatingKeys {
    let keys = RotatingKeys::new(1).unwrap();
    keys.insert(
        KeyId::new("test-only").unwrap(),
        SecretKey::new([7; 32]),
        true,
    )
    .unwrap();
    keys
}
fn codec(keys: RotatingKeys) -> EncryptionCodec<RotatingKeys> {
    EncryptionCodec::new(
        keys,
        EncryptionPolicy {
            send: Selection::All,
            receive_required: Selection::All,
        },
        EnvelopeLimits::default(),
    )
}

#[tokio::test]
async fn confirmed_wire_publication_and_receive_failures_isolate_first_middle_last() {
    for failed in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let keys = keys();
        let received = Arc::new(Mutex::new(Vec::new()));
        let seen = received.clone();
        let receiver_codec = codec(keys.clone());
        let server = tokio::spawn(async move {
            for _ in 0..4 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let count = socket.read_u32().await.unwrap() as usize;
                assert!(count <= EnvelopeLimits::default().max_bytes);
                let mut bytes = vec![0; count];
                socket.read_exact(&mut bytes).await.unwrap();
                assert!(!String::from_utf8_lossy(&bytes).contains("\"payload\":0"));
                let envelope = receiver_codec.decode_bytes(&bytes).await.unwrap();
                seen.lock().unwrap().push(envelope);
                socket.write_u8(1).await.unwrap(); // only then is publication confirmed
            }
        });
        let transport = EncryptedTransport::new(ConfirmedTcp { address }, codec(keys));
        let executor = PolicyExecutor::new(PolicySet::default());
        let mut sending = InMemoryEndpoint::new("send", Direction::Sending, 3).unwrap();
        let mut receiving = InMemoryEndpoint::new("receive", Direction::Handling, 3).unwrap();
        let mut original = Vec::new();
        for number in 0..3 {
            let envelope = Envelope::encode(&Work(number)).unwrap();
            original.push(envelope.to_vec().unwrap());
            sending.enqueue(MemoryDelivery::new(envelope)).unwrap();
        }
        let (_sender, mut stop) = watch::channel(false);
        for _ in 0..3 {
            sending
                .run_once(&executor, Duration::ZERO, &mut stop, |envelope| {
                    let transport = &transport;
                    async move {
                        let Work(number) = envelope.decode().unwrap();
                        if number == failed {
                            return Err(Box::new(io::Error::other("scripted pre-send failure"))
                                as PolicyError);
                        }
                        transport
                            .publish("receive", &envelope)
                            .await
                            .map_err(|e| Box::new(e) as PolicyError)
                    }
                })
                .await
                .unwrap();
        }
        let id = sending.deliveries()[failed as usize].envelope.id();
        sending.retry_dead_letter(id, Duration::ZERO).unwrap();
        sending
            .run_once(&executor, Duration::ZERO, &mut stop, |envelope| {
                let transport = &transport;
                async move {
                    transport
                        .publish("receive", &envelope)
                        .await
                        .map_err(|e| Box::new(e) as PolicyError)
                }
            })
            .await
            .unwrap();
        // Fourth actual publication proves repeat encoding leaves live original
        // untouched; the receiving endpoint below refuses duplicate identity.
        transport
            .publish("receive", &sending.deliveries()[0].envelope)
            .await
            .unwrap();
        server.await.unwrap();
        assert!(
            sending
                .deliveries()
                .iter()
                .all(|d| d.status == MemoryStatus::Completed)
        );
        let received = received.lock().unwrap().clone();
        for envelope in received.iter().take(3) {
            receiving
                .enqueue(MemoryDelivery::new(envelope.clone()))
                .unwrap();
        }
        let calls = Arc::new(Mutex::new([0; 3]));
        for _ in 0..3 {
            let calls = calls.clone();
            receiving
                .run_once(&executor, Duration::ZERO, &mut stop, move |envelope| {
                    let calls = calls.clone();
                    async move {
                        let Work(number) = envelope.decode().unwrap();
                        calls.lock().unwrap()[number as usize] += 1;
                        if number == failed {
                            Err(Box::new(io::Error::other("scripted handler failure"))
                                as PolicyError)
                        } else {
                            Ok(())
                        }
                    }
                })
                .await
                .unwrap();
        }
        receiving.retry_dead_letter(id, Duration::ZERO).unwrap();
        let observed = calls.clone();
        receiving
            .run_once(&executor, Duration::ZERO, &mut stop, move |envelope| {
                let observed = observed.clone();
                async move {
                    let Work(number) = envelope.decode().unwrap();
                    observed.lock().unwrap()[number as usize] += 1;
                    Ok(())
                }
            })
            .await
            .unwrap();
        let mut expected = [1; 3];
        expected[failed as usize] = 2;
        assert_eq!(*calls.lock().unwrap(), expected);
        for (index, delivery) in sending.deliveries().iter().enumerate() {
            assert_eq!(delivery.envelope.to_vec().unwrap(), original[index]);
            assert_eq!(
                delivery.attempts.sending,
                if index == failed as usize { 2 } else { 1 }
            );
            assert_eq!(delivery.attempts.handling, 0);
        }
        assert!(
            receiving
                .enqueue(MemoryDelivery::new(received[3].clone()))
                .is_err()
        );
        assert!(
            receiving
                .deliveries()
                .iter()
                .all(|d| d.status == MemoryStatus::Completed)
        );
    }
}
