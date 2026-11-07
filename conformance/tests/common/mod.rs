//! The same adapter contract is exercised against independent broker implementations.

use std::error::Error;
use std::time::Duration;

use raddle::transport::Transport;
use raddle::wire::{Envelope, WireMessage};
use raddle_servicebus::{Connection, Source};
use serde::{Deserialize, Serialize};

pub type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Order {
    pub number: u32,
}
impl WireMessage for Order {
    const NAME: &'static str = "orders.placed";
    const VERSION: u32 = 1;
}

pub async fn exercise(connection: &Connection) -> TestResult {
    let started = tokio::time::Instant::now();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .try_init();
    let wait = Duration::from_secs(30);
    eprintln!("adapter contract: open publishers");
    let publisher = connection
        .publisher([
            ("orders".into(), "orders".into()),
            ("events".into(), "events".into()),
            ("sessions".into(), "sessions".into()),
        ])
        .await?;
    let envelope = Envelope::encode(&Order { number: 42 })?;
    eprintln!("adapter contract: publish queue");
    publisher.publish("orders", &envelope).await?;
    let mut receiver = connection
        .receiver(Source::Queue("orders".into()), None, 0)
        .await?;
    eprintln!("adapter contract: receive queue");
    let mut received = receiver.receive(1, wait).await?;
    assert_eq!(received.len(), 1);
    let first = &mut received[0];
    assert_eq!(first.decode()?.id(), envelope.id());
    assert_eq!(first.decode()?.decode::<Order>()?, Order { number: 42 });
    eprintln!("adapter contract: renew and abandon");
    receiver.renew(first).await?;
    receiver.abandon(first).await?;
    assert!(first.is_settled());
    let mut received = receiver.receive(1, wait).await?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].decode()?.id(), envelope.id());
    eprintln!("adapter contract: dead-letter queue");
    receiver
        .dead_letter(&mut received[0], "test-poison")
        .await
        .map_err(|error| format!("dead-lettering queue delivery: {error}"))?;
    assert!(received[0].is_settled());
    let mut dead = connection
        .receiver(Source::DeadLetters("orders".into()), None, 0)
        .await
        .map_err(|error| format!("opening queue dead-letter receiver: {error}"))?;
    let mut received = dead
        .receive(1, wait)
        .await
        .map_err(|error| format!("receiving queue dead letter: {error}"))?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].decode()?.id(), envelope.id());
    dead.complete(&mut received[0])
        .await
        .map_err(|error| format!("completing queue dead letter: {error}"))?;
    assert!(dead.complete(&mut received[0]).await.is_err());
    dead.close()
        .await
        .map_err(|error| format!("closing queue dead-letter receiver: {error}"))?;
    receiver
        .close()
        .await
        .map_err(|error| format!("closing queue receiver: {error}"))?;

    eprintln!(
        "adapter contract: topic subscription at {:?}",
        started.elapsed()
    );
    let mut subscription = connection
        .receiver(
            Source::Subscription {
                topic: "events".into(),
                subscription: "audit".into(),
            },
            None,
            0,
        )
        .await
        .map_err(|error| format!("opening subscription: {error}"))?;
    publisher
        .publish("events", &envelope)
        .await
        .map_err(|error| format!("publishing topic: {error}"))?;
    let mut received = subscription
        .receive(1, wait)
        .await
        .map_err(|error| format!("receiving subscription: {error}"))?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].decode()?.id(), envelope.id());
    eprintln!(
        "adapter contract: dead-letter subscription at {:?}",
        started.elapsed()
    );
    subscription
        .dead_letter(&mut received[0], "test-subscription-poison")
        .await
        .map_err(|error| format!("dead-lettering subscription: {error}"))?;
    subscription
        .close()
        .await
        .map_err(|error| format!("closing subscription: {error}"))?;
    eprintln!(
        "adapter contract: open subscription dead letters at {:?}",
        started.elapsed()
    );
    let mut subscription_dead = connection
        .receiver(
            Source::SubscriptionDeadLetters {
                topic: "events".into(),
                subscription: "audit".into(),
            },
            None,
            0,
        )
        .await
        .map_err(|error| format!("opening subscription dead letters: {error}"))?;
    eprintln!(
        "adapter contract: receive subscription dead letters at {:?}",
        started.elapsed()
    );
    let mut received = subscription_dead
        .receive(1, wait)
        .await
        .map_err(|error| format!("receiving subscription dead letters: {error}"))?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].decode()?.id(), envelope.id());
    eprintln!(
        "adapter contract: complete subscription dead letter at {:?}",
        started.elapsed()
    );
    subscription_dead
        .complete(&mut received[0])
        .await
        .map_err(|error| format!("completing subscription dead letter: {error}"))?;
    subscription_dead
        .close()
        .await
        .map_err(|error| format!("closing subscription dead letters: {error}"))?;

    eprintln!("adapter contract: bounded partial batches and idle waits");
    for prefetch in [0, 2] {
        let mut batches = connection
            .receiver(Source::Queue("orders".into()), None, prefetch)
            .await?;
        let mut expected = std::collections::HashSet::new();
        for number in 100..103 {
            let message = Envelope::encode(&Order { number })?;
            expected.insert(message.id());
            publisher
                .publish("orders", &message)
                .await
                .map_err(|error| {
                    format!(
                        "publishing bounded batch, prefetch {prefetch}, order {number}: {error}"
                    )
                })?;
        }
        // Real brokers may split even pre-published messages across partial
        // batches. Check custody and bounds without assuming network grouping.
        let deadline = tokio::time::Instant::now() + wait;
        let mut saw_partial = false;
        while !expected.is_empty() {
            let started = tokio::time::Instant::now();
            let remaining = deadline.saturating_duration_since(started);
            assert!(
                !remaining.is_zero(),
                "published deliveries were not all received"
            );
            let mut batch = tokio::time::timeout(
                remaining.min(Duration::from_secs(7)),
                batches.receive(2, remaining.min(Duration::from_secs(2))),
            )
            .await??;
            assert!(
                batch.len() <= 2,
                "receive exceeded the requested batch size"
            );
            assert!(started.elapsed() < Duration::from_secs(7));
            saw_partial |= batch.len() == 1;
            for held in &mut batch {
                assert!(
                    expected.remove(&held.decode()?.id()),
                    "unexpected or duplicate delivery"
                );
                batches.complete(held).await.map_err(|error| {
                    format!("completing bounded batch, prefetch {prefetch}: {error}")
                })?;
                assert!(held.is_settled());
            }
        }
        assert!(expected.is_empty());
        assert!(
            saw_partial,
            "the odd delivery count must include a nonempty partial batch"
        );
        let started = tokio::time::Instant::now();
        assert!(
            tokio::time::timeout(
                Duration::from_secs(3),
                batches.receive(1, Duration::from_millis(100)),
            )
            .await??
            .is_empty()
        );
        assert!(started.elapsed() < Duration::from_secs(3));
        batches.close().await?;
    }

    eprintln!("adapter contract: session");
    let session_message = Envelope::encode(&Order { number: 7 })?
        .with_header("session-id".into(), "order-7".into())?;
    publisher
        .publish("sessions", &session_message)
        .await
        .map_err(|error| format!("publishing session message: {error}"))?;
    let mut session = connection
        .receiver(Source::Queue("sessions".into()), Some("order-7"), 0)
        .await
        .map_err(|error| format!("opening session receiver: {error}"))?;
    session
        .renew_session()
        .await
        .map_err(|error| format!("renewing session: {error}"))?;
    let mut received = session
        .receive(1, wait)
        .await
        .map_err(|error| format!("receiving session: {error}"))?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].decode()?.id(), session_message.id());
    assert_eq!(
        received[0].decode()?.decode::<Order>()?,
        Order { number: 7 }
    );
    session
        .complete(&mut received[0])
        .await
        .map_err(|error| format!("completing session message: {error}"))?;
    assert!(received[0].is_settled());
    session
        .close()
        .await
        .map_err(|error| format!("closing session receiver: {error}"))?;
    publisher
        .close()
        .await
        .map_err(|error| format!("closing publishers: {error}"))?;
    Ok(())
}
