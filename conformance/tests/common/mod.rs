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
    let mut received = receiver.receive(1, wait).await?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].decode()?.id(), envelope.id());
    eprintln!("adapter contract: dead-letter queue");
    receiver
        .dead_letter(&mut received[0], "test-poison")
        .await
        .map_err(|error| format!("dead-lettering queue delivery: {error}"))?;
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

    eprintln!("adapter contract: topic subscription");
    let mut subscription = connection
        .receiver(
            Source::Subscription {
                topic: "events".into(),
                subscription: "audit".into(),
            },
            None,
            0,
        )
        .await?;
    publisher.publish("events", &envelope).await?;
    let mut received = subscription.receive(1, wait).await?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].decode()?.id(), envelope.id());
    subscription
        .dead_letter(&mut received[0], "test-subscription-poison")
        .await?;
    subscription.close().await?;
    let mut subscription_dead = connection
        .receiver(
            Source::SubscriptionDeadLetters {
                topic: "events".into(),
                subscription: "audit".into(),
            },
            None,
            0,
        )
        .await?;
    let mut received = subscription_dead.receive(1, wait).await?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].decode()?.id(), envelope.id());
    subscription_dead.complete(&mut received[0]).await?;
    subscription_dead.close().await?;

    eprintln!("adapter contract: session");
    let session_message = Envelope::encode(&Order { number: 7 })?
        .with_header("session-id".into(), "order-7".into())?;
    publisher.publish("sessions", &session_message).await?;
    let mut session = connection
        .receiver(Source::Queue("sessions".into()), Some("order-7"), 0)
        .await?;
    session.renew_session().await?;
    let mut received = session.receive(1, wait).await?;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].decode()?.id(), session_message.id());
    assert_eq!(
        received[0].decode()?.decode::<Order>()?,
        Order { number: 7 }
    );
    session.complete(&mut received[0]).await?;
    session.close().await?;
    publisher.close().await?;
    Ok(())
}
