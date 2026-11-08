//! Explicit live gate; only run against an existing isolated Azure test entity.

use std::error::Error;
use std::time::Duration;

use raddle::transport::Transport;
use raddle::wire::{Envelope, WireMessage};
use raddle_servicebus::{Connection, NetworkTransport, Source};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Probe {
    text: String,
}
impl WireMessage for Probe {
    const NAME: &'static str = "raddle.test-probe";
    const VERSION: u32 = 1;
}

#[tokio::test]
#[ignore = "requires Azure credentials and an explicitly confirmed isolated test queue"]
async fn azure_confirmed_send_receive_renew_and_complete() -> Result<(), Box<dyn Error>> {
    if std::env::var("RADDLE_AZURE_TEST_CONFIRM").as_deref() != Ok("isolated") {
        return Err(
            "set RADDLE_AZURE_TEST_CONFIRM=isolated only for a dedicated test queue".into(),
        );
    }
    let secret = std::env::var("RADDLE_AZURE_CONNECTION_STRING")?;
    let queue = std::env::var("RADDLE_AZURE_TEST_QUEUE")?;
    let connection = Connection::connect(&secret, NetworkTransport::Tls).await?;
    let publisher = connection
        .publisher([("test".to_owned(), queue.clone())])
        .await?;
    let mut receiver = connection.receiver(Source::Queue(queue), None, 0).await?;
    let result = async {
        let envelope = Envelope::encode(&Probe {
            text: "raddle conformance".into(),
        })?;
        publisher.publish("test", &envelope).await?;
        let mut received = receiver.receive(1, Duration::from_secs(10)).await?;
        let held = received
            .first_mut()
            .ok_or("Azure did not deliver the probe")?;
        if held.decode()?.id() != envelope.id() {
            receiver.abandon(held).await?;
            return Err("test queue was not isolated; unrelated message was abandoned".into());
        }
        receiver.renew(held).await?;
        receiver.complete(held).await?;
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    let close_receiver = receiver.close().await;
    let close_publisher = publisher.close().await;
    let close_connection = connection.close().await;
    result?;
    close_receiver?;
    close_publisher?;
    close_connection?;
    Ok(())
}
