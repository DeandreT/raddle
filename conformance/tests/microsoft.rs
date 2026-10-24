//! Opt-in conformance against Microsoft's official local Service Bus emulator.

mod common;
#[path = "support/microsoft.rs"]
mod fixture;

use std::time::Duration;

use raddle::transport::Transport;
use raddle::wire::Envelope;
use raddle_servicebus::{Connection, Source};

use common::{Order, TestResult};
use fixture::{connection_string, endpoint};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an isolated official emulator and RADDLE_MICROSOFT_TEST_ENDPOINT"]
async fn official_emulator_satisfies_the_shared_adapter_contract() -> TestResult {
    eprintln!("official emulator: connecting");
    let endpoint = endpoint()?;
    let connection = Connection::emulator(&connection_string(endpoint)?, endpoint).await?;
    let result =
        tokio::time::timeout(Duration::from_secs(120), common::exercise(&connection)).await;
    let closed = connection.close().await;
    result??;
    closed?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an isolated official emulator and RADDLE_MICROSOFT_TEST_ENDPOINT"]
async fn official_emulator_accepts_the_raw_client_links() -> TestResult {
    let endpoint = endpoint()?;
    let options = azservicebus::ServiceBusClientOptions {
        custom_endpoint_address: Some(url::Url::parse(&format!("amqp://{endpoint}"))?),
        retry_options: azservicebus::ServiceBusRetryOptions {
            max_retries: 0,
            try_timeout: Duration::from_secs(30),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut client = Box::pin(
        azservicebus::ServiceBusClient::unsecured()
            .new_from_connection_string(&connection_string(endpoint)?, options),
    )
    .await
    .map_err(|error| format!("raw client connection: {error:?}"))?;
    let result = match Box::pin(client.create_sender("orders", Default::default())).await {
        Ok(sender) => Box::pin(sender.dispose())
            .await
            .map_err(|error| format!("raw sender close: {error:?}")),
        Err(error) => Err(format!("raw sender open: {error:?}")),
    };
    let closed = Box::pin(client.dispose()).await;
    result?;
    closed?;
    Ok(())
}

#[test]
fn numeric_ipv4_and_ipv6_hosts_are_preserved_in_logical_addresses() -> TestResult {
    for (address, prefix) in [
        ("127.0.0.1:12345", "Endpoint=sb://127.0.0.1/;"),
        ("[::1]:12345", "Endpoint=sb://[::1]/;"),
    ] {
        assert!(connection_string(address.parse()?)?.starts_with(prefix));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the official emulator fixture with its five-second lease-probe queue"]
async fn expired_lock_is_refused_and_work_survives_reconnection() -> TestResult {
    let endpoint = endpoint()?;
    let connection_string = connection_string(endpoint)?;
    let connection = Connection::emulator(&connection_string, endpoint).await?;
    let envelope = Envelope::encode(&Order { number: 91 })?;
    let result: TestResult = async {
        let publisher = connection
            .publisher([("probe".into(), "lease-probe".into())])
            .await?;
        publisher.publish("probe", &envelope).await?;
        let mut receiver = connection
            .receiver(Source::Queue("lease-probe".into()), None, 0)
            .await?;
        let mut received = receiver.receive(1, Duration::from_secs(10)).await?;
        let held = received.first_mut().ok_or("missing original delivery")?;
        assert_eq!(held.decode()?.id(), envelope.id());
        // This tests the emulator's real five-second clock-based lease, not a mock clock.
        tokio::time::sleep(Duration::from_secs(7)).await;
        assert!(receiver.renew(held).await.is_err());
        receiver.close().await?;
        publisher.close().await?;
        Ok(())
    }
    .await;
    let closed = connection.close().await;
    result?;
    closed?;

    let replacement = Connection::emulator(&connection_string, endpoint).await?;
    let result: TestResult = async {
        let mut receiver = replacement
            .receiver(Source::Queue("lease-probe".into()), None, 0)
            .await?;
        let mut received = receiver.receive(1, Duration::from_secs(10)).await?;
        let held = received.first_mut().ok_or("missing recovered delivery")?;
        assert_eq!(held.decode()?.id(), envelope.id());
        receiver.complete(held).await?;
        receiver.close().await?;
        Ok(())
    }
    .await;
    let closed = replacement.close().await;
    result?;
    closed?;
    Ok(())
}
