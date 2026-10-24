//! Actual loopback AMQP compatibility with an immutable Switchyard revision.

use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;

use raddle::transport::Transport;
use raddle::wire::{Envelope, WireMessage};
use raddle_servicebus::{Connection, Source};
use serde::{Deserialize, Serialize};
use sw_auth::{
    PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule,
};
use sw_domain::{
    CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine, SubscriptionConfig,
    SubscriptionName, TopicConfig,
};
use sw_server::{Broker, LocalProposer, SystemClock};
use sw_storage::{FjallStore, MemoryStore, StateStore};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const HOST: &str = "raddle.servicebus.windows.net";
// Public fixture-only key. It is not used outside this ephemeral loopback broker.
const CONNECTION: &str = "Endpoint=sb://raddle.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=raddle-public-fixture-key";

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Order {
    number: u32,
}
impl WireMessage for Order {
    const NAME: &'static str = "orders.placed";
    const VERSION: u32 = 1;
}

struct Node {
    broker: Broker,
    address: SocketAddr,
    listener: JoinHandle<std::io::Result<()>>,
}

impl Node {
    async fn start<S: StateStore>(store: S) -> TestResult<Self> {
        let broker = Broker::spawn(LocalProposer::new(StateMachine::new(store), SystemClock));
        let namespace = NamespaceName::new("raddle")?;
        for (entity, sessions) in [("orders", false), ("sessions", true)] {
            broker.handle().submit_blocking(
                namespace.clone(),
                EntityPath::new(entity)?,
                CommandKind::CreateQueue {
                    config: QueueConfig {
                        requires_session: sessions,
                        ..QueueConfig::default()
                    },
                },
            )?;
        }
        broker.handle().submit_blocking(
            namespace.clone(),
            EntityPath::new("events")?,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        broker.handle().submit_blocking(
            namespace.clone(),
            EntityPath::new("events")?,
            CommandKind::CreateSubscription {
                name: SubscriptionName::new("audit")?,
                config: SubscriptionConfig::default(),
            },
        )?;
        let policy = SharedAccessPolicy::new([SharedAccessRule::new(
            "test",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new("raddle-public-fixture-key")?,
            None,
            PermissionSet::MANAGE,
        )?])?;
        let listener = sw_protocol::AmqpListener::new(broker.handle(), namespace)
            .with_shared_access_authentication(sw_protocol::SharedAccessAuthentication::new(
                policy, HOST,
            )?);
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?;
        let listener = tokio::spawn(listener.serve(socket));
        Ok(Self {
            broker,
            address,
            listener,
        })
    }

    async fn close(self) {
        self.listener.abort();
        let _ = self.listener.await;
        drop(self.broker);
    }
}

async fn exercise(node: Node) -> TestResult {
    let outcome = async {
        eprintln!("conformance: connect");
        let connection = Connection::emulator(CONNECTION, node.address).await?;
        eprintln!("conformance: open publishers");
        let publisher = connection
            .publisher([
                ("orders".into(), "orders".into()),
                ("events".into(), "events".into()),
                ("sessions".into(), "sessions".into()),
            ])
            .await?;
        let envelope = Envelope::encode(&Order { number: 42 })?;
        eprintln!("conformance: send queue");
        publisher.publish("orders", &envelope).await?;
        eprintln!("conformance: receive queue");
        let mut receiver = connection
            .receiver(Source::Queue("orders".into()), None, 0)
            .await?;
        let mut received = receiver.receive(1, Duration::from_secs(2)).await?;
        assert_eq!(received.len(), 1);
        let first = &mut received[0];
        assert_eq!(first.decode()?.id(), envelope.id());
        assert_eq!(first.decode()?.decode::<Order>()?, Order { number: 42 });
        eprintln!("conformance: renew/abandon");
        receiver.renew(first).await?;
        receiver.abandon(first).await?;
        let mut received = receiver.receive(1, Duration::from_secs(2)).await?;
        assert_eq!(received.len(), 1);
        eprintln!("conformance: dead-letter");
        receiver
            .dead_letter(&mut received[0], "test-poison")
            .await?;
        eprintln!("conformance: open dead-letter receiver");
        let mut dead = connection
            .receiver(Source::DeadLetters("orders".into()), None, 0)
            .await?;
        let mut received = dead.receive(1, Duration::from_secs(2)).await?;
        assert_eq!(received.len(), 1);
        dead.complete(&mut received[0]).await?;
        assert!(dead.complete(&mut received[0]).await.is_err());
        dead.close().await?;
        receiver.close().await?;

        eprintln!("conformance: topic subscription");
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
        let mut received = subscription.receive(1, Duration::from_secs(2)).await?;
        assert_eq!(received.len(), 1);
        subscription.complete(&mut received[0]).await?;
        subscription.close().await?;

        eprintln!("conformance: session");
        let session_message = Envelope::encode(&Order { number: 7 })?
            .with_header("session-id".into(), "order-7".into())?;
        publisher.publish("sessions", &session_message).await?;
        let mut session = connection
            .receiver(Source::Queue("sessions".into()), Some("order-7"), 0)
            .await?;
        eprintln!("conformance: renew session");
        session.renew_session().await?;
        let mut received = session.receive(1, Duration::from_secs(2)).await?;
        assert_eq!(received.len(), 1);
        session.complete(&mut received[0]).await?;
        session.close().await?;
        publisher.close().await?;
        connection.close().await?;
        Ok(())
    }
    .await;
    node.close().await;
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_backend_round_trip() -> TestResult {
    let node = Node::start(MemoryStore::default()).await?;
    tokio::time::timeout(Duration::from_secs(90), exercise(node)).await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_backend_round_trip() -> TestResult {
    let directory = tempfile::tempdir()?;
    let node = Node::start(FjallStore::open(directory.path())?).await?;
    tokio::time::timeout(Duration::from_secs(90), exercise(node)).await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_client_connection() -> TestResult {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .try_init();
    let node = Node::start(MemoryStore::default()).await?;
    let options = azservicebus::ServiceBusClientOptions {
        custom_endpoint_address: Some(url::Url::parse(&format!("amqp://{}", node.address))?),
        retry_options: azservicebus::ServiceBusRetryOptions {
            max_retries: 0,
            try_timeout: Duration::from_secs(5),
            ..Default::default()
        },
        ..Default::default()
    };
    let result = Box::pin(
        azservicebus::ServiceBusClient::unsecured().new_from_connection_string(CONNECTION, options),
    )
    .await;
    let outcome = match result {
        Ok(mut client) => {
            let outcome = match Box::pin(client.create_sender("orders", Default::default())).await {
                Ok(mut sender) => {
                    let result = Box::pin(
                        sender.send_message(azservicebus::ServiceBusMessage::new(vec![1, 2, 3])),
                    )
                    .await
                    .map_err(|error| format!("fixture send orders: {error:?}").into());
                    let _ = Box::pin(sender.dispose()).await;
                    result
                }
                Err(error) => Err(format!("fixture sender orders: {error:?}").into()),
            };
            let _ = Box::pin(client.dispose()).await;
            outcome
        }
        Err(error) => Err(format!("fixture client connect: {error:?}").into()),
    };
    node.close().await;
    outcome
}

#[test]
fn fully_qualified_link_addresses_resolve_to_their_entity() -> TestResult {
    let attachment = sw_protocol::parse_attachment("amqps://raddle.servicebus.windows.net/orders")?;
    match attachment {
        sw_protocol::Attachment::Queue(entity) => assert_eq!(entity.as_str(), "orders"),
        _ => panic!("absolute queue address did not resolve to a queue"),
    }
    Ok(())
}
