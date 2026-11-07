//! Broker refusal must never acknowledge a publication or complete its outbox job.

use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::Utc;
use raddle::transport::Transport;
use raddle::wire::{Envelope, WireMessage};
use raddle_postgres::{Delivery, DeliveryKind, JobStatus, OutboxWorker, RetryPolicy, Store};
use raddle_servicebus::{Connection, Source};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sw_auth::{
    PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule,
};
use sw_domain::{CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine};
use sw_server::{Broker, LocalProposer, ManualClock};
use sw_storage::MemoryStore;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const HOST: &str = "refused.servicebus.windows.net";
// Public, isolated fixture key; never used with Azure or another service.
const CONNECTION: &str = "Endpoint=sb://refused.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=public-refusal-fixture";
const TENANT: &str = "refusal-test";

#[derive(Serialize, Deserialize)]
struct Order(u32);
impl WireMessage for Order {
    const NAME: &'static str = "test.refused-order";
    const VERSION: u32 = 1;
}

struct Node {
    _broker: Broker,
    address: SocketAddr,
    listener: JoinHandle<std::io::Result<()>>,
}

impl Node {
    async fn start() -> TestResult<Self> {
        let clock = ManualClock::at(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_millis()
                .try_into()?,
        );
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            clock,
        ));
        let namespace = NamespaceName::new("refused")?;
        // Orders without a session are actually refused by this broker, not by
        // an application fault script or a transport wrapper.
        for (entity, requires_session) in [("blocked", true), ("accepted", false)] {
            broker.handle().submit_blocking(
                namespace.clone(),
                EntityPath::new(entity)?,
                CommandKind::CreateQueue {
                    config: QueueConfig {
                        requires_session,
                        ..Default::default()
                    },
                },
            )?;
        }
        let policy = SharedAccessPolicy::new([SharedAccessRule::new(
            "test",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new("public-refusal-fixture")?,
            None,
            PermissionSet::MANAGE,
        )?])?;
        let listener = sw_protocol::AmqpListener::new(broker.handle(), namespace)
            .with_shared_access_authentication(sw_protocol::SharedAccessAuthentication::new(
                policy, HOST,
            )?);
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?;
        Ok(Self {
            _broker: broker,
            address,
            listener: tokio::spawn(listener.serve(socket)),
        })
    }

    async fn close(&mut self) {
        self.listener.abort();
        let _ = (&mut self.listener).await;
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn servicebus_publisher_refuses_without_completing_or_blocking_another_route() -> TestResult {
    tokio::time::timeout(Duration::from_secs(90), async {
        let mut node = Node::start().await?;
        let connection = Connection::emulator(CONNECTION, node.address).await?;
        let publisher = connection
            .publisher([
                ("blocked".into(), "blocked".into()),
                ("healthy".into(), "accepted".into()),
            ])
            .await?;
        let refused = publisher
            .publish("blocked", &Envelope::encode(&Order(1))?)
            .await;
        let accepted = publisher
            .publish("healthy", &Envelope::encode(&Order(2))?)
            .await;
        let closed = publisher.close().await;
        let connection_closed = connection.close().await;
        node.close().await;
        closed?;
        connection_closed?;
        accepted?;
        if refused.is_ok() {
            return Err("repeated broker refusal was reported as publication success".into());
        }
        Ok(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_single_collected_and_explicit_batches_refuse_with_default_retries() -> TestResult {
    tokio::time::timeout(Duration::from_secs(120), async {
        let mut node = Node::start().await?;
        let options = azservicebus::ServiceBusClientOptions {
            custom_endpoint_address: Some(url::Url::parse(&format!("amqp://{}", node.address))?),
            ..Default::default()
        };
        assert_eq!(options.retry_options.max_retries, 3);
        assert_eq!(options.retry_options.delay, Duration::from_millis(800));
        let mut client = Box::pin(
            azservicebus::ServiceBusClient::unsecured()
                .new_from_connection_string(CONNECTION, options),
        )
        .await?;
        let mut sender = Box::pin(client.create_sender("blocked", Default::default())).await?;
        let single =
            Box::pin(sender.send_message(azservicebus::ServiceBusMessage::new(vec![1]))).await;
        let collected = Box::pin(sender.send_messages([
            azservicebus::ServiceBusMessage::new(vec![2]),
            azservicebus::ServiceBusMessage::new(vec![3]),
        ]))
        .await;
        let mut batch = sender.create_message_batch(Default::default())?;
        batch.try_add_message(azservicebus::ServiceBusMessage::new(vec![4]))?;
        batch.try_add_message(azservicebus::ServiceBusMessage::new(vec![5]))?;
        let explicit = Box::pin(sender.send_message_batch(batch)).await;
        let sender_closed = Box::pin(sender.dispose()).await;
        let client_closed = Box::pin(client.dispose()).await;
        node.close().await;
        sender_closed?;
        client_closed?;
        if single.is_ok() || collected.is_ok() || explicit.is_ok() {
            return Err(
                "a refused single/collected/explicit batch was reported as accepted".into(),
            );
        }
        Ok(())
    })
    .await?
}

async fn outbox_exchange(connection: &Connection, pool: PgPool) -> TestResult {
    let store = Store::new(pool.clone());
    let publisher = connection
        .publisher([
            ("blocked".into(), "blocked".into()),
            ("healthy".into(), "accepted".into()),
        ])
        .await?;
    let outbox = OutboxWorker::new(store.clone(), publisher, RetryPolicy::default());
    let bad = Envelope::encode(&Order(10))?;
    let good = Envelope::encode(&Order(11))?;
    let mut scope = store.begin().await?;
    let bad_id = scope
        .stage(
            "publication",
            TENANT,
            &bad,
            &[Delivery::new(DeliveryKind::Remote, "blocked", Utc::now())],
        )
        .await?[0];
    let good_id = scope
        .stage(
            "publication",
            TENANT,
            &good,
            &[Delivery::new(DeliveryKind::Remote, "healthy", Utc::now())],
        )
        .await?[0];
    scope.commit(Utc::now()).await?;
    let negative = async {
        if outbox.run_once(2).await? != 2 {
            return Err("did not claim both independent jobs".into());
        }
        let failed = store
            .inspect(TENANT, bad_id)
            .await?
            .ok_or("missing failed job")?;
        let successful = store
            .inspect(TENANT, good_id)
            .await?
            .ok_or("missing successful job")?;
        if failed.status != JobStatus::Ready
            || failed.attempts != 1
            || failed.last_error.as_deref() != Some("publish-failed")
        {
            return Err(format!(
                "refused outbox publication must remain retryable, got {failed:?}"
            )
            .into());
        }
        if successful.status != JobStatus::Completed || successful.attempts != 1 {
            return Err("unrelated accepted job did not complete independently".into());
        }
        sqlx::query("UPDATE raddle_jobs SET available_at=now() WHERE id=$1")
            .bind(bad_id)
            .execute(&pool)
            .await?;
        if outbox.run_once(2).await? != 1 {
            return Err("retry must claim only the failed job".into());
        }
        let failed = store
            .inspect(TENANT, bad_id)
            .await?
            .ok_or("missing retried job")?;
        if failed.status != JobStatus::Ready || failed.attempts != 2 {
            return Err("repeated refusal completed or discarded the outbox job".into());
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    let closed = outbox.into_transport().close().await;
    negative?;
    closed?;

    // Correct only the route configuration. The existing durable envelope,
    // identity and successful job remain unchanged.
    let publisher = connection
        .publisher([("blocked".into(), "accepted".into())])
        .await?;
    let outbox = OutboxWorker::new(store.clone(), publisher, RetryPolicy::default());
    let positive = async {
        sqlx::query("UPDATE raddle_jobs SET available_at=now() WHERE id=$1")
            .bind(bad_id)
            .execute(&pool)
            .await?;
        if outbox.run_once(4).await? != 1 {
            return Err("corrected retry did not isolate the failed job".into());
        }
        let corrected = store
            .inspect(TENANT, bad_id)
            .await?
            .ok_or("missing corrected job")?;
        let successful = store
            .inspect(TENANT, good_id)
            .await?
            .ok_or("missing successful job")?;
        if corrected.status != JobStatus::Completed
            || corrected.attempts != 3
            || corrected.message_id != bad.id()
        {
            return Err("corrected retry lost its durable identity or did not complete".into());
        }
        if successful.status != JobStatus::Completed || successful.attempts != 1 {
            return Err("successful publication was duplicated by a failed-job retry".into());
        }
        let mut receiver = connection
            .receiver(Source::Queue("accepted".into()), None, 0)
            .await?;
        let mut deliveries = receiver.receive(2, Duration::from_secs(5)).await?;
        let observed = deliveries
            .iter()
            .map(|message| message.decode().map(|envelope| envelope.id()))
            .collect::<Result<std::collections::HashSet<_>, _>>()?;
        if deliveries.len() != 2
            || observed != std::collections::HashSet::from([bad.id(), good.id()])
        {
            return Err(
                "accepted broker messages were missing, changed identity or duplicated".into(),
            );
        }
        for message in &mut deliveries {
            receiver.complete(message).await?;
        }
        let extra = receiver.receive(1, Duration::from_millis(100)).await?;
        receiver.close().await?;
        if !extra.is_empty() {
            return Err("unexpected duplicate accepted publication".into());
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    let closed = outbox.into_transport().close().await;
    positive?;
    closed?;
    Ok(())
}

#[sqlx::test(migrations = "../crates/raddle-postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn refused_servicebus_publication_keeps_outbox_retryable_and_successes_completed(
    pool: PgPool,
) -> TestResult {
    tokio::time::timeout(Duration::from_secs(120), async {
        let mut node = Node::start().await?;
        let connection = Connection::emulator(CONNECTION, node.address).await?;
        let result = Box::pin(outbox_exchange(&connection, pool)).await;
        let closed = connection.close().await;
        node.close().await;
        result?;
        closed?;
        Ok(())
    })
    .await?
}
