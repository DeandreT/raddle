//! Broker refusal must never acknowledge a publication or complete its outbox job.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{TimeDelta, Utc};
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle_postgres::{Delivery, DeliveryKind, JobStatus, OutboxWorker, RetryPolicy, Store};
use raddle_servicebus::{Connection, Publisher, Source};
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
use uuid::Uuid;

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

struct ObservedPublisher {
    inner: Publisher,
    dispatched: Arc<Mutex<Vec<Uuid>>>,
}

impl Transport for ObservedPublisher {
    async fn publish(&self, destination: &str, envelope: &Envelope) -> Result<(), TransportError> {
        self.dispatched
            .lock()
            .expect("fixture dispatch recorder")
            .push(envelope.id());
        self.inner.publish(destination, envelope).await
    }
}

async fn check_successful_jobs(store: &Store, jobs: &[(Uuid, Uuid)]) -> TestResult {
    for &(job_id, message_id) in jobs {
        let successful = store
            .inspect(TENANT, job_id)
            .await?
            .ok_or("missing successful job")?;
        if successful.status != JobStatus::Completed
            || successful.attempts != 1
            || successful.message_id != message_id
            || successful.last_error.is_some()
        {
            return Err(format!(
                "accepted job was blocked, changed or retried after another job's refusal: {successful:?}"
            )
            .into());
        }
    }
    Ok(())
}

async fn outbox_exchange(connection: &Connection, pool: PgPool, bad_position: usize) -> TestResult {
    let store = Store::new(pool.clone());
    let publisher = connection
        .publisher([
            ("blocked".into(), "blocked".into()),
            ("healthy".into(), "accepted".into()),
        ])
        .await?;
    let dispatched = Arc::new(Mutex::new(Vec::new()));
    let publisher = ObservedPublisher {
        inner: publisher,
        dispatched: Arc::clone(&dispatched),
    };
    let outbox = OutboxWorker::new(store.clone(), publisher, RetryPolicy::default());
    let messages = [
        Envelope::encode(&Order(10))?,
        Envelope::encode(&Order(11))?,
        Envelope::encode(&Order(12))?,
    ];
    let bad = &messages[bad_position];
    let expected_order: Vec<_> = messages.iter().map(Envelope::id).collect();
    // All jobs are already due, with distinct times established before staging.
    // Stage in the same order because the store selects candidates by sequence.
    // The recorder below additionally checks the real claimed-job dispatch order.
    let base = chrono::DateTime::from_timestamp(Utc::now().timestamp() - 10, 0)
        .ok_or("invalid fixture schedule base")?;
    let mut scope = store.begin().await?;
    let mut job_ids = Vec::new();
    for (position, message) in messages.iter().enumerate() {
        let destination = if position == bad_position {
            "blocked"
        } else {
            "healthy"
        };
        let job_id = scope
            .stage(
                "publication",
                TENANT,
                message,
                &[Delivery::new(
                    DeliveryKind::Remote,
                    destination,
                    base + TimeDelta::seconds(position as i64),
                )],
            )
            .await?[0];
        job_ids.push(job_id);
    }
    scope.commit(Utc::now()).await?;
    let bad_id = job_ids[bad_position];
    let successful_jobs: Vec<_> = job_ids
        .iter()
        .zip(&messages)
        .enumerate()
        .filter(|(position, _)| *position != bad_position)
        .map(|(_, (&job_id, message))| (job_id, message.id()))
        .collect();
    let negative = async {
        let scheduled: Vec<(Uuid, chrono::DateTime<Utc>)> = sqlx::query_as(
            "SELECT id,available_at FROM raddle_jobs WHERE id=ANY($1) ORDER BY available_at",
        )
        .bind(&job_ids)
        .fetch_all(&pool)
        .await?;
        for (position, &(id, available_at)) in scheduled.iter().enumerate() {
            if id != job_ids[position]
                || available_at != base + TimeDelta::seconds(position as i64)
                || available_at >= Utc::now()
            {
                return Err("fixture jobs were not distinctly ordered and due before claiming".into());
            }
        }
        if scheduled.len() != 3 || outbox.run_once(3).await? != 3 {
            return Err("did not claim all three independent jobs".into());
        }
        let observed_order = dispatched.lock().expect("fixture dispatch recorder").clone();
        if observed_order != expected_order {
            return Err(format!(
                "failed-job position {bad_position} was not exercised in actual dispatch order: {observed_order:?}"
            )
            .into());
        }
        let failed = store
            .inspect(TENANT, bad_id)
            .await?
            .ok_or("missing failed job")?;
        if failed.status != JobStatus::Ready
            || failed.attempts != 1
            || failed.last_error.as_deref() != Some("publish-failed")
        {
            return Err(format!(
                "refused outbox publication must remain retryable, got {failed:?}"
            )
            .into());
        }
        check_successful_jobs(&store, &successful_jobs).await?;
        sqlx::query("UPDATE raddle_jobs SET available_at=now() WHERE id=$1")
            .bind(bad_id)
            .execute(&pool)
            .await?;
        if outbox.run_once(3).await? != 1 {
            return Err("retry must claim only the failed job".into());
        }
        let failed = store
            .inspect(TENANT, bad_id)
            .await?
            .ok_or("missing retried job")?;
        if failed.status != JobStatus::Ready || failed.attempts != 2 {
            return Err("repeated refusal completed or discarded the outbox job".into());
        }
        check_successful_jobs(&store, &successful_jobs).await?;
        let mut expected_dispatches = expected_order.clone();
        expected_dispatches.push(bad.id());
        if *dispatched.lock().expect("fixture dispatch recorder") != expected_dispatches {
            return Err("refusal retry dispatched an already successful job".into());
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    let closed = outbox.into_transport().inner.close().await;
    negative?;
    closed?;

    // Correct only the route configuration. The existing durable envelope,
    // identity and both successful jobs remain unchanged.
    let publisher = connection
        .publisher([("blocked".into(), "accepted".into())])
        .await?;
    let publisher = ObservedPublisher {
        inner: publisher,
        dispatched: Arc::clone(&dispatched),
    };
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
        if corrected.status != JobStatus::Completed
            || corrected.attempts != 3
            || corrected.message_id != bad.id()
        {
            return Err("corrected retry lost its durable identity or did not complete".into());
        }
        check_successful_jobs(&store, &successful_jobs).await?;
        let mut expected_dispatches = expected_order.clone();
        expected_dispatches.extend([bad.id(), bad.id()]);
        if *dispatched.lock().expect("fixture dispatch recorder") != expected_dispatches {
            return Err("corrected retry dispatched an already successful job".into());
        }
        let mut receiver = connection
            .receiver(Source::Queue("accepted".into()), None, 0)
            .await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut deliveries = Vec::new();
        while deliveries.len() < 3 {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err("accepted broker messages did not arrive before the deadline".into());
            }
            // Bounded receives may legally return partial batches.
            let mut received = receiver
                .receive((3 - deliveries.len()) as u32, remaining)
                .await?;
            if received.is_empty() {
                return Err("accepted broker messages were missing".into());
            }
            deliveries.append(&mut received);
        }
        let observed = deliveries
            .iter()
            .map(|message| message.decode().map(|envelope| envelope.id()))
            .collect::<Result<std::collections::HashSet<_>, _>>()?;
        if deliveries.len() != 3 || observed != expected_order.iter().copied().collect() {
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
    let closed = outbox.into_transport().inner.close().await;
    positive?;
    closed?;
    Ok(())
}

#[sqlx::test(migrations = "../crates/raddle-postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn refused_servicebus_publication_keeps_outbox_retryable_and_successes_completed(
    pool: PgPool,
) -> TestResult {
    for bad_position in 0..3 {
        tokio::time::timeout(Duration::from_secs(120), async {
            let mut node = Node::start().await?;
            let connection = Connection::emulator(CONNECTION, node.address).await?;
            let result = Box::pin(outbox_exchange(&connection, pool.clone(), bad_position)).await;
            let closed = connection.close().await;
            node.close().await;
            result?;
            closed?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await??;
    }
    Ok(())
}
