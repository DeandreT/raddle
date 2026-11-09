//! Real PostgreSQL returned cascades and native TCP failed-destination recovery.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::routing::EndpointAlias;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle_host::Host;
use raddle_messaging::effects::{Effect, Effects, HandlerOutput};
use raddle_messaging::routing::{
    MessageDeclaration, MessageRole, MessageSelector, RoutingConfig, RoutingError,
};
use raddle_messaging::scoped::{HandleFuture, LocalOnlyFlush, ScopedHandler};
use raddle_messaging::{
    BoxFuture, BusBuilder, DurableEndpoint, JsonIngress, LocalQueue, MessageBus, MessageContext,
    QueueMode, QueueSettings, TrustedOrigin,
};
use raddle_postgres::{OutboxWorker, RetryPolicy, Scope, Store};
use raddle_tcp::{Consumer, Delivery, Endpoint, Limits, Peer, Publisher, Receiver};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize)]
struct Root(i32);
impl WireMessage for Root {
    const NAME: &'static str = "routing.root";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct Event(i32);
impl WireMessage for Event {
    const NAME: &'static str = "routing.event";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct Projected(i32);
impl WireMessage for Projected {
    const NAME: &'static str = "routing.projected-event";
    const VERSION: u32 = 1;
}
#[derive(Debug, thiserror::Error)]
#[error("fixture operation failed")]
struct Fault;
struct RootHandler;
impl ScopedHandler<Root> for RootHandler {
    type Resources = ();
    type Reply = ();
    type Error = Fault;
    fn create<'a>(
        &'a self,
        _: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Fault>> {
        Box::pin(async { Ok(()) })
    }
    fn handle<'a>(
        &'a self,
        message: &'a Root,
        _: &'a mut (),
        scope: &'a mut Scope<'static>,
        context: &'a MessageContext,
    ) -> HandleFuture<'a, (), (), Fault> {
        Box::pin(async move {
            sqlx::query("INSERT INTO routing_facts(endpoint,number) VALUES('root',$1)")
                .bind(message.0)
                .execute(scope.connection())
                .await
                .map_err(|_| Fault)?;
            let mut effects = Effects::default();
            for intent in context
                .returned_many(&Event(message.0), true)
                .map_err(|_| Fault)?
            {
                effects.push(Effect::Outgoing(intent)).map_err(|_| Fault)?;
            }
            Ok(HandlerOutput { reply: (), effects })
        })
    }
}
struct Business(&'static str);
impl ScopedHandler<Projected> for Business {
    type Resources = ();
    type Reply = ();
    type Error = Fault;
    fn create<'a>(
        &'a self,
        _: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> BoxFuture<'a, Result<(), Fault>> {
        Box::pin(async { Ok(()) })
    }
    fn handle<'a>(
        &'a self,
        message: &'a Projected,
        _: &'a mut (),
        scope: &'a mut Scope<'static>,
        _: &'a MessageContext,
    ) -> HandleFuture<'a, (), (), Fault> {
        Box::pin(async move {
            sqlx::query("INSERT INTO routing_facts(endpoint,number) VALUES($1,$2)")
                .bind(self.0)
                .bind(message.0)
                .execute(scope.connection())
                .await
                .map_err(|_| Fault)?;
            Ok(HandlerOutput::reply(()))
        })
    }
}
fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("routing-tenant", "configured-peer", None).unwrap()
}
fn retry() -> RetryPolicy {
    RetryPolicy::try_new(
        Duration::from_millis(1),
        Duration::from_millis(2),
        Duration::from_secs(15),
    )
    .unwrap()
}
#[derive(Clone)]
struct Shared(Arc<Publisher>);
impl Transport for Shared {
    async fn publish(&self, destination: &str, envelope: &Envelope) -> Result<(), TransportError> {
        self.0.publish(destination, envelope).await
    }
}
struct Inbox {
    bus: MessageBus,
    failing: AtomicI32,
    lost_ack: AtomicI32,
    calls: AtomicUsize,
}
impl Consumer for Inbox {
    async fn accept(&self, _: Peer, envelope: Envelope) -> Delivery {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let Ok(message) = envelope.decode::<Projected>() else {
            return Delivery::Rejected;
        };
        if message.0 == self.failing.load(Ordering::SeqCst) {
            return Delivery::Retry;
        }
        let Ok(bytes) = envelope.to_vec() else {
            return Delivery::Rejected;
        };
        if self
            .bus
            .receive(
                &JsonIngress::new(Default::default()),
                &bytes,
                origin(),
                Utc::now(),
                true,
            )
            .await
            .is_err()
        {
            return Delivery::Retry;
        }
        if message.0 == self.lost_ack.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        Delivery::Accepted
    }
}
fn receiving(store: &Store, name: &'static str) -> (MessageBus, DurableEndpoint) {
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.durable(name, store.clone()).unwrap();
    let mut config = RoutingConfig::default();
    config
        .declare(MessageDeclaration::of::<Projected>(MessageRole::Handled).unwrap())
        .unwrap();
    config.local_route::<Projected>(name).unwrap();
    let mut endpoint = DurableEndpoint::new(store.clone(), name, 1, retry()).unwrap();
    endpoint
        .handle::<Projected, _>(Arc::new(Business(name)), Arc::new(LocalOnlyFlush))
        .unwrap();
    let bus = builder.build_routed(config).unwrap();
    endpoint.message_bus(bus.clone());
    (bus, endpoint)
}

async fn drain(endpoint: &DurableEndpoint) {
    for _ in 0..32 {
        if endpoint.run_once().await.unwrap() == 0 {
            return;
        }
    }
    panic!("bounded durable fixture did not drain");
}

#[sqlx::test(migrations = "../postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn returned_projections_commit_once_retry_only_failed_destinations_and_dedup_lost_ack(
    pool: PgPool,
) {
    sqlx::query(
        "CREATE TABLE routing_facts(endpoint text,number integer,PRIMARY KEY(endpoint,number))",
    )
    .execute(&pool)
    .await
    .unwrap();
    let store = Store::new(pool.clone());
    // Failure position moves through first/middle/last; all current healthy roots
    // and later new work must progress, without retrying successful destinations.
    for failed_position in 1..=3 {
        let base = failed_position * 10;
        let (bus_a, endpoint_a) = receiving(&store, "inbox-a");
        let (bus_b, endpoint_b) = receiving(&store, "inbox-b");
        let peer_a = Arc::new(Inbox {
            bus: bus_a,
            failing: AtomicI32::new(-1),
            lost_ack: AtomicI32::new(-1),
            calls: AtomicUsize::new(0),
        });
        let peer_b = Arc::new(Inbox {
            bus: bus_b,
            failing: AtomicI32::new(base + 4),
            lost_ack: AtomicI32::new(base + 1),
            calls: AtomicUsize::new(0),
        });
        let receiver_a = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Limits::default())
            .await
            .unwrap();
        let receiver_b = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Limits::default())
            .await
            .unwrap();
        let publisher = Shared(Arc::new(
            Publisher::try_new(
                BTreeMap::from([
                    (
                        "a".into(),
                        Endpoint::try_new("127.0.0.1", receiver_a.local_addr().unwrap().port())
                            .unwrap(),
                    ),
                    (
                        "b".into(),
                        Endpoint::try_new("127.0.0.1", receiver_b.local_addr().unwrap().port())
                            .unwrap(),
                    ),
                ]),
                Limits {
                    deadline: Duration::from_secs(5),
                    ..Default::default()
                },
            )
            .unwrap(),
        ));
        let stop = CancellationToken::new();
        let running_a = tokio::spawn(receiver_a.run(peer_a.clone(), stop.clone()));
        let running_b = tokio::spawn(receiver_b.run(peer_b.clone(), stop.clone()));
        let mut host = Host::new(Duration::from_secs(5)).unwrap();
        let shutdown = host.handle();
        let settings = QueueSettings {
            mode: QueueMode::Buffered,
            capacity: 16,
            parallelism: 1,
        };
        let (execution, runner) = LocalQueue::new(settings).unwrap();
        runner.attach(&mut host, "execution").unwrap();
        let (roots, runner) = LocalQueue::new(settings).unwrap();
        runner.attach(&mut host, "roots").unwrap();
        let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
        builder.execution_queue(execution).unwrap();
        builder.local("roots", roots).unwrap();
        builder.remote("a", publisher.clone()).unwrap();
        builder.remote("b", publisher.clone()).unwrap();
        let root = builder
            .scoped::<Root, _>(
                "roots",
                Arc::new(RootHandler),
                store.clone(),
                Arc::new(LocalOnlyFlush),
            )
            .unwrap();
        builder.route::<Root>("roots").unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let mapped = count.clone();
        let mut config = RoutingConfig::default();
        config
            .declare(MessageDeclaration::of::<Event>(MessageRole::Outgoing).unwrap())
            .unwrap();
        config
            .declare(MessageDeclaration::of::<Projected>(MessageRole::PublishOnly).unwrap())
            .unwrap();
        config
            .transform::<Event, Projected>("project", 1, move |message| {
                mapped.fetch_add(1, Ordering::SeqCst);
                if message.0 == base + failed_position {
                    return Err(RoutingError::Projection);
                }
                Ok(Projected(message.0))
            })
            .unwrap();
        config
            .rule(
                "fanout",
                "Independent durable destinations",
                MessageSelector::exact::<Projected>().unwrap(),
                vec![
                    EndpointAlias::try_new("a").unwrap(),
                    EndpointAlias::try_new("b").unwrap(),
                ],
            )
            .unwrap();
        let bus = builder.build_routed(config).unwrap();
        let hosting = tokio::spawn(host.run());
        for position in 1..=3 {
            let context = MessageContext::new(
                origin(),
                Envelope::encode(&Root(base + position)).unwrap(),
                Utc::now(),
            );
            assert_eq!(
                bus.invoke_scoped(&root, context).await.is_ok(),
                position != failed_position
            );
        }
        // Actual bus Send also drives the normal host-owned scoped cascade.
        bus.send(&Root(base + 4), origin()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop { let ready: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM routing_facts WHERE endpoint='root' AND number=$1)").bind(base + 4).fetch_one(&pool).await.unwrap();
                if ready { break; } tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.unwrap();
        let before = count.load(Ordering::SeqCst);
        let worker = OutboxWorker::new(store.clone(), publisher, retry());
        assert_eq!(worker.run_once_for("a", 32).await.unwrap(), 3);
        assert_eq!(worker.run_once_for("b", 32).await.unwrap(), 3);
        let a_calls = peer_a.calls.load(Ordering::SeqCst);
        assert_eq!(a_calls, 3);
        drain(&endpoint_a).await;
        drain(&endpoint_b).await;
        // One B failure has no custody; a lost ACK may already have committed B.
        let jobs_before: i64 = sqlx::query_scalar("SELECT count(*) FROM raddle_jobs WHERE kind='local' AND destination='inbox-b' AND status='completed'")
            .fetch_one(&pool).await.unwrap();
        assert!(jobs_before >= 1);
        peer_b.failing.store(-1, Ordering::SeqCst);
        peer_b.lost_ack.store(-1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(5)).await;
        let expected_retry = if failed_position == 1 { 1 } else { 2 };
        assert_eq!(worker.run_once_for("b", 32).await.unwrap(), expected_retry);
        assert_eq!(worker.run_once_for("a", 32).await.unwrap(), 0);
        drain(&endpoint_b).await;
        assert_eq!(peer_a.calls.load(Ordering::SeqCst), a_calls);
        assert_eq!(
            count.load(Ordering::SeqCst),
            before,
            "durable retry must not project again"
        );
        let facts: Vec<(String, i32)> = sqlx::query_as("SELECT endpoint,number FROM routing_facts WHERE number>$1 AND number<=$2 ORDER BY endpoint,number")
            .bind(base).bind(base + 4).fetch_all(&pool).await.unwrap();
        assert_eq!(facts.len(), 9);
        assert!(
            facts
                .iter()
                .all(|(_, number)| *number != base + failed_position)
        );
        let duplicates: i64 = sqlx::query_scalar("SELECT count(*) FROM (SELECT source,tenant,destination,kind,message_id FROM raddle_jobs GROUP BY source,tenant,destination,kind,message_id HAVING count(*)>1) x").fetch_one(&pool).await.unwrap();
        assert_eq!(duplicates, 0);
        let completed: bool = sqlx::query_scalar(
            "SELECT bool_and(status='completed') FROM raddle_jobs WHERE kind='remote'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(completed);
        let once: bool = sqlx::query_scalar(
            "SELECT bool_and(attempts=1) FROM raddle_jobs WHERE kind='remote' AND destination='a'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(once);
        // New work after corrected recovery remains independently deliverable.
        let context = MessageContext::new(
            origin(),
            Envelope::encode(&Root(base + 5)).unwrap(),
            Utc::now(),
        );
        bus.invoke_scoped(&root, context).await.unwrap();
        assert_eq!(worker.run_once_for("a", 32).await.unwrap(), 1);
        assert_eq!(worker.run_once_for("b", 32).await.unwrap(), 1);
        drain(&endpoint_a).await;
        drain(&endpoint_b).await;
        let new_facts: i64 =
            sqlx::query_scalar("SELECT count(*) FROM routing_facts WHERE number=$1")
                .bind(base + 5)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(new_facts, 3);
        shutdown.shutdown();
        hosting.await.unwrap().unwrap();
        stop.cancel();
        running_a.await.unwrap().unwrap();
        running_b.await.unwrap().unwrap();
    }
}
