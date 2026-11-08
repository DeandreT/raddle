//! The shipping native bus adapter over the existing public RabbitMQ transport.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use futures_util::FutureExt;
use lapin::options::QueueDeleteOptions;
use raddle::wire::{Envelope, WireMessage};
use raddle::{Request, RuntimeBuilder};
use raddle_messaging::{
    BusBuilder, BusError, InvokeError, InvokeOptions, MessageContext, RemoteResponder,
    ReplyHubInvoker, TrustedOrigin,
};
use raddle_rabbitmq::{Connection, Queue, Route, Topology};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, Semaphore};

#[derive(Serialize, Deserialize)]
struct Query(u32);
impl WireMessage for Query {
    const NAME: &'static str = "test.rabbit-rpc-query";
    const VERSION: u32 = 1;
}
#[derive(Debug, Serialize, Deserialize)]
struct Reply(u32);
impl WireMessage for Reply {
    const NAME: &'static str = "test.rabbit-rpc-reply";
    const VERSION: u32 = 1;
}
#[derive(Debug, thiserror::Error)]
#[error("negative acknowledgement test")]
struct DomainError;
impl Request for Query {
    type Reply = Reply;
    type Error = DomainError;
}
fn origin(source: &str, tenant: &str) -> TrustedOrigin {
    TrustedOrigin::authenticated(tenant, source, None).unwrap()
}
fn route(alias: &str, queue: &str) -> Route {
    Route {
        alias: alias.into(),
        exchange: String::new(),
        routing_key: queue.into(),
    }
}

#[tokio::test]
#[ignore = "requires disposable RabbitMQ via RADDLE_RABBITMQ_TEST_URI"]
async fn real_rabbit_bus_correlates_reordered_replies_failure_timeout_cancellation_and_late_duplicates()
 {
    let uri = std::env::var("RADDLE_RABBITMQ_TEST_URI")
        .expect("explicit disposable RabbitMQ URL required");
    let connection = Connection::connect(&uri).await.unwrap();
    let run = uuid::Uuid::new_v4();
    let requests = format!("raddle-bus-{run}-requests");
    let replies = format!("raddle-bus-{run}-replies");
    let topology = Topology::new(
        [],
        [
            Queue {
                name: requests.clone(),
                durable: true,
            },
            Queue {
                name: replies.clone(),
                durable: true,
            },
        ],
        [],
    )
    .unwrap();
    eprintln!("run-owned queues: {requests}, {replies}");
    let outcome = AssertUnwindSafe(async {
        tokio::time::timeout(Duration::from_secs(60), async {
            connection.declare_topology(&topology).await.unwrap();
            let remote = Arc::new(
                ReplyHubInvoker::new(
                    connection
                        .publisher([route("rpc-peer", &requests)])
                        .await
                        .unwrap(),
                    8,
                    "rpc-replies",
                    [("rpc-peer".into(), "peer-auth".into())],
                )
                .unwrap(),
            );
            let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
            builder
                .remote(
                    "rpc-peer",
                    connection
                        .publisher([route("rpc-peer", &requests)])
                        .await
                        .unwrap(),
                )
                .unwrap();
            builder.route::<Query>("rpc-peer").unwrap();
            builder.remote_invoker(remote.clone(), true);
            let bus = builder.build();
            let mut runtime = RuntimeBuilder::default();
            runtime
                .handle(|query: Query| async move {
                    if query.0 == 13 {
                        Err(DomainError)
                    } else {
                        Ok(Reply(query.0 * 2))
                    }
                })
                .unwrap();
            let mut responder = RemoteResponder::new(
                connection
                    .publisher([route("rpc-replies", &replies)])
                    .await
                    .unwrap(),
                runtime.build(),
                ["rpc-replies".into()],
            )
            .unwrap();
            responder.handle::<Query>().unwrap();
            let responder = Arc::new(responder);
            let mut incoming = connection.receiver(&requests, 8).await.unwrap();
            let mut reply_receiver = connection.receiver(&replies, 8).await.unwrap();
            let timeout_entered = Arc::new(Notify::new());
            let cancel_entered = Arc::new(Notify::new());
            let release_late = Arc::new(Semaphore::new(0));
            let peer_gate = Arc::clone(&release_late);
            let timed = Arc::clone(&timeout_entered);
            let cancelled = Arc::clone(&cancel_entered);
            let peer = tokio::spawn(async move {
                let mut handling = tokio::task::JoinSet::new();
                for _ in 0..5 {
                    let mut delivery = incoming
                        .receive(Duration::from_secs(10))
                        .await
                        .unwrap()
                        .unwrap();
                    let envelope = delivery.decode().unwrap();
                    let value = envelope.decode::<Query>().unwrap().0;
                    let responder = Arc::clone(&responder);
                    let gate = Arc::clone(&peer_gate);
                    let timed = Arc::clone(&timed);
                    let cancelled = Arc::clone(&cancelled);
                    handling.spawn(async move {
                        if value == 21 {
                            tokio::time::sleep(Duration::from_millis(30)).await;
                        }
                        if value == 99 {
                            timed.notify_one();
                            gate.acquire().await.unwrap().forget();
                        }
                        if value == 100 {
                            cancelled.notify_one();
                            gate.acquire().await.unwrap().forget();
                        }
                        let context = MessageContext::new(
                            origin("client-auth", "tenant-a"),
                            envelope,
                            Utc::now(),
                        );
                        responder.respond(&context).await.unwrap();
                        delivery.complete().await.unwrap();
                    });
                }
                while let Some(result) = handling.join_next().await {
                    result.unwrap();
                }
                incoming.close().await.unwrap();
            });
            let received = Arc::new(Mutex::new(Vec::new()));
            let observed = Arc::clone(&received);
            let registry = Arc::clone(&remote);
            let replies_task = tokio::spawn(async move {
                for _ in 0..5 {
                    let mut delivery = reply_receiver
                        .receive(Duration::from_secs(10))
                        .await
                        .unwrap()
                        .unwrap();
                    let envelope = delivery.decode().unwrap();
                    assert!(
                        !registry
                            .accept_reply(&origin("forged-peer", "tenant-a"), envelope.clone())
                            .unwrap()
                    );
                    assert!(
                        !registry
                            .accept_reply(&origin("peer-auth", "wrong-tenant"), envelope.clone())
                            .unwrap()
                    );
                    let accepted = registry
                        .accept_reply(&origin("peer-auth", "tenant-a"), envelope.clone())
                        .unwrap();
                    assert!(
                        !registry
                            .accept_reply(&origin("peer-auth", "tenant-a"), envelope)
                            .unwrap()
                    );
                    observed.lock().unwrap().push(accepted);
                    delivery.complete().await.unwrap();
                }
                reply_receiver.close().await.unwrap();
            });
            let (a, b) = tokio::join!(
                bus.invoke(Query(21), origin("client-auth", "tenant-a")),
                bus.invoke(Query(22), origin("client-auth", "tenant-a"))
            );
            assert_eq!(a.unwrap().0, 42);
            assert_eq!(b.unwrap().0, 44);
            assert!(matches!(
                bus.invoke(Query(13), origin("client-auth", "tenant-a"))
                    .await,
                Err(InvokeError::Bus(BusError::RemoteFailure))
            ));
            assert_eq!(remote.pending().unwrap(), 0);
            let timed_bus = bus.clone();
            let timeout_call = tokio::spawn(async move {
                timed_bus
                    .invoke_with(
                        Query(99),
                        origin("client-auth", "tenant-a"),
                        InvokeOptions {
                            remote_timeout: Duration::from_secs(2),
                            ..Default::default()
                        },
                    )
                    .await
            });
            timeout_entered.notified().await;
            assert!(matches!(
                timeout_call.await.unwrap(),
                Err(InvokeError::TimedOut)
            ));
            assert_eq!(remote.pending().unwrap(), 0);
            release_late.add_permits(1);
            let context = MessageContext::new(
                origin("client-auth", "tenant-a"),
                Envelope::encode(&Query(100)).unwrap(),
                Utc::now(),
            );
            let cancellation = context.cancellation().clone();
            let cancelled_bus = bus.clone();
            let cancel_call = tokio::spawn(async move {
                cancelled_bus
                    .invoke_context(Query(100), context, InvokeOptions::default())
                    .await
            });
            cancel_entered.notified().await;
            cancellation.cancel();
            assert!(matches!(
                cancel_call.await.unwrap(),
                Err(InvokeError::Cancelled)
            ));
            assert_eq!(remote.pending().unwrap(), 0);
            release_late.add_permits(1);
            peer.await.unwrap();
            replies_task.await.unwrap();
            assert_eq!(
                received
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|accepted| **accepted)
                    .count(),
                3
            );
            assert_eq!(
                received
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|accepted| !**accepted)
                    .count(),
                2
            );
            assert_eq!(remote.pending().unwrap(), 0);
        })
        .await
        .expect("bounded native broker request/reply qualification");
    })
    .catch_unwind()
    .await;
    // Delete only the two names captured for this run, even if an assertion fails.
    let cleanup = lapin::Connection::connect(&uri, lapin::ConnectionProperties::default())
        .await
        .unwrap();
    let channel = cleanup.create_channel().await.unwrap();
    for name in [&requests, &replies] {
        let deleted = channel
            .queue_delete(name.clone().into(), QueueDeleteOptions::default())
            .await;
        // A failed topology declaration may not have created either name.
        // Success requires confirmed deletion of both captured queues.
        if outcome.is_ok() {
            deleted.unwrap();
        }
    }
    cleanup.close(200, "test cleanup".into()).await.unwrap();
    let closed = connection.close().await;
    if outcome.is_ok() {
        closed.unwrap();
    }
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
