//! Unsupported profiles refuse before actual buffer or transport admission.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::grouping::{Dispatch, DispatchOptions, GroupId};
use raddle::sequencing::SlotSet;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle_messaging::{
    BusBuilder, BusError, EndpointPolicies, LocalQueue, MessageContext, PolicyQueueBuilder,
    QueueMode, QueueSettings, TrustedOrigin,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Work;
impl WireMessage for Work {
    const NAME: &'static str = "group.work";
    const VERSION: u32 = 1;
}
impl raddle::Request for Work {
    type Reply = Work;
    type Error = std::convert::Infallible;
}

struct Scoped(Arc<AtomicUsize>);
impl raddle_messaging::scoped::ScopedHandler<Work> for Scoped {
    type Resources = ();
    type Reply = ();
    type Error = std::convert::Infallible;
    fn create<'a>(
        &'a self,
        _: &'a mut raddle_postgres::Scope<'static>,
        _: &'a MessageContext,
    ) -> raddle_messaging::BoxFuture<'a, Result<(), Self::Error>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
    fn handle<'a>(
        &'a self,
        _: &'a Work,
        _: &'a mut (),
        _: &'a mut raddle_postgres::Scope<'static>,
        _: &'a MessageContext,
    ) -> raddle_messaging::scoped::HandleFuture<'a, (), (), Self::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(raddle_messaging::effects::HandlerOutput::reply(())) })
    }
}

#[tokio::test]
async fn direct_scoped_execution_and_rpc_refuse_before_factory_waiter_or_publication() {
    use raddle_messaging::{RemoteInvoker, RemoteResponder, ReplyHubInvoker};
    let calls = Arc::new(AtomicUsize::new(0));
    let envelope = Envelope::encode(&Work)
        .unwrap()
        .with_dispatch(Dispatch::resolved(Some(GroupId::new("g")), None))
        .unwrap();
    let context = MessageContext::new(origin(), envelope.clone(), Utc::now());
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://invalid:invalid@127.0.0.1:1/invalid")
        .unwrap();
    pool.close().await;
    let bus = BusBuilder::new(RuntimeBuilder::default().build()).build();
    assert!(matches!(
        bus.execute::<Work, _>(
            Arc::new(Scoped(calls.clone())),
            context.clone(),
            raddle_postgres::Store::new(pool),
            Arc::new(raddle_messaging::scoped::LocalOnlyFlush)
        )
        .await,
        Err(raddle_messaging::scoped::Failure::Wire(_))
    ));
    let invoker = ReplyHubInvoker::new(
        Publisher(calls.clone()),
        4,
        "replies",
        vec![("orders".into(), "peer".into())],
    )
    .unwrap();
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            invoker.request("orders", &context, Work::NAME)
        )
        .await
        .unwrap(),
        Err(BusError::Wire(_))
    ));
    assert_eq!(invoker.pending().unwrap(), 0);
    assert!(matches!(
        invoker.accept_reply(&origin(), envelope),
        Err(BusError::Wire(_))
    ));
    let responder = RemoteResponder::new(
        Publisher(calls.clone()),
        RuntimeBuilder::default().build(),
        vec!["replies".into()],
    )
    .unwrap();
    assert!(matches!(
        responder.respond(&context).await,
        Err(BusError::Wire(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn duplicate_slot_domain_with_independent_owner_is_refused_atomically() {
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    let settings = QueueSettings {
        mode: QueueMode::Buffered,
        capacity: 2,
        parallelism: 2,
    };
    let slots = SlotSet::new("shared", 7).unwrap();
    let (first, _) = LocalQueue::partitioned(settings, slots.clone()).unwrap();
    builder.local("first", first).unwrap();
    for count in [7, 9] {
        let (other, _) =
            LocalQueue::partitioned(settings, SlotSet::new("shared", count).unwrap()).unwrap();
        assert!(builder.local("second", other).is_err());
    }
    let (second, _) = LocalQueue::partitioned(settings, slots).unwrap();
    builder.local("second", second).unwrap();
}
fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("tenant", "source", None).unwrap()
}
#[derive(Clone)]
struct Publisher(Arc<AtomicUsize>);
impl Transport for Publisher {
    async fn publish(&self, _: &str, _: &Envelope) -> Result<(), TransportError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn direct_policy_admission_refuses_without_retained_identity_or_attempt() {
    let (queue, _runner) = PolicyQueueBuilder::new(
        "policy",
        8,
        Duration::from_secs(1),
        EndpointPolicies::new(Default::default()),
    )
    .unwrap()
    .build()
    .unwrap();
    let envelope = Envelope::encode(&Work)
        .unwrap()
        .with_dispatch(Dispatch::resolved(Some(GroupId::new("g")), None))
        .unwrap();
    assert!(
        queue
            .submit(MessageContext::new(origin(), envelope, Utc::now()))
            .await
            .is_err()
    );
    assert!(queue.deliveries().unwrap().is_empty());
}

#[tokio::test]
async fn mixed_durable_remote_or_policy_route_refuses_before_any_sibling_ack() {
    for unsupported in ["durable", "remote", "policy"] {
        let calls = Arc::new(AtomicUsize::new(0));
        let (queue, _runner) = LocalQueue::partitioned(
            QueueSettings {
                mode: QueueMode::Buffered,
                capacity: 8,
                parallelism: 2,
            },
            SlotSet::new("local", 7).unwrap(),
        )
        .unwrap();
        let observed = queue.clone();
        let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
        builder.local("local", queue).unwrap();
        builder
            .handle::<Work, _, _>("local", |_, _| async { Ok(()) })
            .unwrap();
        match unsupported {
            "durable" => {
                // Closed lazy pool: any accidental SQL admission is a Storage
                // failure, never the expected pre-admission Invalid refusal.
                let pool = sqlx::postgres::PgPoolOptions::new()
                    .connect_lazy("postgres://invalid:invalid@127.0.0.1:1/invalid")
                    .unwrap();
                pool.close().await;
                builder
                    .durable("unsupported", raddle_postgres::Store::new(pool))
                    .unwrap();
            }
            "remote" => builder
                .remote("unsupported", Publisher(calls.clone()))
                .unwrap(),
            _ => {
                let (queue, _runner) = PolicyQueueBuilder::new(
                    "unsupported",
                    8,
                    Duration::from_secs(1),
                    EndpointPolicies::new(Default::default()),
                )
                .unwrap()
                .build()
                .unwrap();
                builder.policy_queue(queue).unwrap();
            }
        }
        builder.route::<Work>("local").unwrap();
        builder.route::<Work>("unsupported").unwrap();
        let bus = builder.build();
        assert!(matches!(
            bus.send(&Work, origin()).await,
            Err(BusError::Invalid(_))
        ));
        assert_eq!(observed.snapshot().buffered, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            bus.send_to_with_options(
                "unsupported",
                &Work,
                origin(),
                DispatchOptions {
                    group: Some(GroupId::new("")),
                    partition_key: None
                }
            )
            .await,
            Err(BusError::Invalid(_))
        ));
        let context = MessageContext::new(
            origin(),
            Envelope::encode(&Work)
                .unwrap()
                .with_dispatch(Dispatch::resolved(None, None))
                .unwrap(),
            Utc::now(),
        );
        assert!(bus.intents(&Work, &context, true).is_err());
    }
}
