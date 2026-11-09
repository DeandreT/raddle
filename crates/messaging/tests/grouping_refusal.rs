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

struct AdapterReply(Envelope, Arc<AtomicUsize>);
static RESPONSE_DECODES: AtomicUsize = AtomicUsize::new(0);
#[derive(Serialize)]
struct AdapterAnswer;
impl<'de> Deserialize<'de> for AdapterAnswer {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        RESPONSE_DECODES.fetch_add(1, Ordering::SeqCst);
        <()>::deserialize(deserializer)?;
        Ok(Self)
    }
}
impl WireMessage for AdapterAnswer {
    const NAME: &'static str = "group.adapter-answer";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct AdapterCall;
impl WireMessage for AdapterCall {
    const NAME: &'static str = "group.adapter-call";
    const VERSION: u32 = 1;
}
impl raddle::Request for AdapterCall {
    type Reply = AdapterAnswer;
    type Error = std::convert::Infallible;
}
impl raddle_messaging::RemoteInvoker for AdapterReply {
    fn request<'a>(
        &'a self,
        _: &'a str,
        _: &'a MessageContext,
        _: &'a str,
    ) -> raddle_messaging::BoxFuture<'a, Result<Envelope, BusError>> {
        self.1.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(self.0.clone()) })
    }
}

#[tokio::test]
async fn custom_rpc_adapter_replies_refuse_all_dispatch_forms_before_typed_decode() {
    let calls = Arc::new(AtomicUsize::new(0));
    for dispatch in [
        None,
        Some(Dispatch::resolved(None, None)),
        Some(Dispatch::resolved(Some(GroupId::new("")), None)),
        Some(Dispatch::resolved(Some(GroupId::new("g")), None)),
        Some(Dispatch::resolved(None, Some("key".into()))),
    ] {
        let mut reply = Envelope::encode(&AdapterAnswer).unwrap();
        if let Some(dispatch) = dispatch.clone() {
            reply = reply.with_dispatch(dispatch).unwrap();
        }
        let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
        builder.remote("peer", Publisher(calls.clone())).unwrap();
        builder.route::<AdapterCall>("peer").unwrap();
        builder.remote_invoker(Arc::new(AdapterReply(reply, calls.clone())), true);
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            builder.build().invoke(AdapterCall, origin()),
        )
        .await
        .unwrap();
        if dispatch.is_some() {
            assert!(matches!(
                result,
                Err(raddle_messaging::InvokeError::Bus(BusError::Wire(
                    raddle::wire::WireError::Invalid(
                        "native dispatch is unsupported by this profile"
                    )
                )))
            ));
        } else {
            assert!(result.is_ok());
        }
        assert_eq!(RESPONSE_DECODES.load(Ordering::SeqCst), 1);
    }
    // Only the custom adapter ran, never its publication transport.
    assert_eq!(calls.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn direct_only_partitioned_handler_refuses_invoke_and_scoped_but_send_to_owns_slots() {
    use raddle_host::Host;
    use tokio::sync::{Semaphore, mpsc};
    let callbacks = Arc::new(AtomicUsize::new(0));
    let called = callbacks.clone();
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(move |_: Work| {
            called.fetch_add(1, Ordering::SeqCst);
            async { Ok(Work) }
        })
        .unwrap();
    let slots = SlotSet::new("direct", 1).unwrap();
    let (queue, runner) = LocalQueue::partitioned(
        QueueSettings {
            mode: QueueMode::Buffered,
            capacity: 2,
            parallelism: 2,
        },
        slots.clone(),
    )
    .unwrap();
    let observed = queue.clone();
    let mut builder = BusBuilder::new(runtime.build());
    builder.local("direct", queue).unwrap();
    let gate = Arc::new(Semaphore::new(0));
    let held = gate.clone();
    let (tx, mut rx) = mpsc::channel(2);
    builder
        .handle::<Work, _, _>("direct", move |_, _| {
            let tx = tx.clone();
            let gate = held.clone();
            let slots = slots.clone();
            async move {
                assert!(slots.try_reserve(0).is_none(), "handler must own its slot");
                tx.send(()).await.unwrap();
                gate.acquire().await.unwrap().forget();
                Ok(())
            }
        })
        .unwrap();
    let bus = builder.build();
    let explanation = bus.explain::<Work>().unwrap();
    assert!(explanation.routes.is_empty());
    assert!(explanation.invocation.is_empty());
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "direct").unwrap();
    let shutdown = host.handle();
    let task = tokio::spawn(host.run());
    bus.send_to("direct", &Work, origin()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        bus.invoke(Work, origin()).await,
        Err(raddle_messaging::InvokeError::Bus(BusError::Invalid(
            "grouped contracts require buffered Send/Publish execution"
        )))
    ));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://invalid:invalid@127.0.0.1:1/invalid")
        .unwrap();
    pool.close().await;
    assert!(matches!(
        bus.execute::<Work, _>(
            Arc::new(Scoped(callbacks.clone())),
            MessageContext::new(origin(), Envelope::encode(&Work).unwrap(), Utc::now()),
            raddle_postgres::Store::new(pool),
            Arc::new(raddle_messaging::scoped::LocalOnlyFlush),
        )
        .await,
        Err(raddle_messaging::scoped::Failure::Wire(_))
    ));
    assert_eq!(callbacks.load(Ordering::SeqCst), 0);
    bus.send_to("direct", &Work, origin()).await.unwrap();
    assert_eq!(observed.snapshot().active, 1);
    assert_eq!(observed.snapshot().buffered, 1);
    assert!(rx.try_recv().is_err());
    gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    gate.add_permits(1);
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(observed.snapshot().completed, 2);
    assert_eq!(observed.snapshot().active, 0);
    assert_eq!(observed.snapshot().buffered, 0);
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
