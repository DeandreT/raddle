//! Concrete wire requests reach declared local family behavior through MessageBus.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle::{ExecutionError, Request, RequestFamily, RuntimeBuilder};
use raddle_messaging::{
    BoxFuture, BusBuilder, BusError, InvokeError, InvokeOptions, MessageContext, RemoteInvoker,
    TrustedOrigin,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Add(u32);
#[derive(Serialize, Deserialize)]
struct Multiply(u32);
#[derive(Serialize, Deserialize)]
struct Missing(u32);
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Answer(u32);
#[derive(Debug, PartialEq, thiserror::Error)]
#[error("value {0} rejected")]
struct Rejected(u32);
impl WireMessage for Add {
    const NAME: &'static str = "families.add";
    const VERSION: u32 = 1;
}
impl WireMessage for Multiply {
    const NAME: &'static str = "families.multiply";
    const VERSION: u32 = 1;
}
impl WireMessage for Missing {
    const NAME: &'static str = "families.missing";
    const VERSION: u32 = 1;
}
impl WireMessage for Answer {
    const NAME: &'static str = "families.answer";
    const VERSION: u32 = 1;
}
impl Request for Add {
    type Reply = Answer;
    type Error = Rejected;
}
impl Request for Multiply {
    type Reply = Answer;
    type Error = Rejected;
}
impl Request for Missing {
    type Reply = Answer;
    type Error = Rejected;
}
enum Arithmetic {
    Add(Add),
    Multiply(Multiply),
}
impl Arithmetic {
    fn value(self) -> u32 {
        match self {
            Self::Add(Add(value)) => value + 1,
            Self::Multiply(Multiply(value)) => value * 2,
        }
    }
}
fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("tenant-a", "family-test", Some("caller".into())).unwrap()
}

struct Peer(Arc<AtomicUsize>);
impl Transport for Peer {
    async fn publish(&self, _: &str, _: &Envelope) -> Result<(), TransportError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(TransportError::Rejected)
    }
}
impl RemoteInvoker for Peer {
    fn request<'a>(
        &'a self,
        _: &'a str,
        context: &'a MessageContext,
        reply: &'a str,
    ) -> BoxFuture<'a, Result<Envelope, BusError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            assert_eq!(context.envelope().contract(), Missing::NAME);
            assert_eq!(reply, Answer::NAME);
            Err(BusError::RemoteFailure)
        })
    }
}

struct Behavior {
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl Behavior {
    fn record(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}
impl Drop for Behavior {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn concrete_wire_requests_and_reply_use_one_local_behavior_before_remote_routes() {
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let behavior = Behavior {
        calls: Arc::clone(&calls),
        drops: Arc::clone(&drops),
    };
    let mut family = RequestFamily::new(move |message: Arithmetic, _| {
        behavior.record();
        async move {
            let value = message.value();
            if value == 0 {
                Err(Rejected(value))
            } else {
                Ok(Answer(value))
            }
        }
    });
    family.member::<Add>(Arithmetic::Add).unwrap();
    family.member::<Multiply>(Arithmetic::Multiply).unwrap();
    let mut runtime = RuntimeBuilder::default();
    runtime.handle_family(family).unwrap();
    let remote_calls = Arc::new(AtomicUsize::new(0));
    let mut builder = BusBuilder::new(runtime.build());
    builder
        .remote("peer", Peer(Arc::clone(&remote_calls)))
        .unwrap();
    builder.remote_invoker(Arc::new(Peer(Arc::clone(&remote_calls))), true);
    builder.route::<Add>("peer").unwrap();
    builder.route::<Multiply>("peer").unwrap();
    builder.route::<Missing>("peer").unwrap();
    let bus = builder.build();
    // The wire carries concrete contracts, never the family enum or Rust type names.
    let add = Envelope::encode(&Add(4)).unwrap();
    let multiply = Envelope::encode(&Multiply(5)).unwrap();
    assert_ne!(add.contract(), multiply.contract());
    let reply = bus
        .invoke(add.decode::<Add>().unwrap(), origin())
        .await
        .unwrap();
    assert_eq!(
        Envelope::encode(&reply)
            .unwrap()
            .decode::<Answer>()
            .unwrap(),
        Answer(5)
    );
    assert_eq!(
        bus.invoke(multiply.decode::<Multiply>().unwrap(), origin())
            .await
            .unwrap(),
        Answer(10)
    );
    assert!(matches!(
        bus.invoke(Multiply(0), origin()).await,
        Err(InvokeError::Local(ExecutionError::Handler(Rejected(0))))
    ));
    assert_eq!(bus.invoke(Add(8), origin()).await.unwrap(), Answer(9));
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert_eq!(remote_calls.load(Ordering::SeqCst), 0);
    // An undeclared concrete is not accidentally treated as a family member.
    assert!(matches!(
        bus.invoke(Missing(1), origin()).await,
        Err(InvokeError::Bus(BusError::RemoteFailure))
    ));
    assert_eq!(remote_calls.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(bus);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn bus_family_deadline_cancellation_and_caller_drop_release_normal_admission() {
    let active = Arc::new(AtomicUsize::new(0));
    let current = Arc::clone(&active);
    let mut family = RequestFamily::new(move |message: Arithmetic, _| {
        let current = Arc::clone(&current);
        async move {
            current.fetch_add(1, Ordering::SeqCst);
            let _guard = Active(current);
            let value = message.value();
            if value == 1 {
                std::future::pending::<()>().await;
            }
            Ok::<_, Rejected>(Answer(value))
        }
    });
    family.member::<Add>(Arithmetic::Add).unwrap();
    family.member::<Multiply>(Arithmetic::Multiply).unwrap();
    let mut runtime = RuntimeBuilder::try_new(1).unwrap();
    runtime.handle_family(family).unwrap();
    let bus = BusBuilder::new(runtime.build()).build();
    assert!(matches!(
        bus.invoke_with(
            Add(0),
            origin(),
            InvokeOptions {
                timeout: Some(Duration::from_millis(10)),
                ..Default::default()
            }
        )
        .await,
        Err(InvokeError::TimedOut)
    ));
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(bus.invoke(Multiply(3), origin()).await.unwrap(), Answer(6));
    let context = MessageContext::new(origin(), Envelope::encode(&Add(0)).unwrap(), Utc::now());
    let cancellation = context.cancellation().clone();
    {
        let mut call =
            std::pin::pin!(bus.invoke_context(Add(0), context, InvokeOptions::default()));
        tokio::select! {
            biased;
            result = &mut call => panic!("unexpected result: {result:?}"),
            () = std::future::ready(()) => {},
        }
        assert_eq!(active.load(Ordering::SeqCst), 1);
        cancellation.cancel();
        assert!(matches!(call.await, Err(InvokeError::Cancelled)));
    }
    assert_eq!(active.load(Ordering::SeqCst), 0);
    {
        let mut call = std::pin::pin!(bus.invoke(Add(0), origin()));
        tokio::select! {
            biased;
            result = &mut call => panic!("unexpected result: {result:?}"),
            () = std::future::ready(()) => {},
        }
        assert_eq!(active.load(Ordering::SeqCst), 1);
    }
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(bus.invoke(Multiply(4), origin()).await.unwrap(), Answer(8));
}
