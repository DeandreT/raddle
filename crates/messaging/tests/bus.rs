//! Application routing, real loopback invocation and bounded queue behavior.
#![allow(clippy::unwrap_used)]

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle::{Request, RuntimeBuilder};
use raddle_host::Host;
use raddle_messaging::{
    BoxFuture, BusBuilder, BusError, InvokeError, InvokeOptions, LocalQueue, MessageContext,
    QueueError, QueueMode, QueueSettings, RemoteInvoker, TrustedOrigin,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, Semaphore};

#[derive(Serialize, Deserialize)]
struct Double(u32);
impl WireMessage for Double {
    const NAME: &'static str = "test.double";
    const VERSION: u32 = 1;
}
#[derive(Debug, Serialize, Deserialize)]
struct Doubled(u32);
impl WireMessage for Doubled {
    const NAME: &'static str = "test.doubled";
    const VERSION: u32 = 1;
}
impl Request for Double {
    type Reply = Doubled;
    type Error = Infallible;
}
fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("tenant-a", "test", Some("caller".into())).unwrap()
}

struct UnusedTransport;
impl Transport for UnusedTransport {
    async fn publish(&self, _: &str, _: &Envelope) -> Result<(), TransportError> {
        Ok(())
    }
}

struct TcpPeer {
    address: std::net::SocketAddr,
    active: Arc<AtomicUsize>,
}

struct NeverReplies;
impl RemoteInvoker for NeverReplies {
    fn request<'a>(
        &'a self,
        _: &'a str,
        _: &'a MessageContext,
        _: &'a str,
    ) -> BoxFuture<'a, Result<Envelope, BusError>> {
        Box::pin(std::future::pending())
    }
}

#[tokio::test(start_paused = true)]
async fn default_execution_is_sixty_seconds_and_remote_response_default_is_five() {
    let mut local = RuntimeBuilder::default();
    local
        .handle(|_: Double| async {
            std::future::pending::<()>().await;
            Ok(Doubled(0))
        })
        .unwrap();
    let bus = BusBuilder::new(local.build()).build();
    let start = tokio::time::Instant::now();
    assert!(matches!(
        bus.invoke(Double(1), origin()).await,
        Err(InvokeError::TimedOut)
    ));
    assert_eq!(start.elapsed(), Duration::from_secs(60));
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.remote("peer", UnusedTransport).unwrap();
    builder.route::<Double>("peer").unwrap();
    builder.remote_invoker(Arc::new(NeverReplies), true);
    let bus = builder.build();
    let start = tokio::time::Instant::now();
    assert!(matches!(
        bus.invoke(Double(1), origin()).await,
        Err(InvokeError::TimedOut)
    ));
    assert_eq!(start.elapsed(), Duration::from_secs(5));
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.remote_invoker(Arc::new(NeverReplies), false);
    assert!(matches!(
        builder.build().invoke(Double(1), origin()).await,
        Err(InvokeError::Bus(BusError::RemoteDisabled))
    ));
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl RemoteInvoker for TcpPeer {
    fn request<'a>(
        &'a self,
        _: &'a str,
        context: &'a MessageContext,
        _: &'a str,
    ) -> BoxFuture<'a, Result<Envelope, BusError>> {
        Box::pin(async move {
            self.active.fetch_add(1, Ordering::SeqCst);
            let _active = Active(Arc::clone(&self.active));
            let mut stream = TcpStream::connect(self.address)
                .await
                .map_err(|_| TransportError::Transient)?;
            let body = context.envelope().to_vec()?;
            stream
                .write_u32(body.len() as u32)
                .await
                .map_err(|_| TransportError::Transient)?;
            stream
                .write_all(&body)
                .await
                .map_err(|_| TransportError::Transient)?;
            if stream
                .read_u8()
                .await
                .map_err(|_| TransportError::Transient)?
                != 0
            {
                return Err(BusError::RemoteFailure);
            }
            let size = stream
                .read_u32()
                .await
                .map_err(|_| TransportError::Transient)?;
            if size > 1_048_576 {
                return Err(TransportError::Rejected.into());
            }
            let mut body = vec![0; size as usize];
            stream
                .read_exact(&mut body)
                .await
                .map_err(|_| TransportError::Transient)?;
            Ok(Envelope::from_slice(&body, Default::default())?)
        })
    }
}

#[tokio::test]
async fn local_first_then_real_loopback_reply_and_negative_acknowledgement() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        for negative in [false, true] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let size = socket.read_u32().await.unwrap();
            let mut bytes = vec![0; size as usize];
            socket.read_exact(&mut bytes).await.unwrap();
            let input = Envelope::from_slice(&bytes, Default::default())
                .unwrap()
                .decode::<Double>()
                .unwrap();
            socket.write_u8(u8::from(negative)).await.unwrap();
            if !negative {
                let bytes = Envelope::encode(&Doubled(input.0 * 2))
                    .unwrap()
                    .to_vec()
                    .unwrap();
                socket.write_u32(bytes.len() as u32).await.unwrap();
                socket.write_all(&bytes).await.unwrap();
            }
        }
    });
    let active = Arc::new(AtomicUsize::new(0));
    let remote = Arc::new(TcpPeer {
        address,
        active: Arc::clone(&active),
    });
    let mut local = RuntimeBuilder::default();
    local
        .handle(|_: Double| async { Ok(Doubled(777)) })
        .unwrap();
    let mut builder = BusBuilder::new(local.build());
    builder.remote("peer", UnusedTransport).unwrap();
    builder.route::<Double>("peer").unwrap();
    builder.remote_invoker(remote.clone(), true);
    assert_eq!(
        builder
            .build()
            .invoke(Double(21), origin())
            .await
            .unwrap()
            .0,
        777
    );
    assert_eq!(active.load(Ordering::SeqCst), 0);

    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.remote("peer", UnusedTransport).unwrap();
    builder.route::<Double>("peer").unwrap();
    builder.remote_invoker(remote, true);
    let bus = builder.build();
    assert_eq!(bus.invoke(Double(21), origin()).await.unwrap().0, 42);
    assert!(matches!(
        bus.invoke(Double(21), origin()).await,
        Err(InvokeError::Bus(BusError::RemoteFailure))
    ));
    peer.await.unwrap();
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(matches!(
        bus.invoke_with(
            Double(1),
            origin(),
            InvokeOptions {
                allow_remote: false,
                ..Default::default()
            }
        )
        .await,
        Err(InvokeError::Bus(BusError::RemoteDisabled))
    ));
}

#[tokio::test]
async fn send_requires_route_publish_no_subscriber_is_ok_and_remote_route_is_unique() {
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    assert_eq!(
        builder.build().timeout_for::<Double>().unwrap(),
        Duration::from_secs(60)
    );
    builder = BusBuilder::new(RuntimeBuilder::default().build());
    let bus = builder.build();
    assert!(matches!(
        bus.send(&Double(1), origin()).await,
        Err(BusError::NoRoute)
    ));
    bus.publish(&Double(1), origin()).await.unwrap();
    assert!(matches!(
        bus.invoke(Double(1), origin()).await,
        Err(InvokeError::Bus(BusError::NoRoute))
    ));
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    for name in ["a", "b"] {
        builder.remote(name, UnusedTransport).unwrap();
        builder.route::<Double>(name).unwrap();
    }
    let bus = builder.build();
    assert!(matches!(
        bus.invoke(Double(1), origin()).await,
        Err(InvokeError::Bus(BusError::Ambiguous))
    ));
}

#[tokio::test]
async fn failing_fanout_admission_does_not_skip_a_later_healthy_destination() {
    let settings = QueueSettings {
        mode: QueueMode::Inline,
        capacity: 1,
        parallelism: 1,
    };
    let (broken, _broken_runner) = LocalQueue::new(settings).unwrap();
    let (healthy, _healthy_runner) = LocalQueue::new(settings).unwrap();
    let called = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&called);
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.local("broken", broken).unwrap();
    builder.local("healthy", healthy).unwrap();
    builder.route::<Double>("broken").unwrap();
    builder.route::<Double>("healthy").unwrap();
    builder
        .handle::<Double, _, _>("healthy", move |_, _| {
            let count = Arc::clone(&count);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();
    assert!(matches!(
        builder.build().publish(&Double(1), origin()).await,
        Err(BusError::Invalid(_))
    ));
    assert_eq!(called.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn default_global_and_type_deadlines_include_admission_and_middleware() {
    let mut runtime = RuntimeBuilder::try_new(1).unwrap();
    runtime
        .handle(|_: Double| async {
            std::future::pending::<()>().await;
            Ok(Doubled(1))
        })
        .unwrap();
    let mut builder = BusBuilder::new(runtime.build());
    builder.timeout(Duration::from_secs(2)).unwrap();
    builder
        .message_timeout::<Double>(Duration::from_millis(50))
        .unwrap();
    let bus = builder.build();
    let start = tokio::time::Instant::now();
    assert!(matches!(
        bus.invoke(Double(1), origin()).await,
        Err(InvokeError::TimedOut)
    ));
    assert_eq!(start.elapsed(), Duration::from_millis(50));
    let start = tokio::time::Instant::now();
    assert!(matches!(
        bus.invoke_with(
            Double(1),
            origin(),
            InvokeOptions {
                timeout: Some(Duration::from_millis(10)),
                ..Default::default()
            }
        )
        .await,
        Err(InvokeError::TimedOut)
    ));
    assert_eq!(start.elapsed(), Duration::from_millis(10));
}

#[tokio::test]
async fn cancellation_drops_real_remote_wait_and_releases_peer_registration() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let received = Arc::new(Notify::new());
    let signal = Arc::clone(&received);
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let size = socket.read_u32().await.unwrap();
        let mut bytes = vec![0; size as usize];
        socket.read_exact(&mut bytes).await.unwrap();
        signal.notify_one();
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    });
    let active = Arc::new(AtomicUsize::new(0));
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.remote("peer", UnusedTransport).unwrap();
    builder.route::<Double>("peer").unwrap();
    builder.remote_invoker(
        Arc::new(TcpPeer {
            address,
            active: Arc::clone(&active),
        }),
        true,
    );
    let bus = builder.build();
    let context = MessageContext::new(origin(), Envelope::encode(&Double(1)).unwrap(), Utc::now());
    let cancellation = context.cancellation().clone();
    let call = bus.invoke_context(Double(1), context, InvokeOptions::default());
    tokio::pin!(call);
    tokio::select! { result = &mut call => panic!("unexpected result: {result:?}"), () = received.notified() => {} }
    cancellation.cancel();
    assert!(matches!(call.await, Err(InvokeError::Cancelled)));
    peer.await.unwrap();
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn buffered_publish_admits_before_execution_pauses_backpressures_and_drains() {
    let (queue, runner) = LocalQueue::new(QueueSettings {
        mode: QueueMode::Buffered,
        capacity: 1,
        parallelism: 1,
    })
    .unwrap();
    queue.pause().unwrap();
    queue.pause().unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "orders").unwrap();
    let shutdown = host.handle();
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.local("orders", queue.clone()).unwrap();
    builder.route::<Double>("orders").unwrap();
    let processed = Arc::new(AtomicUsize::new(0));
    let output = Arc::clone(&processed);
    builder
        .handle::<Double, _, _>("orders", move |_, _| {
            let output = Arc::clone(&output);
            async move {
                output.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();
    let bus = builder.build();
    let host_task = tokio::spawn(host.run());
    bus.publish(&Double(1), origin()).await.unwrap();
    assert_eq!(processed.load(Ordering::SeqCst), 0);
    assert_eq!(queue.snapshot().buffered, 1);
    let second = bus.send(&Double(2), origin());
    tokio::pin!(second);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut second)
            .await
            .is_err()
    );
    queue.resume();
    queue.resume();
    second.await.unwrap();
    shutdown.shutdown();
    host_task.await.unwrap().unwrap();
    assert_eq!(processed.load(Ordering::SeqCst), 2);
    assert_eq!(queue.snapshot().active, 0);
    assert!(!queue.snapshot().accepting);
    assert!(matches!(
        bus.send(&Double(3), origin()).await,
        Err(BusError::Queue(QueueError::Closed))
    ));
}

#[tokio::test]
async fn queue_failures_do_not_block_siblings_and_execution_has_its_own_bound() {
    let (queue, runner) = LocalQueue::new(QueueSettings {
        mode: QueueMode::Buffered,
        capacity: 8,
        parallelism: 1,
    })
    .unwrap();
    let mut host = Host::new(Duration::from_secs(5)).unwrap();
    runner.attach(&mut host, "jobs").unwrap();
    let shutdown = host.handle();
    let host_task = tokio::spawn(host.run());
    let gate = Arc::new(Semaphore::new(0));
    let started = Arc::new(Notify::new());
    let start_signal = Arc::clone(&started);
    let hold = Arc::clone(&gate);
    queue
        .submit(async move {
            start_signal.notify_one();
            hold.acquire().await.unwrap().forget();
            Err(QueueError::Handler)
        })
        .await
        .unwrap();
    started.notified().await;
    queue
        .submit(async { panic!("isolated queued handler panic") })
        .await
        .unwrap();
    queue.submit(async { Ok(()) }).await.unwrap();
    assert_eq!(queue.snapshot().active, 1);
    gate.add_permits(1);
    shutdown.shutdown();
    host_task.await.unwrap().unwrap();
    assert_eq!(queue.snapshot().failed, 2);
    assert_eq!(queue.snapshot().completed, 1);
}
