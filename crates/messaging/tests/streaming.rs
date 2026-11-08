//! Caller-polled local input/output streams and their cancellation ownership.
#![allow(clippy::unwrap_used)]

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::stream::{self, Stream, StreamExt};
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle::{ExecutionError, Request, RuntimeBuilder};
use raddle_messaging::{
    BoxFuture, BusBuilder, BusError, InputStream, MessageBus, MessageContext, RemoteInvoker,
    StreamError, StreamOptions,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Debug, thiserror::Error, PartialEq)]
#[error("test source failure")]
struct Failed;

struct Source {
    items: VecDeque<Result<u32, Failed>>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    pending: bool,
}

impl Stream for Source {
    type Item = Result<u32, Failed>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        match self.items.pop_front() {
            Some(item) => Poll::Ready(Some(item)),
            None if self.pending => Poll::Pending,
            None => Poll::Ready(None),
        }
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn probe(
    items: Vec<Result<u32, Failed>>,
    pending: bool,
) -> (Source, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Source {
            items: items.into(),
            polls: Arc::clone(&polls),
            drops: Arc::clone(&drops),
            pending,
        },
        polls,
        drops,
    )
}

struct Output {
    source: Source,
    started: Option<oneshot::Sender<()>>,
    gate: Option<oneshot::Receiver<()>>,
    fail: bool,
}

impl Request for Output {
    type Reply = Source;
    type Error = Failed;
}

fn output(source: Source) -> Output {
    Output {
        source,
        started: None,
        gate: None,
        fail: false,
    }
}

fn bus() -> MessageBus {
    let mut runtime = RuntimeBuilder::try_new(1).unwrap();
    runtime
        .handle(|message: Output| async move {
            if let Some(started) = message.started {
                let _ = started.send(());
            }
            if let Some(gate) = message.gate {
                let _ = gate.await;
            }
            if message.fail {
                return Err(Failed);
            }
            Ok(message.source)
        })
        .unwrap();
    runtime
        .handle(|mut input: InputStream<u32, u32, Failed>| async move {
            let mut sum = 0;
            while let Some(item) = input.next().await {
                sum += item?;
            }
            Ok(sum)
        })
        .unwrap();
    BusBuilder::new(runtime.build()).build()
}

#[tokio::test]
async fn output_is_lazy_first_item_is_incremental_and_consumer_controls_polling() {
    let (source, polls, drops) = probe(vec![Ok(1), Ok(2)], true);
    let mut output = bus().stream(output(source), StreamOptions::default());
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    // The unfinished source cannot complete, but its first item is available now.
    assert_eq!(output.next().await.unwrap().unwrap(), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    tokio::task::yield_now().await;
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(output.next().await.unwrap().unwrap(), 2);
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    drop(output);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn empty_source_and_terminal_source_failure_release_without_polling_successors() {
    let (empty, _, drops) = probe(vec![], false);
    let mut empty = bus().stream(output(empty), StreamOptions::default());
    assert!(empty.next().await.is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let (source, polls, drops) = probe(vec![Ok(1), Err(Failed), Ok(3)], false);
    let bus = bus();
    let mut failed = bus.stream(output(source), StreamOptions::default());
    assert_eq!(failed.next().await.unwrap().unwrap(), 1);
    assert!(matches!(
        failed.next().await,
        Some(Err(StreamError::Source(Failed)))
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(failed.next().await.is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    let (healthy, _, _) = probe(vec![Ok(4)], false);
    assert_eq!(
        bus.stream(output(healthy), StreamOptions::default())
            .next()
            .await
            .unwrap()
            .unwrap(),
        4
    );
}

#[tokio::test]
async fn handler_failure_is_distinct_and_does_not_poll_source() {
    let (source, polls, drops) = probe(vec![Ok(1)], false);
    let mut request = output(source);
    request.fail = true;
    let mut result = bus().stream(request, StreamOptions::default());
    assert!(matches!(
        result.next().await,
        Some(Err(StreamError::Local(ExecutionError::Handler(Failed))))
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(result.next().await.is_none());
}

#[tokio::test]
async fn cancellation_drops_pending_source_and_does_not_cancel_parent_or_neighbor() {
    let parent = raddle_messaging::Cancellation::new();
    let options = StreamOptions {
        cancellation: parent.child(),
        ..StreamOptions::default()
    };
    let cancellation = options.cancellation.clone();
    let bus = bus();
    let (source, _, drops) = probe(vec![Ok(1)], true);
    let mut result = bus.stream(output(source), options);
    assert_eq!(result.next().await.unwrap().unwrap(), 1);
    let mut pending = Box::pin(result.next());
    assert!(futures_util::poll!(&mut pending).is_pending());
    cancellation.cancel();
    assert!(matches!(pending.await, Some(Err(StreamError::Cancelled))));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(!parent.is_cancelled());
    assert!(result.next().await.is_none());
    let (healthy, _, _) = probe(vec![Ok(9)], false);
    assert_eq!(
        bus.stream(output(healthy), StreamOptions::default())
            .next()
            .await
            .unwrap()
            .unwrap(),
        9
    );
}

#[tokio::test]
async fn dropping_pending_setup_releases_admission_and_owned_source() {
    let bus = bus();
    let (source, polls, drops) = probe(vec![Ok(1)], false);
    let (started, entered) = oneshot::channel();
    let (_release, gate) = oneshot::channel();
    let mut request = output(source);
    request.started = Some(started);
    request.gate = Some(gate);
    let mut result = bus.stream(request, StreamOptions::default());
    let mut next = Box::pin(result.next());
    assert!(futures_util::poll!(&mut next).is_pending());
    entered.await.unwrap();
    drop(next);
    drop(result);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let (healthy, _, _) = probe(vec![Ok(2)], false);
    assert_eq!(
        bus.stream(output(healthy), StreamOptions::default())
            .next()
            .await
            .unwrap()
            .unwrap(),
        2
    );
}

#[tokio::test(start_paused = true)]
async fn setup_timeout_drops_handler_but_does_not_limit_later_output_consumption() {
    let bus = bus();
    let (source, _, drops) = probe(vec![Ok(1)], false);
    let (_release, gate) = oneshot::channel();
    let mut request = output(source);
    request.gate = Some(gate);
    let options = StreamOptions {
        timeout: Duration::from_secs(1),
        ..StreamOptions::default()
    };
    let mut result = bus.stream(request, options.clone());
    assert!(matches!(
        result.next().await,
        Some(Err(StreamError::TimedOut))
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let (healthy, _, _) = probe(vec![Ok(2), Ok(3)], false);
    let mut healthy = bus.stream(output(healthy), options);
    assert_eq!(healthy.next().await.unwrap().unwrap(), 2);
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(healthy.next().await.unwrap().unwrap(), 3);
}

#[tokio::test]
async fn input_aggregation_empty_and_source_errors_are_typed_and_isolated() {
    let bus = bus();
    for (items, expected) in [(vec![], 0), (vec![Ok(1), Ok(2), Ok(3)], 6)] {
        let (source, _, drops) = probe(items, false);
        assert_eq!(
            bus.invoke_stream(
                InputStream::<_, u32, _>::new(source),
                StreamOptions::default()
            )
            .await
            .unwrap(),
            expected
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    let (bad, polls, drops) = probe(vec![Ok(1), Err(Failed), Ok(3)], false);
    let result = bus
        .invoke_stream(InputStream::<_, u32, _>::new(bad), StreamOptions::default())
        .await;
    assert!(matches!(
        result,
        Err(StreamError::Local(ExecutionError::Handler(Failed)))
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        bus.invoke_stream(
            InputStream::<u32, u32, Failed>::new(stream::iter([Ok(7)])),
            StreamOptions::default()
        )
        .await
        .unwrap(),
        7u32
    );
}

#[tokio::test(start_paused = true)]
async fn input_deadline_and_cancellation_drop_source_and_release_admission() {
    let bus = bus();
    let (source, _, drops) = probe(vec![Ok(1)], true);
    let options = StreamOptions {
        timeout: Duration::from_secs(1),
        ..StreamOptions::default()
    };
    assert!(matches!(
        bus.invoke_stream(InputStream::<_, u32, _>::new(source), options)
            .await,
        Err(StreamError::TimedOut)
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let options = StreamOptions::default();
    let cancel = options.cancellation.clone();
    let (source, polls, drops) = probe(vec![Ok(1)], true);
    let mut pending = Box::pin(bus.invoke_stream(InputStream::<_, u32, _>::new(source), options));
    assert!(futures_util::poll!(&mut pending).is_pending());
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    cancel.cancel();
    assert!(matches!(pending.await, Err(StreamError::Cancelled)));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        bus.invoke_stream(
            InputStream::<u32, u32, Failed>::new(stream::iter([Ok(5)])),
            StreamOptions::default()
        )
        .await
        .unwrap(),
        5u32
    );
}

#[tokio::test]
async fn invalid_timeout_and_precancelled_execution_never_poll_source() {
    for cancel in [false, true] {
        let mut options = StreamOptions::default();
        if cancel {
            options.cancellation.cancel();
        } else {
            options.timeout = Duration::ZERO;
        }
        let (source, polls, drops) = probe(vec![Ok(1)], false);
        let mut result = bus().stream(output(source), options);
        let result = result.next().await.unwrap();
        assert!(if cancel {
            matches!(result, Err(StreamError::Cancelled))
        } else {
            matches!(result, Err(StreamError::InvalidTimeout))
        });
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[derive(Serialize, Deserialize)]
struct Routed;

impl WireMessage for Routed {
    const NAME: &'static str = "streaming.routed";
    const VERSION: u32 = 1;
}

impl Request for Routed {
    type Reply = stream::Empty<Result<u32, Failed>>;
    type Error = Failed;
}

struct Remote(Arc<AtomicUsize>);

impl RemoteInvoker for Remote {
    fn request<'a>(
        &'a self,
        _: &'a str,
        _: &'a MessageContext,
        _: &'a str,
    ) -> BoxFuture<'a, Result<Envelope, BusError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(BusError::RemoteFailure)
        })
    }
}

impl Transport for Remote {
    async fn publish(&self, _: &str, _: &Envelope) -> Result<(), TransportError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(TransportError::Rejected)
    }
}

#[tokio::test]
async fn missing_local_handler_refuses_configured_remote_route_without_any_io() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder
        .remote("remote", Remote(Arc::clone(&calls)))
        .unwrap();
    builder.route::<Routed>("remote").unwrap();
    builder.remote_invoker(Arc::new(Remote(Arc::clone(&calls))), true);
    let bus = builder.build();
    let mut result = bus.stream(Routed, StreamOptions::default());
    assert!(matches!(
        result.next().await,
        Some(Err(StreamError::Local(ExecutionError::Unregistered { .. })))
    ));
    assert!(matches!(
        bus.invoke_stream(
            InputStream::<u32, u32, Failed>::new(stream::empty()),
            StreamOptions::default()
        )
        .await,
        Err(StreamError::Local(ExecutionError::Unregistered { .. }))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_input_execution_drops_source_and_releases_admission() {
    let bus = bus();
    let (source, polls, drops) = probe(vec![Ok(1)], true);
    let input = InputStream::<_, u32, _>::new(source);
    let mut pending = Box::pin(bus.invoke_stream(input, StreamOptions::default()));
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert!(futures_util::poll!(&mut pending).is_pending());
    drop(pending);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        bus.invoke_stream(
            InputStream::<u32, u32, Failed>::new(stream::iter([Ok(8)])),
            StreamOptions::default()
        )
        .await
        .unwrap(),
        8
    );
}

#[tokio::test]
async fn cancellation_during_setup_drops_owned_source_without_polling_it() {
    let bus = bus();
    let (source, polls, drops) = probe(vec![Ok(1)], false);
    let (_release, gate) = oneshot::channel();
    let mut request = output(source);
    request.gate = Some(gate);
    let options = StreamOptions::default();
    let cancel = options.cancellation.clone();
    let mut output = bus.stream(request, options);
    let mut pending = Box::pin(output.next());
    assert!(futures_util::poll!(&mut pending).is_pending());
    cancel.cancel();
    assert!(matches!(pending.await, Some(Err(StreamError::Cancelled))));
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(output.next().await.is_none());
}

#[tokio::test]
async fn caller_diagnostic_context_reaches_the_actual_output_handler() {
    let options = StreamOptions::default();
    let expected = options.context;
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle_with_context(move |request: Output, context| async move {
            assert_eq!(context, expected);
            Ok(request.source)
        })
        .unwrap();
    let bus = BusBuilder::new(runtime.build()).build();
    let (source, _, _) = probe(vec![Ok(6)], false);
    assert_eq!(
        bus.stream(output(source), options)
            .next()
            .await
            .unwrap()
            .unwrap(),
        6
    );
}
