use std::error::Error;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::stream::{self, Stream, StreamExt, TryStreamExt};
use raddle::{ExecutionContext, ExecutionError, Request};

use super::MessageBus;
use crate::Cancellation;

/// Caller-owned local streaming execution. No field enables remote routing.
#[derive(Clone, Debug)]
pub struct StreamOptions {
    /// Output-stream handler setup budget, or the whole input-stream aggregation
    /// budget, including runtime admission. Must be between zero and one day,
    /// exclusive of zero. Output item production is bounded by cancellation,
    /// not this setup deadline.
    pub timeout: Duration,
    /// Cancels this execution without affecting unrelated handlers or streams.
    pub cancellation: Cancellation,
    /// Explicit diagnostic context, not an authenticated identity or authority.
    pub context: ExecutionContext,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            cancellation: Cancellation::new(),
            context: ExecutionContext::new(),
        }
    }
}

/// A terminal streaming failure. No automatic retry or rollback is implied.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StreamError<E: Error + 'static> {
    /// Local registration, admission or typed handler failure.
    #[error("local streaming handler failed: {0}")]
    Local(#[from] ExecutionError<E>),
    /// The output source failed after handler setup; remaining items are dropped.
    #[error("local stream source failed: {0}")]
    Source(E),
    /// Caller cancellation was observed; unfinished local work was dropped.
    #[error("local streaming execution cancelled")]
    Cancelled,
    /// Handler setup or single-response aggregation exceeded its total budget.
    #[error("local streaming execution deadline expired")]
    TimedOut,
    /// Invalid budget; no handler or source is polled.
    #[error("local streaming timeout must be nonzero and at most one day")]
    InvalidTimeout,
}

type Source<T, E> = Pin<Box<dyn Stream<Item = Result<T, E>> + Send>>;

/// A lazy, caller-polled local result stream with no producer task or queue.
///
/// One poll requests at most one source item. The first error is terminal, and
/// dropping the stream drops handler setup or the source immediately. Application
/// sources must also be cancellation safe; drop cannot undo completed side effects
/// or join tasks that application code detached itself. Runtime admission covers
/// handler setup, not the lifetime of a returned source.
pub struct LocalStream<T, E: Error + 'static> {
    source: Source<T, StreamError<E>>,
}

impl<T, E: Error + 'static> Stream for LocalStream<T, E> {
    type Item = Result<T, StreamError<E>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.source.as_mut().poll_next(cx)
    }
}

/// A local request whose input is a caller-owned fallible stream and whose reply
/// is one typed value. Register its handler with ordinary `RuntimeBuilder::handle`.
///
/// The handler polls this request directly as a stream. No serialization, wire
/// contract, implicit buffering or source task is introduced. Input errors remain
/// typed `E`; a handler must propagate or explicitly handle them like any other
/// input validation failure.
pub struct InputStream<T, R, E> {
    source: Source<T, E>,
    reply: PhantomData<fn() -> R>,
}

impl<T, R, E> InputStream<T, R, E> {
    /// Owns a source without polling or allocating a buffering queue.
    pub fn new(source: impl Stream<Item = Result<T, E>> + Send + 'static) -> Self {
        Self {
            source: Box::pin(source),
            reply: PhantomData,
        }
    }
}

impl<T, R, E> Stream for InputStream<T, R, E> {
    type Item = Result<T, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.source.as_mut().poll_next(cx)
    }
}

impl<T, R, E> Request for InputStream<T, R, E>
where
    T: 'static,
    R: Send + 'static,
    E: Error + Send + Sync + 'static,
{
    type Reply = R;
    type Error = E;
}

impl MessageBus {
    /// Executes a local handler lazily and pulls its typed output one item at a time.
    ///
    /// A missing local handler returns `ExecutionError::Unregistered`, even if a
    /// remote invoker and matching routes exist. Neither the request nor items
    /// need implement `WireMessage`. Cancellation is checked before every poll;
    /// errors terminate and release the source without polling later items.
    pub fn stream<M, S, T>(&self, message: M, options: StreamOptions) -> LocalStream<T, M::Error>
    where
        M: Request<Reply = S>,
        S: Stream<Item = Result<T, M::Error>> + Send + 'static,
        T: Send + 'static,
    {
        let bus = self.clone();
        let cancellation = options.cancellation.clone();
        let setup = stream::once(async move {
            let result = bus.invoke_stream_local(message, options).await?;
            Ok::<_, StreamError<M::Error>>(result.map_err(StreamError::Source))
        });
        let source = Box::pin(setup.try_flatten());
        let source = stream::unfold(Some((source, cancellation)), |state| async move {
            let (mut source, cancellation) = state?;
            tokio::select! {
                biased;
                () = cancellation.cancelled() => Some((Err(StreamError::Cancelled), None)),
                next = source.next() => match next {
                    Some(Ok(item)) => Some((Ok(item), Some((source, cancellation)))),
                    Some(Err(error)) => Some((Err(error), None)),
                    None => None,
                },
            }
        });
        LocalStream {
            source: Box::pin(source),
        }
    }

    /// Runs one locally registered input-stream handler to a single typed reply.
    ///
    /// The whole handler, including admission and source consumption, shares one
    /// deadline. Dropping this future or cancelling drops unfinished handler/source
    /// state and releases runtime admission. It never invokes a remote peer.
    pub async fn invoke_stream<T, R, E>(
        &self,
        input: InputStream<T, R, E>,
        options: StreamOptions,
    ) -> Result<R, StreamError<E>>
    where
        T: 'static,
        R: Send + 'static,
        E: Error + Send + Sync + 'static,
    {
        self.invoke_stream_local(input, options).await
    }

    async fn invoke_stream_local<M: Request>(
        &self,
        message: M,
        options: StreamOptions,
    ) -> Result<M::Reply, StreamError<M::Error>> {
        if options.timeout.is_zero() || options.timeout > Duration::from_secs(86_400) {
            return Err(StreamError::InvalidTimeout);
        }
        tokio::select! {
            biased;
            () = options.cancellation.cancelled() => Err(StreamError::Cancelled),
            result = tokio::time::timeout(
                options.timeout,
                self.0.runtime.invoke_with_context(message, options.context),
            ) => result.map_err(|_| StreamError::TimedOut)?.map_err(StreamError::Local),
        }
    }
}
