//! Typed, ordered async middleware, with a single-use continuation.
//!
//! ```
//! use std::convert::Infallible;
//! use raddle::pipeline::Pipeline;
//! use raddle::{Request, RuntimeBuilder};
//!
//! struct Count(u32);
//! impl Request for Count { type Reply = u32; type Error = Infallible; }
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let pipeline = Pipeline::new(|message: Count, _| async move { Ok(message.0 * 2) })
//!     .validate(|message: Count, _| async move { Ok(message) });
//! let mut builder = RuntimeBuilder::default();
//! builder.handle_pipeline(pipeline)?;
//! assert_eq!(builder.build().invoke(Count(21)).await?, 42);
//! # Ok(())
//! # }
//! ```

use std::future::Future;

use futures_util::future::BoxFuture;

use crate::ExecutionContext;

/// An asynchronous layer around a typed message and typed result.
///
/// Layers can inspect or replace the owned input, short-circuit with a typed
/// result, and observe the result after awaiting `next.run`. `Next` is consumed,
/// so a layer cannot accidentally execute the downstream handler twice.
pub trait Middleware<M, R, E>: Send + Sync {
    /// Runs this layer; all borrows are limited to this execution future.
    fn call<'execution>(
        &'execution self,
        message: M,
        context: ExecutionContext,
        next: Next<'execution, M, R, E>,
    ) -> BoxFuture<'execution, Result<R, E>>;
}

impl<M, R, E, F> Middleware<M, R, E> for F
where
    F: for<'execution> Fn(
            M,
            ExecutionContext,
            Next<'execution, M, R, E>,
        ) -> BoxFuture<'execution, Result<R, E>>
        + Send
        + Sync,
{
    fn call<'execution>(
        &'execution self,
        message: M,
        context: ExecutionContext,
        next: Next<'execution, M, R, E>,
    ) -> BoxFuture<'execution, Result<R, E>> {
        self(message, context, next)
    }
}

trait Terminal<M, R, E>: Send + Sync {
    fn call(&self, message: M, context: ExecutionContext) -> BoxFuture<'_, Result<R, E>>;
}

impl<M, R, E, F, Fut> Terminal<M, R, E> for F
where
    F: Fn(M, ExecutionContext) -> Fut + Send + Sync,
    Fut: Future<Output = Result<R, E>> + Send + 'static,
{
    fn call(&self, message: M, context: ExecutionContext) -> BoxFuture<'_, Result<R, E>> {
        Box::pin(self(message, context))
    }
}

/// A single-use continuation through the remaining layers and terminal handler.
///
/// A continuation cannot replay downstream side effects:
///
/// ```compile_fail
/// use std::convert::Infallible;
/// use raddle::ExecutionContext;
/// use raddle::pipeline::Next;
/// async fn replay(next: Next<'_, u32, u32, Infallible>, context: ExecutionContext) {
///     let _ = next.run(1, context).await;
///     let _ = next.run(1, context).await; // `next` was consumed.
/// }
/// ```
pub struct Next<'execution, M, R, E> {
    layers: &'execution [Box<dyn Middleware<M, R, E>>],
    terminal: &'execution dyn Terminal<M, R, E>,
}

impl<'execution, M, R, E> Next<'execution, M, R, E> {
    /// Runs the rest of the pipeline once, retaining typed success and failure.
    pub fn run(self, message: M, context: ExecutionContext) -> BoxFuture<'execution, Result<R, E>> {
        match self.layers.split_first() {
            Some((layer, layers)) => layer.call(
                message,
                context,
                Next {
                    layers,
                    terminal: self.terminal,
                },
            ),
            None => self.terminal.call(message, context),
        }
    }
}

/// An explicitly assembled pipeline, first-added layer outermost.
///
/// With layers A then B, entry order is A, B, handler; exit order is B, A.
/// Register with `RuntimeBuilder::handle_pipeline` or `subscribe_pipeline` to
/// place the entire pipeline inside the runtime's shared admission boundary.
/// Direct `execute` calls have no runtime admission limit.
pub struct Pipeline<M, R, E> {
    layers: Vec<Box<dyn Middleware<M, R, E>>>,
    terminal: Box<dyn Terminal<M, R, E>>,
}

impl<M: Send + 'static, R: Send + 'static, E: Send + 'static> Pipeline<M, R, E> {
    /// Starts a pipeline with an ordinary typed async terminal handler.
    pub fn new<F, Fut>(handler: F) -> Self
    where
        F: Fn(M, ExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
    {
        Self {
            layers: Vec::new(),
            terminal: Box::new(handler),
        }
    }

    /// Appends a layer inside earlier layers and outside later layers.
    pub fn layer(mut self, layer: impl Middleware<M, R, E> + 'static) -> Self {
        self.layers.push(Box::new(layer));
        self
    }

    /// Appends a closure layer with explicit continuation-lifetime inference.
    ///
    /// Return `Box::pin(async move { ... })` to await downstream execution and
    /// inspect its result, or return `next.run(message, context)` directly.
    pub fn layer_fn<F>(self, layer: F) -> Self
    where
        F: for<'execution> Fn(
                M,
                ExecutionContext,
                Next<'execution, M, R, E>,
            ) -> BoxFuture<'execution, Result<R, E>>
            + Send
            + Sync
            + 'static,
    {
        self.layer(layer)
    }

    /// Appends async validation that returns the owned message on success.
    ///
    /// Failure short-circuits subsequent layers and the handler. Ownership allows
    /// non-`Clone`, non-`Sync` requests and normalization without copying input.
    pub fn validate<F, Fut>(self, validator: F) -> Self
    where
        F: Fn(M, ExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<M, E>> + Send + 'static,
    {
        self.layer(Validation { validator })
    }

    /// Executes without spawning work. Dropping this future drops every layer.
    pub fn execute(&self, message: M, context: ExecutionContext) -> BoxFuture<'_, Result<R, E>> {
        Next {
            layers: &self.layers,
            terminal: self.terminal.as_ref(),
        }
        .run(message, context)
    }
}

struct Validation<F> {
    validator: F,
}

impl<M, R, E, F, Fut> Middleware<M, R, E> for Validation<F>
where
    M: Send + 'static,
    R: Send + 'static,
    E: Send + 'static,
    F: Fn(M, ExecutionContext) -> Fut + Send + Sync,
    Fut: Future<Output = Result<M, E>> + Send + 'static,
{
    fn call<'execution>(
        &'execution self,
        message: M,
        context: ExecutionContext,
        next: Next<'execution, M, R, E>,
    ) -> BoxFuture<'execution, Result<R, E>> {
        Box::pin(async move {
            let message = (self.validator)(message, context).await?;
            next.run(message, context).await
        })
    }
}
