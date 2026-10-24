use std::any::Any;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::{Event, EventDelivery, Request, SubscriptionName};

type BoxError = Box<dyn Error + Send + Sync>;
type BoxValue = Box<dyn Any + Send>;
pub(crate) type HandlerResult = Result<BoxValue, BoxError>;
pub(crate) type HandlerFuture<'handler> =
    Pin<Box<dyn Future<Output = HandlerResult> + Send + 'handler>>;

pub(crate) trait ErasedRequest: Send + Sync {
    fn call(&self, message: BoxValue) -> HandlerFuture<'_>;
}

pub(crate) trait ErasedEvent: Send + Sync {
    fn call(&self, message: Arc<dyn Any + Send + Sync>) -> HandlerFuture<'_>;
}

#[derive(Debug, thiserror::Error)]
#[error("internal handler input type mismatch")]
struct InputTypeMismatch;

impl<M, F, Fut> ErasedRequest for TypedRequest<M, F>
where
    M: Request,
    F: Fn(M) -> Fut + Send + Sync,
    Fut: Future<Output = Result<M::Reply, M::Error>> + Send,
{
    fn call(&self, message: BoxValue) -> HandlerFuture<'_> {
        Box::pin(async move {
            let message = message
                .downcast::<M>()
                .map_err(|_| Box::new(InputTypeMismatch) as BoxError)?;
            (self.function)(*message)
                .await
                .map(|reply| Box::new(reply) as BoxValue)
                .map_err(|error| Box::new(error) as BoxError)
        })
    }
}

pub(crate) struct TypedRequest<M, F> {
    pub function: F,
    pub marker: std::marker::PhantomData<fn(M)>,
}

pub(crate) struct TypedEvent<E, F> {
    pub function: F,
    pub marker: std::marker::PhantomData<fn(E)>,
}

impl<E, F, Fut> ErasedEvent for TypedEvent<E, F>
where
    E: Event,
    F: Fn(EventDelivery<E>) -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), E::Error>> + Send,
{
    fn call(&self, message: Arc<dyn Any + Send + Sync>) -> HandlerFuture<'_> {
        Box::pin(async move {
            let message = message
                .downcast::<E>()
                .map_err(|_| Box::new(InputTypeMismatch) as BoxError)?;
            (self.function)(EventDelivery::new(message))
                .await
                .map(|()| Box::new(()) as BoxValue)
                .map_err(|error| Box::new(error) as BoxError)
        })
    }
}

pub(crate) struct Subscriber {
    pub name: SubscriptionName,
    pub handler: Box<dyn ErasedEvent>,
}

pub(crate) fn typed_result<T, E>(result: HandlerResult) -> Result<T, crate::ExecutionError<E>>
where
    T: Send + 'static,
    E: Error + Send + Sync + 'static,
{
    match result {
        Ok(value) => value
            .downcast::<T>()
            .map(|value| *value)
            .map_err(|_| crate::ExecutionError::TypeMismatch),
        Err(error) => match error.downcast::<E>() {
            Ok(error) => Err(crate::ExecutionError::Handler(*error)),
            Err(_) => Err(crate::ExecutionError::TypeMismatch),
        },
    }
}
