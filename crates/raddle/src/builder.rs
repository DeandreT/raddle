use std::any::{self, TypeId};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::marker::PhantomData;

use tokio::sync::Semaphore;

use crate::handler::{ErasedRequest, Subscriber, TypedEvent, TypedRequest};
use crate::{
    ConfigurationError, Event, EventDelivery, RegistrationError, Request, Runtime, SubscriptionName,
};

/// Explicit registrations and the shared maximum number of active handlers.
///
/// Defaults to 64 active handlers. Each request and each individual event
/// subscriber uses one permit. A failed registration changes nothing.
pub struct RuntimeBuilder {
    requests: HashMap<TypeId, Box<dyn ErasedRequest>>,
    events: HashMap<TypeId, Vec<Subscriber>>,
    subscriptions: HashSet<SubscriptionName>,
    concurrency: usize,
}

impl RuntimeBuilder {
    /// Creates an empty registry with an explicit concurrency bound.
    ///
    /// Rejects zero or limits larger than Tokio's supported semaphore capacity.
    pub fn try_new(concurrency: usize) -> Result<Self, ConfigurationError> {
        if concurrency == 0 || concurrency > Semaphore::MAX_PERMITS {
            return Err(ConfigurationError::InvalidConcurrency {
                requested: concurrency,
                maximum: Semaphore::MAX_PERMITS,
            });
        }
        Ok(Self {
            requests: HashMap::new(),
            events: HashMap::new(),
            subscriptions: HashSet::new(),
            concurrency,
        })
    }

    /// Registers an ordinary async function or reusable closure for a request.
    ///
    /// Captured state must be safe for concurrent calls. Requests need not be
    /// `Clone` or `Sync`. Duplicate request registrations are rejected.
    ///
    /// Handlers must be reusable, not closures that consume captured state:
    ///
    /// ```compile_fail
    /// use std::convert::Infallible;
    /// use raddle::{Request, RuntimeBuilder};
    /// struct Read;
    /// impl Request for Read {
    ///     type Reply = String;
    ///     type Error = Infallible;
    /// }
    /// let value = String::from("consumed");
    /// let mut builder = RuntimeBuilder::default();
    /// builder.handle(move |_: Read| async move { Ok(value) }).unwrap();
    /// ```
    ///
    /// Captured borrows cannot outlive their owner:
    ///
    /// ```compile_fail
    /// use std::convert::Infallible;
    /// use raddle::{Request, RuntimeBuilder};
    /// struct Read;
    /// impl Request for Read {
    ///     type Reply = usize;
    ///     type Error = Infallible;
    /// }
    /// let mut builder = RuntimeBuilder::default();
    /// {
    ///     let value = String::from("borrowed");
    ///     builder.handle(|_: Read| async { Ok(value.len()) }).unwrap();
    /// }
    /// let _runtime = builder.build();
    /// ```
    pub fn handle<M, F, Fut>(&mut self, handler: F) -> Result<&mut Self, RegistrationError>
    where
        M: Request,
        F: Fn(M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<M::Reply, M::Error>> + Send + 'static,
    {
        let message = TypeId::of::<M>();
        if self.requests.contains_key(&message) {
            return Err(RegistrationError::DuplicateRequest {
                message: any::type_name::<M>(),
            });
        }
        self.requests.insert(
            message,
            Box::new(TypedRequest::<M, F> {
                function: handler,
                marker: PhantomData,
            }),
        );
        Ok(self)
    }

    /// Registers an independent event subscriber with a runtime-wide unique name.
    ///
    /// Registration order determines report order, not execution order. Handlers
    /// receive shared immutable deliveries and need not clone payloads.
    pub fn subscribe<E, F, Fut>(
        &mut self,
        name: SubscriptionName,
        handler: F,
    ) -> Result<&mut Self, RegistrationError>
    where
        E: Event,
        F: Fn(EventDelivery<E>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E::Error>> + Send + 'static,
    {
        if self.subscriptions.contains(&name) {
            return Err(RegistrationError::DuplicateSubscription { name });
        }
        self.subscriptions.insert(name.clone());
        self.events
            .entry(TypeId::of::<E>())
            .or_default()
            .push(Subscriber {
                name,
                handler: Box::new(TypedEvent::<E, F> {
                    function: handler,
                    marker: PhantomData,
                }),
            });
        Ok(self)
    }

    /// Freezes the registry. Runtime clones share admission and registrations.
    pub fn build(self) -> Runtime {
        Runtime::new(self.requests, self.events, self.concurrency)
    }
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self {
            requests: HashMap::new(),
            events: HashMap::new(),
            subscriptions: HashSet::new(),
            concurrency: 64,
        }
    }
}
