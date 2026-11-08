use std::any::{self, TypeId};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::handler::{ErasedRequest, Subscriber, TypedEvent, TypedRequest};
use crate::pipeline::Pipeline;
use crate::{
    ConfigurationError, Event, EventDelivery, EventFamily, ExecutionContext,
    FamilyRegistrationError, RegistrationError, Request, RequestFamily, Runtime, SubscriptionName,
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
        self.handle_with_context(move |message, _context| handler(message))
    }

    /// Registers a handler that receives explicit local execution context.
    pub fn handle_with_context<M, F, Fut>(
        &mut self,
        handler: F,
    ) -> Result<&mut Self, RegistrationError>
    where
        M: Request,
        F: Fn(M, ExecutionContext) -> Fut + Send + Sync + 'static,
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
        self.subscribe_with_context(name, move |delivery, _context| handler(delivery))
    }

    /// Registers an event subscriber that receives explicit execution context.
    pub fn subscribe_with_context<E, F, Fut>(
        &mut self,
        name: SubscriptionName,
        handler: F,
    ) -> Result<&mut Self, RegistrationError>
    where
        E: Event,
        F: Fn(EventDelivery<E>, ExecutionContext) -> Fut + Send + Sync + 'static,
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

    /// Atomically installs all concrete handlers in a declared request family.
    ///
    /// An empty family or any conflict with an ordinary or family registration
    /// rejects the whole group, leaving this builder unchanged. The consumed
    /// declaration is dropped on failure. Each installed member uses ordinary
    /// exact-type dispatch, typed results, admission and caller-owned cancellation.
    pub fn handle_family<T, R, E, F>(
        &mut self,
        family: RequestFamily<T, R, E, F>,
    ) -> Result<&mut Self, FamilyRegistrationError> {
        if family.members.is_empty() {
            return Err(FamilyRegistrationError::Empty);
        }
        for member in &family.members {
            if self.requests.contains_key(&member.message) {
                return Err(RegistrationError::DuplicateRequest {
                    message: member.diagnostic,
                }
                .into());
            }
        }
        self.requests.extend(
            family
                .members
                .into_iter()
                .map(|member| (member.message, member.handler)),
        );
        Ok(self)
    }

    /// Atomically installs a family's independent named event subscribers.
    ///
    /// Names remain runtime-wide unique. An empty family or any existing name
    /// rejects the entire group; no earlier member is partially installed.
    /// Every concrete subscription retains ordinary fan-out failure isolation,
    /// admission, context and report ordering. No tasks are spawned.
    pub fn subscribe_family<T, E, F>(
        &mut self,
        family: EventFamily<T, E, F>,
    ) -> Result<&mut Self, FamilyRegistrationError> {
        if family.members.is_empty() {
            return Err(FamilyRegistrationError::Empty);
        }
        for (_, subscriber) in &family.members {
            if self.subscriptions.contains(&subscriber.name) {
                return Err(RegistrationError::DuplicateSubscription {
                    name: subscriber.name.clone(),
                }
                .into());
            }
        }
        for (message, subscriber) in family.members {
            self.subscriptions.insert(subscriber.name.clone());
            self.events.entry(message).or_default().push(subscriber);
        }
        Ok(self)
    }

    /// Registers a typed request pipeline under the shared admission limit.
    pub fn handle_pipeline<M: Request>(
        &mut self,
        pipeline: Pipeline<M, M::Reply, M::Error>,
    ) -> Result<&mut Self, RegistrationError> {
        let pipeline = Arc::new(pipeline);
        self.handle_with_context(move |message, context| {
            let pipeline = Arc::clone(&pipeline);
            async move { pipeline.execute(message, context).await }
        })
    }

    /// Registers an independent event pipeline under the shared admission limit.
    pub fn subscribe_pipeline<E: Event>(
        &mut self,
        name: SubscriptionName,
        pipeline: Pipeline<EventDelivery<E>, (), E::Error>,
    ) -> Result<&mut Self, RegistrationError> {
        let pipeline = Arc::new(pipeline);
        self.subscribe_with_context(name, move |delivery, context| {
            let pipeline = Arc::clone(&pipeline);
            async move { pipeline.execute(delivery, context).await }
        })
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
