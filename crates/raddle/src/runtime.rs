use std::any::{self, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use futures_util::stream::FuturesUnordered;
use tokio::sync::Semaphore;

use crate::handler::{self, ErasedRequest, Subscriber};
use crate::{Event, ExecutionError, PublishReport, Request, SubscriptionOutcome};

struct Registry {
    requests: HashMap<TypeId, Box<dyn ErasedRequest>>,
    events: HashMap<TypeId, Vec<Subscriber>>,
    admission: Semaphore,
}

/// An immutable registry and bounded local executor.
///
/// Clones share the same registrations and concurrency limit. Calling methods
/// does not spawn tasks: futures are driven by their callers. There is no
/// durable queue, implicit retry, global singleton, or runtime-owned thread.
///
/// A handler must not recursively invoke this runtime while holding its last
/// permit. Call shared business functions directly instead of re-entering it.
#[derive(Clone)]
pub struct Runtime {
    registry: Arc<Registry>,
}

impl Runtime {
    pub(crate) fn new(
        requests: HashMap<TypeId, Box<dyn ErasedRequest>>,
        events: HashMap<TypeId, Vec<Subscriber>>,
        concurrency: usize,
    ) -> Self {
        Self {
            registry: Arc::new(Registry {
                requests,
                events,
                admission: Semaphore::new(concurrency),
            }),
        }
    }

    /// Executes one local handler, preserving its typed reply and domain error.
    ///
    /// Waits for shared admission. Dropping the future cancels unfinished
    /// execution and releases its permit. Cancellation does not undo side
    /// effects. No execution is started for an unregistered request.
    pub async fn invoke<M: Request>(
        &self,
        message: M,
    ) -> Result<M::Reply, ExecutionError<M::Error>> {
        use tracing::Instrument;

        let handler =
            self.registry
                .requests
                .get(&TypeId::of::<M>())
                .ok_or(ExecutionError::Unregistered {
                    message: any::type_name::<M>(),
                })?;
        let _permit = self
            .registry
            .admission
            .acquire()
            .await
            .map_err(|_| ExecutionError::Closed)?;
        let result = handler
            .call(Box::new(message))
            .instrument(tracing::debug_span!(
                "raddle.invoke",
                message = any::type_name::<M>()
            ))
            .await;
        handler::typed_result(result)
    }

    /// Runs independent local subscribers and returns every outcome.
    ///
    /// Subscriber futures share one immutable event allocation, and each waits
    /// for its own admission permit. Outcomes retain registration order, not
    /// completion order. Domain errors do not cancel siblings. A closed runtime
    /// reports `Closed` for subscribers that have not acquired admission.
    ///
    /// An event without subscribers has an empty successful report, even after
    /// admission closes, because no handler needs admission.
    pub async fn publish<E: Event>(&self, event: E) -> PublishReport<E::Error> {
        use futures_util::stream::StreamExt;
        use tracing::Instrument;

        let Some(subscribers) = self.registry.events.get(&TypeId::of::<E>()) else {
            return PublishReport {
                outcomes: Vec::new(),
            };
        };
        let event: Arc<dyn any::Any + Send + Sync> = Arc::new(event);
        let mut pending = FuturesUnordered::new();
        for (index, subscriber) in subscribers.iter().enumerate() {
            let event = Arc::clone(&event);
            pending.push(async move {
                let result = match self.registry.admission.acquire().await {
                    Ok(_permit) => {
                        let result = subscriber
                            .handler
                            .call(event)
                            .instrument(tracing::debug_span!(
                                "raddle.publish",
                                message = any::type_name::<E>(),
                                subscription = %subscriber.name,
                            ))
                            .await;
                        handler::typed_result(result)
                    }
                    Err(_) => Err(ExecutionError::Closed),
                };
                (
                    index,
                    SubscriptionOutcome {
                        name: subscriber.name.clone(),
                        result,
                    },
                )
            });
        }
        let mut completed = Vec::with_capacity(subscribers.len());
        while let Some(outcome) = pending.next().await {
            completed.push(outcome);
        }
        completed.sort_unstable_by_key(|(index, _)| *index);
        PublishReport {
            outcomes: completed.into_iter().map(|(_, outcome)| outcome).collect(),
        }
    }

    /// Rejects new and waiting handlers across all runtime clones.
    ///
    /// Already admitted handlers are not canceled. Applications should stop
    /// intake, close admission, and join their own in-flight execution tasks.
    pub fn close(&self) {
        self.registry.admission.close();
    }

    /// Whether admission has been permanently closed.
    pub fn is_closed(&self) -> bool {
        self.registry.admission.is_closed()
    }
}
