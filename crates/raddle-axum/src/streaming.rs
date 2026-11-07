use std::convert::Infallible;
use std::time::Duration;

use axum::Extension;
use axum::extract::Query;
use axum::extract::rejection::QueryRejection;
use axum::response::Sse;
use axum::response::sse::{Event, KeepAlive};
use axum::routing::get;
use raddle::Request;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use tokio::sync::mpsc;

use crate::{ApiBuilder, ApiError, Authenticated, Permission, Principal, specification};

/// Finite SSE delivery limits, in addition to a bounded producer channel.
#[derive(Clone, Copy, Debug)]
pub struct StreamOptions {
    max_events: u32,
    duration: Duration,
}

impl StreamOptions {
    /// Validates one through 100,000 events and a lifetime up to one hour.
    pub fn try_new(max_events: u32, duration: Duration) -> Result<Self, ApiError> {
        if !(1..=100_000).contains(&max_events)
            || duration.is_zero()
            || duration > Duration::from_secs(3600)
        {
            return Err(ApiError::invalid());
        }
        Ok(Self {
            max_events,
            duration,
        })
    }
}

/// A bounded, caller-produced stream. Dropping the HTTP body closes its receiver.
pub struct EventStream<T> {
    receiver: mpsc::Receiver<T>,
    options: StreamOptions,
}

impl<T> EventStream<T> {
    /// Creates a finite 1–256 element channel. The application owns producer tasks.
    pub fn channel(
        capacity: usize,
        options: StreamOptions,
    ) -> Result<(mpsc::Sender<T>, Self), ApiError> {
        if !(1..=256).contains(&capacity) {
            return Err(ApiError::invalid());
        }
        let (sender, receiver) = mpsc::channel(capacity);
        Ok((sender, Self { receiver, options }))
    }
}

impl ApiBuilder {
    /// Registers authenticated GET query ingress returning bounded JSON SSE events.
    ///
    /// The runtime permit covers stream creation, not its lifetime. The application
    /// owns its producers, which should stop when sending returns a closed error.
    /// Serialization failure closes the stream without exposing serializer errors.
    pub fn events<M, T>(&mut self, path: &str) -> Result<&mut Self, ApiError>
    where
        M: Request<Reply = EventStream<T>> + DeserializeOwned + JsonSchema,
        T: Serialize + JsonSchema + Send + 'static,
    {
        let mut schemas = self.schemas.clone();
        let index = self.registered.len() + 1;
        let query = specification::schema::<M>(
            &format!("StreamQuery{index}"),
            Contract::Deserialize,
            &mut schemas,
        );
        let event = specification::schema::<T>(
            &format!("StreamEvent{index}"),
            Contract::Serialize,
            &mut schemas,
        );
        // SSE has query parameters, never a JSON GET request body.
        let parameters = super::binding::parameters_for_query(&query, &schemas)?;
        if path.contains('{') {
            return Err(ApiError::invalid());
        }
        self.reserve_template(path, "get")?;
        let mut operation =
            specification::operation("get", index, None, json!({"type":"string"}), 200);
        operation["parameters"] = json!(parameters);
        operation["responses"]["200"]["content"] =
            json!({"text/event-stream":{"schema":{"type":"string"},"x-raddle-event-schema":event}});
        self.paths
            .entry(path.to_owned())
            .or_insert_with(|| json!({}))["get"] = operation;
        self.schemas = schemas;
        let runtime = self.runtime.clone();
        self.router = self.router.clone().route(
            path,
            get(
                move |Extension(principal): Extension<Principal>,
                      query: Result<Query<M>, QueryRejection>| {
                    let runtime = runtime.clone();
                    async move {
                        principal.require(Permission::Invoke)?;
                        let Query(message) = query.map_err(|_| ApiError::invalid())?;
                        let stream = runtime
                            .invoke(Authenticated::new(principal, message))
                            .await
                            .map_err(crate::binding::execution_error)?;
                        let deadline = tokio::time::Instant::now() + stream.options.duration;
                        let events = futures_util::stream::unfold(
                            (stream, 0u32, deadline),
                            |(mut stream, count, deadline)| async move {
                                if count >= stream.options.max_events
                                    || tokio::time::Instant::now() >= deadline
                                {
                                    return None;
                                }
                                let value =
                                    tokio::time::timeout_at(deadline, stream.receiver.recv())
                                        .await
                                        .ok()??;
                                if tokio::time::Instant::now() >= deadline {
                                    return None;
                                }
                                let json = serde_json::to_string(&value).ok()?;
                                if tokio::time::Instant::now() >= deadline {
                                    return None;
                                }
                                Some((
                                    Ok::<_, Infallible>(Event::default().data(json)),
                                    (stream, count + 1, deadline),
                                ))
                            },
                        );
                        Ok::<_, ApiError>(
                            Sse::new(events)
                                .keep_alive(KeepAlive::new().interval(Duration::from_secs(15))),
                        )
                    }
                },
            ),
        );
        Ok(self)
    }
}
