use std::collections::BTreeMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use raddle::wire::WireMessage;
use raddle::{ExecutionError, Request, Runtime, SubscriptionName};
use serde::Serialize;
use tonic::Status;

use crate::json::{self, EncodeError};
use crate::server::{Dispatcher, GrpcService};
use crate::{Authenticated, Authenticator, ConfigurationError, Principal, RpcContext};

type DispatchFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>, Status>> + Send>>;

pub(crate) trait RegisteredRequest: Send + Sync {
    fn invoke(
        &self,
        runtime: Runtime,
        principal: Principal,
        context: RpcContext,
        json: Vec<u8>,
    ) -> DispatchFuture;
}

struct TypedRequest<M>(PhantomData<fn(M)>);

impl<M> RegisteredRequest for TypedRequest<M>
where
    M: Request + WireMessage,
    M::Reply: Serialize,
{
    fn invoke(
        &self,
        runtime: Runtime,
        principal: Principal,
        context: RpcContext,
        bytes: Vec<u8>,
    ) -> DispatchFuture {
        Box::pin(async move {
            let message = serde_json::from_slice::<M>(&bytes)
                .map_err(|_| Status::invalid_argument("invalid request JSON"))?;
            if context.is_cancelled() {
                return Err(Status::deadline_exceeded("RPC deadline exceeded"));
            }
            let reply = runtime
                .invoke(Authenticated::new(principal, context, message))
                .await
                .map_err(|error| match error {
                    ExecutionError::Handler(_) => Status::failed_precondition("request rejected"),
                    ExecutionError::Closed | ExecutionError::Unregistered { .. } => {
                        Status::unavailable("runtime unavailable")
                    }
                    _ => Status::internal("internal execution failure"),
                })?;
            json::encode(&reply).map_err(|error| match error {
                EncodeError::TooLarge => Status::resource_exhausted("response size limit exceeded"),
                EncodeError::Serialization => Status::internal("response serialization failed"),
            })
        })
    }
}

pub(crate) struct Registration {
    pub(crate) permission: String,
    pub(crate) request: Box<dyn RegisteredRequest>,
}

/// Explicit typed wire-contract allowlist, paired with ordinary runtime handlers.
/// No listener, registration discovery or background task is created.
pub struct GrpcBuilder {
    runtime: Runtime,
    auth: Arc<dyn Authenticator>,
    registrations: BTreeMap<(&'static str, u32), Registration>,
    max_duration: Duration,
    capacity: usize,
}

impl GrpcBuilder {
    /// Requires real application-owned bearer verification. Default budgets are
    /// 128 in-flight RPCs and 30 seconds per whole RPC (including decode and auth).
    pub fn new(runtime: Runtime, authenticator: impl Authenticator) -> Self {
        Self {
            runtime,
            auth: Arc::new(authenticator),
            registrations: BTreeMap::new(),
            max_duration: Duration::from_secs(30),
            capacity: 128,
        }
    }

    /// Caps the entire RPC to 1 ms–120 seconds. Shorter grpc-timeout values win.
    pub fn with_max_duration(mut self, duration: Duration) -> Result<Self, ConfigurationError> {
        if !(Duration::from_millis(1)..=Duration::from_secs(120)).contains(&duration) {
            return Err(ConfigurationError::InvalidBudget);
        }
        self.max_duration = duration;
        Ok(self)
    }

    /// Sets an immediate-admission budget (1–65535) shared by all service clones.
    /// It bounds decoded requests and auth/handler waits, independently of Runtime.
    pub fn with_capacity(mut self, capacity: usize) -> Result<Self, ConfigurationError> {
        if !(1..=65535).contains(&capacity) {
            return Err(ConfigurationError::InvalidBudget);
        }
        self.capacity = capacity;
        Ok(self)
    }

    /// Exposes exactly `M::NAME` and `M::VERSION`, requiring this permission alias.
    /// A matching `Authenticated<M>` handler must be registered in the runtime.
    /// At most 256 distinct contract/version pairs may be exposed.
    pub fn request<M>(&mut self, permission: &str) -> Result<&mut Self, ConfigurationError>
    where
        M: Request + WireMessage,
        M::Reply: Serialize,
    {
        if M::NAME.len() > 128
            || SubscriptionName::try_new(M::NAME).is_err()
            || M::VERSION == 0
            || self.registrations.contains_key(&(M::NAME, M::VERSION))
        {
            return Err(ConfigurationError::InvalidContract);
        }
        if permission.len() > 128 || SubscriptionName::try_new(permission).is_err() {
            return Err(ConfigurationError::InvalidPrincipal);
        }
        if self.registrations.len() >= 256 {
            return Err(ConfigurationError::InvalidBudget);
        }
        self.registrations.insert(
            (M::NAME, M::VERSION),
            Registration {
                permission: permission.into(),
                request: Box::new(TypedRequest::<M>(PhantomData)),
            },
        );
        Ok(self)
    }

    /// Produces a standard Tonic service for the application's own server builder.
    pub fn build(self) -> GrpcService {
        GrpcService::new(
            Dispatcher {
                runtime: self.runtime,
                auth: self.auth,
                registrations: Arc::new(self.registrations),
            },
            self.max_duration,
            self.capacity,
        )
    }
}
