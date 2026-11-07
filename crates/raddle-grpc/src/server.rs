use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use http::{HeaderMap, Method, Request, Response, Version};
use http_body::{Frame, SizeHint};
use raddle::Runtime;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tonic::Status;
use tonic::body::Body;
use tonic::server::NamedService;
use tower_service::Service;

use crate::protocol::dispatch_server::{Dispatch, DispatchServer};
use crate::protocol::{InvokeReply, InvokeRequest};
use crate::registry::Registration;
use crate::rpc_context::CancelOnDrop;
use crate::{AuthenticationError, Authenticator, RpcContext};

/// The one canonical, case-sensitive protobuf gRPC unary route.
pub const INVOKE_PATH: &str = "/raddle.v1.Dispatch/Invoke";
/// Maximum encoded protobuf request or reply, before gRPC's five-byte frame prefix.
pub const MAX_MESSAGE_BYTES: usize = 1_048_576;
/// JSON budget, leaving bounded room for protobuf contract/version framing.
pub const MAX_JSON_BYTES: usize = MAX_MESSAGE_BYTES - 256;
/// Maximum combined initial/trailing metadata field count, including transport headers.
pub const MAX_HEADERS: usize = 32;
/// Maximum combined initial/trailing name/value bytes plus 32 bytes per field.
pub const MAX_HEADER_BYTES: usize = 8192;

#[derive(Clone)]
pub(crate) struct Dispatcher {
    pub(crate) runtime: Runtime,
    pub(crate) auth: Arc<dyn Authenticator>,
    pub(crate) registrations: Arc<BTreeMap<(&'static str, u32), Registration>>,
}

#[tonic::async_trait]
impl Dispatch for Dispatcher {
    async fn invoke(
        &self,
        request: tonic::Request<InvokeRequest>,
    ) -> Result<tonic::Response<InvokeReply>, Status> {
        let context = request
            .extensions()
            .get::<RpcContext>()
            .cloned()
            .ok_or_else(|| Status::internal("missing call context"))?;
        let authorization = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("bearer credential required"))?;
        let token = authorization
            .strip_prefix("Bearer ")
            .filter(|token| {
                !token.is_empty()
                    && token.len() <= 4096
                    && token.bytes().all(|byte| byte.is_ascii_graphic())
            })
            .ok_or_else(|| Status::unauthenticated("invalid bearer credential"))?;
        let principal =
            self.auth
                .authenticate(token.to_owned())
                .await
                .map_err(|error| match error {
                    AuthenticationError::Rejected => Status::unauthenticated("credential rejected"),
                    AuthenticationError::Unavailable => {
                        Status::unavailable("credential verifier unavailable")
                    }
                })?;
        let message = request.into_inner();
        if message.json.len() > MAX_JSON_BYTES {
            return Err(Status::resource_exhausted("request size limit exceeded"));
        }
        let registration = self
            .registrations
            .iter()
            .find(|((name, version), _)| *name == message.contract && *version == message.version)
            .map(|(_, registration)| registration)
            .ok_or_else(|| Status::not_found("RPC contract or version not registered"))?;
        if !principal.allows(&registration.permission) {
            return Err(Status::permission_denied("RPC permission required"));
        }
        let json = registration
            .request
            .invoke(self.runtime.clone(), principal, context, message.json)
            .await?;
        Ok(tonic::Response::new(InvokeReply { json }))
    }
}

/// A standard protobuf gRPC service with bounded decode, metadata, admission and
/// whole-call time. Add to an application-owned `tonic::transport::Server`.
/// Clones share the registry and the immediate-admission budget.
#[derive(Clone)]
pub struct GrpcService {
    inner: DispatchServer<Dispatcher>,
    max_duration: Duration,
    admission: Arc<Semaphore>,
}

impl GrpcService {
    pub(crate) fn new(dispatcher: Dispatcher, max_duration: Duration, capacity: usize) -> Self {
        Self {
            inner: DispatchServer::new(dispatcher)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES),
            max_duration,
            admission: Arc::new(Semaphore::new(capacity)),
        }
    }
}

impl NamedService for GrpcService {
    const NAME: &'static str = "raddle.v1.Dispatch";
}

type ResponseFuture = Pin<Box<dyn Future<Output = Result<Response<Body>, Infallible>> + Send>>;

impl<B> Service<Request<B>> for GrpcService
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <DispatchServer<Dispatcher> as Service<Request<B>>>::poll_ready(&mut self.inner, context)
    }

    fn call(&mut self, mut request: Request<B>) -> Self::Future {
        if request.method() != Method::POST
            || request.version() != Version::HTTP_2
            || request.uri().path() != INVOKE_PATH
            || request.uri().query().is_some()
        {
            return immediate(Status::unimplemented("unknown RPC route"));
        }
        let metadata = match headers_valid(request.headers()) {
            Ok(metadata) => metadata,
            Err(status) => return immediate(status),
        };
        let duration = match timeout(request.headers()) {
            Ok(Some(duration)) => duration.min(self.max_duration),
            Ok(None) => self.max_duration,
            Err(status) => return immediate(status),
        };
        if duration.is_zero() {
            return immediate(Status::deadline_exceeded("RPC deadline exceeded"));
        }
        let permit = match Arc::clone(&self.admission).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return immediate(Status::resource_exhausted("RPC capacity exhausted")),
        };
        let deadline = Instant::now() + duration;
        let context = RpcContext::new(deadline);
        let mut cancellation = CancelOnDrop::new(context.clone());
        request.extensions_mut().insert(context);
        // Tonic merges trailers into initial metadata before calling Dispatcher.
        // Inspect raw frames first: merging can overwrite matching header names,
        // hiding ambiguous credentials and the original metadata resource cost.
        let request = request.map(|body| ValidatedBody {
            body: Box::pin(body),
            metadata,
            failed: false,
        });
        let future = self.inner.call(request);
        Box::pin(async move {
            let _permit = permit;
            match tokio::time::timeout_at(deadline, future).await {
                Ok(Ok(response)) => {
                    if Instant::now() >= deadline {
                        return Ok(Status::deadline_exceeded("RPC deadline exceeded").into_http());
                    }
                    cancellation.disarm();
                    // Tonic's length-prefix decoder uses OUT_OF_RANGE before it
                    // reaches the handler. This service never issues that code
                    // itself; normalize it to the documented resource budget.
                    if response
                        .headers()
                        .get("grpc-status")
                        .is_some_and(|status| status == "11")
                    {
                        Ok(
                            Status::resource_exhausted("RPC message size limit exceeded")
                                .into_http(),
                        )
                    } else {
                        Ok(response)
                    }
                }
                Ok(Err(never)) => match never {},
                Err(_) => Ok(Status::deadline_exceeded("RPC deadline exceeded").into_http()),
            }
        })
    }
}

fn immediate(status: Status) -> ResponseFuture {
    Box::pin(std::future::ready(Ok(status.into_http())))
}

fn headers_valid(headers: &HeaderMap) -> Result<MetadataBudget, Status> {
    let mut budget = MetadataBudget {
        fields: 0,
        bytes: 0,
    };
    budget.include(headers)?;
    if headers.get_all("authorization").iter().count() > 1 {
        return Err(Status::unauthenticated("ambiguous bearer credential"));
    }
    if headers.get_all("grpc-timeout").iter().count() > 1 {
        return Err(Status::invalid_argument("ambiguous RPC timeout"));
    }
    Ok(budget)
}

struct MetadataBudget {
    fields: usize,
    bytes: usize,
}

impl MetadataBudget {
    fn include(&mut self, headers: &HeaderMap) -> Result<(), Status> {
        self.fields = self.fields.saturating_add(headers.len());
        if self.fields > MAX_HEADERS {
            return Err(Status::resource_exhausted("RPC metadata limit exceeded"));
        }
        for (name, value) in headers {
            self.bytes = self
                .bytes
                .saturating_add(name.as_str().len())
                .saturating_add(value.len())
                .saturating_add(32);
            if self.bytes > MAX_HEADER_BYTES {
                return Err(Status::resource_exhausted("RPC metadata limit exceeded"));
            }
        }
        Ok(())
    }

    fn trailers(&mut self, trailers: &HeaderMap) -> Result<(), Status> {
        self.include(trailers)?;
        if trailers.contains_key("authorization") {
            return Err(Status::unauthenticated(
                "bearer credentials require initial metadata",
            ));
        }
        if trailers.contains_key("grpc-timeout") {
            return Err(Status::invalid_argument(
                "RPC timeout requires initial metadata",
            ));
        }
        Ok(())
    }
}

struct ValidatedBody<B> {
    body: Pin<Box<B>>,
    metadata: MetadataBudget,
    failed: bool,
}

impl<B> http_body::Body for ValidatedBody<B>
where
    B: http_body::Body,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Data = B::Data;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Status>>> {
        let this = self.get_mut();
        if this.failed {
            return Poll::Ready(None);
        }
        match this.body.as_mut().poll_frame(context) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref()
                    && let Err(status) = this.metadata.trailers(trailers)
                {
                    this.failed = true;
                    return Poll::Ready(Some(Err(status)));
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.failed = true;
                Poll::Ready(Some(Err(Status::from_error(error.into()))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.failed || self.body.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

// Tonic 0.14 Request has set_timeout but no deadline getter. Parse the standard
// grpc-timeout syntax at HTTP ingress, before decode/auth consume the budget.
fn timeout(headers: &HeaderMap) -> Result<Option<Duration>, Status> {
    let Some(value) = headers.get("grpc-timeout") else {
        return Ok(None);
    };
    let invalid = || Status::invalid_argument("invalid RPC timeout");
    let bytes = value.as_bytes();
    if !(2..=9).contains(&bytes.len()) {
        return Err(invalid());
    }
    let (digits, suffix) = bytes.split_at(bytes.len() - 1);
    if !digits.iter().all(u8::is_ascii_digit) {
        return Err(invalid());
    }
    let number = digits
        .iter()
        .fold(0u64, |number, digit| number * 10 + u64::from(digit - b'0'));
    let duration = match suffix[0] {
        b'n' => Duration::from_nanos(number),
        b'u' => Duration::from_micros(number),
        b'm' => Duration::from_millis(number),
        b'S' => Duration::from_secs(number),
        b'M' => Duration::from_secs(number * 60),
        b'H' => Duration::from_secs(number * 3600),
        _ => return Err(invalid()),
    };
    Ok(Some(duration))
}
