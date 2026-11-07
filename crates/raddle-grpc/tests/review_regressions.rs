//! Real HTTP/2 regressions for request trailers and remote status preservation.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::VecDeque;
use std::convert::Infallible;
use std::future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use http::{HeaderMap, HeaderValue};
use http_body::Body as HttpBody;
use http_body::Frame;
use prost::Message;
use prost::bytes::Bytes;
use raddle::wire::WireMessage;
use raddle::{Request, RuntimeBuilder};
use raddle_grpc::protocol::dispatch_server::{Dispatch, DispatchServer};
use raddle_grpc::protocol::{InvokeReply, InvokeRequest};
use raddle_grpc::{
    Authenticated, ClientError, GrpcBuilder, GrpcClient, GrpcService, INVOKE_PATH, Principal,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::body::Body;
use tonic::server::NamedService;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Code, Status};
use tower_service::Service;

#[derive(Serialize, Deserialize)]
struct Echo;
impl Request for Echo {
    type Reply = String;
    type Error = Infallible;
}
impl WireMessage for Echo {
    const NAME: &'static str = "review.echo";
    const VERSION: u32 = 1;
}

fn counted_service() -> (GrpcService, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(|_: Authenticated<Echo>| async { Ok("ok".to_owned()) })
        .unwrap();
    let mut grpc = GrpcBuilder::new(runtime.build(), {
        let calls = Arc::clone(&calls);
        move |_: String| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(Principal::new("tenant", "subject", &["echo.invoke"]).unwrap()) }
        }
    });
    grpc.request::<Echo>("echo.invoke").unwrap();
    (grpc.build(), calls)
}

struct RunningServer {
    channel: Option<Channel>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl RunningServer {
    async fn start<S>(service: S) -> Self
    where
        S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            Server::builder()
                .http2_max_header_list_size(16 * 1024)
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                })
                .await
        });
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await
            .unwrap();
        Self {
            channel: Some(channel),
            stop: Some(stop),
            task: Some(task),
        }
    }

    fn channel(&self) -> Channel {
        self.channel.as_ref().unwrap().clone()
    }

    async fn close(mut self) {
        drop(self.channel.take());
        let _ = self.stop.take().unwrap().send(());
        let mut task = self.task.take().unwrap();
        match tokio::time::timeout(Duration::from_secs(10), &mut task).await {
            Ok(Ok(Ok(()))) => {}
            other => {
                task.abort();
                let _ = task.await;
                panic!("server did not stop: {other:?}");
            }
        }
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct FramedRequest {
    frames: VecDeque<Frame<Bytes>>,
}

impl http_body::Body for FramedRequest {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.frames.pop_front().map(Ok))
    }
    fn is_end_stream(&self) -> bool {
        self.frames.is_empty()
    }
}

async fn raw_call(channel: Channel, initial: HeaderMap, trailers: HeaderMap) -> Code {
    tokio::time::timeout(Duration::from_secs(10), async move {
        let message = InvokeRequest {
            contract: Echo::NAME.into(),
            version: Echo::VERSION,
            json: b"null".to_vec(),
        }
        .encode_to_vec();
        let mut data = Vec::with_capacity(message.len() + 5);
        data.push(0);
        data.extend_from_slice(&(message.len() as u32).to_be_bytes());
        data.extend_from_slice(&message);
        let body = Body::new(FramedRequest {
            frames: VecDeque::from([Frame::data(Bytes::from(data)), Frame::trailers(trailers)]),
        });
        let mut request = http::Request::builder()
            .method("POST")
            .uri(INVOKE_PATH)
            .version(http::Version::HTTP_2)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(body)
            .unwrap();
        request.headers_mut().extend(initial);
        let mut channel = channel;
        future::poll_fn(|context| channel.poll_ready(context))
            .await
            .unwrap();
        let response = channel.call(request).await.unwrap();
        let mut status = response
            .headers()
            .get("grpc-status")
            .map(|value| value.to_str().unwrap().parse::<i32>().unwrap());
        let mut body = Box::pin(response.into_body());
        while let Some(frame) = future::poll_fn(|context| body.as_mut().poll_frame(context)).await {
            let frame = frame.unwrap();
            if let Some(trailers) = frame.trailers_ref()
                && let Some(value) = trailers.get("grpc-status")
            {
                status = Some(value.to_str().unwrap().parse::<i32>().unwrap());
            }
        }
        Code::from_i32(status.expect("missing grpc-status"))
    })
    .await
    .unwrap()
}

fn authorization() -> HeaderMap {
    let mut metadata = HeaderMap::new();
    metadata.insert("authorization", HeaderValue::from_static("Bearer verified"));
    metadata
}

#[tokio::test]
async fn trailing_duplicate_bearer_is_rejected_before_authentication() {
    let (service, calls) = counted_service();
    let server = RunningServer::start(service).await;
    let code = raw_call(server.channel(), authorization(), authorization()).await;
    server.close().await;
    assert_eq!(code, Code::Unauthenticated);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn trailer_only_bearer_is_rejected_before_authentication() {
    let (service, calls) = counted_service();
    let server = RunningServer::start(service).await;
    let code = raw_call(server.channel(), HeaderMap::new(), authorization()).await;
    server.close().await;
    assert_eq!(code, Code::Unauthenticated);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn oversized_trailer_field_count_is_rejected_before_authentication() {
    let (service, calls) = counted_service();
    let server = RunningServer::start(service).await;
    let mut trailers = HeaderMap::new();
    for index in 0..33 {
        trailers.insert(
            format!("x-trailer-{index}")
                .parse::<http::header::HeaderName>()
                .unwrap(),
            HeaderValue::from_static("value"),
        );
    }
    let code = raw_call(server.channel(), authorization(), trailers).await;
    server.close().await;
    assert_eq!(code, Code::ResourceExhausted);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn oversized_trailer_bytes_are_rejected_before_authentication() {
    let (service, calls) = counted_service();
    let server = RunningServer::start(service).await;
    let mut trailers = HeaderMap::new();
    trailers.insert(
        "x-trailer",
        HeaderValue::try_from("x".repeat(9000)).unwrap(),
    );
    let code = raw_call(server.channel(), authorization(), trailers).await;
    server.close().await;
    assert_eq!(code, Code::ResourceExhausted);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn initial_and_trailing_metadata_share_one_byte_budget_before_merge() {
    let (service, calls) = counted_service();
    let server = RunningServer::start(service).await;
    let mut initial = authorization();
    initial.insert(
        "x-overwritten",
        HeaderValue::try_from("x".repeat(4500)).unwrap(),
    );
    let mut trailers = HeaderMap::new();
    trailers.insert(
        "x-overwritten",
        HeaderValue::try_from("x".repeat(4500)).unwrap(),
    );
    let code = raw_call(server.channel(), initial, trailers).await;
    server.close().await;
    assert_eq!(code, Code::ResourceExhausted);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn initial_and_trailing_metadata_share_one_field_budget_before_merge() {
    let (service, calls) = counted_service();
    let server = RunningServer::start(service).await;
    let mut initial = authorization();
    let mut trailers = HeaderMap::new();
    for index in 0..17 {
        let name = format!("x-overwritten-{index}")
            .parse::<http::header::HeaderName>()
            .unwrap();
        initial.insert(name.clone(), HeaderValue::from_static("initial"));
        trailers.insert(name, HeaderValue::from_static("trailing"));
    }
    let code = raw_call(server.channel(), initial, trailers).await;
    server.close().await;
    assert_eq!(code, Code::ResourceExhausted);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[derive(Clone)]
struct RemoteOutOfRange;
#[tonic::async_trait]
impl Dispatch for RemoteOutOfRange {
    async fn invoke(
        &self,
        _: tonic::Request<InvokeRequest>,
    ) -> Result<tonic::Response<InvokeReply>, Status> {
        Err(Status::out_of_range("value outside supported range"))
    }
}

#[tokio::test]
async fn typed_client_preserves_standard_remote_out_of_range() {
    let server = RunningServer::start(DispatchServer::new(RemoteOutOfRange)).await;
    let mut client = GrpcClient::new(server.channel());
    let error = client
        .invoke(&Echo, "verified", Duration::from_secs(5))
        .await
        .unwrap_err();
    drop(client);
    server.close().await;
    match error {
        ClientError::Rpc(status) => {
            assert_eq!(status.code(), Code::OutOfRange);
            assert_eq!(status.message(), "value outside supported range");
        }
        other => panic!("unexpected client error: {other}"),
    }
}

#[tokio::test]
async fn bounded_non_security_trailers_remain_supported() {
    let (service, calls) = counted_service();
    let server = RunningServer::start(service).await;
    let mut initial = authorization();
    initial.insert("x-note", HeaderValue::from_static("initial"));
    let mut trailers = HeaderMap::new();
    trailers.insert("x-note", HeaderValue::from_static("trailing"));
    let code = raw_call(server.channel(), initial, trailers).await;
    server.close().await;
    assert_eq!(code, Code::Ok);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn trailing_timeout_is_rejected_before_authentication() {
    let (service, calls) = counted_service();
    let server = RunningServer::start(service).await;
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-timeout", HeaderValue::from_static("10000S"));
    let code = raw_call(server.channel(), authorization(), trailers).await;
    server.close().await;
    assert_eq!(code, Code::InvalidArgument);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
