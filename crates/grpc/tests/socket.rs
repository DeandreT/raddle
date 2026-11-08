//! Actual loopback HTTP/2 sockets using generated and standard Tonic clients.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::convert::Infallible;
use std::future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use prost::Message;
use raddle::wire::WireMessage;
use raddle::{Request, RuntimeBuilder};
use raddle_grpc::protocol::dispatch_client::DispatchClient;
use raddle_grpc::protocol::{InvokeReply, InvokeRequest};
use raddle_grpc::{
    Authenticated, AuthenticationError, ClientError, GrpcBuilder, GrpcClient, GrpcService,
    MAX_JSON_BYTES, MAX_MESSAGE_BYTES, Principal, RpcContext,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{Notify, Semaphore, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint, Server};

#[derive(Serialize, Deserialize)]
struct Echo {
    text: String,
}
impl Request for Echo {
    type Reply = String;
    type Error = Rejection;
}
impl WireMessage for Echo {
    const NAME: &'static str = "test.echo";
    const VERSION: u32 = 1;
}

#[derive(Debug, thiserror::Error)]
#[error("secret customer payload in domain failure")]
struct Rejection;

async fn auth(token: String) -> Result<Principal, AuthenticationError> {
    match token.as_str() {
        "verified" => {
            Ok(Principal::new("verified-tenant", "verified-subject", &["echo.invoke"]).unwrap())
        }
        "forbidden" => Ok(Principal::new("another-tenant", "subject", &[]).unwrap()),
        "unavailable" => Err(AuthenticationError::Unavailable),
        "slow-auth" => {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Err(AuthenticationError::Rejected)
        }
        _ => Err(AuthenticationError::Rejected),
    }
}

fn echo_service() -> (GrpcService, raddle::Runtime) {
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(|request: Authenticated<Echo>| async move {
            if request.message().text == "reject" {
                return Err(Rejection);
            }
            Ok(format!(
                "{}:{}:{}",
                request.principal().tenant(),
                request.principal().subject(),
                request.message().text
            ))
        })
        .unwrap();
    let runtime = runtime.build();
    let mut grpc = GrpcBuilder::new(runtime.clone(), auth);
    grpc.request::<Echo>("echo.invoke").unwrap();
    (grpc.build(), runtime)
}

struct RunningServer {
    channel: Option<Channel>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl RunningServer {
    async fn start(service: GrpcService) -> Self {
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
    fn client(&self) -> DispatchClient<Channel> {
        DispatchClient::new(self.channel())
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
                panic!("server failed to stop: {other:?}");
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

fn request(
    contract: &str,
    version: u32,
    json: Vec<u8>,
    token: Option<&str>,
) -> tonic::Request<InvokeRequest> {
    let mut request = tonic::Request::new(InvokeRequest {
        contract: contract.into(),
        version,
        json,
    });
    if let Some(token) = token {
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
        );
    }
    request
}

fn echo(token: &str) -> tonic::Request<InvokeRequest> {
    request("test.echo", 1, br#"{"text":"hello"}"#.to_vec(), Some(token))
}

#[test]
fn protobuf_field_numbers_and_identity_validation_are_stable() {
    assert_eq!(
        InvokeRequest {
            contract: "x".into(),
            version: 1,
            json: b"{}".to_vec()
        }
        .encode_to_vec(),
        [10, 1, b'x', 16, 1, 26, 2, b'{', b'}']
    );
    assert_eq!(
        InvokeReply {
            json: b"{}".to_vec()
        }
        .encode_to_vec(),
        [10, 2, b'{', b'}']
    );
    for invalid in ["", "tenant\n", "tenant with spaces"] {
        assert!(Principal::new(invalid, "subject", &[]).is_err());
    }
    assert!(Principal::new("tenant", "subject", &["*"]).is_err());
    assert!(Principal::new("tenant", "subject", &["invoke", "invoke"]).is_err());
    assert!(Principal::new("tenant", "subject", &["invoke"; 129]).is_err());
}

#[tokio::test]
async fn actual_http2_typed_and_standard_clients_preserve_verified_ownership() {
    let (service, _) = echo_service();
    let server = RunningServer::start(service).await;
    let mut typed = GrpcClient::new(server.channel());
    let reply = typed
        .invoke(
            &Echo {
                text: "typed".into(),
            },
            "verified",
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(reply, "verified-tenant:verified-subject:typed");
    let mut raw = server.client();
    let mut forged = echo("verified");
    forged
        .metadata_mut()
        .insert("x-tenant", MetadataValue::from_static("victim"));
    forged
        .metadata_mut()
        .insert("x-subject", MetadataValue::from_static("administrator"));
    forged
        .metadata_mut()
        .insert("x-permissions", MetadataValue::from_static("*"));
    let reply = raw.invoke(forged).await.unwrap().into_inner();
    assert_eq!(
        serde_json::from_slice::<String>(&reply.json).unwrap(),
        "verified-tenant:verified-subject:hello"
    );
    // Invoke the same included .proto through Tonic's ordinary codec API, not our helper.
    let mut standard = tonic::client::Grpc::new(server.channel());
    standard.ready().await.unwrap();
    let reply: tonic::Response<InvokeReply> = standard
        .unary(
            echo("verified"),
            http::uri::PathAndQuery::from_static("/raddle.v1.Dispatch/Invoke"),
            tonic_prost::ProstCodec::default(),
        )
        .await
        .unwrap();
    assert!(!reply.into_inner().json.is_empty());
    standard.ready().await.unwrap();
    let status = standard
        .unary::<InvokeRequest, InvokeReply, _>(
            echo("verified"),
            http::uri::PathAndQuery::from_static("/raddle.v1.Dispatch/invoke"),
            tonic_prost::ProstCodec::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented);
    drop((typed, raw, standard));
    server.close().await;
}

#[tokio::test]
async fn authentication_authorization_exact_contracts_and_errors_are_sanitized() {
    let (service, runtime) = echo_service();
    let server = RunningServer::start(service).await;
    let mut client = server.client();
    for token in [None, Some("bad")] {
        assert_eq!(
            client
                .invoke(request("test.echo", 1, b"{}".to_vec(), token))
                .await
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
    }
    let mut forbidden = echo("forbidden");
    forbidden
        .metadata_mut()
        .insert("x-tenant", MetadataValue::from_static("verified-tenant"));
    forbidden
        .metadata_mut()
        .insert("x-permissions", MetadataValue::from_static("echo.invoke"));
    assert_eq!(
        client.invoke(forbidden).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        client.invoke(echo("unavailable")).await.unwrap_err().code(),
        Code::Unavailable
    );
    for (contract, version) in [
        ("unknown", 1),
        ("test.echo", 2),
        ("Test.Echo", 1),
        ("test.echo", 0),
    ] {
        assert_eq!(
            client
                .invoke(request(contract, version, b"{}".to_vec(), Some("verified")))
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
    }
    let domain = client
        .invoke(request(
            "test.echo",
            1,
            br#"{"text":"reject"}"#.to_vec(),
            Some("verified"),
        ))
        .await
        .unwrap_err();
    assert_eq!(domain.code(), Code::FailedPrecondition);
    assert_eq!(domain.message(), "request rejected");
    let invalid = client
        .invoke(request(
            "test.echo",
            1,
            b"{secret malformed input".to_vec(),
            Some("verified"),
        ))
        .await
        .unwrap_err();
    assert_eq!(invalid.code(), Code::InvalidArgument);
    assert_eq!(invalid.message(), "invalid request JSON");
    let nested = format!("{}0{}", "[".repeat(200), "]".repeat(200));
    assert_eq!(
        client
            .invoke(request(
                "test.echo",
                1,
                nested.into_bytes(),
                Some("verified")
            ))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    runtime.close();
    assert_eq!(
        client.invoke(echo("verified")).await.unwrap_err().code(),
        Code::Unavailable
    );
    drop(client);
    server.close().await;
}

#[tokio::test]
async fn protobuf_and_json_size_metadata_count_and_bytes_have_resource_status() {
    let (service, _) = echo_service();
    let server = RunningServer::start(service).await;
    let mut client = server.client();
    for size in [MAX_JSON_BYTES + 1, MAX_MESSAGE_BYTES + 1024] {
        let status = client
            .invoke(request("test.echo", 1, vec![b' '; size], Some("verified")))
            .await
            .unwrap_err();
        assert_eq!(
            status.code(),
            Code::ResourceExhausted,
            "unexpected decoder budget status: {status}"
        );
    }
    let mut count = echo("verified");
    for index in 0..33 {
        count.metadata_mut().insert(
            format!("x-field-{index}")
                .parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>()
                .unwrap(),
            MetadataValue::from_static("value"),
        );
    }
    assert_eq!(
        client.invoke(count).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    let mut bytes = echo("verified");
    bytes.metadata_mut().insert(
        "x-large",
        MetadataValue::try_from("x".repeat(9000)).unwrap(),
    );
    assert_eq!(
        client.invoke(bytes).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    let mut duplicate = echo("verified");
    duplicate.metadata_mut().append(
        "authorization",
        MetadataValue::from_static("Bearer forbidden"),
    );
    assert_eq!(
        client.invoke(duplicate).await.unwrap_err().code(),
        Code::Unauthenticated
    );
    assert!(client.invoke(echo("verified")).await.is_ok());
    drop(client);
    server.close().await;
}

#[derive(Serialize, Deserialize)]
struct BrokenReplyRequest;
impl Request for BrokenReplyRequest {
    type Reply = BrokenReply;
    type Error = Infallible;
}
impl WireMessage for BrokenReplyRequest {
    const NAME: &'static str = "test.broken";
    const VERSION: u32 = 1;
}
#[derive(Deserialize)]
struct BrokenReply;
impl Serialize for BrokenReply {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("secret serializer error"))
    }
}

#[derive(Deserialize)]
struct BrokenClientRequest;
impl Request for BrokenClientRequest {
    type Reply = String;
    type Error = Infallible;
}
impl WireMessage for BrokenClientRequest {
    const NAME: &'static str = "test.client-failure";
    const VERSION: u32 = 1;
}
impl Serialize for BrokenClientRequest {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("secret request serializer error"))
    }
}

#[tokio::test]
async fn client_preflight_bounds_and_serializer_failures_never_send_invalid_requests() {
    let (service, _) = echo_service();
    let server = RunningServer::start(service).await;
    let mut client = GrpcClient::new(server.channel());
    assert!(matches!(
        client
            .invoke(&BrokenClientRequest, "verified", Duration::from_secs(5))
            .await,
        Err(ClientError::Serialization)
    ));
    assert!(matches!(
        client
            .invoke(
                &Echo {
                    text: "x".repeat(MAX_MESSAGE_BYTES + 1)
                },
                "verified",
                Duration::from_secs(5)
            )
            .await,
        Err(ClientError::TooLarge)
    ));
    assert!(matches!(
        client
            .invoke(&Echo { text: "x".into() }, "verified", Duration::ZERO)
            .await,
        Err(ClientError::InvalidInput)
    ));
    assert!(matches!(
        client
            .invoke(
                &Echo { text: "x".into() },
                "not a token",
                Duration::from_secs(5)
            )
            .await,
        Err(ClientError::InvalidInput)
    ));
    assert!(
        client
            .invoke(
                &Echo {
                    text: "valid".into()
                },
                "verified",
                Duration::from_secs(5)
            )
            .await
            .is_ok()
    );
    drop(client);
    server.close().await;
}

#[test]
fn registration_and_policy_failures_preserve_previous_allowlist() {
    let runtime = RuntimeBuilder::default().build();
    assert!(
        GrpcBuilder::new(runtime.clone(), auth)
            .with_capacity(0)
            .is_err()
    );
    assert!(
        GrpcBuilder::new(runtime.clone(), auth)
            .with_capacity(65536)
            .is_err()
    );
    assert!(
        GrpcBuilder::new(runtime.clone(), auth)
            .with_max_duration(Duration::ZERO)
            .is_err()
    );
    assert!(
        GrpcBuilder::new(runtime.clone(), auth)
            .with_max_duration(Duration::from_secs(121))
            .is_err()
    );
    let mut grpc = GrpcBuilder::new(runtime, auth);
    assert!(grpc.request::<Echo>("*").is_err());
    grpc.request::<Echo>("echo.invoke").unwrap();
    assert!(grpc.request::<Echo>("echo.invoke").is_err());
}

#[tokio::test]
async fn response_serializer_failure_and_oversized_reply_are_sanitized() {
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(|_: Authenticated<BrokenReplyRequest>| async { Ok(BrokenReply) })
        .unwrap();
    runtime
        .handle(|_: Authenticated<Echo>| async { Ok("x".repeat(MAX_MESSAGE_BYTES + 1)) })
        .unwrap();
    let mut grpc = GrpcBuilder::new(runtime.build(), auth);
    grpc.request::<BrokenReplyRequest>("echo.invoke").unwrap();
    grpc.request::<Echo>("echo.invoke").unwrap();
    let server = RunningServer::start(grpc.build()).await;
    let mut client = server.client();
    let status = client
        .invoke(request(
            "test.broken",
            1,
            b"null".to_vec(),
            Some("verified"),
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Internal);
    assert_eq!(status.message(), "response serialization failed");
    assert_eq!(
        client.invoke(echo("verified")).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    drop(client);
    server.close().await;
}

struct Dropped {
    notify: Arc<Notify>,
    _context: RpcContext,
}
impl Drop for Dropped {
    fn drop(&mut self) {
        self.notify.notify_one();
    }
}

struct WaitingService {
    service: GrpcService,
    started: Arc<Notify>,
    dropped: Arc<Notify>,
    captured: Arc<tokio::sync::Mutex<Option<RpcContext>>>,
}

fn waiting_service(duration: Duration, capacity: usize) -> WaitingService {
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    let captured = Arc::new(tokio::sync::Mutex::new(None));
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle({
            let (started, dropped, captured) = (
                Arc::clone(&started),
                Arc::clone(&dropped),
                Arc::clone(&captured),
            );
            move |request: Authenticated<Echo>| {
                let (started, dropped, captured) = (
                    Arc::clone(&started),
                    Arc::clone(&dropped),
                    Arc::clone(&captured),
                );
                async move {
                    let _guard = Dropped {
                        notify: dropped,
                        _context: request.context().clone(),
                    };
                    *captured.lock().await = Some(request.context().clone());
                    started.notify_one();
                    future::pending::<Result<String, Rejection>>().await
                }
            }
        })
        .unwrap();
    let mut grpc = GrpcBuilder::new(runtime.build(), auth)
        .with_max_duration(duration)
        .unwrap()
        .with_capacity(capacity)
        .unwrap();
    grpc.request::<Echo>("echo.invoke").unwrap();
    WaitingService {
        service: grpc.build(),
        started,
        dropped,
        captured,
    }
}

#[tokio::test]
async fn configured_total_deadline_cancels_typed_handler_and_admission_is_reusable() {
    let WaitingService {
        service,
        started,
        dropped,
        captured,
    } = waiting_service(Duration::from_millis(100), 1);
    let server = RunningServer::start(service).await;
    let mut client = server.client();
    let status = client.invoke(echo("verified")).await.unwrap_err();
    assert_eq!(status.code(), Code::DeadlineExceeded);
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), dropped.notified())
        .await
        .unwrap();
    let context = captured.lock().await.clone().unwrap();
    assert!(context.is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), context.cancelled())
        .await
        .unwrap();
    // Another RPC reaches a handler instead of leaking the one admission permit.
    assert_eq!(
        client.invoke(echo("verified")).await.unwrap_err().code(),
        Code::DeadlineExceeded
    );
    drop(client);
    server.close().await;
}

#[tokio::test]
async fn client_stream_cancellation_releases_handler_and_signals_only_its_context() {
    let WaitingService {
        service,
        started,
        dropped,
        captured,
    } = waiting_service(Duration::from_secs(10), 1);
    let server = RunningServer::start(service).await;
    let mut client = server.client();
    let mut call = Box::pin(client.invoke(echo("verified")));
    tokio::select! {
        result=&mut call => panic!("waiting handler completed: {result:?}"),
        result=tokio::time::timeout(Duration::from_secs(5),started.notified()) => { result.unwrap(); },
    }
    drop(call);
    tokio::time::timeout(Duration::from_secs(5), dropped.notified())
        .await
        .unwrap();
    let context = captured.lock().await.clone().unwrap();
    tokio::time::timeout(Duration::from_secs(5), context.cancelled())
        .await
        .unwrap();
    assert!(context.is_cancelled());
    // A fresh call is independent and can enter after cancellation.
    let mut next = Box::pin(client.invoke(echo("verified")));
    tokio::select! {
        result=&mut next => panic!("second handler unexpectedly completed: {result:?}"),
        result=tokio::time::timeout(Duration::from_secs(5),started.notified()) => { result.unwrap(); },
    }
    assert!(!captured.lock().await.as_ref().unwrap().is_cancelled());
    drop(next);
    tokio::time::timeout(Duration::from_secs(5), dropped.notified())
        .await
        .unwrap();
    drop(client);
    server.close().await;
}

#[tokio::test]
async fn caller_timeout_and_authentication_share_the_whole_rpc_budget() {
    let WaitingService {
        service,
        dropped,
        captured,
        ..
    } = waiting_service(Duration::from_secs(10), 2);
    let server = RunningServer::start(service).await;
    let mut typed = GrpcClient::new(server.channel());
    let error = typed
        .invoke(
            &Echo {
                text: "wait".into(),
            },
            "verified",
            Duration::from_millis(100),
        )
        .await
        .unwrap_err();
    match error {
        ClientError::Rpc(status) => assert!(matches!(
            status.code(),
            Code::DeadlineExceeded | Code::Cancelled
        )),
        other => panic!("wrong timeout error: {other}"),
    }
    tokio::time::timeout(Duration::from_secs(5), dropped.notified())
        .await
        .unwrap();
    assert!(captured.lock().await.as_ref().unwrap().is_cancelled());
    drop(typed);
    server.close().await;

    let mut grpc = GrpcBuilder::new(RuntimeBuilder::default().build(), auth)
        .with_max_duration(Duration::from_millis(100))
        .unwrap();
    grpc.request::<Echo>("echo.invoke").unwrap();
    let server = RunningServer::start(grpc.build()).await;
    let mut client = server.client();
    assert_eq!(
        client.invoke(echo("slow-auth")).await.unwrap_err().code(),
        Code::DeadlineExceeded
    );
    let mut invalid = echo("verified");
    invalid
        .metadata_mut()
        .insert("grpc-timeout", MetadataValue::from_static("invalid"));
    assert_eq!(
        client.invoke(invalid).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    drop(client);
    server.close().await;
}

#[tokio::test]
async fn adapter_capacity_is_shared_and_runtime_concurrency_stays_bounded() {
    let gate = Arc::new(Semaphore::new(0));
    let started = Arc::new(Notify::new());
    let active = Arc::new(AtomicUsize::new(0));
    let high = Arc::new(AtomicUsize::new(0));
    let mut runtime = RuntimeBuilder::try_new(1).unwrap();
    runtime
        .handle({
            let (gate, started, active, high) = (
                Arc::clone(&gate),
                Arc::clone(&started),
                Arc::clone(&active),
                Arc::clone(&high),
            );
            move |_: Authenticated<Echo>| {
                let (gate, started, active, high) = (
                    Arc::clone(&gate),
                    Arc::clone(&started),
                    Arc::clone(&active),
                    Arc::clone(&high),
                );
                async move {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    high.fetch_max(current, Ordering::SeqCst);
                    started.notify_one();
                    gate.acquire().await.unwrap().forget();
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok("done".to_owned())
                }
            }
        })
        .unwrap();
    let mut grpc = GrpcBuilder::new(runtime.build(), auth)
        .with_capacity(1)
        .unwrap();
    grpc.request::<Echo>("echo.invoke").unwrap();
    let server = RunningServer::start(grpc.build()).await;
    let mut first = server.client();
    let mut second = server.client();
    let mut running = Box::pin(first.invoke(echo("verified")));
    tokio::select! { result=&mut running=>panic!("early completion: {result:?}"), result=tokio::time::timeout(Duration::from_secs(5),started.notified())=>{result.unwrap();} }
    assert_eq!(
        second.invoke(echo("verified")).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    gate.add_permits(1);
    running.await.unwrap();
    assert_eq!(high.load(Ordering::SeqCst), 1);
    gate.add_permits(1);
    assert!(second.invoke(echo("verified")).await.is_ok());
    assert_eq!(active.load(Ordering::SeqCst), 0);
    drop((first, second));
    server.close().await;
}
