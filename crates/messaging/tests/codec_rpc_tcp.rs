//! Real selected RPC, including raw wire captures and a separately owned process.
//! Synthetic loopback identities are application trust assertions, not TCP auth.
#![cfg(feature = "serialization-rpc")]
#![allow(clippy::unwrap_used)]

mod codec_rpc_support;

use std::io::{BufRead, BufReader, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::transport::{Transport, TransportError};
use raddle::wire::Envelope;
use raddle_messaging::{
    BusBuilder, BusError, InvokeError, InvokeOptions, MessageContext, RemoteResponder,
    ReplyHubInvoker,
};
use raddle_serialization::EndpointCodecs;
use raddle_tcp::{Consumer, Delivery, Limits, Peer, Publisher, Receiver};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use codec_rpc_support::{Answer, Double, Frame, Refused, Response};
use codec_rpc_support::{assert_representation, codecs, origin, publisher, selected};

struct Requests(RemoteResponder<PoisonNeighbor>);
impl Consumer for Requests {
    async fn accept(&self, _: Peer, envelope: Envelope) -> Delivery {
        match self
            .0
            .respond(&MessageContext::new(origin("caller"), envelope, Utc::now()))
            .await
        {
            Ok(()) => Delivery::Accepted,
            Err(_) => Delivery::Rejected,
        }
    }
}

struct Replies {
    invoker: Arc<ReplyHubInvoker<Publisher>>,
    refused: Arc<AtomicUsize>,
}
impl Consumer for Replies {
    async fn accept(&self, _: Peer, envelope: Envelope) -> Delivery {
        match self.invoker.accept_reply(&origin("server"), envelope) {
            Ok(false) => {
                self.refused.fetch_add(1, Ordering::SeqCst);
                Delivery::Accepted
            }
            Ok(true) => Delivery::Accepted,
            Err(_) => Delivery::Retry,
        }
    }
}

// An intentionally bad neighbor before every real reply travels through exactly
// the same configured production publisher/socket and must not steal its waiter.
struct PoisonNeighbor(Publisher);
impl Transport for PoisonNeighbor {
    async fn publish(&self, alias: &str, envelope: &Envelope) -> Result<(), TransportError> {
        let frame = envelope.decode::<Frame>().unwrap();
        let malformed = envelope
            .rebody(
                &Frame {
                    mode: frame.mode,
                    response: Response::Success {
                        body: b"RDSB\x02malformed".to_vec(),
                    },
                },
                Default::default(),
            )
            .unwrap();
        self.0.publish(alias, &malformed).await?;
        self.0.publish(alias, envelope).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "child helper, invoked by selected_application_bodies_cross_processes"]
async fn selected_rpc_child() {
    let Ok(address) = std::env::var("RADDLE_CODEC_RPC_REPLY") else {
        return;
    };
    let address: SocketAddr = address.parse().unwrap();
    assert!(address.ip().is_loopback());
    let media = std::env::var("RADDLE_CODEC_RPC_MEDIA").unwrap();
    let codecs = codecs(&media);
    let mut runtime = RuntimeBuilder::default();
    runtime
        .handle(|request: Double| async move {
            if request.number == 999 {
                return Err(Refused);
            }
            if request.number == 888 {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            Ok(Answer {
                number: request.number * 2,
            })
        })
        .unwrap();
    // Reply alias deliberately retains JSON OUTER protocol; its application
    // response bytes inside the v2 frame are genuinely selected by the responder.
    let mut responder = RemoteResponder::new(
        PoisonNeighbor(publisher("replies", address)),
        runtime.build(),
        ["replies".into()],
    )
    .unwrap()
    .with_selected_rpc(selected(Arc::clone(&codecs)))
    .unwrap();
    responder.handle::<Double>().unwrap();
    let receiver = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Limits::default())
        .await
        .unwrap()
        .with_codecs(codecs);
    println!("RADDLE_CODEC_RPC_READY={}", receiver.local_addr().unwrap());
    std::io::stdout().flush().unwrap();
    let stop = CancellationToken::new();
    let task = tokio::spawn(receiver.run(Arc::new(Requests(responder)), stop.clone()));
    tokio::task::spawn_blocking(|| {
        let mut command = String::new();
        std::io::stdin().read_line(&mut command).unwrap();
        assert_eq!(command.trim(), "stop");
    })
    .await
    .unwrap();
    stop.cancel();
    task.await.unwrap().unwrap();
}

struct OwnedChild(Child);
impl OwnedChild {
    fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.0.wait()
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

type Capture = Arc<Mutex<Vec<([u8; 5], Vec<u8>)>>>;

// Capture actual bytes between production TCP endpoints without substituting a
// fake transport. This fixture forwards the existing framing, not codec output.
async fn proxy(
    upstream: SocketAddr,
    stop: CancellationToken,
) -> (SocketAddr, Capture, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let captures = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&captures);
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                () = stop.cancelled() => break,
                Some(result) = connections.join_next(), if !connections.is_empty() => { result.unwrap(); },
                connection = listener.accept() => {
                    let (mut input, _) = connection.unwrap();
                    let captured = Arc::clone(&captured);
                    connections.spawn(async move {
                        let mut output = TcpStream::connect(upstream).await.unwrap();
                        let mut magic = [0; 5];
                        input.read_exact(&mut magic).await.unwrap();
                        let count = input.read_u16().await.unwrap();
                        output.write_all(&magic).await.unwrap();
                        output.write_u16(count).await.unwrap();
                        for _ in 0..count {
                            let length = input.read_u32().await.unwrap();
                            assert!(length <= 1_048_576);
                            let mut bytes = vec![0; length as usize];
                            input.read_exact(&mut bytes).await.unwrap();
                            captured.lock().unwrap().push((magic, bytes.clone()));
                            output.write_u32(length).await.unwrap();
                            output.write_all(&bytes).await.unwrap();
                            let Ok(status) = output.read_u8().await else { return };
                            // This path uses separate reply aliases, not RDTC inline replies.
                            input.write_u8(status).await.unwrap();
                        }
                    });
                }
            }
        }
        connections.abort_all();
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result {
                assert!(error.is_cancelled());
            }
        }
    });
    (address, captures, task)
}

fn bus(
    runtime: raddle::Runtime,
    address: SocketAddr,
    codecs: Arc<EndpointCodecs>,
    invoker: Arc<ReplyHubInvoker<Publisher>>,
) -> raddle_messaging::MessageBus {
    let mut bus = BusBuilder::new(runtime);
    bus.remote(
        "requests",
        publisher("requests", address)
            .with_codecs("requests", codecs)
            .unwrap(),
    )
    .unwrap();
    bus.route::<Double>("requests").unwrap();
    bus.remote_invoker(invoker, true);
    bus.build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_application_bodies_cross_processes_with_wire_captures_and_healthy_neighbors() {
    for media in ["application/json", "binary/messagepack", "binary/protobuf"] {
        let replies = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Limits::default())
            .await
            .unwrap();
        let stop = CancellationToken::new();
        let (reply_address, reply_capture, reply_proxy) =
            proxy(replies.local_addr().unwrap(), stop.clone()).await;
        let mut child = OwnedChild(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "selected_rpc_child", "--ignored", "--nocapture"])
                .env("RADDLE_CODEC_RPC_REPLY", reply_address.to_string())
                .env("RADDLE_CODEC_RPC_MEDIA", media)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let output = child.0.stdout.take().unwrap();
        let (ready, address) = oneshot::channel();
        let drain = tokio::task::spawn_blocking(move || {
            let mut ready = Some(ready);
            for line in BufReader::new(output).lines() {
                let line = line.unwrap();
                if let Some((_, address)) = line.split_once("RADDLE_CODEC_RPC_READY=")
                    && let Some(ready) = ready.take()
                {
                    let _ = ready.send(address.parse::<SocketAddr>().unwrap());
                }
            }
        });
        let server = tokio::time::timeout(Duration::from_secs(60), address)
            .await
            .unwrap()
            .unwrap();
        assert!(server.ip().is_loopback());
        let (request_address, request_capture, request_proxy) = proxy(server, stop.clone()).await;
        let codecs = codecs(media);
        let invoker = Arc::new(
            ReplyHubInvoker::new(
                publisher("requests", request_address)
                    .with_codecs("requests", Arc::clone(&codecs))
                    .unwrap(),
                2,
                "replies",
                [("requests".into(), "server".into())],
            )
            .unwrap()
            .with_selected_rpc(selected(Arc::clone(&codecs)))
            .unwrap(),
        );
        let refused = Arc::new(AtomicUsize::new(0));
        let listening = tokio::spawn(replies.run(
            Arc::new(Replies {
                invoker: invoker.clone(),
                refused: refused.clone(),
            }),
            stop.clone(),
        ));
        let remote = bus(
            RuntimeBuilder::default().build(),
            request_address,
            Arc::clone(&codecs),
            Arc::clone(&invoker),
        );
        for bad in 0..3 {
            for position in 0..3 {
                let result = remote
                    .invoke(
                        Double {
                            number: if position == bad { 999 } else { 21 },
                        },
                        origin("caller"),
                    )
                    .await;
                if position == bad {
                    assert!(matches!(
                        result,
                        Err(InvokeError::Bus(BusError::RemoteFailure))
                    ));
                } else {
                    assert_eq!(result.unwrap().number, 42);
                }
            }
            assert_eq!(
                remote
                    .invoke(Double { number: 2 }, origin("caller"))
                    .await
                    .unwrap()
                    .number,
                4
            );
        }
        assert_eq!(invoker.pending().unwrap(), 0);
        assert_eq!(refused.load(Ordering::SeqCst), 12);
        // Local-first does not hit the socket, even with explicit selected remote configuration.
        let before = request_capture.lock().unwrap().len();
        let mut runtime = RuntimeBuilder::default();
        runtime
            .handle(|_: Double| async { Ok(Answer { number: 63 }) })
            .unwrap();
        assert_eq!(
            bus(
                runtime.build(),
                request_address,
                Arc::clone(&codecs),
                invoker.clone()
            )
            .invoke(Double { number: 21 }, origin("caller"))
            .await
            .unwrap()
            .number,
            63
        );
        assert_eq!(request_capture.lock().unwrap().len(), before);
        assert!(matches!(
            remote
                .invoke_with(
                    Double { number: 888 },
                    origin("caller"),
                    InvokeOptions {
                        remote_timeout: Duration::from_millis(100),
                        ..Default::default()
                    }
                )
                .await,
            Err(InvokeError::TimedOut)
        ));
        assert_eq!(invoker.pending().unwrap(), 0);
        assert_eq!(
            remote
                .invoke(Double { number: 3 }, origin("caller"))
                .await
                .unwrap()
                .number,
            6
        );

        // Inspect raw socket representations, not a registered-codec-only round trip.
        {
            let requests = request_capture.lock().unwrap();
            assert!(requests.iter().all(|(magic, _)| magic == b"RDTC\x02"));
            let request = &requests
                .iter()
                .find(|(_, bytes)| {
                    codecs
                        .decode(bytes)
                        .unwrap()
                        .decode::<Double>()
                        .unwrap()
                        .number
                        == 21
                })
                .unwrap()
                .1;
            assert_representation(request, media, 21);
        }
        {
            let captured = reply_capture.lock().unwrap();
            assert!(captured.iter().all(|(magic, _)| magic == b"RDTC\x01"));
            let good = captured
                .iter()
                .find_map(|(_, bytes)| {
                    let frame = Envelope::from_slice(bytes, Default::default())
                        .unwrap()
                        .decode::<Frame>()
                        .unwrap();
                    match frame.response {
                        Response::Success { body }
                            if body.starts_with(b"RDSB\x01")
                                && codecs
                                    .decode(&body)
                                    .unwrap()
                                    .decode::<Answer>()
                                    .unwrap()
                                    .number
                                    == 42 =>
                        {
                            Some(body)
                        }
                        _ => None,
                    }
                })
                .unwrap();
            assert_representation(&good, media, 42);
        }
        child.0.stdin.take().unwrap().write_all(b"stop\n").unwrap();
        let status = tokio::task::spawn_blocking(move || child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success());
        drain.await.unwrap();
        stop.cancel();
        listening.await.unwrap().unwrap();
        reply_proxy.await.unwrap();
        request_proxy.await.unwrap();
    }
}
