//! Shipping native TCP request/reply over two independently owned OS processes.
//!
//! The fixture trusts its explicitly configured, synthetic loopback participants.
//! Raw TCP and these application headers are not authentication in deployments.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use raddle::wire::{Envelope, WireMessage};
use raddle::{Request, Runtime, RuntimeBuilder};
use raddle_messaging::{
    BusBuilder, BusError, InvokeError, InvokeOptions, MessageContext, RemoteResponder,
    ReplyHubInvoker, TrustedOrigin,
};
use raddle_tcp::{Consumer, Delivery, Endpoint, Limits, Peer, Publisher, Receiver};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[derive(Deserialize, Serialize)]
struct Double(u32);
impl WireMessage for Double {
    const NAME: &'static str = "tcp.bus-double";
    const VERSION: u32 = 1;
}
impl Request for Double {
    type Reply = Answer;
    type Error = Refused;
}
#[derive(Deserialize, Serialize)]
struct Answer(u32);
impl WireMessage for Answer {
    const NAME: &'static str = "tcp.bus-answer";
    const VERSION: u32 = 1;
}
#[derive(Debug, thiserror::Error)]
#[error("synthetic request refused")]
struct Refused;

fn origin(source: &str) -> TrustedOrigin {
    TrustedOrigin::authenticated("configured-tenant", source, None).unwrap()
}
fn publisher(alias: &str, address: SocketAddr) -> Publisher {
    Publisher::try_new(
        BTreeMap::from([(
            alias.into(),
            Endpoint::try_new(address.ip().to_string(), address.port()).unwrap(),
        )]),
        Limits::default(),
    )
    .unwrap()
}

struct Requests(RemoteResponder<Publisher>);
impl Consumer for Requests {
    async fn accept(&self, _: Peer, envelope: Envelope) -> Delivery {
        let context = MessageContext::new(origin("configured-caller"), envelope, Utc::now());
        match self.0.respond(&context).await {
            Ok(()) => Delivery::Accepted,
            Err(_) => Delivery::Retry,
        }
    }
}
struct Replies(Arc<ReplyHubInvoker<Publisher>>);
impl Consumer for Replies {
    async fn accept(&self, _: Peer, envelope: Envelope) -> Delivery {
        match self.0.accept_reply(&origin("configured-server"), envelope) {
            Ok(_) => Delivery::Accepted,
            Err(_) => Delivery::Retry,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "subprocess helper, invoked only by shipping_tcp_remote_invoke_crosses_processes"]
async fn native_tcp_child() {
    let Ok(reply_address) = std::env::var("RADDLE_TCP_FIXTURE_REPLY") else {
        return;
    };
    let reply_address: SocketAddr = reply_address.parse().unwrap();
    assert!(reply_address.ip().is_loopback());
    let mut runtime = RuntimeBuilder::try_new(2).unwrap();
    runtime
        .handle(|input: Double| async move {
            if input.0 == 999 {
                return Err(Refused);
            }
            if input.0 == 888 {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            Ok(Answer(input.0 * 2))
        })
        .unwrap();
    let mut responder = RemoteResponder::new(
        publisher("replies", reply_address),
        runtime.build(),
        ["replies".into()],
    )
    .unwrap();
    responder.handle::<Double>().unwrap();
    let listener = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Limits::default())
        .await
        .unwrap();
    println!("RADDLE_TCP_READY={}", listener.local_addr().unwrap());
    std::io::stdout().flush().unwrap();
    let stop = CancellationToken::new();
    let task = tokio::spawn(listener.run(Arc::new(Requests(responder)), stop.clone()));
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

fn bus(
    runtime: Runtime,
    address: SocketAddr,
    invoker: Arc<ReplyHubInvoker<Publisher>>,
) -> raddle_messaging::MessageBus {
    let mut builder = BusBuilder::new(runtime);
    builder
        .remote("requests", publisher("requests", address))
        .unwrap();
    builder.route::<Double>("requests").unwrap();
    builder.remote_invoker(invoker, true);
    builder.build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shipping_tcp_remote_invoke_crosses_processes_and_preserves_failure_boundaries() {
    let replies = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Limits::default())
        .await
        .unwrap();
    let reply_address = replies.local_addr().unwrap();
    let mut child = OwnedChild(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "native_tcp_child", "--ignored", "--nocapture"])
            .env("RADDLE_TCP_FIXTURE_REPLY", reply_address.to_string())
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
            if let Some((_, value)) = line.split_once("RADDLE_TCP_READY=")
                && let Some(ready) = ready.take()
            {
                let _ = ready.send(value.parse::<SocketAddr>().unwrap());
            }
        }
    });
    let server_address = tokio::time::timeout(Duration::from_secs(60), address)
        .await
        .unwrap()
        .unwrap();
    assert!(server_address.ip().is_loopback());
    let invoker = Arc::new(
        ReplyHubInvoker::new(
            publisher("requests", server_address),
            2,
            "replies",
            [("requests".into(), "configured-server".into())],
        )
        .unwrap(),
    );
    let stop = CancellationToken::new();
    let listening =
        tokio::spawn(replies.run(Arc::new(Replies(Arc::clone(&invoker))), stop.clone()));
    let remote = bus(
        RuntimeBuilder::default().build(),
        server_address,
        Arc::clone(&invoker),
    );
    for bad in [0, 1, 2] {
        for index in 0..3 {
            let result = remote
                .invoke(
                    Double(if index == bad { 999 } else { 21 }),
                    origin("configured-caller"),
                )
                .await;
            if index == bad {
                assert!(matches!(
                    result,
                    Err(InvokeError::Bus(BusError::RemoteFailure))
                ));
            } else {
                assert_eq!(result.unwrap().0, 42);
            }
        }
        assert_eq!(
            remote
                .invoke(Double(2), origin("configured-caller"))
                .await
                .unwrap()
                .0,
            4
        );
    }
    assert_eq!(invoker.pending().unwrap(), 0);
    let mut local = RuntimeBuilder::default();
    local
        .handle(|input: Double| async move { Ok(Answer(input.0 * 3)) })
        .unwrap();
    let local = bus(local.build(), server_address, Arc::clone(&invoker));
    assert_eq!(
        local
            .invoke(Double(21), origin("configured-caller"))
            .await
            .unwrap()
            .0,
        63
    );
    let disabled = InvokeOptions {
        allow_remote: false,
        ..Default::default()
    };
    assert!(matches!(
        remote
            .invoke_with(Double(1), origin("configured-caller"), disabled)
            .await,
        Err(InvokeError::Bus(BusError::RemoteDisabled))
    ));
    let timeout = InvokeOptions {
        remote_timeout: Duration::from_millis(100),
        ..Default::default()
    };
    assert!(matches!(
        remote
            .invoke_with(Double(888), origin("configured-caller"), timeout)
            .await,
        Err(InvokeError::TimedOut)
    ));
    assert_eq!(invoker.pending().unwrap(), 0);
    assert_eq!(
        remote
            .invoke(Double(3), origin("configured-caller"))
            .await
            .unwrap()
            .0,
        6
    );
    child.0.stdin.take().unwrap().write_all(b"stop\n").unwrap();
    let status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
    drain.await.unwrap();
    stop.cancel();
    listening.await.unwrap().unwrap();
}
