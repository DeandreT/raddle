//! Real-socket request/reply through the ordinary bounded raddle runtime.
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::Ipv4Addr;
use std::sync::Arc;

use raddle::wire::{Envelope, WireMessage};
use raddle::{Request, Runtime, RuntimeBuilder};
use raddle_tcp::{Consumer, Delivery, Endpoint, Limits, Peer, Publisher, Receiver};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Deserialize, Serialize)]
struct Double(u32);
impl WireMessage for Double {
    const NAME: &'static str = "example.double";
    const VERSION: u32 = 1;
}
impl Request for Double {
    type Reply = Answer;
    type Error = Infallible;
}
#[derive(Deserialize, Serialize)]
struct Answer(u64);
impl WireMessage for Answer {
    const NAME: &'static str = "example.answer";
    const VERSION: u32 = 1;
}

struct Application(Runtime);
impl Consumer for Application {
    async fn accept(&self, _: Peer, envelope: Envelope) -> Delivery {
        let Ok(command) = envelope.decode::<Double>() else {
            return Delivery::Rejected;
        };
        let Ok(answer) = self.0.invoke(command).await else {
            return Delivery::Retry;
        };
        match Envelope::encode(&answer)
            .and_then(|reply| reply.correlated(envelope.id(), envelope.id()))
        {
            Ok(reply) => Delivery::Reply(reply),
            Err(_) => Delivery::Retry,
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = RuntimeBuilder::try_new(4)?;
    runtime.handle(|message: Double| async move { Ok(Answer(u64::from(message.0) * 2)) })?;
    let receiver = Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Limits::default()).await?;
    let address = receiver.local_addr()?;
    let stop = CancellationToken::new();
    let running = tokio::spawn(receiver.run(Arc::new(Application(runtime.build())), stop.clone()));
    let publisher = Publisher::try_new(
        BTreeMap::from([(
            "double".into(),
            Endpoint::try_new("127.0.0.1", address.port())?,
        )]),
        Limits::default(),
    )?;
    let request = Envelope::encode(&Double(21))?;
    let answer = publisher.request("double", &request, Answer::NAME).await;
    stop.cancel();
    running.await??;
    assert_eq!(answer?.decode::<Answer>()?.0, 42);
    Ok(())
}
