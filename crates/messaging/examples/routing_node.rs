//! Two-process native routing walkthrough. Raw loopback TCP is not authentication.
//! Run receiver first, then sender with its printed port; send `stop` on receiver
//! stdin after the sender exits. Every business record includes its real envelope ID.

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::routing::EndpointAlias;
use raddle::wire::{Envelope, WireMessage};
use raddle_messaging::routing::{MessageDeclaration, MessageRole, MessageSelector, RoutingConfig};
use raddle_messaging::{
    BusBuilder, JsonIngress, LocalQueue, MessageBus, QueueMode, QueueSettings, TrustedOrigin,
};
use raddle_tcp::{Consumer, Delivery, Endpoint, Limits, Peer, Publisher, Receiver};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize)]
struct Order {
    number: u32,
    mode: String,
}
impl WireMessage for Order {
    const NAME: &'static str = "routing.example-order";
    const VERSION: u32 = 1;
}
fn origin() -> Result<TrustedOrigin, raddle::wire::WireError> {
    TrustedOrigin::authenticated("example-tenant", "configured-example-peer", None)
}
struct Inbox(MessageBus);
impl Consumer for Inbox {
    async fn accept(&self, _: Peer, envelope: Envelope) -> Delivery {
        let Ok(bytes) = envelope.to_vec() else {
            return Delivery::Rejected;
        };
        let Ok(origin) = origin() else {
            return Delivery::Rejected;
        };
        match self
            .0
            .receive(
                &JsonIngress::new(Default::default()),
                &bytes,
                origin,
                Utc::now(),
                true,
            )
            .await
        {
            Ok(()) => Delivery::Accepted,
            Err(_) => Delivery::Retry,
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().collect();
    match arguments.get(1).map(String::as_str) {
        Some("receiver") => {
            let receiver =
                Receiver::bind((Ipv4Addr::LOCALHOST, 0).into(), Limits::default()).await?;
            let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
            let (queue, _runner) = LocalQueue::new(QueueSettings {
                mode: QueueMode::Inline,
                capacity: 8,
                parallelism: 1,
            })?;
            builder.local("business", queue)?;
            builder.handle::<Order, _, _>("business", |order, context| async move {
                println!("{}", serde_json::json!({"event":"business", "id":context.envelope().id(), "number":order.number,
                    "mode":order.mode, "contract":context.envelope().contract(), "tenant":context.origin().tenant()}));
                let _ = std::io::stdout().flush();
                Ok(())
            })?;
            let mut config = RoutingConfig::default();
            config.declare(MessageDeclaration::of::<Order>(MessageRole::Handled)?)?;
            config.local_route::<Order>("business")?;
            let bus = builder.build_routed(config)?;
            println!(
                "{}",
                serde_json::json!({"event":"ready", "port":receiver.local_addr()?.port()})
            );
            std::io::stdout().flush()?;
            let cancellation = CancellationToken::new();
            let signal = cancellation.clone();
            let input = std::thread::spawn(move || {
                let _ = std::io::stdin().lock().lines().next();
                signal.cancel();
            });
            receiver.run(Arc::new(Inbox(bus)), cancellation).await?;
            input.join().map_err(|_| "receiver stdin task failed")?;
        }
        Some("sender") => {
            let port: u16 = arguments
                .get(2)
                .ok_or("sender requires receiver port")?
                .parse()?;
            let publisher = Publisher::try_new(
                BTreeMap::from([("peer".into(), Endpoint::try_new("127.0.0.1", port)?)]),
                Limits {
                    deadline: Duration::from_secs(10),
                    ..Default::default()
                },
            )?;
            let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
            builder.remote("peer", publisher)?;
            let mut config = RoutingConfig::default();
            config.declare(MessageDeclaration::of::<Order>(MessageRole::PublishOnly)?)?;
            config.rule(
                "order-publication",
                "Send declared example orders to configured peer",
                MessageSelector::exact::<Order>()?,
                vec![EndpointAlias::try_new("peer")?],
            )?;
            let bus = builder.build_routed(config)?;
            bus.send_to(
                "peer",
                &Order {
                    number: 1,
                    mode: "destination".into(),
                },
                origin()?,
            )
            .await?;
            bus.publish(
                &Order {
                    number: 2,
                    mode: "publication".into(),
                },
                origin()?,
            )
            .await?;
            println!(
                "{}",
                serde_json::json!({"event":"confirmed", "destinations":bus.explain::<Order>()?.routes.iter().map(|r| &r.endpoint).collect::<Vec<_>>() })
            );
        }
        _ => return Err("usage: routing_node receiver | sender PORT".into()),
    }
    Ok(())
}
