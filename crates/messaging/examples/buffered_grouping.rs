//! Complete in-memory Send/Publish/direct/receive composition; no broker or database.

use std::error::Error;
use std::time::Duration;

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::grouping::{DispatchOptions, GroupId};
use raddle::routing::EndpointAlias;
use raddle::sequencing::SlotSet;
use raddle::wire::{Envelope, WireMessage};
use raddle_host::Host;
use raddle_messaging::routing::{
    GroupingRule, MessageDeclaration, MessageRole, MessageSelector, RoutingConfig, RuleMatch,
};
use raddle_messaging::{
    BusBuilder, JsonIngress, LocalQueue, QueueError, QueueMode, QueueSettings, TrustedOrigin,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Serialize, Deserialize)]
struct OrderCommand {
    order_id: String,
    sequence: u32,
}
impl WireMessage for OrderCommand {
    const NAME: &'static str = "example.order-command";
    const VERSION: u32 = 1;
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let (queue, runner) = LocalQueue::partitioned(
        QueueSettings {
            mode: QueueMode::Buffered,
            capacity: 8,
            parallelism: 2,
        },
        SlotSet::new("orders", 7)?,
    )?;
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.local("orders", queue)?;
    let (done, mut received) = mpsc::channel(4);
    builder.handle::<OrderCommand, _, _>("orders", move |command, context| {
        let done = done.clone();
        async move {
            let group = context
                .envelope()
                .dispatch()
                .and_then(|dispatch| dispatch.group())
                .ok_or(QueueError::Handler)?;
            done.send((command.sequence, group.as_str().to_owned()))
                .await
                .map_err(|_| QueueError::Closed)
        }
    })?;
    let mut routing = RoutingConfig::default();
    routing
        .declare(MessageDeclaration::of::<OrderCommand>(MessageRole::Handled)?.family("orders")?)?;
    routing.grouping().rule(GroupingRule::tenant())?;
    routing.topology(
        "order-topology",
        MessageSelector::family("orders")?,
        vec![],
        vec![GroupingRule::typed::<OrderCommand>(|command| {
            RuleMatch::Matched(GroupId::new(&command.order_id))
        })],
        vec![EndpointAlias::try_new("orders")?],
    )?;
    let bus = builder.build_routed(routing)?;
    let origin = TrustedOrigin::authenticated("example-tenant", "example-ingress", None)?;
    let mut host = Host::new(Duration::from_secs(5))?;
    runner.attach(&mut host, "orders")?;
    let shutdown = host.handle();
    let owner = tokio::spawn(host.run());
    let command = |sequence| OrderCommand {
        order_id: "order-42".into(),
        sequence,
    };
    bus.send(&command(1), origin.clone()).await?;
    bus.publish_with_options(
        &command(2),
        origin.clone(),
        DispatchOptions {
            group: Some(GroupId::new("order-42")),
            partition_key: None,
        },
    )
    .await?;
    bus.send_to("orders", &command(3), origin.clone()).await?;
    bus.receive(
        &JsonIngress::new(Default::default()),
        &Envelope::encode(&command(4))?.to_vec()?,
        origin,
        Utc::now(),
        true,
    )
    .await?;
    // Actual completion receipts establish receiver execution before shutdown;
    // successful buffer admission alone does not mean the handler has run.
    for expected in 1..=4 {
        let (sequence, group) = tokio::time::timeout(Duration::from_secs(5), received.recv())
            .await?
            .ok_or("completion receiver closed")?;
        if sequence != expected || group != "order-42" {
            return Err("unexpected execution identity/order".into());
        }
    }
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), owner).await???;
    println!("Completed four buffered commands through one slot owner.");
    Ok(())
}
