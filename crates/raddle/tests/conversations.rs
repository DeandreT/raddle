//! Ephemeral conversation identity, source validation and capacity release.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use raddle::conversation::{Error, ReplyHub};
use raddle::wire::{Envelope, WireMessage};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Reply(u32);
impl WireMessage for Reply {
    const NAME: &'static str = "orders.reply";
    const VERSION: u32 = 1;
}

#[tokio::test]
async fn validates_responder_and_consumes_only_one_matching_reply() {
    let hub = ReplyHub::try_new(1).unwrap();
    let id = Uuid::new_v4();
    let waiter = hub.register(id, "orders", Reply::NAME).unwrap();
    let reply = Envelope::encode(&Reply(42)).unwrap().correlated(id, id);
    assert!(!hub.deliver("other", reply.clone()).unwrap());
    assert!(hub.deliver("orders", reply.clone()).unwrap());
    assert!(!hub.deliver("orders", reply).unwrap());
    assert_eq!(
        waiter
            .wait(Duration::from_secs(1))
            .await
            .unwrap()
            .decode::<Reply>()
            .unwrap()
            .0,
        42
    );
}

#[tokio::test]
async fn timeout_and_drop_release_bounded_capacity() {
    let hub = ReplyHub::try_new(1).unwrap();
    let waiter = hub.register(Uuid::new_v4(), "orders", Reply::NAME).unwrap();
    assert!(hub.register(Uuid::new_v4(), "orders", Reply::NAME).is_err());
    drop(waiter);
    let waiter = hub.register(Uuid::new_v4(), "orders", Reply::NAME).unwrap();
    assert!(matches!(
        waiter.wait(Duration::from_millis(1)).await,
        Err(Error::Timeout)
    ));
    assert!(hub.register(Uuid::new_v4(), "orders", Reply::NAME).is_ok());
}

#[tokio::test]
async fn dropping_an_old_waiter_cannot_remove_a_new_registration() {
    let hub = ReplyHub::try_new(1).unwrap();
    let id = Uuid::new_v4();
    let old = hub.register(id, "orders", Reply::NAME).unwrap();
    hub.deliver(
        "orders",
        Envelope::encode(&Reply(1)).unwrap().correlated(id, id),
    )
    .unwrap();
    let new = hub.register(id, "orders", Reply::NAME).unwrap();
    drop(old);
    assert!(
        hub.deliver(
            "orders",
            Envelope::encode(&Reply(2)).unwrap().correlated(id, id)
        )
        .unwrap()
    );
    assert_eq!(
        new.wait(Duration::from_secs(1))
            .await
            .unwrap()
            .decode::<Reply>()
            .unwrap()
            .0,
        2
    );
}
