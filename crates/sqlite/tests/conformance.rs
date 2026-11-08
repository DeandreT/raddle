//! The same custody invariants run against the SQLite implementation.
#![allow(clippy::unwrap_used)]

use chrono::Utc;
use raddle::wire::{Envelope, WireMessage};
use raddle_sqlite::{Delivery, DeliveryKind, Error, Store};
use raddle_testkit::conformance::{self, CustodyProbe, CustodyResult};
use serde::{Deserialize, Serialize};

struct Probe(Store);
impl CustodyProbe for Probe {
    type Error = Error;
    async fn stage(&self, tenant: &str, envelope: &Envelope) -> Result<CustodyResult, Error> {
        let mut scope = self.0.begin().await?;
        match scope
            .stage(
                "conformance",
                tenant,
                envelope,
                &[Delivery::new(
                    DeliveryKind::Local,
                    "test",
                    envelope.created_at(),
                )],
            )
            .await
        {
            Ok(ids) => {
                scope.commit(Utc::now()).await?;
                Ok(CustodyResult::Accepted(ids[0]))
            }
            Err(Error::IdentityConflict) => {
                scope.rollback().await?;
                Ok(CustodyResult::Conflict)
            }
            Err(error) => Err(error),
        }
    }
}
#[derive(Serialize, Deserialize)]
struct Message(u32);
impl WireMessage for Message {
    const NAME: &'static str = "test.conformance";
    const VERSION: u32 = 1;
}

#[tokio::test]
async fn shared_custody_contract() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path().join("test.sqlite"))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let original = Envelope::encode(&Message(1)).unwrap();
    let changed =
        Envelope::with_identity(original.id(), original.created_at(), &Message(2)).unwrap();
    conformance::verify_custody(&Probe(store.clone()), &original, &changed)
        .await
        .unwrap();
    let claims = store
        .claim(
            DeliveryKind::Local,
            1,
            std::time::Duration::from_secs(30),
            Utc::now(),
        )
        .await
        .unwrap();
    let debug = format!("{:?}", claims[0]);
    assert!(!debug.contains("bytes"));
    assert!(!debug.contains("tenant"));
    store.close().await;
}
