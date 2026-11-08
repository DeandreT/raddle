//! Shared custody invariants against a real, isolated PostgreSQL database.
#![allow(clippy::unwrap_used)]

use chrono::Utc;
use raddle::wire::{Envelope, WireMessage};
use raddle_postgres::{Delivery, DeliveryKind, Error, Store};
use raddle_testkit::conformance::{self, CustodyProbe, CustodyResult};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

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

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn shared_custody_contract(pool: PgPool) {
    let original = Envelope::encode(&Message(1)).unwrap();
    let changed =
        Envelope::with_identity(original.id(), original.created_at(), &Message(2)).unwrap();
    conformance::verify_custody(&Probe(Store::new(pool)), &original, &changed)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn upgrade_from_initial_schema_preserves_queued_work(pool: PgPool) {
    let mut initial = sqlx::migrate!();
    initial.migrations = std::borrow::Cow::Owned(initial.iter().take(1).cloned().collect());
    initial.run(&pool).await.unwrap();
    let store = Store::new(pool);
    let original = Envelope::encode(&Message(42)).unwrap();
    Probe(store.clone())
        .stage("conformance-a", &original)
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let claims = store
        .claim(
            DeliveryKind::Local,
            1,
            std::time::Duration::from_secs(30),
            Utc::now(),
        )
        .await
        .unwrap();
    assert_eq!(
        claims[0].envelope().unwrap().decode::<Message>().unwrap().0,
        42
    );
    let debug = format!("{:?}", claims[0]);
    assert!(!debug.contains("envelope"));
    assert!(!debug.contains("tenant"));
    store.migrate().await.unwrap();
}
