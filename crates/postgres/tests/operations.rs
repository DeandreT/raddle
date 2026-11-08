//! Payload-free tenant statistics and availability against PostgreSQL.
#![allow(clippy::unwrap_used)]

use chrono::Utc;
use raddle::wire::{Envelope, WireMessage};
use raddle_postgres::{Delivery, DeliveryKind, Store};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Serialize, Deserialize)]
struct Event {
    private: String,
}
impl WireMessage for Event {
    const NAME: &'static str = "operations.event";
    const VERSION: u32 = 1;
}

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn statistics_are_tenant_scoped_and_never_include_payloads(pool: PgPool) {
    let store = Store::new(pool);
    store.migrate().await.unwrap();
    assert!(store.is_available().await);
    for tenant in ["one", "two"] {
        let mut scope = store.begin().await.unwrap();
        scope
            .stage(
                "test",
                tenant,
                &Envelope::encode(&Event {
                    private: "sensitive-body".into(),
                })
                .unwrap(),
                &[Delivery::new(DeliveryKind::Local, "handler", Utc::now())],
            )
            .await
            .unwrap();
        scope.commit(Utc::now()).await.unwrap();
    }
    let counts = store.statistics("one").await.unwrap();
    assert_eq!(counts.ready, 1);
    assert_eq!(counts.due, 1);
    assert_eq!(counts.dead, 0);
    assert!(
        !serde_json::to_string(&counts)
            .unwrap()
            .contains("sensitive-body")
    );
    assert_eq!(store.statistics("absent").await.unwrap().ready, 0);
    assert!(store.statistics("").await.is_err());
}
