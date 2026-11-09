use chrono::Utc;
use raddle_postgres::{
    Store,
    external_tables::{Aliases, Correction, Listener, ReplayOutcome},
};
use serde_json::json;
use sqlx::PgPool;

use super::support::{self, Order};

#[derive(serde::Serialize)]
struct PanickingModel;
impl<'de> serde::Deserialize<'de> for PanickingModel {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        panic!("injected application decoder panic")
    }
}
impl raddle::wire::WireMessage for PanickingModel {
    const NAME: &'static str = "external.panicking-model";
    const VERSION: u32 = 1;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_decoder_unwind_is_row_local_and_long_explicit_aliases_work(pool: PgPool) {
    let mut c = support::config();
    c.alias_column = Some("kind".into());
    let long = "a".repeat(500);
    let mut aliases = Aliases::default();
    aliases.register::<Order>("order").unwrap();
    aliases.register::<Order>(&long).unwrap();
    aliases.register::<PanickingModel>("panic").unwrap();
    let listener = Listener::new(
        Store::new(pool.clone()),
        c.clone(),
        aliases,
        Default::default(),
    )
    .unwrap();
    listener.provision_source().await.unwrap();
    support::effects(&pool).await;
    support::insert(&pool, &c, 1, json!({"value":1}), Some("panic")).await;
    support::insert(&pool, &c, 2, json!({"value":2}), Some(&long)).await;
    support::insert(&pool, &c, 3, json!({"value":3}), Some("order")).await;
    let report = listener.poll_once(Utc::now()).await.unwrap();
    assert_eq!((report.accepted, report.quarantined), (2, 1));
    let item = listener
        .search_quarantine(0, 10)
        .await
        .unwrap()
        .items
        .remove(0);
    assert_eq!(item.code, "decoder-panic");
    support::worker(&pool).run_once(10).await.unwrap();
    support::healthy(&pool, &listener, &c, 4).await;
    let correction = Correction {
        body: "{\"value\":1}".into(),
        alias: Some("order".into()),
    };
    assert_eq!(
        listener
            .replay_quarantined(
                item.sequence,
                Some(&correction),
                "op",
                "replace failed projection",
                Utc::now()
            )
            .await
            .unwrap(),
        ReplayOutcome::Accepted
    );
    support::worker(&pool).run_once(10).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 4);
}
