use chrono::Utc;
use raddle::wire::{Codecs, WireMessage};
use raddle_postgres::{
    RetryPolicy, Store, Worker,
    external_tables::{Aliases, Listener, ReplayOutcome},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{self, Order};

#[derive(Serialize, Deserialize)]
struct AddedType {
    text: String,
}
impl WireMessage for AddedType {
    const NAME: &'static str = "external.added";
    const VERSION: u32 = 1;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_new_registered_contract_replays_without_resetting_accepted_receipts(
    pool: PgPool,
) {
    let mut config = support::config();
    config.alias_column = Some("kind".into());
    config.default_alias = None;
    let first = support::listener(&pool, config.clone(), Default::default());
    first.provision_source().await.unwrap();
    support::effects(&pool).await;
    support::insert(&pool, &config, 1, json!({"value":1}), Some("order")).await;
    support::insert(
        &pool,
        &config,
        2,
        json!({"text":"new contract"}),
        Some("added"),
    )
    .await;
    let report = first.poll_once(Utc::now()).await.unwrap();
    assert_eq!((report.accepted, report.quarantined), (1, 1));
    support::worker(&pool).run_once(10).await.unwrap();
    let receipt: Vec<u8> = sqlx::query_scalar(
        "SELECT canonical_hash FROM raddle_external_inputs WHERE input_id=$1 AND status='accepted'",
    )
    .bind(Uuid::from_u128(1))
    .fetch_one(&pool)
    .await
    .unwrap();
    let envelope: Vec<u8> =
        sqlx::query_scalar("SELECT envelope FROM raddle_jobs WHERE message_id=$1")
            .bind(Uuid::from_u128(1))
            .fetch_one(&pool)
            .await
            .unwrap();
    let sequence = first.search_quarantine(0, 10).await.unwrap().items[0].sequence;
    let mut aliases = Aliases::default();
    aliases.register::<Order>("order").unwrap();
    aliases.register::<AddedType>("added").unwrap();
    let updated = Listener::new(
        Store::new(pool.clone()),
        config.clone(),
        aliases,
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        updated
            .replay_quarantined(
                sequence,
                None,
                "operator",
                "add explicit native contract",
                Utc::now()
            )
            .await
            .unwrap(),
        ReplayOutcome::Accepted
    );
    let mut worker = Worker::new(Store::new(pool.clone()), RetryPolicy::default());
    worker
        .handle::<AddedType, _>("external-inbox", Codecs::default(), |message, scope| {
            Box::pin(async move {
                let id = scope.envelope().unwrap().unwrap().id();
                sqlx::query("INSERT INTO external_effects VALUES($1,$2)")
                    .bind(id)
                    .bind(message.text)
                    .execute(scope.connection())
                    .await
                    .unwrap();
                Ok(())
            })
        })
        .unwrap();
    worker.run_once(10).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 2);
    let retained: Vec<u8> = sqlx::query_scalar(
        "SELECT canonical_hash FROM raddle_external_inputs WHERE input_id=$1 AND status='accepted'",
    )
    .bind(Uuid::from_u128(1))
    .fetch_one(&pool)
    .await
    .unwrap();
    let retained_envelope: Vec<u8> =
        sqlx::query_scalar("SELECT envelope FROM raddle_jobs WHERE message_id=$1")
            .bind(Uuid::from_u128(1))
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(receipt, retained);
    assert_eq!(envelope, retained_envelope);
    support::insert(&pool, &config, 1, json!({"value":1}), Some("order")).await;
    assert_eq!(updated.poll_once(Utc::now()).await.unwrap().duplicates, 1);
    assert_eq!(support::count(&pool, "raddle_jobs").await, 2);
}
