use chrono::Utc;
use raddle_postgres::{
    Store,
    external_tables::{Aliases, Correction, Listener, Options, ReplayOutcome},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{self, Order};

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_pending_snapshot_redelivery_reuses_budget_and_requires_audit(
    pool: PgPool,
) {
    let options = Options {
        quarantine_rows: 1,
        ..Default::default()
    };
    let mut config = support::config();
    config.alias_column = Some("kind".into());
    let listener = support::listener(&pool, config.clone(), options.clone());
    listener.provision_source().await.unwrap();
    support::effects(&pool).await;
    support::insert(&pool, &config, 1, json!({"value":1}), Some("future")).await;
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().quarantined, 1);
    let original = listener
        .search_quarantine(0, 10)
        .await
        .unwrap()
        .items
        .remove(0);
    support::insert(&pool, &config, 1, json!({"value":1}), Some("future")).await;
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().quarantined, 1);
    assert_eq!(support::count(&pool, "ext.items").await, 0);
    sqlx::query("INSERT INTO ext.items VALUES($1,'{\"value\":1}',$2::timestamptz,'future')")
        .bind(Uuid::from_u128(1))
        .bind(&original.diagnostic_time)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().quarantined, 1);
    assert_eq!(support::count(&pool, "ext.items").await, 0);
    let mut aliases = Aliases::default();
    aliases.register::<Order>("order").unwrap();
    aliases.register::<Order>("future").unwrap();
    let updated =
        Listener::new(Store::new(pool.clone()), config.clone(), aliases, options).unwrap();
    support::insert(&pool, &config, 1, json!({"value":1}), Some("future")).await;
    assert_eq!(updated.poll_once(Utc::now()).await.unwrap().quarantined, 1);
    assert_eq!(
        support::count(&pool, "raddle_jobs").await,
        0,
        "registration change bypassed raw replay audit"
    );
    assert_eq!(
        updated.search_quarantine(0, 10).await.unwrap().items.len(),
        1
    );
    support::healthy(&pool, &updated, &config, 2).await;
    assert_eq!(
        updated
            .replay_quarantined(
                original.sequence,
                None,
                "operator",
                "registered alias corrected",
                Utc::now()
            )
            .await
            .unwrap(),
        ReplayOutcome::Accepted
    );
    support::worker(&pool).run_once(10).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 2);
    let snapshots: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raddle_external_inputs WHERE code IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(snapshots, 1);
}

#[derive(Serialize, Deserialize)]
struct NullBody;
impl raddle::wire::WireMessage for NullBody {
    const NAME: &'static str = "external.null";
    const VERSION: u32 = 1;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_sql_null_is_not_json_null_and_default_mode_corrections_deduplicate(
    pool: PgPool,
) {
    let mut config = support::config();
    config.default_alias = Some("null-body".into());
    let mut aliases = Aliases::default();
    aliases.register::<NullBody>("null-body").unwrap();
    let listener = Listener::new(
        Store::new(pool.clone()),
        config.clone(),
        aliases,
        Default::default(),
    )
    .unwrap();
    listener.provision_source().await.unwrap();
    support::insert(&pool, &config, 1, json!(null), None).await;
    sqlx::query("INSERT INTO ext.items VALUES($1,NULL,clock_timestamp())")
        .bind(Uuid::from_u128(2))
        .execute(&pool)
        .await
        .unwrap();
    let report = listener.poll_once(Utc::now()).await.unwrap();
    assert_eq!((report.accepted, report.quarantined), (1, 1));
    let hashes: Vec<Vec<u8>> =
        sqlx::query_scalar("SELECT content_hash FROM raddle_external_inputs ORDER BY input_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_ne!(
        hashes[0], hashes[1],
        "SQL NULL was conflated with stored JSON null"
    );
    let item = listener
        .search_quarantine(0, 10)
        .await
        .unwrap()
        .items
        .remove(0);
    assert_eq!(item.code, "missing-body");
    assert!(item.body.is_none());
    let correction = Correction {
        body: "null".into(),
        alias: Some("null-body".into()),
    };
    assert_eq!(
        listener
            .replay_quarantined(
                item.sequence,
                Some(&correction),
                "operator",
                "explicit JSON null body",
                Utc::now()
            )
            .await
            .unwrap(),
        ReplayOutcome::Accepted
    );
    support::insert(&pool, &config, 2, json!(null), None).await;
    assert_eq!(
        listener.poll_once(Utc::now()).await.unwrap().duplicates,
        1,
        "operator alias was treated as a nonexistent source column"
    );
    sqlx::query("INSERT INTO ext.items VALUES($1,NULL,clock_timestamp())")
        .bind(Uuid::from_u128(2))
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().duplicates, 1);
    assert_eq!(support::count(&pool, "raddle_jobs").await, 2);
    assert_eq!(support::count(&pool, "ext.items").await, 0);
}
