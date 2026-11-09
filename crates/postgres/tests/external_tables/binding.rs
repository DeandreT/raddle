use chrono::Utc;
use raddle_postgres::external_tables::{Correction, Error, Options, ReplayOutcome};
use serde_json::json;
use sqlx::PgPool;

use super::support;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_raw_replay_refuses_rebound_snapshot_and_lookalike_reference(pool: PgPool) {
    let options = Options {
        body_bytes: 64,
        ..Default::default()
    };
    let listener = support::setup(&pool, options.clone()).await;
    let config = support::config();
    support::insert(&pool, &config, 1, json!({"value":"bad"}), None).await;
    support::insert(
        &pool,
        &config,
        2,
        json!({"value":2,"padding":"x".repeat(2000)}),
        None,
    )
    .await;
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().quarantined, 2);
    let page = listener.search_quarantine(0, 10).await.unwrap();
    assert!(!page.items[0].source_retained);
    assert!(page.items[1].source_retained);
    sqlx::raw_sql("CREATE TABLE ext.lookalike (LIKE ext.items INCLUDING ALL); INSERT INTO ext.lookalike SELECT * FROM ext.items;")
        .execute(&pool).await.unwrap();
    let mut rebound = config.clone();
    rebound.table = "lookalike".into();
    let wrong = support::listener(&pool, rebound, options.clone());
    let correction = Correction {
        body: "{\"value\":42}".into(),
        alias: None,
    };
    for item in &page.items {
        assert!(matches!(
            wrong
                .replay_quarantined(
                    item.sequence,
                    Some(&correction),
                    "operator",
                    "wrong physical source",
                    Utc::now()
                )
                .await,
            Err(Error::Schema)
        ));
    }
    let mut wrong_columns = config.clone();
    wrong_columns.timestamp_column = "other_time".into();
    let wrong_columns = support::listener(&pool, wrong_columns, options);
    assert!(matches!(
        wrong_columns
            .replay_quarantined(
                page.items[0].sequence,
                Some(&correction),
                "operator",
                "changed projection",
                Utc::now()
            )
            .await,
        Err(Error::Schema)
    ));
    assert_eq!(support::count(&pool, "raddle_jobs").await, 0);
    assert_eq!(
        listener.search_quarantine(0, 10).await.unwrap().items.len(),
        2
    );
    assert_eq!(support::count(&pool, "ext.items").await, 1);
    for item in page.items {
        assert_eq!(
            listener
                .replay_quarantined(
                    item.sequence,
                    Some(&correction),
                    "operator",
                    "original source binding",
                    Utc::now()
                )
                .await
                .unwrap(),
            ReplayOutcome::Accepted
        );
    }
    support::worker(&pool).run_once(10).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 2);
    assert_eq!(support::count(&pool, "ext.items").await, 1);
    assert_eq!(support::count(&pool, "ext.lookalike").await, 1);
}
