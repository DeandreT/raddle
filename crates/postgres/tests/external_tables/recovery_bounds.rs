use chrono::Utc;
use raddle_postgres::external_tables::{Correction, Options, ReplayOutcome};
use serde_json::json;
use sqlx::PgPool;

use super::support;

async fn evidence(pool: &PgPool, sequence: i64) -> (Vec<u8>, i64, i64) {
    sqlx::query_as("SELECT sha256(convert_to(jsonb_build_array(body,alias)::text,'UTF8')),coalesce(octet_length(convert_to(body,'UTF8')),0)::bigint,coalesce(octet_length(convert_to(alias,'UTF8')),0)::bigint FROM raddle_external_inputs WHERE sequence=$1")
        .bind(sequence).fetch_one(pool).await.unwrap()
}

async fn smaller_recovery_budget(pool: PgPool, shrink_body: bool) {
    let mut config = support::config();
    config.alias_column = Some("kind".into());
    let original_options = Options {
        body_bytes: 4096,
        alias_bytes: 1024,
        ..Default::default()
    };
    let original = support::listener(&pool, config.clone(), original_options.clone());
    original.provision_source().await.unwrap();
    support::effects(&pool).await;
    let body = if shrink_body {
        json!({"value":1,"padding":"x".repeat(2048)})
    } else {
        json!({"value":1})
    };
    let alias = if shrink_body {
        "unknown".into()
    } else {
        "x".repeat(512)
    };
    support::insert(&pool, &config, 1, body.clone(), Some(&alias)).await;
    support::insert(&pool, &config, 2, body, Some(&alias)).await;
    support::insert(&pool, &config, 3, json!({"value":3}), Some("order")).await;
    let report = original.poll_once(Utc::now()).await.unwrap();
    assert_eq!((report.quarantined, report.accepted), (2, 1));
    let old = original.search_quarantine(0, 10).await.unwrap().items;
    assert!(old.iter().all(|item| item.snapshot_available));
    let sequences = [old[0].sequence, old[1].sequence];
    let before = [
        evidence(&pool, sequences[0]).await,
        evidence(&pool, sequences[1]).await,
    ];
    support::worker(&pool).run_once(10).await.unwrap();
    let options = if shrink_body {
        Options {
            body_bytes: 64,
            ..original_options
        }
    } else {
        Options {
            alias_bytes: 16,
            ..original_options
        }
    };
    let current = support::listener(&pool, config.clone(), options.clone());
    let page = current.search_quarantine(0, 10).await.unwrap();
    assert_eq!(page.items.len(), 2);
    for item in page.items {
        assert!(!item.snapshot_available);
        assert!(
            !item.source_retained,
            "test must cover an older complete ledger snapshot"
        );
        assert!(
            item.body
                .as_ref()
                .is_none_or(|v| v.len() <= options.body_bytes as usize)
        );
        assert!(
            item.alias
                .as_ref()
                .is_none_or(|v| v.len() <= options.alias_bytes as usize)
        );
        if shrink_body {
            assert!(item.body.is_none());
            assert!(item.body_bytes.unwrap() > i64::from(options.body_bytes));
        } else {
            assert!(item.alias.is_none());
            assert!(item.alias_bytes.unwrap() > i64::from(options.alias_bytes));
        }
    }
    assert_eq!(
        current
            .replay_quarantined(
                sequences[0],
                None,
                "operator",
                "original outside current budget",
                Utc::now()
            )
            .await
            .unwrap(),
        ReplayOutcome::Unavailable
    );
    let state: (String, Option<chrono::DateTime<Utc>>) =
        sqlx::query_as("SELECT status,replayed_at FROM raddle_external_inputs WHERE sequence=$1")
            .bind(sequences[0])
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, ("quarantined".into(), None));
    let correction = Correction {
        body: "{\"value\":42}".into(),
        alias: Some("order".into()),
    };
    assert_eq!(
        current
            .replay_quarantined(
                sequences[1],
                Some(&correction),
                "operator",
                "small correction",
                Utc::now()
            )
            .await
            .unwrap(),
        ReplayOutcome::Accepted
    );
    assert_eq!(before[0], evidence(&pool, sequences[0]).await);
    assert_eq!(before[1], evidence(&pool, sequences[1]).await);
    support::healthy(&pool, &current, &config, 4).await;
    assert_eq!(
        current
            .replay_quarantined(
                sequences[0],
                Some(&correction),
                "operator",
                "correct remaining failure",
                Utc::now()
            )
            .await
            .unwrap(),
        ReplayOutcome::Accepted
    );
    assert_eq!(
        current
            .replay_quarantined(
                sequences[1],
                Some(&correction),
                "operator",
                "repeat",
                Utc::now()
            )
            .await
            .unwrap(),
        ReplayOutcome::AlreadyResolved
    );
    support::worker(&pool).run_once(10).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 4);
    assert_eq!(before[0], evidence(&pool, sequences[0]).await);
    assert_eq!(before[1], evidence(&pool, sequences[1]).await);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_recovery_body_budget_applies_to_older_snapshots(pool: PgPool) {
    smaller_recovery_budget(pool, true).await;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_recovery_alias_budget_applies_to_older_snapshots(pool: PgPool) {
    smaller_recovery_budget(pool, false).await;
}
