use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use chrono::Utc;
use raddle::wire::Codecs;
use raddle_postgres::external_tables::{Correction, Options, ReplayOutcome};
use raddle_postgres::{DeadLetterQuery, HandlerFailure, RetryPolicy, Store, Worker};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use super::support::{self, Order};

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_c03_default_and_mixed_custom_aliases_execute_and_drain(pool: PgPool) {
    let mut c = support::config();
    c.id_column = "pk".into();
    c.body_column = "message_body".into();
    c.timestamp_column = "added".into();
    c.alias_column = Some("message_kind".into());
    let listener = support::listener(&pool, c.clone(), Default::default());
    listener.provision_source().await.unwrap();
    support::effects(&pool).await;
    support::insert(&pool, &c, 1, json!({"value":7}), None).await;
    support::insert(&pool, &c, 2, json!({"text":"other"}), Some("other")).await;
    support::insert(
        &pool,
        &c,
        3,
        json!({"value":9,"tenant":"forged","destination":"forged","headers":{"reply-to":"evil"}}),
        Some("order"),
    )
    .await;
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().accepted, 3);
    assert_eq!(support::count(&pool, &support::table(&c)).await, 0);
    assert_eq!(support::worker(&pool).run_once(100).await.unwrap(), 3);
    assert_eq!(support::count(&pool, "external_effects").await, 3);
    let authority: Vec<(String, String, String)> =
        sqlx::query_as("SELECT tenant,source,destination FROM raddle_jobs")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(authority.iter().all(|v| v
        == &(
            "tenant".into(),
            "external-native".into(),
            "external-inbox".into()
        )));
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_c04_handler_dead_letter_and_existing_audited_replay(pool: PgPool) {
    let listener = support::setup(&pool, Default::default()).await;
    for id in 1..=9 {
        support::insert(&pool, &support::config(), id, json!({"value":id}), None).await;
    }
    listener.poll_once(Utc::now()).await.unwrap();
    let failing = Arc::new(AtomicBool::new(true));
    let flag = Arc::clone(&failing);
    let mut worker = Worker::new(Store::new(pool.clone()), RetryPolicy::default());
    worker
        .handle::<Order, _>("external-inbox", Codecs::default(), move |m, scope| {
            let flag = Arc::clone(&flag);
            Box::pin(async move {
                if flag.load(Ordering::SeqCst) && [1, 5, 9].contains(&m.value) {
                    return Err(HandlerFailure::Reject);
                }
                sqlx::query("INSERT INTO external_effects VALUES($1,$2)")
                    .bind(scope.envelope().unwrap().unwrap().id())
                    .bind(m.value.to_string())
                    .execute(scope.connection())
                    .await
                    .unwrap();
                Ok(())
            })
        })
        .unwrap();
    worker.run_once(100).await.unwrap();
    let store = Store::new(pool.clone());
    let page = store
        .search_dead_letters(
            "tenant",
            &DeadLetterQuery {
                limit: 10,
                after: 0,
                destination: None,
                contract: None,
                error_code: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 3);
    assert_eq!(support::count(&pool, "external_effects").await, 6);
    assert_eq!(support::count(&pool, "ext.items").await, 0);
    support::insert(&pool, &support::config(), 20, json!({"value":20}), None).await;
    listener.poll_once(Utc::now()).await.unwrap();
    worker.run_once(100).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 7);
    failing.store(false, Ordering::SeqCst);
    for item in page.items {
        assert!(
            store
                .replay_dead_letter_bounded(
                    "tenant",
                    item.id,
                    "operator",
                    "handler corrected",
                    Utc::now()
                )
                .await
                .unwrap()
        );
    }
    worker.run_once(100).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 10);
    assert_eq!(support::count(&pool, "raddle_replay_audit").await, 3);
    let unhealthy: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raddle_jobs WHERE status<>'completed' OR attempts<>1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        unhealthy, 0,
        "replay changed a successful sibling's execution state"
    );
    assert!(
        listener
            .search_quarantine(0, 10)
            .await
            .unwrap()
            .items
            .is_empty()
    );
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_raw_first_middle_last_corrected_failed_only_and_wire_guard(pool: PgPool) {
    let mut c = support::config();
    c.alias_column = Some("kind".into());
    let options = Options {
        body_bytes: 128,
        alias_bytes: 32,
        ..Default::default()
    };
    let listener = support::listener(&pool, c.clone(), options);
    listener.provision_source().await.unwrap();
    support::effects(&pool).await;
    for position in 0..3 {
        for index in 0..3 {
            let id = 1 + position * 3 + index;
            let (body, alias) = if index == position {
                match position {
                    0 => (json!({"value":1}), "unknown"),
                    1 => (json!({"value":"not an integer"}), "order"),
                    _ => (json!({"format":"raddle-aes-256-gcm-v1","value":0}), "order"),
                }
            } else {
                (json!({"value":id}), "order")
            };
            support::insert(&pool, &c, id, body, Some(alias)).await;
        }
    }
    let report = listener.poll_once(Utc::now()).await.unwrap();
    assert_eq!((report.accepted, report.quarantined), (6, 3));
    support::worker(&pool).run_once(100).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 6);
    support::healthy(&pool, &listener, &c, 20).await;
    let page = listener.search_quarantine(0, 10).await.unwrap();
    assert_eq!(page.items.len(), 3);
    assert!(
        page.items
            .iter()
            .any(|i| i.code == "unsupported-wire-framing")
    );
    for item in page.items {
        assert!(!item.source_retained);
        assert!(item.body.is_some());
        let correction = Correction {
            body: "{\"value\":42}".into(),
            alias: Some("order".into()),
        };
        assert_eq!(
            listener
                .replay_quarantined(
                    item.sequence,
                    Some(&correction),
                    "operator",
                    "correct failed input",
                    Utc::now()
                )
                .await
                .unwrap(),
            ReplayOutcome::Accepted
        );
        assert_eq!(
            listener
                .replay_quarantined(
                    item.sequence,
                    Some(&correction),
                    "operator",
                    "repeat",
                    Utc::now()
                )
                .await
                .unwrap(),
            ReplayOutcome::AlreadyResolved
        );
    }
    support::worker(&pool).run_once(100).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 10);
    // The producer retries the same original unknown-alias input after correction.
    support::insert(&pool, &c, 1, json!({"value":1}), Some("unknown")).await;
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().duplicates, 1);
    assert_eq!(support::count(&pool, "ext.items").await, 0);
    assert_eq!(support::worker(&pool).run_once(100).await.unwrap(), 0);
    let audits:i64=sqlx::query_scalar("SELECT count(*) FROM raddle_external_inputs WHERE replay_actor='operator' AND replay_hash IS NOT NULL AND replayed_at IS NOT NULL")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(audits, 3);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_oversized_body_alias_references_and_conflict_checks(pool: PgPool) {
    let mut c = support::config();
    c.alias_column = Some("kind".into());
    let listener = support::listener(
        &pool,
        c.clone(),
        Options {
            body_bytes: 64,
            alias_bytes: 16,
            ..Default::default()
        },
    );
    listener.provision_source().await.unwrap();
    support::effects(&pool).await;
    for id in 1..=6 {
        let body = if [1, 3, 6].contains(&id) {
            json!({"value":1,"padding":"x".repeat(200_000)})
        } else {
            json!({"value":id})
        };
        support::insert(&pool, &c, id, body, Some("order")).await;
    }
    support::insert(&pool, &c, 7, json!({"value":7}), Some(&"a".repeat(200_000))).await;
    let report = listener.poll_once(Utc::now()).await.unwrap();
    assert_eq!((report.accepted, report.quarantined), (3, 4));
    assert_eq!(support::count(&pool, "ext.items").await, 4);
    let page = listener.search_quarantine(0, 10).await.unwrap();
    assert!(page.items.iter().all(|i| i.source_retained
        && i.body.is_none()
        && i.alias.is_none()
        && i.fingerprint.len() == 32));
    support::worker(&pool).run_once(100).await.unwrap();
    support::healthy(&pool, &listener, &c, 20).await;
    let correction = Correction {
        body: "{\"value\":9}".into(),
        alias: Some("order".into()),
    };
    let first = &page.items[0];
    sqlx::query("UPDATE ext.items SET json='{\"value\":99}' WHERE id=$1")
        .bind(first.input_id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        listener
            .replay_quarantined(
                first.sequence,
                Some(&correction),
                "operator",
                "changed reference",
                Utc::now()
            )
            .await
            .is_err()
    );
    let second = &page.items[1];
    sqlx::query("DELETE FROM ext.items WHERE id=$1")
        .bind(second.input_id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        listener
            .replay_quarantined(
                second.sequence,
                Some(&correction),
                "operator",
                "missing reference",
                Utc::now()
            )
            .await
            .is_err()
    );
    let third = &page.items[2];
    assert_eq!(
        listener
            .replay_quarantined(
                third.sequence,
                Some(&correction),
                "operator",
                "explicit bounded correction",
                Utc::now()
            )
            .await
            .unwrap(),
        ReplayOutcome::Accepted
    );
    let original: i64 =
        sqlx::query_scalar("SELECT octet_length(json::text)::bigint FROM ext.items WHERE id=$1")
            .bind(third.input_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(original > 64, "oversized original was damaged");
    support::worker(&pool).run_once(100).await.unwrap();
    for _ in 0..3 {
        listener.poll_once(Utc::now()).await.unwrap();
    }
    assert_eq!(support::count(&pool, "external_effects").await, 5);
    let accepted: Vec<Uuid> =
        sqlx::query_scalar("SELECT message_id FROM raddle_jobs WHERE tenant='tenant'")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(accepted.len(), 5);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_raw_alias_registration_replay_and_tenant_boundary(pool: PgPool) {
    use raddle_postgres::external_tables::{Aliases, Listener};
    let mut c = support::config();
    c.alias_column = Some("kind".into());
    c.default_alias = None;
    let listener = support::listener(&pool, c.clone(), Default::default());
    listener.provision_source().await.unwrap();
    support::effects(&pool).await;
    support::insert(&pool, &c, 1, json!({"value":7}), Some("future")).await;
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().quarantined, 1);
    let sequence = listener.search_quarantine(0, 10).await.unwrap().items[0].sequence;
    let mut other_config = c.clone();
    other_config.tenant = "other-tenant".into();
    let other = support::listener(&pool, other_config, Default::default());
    assert!(
        other
            .search_quarantine(0, 10)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert_eq!(
        other
            .replay_quarantined(sequence, None, "op", "wrong tenant", Utc::now())
            .await
            .unwrap(),
        ReplayOutcome::NotFound
    );
    let mut aliases = Aliases::default();
    aliases.register::<Order>("future").unwrap();
    let updated = Listener::new(Store::new(pool.clone()), c, aliases, Default::default()).unwrap();
    assert_eq!(
        updated
            .replay_quarantined(sequence, None, "op", "register explicit alias", Utc::now())
            .await
            .unwrap(),
        ReplayOutcome::Accepted
    );
    support::worker(&pool).run_once(10).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 1);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_known_canonical_and_selected_wire_roots_are_not_application_bodies(
    pool: PgPool,
) {
    let listener = support::setup(&pool, Default::default()).await;
    let canonical = raddle::wire::Envelope::encode(&Order { value: 8 }).unwrap();
    let root: serde_json::Value = serde_json::from_slice(&canonical.to_vec().unwrap()).unwrap();
    support::insert(&pool, &support::config(), 1, root, None).await;
    support::insert(&pool, &support::config(), 2, json!("RDSB1selected"), None).await;
    support::insert(&pool, &support::config(), 3, json!({"value":3}), None).await;
    support::insert(
        &pool,
        &support::config(),
        4,
        json!({
            "id":"invalid", "contract":"external.order", "version":0, "created_at":"invalid",
            "payload":{"format":"raddle-aes-256-gcm-v1"}
        }),
        None,
    )
    .await;
    let report = listener.poll_once(Utc::now()).await.unwrap();
    assert_eq!((report.quarantined, report.accepted), (3, 1));
    assert!(
        listener
            .search_quarantine(0, 10)
            .await
            .unwrap()
            .items
            .iter()
            .all(|i| i.code == "unsupported-wire-framing")
    );
    support::worker(&pool).run_once(10).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 1);
}
