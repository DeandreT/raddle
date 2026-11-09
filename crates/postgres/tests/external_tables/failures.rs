use chrono::Utc;
use raddle_postgres::{
    Store,
    external_tables::{Correction, Options, ReplayOutcome},
};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use super::support;

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_full_quarantine_and_fair_cursor_preserve_new_work(pool: PgPool) {
    let c = support::config();
    let listener = support::setup(
        &pool,
        Options {
            batch_size: 1,
            quarantine_rows: 1,
            ..Default::default()
        },
    )
    .await;
    support::insert(&pool, &c, 1, json!({"value":"bad"}), None).await;
    support::insert(&pool, &c, 2, json!({"value":"bad"}), None).await;
    support::insert(&pool, &c, 3, json!({"value":3}), None).await;
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().quarantined, 1);
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().deferred, 1);
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().accepted, 1);
    support::worker(&pool).run_once(10).await.unwrap();
    support::healthy(&pool, &listener, &c, 4).await;
    assert_eq!(support::count(&pool, "ext.items").await, 1);
    assert_eq!(
        listener.search_quarantine(0, 10).await.unwrap().items.len(),
        1
    );
    // Refusal did not silently replace the initial durable observation time.
    let before: chrono::DateTime<Utc> =
        sqlx::query_scalar("SELECT first_seen FROM raddle_external_inputs WHERE input_id=$1")
            .bind(Uuid::from_u128(2))
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query("UPDATE ext.items SET json='{\"value\":2}' WHERE id=$1")
        .bind(Uuid::from_u128(2))
        .execute(&pool)
        .await
        .unwrap();
    for _ in 0..3 {
        listener.poll_once(Utc::now()).await.unwrap();
    }
    let after: chrono::DateTime<Utc> = sqlx::query_scalar(
        "SELECT first_seen FROM raddle_external_inputs WHERE input_id=$1 AND status='accepted'",
    )
    .bind(Uuid::from_u128(2))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(before, after);
    support::worker(&pool).run_once(10).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 3);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_byte_budget_and_admission_refusal_retain_sources(pool: PgPool) {
    let listener = support::setup(
        &pool,
        Options {
            quarantine_bytes: 0,
            ..Default::default()
        },
    )
    .await;
    support::insert(&pool, &support::config(), 1, json!({"value":"bad"}), None).await;
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().deferred, 1);
    assert_eq!(support::count(&pool, "ext.items").await, 1);
    assert!(
        listener
            .search_quarantine(0, 10)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    sqlx::query("UPDATE ext.items SET json='{\"value\":1}'")
        .execute(&pool)
        .await
        .unwrap();
    let store = Store::new(pool.clone());
    store.set_capacity("tenant", 1).await.unwrap();
    listener.poll_once(Utc::now()).await.unwrap();
    for id in 2..=3 {
        support::insert(&pool, &support::config(), id, json!({"value":id}), None).await;
    }
    assert_eq!(listener.poll_once(Utc::now()).await.unwrap().deferred, 2);
    assert_eq!(support::count(&pool, "ext.items").await, 2);
    let first_seen: chrono::DateTime<Utc> =
        sqlx::query_scalar("SELECT first_seen FROM raddle_external_inputs WHERE input_id=$1")
            .bind(Uuid::from_u128(2))
            .fetch_one(&pool)
            .await
            .unwrap();
    let worker = support::worker(&pool);
    for _ in 0..4 {
        worker.run_once(10).await.unwrap();
        listener.poll_once(Utc::now()).await.unwrap();
    }
    worker.run_once(10).await.unwrap();
    let bytes: Vec<u8> = sqlx::query_scalar("SELECT envelope FROM raddle_jobs WHERE message_id=$1")
        .bind(Uuid::from_u128(2))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        raddle::wire::Envelope::from_slice(&bytes, Default::default())
            .unwrap()
            .created_at(),
        first_seen
    );
    assert_eq!(support::count(&pool, "external_effects").await, 3);
    assert_eq!(support::count(&pool, "ext.items").await, 0);
}

async fn fault_scenario(pool: PgPool, kind: &str) {
    let listener = support::setup(&pool, Default::default()).await;
    sqlx::query("CREATE TABLE external_faults(id uuid PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    for id in [1, 5, 9] {
        sqlx::query("INSERT INTO external_faults VALUES($1)")
            .bind(Uuid::from_u128(id))
            .execute(&pool)
            .await
            .unwrap();
    }
    match kind {
        "delete" => {
            sqlx::raw_sql("CREATE FUNCTION external_delete_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS(SELECT 1 FROM external_faults WHERE id=OLD.id) THEN RETURN NULL; END IF; RETURN OLD; END $$; CREATE TRIGGER external_fault BEFORE DELETE ON ext.items FOR EACH ROW EXECUTE FUNCTION external_delete_fault()")
                .execute(&pool).await.unwrap();
        }
        _ => {
            let status = if kind == "quarantine" {
                "quarantined"
            } else {
                "accepted"
            };
            sqlx::raw_sql(&format!("CREATE FUNCTION external_ledger_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='{status}' AND EXISTS(SELECT 1 FROM external_faults WHERE id=NEW.input_id) THEN RAISE EXCEPTION 'external native injected ledger fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER external_fault BEFORE UPDATE ON raddle_external_inputs FOR EACH ROW EXECUTE FUNCTION external_ledger_fault()"))
                .execute(&pool).await.unwrap();
        }
    }
    for id in 1..=9 {
        let body = if kind == "quarantine" && [1, 5, 9].contains(&id) {
            json!({"value":"bad"})
        } else {
            json!({"value":id})
        };
        support::insert(&pool, &support::config(), id, body, None).await;
    }
    let report = listener.poll_once(Utc::now()).await.unwrap();
    assert_eq!((report.accepted, report.deferred), (6, 3));
    assert_eq!(support::count(&pool, "ext.items").await, 3);
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raddle_external_inputs WHERE input_id=ANY($1) AND status='observed'",
    )
    .bind(vec![
        Uuid::from_u128(1),
        Uuid::from_u128(5),
        Uuid::from_u128(9),
    ])
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pending, 3);
    support::worker(&pool).run_once(100).await.unwrap();
    support::healthy(&pool, &listener, &support::config(), 20).await;
    sqlx::query("DELETE FROM external_faults")
        .execute(&pool)
        .await
        .unwrap();
    listener.poll_once(Utc::now()).await.unwrap();
    if kind == "quarantine" {
        for item in listener.search_quarantine(0, 10).await.unwrap().items {
            assert_eq!(
                listener
                    .replay_quarantined(
                        item.sequence,
                        Some(&Correction {
                            body: "{\"value\":42}".into(),
                            alias: None
                        }),
                        "operator",
                        "repair failed row",
                        Utc::now()
                    )
                    .await
                    .unwrap(),
                ReplayOutcome::Accepted
            );
        }
    }
    support::worker(&pool).run_once(100).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 10);
    assert_eq!(support::count(&pool, "ext.items").await, 0);
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_zero_row_delete_rolls_back_inbox_and_receipt_first_middle_last(
    pool: PgPool,
) {
    fault_scenario(pool, "delete").await;
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_receipt_fault_rolls_back_inbox_first_middle_last(pool: PgPool) {
    fault_scenario(pool, "receipt").await;
}
#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_quarantine_fault_retains_first_middle_last_and_healthy_progress(
    pool: PgPool,
) {
    fault_scenario(pool, "quarantine").await;
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn external_native_raw_replay_validation_capacity_audit_fault_and_concurrency(pool: PgPool) {
    let listener = support::setup(&pool, Default::default()).await;
    support::insert(&pool, &support::config(), 1, json!({"value":"bad"}), None).await;
    listener.poll_once(Utc::now()).await.unwrap();
    let id = listener.search_quarantine(0, 10).await.unwrap().items[0].sequence;
    let bad = Correction {
        body: "{\"value\":\"bad\"}".into(),
        alias: None,
    };
    assert!(
        listener
            .replay_quarantined(id, Some(&bad), "op", "invalid correction", Utc::now())
            .await
            .is_err()
    );
    let correction = Correction {
        body: "{\"value\":1}".into(),
        alias: None,
    };
    sqlx::raw_sql("CREATE FUNCTION external_audit_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='replayed' THEN RAISE EXCEPTION 'external native injected audit fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER external_audit_fault BEFORE UPDATE ON raddle_external_inputs FOR EACH ROW EXECUTE FUNCTION external_audit_fault()")
        .execute(&pool).await.unwrap();
    assert!(
        listener
            .replay_quarantined(id, Some(&correction), "op", "audit unavailable", Utc::now())
            .await
            .is_err()
    );
    assert_eq!(support::count(&pool, "raddle_jobs").await, 0);
    support::healthy(&pool, &listener, &support::config(), 20).await;
    sqlx::query("DROP TRIGGER external_audit_fault ON raddle_external_inputs")
        .execute(&pool)
        .await
        .unwrap();
    let store = Store::new(pool.clone());
    store.set_capacity("tenant", 1).await.unwrap();
    support::insert(&pool, &support::config(), 21, json!({"value":21}), None).await;
    listener.poll_once(Utc::now()).await.unwrap();
    assert!(
        listener
            .replay_quarantined(id, Some(&correction), "op", "capacity full", Utc::now())
            .await
            .is_err()
    );
    assert_eq!(
        listener.search_quarantine(0, 10).await.unwrap().items.len(),
        1
    );
    support::worker(&pool).run_once(100).await.unwrap();
    let (a, b) = tokio::join!(
        listener.replay_quarantined(id, Some(&correction), "op", "correct", Utc::now()),
        listener.replay_quarantined(id, Some(&correction), "op", "concurrent repeat", Utc::now())
    );
    let mut outcomes = vec![a.unwrap(), b.unwrap()];
    outcomes.sort_by_key(|o| matches!(o, ReplayOutcome::AlreadyResolved));
    assert_eq!(
        outcomes,
        [ReplayOutcome::Accepted, ReplayOutcome::AlreadyResolved]
    );
    support::worker(&pool).run_once(100).await.unwrap();
    assert_eq!(support::count(&pool, "external_effects").await, 3);
    let audits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raddle_external_inputs WHERE replayed_at IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(audits, 1);
}
