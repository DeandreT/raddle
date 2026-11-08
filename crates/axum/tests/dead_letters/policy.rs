//! HTTP management cannot turn preserved policy uncertainty into execution authority.

use std::time::Duration;

use chrono::Utc;
use raddle::wire::Envelope;
use raddle_axum::dead_letters::DeadLetterAction;
use raddle_postgres::endpoint_policies::PolicyRecovery;
use raddle_postgres::{JobStatus, Store};
use sqlx::PgPool;
use tokio::task::JoinSet;

use super::{Failed, api, checked, client, mutation, serve, stage};

#[sqlx::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn socket_replay_preserves_uncertainty_and_discard_never_reopens_old_recovery(pool: PgPool) {
    let store = Store::new(pool.clone());
    store.migrate().await.unwrap();
    let (held, _, _) = stage(&store, "tenant", "policy-held", "held work").await;
    let (retired, _, mut successor_delivery) =
        stage(&store, "tenant", "policy-retired", "retired work").await;
    let (healthy, _, _) = stage(&store, "tenant", "policy-healthy", "healthy work").await;
    // Persisted conservative metadata is independent from the dead-letter state.
    // The HTTP action cannot claim to have reconciled the historical attempt.
    for id in [held, retired] {
        sqlx::query(
            "INSERT INTO raddle_policy_attempts(job_id,handling,uncertain) VALUES($1,1,true)",
        )
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    }
    let api = api(store.clone(), None);
    let mut tasks = JoinSet::new();
    let base = serve(&api, &mut tasks).await;
    let client = client();
    let response = client
        .post(format!("{base}/dead-letters/replay"))
        .bearer_auth("replayer")
        .json(&mutation(
            &[held.to_string(), healthy.to_string()],
            DeadLetterAction::Replay,
        ))
        .send()
        .await
        .unwrap();
    let result = checked(&api, "/dead-letters/replay", "post", 200, response).await;
    assert_eq!(result["results"][0]["outcome"], "replayed");
    assert_eq!(result["results"][1]["outcome"], "replayed");
    assert!(
        store
            .claim_policy_for("policy-held", 8, Duration::from_secs(30), Utc::now())
            .await
            .unwrap()
            .is_empty()
    );
    let healthy_claims = store
        .claim_policy_for("policy-healthy", 8, Duration::from_secs(30), Utc::now())
        .await
        .unwrap();
    assert_eq!(healthy_claims.len(), 1);
    assert_eq!(healthy_claims[0].id(), healthy);
    store
        .complete(&healthy_claims[0], Utc::now())
        .await
        .unwrap();
    let response = client
        .delete(format!("{base}/dead-letters/"))
        .bearer_auth("discarder")
        .json(&mutation(&[retired.to_string()], DeadLetterAction::Discard))
        .send()
        .await
        .unwrap();
    assert_eq!(
        checked(&api, "/dead-letters/", "delete", 200, response).await["results"][0]["outcome"],
        "discarded"
    );
    let later = Utc::now() + chrono::Duration::hours(1);
    for id in [held, retired] {
        assert!(
            !store
                .recover_policy_uncertain(
                    "tenant",
                    id,
                    PolicyRecovery::RolledBack,
                    "operator",
                    "old recovery request",
                    later,
                )
                .await
                .unwrap()
        );
        let marker: bool =
            sqlx::query_scalar("SELECT uncertain FROM raddle_policy_attempts WHERE job_id=$1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(marker);
    }
    let response = client
        .post(format!("{base}/dead-letters/replay"))
        .bearer_auth("replayer")
        .json(&mutation(&[retired.to_string()], DeadLetterAction::Replay))
        .send()
        .await
        .unwrap();
    assert_eq!(
        checked(&api, "/dead-letters/replay", "post", 200, response).await["results"][0]["outcome"],
        "not_eligible"
    );
    successor_delivery.available_at = Utc::now();
    let mut scope = store.begin().await.unwrap();
    let successor = scope
        .stage(
            "dlq-tests",
            "tenant",
            &Envelope::encode(&Failed {
                private: "new ordered arrival".into(),
            })
            .unwrap(),
            &[successor_delivery],
        )
        .await
        .unwrap()[0];
    scope.commit(Utc::now()).await.unwrap();
    let claims = store
        .claim_policy_for("policy-retired", 8, Duration::from_secs(30), Utc::now())
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].id(), successor);
    store.complete(&claims[0], Utc::now()).await.unwrap();
    assert_eq!(
        store
            .inspect("tenant", retired)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Discarded
    );
    assert_eq!(
        store
            .inspect("tenant", healthy)
            .await
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Completed
    );
    let audit_counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM raddle_discard_audit WHERE job_id=$1),\
                (SELECT count(*) FROM raddle_replay_audit WHERE job_id=$1)",
    )
    .bind(retired)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(audit_counts, (1, 0));
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}
