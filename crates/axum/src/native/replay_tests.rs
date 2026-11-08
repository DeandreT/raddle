#![allow(clippy::unwrap_used)]

use super::begin_claimed_work;
use chrono::Utc;
use raddle_postgres::http_responses::{self, ClaimResult, ResponseKey};
use raddle_postgres::{Error, Store};
use sqlx::PgPool;

#[sqlx::test(migrations = "../postgres/migrations")]
async fn failed_business_open_releases_only_the_confirmed_prebusiness_claim(pool: PgPool) {
    let store = Store::new(pool.clone());
    let key = ResponseKey::new(None, None, Some("opening"), "retry").unwrap();
    let mut scope = store.begin().await.unwrap();
    let claim = match http_responses::claim(&mut scope, &key, &[0; 32], Utc::now())
        .await
        .unwrap()
    {
        ClaimResult::Claimed(claim) => claim,
        other => panic!("unexpected {other:?}"),
    };
    scope.commit(Utc::now()).await.unwrap();
    // The opening future is the exact boundary used after claim COMMIT. A pool
    // acquisition failure must compensate before any handler/scope can run.
    let failure = std::future::ready(Err(Error::Database(sqlx::Error::PoolTimedOut)));
    let result = begin_claimed_work(&store, &claim, failure).await;
    assert!(result.is_err());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM raddle_http_responses")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let mut scope = store.begin().await.unwrap();
    assert!(matches!(
        http_responses::claim(&mut scope, &key, &[0; 32], Utc::now())
            .await
            .unwrap(),
        ClaimResult::Claimed(_)
    ));
    scope.commit(Utc::now()).await.unwrap();
    assert!(
        !store.release_http_response(&claim).await.unwrap(),
        "old compensation cannot release the retry owner"
    );
}
