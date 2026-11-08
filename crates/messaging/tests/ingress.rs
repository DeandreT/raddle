//! Pre-custody ordering and canonical identity over real PostgreSQL inbox storage.
#![allow(clippy::unwrap_used)]

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::wire::{Envelope, WireError, WireMessage};
use raddle_messaging::{
    BoxFuture, BusBuilder, BusError, IngressDecoder, IngressError, TrustedOrigin,
};
use raddle_postgres::{Error, Store};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Serialize, Deserialize)]
struct Input(u32);
impl WireMessage for Input {
    const NAME: &'static str = "test.canonical-ingress";
    const VERSION: u32 = 1;
}

// A boundary fixture, not a cryptographic implementation or crypto qualification:
// different transport frames map to one canonical message.
struct CanonicalDecoder(Envelope);
impl IngressDecoder for CanonicalDecoder {
    type Error = WireError;
    fn decode<'a>(
        &'a self,
        bytes: &'a [u8],
        origin: &'a TrustedOrigin,
    ) -> BoxFuture<'a, Result<Envelope, WireError>> {
        Box::pin(async move {
            assert_eq!(origin.tenant(), "authenticated-tenant");
            match bytes {
                b"first-wire-frame" | b"new-nonce-or-key-frame" => Ok(self.0.clone()),
                _ => Err(WireError::Invalid("rejected pre-custody frame")),
            }
        })
    }
}
fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("authenticated-tenant", "authenticated-ingress", None).unwrap()
}

#[sqlx::test(migrations = "../raddle-postgres/migrations")]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn decoder_runs_before_canonical_identity_comparison_and_concurrent_retry_admission(
    pool: PgPool,
) {
    let envelope = Envelope::encode(&Input(1))
        .unwrap()
        .with_header("tenant".into(), "untrusted-tenant".into())
        .unwrap();
    let changed = Envelope::with_identity(envelope.id(), envelope.created_at(), &Input(2))
        .unwrap()
        .with_header("tenant".into(), "untrusted-tenant".into())
        .unwrap();
    let decoder = CanonicalDecoder(envelope.clone());
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.durable("inbox", Store::new(pool.clone())).unwrap();
    builder.route::<Input>("inbox").unwrap();
    let bus = builder.build();
    assert!(matches!(
        bus.receive(&decoder, b"reject", origin(), Utc::now(), true)
            .await,
        Err(IngressError::Decode(_))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    let (first, retry) = tokio::join!(
        bus.receive(&decoder, b"first-wire-frame", origin(), Utc::now(), true),
        bus.receive(
            &decoder,
            b"new-nonce-or-key-frame",
            origin(),
            Utc::now(),
            true
        ),
    );
    first.unwrap();
    retry.unwrap();
    // A later arrival has a different receipt time, not different message content.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    bus.receive(
        &decoder,
        b"new-nonce-or-key-frame",
        origin(),
        Utc::now(),
        true,
    )
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let (source, tenant, stored): (String, String, Vec<u8>) =
        sqlx::query_as("SELECT source,tenant,envelope FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(source, "authenticated-ingress");
    assert_eq!(tenant, "authenticated-tenant");
    let stored = Envelope::from_slice(&stored, Default::default()).unwrap();
    assert_eq!(stored.id(), envelope.id());
    assert_eq!(stored.decode::<Input>().unwrap().0, 1);
    assert!(matches!(
        bus.receive(
            &CanonicalDecoder(changed),
            b"first-wire-frame",
            origin(),
            Utc::now(),
            true
        )
        .await,
        Err(IngressError::Admission(BusError::Storage(
            Error::IdentityConflict
        )))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raddle_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}
