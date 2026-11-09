//! Encryption profiles explicitly refuse unsupported native dispatch metadata.
#![allow(clippy::unwrap_used)]
use raddle::grouping::Dispatch;
use raddle::wire::{Envelope, WireMessage};
use raddle_crypto::{EncryptionCodec, EncryptionPolicy, RotatingKeys};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Work;
impl WireMessage for Work {
    const NAME: &'static str = "group.work";
    const VERSION: u32 = 1;
}

#[tokio::test]
async fn dispatch_crypto_profile_refuses_even_when_encryption_selection_is_none() {
    let codec = EncryptionCodec::new(
        RotatingKeys::new(1).unwrap(),
        EncryptionPolicy::default(),
        Default::default(),
    );
    let envelope = Envelope::encode(&Work)
        .unwrap()
        .with_dispatch(Dispatch::resolved(None, None))
        .unwrap();
    assert!(codec.encode(&envelope).await.is_err());
    assert!(codec.decode(&envelope).await.is_err());
    assert!(
        codec
            .decode_bytes(&envelope.to_vec().unwrap())
            .await
            .is_err()
    );
}
