//! Native authenticated-encryption, rotation and receive-policy contracts.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use raddle::wire::{Envelope, EnvelopeLimits, WireMessage};
use raddle_crypto::{
    CryptoError, EncryptionCodec, EncryptionPolicy, KeyId, KeyMaterial, KeyProvider,
    KeyProviderError, RotatingKeys, SecretKey, Selection,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Secret {
    text: String,
}
impl WireMessage for Secret {
    const NAME: &'static str = "secret";
    const VERSION: u32 = 1;
}

fn ring() -> RotatingKeys {
    let ring = RotatingKeys::new(2).unwrap();
    ring.insert(KeyId::new("one").unwrap(), SecretKey::new([17; 32]), true)
        .unwrap();
    ring
}
fn all() -> EncryptionPolicy {
    EncryptionPolicy {
        send: Selection::All,
        receive_required: Selection::All,
    }
}
fn envelope() -> Envelope {
    Envelope::encode(&Secret {
        text: "never print this application payload".into(),
    })
    .unwrap()
    .correlated(Uuid::new_v4(), Uuid::new_v4())
    .unwrap()
    .with_header("tenant".into(), "untrusted-original".into())
    .unwrap()
}
fn mutate(envelope: &Envelope, change: impl FnOnce(&mut Value)) -> Envelope {
    let mut value = serde_json::to_value(envelope).unwrap();
    change(&mut value);
    Envelope::from_slice(
        &serde_json::to_vec(&value).unwrap(),
        EnvelopeLimits::default(),
    )
    .unwrap()
}

#[tokio::test]
async fn roundtrip_preserves_all_envelope_fields_without_mutating_local_copy() {
    let codec = EncryptionCodec::new(ring(), all(), EnvelopeLimits::default());
    let local = envelope();
    let original = local.to_vec().unwrap();
    let encrypted = codec.encode(&local).await.unwrap();
    assert_ne!(original, encrypted.to_vec().unwrap());
    assert_eq!(original, local.to_vec().unwrap());
    let recovered = codec
        .decode_bytes(&encrypted.to_vec().unwrap())
        .await
        .unwrap();
    assert_eq!(original, recovered.to_vec().unwrap());
    let reformatted =
        serde_json::to_vec_pretty(&serde_json::to_value(&encrypted).unwrap()).unwrap();
    assert_eq!(
        codec
            .decode_bytes(&reformatted)
            .await
            .unwrap()
            .to_vec()
            .unwrap(),
        original
    );
    assert_eq!(
        local.decode::<Secret>().unwrap(),
        recovered.decode::<Secret>().unwrap()
    );
    assert_eq!(
        codec.encode(&encrypted).await.unwrap_err(),
        CryptoError::AlreadyEncrypted
    );
}

#[tokio::test]
async fn nonces_do_not_repeat_and_rotation_keeps_historical_keys() {
    let ring = ring();
    let codec = EncryptionCodec::new(ring.clone(), all(), EnvelopeLimits::default());
    let local = envelope();
    let mut nonces = BTreeSet::new();
    for _ in 0..2048 {
        let wire = serde_json::to_value(codec.encode(&local).await.unwrap()).unwrap();
        assert!(nonces.insert(wire["payload"]["nonce"].as_str().unwrap().to_owned()));
    }
    let old = codec.encode(&local).await.unwrap();
    ring.insert(
        KeyId::new("two").unwrap(),
        SecretKey::generate().unwrap(),
        true,
    )
    .unwrap();
    let new = codec.encode(&local).await.unwrap();
    assert_eq!(
        serde_json::to_value(&new).unwrap()["payload"]["key_id"],
        "two"
    );
    assert_eq!(
        codec.decode(&old).await.unwrap().to_vec().unwrap(),
        local.to_vec().unwrap()
    );
    assert_eq!(
        codec.decode(&new).await.unwrap().to_vec().unwrap(),
        local.to_vec().unwrap()
    );
    assert!(ring.retire(&KeyId::new("two").unwrap()).is_err());
    ring.retire(&KeyId::new("one").unwrap()).unwrap();
    assert_eq!(
        codec.decode(&old).await.unwrap_err(),
        CryptoError::KeyUnavailable
    );
}

#[tokio::test]
async fn authenticated_metadata_ciphertext_nonce_and_wrong_key_fail_before_handler() {
    let ring = ring();
    let codec = EncryptionCodec::new(ring.clone(), all(), EnvelopeLimits::default());
    ring.insert(
        KeyId::new("same-bytes-other-id").unwrap(),
        SecretKey::new([17; 32]),
        false,
    )
    .unwrap();
    let encrypted = codec.encode(&envelope()).await.unwrap();
    type Mutation = Box<dyn Fn(&mut Value)>;
    let mutations: Vec<Mutation> = vec![
        Box::new(|v| v["contract"] = json!("different")),
        Box::new(|v| v["version"] = json!(2)),
        Box::new(|v| v["payload"]["key_id"] = json!("same-bytes-other-id")),
        Box::new(|v| v["payload"]["inner_content_type"] = json!("application/xml")),
        Box::new(|v| {
            v["payload"]["ciphertext"] = json!("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
        }),
        Box::new(|v| v["payload"]["nonce"] = json!("AAAAAAAAAAAAAAAA")),
    ];
    let mut handler_calls = 0;
    for change in mutations {
        let tampered = mutate(&encrypted, change);
        if codec.decode(&tampered).await.is_ok() {
            handler_calls += 1;
        }
    }
    assert_eq!(handler_calls, 0);
    let wrong_ring = RotatingKeys::new(1).unwrap();
    wrong_ring
        .insert(KeyId::new("one").unwrap(), SecretKey::new([18; 32]), true)
        .unwrap();
    let wrong = EncryptionCodec::new(wrong_ring, all(), EnvelopeLimits::default());
    assert_eq!(
        wrong.decode(&encrypted).await.unwrap_err(),
        CryptoError::Authentication
    );
    let unknown = mutate(&encrypted, |v| v["payload"]["key_id"] = json!("unknown"));
    assert_eq!(
        codec.decode(&unknown).await.unwrap_err(),
        CryptoError::KeyUnavailable
    );
}

#[tokio::test]
async fn immutable_metadata_tamper_is_rejected_without_upgrading_tenant_authorization() {
    let codec = EncryptionCodec::new(ring(), all(), EnvelopeLimits::default());
    let wire = codec.encode(&envelope()).await.unwrap();
    type Mutation = Box<dyn Fn(&mut Value)>;
    let changes: Vec<Mutation> = vec![
        Box::new(|v| v["id"] = json!(Uuid::new_v4())),
        Box::new(|v| v["correlation_id"] = json!(Uuid::new_v4())),
        Box::new(|v| v["causation_id"] = json!(Uuid::new_v4())),
        Box::new(|v| v["created_at"] = json!("2020-01-01T00:00:00Z")),
        Box::new(|v| v["version"] = json!(2)),
        Box::new(|v| v["headers"]["tenant"] = json!("forged-membership")),
        Box::new(|v| v["headers"]["new-header"] = json!("injected")),
        Box::new(|v| {
            v["headers"].as_object_mut().unwrap().remove("tenant");
        }),
    ];
    for change in changes {
        assert_eq!(
            codec.decode(&mutate(&wire, change)).await.unwrap_err(),
            CryptoError::Authentication
        );
    }
    // Integrity preserves the sender's data; it does not grant tenant membership.
    let decoded = codec.decode(&wire).await.unwrap();
    assert_eq!(decoded.headers()["tenant"], "untrusted-original");
}

#[tokio::test]
async fn malformed_oversized_duplicate_setup_and_plaintext_requirements() {
    let local = envelope();
    let codec = EncryptionCodec::new(ring(), all(), EnvelopeLimits::default());
    assert_eq!(
        codec.decode(&local).await.unwrap_err(),
        CryptoError::Required
    );
    assert!(codec.decode_bytes(b"not-json").await.is_err());
    let encrypted = codec.encode(&local).await.unwrap();
    for field in ["nonce", "ciphertext", "key_id"] {
        let bad = mutate(&encrypted, |v| v["payload"][field] = json!("!!!"));
        assert!(codec.decode(&bad).await.is_err());
    }
    let unknown_field = mutate(&encrypted, |v| v["payload"]["future"] = json!(true));
    assert_eq!(
        codec.decode(&unknown_field).await.unwrap_err(),
        CryptoError::Framing
    );
    let unknown_format = mutate(&encrypted, |v| {
        v["payload"]["format"] = json!("raddle-aes-future")
    });
    assert_eq!(
        codec.decode(&unknown_format).await.unwrap_err(),
        CryptoError::Framing
    );
    let bounded = EncryptionCodec::new(
        ring(),
        all(),
        EnvelopeLimits {
            max_bytes: local.to_vec().unwrap().len(),
            max_headers: 1,
        },
    );
    assert_eq!(
        bounded.encode(&local).await.unwrap_err(),
        CryptoError::Limits
    );
    assert_eq!(
        bounded.decode(&encrypted).await.unwrap_err(),
        CryptoError::Limits
    );
    assert_eq!(
        bounded.decode_bytes(&vec![b' '; 2000]).await.unwrap_err(),
        CryptoError::Limits
    );
    let no_headers = EncryptionCodec::new(
        ring(),
        all(),
        EnvelopeLimits {
            max_bytes: 10000,
            max_headers: 0,
        },
    );
    assert_eq!(
        no_headers.encode(&local).await.unwrap_err(),
        CryptoError::Limits
    );
    // No implicit double setup: a second encode of the wire copy refuses it.
    assert_eq!(
        codec.encode(&encrypted).await.unwrap_err(),
        CryptoError::AlreadyEncrypted
    );
}

#[tokio::test]
async fn selective_policies_and_typed_fault_pairing_are_order_independent() {
    use raddle_policies::{AttemptCounters, ExceptionNode, FailureDisposition, Fault};
    let selected = Selection::Contracts(BTreeSet::from(["secret".into()]));
    let policy = EncryptionPolicy {
        send: selected.clone(),
        receive_required: selected,
    };
    let codec = EncryptionCodec::new(ring(), policy, EnvelopeLimits::default());
    let local = envelope();
    let fault = Fault::<Secret>::from_envelope(
        &local,
        AttemptCounters {
            handling: 3,
            sending: 0,
        },
        1234,
        "source".into(),
        None,
        FailureDisposition::DeadLetter,
        ExceptionNode {
            summary: "redacted".into(),
            cause: None,
        },
    )
    .unwrap();
    let fault = Envelope::encode(&fault).unwrap();
    assert_eq!(
        codec.decode(&fault).await.unwrap_err(),
        CryptoError::Required
    );
    let forged_plaintext = mutate(&fault, |v| {
        v["payload"]["source_contract"] = json!("unselected")
    });
    assert_eq!(
        codec.decode(&forged_plaintext).await.unwrap_err(),
        CryptoError::Required
    );
    let wire = codec.encode(&fault).await.unwrap();
    assert_ne!(wire.to_vec().unwrap(), fault.to_vec().unwrap());
    assert_eq!(
        codec.decode(&wire).await.unwrap().to_vec().unwrap(),
        fault.to_vec().unwrap()
    );
    let tampered = mutate(&wire, |v| v["payload"]["fault_source"] = json!("other"));
    assert_eq!(
        codec.decode(&tampered).await.unwrap_err(),
        CryptoError::Authentication
    );
    let other = mutate(&local, |v| v["contract"] = json!("other"));
    assert_eq!(
        codec.encode(&other).await.unwrap().to_vec().unwrap(),
        other.to_vec().unwrap()
    );
    assert!(codec.decode(&other).await.is_ok());
}

#[test]
fn keys_are_private_redacted_bounded_and_rotation_ids_immutable() {
    let key = SecretKey::new([17; 32]);
    assert_eq!(format!("{key:?}"), "SecretKey([REDACTED])");
    let material = KeyMaterial::new(KeyId::new("public-id").unwrap(), Arc::new(key));
    assert!(format!("{material:?}").contains("[REDACTED]"));
    let ring = ring();
    assert!(
        ring.insert(KeyId::new("one").unwrap(), SecretKey::new([18; 32]), true)
            .is_err()
    );
    ring.insert(KeyId::new("two").unwrap(), SecretKey::new([18; 32]), false)
        .unwrap();
    assert!(
        ring.insert(
            KeyId::new("three").unwrap(),
            SecretKey::new([19; 32]),
            false
        )
        .is_err()
    );
    for id in ["", "bad\nvalue", "has space"] {
        assert!(KeyId::new(id).is_err());
    }
}

#[tokio::test]
async fn cancellation_drops_provider_future_without_publishing_or_mutating_message() {
    struct Blocking {
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }
    struct Guard(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    impl KeyProvider for Blocking {
        async fn active(&self) -> Result<KeyMaterial, KeyProviderError> {
            let _guard = Guard(self.dropped.clone());
            std::future::pending().await
        }
        async fn lookup(&self, _: &KeyId) -> Result<KeyMaterial, KeyProviderError> {
            std::future::pending().await
        }
    }
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let codec = EncryptionCodec::new(
        Blocking {
            dropped: dropped.clone(),
        },
        all(),
        EnvelopeLimits::default(),
    );
    let local = envelope();
    let before = local.to_vec().unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), codec.encode(&local))
            .await
            .is_err()
    );
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(local.to_vec().unwrap(), before);
}
