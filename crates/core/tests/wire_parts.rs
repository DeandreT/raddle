//! Validated metadata bridges without changing core runtime behavior.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;

use chrono::Utc;
use raddle::wire::{Envelope, EnvelopeLimits, EnvelopeMetadata, WireMessage};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Current {
    text: String,
}
impl WireMessage for Current {
    const NAME: &'static str = "parts.current";
    const VERSION: u32 = 2;
}

#[test]
fn validated_parts_preserve_every_independently_optional_metadata_field_and_rebody_changes_only_schema()
 {
    for (correlation_id, causation_id) in [
        (None, None),
        (Some(Uuid::from_u128(2)), None),
        (None, Some(Uuid::from_u128(3))),
        (Some(Uuid::from_u128(2)), Some(Uuid::from_u128(3))),
    ] {
        let metadata = EnvelopeMetadata {
            id: Uuid::from_u128(1),
            contract: "parts.old".into(),
            version: 1,
            created_at: Utc::now(),
            correlation_id,
            causation_id,
            headers: BTreeMap::from([("schema".into(), "old".into())]),
        };
        let envelope = Envelope::from_parts(
            metadata.clone(),
            json!({"old":"value"}),
            EnvelopeLimits::default(),
        )
        .unwrap();
        let current = envelope
            .rebody(
                &Current {
                    text: "value".into(),
                },
                EnvelopeLimits::default(),
            )
            .unwrap();
        assert_eq!(current.id(), metadata.id);
        assert_eq!(current.created_at(), metadata.created_at);
        assert_eq!(current.correlation_id(), correlation_id);
        assert_eq!(current.causation_id(), causation_id);
        assert_eq!(current.headers(), &metadata.headers);
        assert_eq!(current.contract(), Current::NAME);
        assert_eq!(current.version(), 2);
        assert_eq!(
            Envelope::from_slice(&current.to_vec().unwrap(), EnvelopeLimits::default())
                .unwrap()
                .content_bytes()
                .unwrap(),
            current.content_bytes().unwrap()
        );
        assert_eq!(envelope.contract(), "parts.old");
        assert_eq!(envelope.version(), 1);
    }
}

#[test]
fn deserialized_metadata_cannot_bypass_identity_contract_version_header_or_byte_validation() {
    let original = Envelope::encode(&Current {
        text: "value".into(),
    })
    .unwrap();
    let metadata = original.metadata();
    for invalid in [
        EnvelopeMetadata {
            id: Uuid::nil(),
            ..metadata.clone()
        },
        EnvelopeMetadata {
            contract: "Invalid contract".into(),
            ..metadata.clone()
        },
        EnvelopeMetadata {
            version: 0,
            ..metadata.clone()
        },
        EnvelopeMetadata {
            headers: BTreeMap::from([("Tenant".into(), "x".into())]),
            ..metadata.clone()
        },
    ] {
        let bytes = serde_json::to_vec(&invalid).unwrap();
        let unchecked: EnvelopeMetadata = serde_json::from_slice(&bytes).unwrap();
        assert!(Envelope::from_parts(unchecked, json!(null), EnvelopeLimits::default()).is_err());
    }
    assert!(
        Envelope::from_parts(
            metadata,
            json!(null),
            EnvelopeLimits {
                max_bytes: 1,
                max_headers: 32
            }
        )
        .is_err()
    );
}
