//! Legacy wire vectors and finite native slot ownership.
#![allow(clippy::unwrap_used)]

use chrono::{TimeZone, Utc};
use raddle::grouping::{Dispatch, DispatchOptions, GroupId};
use raddle::sequencing::SlotSet;
use raddle::wire::{Codecs, Envelope, EnvelopeMetadata, WireMessage};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Work {
    id: u32,
}
impl WireMessage for Work {
    const NAME: &'static str = "group.work";
    const VERSION: u32 = 1;
}

fn original() -> Envelope {
    Envelope::with_identity(
        Uuid::from_u128(1),
        Utc.timestamp_opt(0, 0).unwrap(),
        &Work { id: 7 },
    )
    .unwrap()
}

#[test]
fn legacy_bytes_content_and_public_metadata_literal_stay_exact() {
    let expected = br#"{"id":"00000000-0000-0000-0000-000000000001","contract":"group.work","version":1,"created_at":"1970-01-01T00:00:00Z","correlation_id":null,"causation_id":null,"headers":{},"payload":{"id":7}}"#;
    let envelope = original();
    assert_eq!(envelope.to_vec().unwrap(), expected);
    assert_eq!(
        Envelope::from_slice(expected, Default::default())
            .unwrap()
            .to_vec()
            .unwrap(),
        expected
    );
    assert_eq!(
        envelope.content_bytes().unwrap(),
        br#"["group.work",1,null,null,{}, {"id":7}]"#
            .iter()
            .copied()
            .filter(|b| *b != b' ')
            .collect::<Vec<_>>()
    );
    let metadata = EnvelopeMetadata {
        id: envelope.id(),
        contract: "group.work".into(),
        version: 1,
        created_at: envelope.created_at(),
        correlation_id: None,
        causation_id: None,
        headers: Default::default(),
    };
    assert_eq!(
        Envelope::from_parts(metadata, json!({"id":7}), Default::default())
            .unwrap()
            .to_vec()
            .unwrap(),
        expected
    );
}

#[test]
fn absent_resolved_none_empty_and_explicit_key_are_distinct_and_preserved() {
    let original = original();
    let absent = original
        .clone()
        .with_dispatch(Dispatch::resolved(None, None))
        .unwrap();
    let empty = original
        .clone()
        .with_dispatch(Dispatch::resolved(Some(GroupId::new("")), Some("".into())))
        .unwrap();
    assert_ne!(
        original.content_bytes().unwrap(),
        absent.content_bytes().unwrap()
    );
    assert_ne!(
        absent.content_bytes().unwrap(),
        empty.content_bytes().unwrap()
    );
    let rebody = empty
        .rebody(&Work { id: 8 }, Default::default())
        .unwrap()
        .correlated(Uuid::from_u128(2), Uuid::from_u128(3))
        .unwrap();
    assert_eq!(rebody.dispatch(), empty.dispatch());
    assert_eq!(
        Envelope::from_slice(&empty.to_vec().unwrap(), Default::default())
            .unwrap()
            .dispatch(),
        empty.dispatch()
    );
    assert!(empty.require_legacy_dispatch().is_err());
    let codecs = Codecs::default();
    assert_eq!(codecs.decode::<Work>(&empty).unwrap().id, 7);
    let explicit = original
        .with_dispatch(Dispatch::explicit(DispatchOptions {
            group: Some(GroupId::new("")),
            partition_key: None,
        }))
        .unwrap();
    assert!(!explicit.dispatch().unwrap().is_resolved());
    for (key, value) in [
        ("version", json!(2)),
        ("unknown", json!(true)),
        ("group", json!(3)),
        ("resolved", json!(null)),
    ] {
        let mut raw = serde_json::to_value(&explicit).unwrap();
        raw["dispatch"][key] = value;
        assert!(
            Envelope::from_slice(&serde_json::to_vec(&raw).unwrap(), Default::default()).is_err()
        );
    }
}

#[test]
fn finite_slots_cover_every_slot_are_stable_and_release_without_key_state() {
    let slots = SlotSet::new("test", 7).unwrap();
    let other = SlotSet::new("test", 7).unwrap();
    let mut seen = [false; 7];
    for n in 0..2000 {
        let group = GroupId::new(format!("key-{n}"));
        let selected = slots.slot("tenant", Some(&group), Uuid::nil());
        assert_eq!(
            selected,
            other.slot("tenant", Some(&group), Uuid::from_u128(9))
        );
        seen[selected] = true;
        let permit = slots.try_reserve(selected).unwrap();
        assert!(slots.try_reserve(selected).is_none());
        drop(permit);
        assert!(slots.try_reserve(selected).is_some());
    }
    assert!(seen.into_iter().all(|used| used));
    assert_eq!(slots.len(), 7);
    assert!(SlotSet::new("test", 0).is_err());
    assert!(SlotSet::new("test", 257).is_err());
}

#[test]
fn dispatch_debug_does_not_disclose_group_or_transport_key() {
    let options = DispatchOptions {
        group: Some(GroupId::new("private-group")),
        partition_key: Some("private-key".into()),
    };
    let text = format!("{options:?} {:?}", Dispatch::explicit(options.clone()));
    assert!(!text.contains("private-group"));
    assert!(!text.contains("private-key"));
}
