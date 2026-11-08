//! Stable subscription identity validation.
#![allow(clippy::expect_used)]

use raddle::{InvalidSubscriptionName, SubscriptionName};

#[test]
fn valid_names_round_trip_without_normalization() {
    for value in ["a", "0", "orders.audit-v2_primary", &"a".repeat(128)] {
        let name = SubscriptionName::try_new(value).expect("valid name");
        assert_eq!(name.as_str(), value);
        assert_eq!(name.to_string(), value);
        assert_eq!(name.as_ref(), value);
        assert_eq!(value.parse::<SubscriptionName>().expect("parses"), name);
    }
}

#[test]
fn empty_names_are_rejected() {
    assert_eq!(
        SubscriptionName::try_new(""),
        Err(InvalidSubscriptionName::Empty)
    );
}

#[test]
fn name_length_is_bounded_in_bytes() {
    assert_eq!(
        SubscriptionName::try_new("a".repeat(129)),
        Err(InvalidSubscriptionName::TooLong { length: 129 })
    );
}

#[test]
fn disallowed_names_are_not_silently_rewritten() {
    for value in [
        "A", " space", "a/b", "a\\b", "é", "a\0", "a\n", ".a", "-a", "_a",
    ] {
        assert!(matches!(
            SubscriptionName::try_new(value),
            Err(InvalidSubscriptionName::InvalidByte { .. })
        ));
    }
    assert_eq!(
        SubscriptionName::try_new("a B"),
        Err(InvalidSubscriptionName::InvalidByte { index: 1 })
    );
}
