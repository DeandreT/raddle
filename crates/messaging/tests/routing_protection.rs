//! Actual crypto v2 roots must be refused before permissive typed projection.
#![cfg(feature = "crypto-ingress")]
#![allow(clippy::unwrap_used)]

use chrono::Utc;
use raddle::RuntimeBuilder;
use raddle::routing::EndpointAlias;
use raddle::transport::{Transport, TransportError};
use raddle::wire::{Envelope, WireMessage};
use raddle_crypto::{EncryptionCodec, EncryptionPolicy, KeyId, RotatingKeys, SecretKey, Selection};
use raddle_messaging::routing::{
    MessageDeclaration, MessageRole, MessageSelector, RoutingConfig, RoutingError,
};
use raddle_messaging::{
    BusBuilder, BusError, IngressError, JsonIngress, MessageContext, TrustedOrigin,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Default, Serialize, Deserialize)]
struct Defaulted {
    #[serde(default)]
    number: i32,
}
impl WireMessage for Defaulted {
    const NAME: &'static str = "routing.defaulted";
    const VERSION: u32 = 1;
}
#[derive(Serialize, Deserialize)]
struct Projected(i32);
impl WireMessage for Projected {
    const NAME: &'static str = "routing.projected";
    const VERSION: u32 = 1;
}
#[derive(Clone, Default)]
struct Recorded(Arc<Mutex<Vec<Envelope>>>);
impl Transport for Recorded {
    async fn publish(&self, _: &str, envelope: &Envelope) -> Result<(), TransportError> {
        self.0.lock().unwrap().push(envelope.clone());
        Ok(())
    }
}
fn origin() -> TrustedOrigin {
    TrustedOrigin::authenticated("tenant", "secure-ingress", None).unwrap()
}

#[tokio::test]
async fn real_protected_roots_cannot_be_stripped_but_decrypted_canonical_input_projects() {
    let keys = RotatingKeys::new(1).unwrap();
    keys.insert(
        KeyId::new("routing-key").unwrap(),
        SecretKey::generate().unwrap(),
        true,
    )
    .unwrap();
    let codec = EncryptionCodec::new(
        keys,
        EncryptionPolicy {
            send: Selection::All,
            receive_required: Selection::All,
        },
        Default::default(),
    );
    let original = Envelope::encode(&Defaulted { number: 42 }).unwrap();
    let protected = codec.encode(&original).await.unwrap();
    let value = serde_json::to_value(&protected).unwrap();
    assert_eq!(value["payload"]["format"], "raddle-aes256gcm-v2");
    // This permissive model really WOULD discard the complete protected root.
    assert_eq!(protected.decode::<Defaulted>().unwrap().number, 0);
    let captured = Recorded::default();
    let mut builder = BusBuilder::new(RuntimeBuilder::default().build());
    builder.remote("target", captured.clone()).unwrap();
    let mut config = RoutingConfig::default();
    config
        .declare(MessageDeclaration::of::<Defaulted>(MessageRole::Outgoing).unwrap())
        .unwrap();
    config
        .declare(MessageDeclaration::of::<Projected>(MessageRole::PublishOnly).unwrap())
        .unwrap();
    config
        .transform::<Defaulted, Projected>("project", 1, |message| Ok(Projected(message.number)))
        .unwrap();
    config
        .rule(
            "target-rule",
            "Plaintext projection destination",
            MessageSelector::exact::<Projected>().unwrap(),
            vec![EndpointAlias::try_new("target").unwrap()],
        )
        .unwrap();
    let bus = builder.build_routed(config).unwrap();
    for bad in 0..3 {
        captured.0.lock().unwrap().clear();
        for position in 0..3 {
            let context = MessageContext::new(
                origin(),
                if position == bad {
                    protected.clone()
                } else {
                    original.clone()
                },
                Utc::now(),
            );
            let result = bus.dispatch(context, true).await;
            if position == bad {
                assert!(matches!(
                    result,
                    Err(BusError::Routing(RoutingError::Protected))
                ));
            } else {
                result.unwrap();
            }
        }
        assert_eq!(captured.0.lock().unwrap().len(), 2);
        bus.receive(
            &codec,
            &protected.to_vec().unwrap(),
            origin(),
            Utc::now(),
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            captured
                .0
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .decode::<Projected>()
                .unwrap()
                .0,
            42
        );
    }
    assert!(matches!(
        bus.receive(
            &JsonIngress::new(Default::default()),
            &protected.to_vec().unwrap(),
            origin(),
            Utc::now(),
            true
        )
        .await,
        Err(IngressError::Admission(BusError::Routing(
            RoutingError::Protected
        )))
    ));
    let parent = MessageContext::new(origin(), original, Utc::now());
    // Outgoing typed frames are guarded too, not only ingress bytes.
    #[derive(Serialize, Deserialize)]
    #[serde(transparent)]
    struct Raw(serde_json::Value);
    impl WireMessage for Raw {
        const NAME: &'static str = Defaulted::NAME;
        const VERSION: u32 = 1;
    }
    let raw = Raw(value["payload"].clone());
    assert!(matches!(
        bus.intents(&raw, &parent, true),
        Err(BusError::Routing(RoutingError::Protected))
    ));
}
