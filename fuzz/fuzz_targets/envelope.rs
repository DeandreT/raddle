#![no_main]

use libfuzzer_sys::fuzz_target;
use raddle::wire::{Envelope, EnvelopeLimits};

fuzz_target!(|bytes: &[u8]| {
    let limits = EnvelopeLimits::default();
    if let Ok(envelope) = Envelope::from_slice(bytes, limits) {
        let encoded = envelope.to_vec().expect("validated serialization");
        assert!(encoded.len() <= limits.max_bytes);
        let again = Envelope::from_slice(&encoded, limits).expect("canonical round trip");
        assert_eq!(again.id(), envelope.id());
        assert_eq!(again.contract(), envelope.contract());
        assert_eq!(again.version(), envelope.version());
        assert_eq!(again.headers(), envelope.headers());
        let id = envelope.id();
        if let Ok(correlated) = envelope.correlated(id, id) {
            let bytes = correlated
                .to_vec()
                .expect("validated correlation serialization");
            let decoded = Envelope::from_slice(&bytes, limits).expect("bounded correlation");
            assert_eq!(decoded.correlation_id(), Some(id));
            assert_eq!(decoded.causation_id(), Some(id));
        }
    }
});
