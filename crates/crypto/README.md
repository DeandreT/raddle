# raddle-crypto

Opt-in AES-256-GCM envelope encryption with application-owned asynchronous key
providers and confirmed transport composition. Native framing authenticates
immutable envelope metadata as well as payload bytes; it does not establish
authorization, tenant membership or replay freshness.

Decrypt and validate before staging canonical durable ingress. Preserve the
original identity across nonce changes and key rotation, and use inbox duplicate
protection for repeated delivery. This crate does not automatically install a
receiver into the runtime or message bus.

See the repository's [policy and encryption guide](../../docs/native-policy-crypto.md)
for configuration, key-provider responsibilities and native interoperability
boundaries. Wolverine/.NET wire compatibility is not asserted.
