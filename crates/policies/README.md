# raddle-policies

Explicit native Rust recovery policies, retry ownership, endpoint circuits,
quotas and fault publication. Applications connect a `PolicyExecutorDriver` to
the endpoint that owns delivery custody; constructing a policy does not install
it into legacy runtime/worker APIs. `raddle-messaging` supplies explicit typed
policy queue and durable endpoint registrations for actual Host-owned execution.

Retries of external effects require explicit replay safety. Keep permanent
refusals, uncertain outcomes and successful custody distinct. Policy state does
not replace durable inbox/outbox state or backend-specific guarantees.

See the repository's [policy and encryption guide](../../docs/native-policy-crypto.md)
for execution semantics and remaining integration boundaries.
