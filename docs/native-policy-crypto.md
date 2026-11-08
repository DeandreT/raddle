# Explicit endpoint policies and payload encryption

`raddle-policies` and `raddle-crypto` are opt-in native Rust components. Neither
crate installs behavior into `Runtime`, `MessageBus`, or the PostgreSQL workers.
An application must connect their driver and receiver boundaries explicitly.
These contracts are not a claim of complete Wolverine parity or .NET wire
compatibility.

## Failure policy ownership

Build a `PolicySet`, move it into `PolicyExecutor`, and bind each execution to a
`PolicyExecutorDriver` that owns one active delivery. `Execution` supplies the
configured endpoint, stable contract, direction, actual attempt counters,
monotonic clock, jitter sample and explicit replay authorization.

For handling, the first matching message rule wins, then endpoint rules, then
global rules. For sending, endpoint rules precede globals; handler rules do not
apply. A matching exhausted rule does not fall through. Filters match exact Rust
error types, predicates, bounded source chains and Boolean combinations. No
Display-string type matching is implied.

Finite `Plan` slots follow the original failed attempt: two recovery slots allow
at most three actual executions. `Tail::Terminal` is discard or dead-letter;
`Tail::Repeat` explicitly opts into indefinite recovery. Pauses decorate the
whole step, including repeated tails. Jitter applies once at the rule level and
never shortens cooldown or scheduled delay. `AttemptCounters` keeps sending and
handling independent.

The executor applies interruptible cooldown, endpoint pause and atomic custody
transitions. It does not call an arbitrary handler again. `RetryInline` requests
another execution from the driver owner. Queued actions do not silently apply
to inline invocation. A closed stop channel or a true stop value retains
custody; a false notification is not cancellation.

The memory driver supplies OS-random rule jitter samples by default;
`with_jitter_source` installs a deterministic application-owned sampler.

`ReplaySafety::Never` is the default. `Idempotent` means the application guards
effects with a stable key; `Transactional` means all failed-attempt effects
really roll back. `AtLeastOnceSend` authorizes duplicate-capable publication,
not repeated handler effects. A database transaction cannot roll back file or
network writes.

## Executable memory endpoint

`InMemoryEndpoint` is an actual bounded volatile queue with a single owned
active claim, queue-tail requeue, due times, endpoint pauses, retained terminal
state and explicit failed-item retry. A failed sibling never replays successful
deliveries. Terminal retention counts against configured capacity; recreate or
externally archive an endpoint deliberately rather than silently evicting
duplicate-protection state.

Cancellation leaves `MemoryStatus::Interrupted`, not completed or dead-lettered.
Resumption requires `resume_interrupted` with explicit repeat authorization. If
the caller drops `run_once` itself, call `recover_interrupted` first; this does
not infer that uncertain effects are safe to repeat. No background task or
unbounded waiting queue is created.

Receive circuits are endpoint-local, bounded rolling samples. Defaults are ten
samples, a 15% failure ratio, ten-minute tracking and five-minute pause. Include
and exclude filters apply before sampling; exclusions and shutdown are not
successful samples that dilute failure ratios. Tripping clears observations.
After cooldown one probe is admitted; success resets, failure pauses again,
and cancellation releases the probe without sampling it.

`Quota` implements bounded token buckets, aligned fixed windows, exact sliding
windows and RAII concurrency permits. Clones share atomic in-process admission.
`ScopedQuotas` uses endpoint configuration before message fallback, and never
allocates per attacker-supplied header or endpoint. `with_quotas` checks before
attempts, retains limited work and pauses/reschedules until the next known
eligibility time. These are not a distributed store, timezone calendar policies,
or a SQL Server provider.

## Terminal and unknown-message paths

`FailureDisposition` separates retry, discard, dead-letter and expiry.
`ExceptionNode::capture` requires an application redactor, bounds every summary
to 1024 UTF-8 bytes, and caps the causal graph at depth ten, including cycles.
`Fault<M>` retains original message identity, counters, time, configured source,
application-resolved tenant and original untrusted headers. Its Debug output
does not expose the message or tenant.

`FaultOptions` is opt-in. Handler DLQ/expiry are eligible; discard is separately
opt-in. Sending, unknown-message and cryptography failures bypass fault
generation. Drivers carry trusted recursive-fault attribution out of band; the
reserved native `raddle.fault` contract is also suppressed. A forged header does
not grant or remove bypass status.

`TerminalFaultPublisher` publishes a typed fault only after memory-driver
terminal settlement. Publication is best-effort, not atomic with DLQ, and a
failed fault publication logs a payload-free warning without retrying the
original. Durable drivers must preserve this ordering and independently define
their post-settlement publication hook. Expiry eligibility exists in
`FaultOptions`; the memory queue does not implement message deadlines.

`UnknownMessageHandler` executes ordered missing-handler observers even when
one observer fails, then settles through the active driver. Default is discard;
DLQ is configurable. Tracking is bounded, redacted in Debug and retains exact
original bytes. Observers must tolerate duplicate observation after custody
I/O uncertainty. This is distinct from an unregistered typed local Request.

## Authenticated encryption

`EncryptionCodec<P>` owns an asynchronous `KeyProvider`. `RotatingKeys` is a
bounded real provider for application-managed keys: key IDs are immutable,
activation retains historical keys, explicit retirement refuses the active key,
and no implicit global cache or background task exists. `SecretKey` is private,
nonserializable, redacted in Debug and zeroized on Drop. Expanded AES and hash
state zeroization is explicitly enabled in the RustCrypto dependencies. The
application must clear any key-byte copies it imported.

Encryption uses RustCrypto AES-256-GCM with a fresh 96-bit nonce from the OS
CSPRNG for every encode. Random nonce uniqueness is probabilistic, not a global
nonce registry or replay defense. Providers must enforce appropriate key-use
and rotation limits across processes. No KMS adapter is implied.

Native framing replaces only the payload with a versioned structured object.
The wire copy preserves identity, creation time, correlation, causation, contract,
version and headers. It is validated again through `Envelope::from_slice` with
explicit encoded byte/header limits; ciphertext/base64 overhead counts against
the same wire limit. The local live envelope is never mutated.

The `raddle-aes256gcm-v2` frame's exact AAD encoding is a JSON tuple in this
order: format marker, original UUID, contract name, schema version, UTC creation
time, optional correlation UUID, optional causation UUID, all application
headers, optional native fault source contract, key ID and inner content type.
Headers serialize in their `BTreeMap` key order; UUID/time values use the same
typed serde representation as `Envelope`. The fixed tuple and sorted map make
AAD independent of incoming JSON field order or whitespace and of serde_json's
optional object-order feature. Inner content type is `application/json`.

No application header is excluded: crypto metadata lives only in the payload
frame. Broker delivery tags, redelivery flags, lock tokens and mutable delivery
options are outside the immutable envelope and outside AAD. Changing UUID or
other stable metadata without the key invalidates the tag before staging,
preventing an identity-rewriting replay from bypassing inbox deduplication.
Exact unchanged replays still need inbox duplicate protection and its retention
policy. Integrity does not authorize the sender's original tenant/header values
or prove membership. The `format` payload field with the `raddle-aes` prefix is
reserved for codec framing. There is no public caller-selectable nonce API.

This stronger native immutable-metadata contract is deliberately distinct from
the pinned Wolverine encryption behavior, which does not authenticate ordinary
outer headers. It is not a .NET binary/framing compatibility claim.

Send selection and receive requirements are independent: none, all, or selected
stable contracts. Native `Fault<M>` values currently share `raddle.fault` as
their wire contract, so any nonempty selective policy conservatively protects
all native faults. Plaintext fault bodies cannot downgrade requirements by
forging `source_contract`. Exact per-generic-fault selection requires a future
distinct typed fault registry and is not claimed by this framing. Unknown keys,
wrong keys, malformed framing, metadata/tag tamper,
oversized messages and required plaintext fail before typed body decoding. The
receiver owner must dead-letter crypto failures directly, bypassing handler
retry policies and terminal faults.

`EncryptedTransport<T, P>` implements confirmed `Transport`: encode finishes
before publication begins; success means the underlying publisher confirmed
acceptance, not handler completion. `publish_encrypted` retains detailed crypto
versus transport errors; the base `Transport` surface maps crypto errors to
`Rejected`. Run `receive` or `EncryptionCodec::decode` before dispatch. Dropping
a codec operation cancels a pending provider future without caching partial
results. `into_parts` permits explicit transport shutdown.

Always decrypt and validate **before** durable inbox comparison or staging.
Fresh random nonces make two wire copies of the same original identity differ;
key rotation changes them again. The canonical durable envelope must be the
byte-identical recovered original, not ciphertext, or legitimate retries cause
`IdentityConflict`. Immutable delivery fingerprint inputs also need to stay
stable across intake retries; do not replace their original available time with
processing-time `now` for every duplicate. Crypto refusal must create neither
an inbox row nor a handler attempt. The `postgres-tests` feature exercises this
ordering against SQLx-created isolated databases; combined with `rabbitmq-tests`
it receives repeated confirmed publications through the actual broker adapter.

## Integration boundaries

A bus adapter must retain one active delivery across `RetryInline`, advance the
appropriate counter only when actual execution starts, preserve counters across
scheduled/requeued work, and honor endpoint-local pauses. It must select
fault/unknown/crypto bypass paths before ordinary handler policy application.
After confirmed sends it must settle only that delivery. A cancellation is not
a send failure or a receive-circuit observation.

The existing PostgreSQL `Store::fail` implements its own bounded backoff and
`Scope::commit` completes its active claim. These public methods do not express
exact scheduled times, queue-tail movement, indefinite retry, independent
persisted counters or endpoint pause state. A faithful durable driver needs
coordinated fenced Store operations; it must not emulate those guarantees with
unfenced direct SQL or by completing an unsent delivery.

The opt-in `rabbitmq-tests` feature enables a disposable-broker test using an
existing `RADDLE_RABBITMQ_TEST_URI`. It declares and deletes only run-unique
input/dead-letter queues and their dead-letter exchange. Crypto failures are
rejected directly into that configured DLQ without handler attempts. No cloud
resource, pipeline, global tool or proprietary binary is needed.
Native tests and broker framing do not qualify Wolverine's .NET serializers;
cross-runtime key-provider/framing interoperability is a separate track.
