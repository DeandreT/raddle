# Explicit endpoint policies and payload encryption

`raddle-policies` and `raddle-crypto` are opt-in native Rust components.
`raddle-messaging` binds policies to actual Host-owned local queues and durable
endpoints through explicit registration. Neither component changes the legacy
`Runtime` or PostgreSQL `Worker` defaults. Encryption still requires an explicit
ingress/transport boundary.
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
One sample is drawn per failed attempt. The rule independently jitters each
original cooldown/scheduled delay and listener pause once with that same sample;
it never jitters their sum or a duration already jittered by another component.
Pause and recovery delays retain their separate clocks/effects rather than
becoming an additional sequential wait.

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
every actual execution, including inline retries and after cooldown, retains
limited work and pauses/reschedules until the next known
eligibility time. These are not a distributed store, timezone calendar policies,
or a SQL Server provider.

Concurrency admission is held only for the actual operation and released before
recovery/cooldown, cancellation or future drop. A retry must acquire its own
permit, never a second permit while holding the previous one. Time quota refusal
does not advance attempts or enter receive-circuit observations. A quota-denied
half-open probe is released; interrupted probes remain explicitly recoverable
without being treated as failed samples.

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

Retired IDs remain permanently reserved for the ring's lifetime, including
across clones, even if reinsertion proposes identical bytes. Retirement frees
live-key capacity but not the finite ID history. `RotatingKeys::new` permits up
to 4096 successful lifetime IDs; `with_lifetime_id_budget` configures a budget
between live capacity and 65536. Exhaustion refuses new insertions without
replacing active keys or evicting reservations. Rejected insertions spend no ID
budget. The metadata is therefore bounded, not an unbounded tombstone set.
Applications replacing a ring or implementing an external provider must retain
key-ID uniqueness across provider lifetimes while historical ciphertext exists.

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

### Application-owned policy execution

`PolicyQueueBuilder::handle::<M, E, _, _>` registers a local typed handler/error;
`scoped::<M, H>` uses the complete existing scoped pipeline against PostgreSQL.
Build the queue, register its sending handle with `BusBuilder::policy_queue`,
route messages to its configured name, and attach its sole runner to `Host`.
Send/Publish acknowledge bounded volatile custody, not handler completion.
Each queue is sequential. Independent named queues provide independent owners.
Capacity includes retained terminal identities; no implicit eviction weakens
failed-only replay or duplicate protection. Identity reuse with different
canonical input or trusted origin is refused. A matching duplicate is a no-op.
Shutdown joins pre-commit cancellation/rollback/Finally/disposal and retains the
interrupted state; process death still loses the entire volatile queue.
Queue-tail recovery merges custody accepted while the prior handler was running
before choosing the recovered item's tail position. Newly accepted siblings do
not get overtaken merely because they were still in the receiving buffer.

For durable execution, call `DurableEndpoint::policies(EndpointPolicies)` before
registering `handle_policy::<M, H>`. Apply migration 009 explicitly first. Do not
mix legacy and policy workers against the same subscription. This opt-in retry
budget replaces `Delivery::max_attempts` and legacy backoff for that subscription;
expiry, trusted source/tenant, partition fencing and inbox/outbox atomicity remain.
Concrete `H::Error` values reach policy filters directly. Other scoped failures
retain their typed `Failure<H::Error>` classification. Every attempt constructs
fresh resources and runs the original pipeline, including cleanup. Post-commit
flush/callback/cleanup errors remain committed successes and never replay a parent.
Strict execution distinguishes a PostgreSQL transaction rejection from uncertain
completion. A server `ERROR` with integrity SQLSTATE class `23`, serialization
failure `40001`, or deadlock `40P01` from the fenced completion/COMMIT path cannot
have committed the root. It remains an order-local `Failure::Database`, allowing
policy disposition without terminating the receiving service. Shared completion
prerequisites are diagnosed separately: PostgreSQL ERROR
SQLSTATE class `42` (query/schema/access), `3F000` (invalid schema), `0A000`
(unsupported feature), and `25006` (read-only transaction) remain shared
`Failure::Database` errors that propagate to the Host. They are not suppressed as
one delivery's lost connection. This diagnosis does not attest rollback or clear
tainted custody; recovery still needs independent verification and authorization.
Other commit errors, including connection loss and statement-completion-unknown
`40003`, and all failed rollback attempts use `UncertainDatabase` and bypass
handler recovery.
After a successful pre-commit rollback, invalid
returned intent, identity conflict, capacity refusal and SQL data/integrity/
transaction-rollback errors remain order-local policy failures. Genuine backend
or schema failures still propagate as infrastructure errors; one malformed
outgoing intent does not stop unrelated orders or receiving owners.

Immediate/cooldown retain the live claim; requeue/schedule release it at the SQL
queue tail with the same identity and counters; discard retains a completed
tombstone, while dead-letter retains quarantine. Pauses are endpoint-local and
durably recorded. Every actual attempt reacquires quota and circuit admission;
quota refusal does not consume a handler attempt or circuit sample. Quotas and
circuit observations are process-local, not distributed limits. Existing in-flight
attempts are not retroactively cancelled when a pause is selected.
Durable receive circuits currently require sequential execution: combining a
circuit with endpoint parallelism greater than one is rejected at registration.
This prevents late concurrent observations from resolving another attempt's
half-open probe. Parallel durable queues without circuits remain supported.
Managed quota admission uses one quota-owned monotonic clock shared across quota
clones, including endpoints started at different times. Clock sampling and
admission are serialized together; retry delays are translated into the receiving
endpoint's scheduling clock. Direct explicit-clock `Quota::acquire` remains
caller-controlled; do not mix that clock domain with managed `acquire_current`
calls on the same quota state.

All retry registration requires an explicit `ReplaySafety`. A SQL scope alone
does not authorize repeating external effects, OnException or cleanup effects.
`Never` refuses recovery rather than replaying an arbitrary operation. Sending-only
`AtLeastOnceSend` is rejected when registering a handling endpoint. Durable
attempt entry records a conservative uncertainty marker before business work;
only a proven failed transaction (joined rollback or known server rejection)
authorizes policy recovery, still subject to explicit replay safety. Commit/rollback I/O
failure or a crash during an attempt leaves recovery blocked, even after lease
expiry. An operator must reconcile and explicitly call the tenant-scoped audited
`Store::recover_policy_uncertain` with `PolicyRecovery` before retrying. This is
deliberately conservative even for SQL-only crashes. A small crash window between
attempt recording and handler entry can consume a policy slot. Inspection's
legacy attempt field is separate from the persisted policy handling counter.

Uncertain recovery locks the matching active tenant job and then its attempt
record before sampling effective caller time. The lease must have expired, the
whole message must still be live, and the uncertainty marker must still be set
after actual pool/job/attempt-row waits. Denial leaves custody, counters, marker
and audit unchanged. Authorized recovery uses that same post-wait microsecond
timestamp for scheduling and audit, without mixing in server wall time. Audit
validation and shared database errors still propagate rather than becoming a
false denial. Recovery never relaxes the active-status or tenant boundary.

Lease expiry during cooldown or asynchronous cleanup can end this owner's retry
permission. Stale ownership affects only that claim, never the Host's cancellation
signal or healthy siblings. The owner cannot extend an expired lease or clear a
successor's uncertainty marker. A scoped post-admission uncertain commit or
rollback retains only the affected delivery, logs a payload-free error, and
bypasses ordinary retry and circuit failure samples. It does not stop the Host
or cancel sibling services: healthy current and newly arriving work use fresh
scopes. SQL custody keeps its uncertainty marker and requires audited operator
reconciliation; no success acknowledgement or automatic lease-recovery replay
is invented. Claim counts returned by `run_once` are observations, not business
successes.

The local policy runner retains uncertain work as `Interrupted` in its existing
volatile custody. It does not claim a durable taint record or audit trail. Only
an explicit `resume_interrupted` safety authorization, after reconciliation, can
resume that identity; duplicate submission and ordinary dead-letter retry cannot
bypass it. Completed siblings remain completed. Process exit still loses this
volatile custody.

Shared claim acquisition, fresh-scope admission, custody-driver and other genuine
infrastructure errors still propagate. A real shared database outage is detected
by the next actual claim or scope admission; an isolated admitted attempt's lost
connection is not treated as proof that the entire database is unavailable.
The transaction pipeline still joins its commit/rollback attempt and Finally/
disposal before handing the uncertain result back to its runner. A successful
root's post-commit failures remain successful roots and never enter this
uncertainty or root-retry path.

Policy custody operations preserve the supplied caller clock and advance it by
monotonic elapsed time from method entry through connection-pool and row-lock
waits. They do not substitute the database wall clock or take the maximum of two
unrelated clock domains. Comparisons use PostgreSQL microsecond precision.
Attempt admission and `policy_execution` check both the live lease and the whole
message's `Delivery::expires_at` after actual waits, before exposing a scope for
business work. Expired input is not acknowledged as success: custody remains
recoverable or is quarantined by normal expiry handling. Settlement may still
dispose of an expired message under a live claim; it never authorizes new business
execution after message expiry.

Both queue owners support independent `UnknownMessageHandler` configuration.
Unknown input defaults to discard and runs missing-handler observers, bypassing
ordinary handler retries/counters. Invalid known bodies are quarantined; ingress
cryptography remains a pre-custody boundary. Trusted attribution comes from the
independently supplied `TrustedOrigin`, never envelope headers. Unknown tracking
at this canonical application boundary retains canonical envelope bytes, not the
original encrypted/framing bytes.

These registrations do not apply policies to ordinary Invoke, remote sends,
the legacy `LocalQueue` or the legacy SQL worker. They do not add broker-native
custody, distributed quotas/circuit observations, automatic terminal fault
publication, resumable AfterCommit callbacks or arbitrary custom recovery actions.
Terminal `Fault<M>` remains an explicit separate post-settlement publication
boundary. No full error-policy/runtime family parity is claimed.

A bus adapter must retain one active delivery across `RetryInline`, advance the
appropriate counter only when actual execution starts, preserve counters across
scheduled/requeued work, and honor endpoint-local pauses. It must select
fault/unknown/crypto bypass paths before ordinary handler policy application.
After confirmed sends it must settle only that delivery. A cancellation is not
a send failure or a receive-circuit observation.

The legacy PostgreSQL `Store::fail` implements its own bounded backoff and
`Scope::commit` completes its active claim. These public methods do not express
exact scheduled times, queue-tail movement, indefinite retry, independent
persisted counters or endpoint pause state. A faithful durable driver needs
coordinated fenced Store operations. The application durable endpoint uses the
dedicated `endpoint_policies` operations; it never emulates those guarantees with
unfenced direct SQL or by completing an unsent delivery.

The opt-in `rabbitmq-tests` feature enables a disposable-broker test using an
existing `RADDLE_RABBITMQ_TEST_URI`. It declares and deletes only run-unique
input/dead-letter queues and their dead-letter exchange. Crypto failures are
rejected directly into that configured DLQ without handler attempts. No cloud
resource, pipeline, global tool or proprietary binary is needed.
Native tests and broker framing do not qualify Wolverine's .NET serializers;
cross-runtime key-provider/framing interoperability is a separate track.
