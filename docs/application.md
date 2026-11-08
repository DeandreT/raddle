# A composed application

The [application example](../crates/host/examples/application.rs) and its
[opt-in integration test](../crates/host/tests/application.rs) execute the
same application function. This is real PostgreSQL execution and RabbitMQ
publication/consumption, not a transport mock or a collection of API sketches.

## Execution path

1. A typed core `Pipeline` validates inputs and returns `PlaceOrder` business
   commands. Inputs run independently: a valid order, an invalid order, and
   another valid order produce two commands and one typed validation failure.
   The invalid input never registers a schedule or enters durable custody.
2. Each valid command registers a persistent typed `RecurringSchedule` inside
   its own explicit `Scope`. Equivalent repeated registration is idempotent.
   The first occurrence is due; a 24-hour interval prevents accidental catch-up
   loops in this bounded demonstration.
3. A hosted `RecurringScheduler` executes real database-clock `run_once` pages.
   Emitted occurrences retain persisted wire identities and enter local custody.
   The scheduler owns its own transactions; this example makes no cluster lease
   or fenced-singleton claim.
4. A hosted transactional `Worker::handle_saga` automatically correlates the
   order ID within the **trusted scope tenant**. A deliberately misleading tenant
   field in the command is not treated as authority. The transition writes
   business state, appends a versioned event using `Scope::append_events`, stages
   the same envelope as an outgoing job, and completes the saga. Those changes
   and incoming job completion share the worker's transaction.
5. A hosted `postgres_outbox` uses an actual RabbitMQ `Publisher`, with mandatory
   routing and confirms. Startup explicitly declares an application-owned durable
   topic exchange, durable queue, and binding using validated `Topology`.
6. A hosted receiver polls RabbitMQ `Basic.Get`, with one in-flight delivery and
   no detached delegate tasks. It validates each delivery, commits a stable,
   idempotent receipt, and only then explicitly acknowledges it. A deliberately
   malformed fixture delivery is durably quarantined with its original bytes
   before acknowledgement; it does not block later valid deliveries. Closing the
   channel requeues unacknowledged messages; successful shutdown joins the current
   callback before closing. This private example consumer uses Lapin so malformed
   raw wire remains available for quarantine without expanding the adapter API.
7. A hosted projector updates each stream's read model and checkpoint in the same
   `Scope`. Streams use independent transactions. Re-running a completed projection
   applies no events and does not increment its read-model application count.

Readiness is reported after actual scheduler/projection database passes, worker
batches, and completed broker polling/callback passes—not merely after creating
tasks. An idle outbox batch still is not an independent broker-health probe. The completion
observer checks persisted business/saga state, all four completed jobs, two
receipts, two successful explicit settlements, one durable quarantine, and
transactional projection checkpoints.
It then requests shutdown and awaits all host-owned tasks. Publication channels
and broker connections close only after joined shutdown.

The verifier compares the original event bytes with the outbox and broker receipt
bytes, including event UUID, command causation/correlation, and typed payload.
Receipts store the validated original broker body, not a decoded-and-reserialized
envelope; valid JSON whitespace and property order are not normalized away.
It also verifies that the broker queue is empty after settlement, both valid
orders completed, and the invalid order and forged tenant have no business state.
These checks do not claim exactly-once broker delivery: receipt idempotency covers
redelivery after commit/ack uncertainty, and external side effects still need
their own idempotency boundary.

## Running locally

Use **dedicated disposable services**, not a shared development or production
database/broker. RabbitMQ must use a numeric loopback address; PostgreSQL may use
numeric loopback or an explicitly selected absolute local Unix socket. The example
applies repository migrations and creates four `raddle_example_*` business
tables; it intentionally retains business state, receipts, events, saga tombstones,
jobs, and projection checkpoints for inspection. Each invocation uses a new tenant.
It cancels its own schedules and deletes only its unique queue/exchange before
returning success, and still attempts both cleanup actions after a returned
failure. Cleanup has a shared five-second outer budget and two-second limits for
each operation. The two owned schedule cancellations, publisher close, and both
entity deletions are polled independently; both connection closes are then
attempted even if earlier cleanup failed or timed out, using the same outer
deadline. No cleanup task is detached. Post-shutdown verification has a separate
five-second deadline, and temporary channel/pool closes also have two-second
limits. These are cooperative async deadlines: blocking code cannot be preempted.

A cleanup timeout is an error, not proof that a resource was deleted, a schedule
cancelled, or an already submitted server operation undone. Dropping a cleanup
future may leave its server-side outcome uncertain. Abrupt process termination
or cancellation of the entire example future cannot guarantee asynchronous cleanup.

```sh
export DATABASE_URL='postgres://USER:PASSWORD@127.0.0.1:PORT/isolated_database'
export RADDLE_RABBITMQ_TEST_URI='amqp://USER:PASSWORD@127.0.0.1:PORT/%2f'
RADDLE_APPLICATION_ALLOW_DISPOSABLE_SERVICES=yes \
  cargo run -p raddle-host --example application --locked

# SQLx creates an isolated database and applies migrations for this test.
cargo test -p raddle-host --test application --locked -- --include-ignored
```

Supply local fixture credentials through your own environment; never commit real
credentials. Neither command provisions cloud resources or modifies Service Bus.
The integration test's SQLx database requires the usual disposable-database
creation permissions. A failed order is not acknowledged as successful, and the
application does not replay earlier successful inputs to correct it.
