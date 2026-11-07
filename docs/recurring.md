# Recurring schedules and typed saga transitions

## Persistent intervals

`RecurringSchedule::new` accepts a validated `ScheduleKey` (tenant, source, name),
first occurrence, interval, explicit missed-tick policy, typed `WireMessage`, and
delivery routes. Intervals are whole microseconds from 1ms through 365 days;
first-occurrence timestamps are truncated to PostgreSQL microsecond precision.
The typed payload is persisted at registration, not recreated by a process-local
callback after restart. Delivery availability becomes the original occurrence
time; expiry, partition, destination, and attempt settings remain as configured.

Register through `Scope::register_recurring` and commit it with application writes.
Equivalent registration preserves progress and cancellation; different settings
under the same identity conflict. `update_recurring` explicitly replaces an
existing definition, resets its first occurrence, and reactivates it. Each
replacement has a new incarnation, so it cannot collide with earlier emitted
occurrences. `cancel_recurring` prevents future dispatch but does not retract
jobs already committed. Cancellation and replacement serialize against dispatch
using the schedule row lock. No helper uses manual transaction control.
Dropping a pending registration, replacement, or cancellation future marks its
borrowed scope rollback-only: cancelling an await cannot leave partially accepted
SQL available for a later application commit.

`RecurringScheduler::run_once(schedule_limit, occurrence_limit)` obtains time from
PostgreSQL's real `clock_timestamp()`. `run_at` instead accepts an explicit
authoritative time; use it only when the caller owns clock consistency, or in
deterministic tests. Neither API starts a task or polling loop.

- `SkipMissed` emits the persisted due occurrence and advances directly to the
  first interval after the page's current time, dropping intervening missed ticks.
  Keeping that original occurrence makes failed/crashed attempts stable even if
  time advances before retry. It uses arithmetic, not a loop over missed timestamps.
- `CatchUp { max_occurrences }` emits oldest due occurrences up to its configured
  bound (1 through 256), leaving the remaining backlog for later pages.
- A page visits at most 256 schedules and commits at most 1024 occurrences.
  Destination fan-out is separately limited to 256 per occurrence. Every
  schedule is attempted only once per page, including failed schedules.

Each schedule has its own transaction, selected with `FOR UPDATE SKIP LOCKED`.
Outgoing jobs pass through `Scope::stage`, retaining its tenant quotas,
idempotency checks, and routing validation. Dispatch and advancement commit
together. A failure rolls back all occurrences for that schedule, records a
bounded error classification and a 30-second retry delay, and allows unrelated
schedules and tenants to continue. The returned `SchedulePage` includes these
failures; applications must monitor them rather than treating a successful page
call as proof every schedule succeeded. A database-wide connection/commit failure
can still return an error. Committed schedule transactions remain committed.

Message identity is derived from the persisted incarnation and original
occurrence time; creation and availability timestamps also retain that original
time. A rollback/crash before commit leaves the same occurrence and identity for
retry. A crash after commit leaves both the outgoing jobs and advanced due time.
Timestamp arithmetic and configuration bounds fail closed. Interval scheduling
is not cron, calendar-month, timezone, or exactly-once external execution.

## Typed saga dispatcher

`Worker::handle_saga::<Message, State, _, _>` registers a typed transition with
the usual subscription and `Codecs`, a selector returning `CorrelationKey`, an
explicit `SagaInitialization`, and an asynchronous transition. Tenant identity is
always obtained from the worker's authenticated `Scope::tenant()`, never a
payload, header, selector, or correlation field.

`SagaInitialization::Start(|| initial_state)` creates missing state only.
`ExistingOnly` acknowledges an unknown correlation as a no-op. Both policies
acknowledge completed tombstones without decoding old state or invoking business
code; neither restarts a finished saga. The selector must validate its business
correlation through `CorrelationKey::new` and choose a retry or reject disposition
when it cannot do so.

The transition receives the decoded message, mutable locked `Saga<State>`, and
the existing worker scope. It may stage outgoing messages, perform application
SQL, and schedule/cancel timeouts. Return an error to roll back all these effects;
the helper also marks the scope rollback-only. The helper saves final state after
a successful transition, while the worker commits state, effects, and inbox
completion together. It opens no hidden transaction. Scheduling a timeout saves
an intermediate revision; do not manually save a terminal tombstone inside the
transition, because final persistence belongs to the helper. Use `saga.finish()`
to request terminal persistence. As with other worker handlers, do not perform
irreversible external effects inside the transaction.

Timeout handlers must still call `Saga::accepts_timeout` before acting, and route
the reserved timeout contract through trusted custody. Registration is not an
authorization or timeout-generation check.
