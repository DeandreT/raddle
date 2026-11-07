# Typed application pipelines

The core crate assembles ordinary Rust functions into ordered asynchronous
pipelines. Registration is explicit; there is no discovery, reflection, code
generation, dependency-injection container, or hidden transport dispatch.

## Middleware and validation

`Pipeline<M, R, E>` retains its message, reply, and domain error types. Construct
it with a terminal async function receiving the owned message and an
`ExecutionContext`. Add `Middleware<M, R, E>` layers, then register it with
`RuntimeBuilder::handle_pipeline`. Event pipelines use
`Pipeline<EventDelivery<E>, (), E::Error>` and `subscribe_pipeline`.

```rust
use raddle::pipeline::Pipeline;
use raddle::{Request, RuntimeBuilder};

struct Count(u32);
#[derive(Debug, thiserror::Error)]
#[error("count must be positive")]
struct InvalidCount;
impl Request for Count {
    type Reply = u32;
    type Error = InvalidCount;
}

async fn example() -> Result<(), Box<dyn std::error::Error>> {
let pipeline = Pipeline::new(|message: Count, _context| async move {
    Ok(message.0 * 2)
}).validate(|message: Count, _context| async move {
    if message.0 == 0 { Err(InvalidCount) } else { Ok(message) }
});
let mut builder = RuntimeBuilder::try_new(1)?;
builder.handle_pipeline(pipeline)?;
assert_eq!(builder.build().invoke(Count(21)).await?, 42);
Ok(())
}
```

Layers run in addition order on entry and reverse order on return. For A then B,
the order is A-entry, B-entry, terminal, B-exit, A-exit. A layer receives a
single-use `Next`; `next.run(message, context).await` consumes it. A layer can
short-circuit with a typed success or error, normalize the owned input, or
inspect/transform the downstream result. Implement the middleware trait for
reusable stateful layers, or use `layer_fn` with a higher-ranked closure returning
a boxed future. There is no built-in replayable continuation.

Async validators return `Result<M, E>`. Returning the owned message avoids
requiring `Clone` or `Sync` for requests; validation can normalize it without
copying. An error skips all remaining inner layers and the terminal. Outer
middleware still observes that error. A rejected event subscriber does not
cancel other subscriptions.

The **entire registered pipeline**, including validation and post-processing,
holds one shared runtime admission permit. Direct `Pipeline::execute` does not
acquire a runtime permit. Dropping either execution future drops unfinished
middleware and handler futures. Registered execution releases its permit,
including when canceled during validation. Closing admission prevents waiting
pipelines from starting validation. Cancellation cannot reverse earlier side
effects. Middleware must not recursively dispatch into the same saturated
runtime; return cascading effects instead.

`ExecutionContext` is immutable local diagnostic identity, not an authorization
principal or a trusted interpretation of incoming headers. Root executions get
fresh execution/correlation IDs. Cascade children have distinct execution IDs,
retain correlation, and record parent identity and depth. Event subscribers
share their publication's context and are distinguished by subscription name.
Use captured typed application state for authorization or services. Ordinary
handlers can opt into `handle_with_context` or `subscribe_with_context` without
using a pipeline.

## Cascading commands and event results

`Cascade<R>` combines a typed successful reply with owned pending `Effects`.
Each batch has a positive finite capacity (64 by default). `push` returns the
unaccepted effect to the caller when full. The application must handle that
failure; no accepted work is silently evicted.

```rust
use std::convert::Infallible;
use raddle::cascade::{Cascade, CascadeLimits, Effect, Effects};
use raddle::{Request, RuntimeBuilder};

struct Save;
impl Request for Save { type Reply = Cascade<u32>; type Error = Infallible; }
struct Audit(u32);
impl Request for Audit { type Reply = (); type Error = Infallible; }

async fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut builder = RuntimeBuilder::try_new(1)?;
builder.handle(|_: Save| async {
    let mut effects = Effects::default();
    // A real handler should map a full batch to its own typed domain error.
    assert!(effects.push(Effect::command(Audit(42))).is_ok());
    Ok(Cascade::new(42, effects))
})?;
builder.handle(|message: Audit| async move { let _id = message.0; Ok(()) })?;
let report = builder.build().invoke_cascade(Save, CascadeLimits::default()).await?;
assert_eq!(*report.reply(), 42);
assert!(report.is_success());
Ok(())
}
```

An ordinary `invoke` still returns exactly its declared typed reply. If that is
`Cascade<R>`, **it does not execute the returned effects**. `invoke_cascade`
explicitly opts into draining them. `Runtime::cascade` runs an already assembled
batch without a root handler.

Effect constructors distinguish their behavior:

- `Effect::command(request)` retains an ordinary typed reply.
- `Effect::cascading(request)` unpacks a `Cascade<R>` reply and enqueues its
  returned effects.
- `Effect::event(event)` retains the complete typed subscriber report.
- `Effect::event_then(event, continuation)` derives further effects from a fully
  successful typed publication report. The continuation never runs on partial
  failure. Subscribers remain ordinary typed handlers; there is no implicit
  interpretation of their return values.

The runner is sequential and breadth-first. It waits for a command or all event
subscribers to finish and **release admission before enqueuing children**. A
concurrency limit of one therefore works without recursive runtime entry.
Independent caller executions still share the original runtime limit. Children
preserve production order; returned outcomes use admission sequence even when
an effect is rejected immediately.

`CascadeLimits` bounds total admitted effects and depth from the root (defaults:
256 effects and 16 edges). The root is not counted. Rejected work gets an
explicit `CascadeLimit::Effects` or `Depth` outcome and is not invoked. The
rejected outcome retains ownership of the never-started effect for explicit
recovery, without replaying successful siblings. Each returned batch is itself
bounded; the outcome list includes rejected effects
and can therefore be larger than the admission budget. A cycle terminates at
these bounds, not through Rust call-stack recursion.

Every outcome retains sequence, parent sequence, context, diagnostic message
type, and `EffectResult`. Successful values can be downcast to the concrete
reply or `PublishReport<E::Error>`. Command failures retain the actual
`ExecutionError<M::Error>`. Partial event failures retain the **entire** typed
subscriber report, including successful siblings. Failures do not cancel
unrelated effects, roll back a successful root, or discard later work. Failed
effects produce no children. No effect or successful subscriber is retried
automatically. To correct a failed order, submit only that corrected order;
rerunning an entire batch can duplicate its earlier side effects.

Execution is volatile and caller-owned. Cancellation drops the current effect
and queued work, with no resumable receipt; already completed effects remain
completed. Panics unwind rather than being converted into domain failures. Use
the persistence adapters when effect intent must survive a crash. Local
cascades do not provide transactions, durable queues, or exactly-once effects.

## Owned retry decisions

`retry::retry_owned` is separate from runtime dispatch. It moves application
state into each attempt, rather than cloning and replaying requests or
continuations. Return `Attempt::Complete(value)` on success,
`Attempt::Failed(error)` for terminal failure, or explicitly return
`Attempt::Retry { state, error, safety, after }` to authorize another attempt.

Each retry requires an application assertion of `RetrySafety::RolledBack` or
`RetrySafety::Idempotent`. Return rollback state only after the failed operation's
effects really have been rolled back. An external operation with an uncertain
outcome requires an actual idempotency boundary and retained identity; merely
returning owned state does not make replay safe. The framework cannot verify
these assertions and does not retry arbitrary handler side effects.

`RetryPolicy` bounds attempts (including the first attempt) and each delay. Its
default allows only one attempt. The final report distinguishes terminal
failure, exhausted attempts, and a requested delay exceeding policy, preserving
the final domain error. Successful attempts never run again. Delays are
caller-polled Tokio timers; dropping the future cancels either the delay or the
in-progress attempt without spawning a worker. An application that needs a
wall-clock deadline can wrap this future in its own timeout.

## Typed routes to configured aliases

`routing::WireMessageType::of::<M>()` validates the actual `WireMessage` name and
positive schema version. `RouteTable` keys routes by that stable identity, never
Rust type names. Versions match exactly. `EndpointAlias` accepts a bounded
symbolic name, not URI syntax. Applications first admit adapter-configured
aliases with `endpoint`, then bind wire types with `route::<M>(&alias)`.
`resolve::<M>()` returns those aliases in registration order. Unknown aliases,
invalid contracts, duplicate bindings, missing routes, and exhausted endpoint,
binding, or fan-out budgets have distinct errors. Failed registration leaves
configuration unchanged.

The table never reads an inbound URL/header or creates a destination. The
application and adapter still own alias-to-endpoint resolution, authentication,
serialization, and confirmed sending; admitting an alias is an explicit
application assertion that its adapter configuration exists. Resolved aliases
are not proof of remote authorization or delivery. Keep route configuration
application-owned and share it immutably once assembled.
