# Native handler composition and sticky routing

`raddle-messaging::handler_routing::HandlerSet` opts into native multi-handler
dispatch on the application bus. It does not change ordinary `Runtime` request
registration, event fan-out, or `MessageBus::invoke`'s local-first behavior.

Build a typed chain with `composition::Group::new("inventory", inventory)` and
`Then::new(first, second)`. Group IDs are explicit stable subscription-compatible
names, not inferred CLR classes or Rust type paths. One group owns one native
`ScopedHandler`, including its Before/Load/Validate/Handle/After/PostProcess
methods or saga methods. Duplicate IDs are rejected. Registration supports up to
16 groups and the existing bus limits on endpoints and routes.

## Classic is the default

`HandlerSet::<Order, _>::new(chain).register(&mut builder, "orders", store, flush)`
registers an actual route and a selected scoped handler. If `orders` does not
exist it creates a bounded buffered local queue. The returned `owners.local`
contains its sending/control handle and sole `QueueRunner`; the application
attaches that runner to `Host`. Nothing starts during registration. An existing
local queue retains its own capacity and parallelism settings.

Every selected group runs in declaration order under **one** SQL scope, with
no member commit. Each group's create, pre-commit phases and inline effects
finish before entering the next group. Its original associated Resources are
used for its effects, not a shared untyped container. Outgoing intents are staged
only after every selected group's pre-commit work succeeds. A later failure
rolls back earlier enrolled SQL and all outgoing intents. External effects are
still not reversible. The combined outgoing budget is 256; overflow fails the
root rather than dropping excess intent.

Resources are a typed recursive tuple of optional member scopes. A group is
constructed only when entered; cancellation while constructing a later group
still leaves earlier scopes owned by the root. Each constructed group receives
Finally and async disposal, in reverse order, even if another group's cleanup
fails or panics. Partial construction remains the individual factory's duty.
AfterCommit runs in forward order after successful flush; one callback failure
does not skip other groups' callbacks. A flush, callback or cleanup failure after
commit does not retry the root.

Member preparation failures preserve the member's concrete `Failure` inside
`CompositionFailure`/`ChainError`. OnException is called on the failing member
after rollback. Cancellation/deadline/panic signals reach every constructed
group. Root staging/commit errors have no invented member owner: all constructed
groups see Failed in Finally, but their OnException callbacks are not supplied
a fabricated member error. Inspect the returned root failure for reconciliation.

`builder.combined_handlers()` returns structured startup diagnostics for selected
endpoint chains containing more than one distinct group. Log these explicitly,
or call `builder.assert_no_combined_handlers()?` before `build()` to make them a
startup error. Multiple phases of one group do not trigger it. No logging service
or hidden startup task is installed.

In classic mode the returned `global` registration permits an explicit typed
scoped invocation of the unassigned chain via `invoke_scoped`; replies are nested
typed tuples containing None for unselected members. Separated and all-sticky
sets return None, not an implicit combined execution. Ordinary Invoke stays on
the pre-existing Runtime/global request registration.
Configure the bus's separate managed `execution_queue` and attach its runner
before using `invoke_scoped`; endpoint registration does not start an invocation
service behind the application's back.

## Separated durable subscriptions

Choose `.behavior(MultipleHandlerBehavior::Separated)` to create a selected
endpoint per unassigned group ID, instead of a combined default queue. Choose
`.custody(Custody::Durable)` for absent non-sticky endpoints to use PostgreSQL
subscriptions. Each returned `owners.durable` entry is a configured
`DurableEndpoint`; bind the frozen bus using `message_bus` if contextual returned
routing is needed, then attach it to Host. Do not register another receiving
owner for the same alias. When adopting an existing durable bus alias, register
through this API before starting any receiver. Repeated owner selection is
rejected rather than silently creating competing receivers.

Send/Publish performs real independent admissions to these subscriptions.
Successful inbox completion persists; a failed sibling retries alone. A native
handler using `Scope::saga`/`save_saga` participates in the same way. There is no
automatic replay of the already committed root or successful saga subscription.
Independent memory queues are volatile and **do not** provide persisted retries.

`endpoint_options(name, options)` sets only that selected chain's deadline and,
for durable endpoints, receiving parallelism, retry budget and typed failure
classifier. A durable failure is settled by the existing endpoint worker; its
policy never replaces another endpoint's policy or ordinary Invoke's handler.
An absent endpoint timeout override retains the bus's configured per-message or
global budget rather than silently replacing it with a new default. Configure
these budgets before registration: durable handlers capture their settings when
their receiving owner is constructed. The default classifier rejects invalid
typed wire input and retries other root failures, like `DurableEndpoint::handle`.
For memory queues a root failure is counted, not automatically retried, and a
failure classifier/retry budget has no receiving-store settlement to control.

## Sticky selections, leftovers and pre-built overrides

`.sticky("inventory", "a")?` assigns only that group to endpoint A (alias `a`).
Aliases use the existing lowercase subscription-name rules. The existing named
local or durable endpoint wins; a **missing sticky name always creates a real
bounded local queue and its route**, even when the set's default custody is
durable. The application receives and must attach the runner. A producer-only
remote alias is rejected because it cannot own native handler execution.

Only the selected sticky groups run there, not all global handlers for the
message. Unassigned leftovers run on the default endpoint and other endpoints
already routed for that contract at registration time. Configure those routes
before registering the set; this is frozen startup composition, not runtime
discovery. Sticky selections and leftover target aliases cannot collide.
Existing outgoing-only remote routes are preserved without attaching a local
handler or creating a broker listener for them.

`builder.sticky_handler::<Order, _>("a", override, store, flush, options)` is the
pre-built profile. Register endpoint overrides **before** the global HandlerSet;
the global registration leaves those explicit endpoints intact. Endpoint A uses
only its override and its own options; B/default use the global selected chain,
and ordinary `MessageBus::invoke` uses the unchanged Runtime request handler.
Missing A creates its actual queue/route and returns its owner. Late duplicate
handler registration is an error, not an in-place replacement of a frozen chain.

Registration works on a configuration snapshot and applies it only after all
routes/handlers validate. Keep the returned owners; dropping a missing queue's
runner closes that receiver instead of creating an invisible background owner.
Use the same Store for an adopted durable alias and the global registration;
there is no cross-store transaction.

## Example and scope

The runnable `handler_dispatch` example sends an order through the bus and
host-owned queue and verifies that two SQL inserts share one transaction:

```sh
RADDLE_EXAMPLE_CONFIRM=isolated DATABASE_URL=postgres://... \
  cargo run -p raddle-messaging --example handler_dispatch
```

Use only a dedicated disposable database. The example applies framework
migrations and creates its namespaced business table; rows remain for inspection.

This native profile follows the public handler contracts in Wolverine V6.62.0,
commit `3653e7a0d8f5eaa2f35b9d4589c1ba22f441549e`, specifically
[multiple handlers](https://github.com/JasperFx/wolverine/blob/3653e7a0d8f5eaa2f35b9d4589c1ba22f441549e/docs/guide/handlers/index.md)
and [sticky handlers](https://github.com/JasperFx/wolverine/blob/3653e7a0d8f5eaa2f35b9d4589c1ba22f441549e/docs/guide/handlers/sticky.md).
It does not implement CLR discovery, counterpart wire compatibility, broker
conventional listener naming, or automatic broker/cloud topology provisioning.
Those wider RT05/RT06 obligations are not established by local native dispatch.
