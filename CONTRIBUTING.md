# Contributing

## Design and style

Follow [Google's Rust style guide](https://google.github.io/styleguide/rustguide.html)
and the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/).
Chromium- and Fuchsia-specific runtime, build-system, and layout rules do not
apply to this Cargo/Tokio project.

- Prefer ownership and borrowing; use shared ownership where independent async
  execution genuinely needs it. Do not clone data merely to bypass borrowing.
- Prefer ordinary functions and types over macros or generated APIs.
- Introduce traits for actual generic or adapter boundaries, not to organize code.
- Use typed errors, meaningful newtypes, descriptive lifetimes, and explicit
  delivery policies instead of ambiguous boolean parameters.
- Keep implementation in focused leaf modules. Non-leaf modules primarily
  declare modules and re-export their public API.
- Import types directly and qualify free functions through their modules. Avoid
  glob imports and preludes. Group imports by the module that defines them.
- Document observable behavior, error cases, cancellation, and invariants. Public
  enums that may grow are non-exhaustive.
- Use stable Rust, rustfmt, and safe code. Production code must not use `unwrap`
  or `expect` for recoverable failures.
- Test defaults and feature combinations without assuming optional runtime or
  transport features are enabled by dependency feature unification.

## Delivery

Use focused issues and conventional commit subjects. Pull request descriptions
summarize implemented changes and link their issues. Keep qualification results
in issue updates rather than pull request descriptions.

Run the local checks in the README on the exact commit being merged. Merge with
an exact-head guard after the relevant checks pass; do not wait for hosted CI.
Do not add standalone CI pipeline definitions. An unqualified feature must not
be described as production ready.

Test Service Bus and Switchyard independently. Search for an existing upstream
issue before reporting a Switchyard defect. Include a minimal sanitized
reproduction, expected/actual protocol behavior, and the exact tested revision.
Never commit credentials, real customer payloads, or private reference material.

Contributions are licensed under the repository's MIT license.
