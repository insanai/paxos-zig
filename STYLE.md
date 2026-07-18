# Paxos Zig coding standard

This project follows TigerStyle and the following reading principles:

> Beautiful is better than ugly.
> Explicit is better than implicit.
> Simple is better than complex.
> Complex is better than complicated.
> Flat is better than nested.
> Sparse is better than dense.
> Readability counts.

These are engineering constraints. They do not excuse a missing invariant or an
unbounded operation. Safety comes first, then performance, then convenience.

## Required checks

Run these commands before submitting a change:

```sh
zig build fmt
zig build test
zig build run
zig build benchmark
zig build book
```

`zig build fmt` runs `zig fmt --check` and the project style checker. The style
checker rejects tabs, Zig source lines above 100 columns, and functions above 70
lines. The two generic type factories are exempt because Zig represents their
namespaces as functions. Their nested functions are still checked.

## Control flow

- Keep control flow explicit and shallow.
- Return early when a precondition fails.
- Split a compound condition when its parts protect different invariants.
- Do not use recursion in protocol or storage paths.
- Bound every loop with a compile time capacity or a validated input length.
- Do not hide protocol transitions in callbacks, reflection, or generic magic.
- Use one explicit switch for message dispatch.

## Memory

- The consensus library does not allocate after initialization.
- Capacities are compile time options and are validated.
- Initialize large values through destination pointers.
- Do not return a large node by value.
- Use fixed width integers for protocol identities, rounds, slots, and counts.
- Use `usize` only for Zig array and slice indexing.
- Values stored in messages must own their data or contain durable identifiers.

## State and invariants

- State the invariant before implementing its transition.
- Assert important preconditions and postconditions in safe builds.
- Pair an assertion before and after a buffer count mutation.
- Durable state never moves backward.
- One ballot and slot never carry two values.
- A committed slot never changes value.
- A later leader preserves the highest accepted value reported by phase one.
- Quorum configuration is valid only when read and write quorums intersect.
- Every message that depends on a write is sent only after that write is synced.

## Functions and names

- A function should do one protocol action.
- Keep functions at or below 70 lines.
- Keep source lines at or below 100 columns.
- Use names from the protocol: `promised`, `accepted`, `committed`, and `ballot`.
- Include units or domains in names when confusion is possible.
- Avoid abbreviations except established protocol terms.
- Put helper functions after the public operation that motivates them.

## Errors

- Handle every error.
- Return an error for invalid input or exhausted bounded capacity.
- Use assertions for internal states that valid callers cannot produce.
- Do not continue after durable storage fails.
- Do not use `catch unreachable` in library code.
- Do not use panics as a normal protocol response.

## Comments and documentation

- Comments explain why a rule exists, not what the next statement says.
- Public methods document ordering, durability, bounds, and ownership.
- Examples must show write before send.
- Documentation must distinguish safety from liveness.
- Unsupported behavior is stated directly. It is not hidden behind broad claims.

## Tests and measurements

- Every protocol bug receives a deterministic failure schedule test.
- Test duplicates, loss, reordering, restart, stale ballots, and bounded capacity.
- Test one voter, the smallest quorum, and the configured maximum.
- A benchmark validates its checksum and protocol message count.
- Cross language measurements pin dependency versions and disclose differences.
- Performance claims use observed numbers and never infer language superiority.

## Review order

Review a change in this order:

1. The invariant remains true.
2. Crash recovery preserves the invariant.
3. Work and memory remain bounded.
4. The public contract remains explicit.
5. Names and control flow remain readable.
6. Tests exercise the dangerous schedule.
7. Measurements support any performance claim.
