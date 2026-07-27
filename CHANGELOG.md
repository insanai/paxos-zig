# Changelog

## 0.1.1 - 2026-07-27

- Enforce the persist-then-send effect order in every optimize mode; a
  violation now stops the process with a stable diagnostic
  (`paxos: messagesSlice before confirmWritesDurable`,
  `paxos: reset discarded unconfirmed writes`).
- Remove the `assert_effect_order` option; the enforced check has no opt-out
  in normal `Options`.
- Add the `paxos.host_managed` namespace for audited hosts that own the
  durability boundary themselves (grouped write barriers). The durable
  benchmark is its first consumer.
- Replace compile-time option assertions with `@compileError` diagnostics
  across `Protocol`, `ReplicatedLog`, `Learner`, and the internal bit set,
  and reject derived effect capacities that overflow `usize`.
- Add `zig build test-misuse` (effect-order misuse fixtures run as child
  processes in Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall) and
  `zig build test-compile-errors` (compile-fail option fixtures); both run
  under `zig build test`, which now also drives the path-dependency
  integration consumer.

## 0.1.0 - 2026-07-18

- Implement bounded classic Paxos and stable-leader Multi-Paxos.
- Add reorder-safe phase-one recovery, learning, catch-up, and durable deltas.
- Add logical election ticks, priorities, heartbeats, resend, and reconnect repair.
- Add flexible quorums, compact vote sets, batch proposal, and decided reads.
- Add stop-sign reconfiguration and bounded checkpoint epochs.
- Add an in-memory replicated-counter example.
- Add locked benchmarks against OmniPaxos 0.2.2 and LibPaxos3 C.
- Add in-place initialization and explicit invariant checks following TigerStyle.
- Add the 97-page *Part Time Parliament* Typst book and Zig package metadata.
