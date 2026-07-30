# Changelog

## Unreleased

## 0.2.0 - 2026-07-30

- Add the native `zxlite` Python SDK for CPython 3.12 and newer, including a
  SQLite-shaped DB-API 2.0 interface and a SQLAlchemy 2.x dialect.
- Add Python-hosted zaxonlite servers plus redundant multi-seed remote
  connections, consistency-aware concurrent reads, serialized writes, and
  typed hybrid search over the native client protocol.
- Add stable C APIs for embedded servers, remote connection pools, prepared
  values and result rows, batched execution, and typed search.
- Add replicated FTS5 and sqlite-vec hybrid search with bounded candidate
  collection, reciprocal-rank and distribution-based fusion, and Zig SIMD
  reranking.
- Add voter replacement, durable stop-sign recovery, and the host-managed
  durability boundary for grouped write barriers.
- Harden Linux and macOS connection shutdown, strict C11 C-ABI builds, and
  Python native linking under Zig 0.16.
- Publish release CLI archives, CPython Stable ABI wheels, checksums, and the
  `zxlite` package on PyPI from the tagged GitHub workflow.

## 0.1.2 - 2026-07-28

- Expose the decided stop slot (`ReplicatedLog.Node.stopSlot`) and a
  by-reference stop-sign accessor (`stopSign`), and latch the slot across
  restore, so hosts no longer hand-track where the seal landed.
- Add `ReplicatedLog.Node.pendingStopSign`: the undecided stop value
  retained in durable accepted state or leader proposals, so a host can
  repair its own durable operation phase after a crash.
- Export `paxos.version`; the benchmarks now print it instead of a
  hard-coded release string.
- Make `StopSign.create` public and extract `StopSign.validateMembers`,
  letting host wire decoders reuse the zero/duplicate member-ID checks.
- Add changed-member `initFromStop` unit coverage and a deterministic
  one-for-one voter-replacement simulation (`sim/reconfiguration.zig`):
  seeded leaders, a dropped and a duplicated seal accept, handover to the
  survivor set, and rejection of the removed voter.
- Add `specs/VoterReplacement.tla`, the bounded host-level model for the
  zaxonlite decided voter replacement built on this library (ZDS 0008).
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
