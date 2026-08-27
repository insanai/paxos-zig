# Changelog

## Unreleased

paxos-zig 0.6.0 and zaxonlite 0.6.0 (ZDS 0011). This is a breaking format
cut with no bridge: wire protocol 9, journal format 2, and the new durable
state anchor replace their predecessors, and artifacts from earlier
releases fail closed at open.

- Replace the bounded epoch with a single `u64` global slot line that is
  never reset. The core keeps a fixed slot-tagged consensus window
  (`window_slots`, a power of two) with a host-licensed memory floor;
  `max_slots` and the 2,044-commit rollover are gone, and with them the
  write-path cost proportional to database size.
- Add chunked leader recovery (`PromiseRange`) with the trimmed-acceptor
  fences: an elected leader never proposes at or below the maximum quorum
  trim anchor or chosen-through. Message and write capacities derive from
  the recovery chunk, not the log length.
- Add certified log trimming: replicas publish durable-state reports, the
  leader proposes the conservative trim `G = min A_i` over data replicas
  as an ordinary chosen entry, and segments wholly below the adopted trim
  are physically unlinked with payload garbage collection behind them.
- Replace the epoch journal with a segmented, manifest-governed journal
  (`consensus/`): first-slot-named segments, sealed trailers carrying a
  `max_promised` ballot rollup, rename-free rotation, and orphan sweeps.
  Replay folds the lifetime journal across configuration changes and
  window reuse.
- Add the alternating durable state anchor (`APPLIED.0/1`): recovery
  replays only the journal suffix above the anchor, so startup cost
  follows the anchor cadence instead of the whole history.
- Add the domain-separated global history hash `H_s`, folding each
  transaction batch's result chain, with per-slot recent marks for
  quorum vouching.
- Rewire decided voter replacement (ZDS 0008) onto global slots: the stop
  sign carries only the next registry digest and the replacement seed,
  survivors continue the same slot line in place, and the joining voter
  fetches the decided registry and catches up from the retained journal.
  Snapshot generations, `CURRENT` pointers, and per-configuration
  journals are gone.
- Add the anchor-pinned state transfer for gaps beyond journal retention:
  the sender pins a fresh anchor and a private image copy, and the
  receiver installs only after a read quorum vouches the anchor's history
  binding and the image digest matches. The stop-sign checkpoint proof
  and its quorum probe are removed.
- Rename the `snapshot` CLI verb to `anchor`; `zaxonlite_snapshot`
  becomes `zaxonlite_state_anchor` in the C ABI. Status output reports
  the global frontier fields (`durable_state_slot`, `memory_floor`,
  `chosen_trim_slot`, `retained_first_slot`, journal totals) instead of
  epoch capacity. Slot fields in the JSON status are `u64`; readers that
  parse them as IEEE doubles lose precision past 2^53.
- Extend the crash matrix over the anchor, trim, reclamation, and payload
  GC failpoints (15 cases), add the nightly long-run retention gate
  (segment rotation, bounded retention, anchored restart), and add the
  moving-window benchmark family (256 window wraps on one slot line).
- Model the design in `specs/GlobalTrim.tla`: the slot-tagged window,
  eviction licensing, trimmed-acceptor election fences, conservative
  trim, and the joiner lease lifecycle, with deliberate-bug validation.

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
