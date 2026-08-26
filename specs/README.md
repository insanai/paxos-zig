# TLA+ specification

`Paxos.tla` models the protocol implemented by `src/protocol.zig` at the
durable state's level of abstraction: `promised`, `accepted`, and
`committed` per node (the `DurableState` struct), plus a monotonic set of
messages. Because the message set only grows, loss, duplication, and
arbitrary reordering are all included in the model, and a crash-restart is
a no-op: nodes act only from durable state, which is exactly the property
the write-ahead host contract must deliver. In-memory state (role,
recovered votes, `next_slot`) is deliberately absent; safety must never
depend on it.

## Action-to-code mapping

| Spec action    | `src/protocol.zig`                                   |
| -------------- | ---------------------------------------------------- |
| `Prepare`      | `startCampaign` (phase 1a broadcast)                 |
| `Promise`      | `onPrepare` (promise + votes, including learned-only |
|                | decrees reported as zero-ballot votes)               |
| `Accept`       | `maybeBecomeLeader` / `propose` (B3 value selection) |
| `Vote`         | `onAccept` (durable vote)                            |
| `Decide`       | `onAccepted` (write-quorum commit)                   |
| `Learn`        | `onCommit` / `recordCommit`                          |

Checked invariants: `Agreement` (no two nodes commit different values for
one slot), `CommitUniqueness` (no two commit messages for one slot
disagree — the code's `ConflictingCommit` can only mean corruption),
`PromisedDominatesVotes` (the promise never trails a recorded vote —
`DurableState.assertValid`), and `Validity`. The simulator in
`sim/simulation.zig` checks the same properties on concrete executions.

## Running TLC

Requires Java. Releases of `tla2tools.jar` up to v1.6.x run on Java 8;
newer releases need Java 11+. From this directory:

```sh
curl -LO https://github.com/tlaplus/tlaplus/releases/download/v1.6.0/tla2tools.jar
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -deadlock -workers 4 Paxos.tla
```

`-deadlock` is required: the monotonic message set means every behavior
eventually stops producing new states, which TLC would otherwise report as
a deadlock.

`Paxos.cfg` bounds the model to three voters plus two non-voting learners,
one slot, one client value plus the no-op, one round per voter (node IDs order
ballots, so three concurrent competing ballots are still explored), and a `MessageBound`
state constraint of fourteen distinct messages — enough for one complete
ballot interleaved with a competing partial ballot, including stale votes,
learned-only decrees, and value recovery.

`LearnersDoNotVote` checks the product-critical separation used by Zaxonlite:
learners may record a commit, but cannot promise, accept, create a ballot, or
change `Quorums`, which is defined solely over `Voters`. Agreement ranges over
the union, so a learner is also forbidden from learning a conflicting value.

Verified 20 July 2026 with TLC 2.14 (tla2tools v1.6.0): no invariant
violation; 85,515,700 states generated, 3,986,355 distinct states, zero states
left on the queue, and a complete breadth-first search to depth 20 in 9 minutes
12 seconds. This is the voter-plus-learner fixture in the checked-in
`Paxos.cfg`, including `LearnersDoNotVote`.

Deeper configurations (two client values, `MaxRound = 2`, or a larger
`MessageBound`) grow past 10^8 states; partial runs of the unconstrained
two-value model explored over 200 million states at depth 21 with no
violation before being stopped. Use a modern ARM-native JDK with
tla2tools v1.8.0 for such runs; they are several times faster than
Java 8 under Rosetta. To model the flexible-quorum
configuration, replace `Quorums` with explicit read/write families whose
members pairwise intersect and use the read family in `Accept`'s
enablement and the write family in `Decide`.

This is a model of the design, not a mechanical refinement of the Zig
code; the conformance appendix in the book records the mapping and its
limits.

## VoterReplacement.tla

`VoterReplacement.tla` is the bounded host-level model for zaxonlite's
decided one-for-one voter replacement (ZDS 0008). It does not re-prove
consensus: the sealed configuration's unique stop-sign choice is
`Paxos.tla`'s job and appears here as the single atomic `ChooseStop`
action. What it checks is the host's durable activation discipline around
that choice. Durable variables survive a crash. The transport generation
does not. TLC explores crash points as action prefixes and requires
transport publication before next-configuration activation.

## Action-to-code mapping

| Spec action       | zaxonlite code                                        |
| ----------------- | ----------------------------------------------------- |
| `Prepare`         | durable `PENDING-OP` prepared record                  |
| `Submit`          | `ReplicatedLog.reconfigure` and durable effects       |
| `MarkProposed`    | pending-record crash repair / proposed record         |
| `ChooseStop`      | chosen stop sign and decided operation record         |
| `StoreBlob`       | `registry.storeBlob` in `completeRollover`            |
|                   | (deterministic reconstruction from zx2 metadata)      |
| `FetchBlob`       | `registry_request`/`registry_data` during install     |
| `InstallState`    | `completeRollover` CURRENT write / `installSnapshot`  |
| `FlipPointer`     | `registry.activatePointer` (the REGISTRY file)        |
| `AdvanceIdentity` | `writeIdentity`                                       |
| `SwapTransport`   | `Server.rebuildTransport` generation publication     |
| `Activate`        | `Node.activateRollover` / replacement activation      |

Checked invariants: `NothingBeforeChoice` (no durable next-configuration
state before consensus chose it), `PointerNeverDangles` (the REGISTRY
pointer names a stored, installed generation), `IdentityFollowsPointer`
(recovery never mixes configurations), `NoActivationBeforeInstall` (a
replacement cannot vote before durable verified installation),
`RemovedStaysSealed`, and `ReplacementFetchesFirst`.
`ChoiceAdvancesFences` binds the decided operation ring and node-ID fence.
`PendingPhaseIsSound` checks the durable submission phase: nothing is
chosen without a durable submission, and the chosen stop may outrun the
coordinator's phase file until `MarkProposed` repairs it on recovery.

Run the complete state-space check with:

```sh
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -deadlock -workers 4 \
    VoterReplacement.tla
```

Verified 28 July 2026 with TLC (tla2tools 2.14, Java 8): complete
search, 7,995 states generated, 600 distinct states, depth 23, no
violation, in about one second.

The invariants are not vacuous: deliberately removing the
CURRENT-before-REGISTRY guard from `FlipPointer` makes TLC report a
`PointerNeverDangles` violation immediately.

## GlobalTrim.tla

`GlobalTrim.tla` is the model for ZDS 0011: global 64-bit slots, the
tagged consensus window, certified log trimming, and the host frontiers
around them. It re-models Paxos shallowly (like `VoterReplacement.tla`
it does not re-prove the deep consensus results that are `Paxos.tla`'s
job) because six of its invariants need ballots, and it models the
window physically: each voter has `W` cells addressed by `slot % W`
with an explicit tag, so cell reuse and its guards are checked as
implemented rather than assumed away.

The voter set is fixed. The lease-protected `Joiner` models the
state-transfer half of a membership change: install at a leased base,
replay the suffix from any surviving node, and count toward the trim
minimum only after its first durable anchor. The activation discipline
of a voter-set change itself remains `VoterReplacement.tla`'s job, and
slot continuity across a stop sign is checked by
`sim/reconfiguration.zig`'s oracles.

## Action-to-code mapping

| Spec action           | Code                                                |
| --------------------- | --------------------------------------------------- |
| `Prepare` / `Promise` | `startCampaign` / `onPrepare` (PromiseV2: votes     |
|                       | above the anchor plus the `chosen_through` summary) |
| `Accept`              | leader range resolution: F = max quorum anchor,     |
|                       | K = max quorum `chosen_through`; never propose at   |
|                       | or below either fence                               |
| `Vote` / `Learn`      | `onAccept` / `recordCommit` tagged-cell install     |
|                       | under the eviction guard (`CanInstall`)             |
| `Decide`              | `onAccepted` write-quorum choice                    |
| `AdvanceChosen`       | `emitContiguous` / `decidedThrough`                 |
| `AdvanceFloor`        | `advanceMemoryFloor`                                |
| `ExecuteNext`         | `applyBatchOffline` and anchor-suffix replay        |
| `AnchorState`         | `applied_anchor.publish`                            |
| `CrashImage`          | power loss before the next APPLIED barrier          |
| `ChooseTrim`          | the chosen `Command.trim` entry (`G = min A_i`      |
|                       | over counted data replicas, `trim.candidate`)       |
| `InstallTrim`         | `installChosenTrim` plus the durable TRIM record    |
| `LocalDelete`         | segment unlink under `trim.deleteFloor`             |
| `CreateLease` /       | transfer-lease lifecycle: chosen lease entry,       |
| `InstallJoiner` /     | pinned image install at the base slot, completion   |
| `CompleteLease`       | only after the receiver's anchor reaches `G`        |

Checked invariants: `Agreement` and `ChosenPrefix` (no commit ever
contradicts the first decision; claimed prefixes are chosen),
`TagNonAliasing` (a tagged cell only sits in its own physical index),
`TrimNeverExceedsCertifiedAnchor`, `AppliedNeverExceedsDurablePages`,
`AcceptedOnlySlotNeverEvicted` (eviction never erases an open phase-two
obligation), `PromiseRangeStartsAboveAnchor`,
`LeaderNeverProposesAtOrBelowAnchor` (every accept is stamped with the
fence its quorum reported and stays strictly above it),
`LocalDeleteNeverExceedsDurableState`, `TransferLeasePreservesSuffix`,
`RecoveryReadyImpliesExactPrefix`, `NoWitnessClaimsMaterializedState`,
`FrontierOrder` (the ZDS 0011 frontier chain), and
`PromisedDominatesVotes`. The `Monotone` action property is
`GlobalSlotNeverDecreases`: decisions are immutable and every durable
frontier is monotone, with the materialized image exempt because rolling
back to the anchor at a crash is exactly what the anchor makes safe.

Run the two checked-in configurations with:

```sh
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -deadlock -workers 4 \
    -config GlobalTrim.cfg GlobalTrim.tla
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -deadlock -workers 4 \
    -config GlobalTrim5.cfg GlobalTrim.tla
```

`MaxMessages` bounds the monotonic message set; sixteen covers a full
ballot over several slots, a competing ballot after a trim, and the
catch-up traffic the window forces, and `MaxSlot = 3` with `W = 2`
still forces physical cell reuse (slot 3 evicts slot 1). Deeper partial
runs support the bounded result: `MaxSlot = 4` at bound eighteen
explored 963 million states to depth 14, and at bound twenty-two 210
million states to depth 13, with no violation before being stopped.

The invariants are not vacuous: removing the eviction guard's
chosen-and-below-floor condition makes TLC report
`AcceptedOnlySlotNeverEvicted` in an eight-state trace, and removing
the leader's K fence (proposing into a slot every quorum member had
chosen and evicted) reports `LeaderNeverProposesAtOrBelowAnchor` in a
twelve-state trace, both at `W = 1`, `MaxSlot = 2`.
