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
