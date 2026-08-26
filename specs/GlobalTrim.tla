----------------------------- MODULE GlobalTrim -----------------------------
(***************************************************************************)
(* Windowed Multi-Paxos with global u64 slots and certified log trimming   *)
(* as specified by ZDS 0011 and implemented by src/protocol.zig plus the   *)
(* zaxonlite host frontiers.  The model checks the properties the epoch    *)
(* rollover used to provide by construction: that reusing a physical       *)
(* window cell, deleting a journal prefix, and anchoring materialized      *)
(* state never lets a leader choose a second value for an old slot.        *)
(*                                                                         *)
(* Per voter, `promised` and the tagged `cells` window correspond to the   *)
(* core's DurableState (the journal replay result); `anchor` is the        *)
(* installed chosen-trim anchor, `chosenThrough` the contiguous chosen     *)
(* prefix C_i, `floor` the memory floor M_i, and `deleted` the local       *)
(* delete floor T_i.  Data replicas additionally track `executed` (E_i,    *)
(* the open SQLite image, volatile) and `applied` (A_i, the durable        *)
(* APPLIED anchor).  The message set is monotonic, so loss, duplication,   *)
(* and reordering are included and a crash-restart of consensus state is   *)
(* a no-op; the one genuinely volatile thing, the materialized image, has  *)
(* its own crash action that rolls `executed` back to `applied`.           *)
(*                                                                         *)
(* Membership scope: the voter set is fixed.  The lease-protected joiner   *)
(* models the state-transfer half of a membership change (install at a     *)
(* leased base, replay the suffix, be counted for trimming only after its  *)
(* first anchor); the activation discipline of a voter-set change itself   *)
(* is already checked by VoterReplacement.tla, and slot continuity across  *)
(* a stop sign is checked by sim/reconfiguration.zig's oracles.            *)
(*                                                                         *)
(* Action mapping to code (see specs/README.md for the full table):       *)
(*   Prepare / Promise    <-> startCampaign / onPrepare (PromiseV2: votes  *)
(*                            above the anchor, chosen_through summary)    *)
(*   Accept               <-> range resolution: F = max quorum anchor,     *)
(*                            K = max quorum chosen_through; never propose *)
(*                            at or below either                           *)
(*   Vote / Learn         <-> onAccept / recordCommit with tagged-cell     *)
(*                            install and the eviction guard               *)
(*   AdvanceFloor         <-> advanceMemoryFloor                           *)
(*   ExecuteNext          <-> applyBatchOffline / replay (Phase A)         *)
(*   AnchorState          <-> applied_anchor.publish                       *)
(*   CrashImage           <-> power loss before the next APPLIED barrier   *)
(*   ChooseTrim           <-> the chosen Trim log entry (G = min A_i)      *)
(*   InstallTrim          <-> installChosenTrim + durable TRIM record      *)
(*   LocalDelete          <-> journal.trimThrough under the T_i formula    *)
(*   CreateLease/Install/CompleteLease <-> transfer lease lifecycle        *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS DataVoters, Witnesses, Joiner, W, MaxSlot, MaxRound, Values,
          Noop, None, MaxMessages

Voters == DataVoters \cup Witnesses

DataNodes == DataVoters \cup {Joiner}

(* Witnesses carry the state variables so the model can prove they never  *)
(* materialize, rather than excluding them by construction.                *)
AnchoredNodes == DataNodes \cup Witnesses

ASSUME /\ DataVoters \cap Witnesses = {}
       /\ Joiner \notin Voters
       /\ W > 0
       /\ MaxSlot >= 1

Slots == 1..MaxSlot

AllValues == Values \cup {Noop}

ZeroBallot == <<0, 0>>

Ballots == (1..MaxRound) \X Voters

BallotLt(a, b) == \/ a[1] < b[1]
                  \/ /\ a[1] = b[1]
                     /\ a[2] < b[2]

BallotLe(a, b) == BallotLt(a, b) \/ a = b

Quorums == {S \in SUBSET Voters : 2 * Cardinality(S) > Cardinality(Voters)}

(* Physical cell index: j(s) = s & (W - 1) in the implementation.          *)
Idx(s) == s % W

EmptyCell == [tag |-> 0, vbal |-> ZeroBallot, vval |-> None, chosen |-> None]

SetMax(S) == CHOOSE x \in S : \A y \in S : y <= x

SetMin(S) == CHOOSE x \in S : \A y \in S : x <= y

VARIABLES promised, cells, chosenThrough, anchor, floor, deleted,
          executed, applied, G, leases, joinerCounted, chosenLog, msgs

vars == <<promised, cells, chosenThrough, anchor, floor, deleted,
          executed, applied, G, leases, joinerCounted, chosenLog, msgs>>

Init ==
  /\ promised = [n \in Voters |-> ZeroBallot]
  /\ cells = [n \in Voters |-> [j \in 0..(W - 1) |-> EmptyCell]]
  /\ chosenThrough = [n \in Voters |-> 0]
  /\ anchor = [n \in Voters |-> 0]
  /\ floor = [n \in Voters |-> 0]
  /\ deleted = [n \in Voters |-> 0]
  /\ executed = [n \in AnchoredNodes |-> 0]
  /\ applied = [n \in AnchoredNodes |-> 0]
  /\ G = 0
  /\ leases = {}
  /\ joinerCounted = FALSE
  /\ chosenLog = [s \in Slots |-> None]
  /\ msgs = {}

Send(m) == msgs' = msgs \cup {m}

(* A cell may be installed for slot s when it is empty, already tagged s,  *)
(* or holds an older slot that is chosen and at or below the memory floor  *)
(* (the eviction rule).  An accepted-only occupant is never evicted; the   *)
(* incoming message is dropped and recovered by resend after the floor     *)
(* advances.                                                               *)
CanInstall(c, s, fl) ==
  \/ c.tag = 0
  \/ c.tag = s
  \/ /\ c.tag < s
     /\ c.chosen /= None
     /\ c.tag <= fl

(* Phase 1a.                                                               *)
Prepare(r, n) ==
  /\ Send([type |-> "prepare", bal |-> <<r, n>>])
  /\ UNCHANGED <<promised, cells, chosenThrough, anchor, floor, deleted,
                 executed, applied, G, leases, joinerCounted, chosenLog>>

(* The vote an acceptor reports for a retained in-window slot above its    *)
(* anchor.  A chosen decree still in the window travels as a zero-ballot   *)
(* vote; evicted chosen decrees are summarized by chosen_through, and      *)
(* trimmed decrees by the anchor itself.                                   *)
CellVote(n, s) ==
  LET c == cells[n][Idx(s)]
  IN IF c.tag /= s \/ s <= anchor[n]
     THEN None
     ELSE IF c.vval /= None
          THEN [bal |-> c.vbal, val |-> c.vval]
          ELSE IF c.chosen /= None
               THEN [bal |-> ZeroBallot, val |-> c.chosen]
               ELSE None

(* Phase 1b: PromiseV2 carries the trim anchor, the contiguous chosen      *)
(* prefix summary, and votes only for slots above the anchor.              *)
Promise(n) ==
  \E m \in msgs :
    /\ m.type = "prepare"
    /\ ~BallotLt(m.bal, promised[n])
    /\ promised' = [promised EXCEPT ![n] = m.bal]
    /\ Send([type |-> "promise", bal |-> m.bal, from |-> n,
             anchor |-> anchor[n], ct |-> chosenThrough[n],
             votes |-> [s \in Slots |-> CellVote(n, s)]])
    /\ UNCHANGED <<cells, chosenThrough, anchor, floor, deleted, executed,
                   applied, G, leases, joinerCounted, chosenLog>>

Replies(Q, b) ==
  {m \in msgs : m.type = "promise" /\ m.bal = b /\ m.from \in Q}

(* B3 value selection over the reported votes.                             *)
ChoosableFor(Q, b, s) ==
  LET replies == Replies(Q, b)
      votes == {v \in {m.votes[s] : m \in replies} : v /= None}
  IN IF votes = {}
     THEN AllValues
     ELSE {w.val : w \in {v \in votes :
                            \A u \in votes : BallotLe(u.bal, v.bal)}}

(* Phase 2a with the trimmed-acceptor rules.  F is the greatest anchor a   *)
(* quorum member reported: every slot at or below it is permanently        *)
(* chosen, and absence of a vote there must never be read as an open       *)
(* instance.  K is the greatest chosen_through: a slot at or below it may  *)
(* have been chosen and evicted at every quorum member, so its value must  *)
(* arrive by catch-up, never by a fresh proposal.  The accept records the  *)
(* fence it was proposed above so the invariant can audit it.              *)
Accept(b, s) ==
  \E Q \in Quorums :
    /\ \A q \in Q : \E m \in msgs : /\ m.type = "promise"
                                    /\ m.bal = b
                                    /\ m.from = q
    /\ LET R == Replies(Q, b)
           F == SetMax({m.anchor : m \in R})
           K == SetMax({m.ct : m \in R})
       IN /\ s > F
          /\ s > K
          /\ \E v \in ChoosableFor(Q, b, s) :
               /\ \A m \in msgs : (/\ m.type = "accept"
                                   /\ m.bal = b
                                   /\ m.slot = s) => m.val = v
               /\ Send([type |-> "accept", bal |-> b, slot |-> s,
                        val |-> v, fence |-> IF F >= K THEN F ELSE K])
    /\ UNCHANGED <<promised, cells, chosenThrough, anchor, floor, deleted,
                   executed, applied, G, leases, joinerCounted, chosenLog>>

(* Phase 2b: install the vote into the tagged cell under the eviction      *)
(* rule.  Retagging clears a stale chosen value; a same-slot revote at a   *)
(* higher ballot keeps it.                                                 *)
Vote(n) ==
  \E m \in msgs :
    /\ m.type = "accept"
    /\ ~BallotLt(m.bal, promised[n])
    /\ m.slot > anchor[n]
    /\ CanInstall(cells[n][Idx(m.slot)], m.slot, floor[n])
    /\ promised' = [promised EXCEPT ![n] = m.bal]
    /\ cells' = [cells EXCEPT ![n] =
                   [@ EXCEPT ![Idx(m.slot)] =
                      [tag |-> m.slot, vbal |-> m.bal, vval |-> m.val,
                       chosen |-> IF cells[n][Idx(m.slot)].tag = m.slot
                                  THEN cells[n][Idx(m.slot)].chosen
                                  ELSE None]]]
    /\ Send([type |-> "accepted", bal |-> m.bal, slot |-> m.slot,
             from |-> n])
    /\ UNCHANGED <<chosenThrough, anchor, floor, deleted, executed,
                   applied, G, leases, joinerCounted, chosenLog>>

(* A write quorum of votes chooses the value.  The oracle log records the  *)
(* first decision; the Agreement invariant compares every later commit     *)
(* against it.                                                             *)
Decide(b, s) ==
  \E Q \in Quorums, m \in msgs :
    /\ m.type = "accept" /\ m.bal = b /\ m.slot = s
    /\ \A q \in Q : \E a \in msgs : /\ a.type = "accepted"
                                    /\ a.bal = b
                                    /\ a.slot = s
                                    /\ a.from = q
    /\ chosenLog' = [chosenLog EXCEPT ![s] = IF chosenLog[s] = None
                                             THEN m.val
                                             ELSE chosenLog[s]]
    /\ Send([type |-> "commit", slot |-> s, val |-> m.val])
    /\ UNCHANGED <<promised, cells, chosenThrough, anchor, floor, deleted,
                   executed, applied, G, leases, joinerCounted>>

(* Learning installs the chosen value into the tagged cell under the same  *)
(* eviction rule; a blocked install is dropped and re-delivered later.     *)
Learn(n) ==
  \E m \in msgs :
    /\ m.type = "commit"
    /\ m.slot > anchor[n]
    /\ CanInstall(cells[n][Idx(m.slot)], m.slot, floor[n])
    /\ cells' = [cells EXCEPT ![n] =
                   [@ EXCEPT ![Idx(m.slot)] =
                      IF cells[n][Idx(m.slot)].tag = m.slot
                      THEN [cells[n][Idx(m.slot)] EXCEPT !.chosen = m.val]
                      ELSE [tag |-> m.slot, vbal |-> ZeroBallot,
                            vval |-> None, chosen |-> m.val]]]
    /\ UNCHANGED <<promised, chosenThrough, anchor, floor, deleted,
                   executed, applied, G, leases, joinerCounted, chosenLog,
                   msgs>>

(* The contiguous chosen prefix advances over in-window chosen cells and   *)
(* over the anchor (a trimmed prefix is chosen by definition).             *)
AdvanceChosen(n) ==
  LET nxt == chosenThrough[n] + 1
  IN /\ nxt \in Slots
     /\ \/ nxt <= anchor[n]
        \/ /\ cells[n][Idx(nxt)].tag = nxt
           /\ cells[n][Idx(nxt)].chosen /= None
     /\ chosenThrough' = [chosenThrough EXCEPT ![n] = nxt]
     /\ UNCHANGED <<promised, cells, anchor, floor, deleted, executed,
                    applied, G, leases, joinerCounted, chosenLog, msgs>>

(* advanceMemoryFloor: the floor never passes the contiguous chosen        *)
(* prefix.  Everything at or below it is journal-durable and recoverable   *)
(* by the host, which is what licenses eviction.                           *)
AdvanceFloor(n) ==
  \E f \in (floor[n] + 1)..chosenThrough[n] :
    /\ floor' = [floor EXCEPT ![n] = f]
    /\ UNCHANGED <<promised, cells, chosenThrough, anchor, deleted,
                   executed, applied, G, leases, joinerCounted, chosenLog,
                   msgs>>

(* A data voter materializes the next chosen command (live apply or the    *)
(* Phase A replay after a crash).  The joiner replays from commit          *)
(* evidence served out of a peer's retained journal.                       *)
ExecuteNext(n) ==
  /\ n \in DataNodes
  /\ LET nxt == executed[n] + 1
     IN /\ nxt \in Slots
        /\ IF n \in DataVoters
           THEN nxt <= chosenThrough[n]
           ELSE /\ applied[n] > 0
                /\ \E m \in msgs : m.type = "commit" /\ m.slot = nxt
        /\ executed' = [executed EXCEPT ![n] = nxt]
  /\ UNCHANGED <<promised, cells, chosenThrough, anchor, floor, deleted,
                 applied, G, leases, joinerCounted, chosenLog, msgs>>

(* applied_anchor.publish: the durable state anchor catches up to the      *)
(* materialized image.                                                     *)
AnchorState(n) ==
  /\ n \in DataNodes
  /\ applied[n] < executed[n]
  /\ applied' = [applied EXCEPT ![n] = executed[n]]
  /\ UNCHANGED <<promised, cells, chosenThrough, anchor, floor, deleted,
                 executed, G, leases, joinerCounted, chosenLog, msgs>>

(* Power loss: the open image rolls back to the durable anchor; consensus  *)
(* state is journal-backed and survives.  Replay is ExecuteNext.           *)
CrashImage(n) ==
  /\ n \in DataNodes
  /\ executed' = [executed EXCEPT ![n] = applied[n]]
  /\ UNCHANGED <<promised, cells, chosenThrough, anchor, floor, deleted,
                 applied, G, leases, joinerCounted, chosenLog, msgs>>

(* The chosen Trim entry: conservative rule over every counted data        *)
(* replica's durable-state frontier.  The joiner is counted only after     *)
(* its lease completed.                                                    *)
CountedData == DataVoters \cup (IF joinerCounted THEN {Joiner} ELSE {})

ChooseTrim ==
  \E g \in (G + 1)..MaxSlot :
    /\ g <= SetMin({applied[n] : n \in CountedData})
    /\ G' = g
    /\ UNCHANGED <<promised, cells, chosenThrough, anchor, floor, deleted,
                   executed, applied, leases, joinerCounted, chosenLog,
                   msgs>>

(* installChosenTrim: a voter adopts a chosen trim no further than its own *)
(* proven chosen prefix.                                                   *)
InstallTrim(n) ==
  \E g \in (anchor[n] + 1)..MaxSlot :
    /\ g <= G
    /\ g <= chosenThrough[n]
    /\ anchor' = [anchor EXCEPT ![n] = g]
    /\ UNCHANGED <<promised, cells, chosenThrough, floor, deleted,
                   executed, applied, G, leases, joinerCounted, chosenLog,
                   msgs>>

(* Physical deletion: T_i <- min(anchor, A_i, lease bases).  A witness has *)
(* no materialized state and deletes through its anchor alone; the anchor  *)
(* is what answers Phase 1 for the deleted prefix.                         *)
LeaseCap == IF leases = {} THEN MaxSlot ELSE SetMin(leases)

DeleteCap(n) ==
  IF n \in DataVoters
  THEN LET a == IF anchor[n] <= applied[n] THEN anchor[n] ELSE applied[n]
       IN IF a <= LeaseCap THEN a ELSE LeaseCap
  ELSE IF anchor[n] <= LeaseCap THEN anchor[n] ELSE LeaseCap

LocalDelete(n) ==
  \E t \in (deleted[n] + 1)..DeleteCap(n) :
    /\ deleted' = [deleted EXCEPT ![n] = t]
    /\ UNCHANGED <<promised, cells, chosenThrough, anchor, floor, executed,
                   applied, G, leases, joinerCounted, chosenLog, msgs>>

(* Transfer lease lifecycle for the joiner: a cluster-visible lease at a   *)
(* sender's durable anchor caps every node's deletion, the joiner installs *)
(* the pinned image at exactly that base, replays the suffix, and is       *)
(* counted for trimming only once its anchor has caught up to the chosen   *)
(* trim.  Sender loss needs no action: the lease is chosen state, not      *)
(* sender state, so any surviving node serves the suffix.                  *)
CreateLease ==
  /\ leases = {}
  /\ ~joinerCounted
  /\ applied[Joiner] = 0
  /\ \E n \in DataVoters :
       /\ applied[n] > 0
       /\ leases' = {applied[n]}
       /\ UNCHANGED <<promised, cells, chosenThrough, anchor, floor,
                      deleted, executed, applied, G, joinerCounted,
                      chosenLog, msgs>>

InstallJoiner ==
  \E b \in leases :
    /\ applied[Joiner] = 0
    /\ applied' = [applied EXCEPT ![Joiner] = b]
    /\ executed' = [executed EXCEPT ![Joiner] = b]
    /\ UNCHANGED <<promised, cells, chosenThrough, anchor, floor, deleted,
                   G, leases, joinerCounted, chosenLog, msgs>>

CompleteLease ==
  \E b \in leases :
    /\ applied[Joiner] >= b
    /\ applied[Joiner] >= G
    /\ leases' = leases \ {b}
    /\ joinerCounted' = TRUE
    /\ UNCHANGED <<promised, cells, chosenThrough, anchor, floor, deleted,
                   executed, applied, G, chosenLog, msgs>>

Next ==
  \/ \E r \in 1..MaxRound, n \in Voters : Prepare(r, n)
  \/ \E n \in Voters : Promise(n) \/ Vote(n) \/ Learn(n)
                         \/ AdvanceChosen(n) \/ AdvanceFloor(n)
                         \/ InstallTrim(n) \/ LocalDelete(n)
  \/ \E b \in Ballots, s \in Slots : Accept(b, s) \/ Decide(b, s)
  \/ \E n \in DataNodes : ExecuteNext(n) \/ AnchorState(n) \/ CrashImage(n)
  \/ ChooseTrim
  \/ CreateLease \/ InstallJoiner \/ CompleteLease

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(* Invariants (ZDS 0011 verification plan).                                *)

(* No commit ever contradicts the first decision for its slot.             *)
Agreement ==
  /\ \A m \in msgs : m.type = "commit" => chosenLog[m.slot] = m.val
  /\ \A n \in Voters, j \in 0..(W - 1) :
       LET c == cells[n][j]
       IN c.chosen /= None => chosenLog[c.tag] = c.chosen

(* Every slot inside a claimed chosen prefix or trim anchor really is      *)
(* chosen.                                                                 *)
ChosenPrefix ==
  \A n \in Voters, s \in Slots :
    s <= chosenThrough[n] => chosenLog[s] /= None

(* A tagged cell only ever sits in its own physical index; a lookup for s  *)
(* can therefore never return state belonging to another slot.             *)
TagNonAliasing ==
  \A n \in Voters, j \in 0..(W - 1) :
    cells[n][j].tag /= 0 => Idx(cells[n][j].tag) = j

(* The chosen trim never passes the minimum durable-state frontier of the  *)
(* counted data replicas.                                                  *)
TrimNeverExceedsCertifiedAnchor ==
  \A n \in CountedData : G <= applied[n]

(* The durable state anchor never claims more than the materialized image  *)
(* holds.                                                                  *)
AppliedNeverExceedsDurablePages ==
  \A n \in DataNodes : applied[n] <= executed[n]

(* An acceptor that voted for a not-yet-chosen slot still holds that vote  *)
(* in its window at an equal or higher ballot: eviction never erases an    *)
(* open Phase-2 obligation.                                                *)
AcceptedOnlySlotNeverEvicted ==
  \A a \in msgs :
    (/\ a.type = "accepted"
     /\ ~\E m \in msgs : m.type = "commit" /\ m.slot = a.slot)
      => LET c == cells[a.from][Idx(a.slot)]
         IN /\ c.tag = a.slot
            /\ c.vval /= None
            /\ BallotLe(a.bal, c.vbal)

(* A promise reports votes only above its own anchor.                      *)
PromiseRangeStartsAboveAnchor ==
  \A m \in msgs :
    m.type = "promise" =>
      \A s \in Slots : m.votes[s] /= None => s > m.anchor

(* Every accept was proposed strictly above the fence its quorum reported, *)
(* and everything at or below that fence was already chosen.               *)
LeaderNeverProposesAtOrBelowAnchor ==
  \A m \in msgs :
    m.type = "accept" =>
      /\ m.slot > m.fence
      /\ \A s \in Slots : s <= m.fence => chosenLog[s] /= None

(* Local deletion never outruns the durable state that recovery starts     *)
(* from, the installed trim anchor, or an active transfer lease.           *)
LocalDeleteNeverExceedsDurableState ==
  \A n \in Voters :
    /\ deleted[n] <= anchor[n]
    /\ n \in DataVoters => deleted[n] <= applied[n]

(* While a lease is active, every node retains the suffix its receiver     *)
(* needs, whatever happened to the original sender.                        *)
TransferLeasePreservesSuffix ==
  \A b \in leases : \A n \in Voters : deleted[n] <= b

(* A materialized image is always an exact chosen prefix.                  *)
RecoveryReadyImpliesExactPrefix ==
  \A n \in DataNodes :
    \A s \in Slots : s <= executed[n] => chosenLog[s] /= None

(* Witnesses vote but never claim materialized state; the trim minimum is  *)
(* computed over data replicas only.                                       *)
NoWitnessClaimsMaterializedState ==
  \A n \in Witnesses : applied[n] = 0 /\ executed[n] = 0

(* The frontier chain of ZDS 0011: T <= A <= E and anchors inside the      *)
(* proven chosen prefix, with the memory floor never past it either.       *)
FrontierOrder ==
  /\ \A n \in Voters :
       /\ anchor[n] <= chosenThrough[n]
       /\ floor[n] <= chosenThrough[n]
       /\ anchor[n] <= G
  /\ \A n \in DataVoters :
       /\ applied[n] <= executed[n]
       /\ executed[n] <= chosenThrough[n]

(* DurableState.assertValid: the promise never trails a recorded vote.     *)
PromisedDominatesVotes ==
  \A n \in Voters, j \in 0..(W - 1) :
    cells[n][j].vval /= None => BallotLe(cells[n][j].vbal, promised[n])

----------------------------------------------------------------------------
(* GlobalSlotNeverDecreases: decisions are immutable and every durable     *)
(* frontier is monotone.  The materialized image (`executed`) is exempt:   *)
(* it may roll back to the anchor at a crash, which is exactly what the    *)
(* APPLIED anchor exists to make safe.                                     *)
Monotone ==
  [][/\ G' >= G
     /\ \A s \in Slots : chosenLog[s] /= None => chosenLog'[s] = chosenLog[s]
     /\ \A n \in Voters : /\ chosenThrough'[n] >= chosenThrough[n]
                          /\ anchor'[n] >= anchor[n]
                          /\ floor'[n] >= floor[n]
                          /\ deleted'[n] >= deleted[n]
     /\ \A n \in DataNodes : applied'[n] >= applied[n]]_vars

----------------------------------------------------------------------------
(* Bounded model checking: the monotonic message set explodes the state    *)
(* space, so behaviors are capped by the configured distinct message       *)
(* count.  Eighteen covers a full ballot over several slots, a competing   *)
(* ballot after a trim, and the catch-up traffic the window forces;        *)
(* deeper bounds trade run time for longer interleavings.                  *)
MessageBound == Cardinality(msgs) <= MaxMessages

=============================================================================
