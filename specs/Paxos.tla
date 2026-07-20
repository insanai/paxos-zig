------------------------------- MODULE Paxos -------------------------------
(***************************************************************************)
(* Multi-slot Paxos as implemented by src/protocol.zig, at the durable     *)
(* state's level of abstraction.  Variables promised, accepted, and        *)
(* committed correspond to `DurableState`; the message set `msgs` is       *)
(* monotonic, so loss, duplication, and arbitrary reordering are all       *)
(* included, and a crash-restart is a no-op (a node acts only from        *)
(* durable state).  In-memory state (role, recovered votes, next_slot) is  *)
(* deliberately not modeled: safety must not depend on it.                 *)
(*                                                                         *)
(* Action mapping to src/protocol.zig:                                     *)
(*   Prepare  <-> startCampaign (phase 1a broadcast)                       *)
(*   Promise  <-> onPrepare (phase 1b: promise + votes, including          *)
(*                learned-only decrees reported as zero-ballot votes)      *)
(*   Accept   <-> maybeBecomeLeader / propose (phase 2a: value chosen      *)
(*                from the highest-ballot vote across a read quorum)       *)
(*   Vote     <-> onAccept (phase 2b: durable vote)                        *)
(*   Decide   <-> onAccepted (write-quorum commit)                         *)
(*   Learn    <-> onCommit / recordCommit                                  *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Voters, Learners, Slots, Values, MaxRound, Noop, None

Nodes == Voters \cup Learners

AllValues == Values \cup {Noop}

ZeroBallot == <<0, 0>>

Ballots == (1..MaxRound) \X Voters

BallotLt(a, b) == \/ a[1] < b[1]
                  \/ /\ a[1] = b[1]
                     /\ a[2] < b[2]

BallotLe(a, b) == BallotLt(a, b) \/ a = b

(* Majority quorums; replace with any read/write families whose members    *)
(* pairwise intersect to model the flexible-quorum configuration.          *)
Quorums == {S \in SUBSET Voters : 2 * Cardinality(S) > Cardinality(Voters)}

VARIABLES promised, accepted, committed, msgs

vars == <<promised, accepted, committed, msgs>>

Init ==
  /\ promised = [n \in Nodes |-> ZeroBallot]
  /\ accepted = [n \in Nodes |-> [s \in Slots |-> None]]
  /\ committed = [n \in Nodes |-> [s \in Slots |-> None]]
  /\ msgs = {}

Send(m) == msgs' = msgs \cup {m}

(* Phase 1a: ballots are partitioned by owner, so uniqueness holds by      *)
(* construction (Lamport's B1 / owner function).                           *)
Prepare(r, n) ==
  /\ Send([type |-> "prepare", bal |-> <<r, n>>])
  /\ UNCHANGED <<promised, accepted, committed>>

(* The vote this node reports for a slot.  A decree learned without        *)
(* voting travels as a zero-ballot vote (paper section 3.1: LastVote       *)
(* replies also carry already-passed decrees); it loses to every real      *)
(* vote, so it can never override the choosing quorum's value.             *)
VoteOrLearned(n, s) ==
  IF accepted[n][s] /= None
  THEN accepted[n][s]
  ELSE IF committed[n][s] /= None
       THEN [bal |-> ZeroBallot, val |-> committed[n][s]]
       ELSE None

(* Phase 1b: promise any ballot not below the current promise, exactly     *)
(* the `lessThan` guard in onPrepare (equal ballots are re-promised).      *)
Promise(n) ==
  \E m \in msgs :
    /\ m.type = "prepare"
    /\ ~BallotLt(m.bal, promised[n])
    /\ promised' = [promised EXCEPT ![n] = m.bal]
    /\ Send([type |-> "promise", bal |-> m.bal, from |-> n,
             votes |-> [s \in Slots |-> VoteOrLearned(n, s)]])
    /\ UNCHANGED <<accepted, committed>>

(* Values the leader of ballot b may send for slot s after hearing the     *)
(* read quorum Q: the highest-ballot vote's value, or anything (including  *)
(* the no-op gap filler) when no quorum member voted.  Lamport's B3.       *)
ChoosableFor(Q, b, s) ==
  LET replies == {m \in msgs : /\ m.type = "promise"
                               /\ m.bal = b
                               /\ m.from \in Q}
      votes == {v \in {m.votes[s] : m \in replies} : v /= None}
  IN IF votes = {}
     THEN AllValues
     ELSE {w.val : w \in {v \in votes :
                            \A u \in votes : BallotLe(u.bal, v.bal)}}

(* Phase 2a: one value per ballot and slot, chosen per B3 from a full      *)
(* read quorum of promises.                                                *)
Accept(b, s) ==
  \E Q \in Quorums :
    /\ \A q \in Q : \E m \in msgs : /\ m.type = "promise"
                                    /\ m.bal = b
                                    /\ m.from = q
    /\ \E v \in ChoosableFor(Q, b, s) :
         /\ \A m \in msgs : (/\ m.type = "accept"
                             /\ m.bal = b
                             /\ m.slot = s) => m.val = v
         /\ Send([type |-> "accept", bal |-> b, slot |-> s, val |-> v])
    /\ UNCHANGED <<promised, accepted, committed>>

(* Phase 2b: vote unless the ballot is below the promise; voting bumps     *)
(* the promise, as onAccept does.                                          *)
Vote(n) ==
  \E m \in msgs :
    /\ m.type = "accept"
    /\ ~BallotLt(m.bal, promised[n])
    /\ promised' = [promised EXCEPT ![n] = m.bal]
    /\ accepted' = [accepted EXCEPT
                      ![n] = [@ EXCEPT ![m.slot] = [bal |-> m.bal,
                                                    val |-> m.val]]]
    /\ Send([type |-> "accepted", bal |-> m.bal, slot |-> m.slot,
             from |-> n])
    /\ UNCHANGED <<committed>>

(* A write quorum of votes for one ballot and slot chooses the value.      *)
Decide(b, s) ==
  \E W \in Quorums, m \in msgs :
    /\ m.type = "accept" /\ m.bal = b /\ m.slot = s
    /\ \A w \in W : \E a \in msgs : /\ a.type = "accepted"
                                    /\ a.bal = b
                                    /\ a.slot = s
                                    /\ a.from = w
    /\ Send([type |-> "commit", slot |-> s, val |-> m.val])
    /\ UNCHANGED <<promised, accepted, committed>>

(* Learning is unconditional, mirroring recordCommit: a stale local vote   *)
(* never blocks a commit.  The Agreement invariant is what forbids two     *)
(* different values from ever being committed.                             *)
Learn(n) ==
  \E m \in msgs :
    /\ m.type = "commit"
    /\ committed' = [committed EXCEPT ![n] = [@ EXCEPT ![m.slot] = m.val]]
    /\ UNCHANGED <<promised, accepted, msgs>>

Next ==
  \/ \E r \in 1..MaxRound, n \in Voters : Prepare(r, n)
  \/ \E n \in Voters : Promise(n) \/ Vote(n)
  \/ \E n \in Nodes : Learn(n)
  \/ \E b \in Ballots, s \in Slots : Accept(b, s) \/ Decide(b, s)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(* Invariants: the code-level oracles in sim/simulation.zig check the      *)
(* same properties on concrete executions.                                 *)

(* Sim invariant 1: no two nodes commit different values for a slot.       *)
Agreement ==
  \A n1, n2 \in Nodes, s \in Slots :
    (committed[n1][s] /= None /\ committed[n2][s] /= None)
      => committed[n1][s] = committed[n2][s]

(* recordCommit's ConflictingCommit can only mean corruption: no two       *)
(* commit messages for one slot ever disagree.                             *)
CommitUniqueness ==
  \A m1, m2 \in msgs :
    (/\ m1.type = "commit"
     /\ m2.type = "commit"
     /\ m1.slot = m2.slot) => m1.val = m2.val

(* DurableState.assertValid: the promise never trails a recorded vote.     *)
PromisedDominatesVotes ==
  \A n \in Nodes, s \in Slots :
    accepted[n][s] /= None => BallotLe(accepted[n][s].bal, promised[n])

(* Only client values or the no-op are ever committed.                     *)
Validity ==
  \A n \in Nodes, s \in Slots :
    committed[n][s] /= None => committed[n][s] \in AllValues

(* Learners can record chosen values but never promise or accept. Adding or *)
(* removing learners therefore cannot change a quorum or the chosen value.  *)
LearnersDoNotVote ==
  \A n \in Learners, s \in Slots :
    /\ promised[n] = ZeroBallot
    /\ accepted[n][s] = None

----------------------------------------------------------------------------
(* Bounded model checking: the monotonic message set makes the full state  *)
(* space explode, so the default configuration caps how many distinct      *)
(* messages a behavior may send.  Fourteen covers one complete ballot      *)
(* (9 messages) interleaved with a competing partial ballot.  Raise the    *)
(* bound for deeper, longer runs.                                          *)
MessageBound == Cardinality(msgs) <= 14

=============================================================================
