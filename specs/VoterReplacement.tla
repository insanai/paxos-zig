-------------------------- MODULE VoterReplacement --------------------------
(***************************************************************************)
(* Bounded host-level model of the decided one-for-one voter replacement   *)
(* implemented by zaxonlite (ZDS 0008).  Paxos.tla proves that the sealed  *)
(* configuration chooses at most one stop sign; this model abstracts that  *)
(* choice into the single atomic action ChooseStop and checks the host's   *)
(* durable activation discipline around it.                                *)
(*                                                                         *)
(* Per node, the variables mirror the durable files:                       *)
(*   blob      <-> registries/<id>, the canonical decided-registry blob    *)
(*                 (a survivor reconstructs it from the chosen stop        *)
(*                 metadata; the replacement fetches and verifies it)      *)
(*   installed <-> CURRENT, the durable next-configuration state           *)
(*   pointer   <-> REGISTRY, the active-registry pointer                   *)
(*   identity  <-> the identity file's configuration_id                    *)
(*   active    <-> the node sends and counts next-configuration votes      *)
(*                                                                         *)
(* Every variable is durable, so a crash-restart is a no-op and the        *)
(* in-process transport swap is behaviorally identical to a restart: TLC   *)
(* explores every crash interleaving as an ordinary action prefix.  The    *)
(* invariants check the write order the code enforces (blob, CURRENT,      *)
(* REGISTRY, identity, activation) and the two product rules: a            *)
(* replacement never participates before durable verified installation,    *)
(* and the removed voter never enters the next configuration.              *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Survivors, Removed, Replacement

Nodes == Survivors \cup {Removed, Replacement}

NextMembers == Survivors \cup {Replacement}

VARIABLES chosen, blob, installed, pointer, identity, active

vars == <<chosen, blob, installed, pointer, identity, active>>

Init ==
  /\ chosen = FALSE
  /\ blob = [n \in Nodes |-> FALSE]
  /\ installed = [n \in Nodes |-> FALSE]
  /\ pointer = [n \in Nodes |-> 1]
  /\ identity = [n \in Nodes |-> 1]
  /\ active = [n \in Nodes |-> FALSE]

(* The sealed configuration's voters choose the stop sign binding the      *)
(* checkpoint, the next member IDs, and the next-registry digest.          *)
ChooseStop ==
  /\ ~chosen
  /\ chosen' = TRUE
  /\ UNCHANGED <<blob, installed, pointer, identity, active>>

(* A survivor reconstructs the next registry deterministically from the    *)
(* chosen stop metadata and stores the blob without the network.           *)
StoreBlob(n) ==
  /\ n \in Survivors
  /\ chosen
  /\ blob' = [blob EXCEPT ![n] = TRUE]
  /\ UNCHANGED <<chosen, installed, pointer, identity, active>>

(* The joining replacement fetches the blob from a member that already     *)
(* activated the next configuration and verifies it against the digest     *)
(* the chosen stop sign bound; a failed verification stores nothing.       *)
FetchBlob ==
  /\ chosen
  /\ \E m \in Survivors : identity[m] = 2
  /\ blob' = [blob EXCEPT ![Replacement] = TRUE]
  /\ UNCHANGED <<chosen, installed, pointer, identity, active>>

(* CURRENT advances: a survivor materializes its own checkpoint; the       *)
(* replacement installs the transferred snapshot only after its fetched    *)
(* registry verified.                                                      *)
InstallState(n) ==
  /\ n \in NextMembers
  /\ chosen
  /\ (n = Replacement) => blob[n]
  /\ installed' = [installed EXCEPT ![n] = TRUE]
  /\ UNCHANGED <<chosen, blob, pointer, identity, active>>

(* REGISTRY advances after CURRENT and only over a stored blob.            *)
FlipPointer(n) ==
  /\ n \in NextMembers
  /\ installed[n]
  /\ blob[n]
  /\ pointer' = [pointer EXCEPT ![n] = 2]
  /\ UNCHANGED <<chosen, blob, installed, identity, active>>

(* The identity file advances last; recovery on either side of it          *)
(* converges through the same rollover path.                               *)
AdvanceIdentity(n) ==
  /\ pointer[n] = 2
  /\ identity' = [identity EXCEPT ![n] = 2]
  /\ UNCHANGED <<chosen, blob, installed, pointer, active>>

(* Participation in the next configuration requires the whole durable      *)
(* chain.  The removed voter has no activation action at all: admission    *)
(* rejects its ID and its own rollover path refuses the configuration      *)
(* that replaced it.                                                       *)
Activate(n) ==
  /\ n \in NextMembers
  /\ identity[n] = 2
  /\ active' = [active EXCEPT ![n] = TRUE]
  /\ UNCHANGED <<chosen, blob, installed, pointer, identity>>

Next ==
  \/ ChooseStop
  \/ FetchBlob
  \/ \E n \in Nodes :
       \/ StoreBlob(n)
       \/ InstallState(n)
       \/ FlipPointer(n)
       \/ AdvanceIdentity(n)
       \/ Activate(n)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

(* Nothing durable exists for the next configuration before consensus      *)
(* chose it.                                                               *)
NothingBeforeChoice ==
  (\E n \in Nodes : blob[n] \/ installed[n] \/ pointer[n] = 2) => chosen

(* The pointer never dangles: it names a stored, installed generation and  *)
(* implies the decision.                                                   *)
PointerNeverDangles ==
  \A n \in Nodes : (pointer[n] = 2) => (blob[n] /\ installed[n] /\ chosen)

(* Recovery selects the old registry or the fully installed new one; the   *)
(* identity file never runs ahead of the pointer, so no node mixes the     *)
(* two configurations.                                                     *)
IdentityFollowsPointer ==
  \A n \in Nodes : (identity[n] = 2) => (pointer[n] = 2)

(* A replacement cannot vote before its durable verified installation.     *)
NoActivationBeforeInstall ==
  \A n \in Nodes :
    active[n] => (installed[n] /\ pointer[n] = 2 /\ identity[n] = 2)

(* The removed voter stays permanently sealed on its final configuration. *)
RemovedStaysSealed ==
  /\ ~active[Removed]
  /\ identity[Removed] = 1
  /\ pointer[Removed] = 1

(* The replacement's state, when present, came through the verified fetch. *)
ReplacementFetchesFirst ==
  installed[Replacement] => blob[Replacement]

=============================================================================
