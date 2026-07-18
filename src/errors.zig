//! Human-friendly error explanations inspired by Elm's "compiler errors for humans".
//!
//! Provides structured, high-context explanations for consensus and protocol
//! error states, complete with hints for resolution.

/// Returns a human-readable, Elm-style compiler error explanation for any
/// protocol or replicated log error code.
pub fn explainError(err: anyerror) []const u8 {
    return switch (err) {
        error.EmptyMembership =>
            \\-- EMPTY MEMBERSHIP ------------------------------------------------------------
            \\
            \\The membership configuration was initialized with zero active nodes.
            \\
            \\A consensus cluster cannot make decisions, run campaigns, or form quorums
            \\without any voting members.
            \\
            \\Hint: Pass a slice of at least one valid non-zero NodeId to Membership.init().
            ,
        error.TooManyMembers =>
            \\-- TOO MANY MEMBERS ------------------------------------------------------------
            \\
            \\The number of configured nodes exceeds the compile-time maximum limit.
            \\
            \\The protocol core allocates static arrays sized for 'max_members' to ensure
            \\zero runtime memory allocation.
            \\
            \\Hint: Increase the 'max_members' field in your Options configuration or
            \\reduce the size of the initial membership list.
            ,
        error.InvalidNodeId =>
            \\-- INVALID NODE ID -------------------------------------------------------------
            \\
            \\A membership configuration contained node ID 0, which is invalid.
            \\
            \\Zero is reserved as a special sentinel value to represent "no node" or "empty"
            \\in the internal structures. Node IDs must be positive non-zero integers.
            \\
            \\Hint: Ensure all NodeIds in your configuration are strictly greater than zero.
            ,
        error.DuplicateNodeId =>
            \\-- DUPLICATE NODE ID -----------------------------------------------------------
            \\
            \\The membership configuration contains the same node ID more than once.
            \\
            \\Voter lists must be unique sets. Duplicate identities break quorum calculations
            \\and could lead to invalid ballot majorities.
            \\
            \\Hint: Filter your node list to ensure every NodeId is unique before initializing
            \\membership.
            ,
        error.InvalidReadQuorum =>
            \\-- INVALID READ QUORUM ----------------------------------------------------------
            \\
            \\The configured read quorum size (Phase 1) is invalid.
            \\
            \\The read quorum size must be greater than zero and less than or equal to the
            \\total number of cluster members.
            \\
            \\Hint: Check your Options config. Ensure read_quorum_size <= member count.
            ,
        error.InvalidWriteQuorum =>
            \\-- INVALID WRITE QUORUM ---------------------------------------------------------
            \\
            \\The configured write quorum size (Phase 2) is invalid.
            \\
            \\The write quorum size must be greater than zero and less than or equal to the
            \\total number of cluster members.
            \\
            \\Hint: Check your Options config. Ensure write_quorum_size <= member count.
            ,
        error.NonIntersectingQuorums =>
            \\-- NON-INTERSECTING QUORUMS ----------------------------------------------------
            \\
            \\The configured read (Phase 1) and write (Phase 2) quorums do not intersect.
            \\
            \\To guarantee safety, the sum of read and write quorum sizes must be strictly
            \\greater than the total cluster size:
            \\    read_quorum_size + write_quorum_size > member_count
            \\This guarantees that any Phase 1 query will discover old Phase 2 votes.
            \\
            \\Hint: Increase read_quorum_size or write_quorum_size so their sum exceeds the
            \\membership count.
            ,
        error.PromiseRegression =>
            \\-- PROMISE REGRESSION -----------------------------------------------------------
            \\
            \\An acceptor attempted to process a promise or prepare that moves backward.
            \\
            \\To protect safety, once an acceptor promises a ballot round B, it must never
            \\accept or promise any ballot lower than B. Moving backward indicates state
            \\corruption or an out-of-order journal replay.
            \\
            \\Hint: Investigate if your local disk journal was truncated, or if the node ID
            \\participated in conflicting campaigns.
            ,
        error.ConflictingValue =>
            \\-- CONFLICTING VALUE -----------------------------------------------------------
            \\
            \\Two different values were proposed or accepted in the same ballot and slot.
            \\
            \\Paxos safety guarantees that a ballot round B can propose at most one value for
            \\slot S. Discovering conflicting values for the same ballot/slot means the
            \\leader or protocol core state has diverged.
            \\
            \\Hint: Ensure your host application does not generate duplicate ballots or bypass
            \\the leader's proposal checks.
            ,
        error.ConflictingCommit =>
            \\-- CONFLICTING COMMIT ----------------------------------------------------------
            \\
            \\A commit was received that conflicts with an already chosen or committed value.
            \\
            \\Consensus guarantees that once a value is committed in slot S, it is permanently
            \\chosen. Receiving a different commit value for slot S represents a major consensus
            \\safety violation.
            \\
            \\Hint: Check if your cluster has a split-brain (multiple active leaders) or if
            \\quorum size validation was bypassed.
            ,
        error.NotMember =>
            \\-- NOT A MEMBER ----------------------------------------------------------------
            \\
            \\An operation was attempted on or by a node that is not in the membership list.
            \\
            \\A node must belong to the active cluster configuration to send messages, vote,
            \\or handle step transitions.
            \\
            \\Hint: Verify that the source/target NodeId matches one of the active nodes in the
            \\current membership configuration.
            ,
        error.NotLeader =>
            \\-- NOT LEADER ------------------------------------------------------------------
            \\
            \\A write proposal was attempted on a node that is not the active cluster leader.
            \\
            \\In Multi-Paxos, clients must submit all write operations to the active leader.
            \\Other nodes act as followers and will reject proposals.
            \\
            \\Hint: Check if the node's current role is .leader before calling propose(), or
            \\route client writes to the correct leader node.
            ,
        error.SlotLimitReached =>
            \\-- SLOT LIMIT REACHED -----------------------------------------------------------
            \\
            \\The bounded log has no free slots available.
            \\
            \\The protocol core uses static memory bounds sized for 'max_slots'. Once next_slot
            \\reaches this compile-time maximum, no further proposals are allowed in this epoch.
            \\
            \\Hint: Perform a database checkpoint, seal the current epoch with a Stop Sign,
            \\and start a new epoch log starting at Slot 1.
            ,
        error.EmptyBatch =>
            \\-- EMPTY BATCH -----------------------------------------------------------------
            \\
            \\A batch proposal was submitted with zero commands.
            \\
            \\You cannot run consensus on an empty set of values. A batch proposal must contain
            \\at least one valid Command.
            \\
            \\Hint: Ensure the command slice passed to proposeBatch() has a length greater than
            \\zero.
            ,
        error.SlotBufferTooSmall =>
            \\-- SLOT BUFFER TOO SMALL ---------------------------------------------------------
            \\
            \\The output buffer provided for assigned slots is smaller than the proposed batch.
            \\
            \\When proposing a batch of commands, you must supply an output slice of Slots
            \\with a length at least equal to the number of commands.
            \\
            \\Hint: Size your output slot buffer to match or exceed the number of items in the
            \\batch.
            ,
        error.InvalidPeer =>
            \\-- INVALID PEER -----------------------------------------------------------------
            \\
            \\A node attempted a peer operation targeting itself.
            \\
            \\Nodes cannot send network peer messages to themselves. Loopback routing is handled
            \\internally via local state transitions.
            \\
            \\Hint: Check the target peer NodeId and verify it is a remote member, not the
            \\local node's own ID.
            ,
        error.InvalidSlot =>
            \\-- INVALID SLOT -----------------------------------------------------------------
            \\
            \\An operation referenced Slot 0, which is invalid.
            \\
            \\Paxos slots in the log are one-indexed. Slot 0 is reserved as a sentinel to
            \\represent "no slot" or "empty log".
            \\
            \\Hint: Verify that slot values passed to catch-up or query APIs are strictly
            \\greater than zero.
            ,
        error.WrongRecipient =>
            \\-- WRONG RECIPIENT --------------------------------------------------------------
            \\
            \\An envelope was delivered to a node that is not the intended recipient.
            \\
            \\The network routing layer delivered a message to Node A when the envelope's
            \\'to' header explicitly named Node B.
            \\
            \\Hint: Check your network transport routing/demultiplexing code to ensure messages
            \\are delivered to the correct local Node instance.
            ,
        error.ReadBufferTooSmall =>
            \\-- READ BUFFER TOO SMALL --------------------------------------------------------
            \\
            \\The user-supplied buffer for reading log entries is too small.
            \\
            \\The database read API requires a destination buffer large enough to hold all available
            \\sequential log entries from the requested slot.
            \\
            \\Hint: Allocate a larger Command buffer before calling read or query methods.
            ,
        error.BallotExhausted =>
            \\-- BALLOT EXHAUSTED -------------------------------------------------------------
            \\
            \\The leader round counter has reached its maximum u64 value.
            \\
            \\No higher ballot round can be generated. The node cannot campaign again in this
            \\epoch.
            \\
            \\Hint: Reinitialize the cluster under a new epoch to reset the ballot round counters.
            ,
        error.InvalidPromise =>
            \\-- INVALID PROMISE --------------------------------------------------------------
            \\
            \\A promise reply reported a count of accepted entries exceeding the slot limit.
            \\
            \\The number of reported accepted slots is larger than compile-time 'max_slots',
            \\indicating network data corruption or mismatched cluster bounds.
            \\
            \\Hint: Verify that all nodes in the cluster are running with identical 'max_slots'
            \\compile-time options.
            ,
        error.MissingNoop =>
            \\-- MISSING NO-OP ----------------------------------------------------------------
            \\
            \\A campaign was started but the host did not supply a No-Op command.
            \\
            \\When recovering slot holes during leader election, the node must propose a No-Op
            \\command for empty slots. If the host has not set the noop payload, it cannot campaign.
            \\
            \\Hint: Provide a valid No-Op payload command when initializing the campaign.
            ,
        error.MissingProposedValue =>
            \\-- MISSING PROPOSED VALUE --------------------------------------------------------
            \\
            \\A leader attempted to replicate a slot but had no proposed value recorded.
            \\
            \\A leader must only send accept messages for slots where a value was either
            \\proposed by a client or recovered from an acceptor's promise.
            \\
            \\Hint: Ensure proposals are not cleared from memory before they are committed.
            ,
        error.UnknownNode =>
            \\-- UNKNOWN NODE -----------------------------------------------------------------
            \\
            \\The node ID was not recognized by the membership configuration.
            \\
            \\Hint: Verify the NodeId is present in your current cluster configuration.
            ,
        error.InvalidConfigurationId =>
            \\-- INVALID CONFIGURATION ID -----------------------------------------------------
            \\
            \\The configuration ID is zero, which is invalid.
            \\
            \\Zero is reserved as a sentinel. Reconfiguration IDs must be positive non-zero integers.
            \\
            \\Hint: Ensure configuration IDs are incremented monotonically and strictly positive.
            ,
        error.MetadataTooLarge =>
            \\-- METADATA TOO LARGE -----------------------------------------------------------
            \\
            \\The reconfiguration metadata size exceeds the compile-time limit.
            \\
            \\Hint: Increase 'max_metadata_bytes' in Options or trim the metadata payload size.
            ,
        error.LogSealed =>
            \\-- LOG SEALED -------------------------------------------------------------------
            \\
            \\A write proposal was rejected because the log has been sealed by a Stop Sign.
            \\
            \\Once a reconfiguration Stop Sign is accepted in the log, the current epoch is
            \\closed. No further appends are permitted.
            \\
            \\Hint: Apply the Stop Sign commit and start a new epoch with the new configuration.
            ,
        error.BatchTooLarge =>
            \\-- BATCH TOO LARGE --------------------------------------------------------------
            \\
            \\The proposed batch size exceeds the compile-time maximum batch size.
            \\
            \\Hint: Decrease the batch size or increase 'max_batch' in Options.
            ,
        error.ConfigurationIdRegression =>
            \\-- CONFIGURATION ID REGRESSION --------------------------------------------------
            \\
            \\The proposed reconfiguration ID is less than or equal to the current epoch ID.
            \\
            \\Epoch configurations must advance monotonically. You cannot switch to an older
            \\or identical configuration ID.
            \\
            \\Hint: Ensure the new configuration ID is strictly greater than the current one.
            ,
        error.ConfigurationIdExhausted =>
            \\-- CONFIGURATION ID EXHAUSTED ---------------------------------------------------
            \\
            \\The reconfiguration ID has reached its maximum u64 limit.
            \\
            \\Hint: Reset the cluster state with a fresh bootstrap if configuration IDs are exhausted.
            ,
        else =>
            \\-- UNEXPECTED ERROR -------------------------------------------------------------
            \\
            \\An unrecognized error occurred in the Paxos state engine.
            \\
            \\Hint: This may be an operating system error (such as Disk Full or I/O failure)
            \\propagating through the host boundary. Check host logs for details.
            ,
    };
}

test "explainError - human friendly messages" {
    const std = @import("std");
    const msg1 = explainError(error.EmptyMembership);
    try std.testing.expect(std.mem.indexOf(u8, msg1, "-- EMPTY MEMBERSHIP") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg1, "Hint: Pass a slice") != null);

    const msg2 = explainError(error.NotLeader);
    try std.testing.expect(std.mem.indexOf(u8, msg2, "-- NOT LEADER") != null);

    const msg3 = explainError(error.UnknownErrorStringOrSentinel);
    try std.testing.expect(std.mem.indexOf(u8, msg3, "-- UNEXPECTED ERROR") != null);
}
