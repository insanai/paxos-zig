//! A pure, bounded Multi-Paxos state machine with explicit effects.
//!
//! The core owns no sockets, threads, clocks, or heap. The host feeds one
//! `Envelope`, tick, or proposal into a `Node` and receives `Effects`: durable
//! writes, outbound messages, and newly decided entries. Safety rests on the
//! host honoring one ordering rule: persist and sync every record in
//! `writesSlice`, call `confirmWritesDurable`, and only then transmit
//! `messagesSlice`. A reply sent before its write is durable can retract a
//! promise or vote after a crash and break agreement.

const std = @import("std");
const BitSet = @import("bit_set.zig").BitSet;

/// Stable identity of one voting member. Zero is reserved.
pub const NodeId = u32;
/// One-based position in the global protocol log. Zero means no slot.
/// A 64-bit slot never resets over a database lifetime (ZDS 0011).
pub const Slot = u64;

/// Compile-time capacities, quorum policy, and logical clock intervals.
pub const Options = struct {
    /// Maximum members represented by fixed arrays.
    max_members: usize = 7,
    /// Maximum slots retained by this protocol instance.
    max_slots: usize = 256,
    /// Phase one quorum, or a majority when null.
    read_quorum_size: ?u16 = null,
    /// Phase two quorum, or a majority when null.
    write_quorum_size: ?u16 = null,
    /// Follower ticks without leader contact before campaigning.
    election_timeout_ticks: u32 = 10,
    /// Leader ticks between heartbeat broadcasts.
    heartbeat_interval_ticks: u32 = 3,
    /// Leader ticks between bounded retransmission scans.
    resend_interval_ticks: u32 = 10,
};

/// Whether the generated type enforces the persist-then-send ordering
/// contract at runtime. `Protocol` always selects `.enforced`; the
/// `host_managed` namespace is the only path to `.host_managed`.
pub const DurabilityGate = enum { enforced, host_managed };

/// Reports a host ordering violation and stops the process in every build
/// mode. Continuing is not safe: a message claiming durability for a write
/// that was never confirmed can retract a promise or vote after a crash.
noinline fn hostOrderViolation(comptime what: []const u8) noreturn {
    @branchHint(.cold);
    std.debug.print("paxos: " ++ what ++ "\n", .{});
    std.process.abort();
}

/// A totally ordered, globally unique proposal attempt.
pub const Ballot = struct {
    round: u64,
    priority: u32 = 0,
    node: NodeId,

    /// Orders below every real ballot. Marks "no promise yet" and tags votes
    /// synthesized for decrees learned without voting (see `Promise`).
    pub const zero: Ballot = .{ .round = 0, .node = 0 };

    /// Compares round, configured priority, and node ID in that order.
    pub fn order(a: Ballot, b: Ballot) std.math.Order {
        if (a.round < b.round) return .lt;
        if (a.round > b.round) return .gt;
        if (a.priority < b.priority) return .lt;
        if (a.priority > b.priority) return .gt;
        if (a.node < b.node) return .lt;
        if (a.node > b.node) return .gt;
        return .eq;
    }

    /// Returns true when `a` is ordered before `b`.
    pub fn lessThan(a: Ballot, b: Ballot) bool {
        return a.order(b) == .lt;
    }

    /// Returns true when every ballot component is equal.
    pub fn eql(a: Ballot, b: Ballot) bool {
        return a.order(b) == .eq;
    }
};

/// Returns a bounded Paxos implementation for a copyable application value.
///
/// Slots are one-based. Slot zero is reserved as the "nothing committed" marker.
/// Values should be self-contained (for example, an ID or fixed-size command),
/// since message serialization and ownership remain the application's concern.
pub fn Protocol(comptime Value: type, comptime options: Options) type {
    return ProtocolGated(Value, options, .enforced);
}

/// Shared implementation behind `Protocol` and `host_managed.Protocol`.
/// Reached from `host_managed.zig` only; root.zig does not re-export it.
pub fn ProtocolGated(
    comptime Value: type,
    comptime options: Options,
    comptime gate: DurabilityGate,
) type {
    comptime {
        if (options.max_members == 0) {
            @compileError("paxos Protocol option max_members must be greater than zero");
        }
        if (options.max_slots == 0) {
            @compileError("paxos Protocol option max_slots must be greater than zero");
        }
        if (options.max_members > std.math.maxInt(u16)) {
            @compileError("paxos Protocol option max_members must be at most 65535");
        }
        if (options.election_timeout_ticks == 0) {
            @compileError("paxos Protocol option election_timeout_ticks must be greater than zero");
        }
        if (options.heartbeat_interval_ticks == 0) {
            @compileError("paxos Protocol option heartbeat_interval_ticks " ++
                "must be greater than zero");
        }
        if (options.resend_interval_ticks == 0) {
            @compileError("paxos Protocol option resend_interval_ticks must be greater than zero");
        }
        const wide_messages = @as(u128, options.max_members) * options.max_slots +
            options.max_members + 1;
        if (wide_messages > std.math.maxInt(usize)) {
            @compileError("paxos Protocol options max_members and max_slots overflow " ++
                "the derived message capacity");
        }
        const wide_writes = @as(u128, options.max_slots) * 2 + 1;
        if (wide_writes > std.math.maxInt(usize)) {
            @compileError("paxos Protocol option max_slots overflows the derived write capacity");
        }
        if (hasPointers(Value)) {
            @compileError(
                "Value type '" ++ @typeName(Value) ++
                    "' must not contain pointers, slices, or references.",
            );
        }
    }

    const max_messages = options.max_members * options.max_slots + options.max_members + 1;
    // A one-node quorum can accept and commit every recovered slot in one step.
    const max_writes = options.max_slots * 2 + 1;
    const MemberSet = BitSet(options.max_members);
    const SlotSet = BitSet(options.max_slots);

    return struct {
        const Self = @This();

        /// One acceptor vote: the ballot it was cast in and the value voted
        /// for. Phase one keeps the highest-ballot vote per slot, which is
        /// exactly the evidence condition B3 requires a new leader to honor.
        pub const Accepted = struct {
            ballot: Ballot,
            value: Value,
        };

        /// Fixed voting membership and validated quorum sizes.
        pub const Membership = struct {
            ids: [options.max_members]NodeId = [_]NodeId{0} ** options.max_members,
            count: u16 = 0,
            read_quorum_size: u16 = 0,
            write_quorum_size: u16 = 0,

            /// Replaces `self` with a validated membership.
            pub fn init(self: *Membership, node_ids: []const NodeId) !void {
                if (node_ids.len == 0) return error.EmptyMembership;
                if (node_ids.len > options.max_members) return error.TooManyMembers;

                self.* = .{};
                for (node_ids, 0..) |id, index| {
                    if (id == 0) return error.InvalidNodeId;
                    if (self.indexOf(id) != null) return error.DuplicateNodeId;
                    self.ids[index] = id;
                    self.count += 1;
                }
                const majority: u16 = self.count / 2 + 1;
                self.read_quorum_size = options.read_quorum_size orelse majority;
                self.write_quorum_size = options.write_quorum_size orelse majority;
                if (self.read_quorum_size == 0 or self.read_quorum_size > self.count) {
                    return error.InvalidReadQuorum;
                }
                if (self.write_quorum_size == 0 or self.write_quorum_size > self.count) {
                    return error.InvalidWriteQuorum;
                }
                const quorum_sum = @as(u32, self.read_quorum_size) + self.write_quorum_size;
                if (quorum_sum <= self.count) return error.NonIntersectingQuorums;
                std.debug.assert(self.count == node_ids.len);
                std.debug.assert(self.readQuorum() <= self.count);
                std.debug.assert(self.writeQuorum() <= self.count);
            }

            /// Returns the stable array index for a member ID.
            pub fn indexOf(self: *const Membership, id: NodeId) ?usize {
                for (self.ids[0..self.count], 0..) |member, index| {
                    if (member == id) return index;
                }
                return null;
            }

            /// Returns whether `id` is a member.
            pub fn contains(self: *const Membership, id: NodeId) bool {
                return self.indexOf(id) != null;
            }

            /// Compatibility alias for the phase two quorum.
            pub fn quorum(self: *const Membership) usize {
                return self.writeQuorum();
            }

            /// Number of complete promises required to finish phase one.
            pub fn readQuorum(self: *const Membership) usize {
                return self.read_quorum_size;
            }

            /// Number of durable acceptances required to choose a value.
            pub fn writeQuorum(self: *const Membership) usize {
                return self.write_quorum_size;
            }
        };

        /// Phase one request (Lamport's NextBallot). `decided_through` is the
        /// candidate's contiguous decided prefix; promisers report votes only
        /// for slots above it, bounding the size of the reply stream.
        pub const Prepare = struct { ballot: Ballot, decided_through: Slot };
        /// Phase one reply carrying one slot's LastVote evidence. An acceptor
        /// sends one promise per known slot above the candidate's decided
        /// prefix and marks the end of the stream with `PromiseDone`. A decree
        /// this node learned without ever voting travels as a zero-ballot
        /// vote: it loses to every real vote, so it can never override the
        /// choosing quorum's value, yet it lets a candidate that is behind on
        /// the log recover already-committed decrees.
        pub const Promise = struct {
            ballot: Ballot,
            slot: Slot,
            accepted: Accepted,
        };
        /// Phase one completion marker. `accepted_count` is how many `Promise`
        /// messages this acceptor sent for `ballot`, so the candidate can
        /// recognize a complete promise set even when the network reorders
        /// the marker ahead of the promises it counts.
        pub const PromiseDone = struct {
            ballot: Ballot,
            accepted_count: Slot,
            decided_through: Slot,
        };
        /// Phase two request (Lamport's BeginBallot): vote for `value` in
        /// `slot` unless a higher ballot has already been promised.
        pub const Accept = struct {
            ballot: Ballot,
            slot: Slot,
            value: Value,
        };
        /// Phase two vote acknowledgement. Sent only after the vote is
        /// durable, so a leader counting these toward the write quorum is
        /// counting synced disks, not volatile memory. `decided_through`
        /// refreshes the leader's retransmission view of this acceptor.
        pub const AcceptedMessage = struct {
            ballot: Ballot,
            slot: Slot,
            decided_through: Slot,
        };
        /// Announcement that `value` was chosen for `slot`. Commits carry no
        /// ballot: a chosen value can never change, so any commit for a slot
        /// is authoritative regardless of which leader sends it.
        pub const Commit = struct {
            slot: Slot,
            value: Value,
        };
        /// Catch-up request: send every known commit at or above `from_slot`.
        pub const Learn = struct { from_slot: Slot };
        /// Rejection of `rejected` by an acceptor already promised to
        /// `promised`. Nacks are a liveness aid only: they steer the loser
        /// back to follower and seed its next ballot round. Safety never
        /// depends on a nack arriving.
        pub const Nack = struct {
            rejected: Ballot,
            promised: Ballot,
            decided_through: Slot,
        };
        /// Leader liveness beacon. `decided_through` advertises the leader's
        /// decided prefix so a restarted or lagging follower can request
        /// catch-up without any explicit recovery handshake.
        pub const Heartbeat = struct { ballot: Ballot, decided_through: Slot };

        /// The complete wire vocabulary. Serialization is the host's concern;
        /// every payload is self-contained and free of pointers.
        pub const Message = union(enum) {
            prepare: Prepare,
            promise: Promise,
            promise_done: PromiseDone,
            accept: Accept,
            accepted: AcceptedMessage,
            commit: Commit,
            learn: Learn,
            nack: Nack,
            heartbeat: Heartbeat,
        };

        /// One addressed message. `step` rejects envelopes not addressed to
        /// the local node and senders outside the fixed membership, but it
        /// trusts `from`; the host transport must authenticate the sender.
        pub const Envelope = struct {
            from: NodeId,
            to: NodeId,
            message: Message,
        };

        /// A durable-state delta. Apply in order before publishing any message
        /// that claims durable state. Hosts may pipeline only the narrow
        /// request class returned by `Effects.preDurableMessages`.
        pub const Write = union(enum) {
            promise: Ballot,
            accept: struct {
                ballot: Ballot,
                slot: Slot,
                value: Value,
            },
            commit: struct {
                slot: Slot,
                value: Value,
            },
        };

        /// One decided entry released to the application. Entries are always
        /// released in slot order as a contiguous prefix, never with gaps.
        pub const Committed = struct {
            slot: Slot,
            value: Value,
        };

        /// Caller-owned output buffers for one atomic protocol transition.
        pub const Effects = struct {
            writes: [max_writes]Write = undefined,
            writes_count: usize = 0,
            messages: [max_messages]Envelope = undefined,
            messages_count: usize = 0,
            committed: [options.max_slots]Committed = undefined,
            committed_count: usize = 0,
            writes_confirmed: bool = true,

            /// Initializes only active counts, leaving large backing arrays untouched.
            pub fn init(self: *Effects) void {
                self.writes_count = 0;
                self.messages_count = 0;
                self.committed_count = 0;
                self.writes_confirmed = true;
            }

            /// Clears counts without touching fixed backing storage.
            pub fn reset(self: *Effects) void {
                std.debug.assert(self.writes_count <= self.writes.len);
                std.debug.assert(self.messages_count <= self.messages.len);
                std.debug.assert(self.committed_count <= self.committed.len);
                if (comptime gate == .enforced) {
                    // A nonempty write batch was discarded without the host
                    // confirming durability; replies for it may already be
                    // out. `writes_confirmed` is false only while such a
                    // batch is pending, so one load decides.
                    if (!self.writes_confirmed) {
                        hostOrderViolation("reset discarded unconfirmed writes");
                    }
                }
                self.writes_count = 0;
                self.messages_count = 0;
                self.committed_count = 0;
                self.writes_confirmed = true;
            }

            /// Durable records in required application order.
            pub fn writesSlice(self: *const Effects) []const Write {
                return self.writes[0..self.writes_count];
            }

            /// Whether this batch contains a promise or vote whose loss after
            /// it is published could violate Paxos safety. Those records need
            /// the host's strongest power-loss barrier. A commit-only batch is
            /// different: the value was already chosen from durable quorum
            /// evidence, so its local commit marker is reconstructible during
            /// phase one. Hosts may omit that derived marker, or append it
            /// without paying a second device-cache barrier.
            pub fn requiresPowerLossBarrier(self: *const Effects) bool {
                for (self.writesSlice()) |write| switch (write) {
                    .promise, .accept => return true,
                    .commit => {},
                };
                return false;
            }

            /// Returns an iterator over the narrow class of outbound messages
            /// that a host may release while this transition's writes are
            /// being synced.  Today that class contains only phase-two
            /// `accept` requests: they ask remote acceptors to make a vote
            /// durable but make no claim that the sender's own vote is
            /// durable.  Promise streams and `accepted` replies remain behind
            /// `confirmWritesDurable` because they do carry durable claims.
            ///
            /// A pipelining host must still serialize this node: it may not
            /// feed a reply or start another transition until it has persisted
            /// `writesSlice` and called `confirmWritesDurable`.  This lets a
            /// leader overlap its local barrier with follower barriers without
            /// counting volatile state toward a quorum.
            pub fn preDurableMessages(self: *const Effects) PreDurableMessageIterator {
                return .{ .messages = self.messages[0..self.messages_count] };
            }

            /// Records that the host satisfied the persistence contract for
            /// every pending write. If `requiresPowerLossBarrier` is true this
            /// means the strongest durable barrier; a commit-only batch may be
            /// omitted or weakly persisted because it is derived from already
            /// durable quorum evidence. Call before `messagesSlice`.
            pub fn confirmWritesDurable(self: *Effects) void {
                self.writes_confirmed = true;
            }

            /// Messages that may be sent only after all writes are synced.
            /// Every build mode checks that `confirmWritesDurable` ran for
            /// this batch and stops the process on a violation.
            pub fn messagesSlice(self: *const Effects) []const Envelope {
                if (comptime gate == .enforced) {
                    // False only while an unconfirmed write batch is pending.
                    if (!self.writes_confirmed) {
                        hostOrderViolation("messagesSlice before confirmWritesDurable");
                    }
                }
                return self.messages[0..self.messages_count];
            }

            pub const PreDurableMessageIterator = struct {
                messages: []const Envelope,
                index: usize = 0,

                pub fn next(self: *PreDurableMessageIterator) ?Envelope {
                    while (self.index < self.messages.len) {
                        const envelope = self.messages[self.index];
                        self.index += 1;
                        switch (envelope.message) {
                            .accept => return envelope,
                            else => {},
                        }
                    }
                    return null;
                }
            };

            /// Newly available contiguous application entries.
            pub fn committedSlice(self: *const Effects) []const Committed {
                return self.committed[0..self.committed_count];
            }

            fn addWrite(self: *Effects, write: Write) void {
                std.debug.assert(self.writes_count < self.writes.len);
                self.writes[self.writes_count] = write;
                self.writes_count += 1;
                self.writes_confirmed = false;
                std.debug.assert(self.writes_count <= self.writes.len);
            }

            fn addMessage(self: *Effects, envelope: Envelope) void {
                std.debug.assert(self.messages_count < self.messages.len);
                self.messages[self.messages_count] = envelope;
                self.messages_count += 1;
                std.debug.assert(self.messages_count <= self.messages.len);
            }

            fn addCommitted(self: *Effects, committed: Committed) void {
                std.debug.assert(self.committed_count < self.committed.len);
                self.committed[self.committed_count] = committed;
                self.committed_count += 1;
                std.debug.assert(self.committed_count <= self.committed.len);
            }
        };

        /// Durable acceptor and learner state reconstructed by journal replay.
        pub const DurableState = struct {
            promised: Ballot = Ballot.zero,
            accepted: [options.max_slots]?Accepted = [_]?Accepted{null} ** options.max_slots,
            committed: [options.max_slots]?Value = [_]?Value{null} ** options.max_slots,

            /// Replays one journal record in original write order. Regressed
            /// promises and conflicting values are reported as errors, not
            /// repaired: a journal that violates durable monotonicity is
            /// corrupt, and the node must stop rather than vote from it.
            pub fn apply(self: *DurableState, write: Write) !void {
                self.assertValid();
                switch (write) {
                    .promise => |ballot| {
                        if (ballot.lessThan(self.promised)) return error.PromiseRegression;
                        self.promised = ballot;
                    },
                    .accept => |accepted| {
                        const index = try slotIndex(accepted.slot);
                        if (accepted.ballot.lessThan(self.promised)) {
                            return error.PromiseRegression;
                        }
                        if (self.accepted[index]) |stored| {
                            if (stored.ballot.eql(accepted.ballot) and
                                !std.meta.eql(stored.value, accepted.value))
                            {
                                return error.ConflictingValue;
                            }
                        }
                        self.promised = accepted.ballot;
                        self.accepted[index] = .{
                            .ballot = accepted.ballot,
                            .value = accepted.value,
                        };
                    },
                    .commit => |committed| {
                        const index = try slotIndex(committed.slot);
                        if (self.committed[index]) |value| {
                            if (!std.meta.eql(value, committed.value)) {
                                return error.ConflictingCommit;
                            }
                        }
                        // A vote for another value may coexist with the
                        // commit; see `recordCommit` for why that is legal.
                        self.committed[index] = committed.value;
                    },
                }
                self.assertValid();
            }

            fn assertValid(self: *const DurableState) void {
                if (!std.debug.runtime_safety) return;
                for (self.accepted) |accepted| {
                    if (accepted) |value| {
                        std.debug.assert(!self.promised.lessThan(value.ballot));
                    }
                }
            }
        };

        /// Proposer status. A `follower` acts only as acceptor and learner,
        /// `preparing` is running phase one, and a `leader` has completed
        /// phase one and may propose new values.
        pub const Role = enum { follower, preparing, leader };

        /// One protocol participant containing proposer, acceptor, and learner roles.
        pub const Node = struct {
            id: NodeId,
            membership: Membership,
            durable: DurableState,
            role: Role = .follower,
            ballot: Ballot = Ballot.zero,
            highest_observed_round: u64 = 0,
            leader_hint: ?NodeId = null,
            next_slot: Slot = 1,
            delivered_through: Slot = 0,
            leader_priority: u32 = 0,
            voting_member: bool = true,
            /// Acceptor-only participants keep voting but never start phase one.
            /// This is a liveness policy; it does not change Paxos safety.
            campaign_enabled: bool = true,
            election_ticks: u32 = 0,
            heartbeat_ticks: u32 = 0,
            resend_ticks: u32 = 0,
            peer_decided_through: [options.max_members]Slot =
                [_]Slot{0} ** options.max_members,

            noop: Value = undefined,
            noop_set: bool = false,
            promise_done: [options.max_members]bool =
                [_]bool{false} ** options.max_members,
            promise_expected: [options.max_members]Slot =
                [_]Slot{0} ** options.max_members,
            promise_received: [options.max_members]Slot =
                [_]Slot{0} ** options.max_members,
            promise_seen: [options.max_members]SlotSet =
                [_]SlotSet{.{}} ** options.max_members,
            recovered: [options.max_slots]?Accepted =
                [_]?Accepted{null} ** options.max_slots,
            proposals: [options.max_slots]?Value =
                [_]?Value{null} ** options.max_slots,
            acknowledgements: [options.max_slots]MemberSet =
                [_]MemberSet{.{}} ** options.max_slots,

            /// Initializes a member with priority zero.
            pub fn init(self: *Node, id: NodeId, membership: *const Membership) !void {
                try self.initWithPriority(id, membership, 0);
            }

            /// Initializes a member with an election priority.
            pub fn initWithPriority(
                self: *Node,
                id: NodeId,
                membership: *const Membership,
                leader_priority: u32,
            ) !void {
                if (!membership.contains(id)) return error.NotMember;
                self.* = .{
                    .id = id,
                    .membership = membership.*,
                    .durable = .{},
                    .leader_priority = leader_priority,
                };
                self.assertValid();
            }

            /// Initializes a non-voting learner for host-certified decisions.
            /// The learner ID must be outside the acceptor membership.
            pub fn initLearner(
                self: *Node,
                id: NodeId,
                membership: *const Membership,
            ) !void {
                if (id == 0) return error.InvalidNodeId;
                if (membership.contains(id)) return error.LearnerIsVoter;
                self.* = .{
                    .id = id,
                    .membership = membership.*,
                    .durable = .{},
                    .voting_member = false,
                    .campaign_enabled = false,
                };
                self.assertValid();
            }

            /// Restores a priority-zero member from replayed durable state.
            ///
            /// `decidedThrough()` reports 0 after restore until this node next
            /// observes a commit or wins an election, so `readDecided` cannot
            /// replay history yet. The host must rebuild application state
            /// from its own snapshot, not from this node.
            pub fn restore(
                self: *Node,
                id: NodeId,
                membership: *const Membership,
                durable: *const DurableState,
            ) !void {
                try self.restoreWithPriority(id, membership, durable, 0);
            }

            /// Restores a prioritized member from replayed durable state.
            ///
            /// `decidedThrough()` reports 0 after restore until this node next
            /// observes a commit or wins an election, so `readDecided` cannot
            /// replay history yet. The host must rebuild application state
            /// from its own snapshot, not from this node.
            pub fn restoreWithPriority(
                self: *Node,
                id: NodeId,
                membership: *const Membership,
                durable: *const DurableState,
                leader_priority: u32,
            ) !void {
                try self.initWithPriority(id, membership, leader_priority);
                self.durable = durable.*;
                self.next_slot = slotAfter(self.highestUsedSlot());
                self.assertValid();
            }

            /// Restores a non-voting learner from its commit-only journal.
            pub fn restoreLearner(
                self: *Node,
                id: NodeId,
                membership: *const Membership,
                durable: *const DurableState,
            ) !void {
                try self.initLearner(id, membership);
                self.durable = durable.*;
                self.next_slot = slotAfter(self.highestUsedSlot());
                self.assertValid();
            }

            /// Starts phase one with a locally unique, monotonically increasing ballot.
            pub fn campaign(self: *Node, noop: Value, effects: *Effects) !void {
                self.assertValid();
                effects.reset();
                if (!self.voting_member) return error.NotVoter;
                if (!self.campaign_enabled) return error.CampaignDisabled;
                try self.startCampaign(noop, effects);
                self.assertValid();
            }

            /// Enables or disables automatic and explicit phase-one campaigns.
            /// A disabled node remains a full acceptor and learner.
            pub fn setCampaignEnabled(self: *Node, enabled: bool) void {
                self.assertValid();
                self.campaign_enabled = enabled;
                if (!enabled and self.role == .preparing) self.role = .follower;
                self.assertValid();
            }

            /// Proposes a value after phase one has established this node as leader.
            pub fn propose(self: *Node, value: Value, effects: *Effects) !Slot {
                self.assertValid();
                effects.reset();
                if (!self.voting_member) return error.NotVoter;
                if (self.role != .leader) return error.NotLeader;
                if (self.next_slot == 0) return error.SlotLimitReached;

                const slot = self.next_slot;
                _ = try slotIndex(slot);
                self.next_slot = slotAfter(slot);
                try self.sendAccept(slot, value, effects);
                self.assertValid();
                return slot;
            }

            /// Proposes several values while returning one combined effect batch.
            pub fn proposeBatch(
                self: *Node,
                values: []const Value,
                slots: []Slot,
                effects: *Effects,
            ) ![]const Slot {
                self.assertValid();
                effects.reset();
                if (!self.voting_member) return error.NotVoter;
                if (self.role != .leader) return error.NotLeader;
                if (values.len == 0) return error.EmptyBatch;
                if (slots.len < values.len) return error.SlotBufferTooSmall;
                if (self.next_slot == 0) return error.SlotLimitReached;

                const first_index = try slotIndex(self.next_slot);
                const remaining = options.max_slots - first_index;
                if (values.len > remaining) return error.SlotLimitReached;

                for (values, slots[0..values.len]) |value, *slot| {
                    slot.* = self.next_slot;
                    self.next_slot = slotAfter(self.next_slot);
                    try self.sendAccept(slot.*, value, effects);
                }
                self.assertValid();
                return slots[0..values.len];
            }

            /// Advances failure detection, heartbeats, and bounded retransmission.
            /// Call at a stable application-defined interval.
            pub fn tick(self: *Node, noop: Value, effects: *Effects) !void {
                self.assertValid();
                effects.reset();
                if (!self.voting_member) return;
                self.election_ticks +|= 1;
                self.heartbeat_ticks +|= 1;
                self.resend_ticks +|= 1;

                if (self.role == .leader) {
                    self.sendHeartbeatIfDue(effects);
                    self.resendIfDue(effects);
                } else if (self.campaign_enabled and
                    self.election_ticks >= options.election_timeout_ticks)
                {
                    try self.startCampaign(noop, effects);
                }
                self.assertValid();
            }

            /// Repairs protocol traffic after the transport reconnects to one peer.
            pub fn reconnected(self: *Node, peer: NodeId, effects: *Effects) !void {
                self.assertValid();
                effects.reset();
                if (!self.membership.contains(peer)) return error.NotMember;
                if (peer == self.id) return error.InvalidPeer;

                if (self.role == .leader) {
                    self.resendTo(peer, effects);
                } else if (self.leader_hint == peer) {
                    const from_slot = slotAfter(self.delivered_through);
                    if (from_slot == 0) return;
                    self.sendTo(peer, effects, .{ .learn = .{
                        .from_slot = from_slot,
                    } });
                }
                self.assertValid();
            }

            /// Requests committed entries from a peer, starting at `from_slot`.
            pub fn requestCatchUp(
                self: *Node,
                peer: NodeId,
                from_slot: Slot,
                effects: *Effects,
            ) !void {
                self.assertValid();
                effects.reset();
                if (!self.membership.contains(peer)) return error.NotMember;
                if (from_slot == 0) return error.InvalidSlot;
                effects.addMessage(.{
                    .from = self.id,
                    .to = peer,
                    .message = .{ .learn = .{ .from_slot = from_slot } },
                });
                self.assertValid();
            }

            /// Processes one authenticated message from the fixed membership.
            pub fn step(self: *Node, envelope: Envelope, effects: *Effects) !void {
                self.assertValid();
                effects.reset();
                if (envelope.to != self.id) return error.WrongRecipient;
                const member = self.membership.indexOf(envelope.from) orelse return error.NotMember;

                if (extractDecidedThrough(envelope.message)) |dt| {
                    self.peer_decided_through[member] = @max(self.peer_decided_through[member], dt);
                }

                if (!self.voting_member) {
                    return switch (envelope.message) {
                        .commit => |message| self.onCommit(
                            envelope.from,
                            message,
                            effects,
                        ),
                        else => error.LearnerMessageForbidden,
                    };
                }

                switch (envelope.message) {
                    .prepare => |message| try self.onPrepare(
                        envelope.from,
                        message.ballot,
                        message.decided_through,
                        effects,
                    ),
                    .promise => |message| try self.onPromise(
                        envelope.from,
                        message,
                        effects,
                    ),
                    .promise_done => |message| try self.onPromiseDone(
                        envelope.from,
                        message,
                        effects,
                    ),
                    .accept => |message| try self.onAccept(
                        envelope.from,
                        message,
                        effects,
                    ),
                    .accepted => |message| try self.onAccepted(
                        envelope.from,
                        message,
                        effects,
                    ),
                    .commit => |message| try self.onCommit(
                        envelope.from,
                        message,
                        effects,
                    ),
                    .learn => |message| try self.onLearn(
                        envelope.from,
                        message.from_slot,
                        effects,
                    ),
                    .nack => |message| self.onNack(envelope.from, message),
                    .heartbeat => |message| self.onHeartbeat(
                        envelope.from,
                        message,
                        effects,
                    ),
                }
                self.assertValid();
            }

            /// Installs one value that the host has certified as chosen by the
            /// current acceptor configuration. Intended for non-voting learners.
            pub fn learnChosen(
                self: *Node,
                from: NodeId,
                slot: Slot,
                value: Value,
                effects: *Effects,
            ) !void {
                self.assertValid();
                effects.reset();
                if (self.voting_member) return error.NotLearner;
                if (!self.membership.contains(from)) return error.NotMember;
                try self.onCommit(from, .{ .slot = slot, .value = value }, effects);
                self.assertValid();
            }

            /// Returns a learned value, or null for an undecided or invalid slot.
            pub fn committedAt(self: *const Node, slot: Slot) ?Value {
                self.assertValid();
                const index = slotIndex(slot) catch return null;
                return self.durable.committed[index];
            }

            /// Returns the last observed leader, if any.
            pub fn currentLeader(self: *const Node) ?NodeId {
                self.assertValid();
                return self.leader_hint;
            }

            /// Returns the highest contiguous slot released to the application.
            pub fn decidedThrough(self: *const Node) Slot {
                self.assertValid();
                return self.delivered_through;
            }

            /// Copies a decided contiguous suffix into caller-owned storage.
            pub fn readDecided(
                self: *const Node,
                from_slot: Slot,
                output: []Committed,
            ) ![]const Committed {
                self.assertValid();
                if (from_slot == 0) return error.InvalidSlot;
                if (from_slot > self.delivered_through) return output[0..0];

                const available = self.delivered_through - from_slot + 1;
                if (output.len < available) return error.ReadBufferTooSmall;
                for (0..available) |offset| {
                    const slot = from_slot + @as(Slot, @intCast(offset));
                    output[offset] = .{ .slot = slot, .value = self.committedAt(slot).? };
                }
                return output[0..available];
            }

            fn assertValid(self: *const Node) void {
                if (!std.debug.runtime_safety) return;
                std.debug.assert(self.id != 0);
                if (self.voting_member) {
                    std.debug.assert(self.membership.contains(self.id));
                } else {
                    std.debug.assert(!self.membership.contains(self.id));
                    std.debug.assert(!self.campaign_enabled);
                }
                std.debug.assert(self.membership.count > 0);
                std.debug.assert(self.membership.count <= options.max_members);
                std.debug.assert(self.next_slot <= options.max_slots);
                std.debug.assert(self.delivered_through <= options.max_slots);

                self.durable.assertValid();
            }

            /// Lamport step 1: choose a ballot above every ballot seen,
            /// owned by this node, and broadcast NextBallot (prepare).
            fn startCampaign(self: *Node, noop: Value, effects: *Effects) !void {
                const greatest_round = @max(
                    @max(self.ballot.round, self.durable.promised.round),
                    self.highest_observed_round,
                );
                if (greatest_round == std.math.maxInt(u64)) return error.BallotExhausted;

                self.ballot = .{
                    .round = greatest_round + 1,
                    .priority = self.leader_priority,
                    .node = self.id,
                };
                self.role = .preparing;
                self.leader_hint = null;
                self.noop = noop;
                self.noop_set = true;
                self.election_ticks = 0;
                self.clearElection();
                self.broadcast(effects, .{ .prepare = .{
                    .ballot = self.ballot,
                    .decided_through = self.delivered_through,
                } });
            }

            fn observeLeader(self: *Node, from: NodeId, ballot: Ballot) void {
                self.leader_hint = from;
                self.election_ticks = 0;
                self.highest_observed_round = @max(
                    self.highest_observed_round,
                    ballot.round,
                );
                if (!self.ballot.eql(ballot)) self.role = .follower;
            }

            /// Lamport step 2: promise a ballot not below the current one
            /// and reply with LastVote information for every slot above
            /// the proposer's decided prefix, including decrees learned
            /// without voting (paper section 3.1) as zero-ballot votes.
            fn onPrepare(
                self: *Node,
                from: NodeId,
                ballot: Ballot,
                decided_through: Slot,
                effects: *Effects,
            ) !void {
                if (ballot.lessThan(self.durable.promised)) {
                    self.sendNack(from, ballot, effects);
                    return;
                }

                if (!ballot.eql(self.durable.promised)) {
                    self.durable.promised = ballot;
                    effects.addWrite(.{ .promise = ballot });
                }
                self.observeLeader(from, ballot);
                if (self.ballot.lessThan(ballot)) self.role = .follower;

                var accepted_count: Slot = 0;
                var slot = decided_through + 1;
                while (slot <= options.max_slots) {
                    std.debug.assert(slot > 0);
                    const index = @as(usize, slot - 1);
                    // Report this slot's vote. A decree that was learned
                    // without voting still travels as a zero-ballot vote:
                    // the paper's parliamentary protocol has legislators
                    // return already-passed decrees with their LastVote
                    // reply so a president behind on the log recovers them.
                    // A zero ballot loses to every real vote, so it can
                    // never override the choosing quorum's value.
                    var known = self.durable.accepted[index];
                    if (known == null) {
                        if (self.durable.committed[index]) |value| {
                            known = .{ .ballot = Ballot.zero, .value = value };
                        }
                    }
                    if (known) |value| {
                        accepted_count += 1;
                        effects.addMessage(.{
                            .from = self.id,
                            .to = from,
                            .message = .{ .promise = .{
                                .ballot = ballot,
                                .slot = slot,
                                .accepted = value,
                            } },
                        });
                    }
                    if (slot == options.max_slots) break;
                    slot = slotAfter(slot);
                }
                effects.addMessage(.{
                    .from = self.id,
                    .to = from,
                    .message = .{ .promise_done = .{
                        .ballot = ballot,
                        .accepted_count = accepted_count,
                        .decided_through = self.delivered_through,
                    } },
                });
            }

            /// Lamport step 3 (collection): keep the highest-ballot vote
            /// per slot across the promise quorum, as condition B3 needs.
            fn onPromise(
                self: *Node,
                from: NodeId,
                message: Promise,
                effects: *Effects,
            ) !void {
                if (self.role != .preparing) return;
                if (!message.ballot.eql(self.ballot)) return;

                const member = self.membership.indexOf(from) orelse return;
                const index = slotIndex(message.slot) catch return;
                if (self.promise_seen[member].insert(index)) {
                    self.promise_received[member] += 1;
                }

                if (self.recovered[index]) |recovered| {
                    if (recovered.ballot.lessThan(message.accepted.ballot)) {
                        self.recovered[index] = message.accepted;
                    }
                } else {
                    self.recovered[index] = message.accepted;
                }
                try self.maybeBecomeLeader(effects);
            }

            fn onPromiseDone(
                self: *Node,
                from: NodeId,
                message: PromiseDone,
                effects: *Effects,
            ) !void {
                if (self.role != .preparing) return;
                if (!message.ballot.eql(self.ballot)) return;
                if (message.accepted_count > options.max_slots) return error.InvalidPromise;

                const member = self.membership.indexOf(from) orelse return;
                self.promise_done[member] = true;
                self.promise_expected[member] = message.accepted_count;
                try self.maybeBecomeLeader(effects);
            }

            /// Lamport step 3 (proposal): with complete promises from a
            /// read quorum, re-drive recovered values, fill gaps with the
            /// no-op decree, and release this node's own decided prefix.
            fn maybeBecomeLeader(self: *Node, effects: *Effects) !void {
                var complete: usize = 0;
                for (0..self.membership.count) |member| {
                    if (!self.promise_done[member]) continue;
                    if (self.promise_received[member] == self.promise_expected[member]) {
                        complete += 1;
                    }
                }
                if (complete < self.membership.readQuorum()) return;
                if (!self.noop_set) return error.MissingNoop;

                self.role = .leader;
                self.leader_hint = self.id;

                const highest = self.highestRecoveredSlot();
                var slot: Slot = 1;
                while (slot <= highest) {
                    std.debug.assert(slot > 0);
                    const index = try slotIndex(slot);
                    if (self.durable.committed[index]) |value| {
                        self.broadcastPeers(effects, .{ .commit = .{
                            .slot = slot,
                            .value = value,
                        } });
                    } else {
                        const value = if (self.recovered[index]) |accepted|
                            accepted.value
                        else
                            self.noop;
                        try self.sendAccept(slot, value, effects);
                    }
                    if (slot == highest) break;
                    slot = slotAfter(slot);
                }
                self.next_slot = slotAfter(highest);
                // A leader restored from its journal re-releases its own
                // contiguous committed prefix; peers hear the re-broadcast
                // commits above, but nobody sends commits to the leader.
                self.emitContiguous(effects);
            }

            /// Lamport step 4: vote unless the ballot is below the promise;
            /// the vote is durable before the reply may be sent.
            fn onAccept(
                self: *Node,
                from: NodeId,
                message: Accept,
                effects: *Effects,
            ) !void {
                const index = try slotIndex(message.slot);
                if (message.ballot.lessThan(self.durable.promised)) {
                    self.sendNack(from, message.ballot, effects);
                    return;
                }

                if (self.durable.accepted[index]) |accepted| {
                    if (accepted.ballot.eql(message.ballot)) {
                        if (!std.meta.eql(accepted.value, message.value)) {
                            return error.ConflictingValue;
                        }
                        effects.addMessage(.{
                            .from = self.id,
                            .to = from,
                            .message = .{ .accepted = .{
                                .ballot = message.ballot,
                                .slot = message.slot,
                                .decided_through = self.delivered_through,
                            } },
                        });
                        return;
                    }
                }

                self.durable.promised = message.ballot;
                self.durable.accepted[index] = .{
                    .ballot = message.ballot,
                    .value = message.value,
                };
                self.observeLeader(from, message.ballot);
                effects.addWrite(.{ .accept = .{
                    .ballot = message.ballot,
                    .slot = message.slot,
                    .value = message.value,
                } });
                effects.addMessage(.{
                    .from = self.id,
                    .to = from,
                    .message = .{ .accepted = .{
                        .ballot = message.ballot,
                        .slot = message.slot,
                        .decided_through = self.delivered_through,
                    } },
                });
            }

            /// Lamport step 5: a write quorum of votes for this ballot
            /// passes the decree.
            fn onAccepted(
                self: *Node,
                from: NodeId,
                message: AcceptedMessage,
                effects: *Effects,
            ) !void {
                if (self.role != .leader) return;
                if (!message.ballot.eql(self.ballot)) return;

                const index = try slotIndex(message.slot);
                const member = self.membership.indexOf(from) orelse return;
                _ = self.acknowledgements[index].insert(member);
                if (self.acknowledgements[index].count() < self.membership.writeQuorum()) {
                    return;
                }
                if (self.durable.committed[index] != null) return;

                const value = self.proposals[index] orelse return error.MissingProposedValue;

                try self.recordCommit(message.slot, value, effects);
                self.broadcastPeers(effects, .{ .commit = .{
                    .slot = message.slot,
                    .value = value,
                } });
            }

            /// Lamport step 6: write the passed decree in the ledger.
            fn onCommit(
                self: *Node,
                from: NodeId,
                message: Commit,
                effects: *Effects,
            ) !void {
                self.leader_hint = from;
                self.election_ticks = 0;
                try self.recordCommit(message.slot, message.value, effects);
            }

            fn onHeartbeat(
                self: *Node,
                from: NodeId,
                message: Heartbeat,
                effects: *Effects,
            ) void {
                if (message.ballot.lessThan(self.durable.promised)) {
                    self.sendNack(from, message.ballot, effects);
                    return;
                }
                if (!message.ballot.eql(self.durable.promised)) return;
                self.observeLeader(from, message.ballot);
                // A restarted follower re-learns silently: the heartbeat
                // advertises the leader's decided prefix, and the learn reply
                // also corrects the leader's stale view of this follower.
                if (message.decided_through > self.delivered_through) {
                    self.sendTo(from, effects, .{ .learn = .{
                        .from_slot = self.delivered_through + 1,
                    } });
                }
            }

            fn recordCommit(self: *Node, slot: Slot, value: Value, effects: *Effects) !void {
                const index = try slotIndex(slot);
                if (self.durable.committed[index]) |committed| {
                    if (!std.meta.eql(committed, value)) return error.ConflictingCommit;
                    // A duplicate commit still releases the contiguous
                    // prefix: after a restart the replayed log is committed
                    // but undelivered, and this is where delivery resumes.
                    self.emitContiguous(effects);
                    return;
                }
                // A differing accepted entry is legal: this node may hold a
                // stale vote from a ballot that never won while a quorum it
                // was not part of chose another value. The commit is
                // authoritative; the stale vote stays and stays harmless
                // because every read quorum intersects the choosing write
                // quorum, whose higher-ballot votes carry the chosen value.
                self.durable.committed[index] = value;
                effects.addWrite(.{ .commit = .{
                    .slot = slot,
                    .value = value,
                } });
                self.emitContiguous(effects);
            }

            fn emitContiguous(self: *Node, effects: *Effects) void {
                var slot = self.delivered_through + 1;
                while (slot <= options.max_slots) {
                    std.debug.assert(slot > 0);
                    const index = @as(usize, slot - 1);
                    const value = self.durable.committed[index] orelse break;
                    effects.addCommitted(.{ .slot = slot, .value = value });
                    self.delivered_through = slot;
                    if (slot == options.max_slots) break;
                    slot = slotAfter(slot);
                }
            }

            fn onLearn(
                self: *Node,
                from: NodeId,
                from_slot: Slot,
                effects: *Effects,
            ) !void {
                if (from_slot == 0) return error.InvalidSlot;
                var slot = from_slot;
                while (slot <= options.max_slots) {
                    std.debug.assert(slot > 0);
                    const index = @as(usize, slot - 1);
                    if (self.durable.committed[index]) |value| {
                        effects.addMessage(.{
                            .from = self.id,
                            .to = from,
                            .message = .{ .commit = .{
                                .slot = slot,
                                .value = value,
                            } },
                        });
                    }
                    if (slot == options.max_slots) break;
                    slot = slotAfter(slot);
                }
            }

            fn onNack(self: *Node, from: NodeId, message: Nack) void {
                _ = from;
                if (!message.rejected.eql(self.ballot)) return;
                if (!self.ballot.lessThan(message.promised)) return;
                self.role = .follower;
                self.leader_hint = message.promised.node;
                self.highest_observed_round = @max(
                    self.highest_observed_round,
                    message.promised.round,
                );
            }

            fn sendAccept(
                self: *Node,
                slot: Slot,
                value: Value,
                effects: *Effects,
            ) !void {
                const index = try slotIndex(slot);
                if (self.proposals[index]) |proposed| {
                    if (!std.meta.eql(proposed, value)) return error.ConflictingValue;
                }
                self.proposals[index] = value;
                self.acknowledgements[index] = .{};

                std.debug.assert(!self.ballot.lessThan(self.durable.promised));
                self.durable.promised = self.ballot;
                self.durable.accepted[index] = .{
                    .ballot = self.ballot,
                    .value = value,
                };
                effects.addWrite(.{ .accept = .{
                    .ballot = self.ballot,
                    .slot = slot,
                    .value = value,
                } });

                const local_member = self.membership.indexOf(self.id).?;
                _ = self.acknowledgements[index].insert(local_member);
                if (self.membership.quorum() == 1) {
                    try self.recordCommit(slot, value, effects);
                }
                self.broadcastPeers(effects, .{ .accept = .{
                    .ballot = self.ballot,
                    .slot = slot,
                    .value = value,
                } });
            }

            fn sendNack(
                self: *Node,
                to: NodeId,
                rejected: Ballot,
                effects: *Effects,
            ) void {
                effects.addMessage(.{
                    .from = self.id,
                    .to = to,
                    .message = .{ .nack = .{
                        .rejected = rejected,
                        .promised = self.durable.promised,
                        .decided_through = self.delivered_through,
                    } },
                });
            }

            fn sendHeartbeatIfDue(self: *Node, effects: *Effects) void {
                if (self.heartbeat_ticks < options.heartbeat_interval_ticks) return;
                self.heartbeat_ticks = 0;
                self.broadcastPeers(effects, .{ .heartbeat = .{
                    .ballot = self.ballot,
                    .decided_through = self.delivered_through,
                } });
            }

            fn resendIfDue(self: *Node, effects: *Effects) void {
                if (self.resend_ticks < options.resend_interval_ticks) return;
                self.resend_ticks = 0;
                for (self.membership.ids[0..self.membership.count]) |peer| {
                    if (peer == self.id) continue;
                    self.resendTo(peer, effects);
                }
            }

            fn resendTo(self: *Node, peer: NodeId, effects: *Effects) void {
                const peer_idx = self.membership.indexOf(peer) orelse return;
                const peer_decided = self.peer_decided_through[peer_idx];

                var slot = peer_decided + 1;
                while (slot <= options.max_slots) {
                    std.debug.assert(slot > 0);
                    const index = @as(usize, slot - 1);
                    if (self.durable.committed[index]) |value| {
                        self.sendTo(peer, effects, .{ .commit = .{
                            .slot = slot,
                            .value = value,
                        } });
                    } else if (self.proposals[index]) |value| {
                        self.sendTo(peer, effects, .{ .accept = .{
                            .ballot = self.ballot,
                            .slot = slot,
                            .value = value,
                        } });
                    }
                    if (slot == options.max_slots) break;
                    slot = slotAfter(slot);
                }
            }

            fn sendTo(
                self: *Node,
                peer: NodeId,
                effects: *Effects,
                message: Message,
            ) void {
                effects.addMessage(.{
                    .from = self.id,
                    .to = peer,
                    .message = message,
                });
            }

            fn broadcast(self: *Node, effects: *Effects, message: Message) void {
                for (self.membership.ids[0..self.membership.count]) |member| {
                    self.sendTo(member, effects, message);
                }
            }

            fn broadcastPeers(self: *Node, effects: *Effects, message: Message) void {
                for (self.membership.ids[0..self.membership.count]) |member| {
                    if (member == self.id) continue;
                    self.sendTo(member, effects, message);
                }
            }

            fn clearElection(self: *Node) void {
                self.promise_done = [_]bool{false} ** options.max_members;
                self.promise_expected = [_]Slot{0} ** options.max_members;
                self.promise_received = [_]Slot{0} ** options.max_members;
                self.promise_seen = [_]SlotSet{.{}} ** options.max_members;
                self.recovered = [_]?Accepted{null} ** options.max_slots;
                self.proposals = [_]?Value{null} ** options.max_slots;
            }

            fn highestRecoveredSlot(self: *const Node) Slot {
                var result: Slot = 0;
                for (self.recovered, 0..) |accepted, index| {
                    if (accepted != null) result = @intCast(index + 1);
                }
                for (self.durable.committed, 0..) |committed, index| {
                    if (committed != null) result = @max(result, @as(Slot, @intCast(index + 1)));
                }
                return result;
            }

            fn highestUsedSlot(self: *const Node) Slot {
                var result: Slot = 0;
                for (self.durable.accepted, 0..) |accepted, index| {
                    if (accepted != null) result = @intCast(index + 1);
                }
                for (self.durable.committed, 0..) |committed, index| {
                    if (committed != null) result = @max(result, @as(Slot, @intCast(index + 1)));
                }
                return result;
            }
        };

        fn slotIndex(slot: Slot) !usize {
            if (slot == 0) return error.InvalidSlot;
            if (slot > options.max_slots) return error.SlotLimitReached;
            return @intCast(slot - 1);
        }

        fn slotAfter(slot: Slot) Slot {
            if (slot >= options.max_slots) return 0;
            return slot + 1;
        }

        fn extractDecidedThrough(message: Message) ?Slot {
            switch (message) {
                .prepare => |m| return m.decided_through,
                .promise_done => |m| return m.decided_through,
                .accepted => |m| return m.decided_through,
                .nack => |m| return m.decided_through,
                .heartbeat => |m| return m.decided_through,
                .learn => |m| return if (m.from_slot > 1) m.from_slot - 1 else 0,
                else => return null,
            }
        }
    };
}

test "ballots are ordered lexicographically" {
    try std.testing.expect(Ballot.lessThan(
        .{ .round = 1, .node = 9 },
        .{ .round = 2, .node = 1 },
    ));
    try std.testing.expect(Ballot.lessThan(
        .{ .round = 2, .node = 1 },
        .{ .round = 2, .node = 2 },
    ));
    try std.testing.expect(Ballot.lessThan(
        .{ .round = 2, .priority = 3, .node = 9 },
        .{ .round = 2, .priority = 4, .node = 1 },
    ));
}

test "membership rejects invalid configurations" {
    const P = Protocol(u64, .{ .max_members = 3, .max_slots = 8 });
    var membership: P.Membership = undefined;
    try std.testing.expectError(error.EmptyMembership, membership.init(&.{}));
    try std.testing.expectError(error.InvalidNodeId, membership.init(&.{ 1, 0 }));
    try std.testing.expectError(error.DuplicateNodeId, membership.init(&.{ 1, 1 }));
}

test "acceptor-only member never campaigns but still promises" {
    const P = Protocol(u64, .{ .max_members = 3, .max_slots = 4 });
    var membership: P.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var node: P.Node = undefined;
    try node.init(2, &membership);
    node.setCampaignEnabled(false);
    var effects = P.Effects{};
    effects.init();

    try std.testing.expectError(error.CampaignDisabled, node.campaign(0, &effects));
    for (0..20) |_| try node.tick(0, &effects);
    try std.testing.expectEqual(P.Role.follower, node.role);

    try node.step(.{
        .from = 1,
        .to = 2,
        .message = .{ .prepare = .{
            .ballot = .{ .round = 1, .node = 1 },
            .decided_through = 0,
        } },
    }, &effects);
    try std.testing.expect(effects.writesSlice().len > 0);
    effects.confirmWritesDurable();
    try std.testing.expect(effects.messagesSlice().len > 0);
}

test "non-voting learner records only host-certified chosen values" {
    const P = Protocol(u64, .{ .max_members = 3, .max_slots = 4 });
    var membership: P.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var learner: P.Node = undefined;
    try learner.initLearner(10, &membership);
    var effects = P.Effects{};
    effects.init();

    try std.testing.expectError(error.NotVoter, learner.campaign(0, &effects));
    try learner.learnChosen(1, 2, 22, &effects);
    try std.testing.expectEqual(@as(usize, 1), effects.writesSlice().len);
    try std.testing.expectEqual(@as(usize, 0), effects.committedSlice().len);
    effects.confirmWritesDurable();
    try learner.learnChosen(2, 1, 11, &effects);
    try std.testing.expectEqual(@as(usize, 2), effects.committedSlice().len);
    try std.testing.expectEqual(@as(u64, 11), effects.committedSlice()[0].value);
    try std.testing.expectEqual(@as(u64, 22), effects.committedSlice()[1].value);
    effects.confirmWritesDurable();
    try std.testing.expectError(
        error.NotMember,
        learner.learnChosen(99, 3, 33, &effects),
    );
}

test "flexible read and write quorums must intersect" {
    const Valid = Protocol(u64, .{
        .max_members = 5,
        .max_slots = 8,
        .read_quorum_size = 4,
        .write_quorum_size = 2,
    });
    var valid: Valid.Membership = undefined;
    try valid.init(&.{ 1, 2, 3, 4, 5 });
    try std.testing.expectEqual(@as(usize, 4), valid.readQuorum());
    try std.testing.expectEqual(@as(usize, 2), valid.writeQuorum());

    const Invalid = Protocol(u64, .{
        .max_members = 5,
        .max_slots = 8,
        .read_quorum_size = 2,
        .write_quorum_size = 3,
    });
    var invalid: Invalid.Membership = undefined;
    try std.testing.expectError(
        error.NonIntersectingQuorums,
        invalid.init(&.{ 1, 2, 3, 4, 5 }),
    );
}

const TestProtocol = Protocol(u64, .{ .max_members = 3, .max_slots = 16 });
const TestEnvelope = TestProtocol.Envelope;

fn appendMessages(
    queue: *[512]TestEnvelope,
    queue_count: *usize,
    effects: *const TestProtocol.Effects,
) void {
    for (effects.messagesSlice()) |message| {
        std.debug.assert(queue_count.* < queue.len);
        queue[queue_count.*] = message;
        queue_count.* += 1;
    }
}

fn drainQueue(
    nodes: *[3]TestProtocol.Node,
    durable: *[3]TestProtocol.DurableState,
    queue: *[512]TestEnvelope,
    queue_count: *usize,
) !void {
    var effects = TestProtocol.Effects{};
    while (queue_count.* > 0) {
        const envelope = queue[0];
        std.mem.copyForwards(
            TestEnvelope,
            queue[0 .. queue_count.* - 1],
            queue[1..queue_count.*],
        );
        queue_count.* -= 1;

        const node_index = switch (envelope.to) {
            1 => @as(usize, 0),
            2 => @as(usize, 1),
            3 => @as(usize, 2),
            else => return error.UnknownNode,
        };
        try nodes[node_index].step(envelope, &effects);
        for (effects.writesSlice()) |write| try durable[node_index].apply(write);
        effects.confirmWritesDurable();
        appendMessages(queue, queue_count, &effects);
    }
}

test "multi-paxos elects a leader and commits consecutive values" {
    var membership: TestProtocol.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var nodes: [3]TestProtocol.Node = undefined;
    try nodes[0].init(1, &membership);
    try nodes[1].init(2, &membership);
    try nodes[2].init(3, &membership);
    var durable = [_]TestProtocol.DurableState{.{}} ** 3;
    var queue: [512]TestEnvelope = undefined;
    var queue_count: usize = 0;
    var effects = TestProtocol.Effects{};

    try nodes[0].campaign(0, &effects);
    appendMessages(&queue, &queue_count, &effects);
    try drainQueue(&nodes, &durable, &queue, &queue_count);
    try std.testing.expectEqual(TestProtocol.Role.leader, nodes[0].role);

    const first = try nodes[0].propose(41, &effects);
    try std.testing.expectEqual(@as(Slot, 1), first);
    for (effects.writesSlice()) |write| try durable[0].apply(write);
    effects.confirmWritesDurable();
    appendMessages(&queue, &queue_count, &effects);
    try drainQueue(&nodes, &durable, &queue, &queue_count);

    const second = try nodes[0].propose(42, &effects);
    try std.testing.expectEqual(@as(Slot, 2), second);
    for (effects.writesSlice()) |write| try durable[0].apply(write);
    effects.confirmWritesDurable();
    appendMessages(&queue, &queue_count, &effects);
    try drainQueue(&nodes, &durable, &queue, &queue_count);

    for (&nodes, &durable) |*node, *stored| {
        try std.testing.expectEqual(@as(?u64, 41), node.committedAt(1));
        try std.testing.expectEqual(@as(?u64, 42), node.committedAt(2));
        try std.testing.expectEqualDeep(stored.*, node.durable);
    }
}

test "duplicate messages are idempotent" {
    var membership: TestProtocol.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var node: TestProtocol.Node = undefined;
    try node.init(2, &membership);
    var effects = TestProtocol.Effects{};
    const prepare = TestEnvelope{
        .from = 1,
        .to = 2,
        .message = .{ .prepare = .{
            .ballot = .{ .round = 1, .node = 1 },
            .decided_through = 0,
        } },
    };
    try node.step(prepare, &effects);
    try std.testing.expectEqual(@as(usize, 1), effects.writes_count);
    effects.confirmWritesDurable();
    try node.step(prepare, &effects);
    try std.testing.expectEqual(@as(usize, 0), effects.writes_count);

    const accept = TestEnvelope{
        .from = 1,
        .to = 2,
        .message = .{ .accept = .{
            .ballot = .{ .round = 1, .node = 1 },
            .slot = 1,
            .value = 99,
        } },
    };
    try node.step(accept, &effects);
    try std.testing.expectEqual(@as(usize, 1), effects.writes_count);
    effects.confirmWritesDurable();
    try node.step(accept, &effects);
    try std.testing.expectEqual(@as(usize, 0), effects.writes_count);
    try std.testing.expectEqual(@as(usize, 1), effects.messages_count);
}

test "phase one tolerates reordering and recovers the highest accepted value" {
    var membership: TestProtocol.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    const lower = Ballot{ .round = 1, .node = 1 };
    const higher = Ballot{ .round = 1, .node = 2 };

    var first_disk = TestProtocol.DurableState{ .promised = lower };
    first_disk.accepted[0] = .{ .ballot = lower, .value = 77 };
    var second_disk = TestProtocol.DurableState{ .promised = higher };
    second_disk.accepted[0] = .{ .ballot = higher, .value = 88 };

    var nodes: [3]TestProtocol.Node = undefined;
    try nodes[0].restore(1, &membership, &first_disk);
    try nodes[1].restore(2, &membership, &second_disk);
    try nodes[2].init(3, &membership);
    var effects = TestProtocol.Effects{};
    try nodes[2].campaign(0, &effects);
    const prepares = effects.messages;
    const prepares_count = effects.messages_count;
    try std.testing.expectEqual(@as(usize, 3), prepares_count);

    var replies: [6]TestEnvelope = undefined;
    var reply_count: usize = 0;
    for (prepares[0..prepares_count]) |prepare| {
        const index = switch (prepare.to) {
            1 => @as(usize, 0),
            2 => @as(usize, 1),
            3 => @as(usize, 2),
            else => unreachable,
        };
        try nodes[index].step(prepare, &effects);
        effects.confirmWritesDurable();

        // Deliver each completion marker before its entry to exercise a network
        // that does not preserve sender ordering.
        for (effects.messagesSlice()) |reply| {
            if (reply.message == .promise_done) {
                replies[reply_count] = reply;
                reply_count += 1;
            }
        }
        for (effects.messagesSlice()) |reply| {
            if (reply.message == .promise) {
                replies[reply_count] = reply;
                reply_count += 1;
            }
        }
    }

    var saw_recovered_accept = false;
    for (replies[0..reply_count]) |reply| {
        try nodes[2].step(reply, &effects);
        effects.confirmWritesDurable();
        for (effects.messagesSlice()) |outbound| {
            switch (outbound.message) {
                .accept => |accept| {
                    if (accept.slot == 1 and accept.value == 88) {
                        saw_recovered_accept = true;
                    }
                },
                else => {},
            }
        }
    }
    try std.testing.expectEqual(TestProtocol.Role.leader, nodes[2].role);
    try std.testing.expect(saw_recovered_accept);
}

test "bounded log reports exhaustion without reusing a slot" {
    const P = Protocol(u64, .{ .max_members = 1, .max_slots = 1 });
    var membership: P.Membership = undefined;
    try membership.init(&.{1});
    var node: P.Node = undefined;
    try node.init(1, &membership);
    node.role = .leader;
    node.ballot = .{ .round = 1, .node = 1 };
    var effects = P.Effects{};

    try std.testing.expectEqual(@as(Slot, 1), try node.propose(1, &effects));
    effects.confirmWritesDurable();
    try std.testing.expectError(error.SlotLimitReached, node.propose(2, &effects));
}

test "one-node quorum commits without a remote acknowledgement" {
    const P = Protocol(u64, .{ .max_members = 1, .max_slots = 2 });
    var membership: P.Membership = undefined;
    try membership.init(&.{1});
    var node: P.Node = undefined;
    try node.init(1, &membership);
    node.role = .leader;
    node.ballot = .{ .round = 1, .node = 1 };
    var effects = P.Effects{};

    try std.testing.expectEqual(@as(Slot, 1), try node.propose(71, &effects));
    try std.testing.expectEqual(@as(usize, 2), effects.writes_count);
    try std.testing.expectEqual(@as(usize, 1), effects.committed_count);
    try std.testing.expectEqual(@as(?u64, 71), node.committedAt(1));
}

test "batch proposal returns consecutive slots and one effect batch" {
    const P = Protocol(u64, .{ .max_members = 1, .max_slots = 4 });
    var membership: P.Membership = undefined;
    try membership.init(&.{1});
    var node: P.Node = undefined;
    try node.init(1, &membership);
    node.role = .leader;
    node.ballot = .{ .round = 1, .node = 1 };
    var effects = P.Effects{};
    var slots: [3]Slot = undefined;

    const proposed = try node.proposeBatch(&.{ 11, 22, 33 }, &slots, &effects);
    try std.testing.expectEqualSlices(Slot, &.{ 1, 2, 3 }, proposed);
    try std.testing.expectEqual(@as(usize, 6), effects.writes_count);
    try std.testing.expectEqual(@as(usize, 3), effects.committed_count);
}

test "tick starts an election and emits leader heartbeats" {
    const P = Protocol(u64, .{
        .max_members = 3,
        .max_slots = 4,
        .election_timeout_ticks = 2,
        .heartbeat_interval_ticks = 2,
    });
    var membership: P.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var node: P.Node = undefined;
    try node.init(1, &membership);
    var effects = P.Effects{};

    try node.tick(0, &effects);
    try std.testing.expectEqual(@as(usize, 0), effects.messages_count);
    try node.tick(0, &effects);
    try std.testing.expectEqual(P.Role.preparing, node.role);
    try std.testing.expectEqual(@as(usize, 3), effects.messages_count);

    node.role = .leader;
    node.leader_hint = 1;
    node.durable.promised = node.ballot;
    node.heartbeat_ticks = 0;
    try node.tick(0, &effects);
    try node.tick(0, &effects);
    try std.testing.expectEqual(@as(usize, 2), effects.messages_count);
    for (effects.messagesSlice()) |message| {
        try std.testing.expect(message.message == .heartbeat);
    }
}

test "durable replay rejects conflicting values" {
    const P = Protocol(u64, .{ .max_members = 1, .max_slots = 1 });
    var durable = P.DurableState{};
    const ballot = Ballot{ .round = 1, .node = 1 };
    try durable.apply(.{ .accept = .{ .ballot = ballot, .slot = 1, .value = 7 } });

    // Two different values in one ballot and slot is corruption.
    try std.testing.expectError(
        error.ConflictingValue,
        durable.apply(.{ .accept = .{ .ballot = ballot, .slot = 1, .value = 8 } }),
    );
    // A commit that differs from a stale local vote is legal Paxos: the
    // choosing quorum may not have included this node.
    try durable.apply(.{ .commit = .{ .slot = 1, .value = 8 } });
    // Two different committed values for one slot is corruption.
    try std.testing.expectError(
        error.ConflictingCommit,
        durable.apply(.{ .commit = .{ .slot = 1, .value = 7 } }),
    );
    // A later-ballot vote may land after the commit; replay accepts it.
    try durable.apply(.{
        .accept = .{
            .ballot = .{ .round = 2, .node = 1 },
            .slot = 1,
            .value = 8,
        },
    });
    // A promise below the recorded vote is regression.
    try std.testing.expectError(
        error.PromiseRegression,
        durable.apply(.{ .accept = .{ .ballot = ballot, .slot = 1, .value = 7 } }),
    );
}

test "leader persists its local acceptance before remote acknowledgements" {
    var membership: TestProtocol.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var leader: TestProtocol.Node = undefined;
    try leader.init(1, &membership);
    leader.role = .leader;
    leader.ballot = .{ .round = 1, .node = 1 };
    var effects = TestProtocol.Effects{};
    _ = try leader.propose(123, &effects);
    effects.confirmWritesDurable();

    for ([_]NodeId{ 2, 3 }) |from| {
        effects.confirmWritesDurable();
        try leader.step(.{
            .from = from,
            .to = 1,
            .message = .{ .accepted = .{
                .ballot = leader.ballot,
                .slot = 1,
                .decided_through = 0,
            } },
        }, &effects);
    }

    try std.testing.expectEqual(@as(?u64, 123), leader.committedAt(1));
    try std.testing.expectEqual(@as(?u64, 123), leader.durable.accepted[0].?.value);
}

test "learners release commits only as a contiguous prefix" {
    var membership: TestProtocol.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var node: TestProtocol.Node = undefined;
    try node.init(2, &membership);
    var effects = TestProtocol.Effects{};
    try node.step(.{
        .from = 1,
        .to = 2,
        .message = .{ .commit = .{ .slot = 2, .value = 22 } },
    }, &effects);
    try std.testing.expectEqual(@as(usize, 0), effects.committed_count);
    effects.confirmWritesDurable();

    try node.step(.{
        .from = 1,
        .to = 2,
        .message = .{ .commit = .{ .slot = 1, .value = 11 } },
    }, &effects);
    try std.testing.expectEqual(@as(usize, 2), effects.committed_count);
    try std.testing.expectEqual(@as(Slot, 1), effects.committed[0].slot);
    try std.testing.expectEqual(@as(Slot, 2), effects.committed[1].slot);
}

test "effects track durability confirmation for each write batch" {
    var membership: TestProtocol.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var node: TestProtocol.Node = undefined;
    try node.init(2, &membership);
    var effects = TestProtocol.Effects{};

    try std.testing.expect(effects.writes_confirmed);
    try node.step(.{
        .from = 1,
        .to = 2,
        .message = .{ .prepare = .{
            .ballot = .{ .round = 1, .node = 1 },
            .decided_through = 0,
        } },
    }, &effects);
    try std.testing.expect(effects.writes_count > 0);
    try std.testing.expect(!effects.writes_confirmed);
    effects.confirmWritesDurable();
    try std.testing.expect(effects.writes_confirmed);
    try std.testing.expect(effects.messagesSlice().len > 0);

    // A confirmed batch may be reset and reused for the next transition.
    effects.reset();
    try std.testing.expect(effects.writes_confirmed);
    try std.testing.expectEqual(@as(usize, 0), effects.writes_count);
}

test "only accept requests are available before durability confirmation" {
    var membership: TestProtocol.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });

    var leader: TestProtocol.Node = undefined;
    try leader.init(1, &membership);
    leader.role = .leader;
    leader.ballot = .{ .round = 1, .node = 1 };
    leader.durable.promised = leader.ballot;

    var effects = TestProtocol.Effects{};
    _ = try leader.propose(42, &effects);
    try std.testing.expect(!effects.writes_confirmed);
    try std.testing.expect(effects.requiresPowerLossBarrier());
    var early = effects.preDurableMessages();
    var accept_count: usize = 0;
    while (early.next()) |envelope| {
        try std.testing.expect(envelope.message == .accept);
        accept_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), accept_count);

    // A promise is evidence about an acceptor's durable state and must not
    // be exposed through the pre-barrier class.
    var follower: TestProtocol.Node = undefined;
    try follower.init(2, &membership);
    effects.confirmWritesDurable();
    try follower.step(.{
        .from = 1,
        .to = 2,
        .message = .{ .prepare = .{
            .ballot = .{ .round = 2, .node = 1 },
            .decided_through = 0,
        } },
    }, &effects);
    var promises = effects.preDurableMessages();
    try std.testing.expect(promises.next() == null);
    try std.testing.expect(effects.requiresPowerLossBarrier());
}

test "commit-only effects are reconstructible derived state" {
    var membership: TestProtocol.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var follower: TestProtocol.Node = undefined;
    try follower.init(2, &membership);
    var effects = TestProtocol.Effects{};

    try follower.step(.{
        .from = 1,
        .to = 2,
        .message = .{ .commit = .{ .slot = 1, .value = 99 } },
    }, &effects);
    try std.testing.expectEqual(@as(usize, 1), effects.writesSlice().len);
    try std.testing.expect(!effects.requiresPowerLossBarrier());
    try std.testing.expectEqual(@as(usize, 1), effects.committedSlice().len);
}

test "catch-up returns known commits from the requested slot" {
    var membership: TestProtocol.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var node: TestProtocol.Node = undefined;
    try node.init(1, &membership);
    node.durable.committed[0] = 10;
    node.durable.committed[1] = 20;
    node.durable.committed[2] = 30;
    var effects = TestProtocol.Effects{};

    try node.step(.{
        .from = 2,
        .to = 1,
        .message = .{ .learn = .{ .from_slot = 2 } },
    }, &effects);
    try std.testing.expectEqual(@as(usize, 2), effects.messages_count);
    for (effects.messagesSlice()) |message| {
        try std.testing.expectEqual(@as(NodeId, 2), message.to);
    }
}

test "reconnected leader retransmits missing commits and open accepts" {
    var membership: TestProtocol.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var leader: TestProtocol.Node = undefined;
    try leader.init(1, &membership);
    leader.role = .leader;
    leader.ballot = .{ .round = 1, .node = 1 };
    leader.durable.promised = leader.ballot;
    leader.durable.committed[0] = 10;
    leader.proposals[1] = 20;
    var effects = TestProtocol.Effects{};

    // The peer reported no progress, so it receives the decided slot as a
    // commit and the still-open slot as a fresh accept, in slot order.
    try leader.reconnected(2, &effects);
    try std.testing.expectEqual(@as(usize, 2), effects.messages_count);
    const commit = effects.messagesSlice()[0];
    try std.testing.expectEqual(@as(NodeId, 2), commit.to);
    try std.testing.expect(commit.message == .commit);
    try std.testing.expectEqual(@as(Slot, 1), commit.message.commit.slot);
    try std.testing.expectEqual(@as(u64, 10), commit.message.commit.value);
    const accept = effects.messagesSlice()[1];
    try std.testing.expectEqual(@as(NodeId, 2), accept.to);
    try std.testing.expect(accept.message == .accept);
    try std.testing.expectEqual(@as(Slot, 2), accept.message.accept.slot);
    try std.testing.expectEqual(@as(u64, 20), accept.message.accept.value);

    try std.testing.expectError(error.InvalidPeer, leader.reconnected(1, &effects));
    try std.testing.expectError(error.NotMember, leader.reconnected(9, &effects));
}

test "reconnected follower asks its leader for undelivered decisions" {
    var membership: TestProtocol.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var follower: TestProtocol.Node = undefined;
    try follower.init(2, &membership);
    follower.leader_hint = 1;
    follower.durable.committed[0] = 10;
    follower.delivered_through = 1;
    var effects = TestProtocol.Effects{};

    // Reconnecting to the presumed leader requests everything undelivered.
    try follower.reconnected(1, &effects);
    try std.testing.expectEqual(@as(usize, 1), effects.messages_count);
    const request = effects.messagesSlice()[0];
    try std.testing.expectEqual(@as(NodeId, 1), request.to);
    try std.testing.expect(request.message == .learn);
    try std.testing.expectEqual(@as(Slot, 2), request.message.learn.from_slot);

    // Reconnecting to a peer that is not the leader hint repairs nothing.
    try follower.reconnected(3, &effects);
    try std.testing.expectEqual(@as(usize, 0), effects.messages_count);
}

test "requestCatchUp round trip returns decided entries from the slot" {
    var membership: TestProtocol.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var provider: TestProtocol.Node = undefined;
    try provider.init(1, &membership);
    provider.durable.committed[0] = 10;
    provider.durable.committed[1] = 20;
    provider.durable.committed[2] = 30;
    var lagging: TestProtocol.Node = undefined;
    try lagging.init(2, &membership);
    lagging.durable.committed[0] = 10;
    lagging.delivered_through = 1;
    var effects = TestProtocol.Effects{};

    try std.testing.expectError(
        error.InvalidSlot,
        lagging.requestCatchUp(1, 0, &effects),
    );
    try std.testing.expectError(
        error.NotMember,
        lagging.requestCatchUp(9, 2, &effects),
    );

    try lagging.requestCatchUp(1, 2, &effects);
    try std.testing.expectEqual(@as(usize, 1), effects.messages_count);
    const request = effects.messagesSlice()[0];
    try std.testing.expectEqual(@as(NodeId, 1), request.to);
    try std.testing.expect(request.message == .learn);
    try std.testing.expectEqual(@as(Slot, 2), request.message.learn.from_slot);

    // The provider answers with every known commit at or above the slot,
    // and delivering those commits advances the lagging decided prefix.
    var provider_effects = TestProtocol.Effects{};
    try provider.step(request, &provider_effects);
    try std.testing.expectEqual(@as(usize, 2), provider_effects.messages_count);
    for (provider_effects.messagesSlice()) |envelope| {
        try std.testing.expectEqual(@as(NodeId, 2), envelope.to);
        try std.testing.expect(envelope.message == .commit);
        try lagging.step(envelope, &effects);
        effects.confirmWritesDurable();
    }
    try std.testing.expectEqual(@as(Slot, 3), lagging.decidedThrough());
    try std.testing.expectEqual(@as(?u64, 20), lagging.committedAt(2));
    try std.testing.expectEqual(@as(?u64, 30), lagging.committedAt(3));
}

test "resendIfDue retransmits to lagging peers after the resend interval" {
    const P = Protocol(u64, .{
        .max_members = 3,
        .max_slots = 4,
        .election_timeout_ticks = 20,
        .heartbeat_interval_ticks = 16,
        .resend_interval_ticks = 2,
    });
    var membership: P.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var leader: P.Node = undefined;
    try leader.init(1, &membership);
    leader.role = .leader;
    leader.ballot = .{ .round = 1, .node = 1 };
    leader.durable.promised = leader.ballot;
    leader.durable.committed[0] = 10;
    leader.proposals[1] = 20;
    var effects = P.Effects{};

    // Nothing is retransmitted before the interval of silence elapses.
    try leader.tick(0, &effects);
    try std.testing.expectEqual(@as(usize, 0), effects.messages_count);
    // At the interval, both lagging peers receive the decided slot as a
    // commit and the open slot as an accept.
    try leader.tick(0, &effects);
    try std.testing.expectEqual(@as(usize, 4), effects.messages_count);
    for (effects.messagesSlice()) |envelope| {
        try std.testing.expect(
            envelope.message == .commit or envelope.message == .accept,
        );
    }
    // The interval counter resets, so the next tick is silent again.
    try leader.tick(0, &effects);
    try std.testing.expectEqual(@as(usize, 0), effects.messages_count);

    // A peer that reported progress is only sent what it still misses.
    leader.peer_decided_through[1] = 1;
    try leader.tick(0, &effects);
    try std.testing.expectEqual(@as(usize, 3), effects.messages_count);
    for (effects.messagesSlice()) |envelope| {
        if (envelope.to == 2) try std.testing.expect(envelope.message == .accept);
    }
}

test "heartbeat ahead of the follower triggers a catch-up request" {
    var membership: TestProtocol.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var follower: TestProtocol.Node = undefined;
    try follower.init(2, &membership);
    const leader_ballot = Ballot{ .round = 1, .node = 1 };
    follower.durable.promised = leader_ballot;
    var effects = TestProtocol.Effects{};

    // The leader advertises a longer decided prefix; the follower learns.
    try follower.step(.{
        .from = 1,
        .to = 2,
        .message = .{ .heartbeat = .{
            .ballot = leader_ballot,
            .decided_through = 3,
        } },
    }, &effects);
    try std.testing.expectEqual(@as(usize, 1), effects.messages_count);
    const request = effects.messagesSlice()[0];
    try std.testing.expectEqual(@as(NodeId, 1), request.to);
    try std.testing.expect(request.message == .learn);
    try std.testing.expectEqual(@as(Slot, 1), request.message.learn.from_slot);
    try std.testing.expectEqual(@as(?NodeId, 1), follower.currentLeader());

    // A heartbeat that is not ahead of this follower requests nothing.
    try follower.step(.{
        .from = 1,
        .to = 2,
        .message = .{ .heartbeat = .{
            .ballot = leader_ballot,
            .decided_through = 0,
        } },
    }, &effects);
    try std.testing.expectEqual(@as(usize, 0), effects.messages_count);
}

test "nack for a higher promise steps the leader down" {
    var membership: TestProtocol.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var leader: TestProtocol.Node = undefined;
    try leader.init(1, &membership);
    leader.role = .leader;
    leader.ballot = .{ .round = 1, .node = 1 };
    leader.durable.promised = leader.ballot;
    var effects = TestProtocol.Effects{};

    // A nack for a ballot this node never issued is ignored.
    try leader.step(.{
        .from = 2,
        .to = 1,
        .message = .{ .nack = .{
            .rejected = .{ .round = 9, .node = 1 },
            .promised = .{ .round = 10, .node = 2 },
            .decided_through = 0,
        } },
    }, &effects);
    try std.testing.expectEqual(TestProtocol.Role.leader, leader.role);

    // A nack that names no higher promise is ignored.
    try leader.step(.{
        .from = 2,
        .to = 1,
        .message = .{ .nack = .{
            .rejected = leader.ballot,
            .promised = leader.ballot,
            .decided_through = 0,
        } },
    }, &effects);
    try std.testing.expectEqual(TestProtocol.Role.leader, leader.role);

    // A higher promise abandons leadership and adopts the winner as hint.
    try leader.step(.{
        .from = 2,
        .to = 1,
        .message = .{ .nack = .{
            .rejected = leader.ballot,
            .promised = .{ .round = 2, .node = 2 },
            .decided_through = 0,
        } },
    }, &effects);
    try std.testing.expectEqual(TestProtocol.Role.follower, leader.role);
    try std.testing.expectEqual(@as(?NodeId, 2), leader.currentLeader());
    try std.testing.expectEqual(@as(u64, 2), leader.highest_observed_round);

    // The next campaign picks a round above the observed higher promise.
    try leader.campaign(0, &effects);
    try std.testing.expectEqual(@as(u64, 3), leader.ballot.round);
}

fn hasPointers(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .pointer => true,
        .@"struct" => |info| {
            inline for (info.fields) |field| {
                if (hasPointers(field.type)) return true;
            }
            return false;
        },
        .@"union" => |info| {
            inline for (info.fields) |field| {
                if (hasPointers(field.type)) return true;
            }
            return false;
        },
        .array => |info| hasPointers(info.child),
        .optional => |info| hasPointers(info.child),
        .error_union => |info| hasPointers(info.payload) or hasPointers(info.error_set),
        else => false,
    };
}
