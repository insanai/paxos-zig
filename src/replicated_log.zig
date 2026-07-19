const std = @import("std");
const protocol = @import("protocol.zig");

/// Compile-time bounds and protocol policy for a sealed, reconfigurable log.
pub const Options = struct {
    max_members: usize = 7,
    max_entries: usize = 256,
    max_batch: usize = 64,
    max_metadata_bytes: usize = 256,
    read_quorum_size: ?u16 = null,
    write_quorum_size: ?u16 = null,
    election_timeout_ticks: u32 = 10,
    heartbeat_interval_ticks: u32 = 3,
    resend_interval_ticks: u32 = 10,
};

/// A bounded replicated log built on the explicit Paxos effect machine.
///
/// Commands and configuration stop signs share one ordered log. A decided stop
/// sign seals the current configuration. The host transfers application state
/// and starts the next configuration, just as it would cross a snapshot epoch.
pub fn ReplicatedLog(comptime Value: type, comptime options: Options) type {
    comptime {
        std.debug.assert(options.max_batch > 0);
        std.debug.assert(options.max_batch <= options.max_entries);
        std.debug.assert(options.max_metadata_bytes <= std.math.maxInt(u16));
    }

    const StopSignType = struct {
        configuration_id: u64,
        members: [options.max_members]protocol.NodeId,
        member_count: u16,
        metadata: [options.max_metadata_bytes]u8,
        metadata_count: u16,

        fn create(
            configuration_id: u64,
            members: []const protocol.NodeId,
            metadata: []const u8,
        ) !@This() {
            if (configuration_id == 0) return error.InvalidConfigurationId;
            if (members.len == 0) return error.EmptyMembership;
            if (members.len > options.max_members) return error.TooManyMembers;
            if (metadata.len > options.max_metadata_bytes) return error.MetadataTooLarge;

            var result = @This(){
                .configuration_id = configuration_id,
                .members = [_]protocol.NodeId{0} ** options.max_members,
                .member_count = @intCast(members.len),
                .metadata = [_]u8{0} ** options.max_metadata_bytes,
                .metadata_count = @intCast(metadata.len),
            };
            for (members, 0..) |member, index| {
                if (member == 0) return error.InvalidNodeId;
                for (members[0..index]) |previous| {
                    if (previous == member) return error.DuplicateNodeId;
                }
                result.members[index] = member;
            }
            @memcpy(result.metadata[0..metadata.len], metadata);
            return result;
        }

        pub fn membersSlice(self: *const @This()) []const protocol.NodeId {
            return self.members[0..self.member_count];
        }

        pub fn metadataSlice(self: *const @This()) []const u8 {
            return self.metadata[0..self.metadata_count];
        }
    };

    const EntryType = union(enum) {
        command: Value,
        stop: StopSignType,
    };

    const Core = protocol.Protocol(EntryType, .{
        .max_members = options.max_members,
        .max_slots = options.max_entries,
        .read_quorum_size = options.read_quorum_size,
        .write_quorum_size = options.write_quorum_size,
        .election_timeout_ticks = options.election_timeout_ticks,
        .heartbeat_interval_ticks = options.heartbeat_interval_ticks,
        .resend_interval_ticks = options.resend_interval_ticks,
    });

    return struct {
        pub const StopSign = StopSignType;
        pub const Entry = EntryType;
        pub const Membership = Core.Membership;
        pub const Effects = Core.Effects;
        pub const Envelope = Core.Envelope;
        pub const Message = Core.Message;
        pub const Write = Core.Write;
        pub const DurableState = Core.DurableState;
        pub const Committed = Core.Committed;
        pub const Role = Core.Role;

        pub const Node = struct {
            core: Core.Node,
            configuration_id: u64,
            stop_sign: ?StopSign = null,
            stop_pending: bool = false,
            batch_entries: [options.max_batch]Entry = undefined,

            /// Initializes an empty configuration with priority zero.
            pub fn init(
                self: *Node,
                id: protocol.NodeId,
                configuration_id: u64,
                membership: *const Membership,
            ) !void {
                try self.initWithPriority(id, configuration_id, membership, 0);
            }

            /// Initializes an empty configuration with an election priority.
            pub fn initWithPriority(
                self: *Node,
                id: protocol.NodeId,
                configuration_id: u64,
                membership: *const Membership,
                leader_priority: u32,
            ) !void {
                if (configuration_id == 0) return error.InvalidConfigurationId;
                try self.core.initWithPriority(id, membership, leader_priority);
                self.configuration_id = configuration_id;
                self.stop_sign = null;
                self.stop_pending = false;
            }

            /// Restores one configuration from replayed durable state.
            pub fn restore(
                self: *Node,
                id: protocol.NodeId,
                configuration_id: u64,
                membership: *const Membership,
                durable: *const DurableState,
            ) !void {
                try self.restoreWithPriority(id, configuration_id, membership, durable, 0);
            }

            /// Restores one configuration with an election priority.
            pub fn restoreWithPriority(
                self: *Node,
                id: protocol.NodeId,
                configuration_id: u64,
                membership: *const Membership,
                durable: *const DurableState,
                leader_priority: u32,
            ) !void {
                if (configuration_id == 0) return error.InvalidConfigurationId;
                try self.core.restoreWithPriority(id, membership, durable, leader_priority);
                self.configuration_id = configuration_id;
                self.stop_sign = null;
                self.stop_pending = false;
                self.observeDurable();
            }

            /// Starts the configuration named by a decided stop sign.
            pub fn initFromStop(
                self: *Node,
                id: protocol.NodeId,
                stop: *const StopSign,
                membership: *Membership,
                leader_priority: u32,
            ) !void {
                try membership.init(stop.membersSlice());
                try self.initWithPriority(
                    id,
                    stop.configuration_id,
                    membership,
                    leader_priority,
                );
            }

            /// Campaigns using an application command that acts as a no-op.
            pub fn campaign(self: *Node, noop: Value, effects: *Effects) !void {
                try self.core.campaign(.{ .command = noop }, effects);
                self.observeEffects(effects);
            }

            /// Appends one command unless a stop sign is pending or decided.
            pub fn append(self: *Node, value: Value, effects: *Effects) !protocol.Slot {
                if (self.stop_pending or self.stop_sign != null) return error.LogSealed;
                const slot = try self.core.propose(.{ .command = value }, effects);
                self.observeEffects(effects);
                return slot;
            }

            /// Appends a bounded command batch into one caller-consumed effect batch.
            pub fn appendBatch(
                self: *Node,
                values: []const Value,
                slots: []protocol.Slot,
                effects: *Effects,
            ) ![]const protocol.Slot {
                if (self.stop_pending or self.stop_sign != null) return error.LogSealed;
                if (values.len > options.max_batch) return error.BatchTooLarge;
                for (values, self.batch_entries[0..values.len]) |value, *entry| {
                    entry.* = .{ .command = value };
                }
                const proposed = try self.core.proposeBatch(
                    self.batch_entries[0..values.len],
                    slots,
                    effects,
                );
                self.observeEffects(effects);
                return proposed;
            }

            /// Proposes a stop sign for a strictly newer configuration.
            pub fn reconfigure(
                self: *Node,
                configuration_id: u64,
                members: []const protocol.NodeId,
                metadata: []const u8,
                effects: *Effects,
            ) !protocol.Slot {
                if (self.stop_pending or self.stop_sign != null) return error.LogSealed;
                if (configuration_id <= self.configuration_id) {
                    return error.ConfigurationIdRegression;
                }
                const stop = try StopSign.create(configuration_id, members, metadata);
                const slot = try self.core.propose(.{ .stop = stop }, effects);
                self.stop_pending = true;
                self.observeEffects(effects);
                return slot;
            }

            /// Seals this bounded epoch and points the next epoch at a snapshot.
            pub fn checkpoint(
                self: *Node,
                snapshot_metadata: []const u8,
                effects: *Effects,
            ) !protocol.Slot {
                if (self.configuration_id == std.math.maxInt(u64)) {
                    return error.ConfigurationIdExhausted;
                }
                const members = self.core.membership.ids[0..self.core.membership.count];
                return self.reconfigure(
                    self.configuration_id + 1,
                    members,
                    snapshot_metadata,
                    effects,
                );
            }

            /// Advances logical clocks and observes any stop sign decision.
            pub fn tick(self: *Node, noop: Value, effects: *Effects) !void {
                try self.core.tick(.{ .command = noop }, effects);
                self.observeEffects(effects);
            }

            /// Processes one protocol message and observes committed stop signs.
            pub fn step(self: *Node, envelope: Envelope, effects: *Effects) !void {
                try self.core.step(envelope, effects);
                self.observeEffects(effects);
            }

            /// Repairs communication to a peer after transport reconnection.
            pub fn reconnected(
                self: *Node,
                peer: protocol.NodeId,
                effects: *Effects,
            ) !void {
                try self.core.reconnected(peer, effects);
            }

            /// Requests decided entries beginning with `from_slot`.
            pub fn requestCatchUp(
                self: *Node,
                peer: protocol.NodeId,
                from_slot: protocol.Slot,
                effects: *Effects,
            ) !void {
                try self.core.requestCatchUp(peer, from_slot, effects);
            }

            /// Reads one decided command or stop sign from this configuration.
            pub fn read(self: *const Node, slot: protocol.Slot) ?Entry {
                return self.core.committedAt(slot);
            }

            /// Returns the contiguous decided prefix in this configuration.
            pub fn decidedThrough(self: *const Node) protocol.Slot {
                return self.core.decidedThrough();
            }

            /// Returns the latest observed leader.
            pub fn currentLeader(self: *const Node) ?protocol.NodeId {
                return self.core.currentLeader();
            }

            /// Returns the durable configuration identity supplied at initialization.
            pub fn configurationId(self: *const Node) u64 {
                return self.configuration_id;
            }

            /// Returns the decided stop sign, if this configuration is sealed.
            pub fn isReconfigured(self: *const Node) ?StopSign {
                return self.stop_sign;
            }

            fn recalculateStopPending(self: *Node) void {
                if (self.stop_sign != null) {
                    self.stop_pending = true;
                    return;
                }
                var pending = false;
                for (self.core.durable.accepted, 0..) |accepted, index| {
                    if (self.core.durable.committed[index] != null) continue;
                    if (accepted) |entry| {
                        switch (entry.value) {
                            .command => {},
                            .stop => {
                                pending = true;
                                break;
                            },
                        }
                    }
                }
                for (self.core.proposals, 0..) |proposal, index| {
                    if (self.core.durable.committed[index] != null) continue;
                    if (proposal) |entry| {
                        switch (entry) {
                            .command => {},
                            .stop => {
                                pending = true;
                                break;
                            },
                        }
                    }
                }
                self.stop_pending = pending;
            }

            fn observeEffects(self: *Node, effects: *const Effects) void {
                for (effects.committedSlice()) |committed| {
                    switch (committed.value) {
                        .command => {},
                        .stop => |stop| {
                            self.stop_sign = stop;
                        },
                    }
                }
                self.recalculateStopPending();
            }

            fn observeDurable(self: *Node) void {
                for (self.core.durable.committed) |committed| {
                    const entry = committed orelse continue;
                    switch (entry) {
                        .command => {},
                        .stop => |stop| {
                            self.stop_sign = stop;
                        },
                    }
                }
                self.recalculateStopPending();
            }
        };
    };
}

test "a decided stop sign seals a configuration" {
    const Log = ReplicatedLog(u64, .{
        .max_members = 3,
        .max_entries = 4,
        .max_batch = 2,
        .max_metadata_bytes = 16,
    });
    var membership: Log.Membership = undefined;
    try membership.init(&.{1});
    var node: Log.Node = undefined;
    try node.init(1, 7, &membership);
    node.core.role = .leader;
    node.core.ballot = .{ .round = 1, .node = 1 };
    var effects = Log.Effects{};

    _ = try node.append(41, &effects);
    effects.confirmWritesDurable();
    _ = try node.reconfigure(8, &.{ 1, 2, 3 }, "snapshot:19", &effects);
    effects.confirmWritesDurable();
    const stop = node.isReconfigured().?;
    try std.testing.expectEqual(@as(u64, 8), stop.configuration_id);
    try std.testing.expectEqualStrings("snapshot:19", stop.metadataSlice());
    try std.testing.expectError(error.LogSealed, node.append(42, &effects));
}

test "replicated log batches commands without allocation" {
    const Log = ReplicatedLog(u64, .{
        .max_members = 1,
        .max_entries = 4,
        .max_batch = 3,
    });
    var membership: Log.Membership = undefined;
    try membership.init(&.{1});
    var node: Log.Node = undefined;
    try node.init(1, 1, &membership);
    node.core.role = .leader;
    node.core.ballot = .{ .round = 1, .node = 1 };
    var effects = Log.Effects{};
    var slots: [3]protocol.Slot = undefined;

    const result = try node.appendBatch(&.{ 3, 5, 8 }, &slots, &effects);
    try std.testing.expectEqualSlices(protocol.Slot, &.{ 1, 2, 3 }, result);
    try std.testing.expectEqual(@as(protocol.Slot, 3), node.decidedThrough());
}

test "checkpoint seals an epoch and initializes the next one" {
    const Log = ReplicatedLog(u64, .{
        .max_members = 1,
        .max_entries = 4,
        .max_batch = 4,
        .max_metadata_bytes = 16,
    });
    var membership: Log.Membership = undefined;
    try membership.init(&.{1});
    var first: Log.Node = undefined;
    try first.init(1, 11, &membership);
    first.core.role = .leader;
    first.core.ballot = .{ .round = 1, .node = 1 };
    var effects = Log.Effects{};

    _ = try first.checkpoint("state:3", &effects);
    const stop = first.isReconfigured().?;
    var next_membership: Log.Membership = undefined;
    var next: Log.Node = undefined;
    try next.initFromStop(1, &stop, &next_membership, 9);
    try std.testing.expectEqual(@as(u64, 12), next.configurationId());
    try std.testing.expectEqual(@as(u32, 9), next.core.leader_priority);
}

test "restore remains sealed after accepting a stop sign" {
    const Log = ReplicatedLog(u64, .{
        .max_members = 3,
        .max_entries = 4,
        .max_batch = 4,
        .max_metadata_bytes = 8,
    });
    var membership: Log.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var leader: Log.Node = undefined;
    try leader.init(1, 4, &membership);
    leader.core.role = .leader;
    leader.core.ballot = .{ .round = 1, .node = 1 };
    var effects = Log.Effects{};

    _ = try leader.reconfigure(5, &.{ 1, 2, 3 }, "next", &effects);
    var durable = Log.DurableState{};
    for (effects.writesSlice()) |write| try durable.apply(write);
    effects.confirmWritesDurable();

    var restored: Log.Node = undefined;
    try restored.restore(1, 4, &membership, &durable);
    try std.testing.expectError(error.LogSealed, restored.append(9, &effects));
}

test "restore does not remain sealed if proposed stop sign was overwritten" {
    const Log = ReplicatedLog(u64, .{
        .max_members = 3,
        .max_entries = 4,
        .max_batch = 4,
        .max_metadata_bytes = 8,
    });
    var membership: Log.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var leader: Log.Node = undefined;
    try leader.init(1, 4, &membership);
    leader.core.role = .leader;
    leader.core.ballot = .{ .round = 1, .node = 1 };
    var effects = Log.Effects{};

    // 1. Propose stop sign in slot 1 (reconfigure)
    _ = try leader.reconfigure(5, &.{ 1, 2, 3 }, "next", &effects);
    var durable = Log.DurableState{};
    for (effects.writesSlice()) |write| try durable.apply(write);
    effects.confirmWritesDurable();

    // 2. Overwrite slot 1 with an accept of a normal command at a higher ballot
    const overwrite_accept = Log.Write{
        .accept = .{
            .ballot = .{ .round = 2, .node = 1 },
            .slot = 1,
            .value = .{ .command = 42 },
        },
    };
    try durable.apply(overwrite_accept);

    // 3. Commit the normal command
    const commit_write = Log.Write{
        .commit = .{
            .slot = 1,
            .value = .{ .command = 42 },
        },
    };
    try durable.apply(commit_write);

    // 4. Restore and verify that append is NOT sealed since the stop sign was overwritten
    var restored: Log.Node = undefined;
    try restored.restore(1, 4, &membership, &durable);
    try std.testing.expect(restored.stop_pending == false);

    // We should be able to append now!
    restored.core.role = .leader;
    restored.core.ballot = .{ .round = 3, .node = 1 };
    effects.reset();
    _ = try restored.append(99, &effects);
}
