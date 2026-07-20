//! Deterministic multi-node reconfiguration harness for the replicated log.
//!
//! Three `ReplicatedLog` nodes exchange messages through an in-memory network
//! with seeded, shuffled delivery, mirroring how the protocol simulator
//! shuttles `Effects` messages. Scenarios elect a leader, commit commands,
//! seal the log with a stop sign under adverse schedules (a dropped and a
//! duplicated accept around the seal, plus reordering), and check the sealing
//! oracles: nothing decides past the seal slot, every node agrees on the stop
//! sign and the sealed prefix, journal replay preserves the seal, and the
//! next epoch started from the stop sign elects a leader and decides entries.
//!
//! Every schedule is a pure function of the seed; failures print the seed so
//! a run can be replayed exactly.

const std = @import("std");
const paxos = @import("paxos");

const max_entries = 8;

const Log = paxos.ReplicatedLog(u64, .{
    .max_members = 4,
    .max_entries = max_entries,
    .max_batch = 2,
    .max_metadata_bytes = 32,
    .election_timeout_ticks = 8,
    .heartbeat_interval_ticks = 2,
    .resend_interval_ticks = 3,
});

const node_count = 3;
const network_capacity = 512;
const journal_capacity = 256;
const max_deliveries = 10_000;
const settle_rounds = 4;
const scenario_seeds = 16;
const noop: u64 = 0;

const Journal = struct {
    entries: [journal_capacity]Log.Write,
    count: usize,
};

/// One fixed-membership epoch: nodes, journals, and an in-memory network.
/// Writes are journaled and confirmed durable before messages are enqueued,
/// honoring the write-before-send contract the protocol requires of hosts.
const Cluster = struct {
    seed: u64,
    prng: std.Random.DefaultPrng,
    membership: Log.Membership,
    ids: [node_count]paxos.NodeId,
    nodes: [node_count]Log.Node,
    effects: [node_count]Log.Effects,
    journals: [node_count]Journal,
    network: [network_capacity]Log.Envelope,
    network_count: usize,

    fn init(
        self: *Cluster,
        seed: u64,
        configuration_id: u64,
        ids: [node_count]paxos.NodeId,
    ) !void {
        self.seed = seed;
        self.prng = std.Random.DefaultPrng.init(seed);
        self.ids = ids;
        try self.membership.init(&self.ids);
        for (0..node_count) |index| {
            try self.nodes[index].init(self.ids[index], configuration_id, &self.membership);
            self.effects[index].init();
            self.journals[index].count = 0;
        }
        self.network_count = 0;
    }

    /// Starts the next epoch named by a decided stop sign.
    fn initFromStop(self: *Cluster, seed: u64, stop: *const Log.StopSign) !void {
        self.seed = seed;
        self.prng = std.Random.DefaultPrng.init(seed);
        const members = stop.membersSlice();
        if (members.len != node_count) {
            return self.fail("stop sign names {d} members; harness expects 3", .{members.len});
        }
        for (0..node_count) |index| self.ids[index] = members[index];
        for (0..node_count) |index| {
            try self.nodes[index].initFromStop(self.ids[index], stop, &self.membership, 0);
            self.effects[index].init();
            self.journals[index].count = 0;
        }
        self.network_count = 0;
    }

    fn indexOf(self: *const Cluster, id: paxos.NodeId) !usize {
        for (self.ids, 0..) |member, index| {
            if (member == id) return index;
        }
        return self.fail("message addressed outside the configuration: {d}", .{id});
    }

    /// Journals the node's writes, confirms durability, then enqueues its
    /// messages. Call exactly once after every node transition.
    fn applyEffects(self: *Cluster, index: usize) !void {
        const effects = &self.effects[index];
        const journal = &self.journals[index];
        const writes = effects.writesSlice();
        if (journal.count + writes.len > journal_capacity) {
            return self.fail("journal capacity exhausted on node {d}", .{self.ids[index]});
        }
        for (writes) |write| {
            journal.entries[journal.count] = write;
            journal.count += 1;
        }
        effects.confirmWritesDurable();
        for (effects.messagesSlice()) |envelope| {
            if (self.network_count == network_capacity) {
                return self.fail("network capacity exhausted", .{});
            }
            self.network[self.network_count] = envelope;
            self.network_count += 1;
        }
    }

    fn deliverAt(self: *Cluster, network_index: usize) !void {
        const envelope = self.network[network_index];
        self.network_count -= 1;
        self.network[network_index] = self.network[self.network_count];
        const to_index = try self.indexOf(envelope.to);
        self.nodes[to_index].step(envelope, &self.effects[to_index]) catch |err| {
            return self.fail("step returned {t}", .{err});
        };
        try self.applyEffects(to_index);
    }

    /// Delivers every queued message in seeded, shuffled order. The shuffle
    /// reorders concurrent messages while losing none, so a fault-free drain
    /// must converge; the delivery bound catches livelock regressions.
    fn drainShuffled(self: *Cluster) !void {
        var deliveries: u32 = 0;
        while (self.network_count > 0) {
            deliveries += 1;
            if (deliveries > max_deliveries) {
                return self.fail("network did not drain in {d} deliveries", .{max_deliveries});
            }
            const pick = self.prng.random().uintLessThan(usize, self.network_count);
            try self.deliverAt(pick);
        }
    }

    /// Ticks every node for `rounds` rounds, draining between rounds, so
    /// heartbeats and retransmissions get a chance to decide anything that
    /// could still decide. Used to harden the "nothing past the seal" oracle.
    fn settle(self: *Cluster, rounds: u32) !void {
        var round: u32 = 0;
        while (round < rounds) : (round += 1) {
            for (0..node_count) |index| {
                self.nodes[index].tick(noop, &self.effects[index]) catch |err| {
                    return self.fail("tick returned {t}", .{err});
                };
                try self.applyEffects(index);
            }
            try self.drainShuffled();
        }
    }

    fn elect(self: *Cluster, index: usize) !void {
        self.nodes[index].campaign(noop, &self.effects[index]) catch |err| {
            return self.fail("campaign returned {t}", .{err});
        };
        try self.applyEffects(index);
        try self.drainShuffled();
        if (self.nodes[index].core.role != .leader) {
            return self.fail("node {d} failed to win a fault-free election", .{self.ids[index]});
        }
    }

    fn appendAt(self: *Cluster, index: usize, value: u64) !paxos.Slot {
        const slot = self.nodes[index].append(value, &self.effects[index]) catch |err| {
            return self.fail("append returned {t}", .{err});
        };
        try self.applyEffects(index);
        return slot;
    }

    fn findAccept(self: *const Cluster, slot: paxos.Slot, to: paxos.NodeId) ?usize {
        for (self.network[0..self.network_count], 0..) |envelope, index| {
            if (envelope.to != to) continue;
            switch (envelope.message) {
                .accept => |accept| if (accept.slot == slot) return index,
                else => {},
            }
        }
        return null;
    }

    /// Adverse schedule: removes one queued accept for `slot` addressed to
    /// `to`, modeling a message lost exactly around the seal.
    fn dropAccept(self: *Cluster, slot: paxos.Slot, to: paxos.NodeId) !void {
        const index = self.findAccept(slot, to) orelse {
            return self.fail("no accept for slot {d} to node {d} to drop", .{ slot, to });
        };
        self.network_count -= 1;
        self.network[index] = self.network[self.network_count];
    }

    /// Adverse schedule: enqueues a second copy of one accept for `slot`
    /// addressed to `to`, modeling a duplicated message around the seal.
    fn duplicateAccept(self: *Cluster, slot: paxos.Slot, to: paxos.NodeId) !void {
        const index = self.findAccept(slot, to) orelse {
            return self.fail("no accept for slot {d} to node {d} to duplicate", .{ slot, to });
        };
        if (self.network_count == network_capacity) {
            return self.fail("network capacity exhausted", .{});
        }
        self.network[self.network_count] = self.network[index];
        self.network_count += 1;
    }

    /// Oracle: every node observes the same stop sign, seals at the same
    /// slot, and holds an identical sealed prefix.
    fn expectSealAgreement(
        self: *Cluster,
        seal_slot: paxos.Slot,
        configuration_id: u64,
        members: []const paxos.NodeId,
        metadata: []const u8,
    ) !void {
        for (0..node_count) |index| {
            const node = &self.nodes[index];
            const stop = node.isReconfigured() orelse {
                return self.fail("node {d} does not observe the seal", .{self.ids[index]});
            };
            if (stop.configuration_id != configuration_id) {
                return self.fail("node {d} sealed the wrong configuration", .{self.ids[index]});
            }
            if (!std.mem.eql(paxos.NodeId, stop.membersSlice(), members)) {
                return self.fail("node {d} sealed the wrong membership", .{self.ids[index]});
            }
            if (!std.mem.eql(u8, stop.metadataSlice(), metadata)) {
                return self.fail("node {d} sealed the wrong metadata", .{self.ids[index]});
            }
            if (node.decidedThrough() != seal_slot) {
                return self.fail("node {d} decided through {d}, expected the seal slot", .{
                    self.ids[index],
                    node.decidedThrough(),
                });
            }
        }
        try self.expectSealedPrefixAgreement(seal_slot);
    }

    /// Oracle: the sealed prefix is identical, entry by entry, on all nodes.
    fn expectSealedPrefixAgreement(self: *Cluster, seal_slot: paxos.Slot) !void {
        var slot: paxos.Slot = 1;
        while (slot <= seal_slot) : (slot += 1) {
            const reference = self.nodes[0].read(slot) orelse {
                return self.fail("node {d} is missing sealed slot {d}", .{ self.ids[0], slot });
            };
            for (1..node_count) |index| {
                const entry = self.nodes[index].read(slot) orelse {
                    return self.fail("node {d} is missing sealed slot {d}", .{
                        self.ids[index],
                        slot,
                    });
                };
                if (!std.meta.eql(reference, entry)) {
                    return self.fail("nodes disagree on sealed slot {d}", .{slot});
                }
            }
        }
    }

    /// Oracle: the sealed configuration decides nothing past the seal slot
    /// and every node refuses new appends.
    fn expectNothingAfterSeal(self: *Cluster, seal_slot: paxos.Slot) !void {
        for (0..node_count) |index| {
            var slot: paxos.Slot = seal_slot + 1;
            while (slot <= max_entries) : (slot += 1) {
                if (self.nodes[index].read(slot) != null) {
                    return self.fail("node {d} decided slot {d} past the seal", .{
                        self.ids[index],
                        slot,
                    });
                }
            }
            _ = self.nodes[index].append(99, &self.effects[index]) catch |err| {
                if (err != error.LogSealed) {
                    return self.fail("sealed append returned {t}", .{err});
                }
                continue;
            };
            return self.fail("node {d} accepted an append after the seal", .{self.ids[index]});
        }
    }

    /// Oracle: replaying a sealed node's journal into a restored node
    /// preserves the seal, so a restart cannot reopen a sealed epoch.
    fn expectReplayPreservesSeal(
        self: *Cluster,
        index: usize,
        configuration_id: u64,
        sealed_configuration_id: u64,
    ) !void {
        var durable = Log.DurableState{};
        const journal = &self.journals[index];
        for (journal.entries[0..journal.count]) |write| {
            durable.apply(write) catch |err| {
                return self.fail("journal replay returned {t}", .{err});
            };
        }
        var restored: Log.Node = undefined;
        restored.restore(
            self.ids[index],
            configuration_id,
            &self.membership,
            &durable,
        ) catch |err| return self.fail("restore returned {t}", .{err});
        const stop = restored.isReconfigured() orelse {
            return self.fail("restored node {d} lost the seal", .{self.ids[index]});
        };
        if (stop.configuration_id != sealed_configuration_id) {
            return self.fail("restored node {d} sealed the wrong configuration", .{
                self.ids[index],
            });
        }
        var effects = Log.Effects{};
        _ = restored.append(99, &effects) catch |err| {
            if (err != error.LogSealed) {
                return self.fail("restored append returned {t}", .{err});
            }
            return;
        };
        return self.fail("restored node {d} accepted an append after the seal", .{
            self.ids[index],
        });
    }

    /// Oracle: a duplicated commit of the seal slot is idempotent: no new
    /// durable write and no movement of the decided prefix.
    fn expectDuplicateSealCommitIsIdempotent(self: *Cluster, seal_slot: paxos.Slot) !void {
        const entry = self.nodes[1].read(seal_slot) orelse {
            return self.fail("node {d} is missing the seal", .{self.ids[1]});
        };
        const decided_before = self.nodes[1].decidedThrough();
        self.nodes[1].step(.{
            .from = self.ids[0],
            .to = self.ids[1],
            .message = .{ .commit = .{ .slot = seal_slot, .value = entry } },
        }, &self.effects[1]) catch |err| {
            return self.fail("duplicate seal commit returned {t}", .{err});
        };
        if (self.effects[1].writesSlice().len != 0) {
            return self.fail("duplicate seal commit produced a durable write", .{});
        }
        try self.applyEffects(1);
        if (self.nodes[1].decidedThrough() != decided_before) {
            return self.fail("duplicate seal commit moved the decided prefix", .{});
        }
    }

    /// Oracle: every node of the new epoch runs the sealed configuration and
    /// decides exactly the given commands in order.
    fn expectEpochDecides(
        self: *Cluster,
        configuration_id: u64,
        values: []const u64,
    ) !void {
        for (0..node_count) |index| {
            const node = &self.nodes[index];
            if (node.configurationId() != configuration_id) {
                return self.fail("node {d} runs the wrong configuration", .{self.ids[index]});
            }
            if (node.decidedThrough() < values.len) {
                return self.fail("node {d} decided only {d} new-epoch entries", .{
                    self.ids[index],
                    node.decidedThrough(),
                });
            }
            for (values, 1..) |value, slot| {
                const entry = node.read(@intCast(slot)) orelse {
                    return self.fail("node {d} is missing new-epoch slot {d}", .{
                        self.ids[index],
                        slot,
                    });
                };
                switch (entry) {
                    .command => |command| if (command != value) {
                        return self.fail("wrong command in new-epoch slot {d}", .{slot});
                    },
                    .stop => return self.fail("unexpected stop in new-epoch slot {d}", .{slot}),
                }
            }
        }
    }

    fn fail(self: *const Cluster, comptime reason: []const u8, args: anytype) anyerror {
        std.debug.print(
            "reconfiguration failure: " ++ reason ++ "\n  seed={d}\n",
            args ++ .{self.seed},
        );
        return error.ReconfigurationFailed;
    }
};

/// Seals via `checkpoint` while a command is still in flight, with one seal
/// accept dropped, one duplicated, and all delivery shuffled by the seed.
fn runCheckpointScenario(cluster: *Cluster, next_epoch: *Cluster, seed: u64) !void {
    try cluster.init(seed, 1, .{ 1, 2, 3 });
    try cluster.elect(0);
    _ = try cluster.appendAt(0, 101);
    _ = try cluster.appendAt(0, 102);
    try cluster.drainShuffled();

    // Leave slot 3 in flight so the seal races an open command.
    const open_slot = try cluster.appendAt(0, 103);
    if (open_slot != 3) return cluster.fail("unexpected open slot {d}", .{open_slot});
    const seal_slot = cluster.nodes[0].checkpoint(
        "snapshot:7",
        &cluster.effects[0],
    ) catch |err| return cluster.fail("checkpoint returned {t}", .{err});
    try cluster.applyEffects(0);
    if (seal_slot != 4) return cluster.fail("unexpected seal slot {d}", .{seal_slot});

    // Adverse schedule around the seal: node 3 loses its seal accept and must
    // learn the seal from the commit alone; node 2 votes on a duplicate.
    try cluster.dropAccept(seal_slot, 3);
    try cluster.duplicateAccept(seal_slot, 2);
    try cluster.drainShuffled();
    try cluster.settle(settle_rounds);

    try cluster.expectSealAgreement(seal_slot, 2, &.{ 1, 2, 3 }, "snapshot:7");
    try cluster.expectNothingAfterSeal(seal_slot);
    for (0..node_count) |index| {
        try cluster.expectReplayPreservesSeal(index, 1, 2);
    }

    // Epoch handover: the stop sign names the next configuration, which
    // elects its own leader and decides fresh entries.
    const stop = cluster.nodes[0].isReconfigured().?;
    try next_epoch.initFromStop(seed, &stop);
    try next_epoch.elect(1);
    _ = try next_epoch.appendAt(1, 201);
    _ = try next_epoch.appendAt(1, 202);
    try next_epoch.drainShuffled();
    try next_epoch.expectEpochDecides(2, &.{ 201, 202 });
}

/// Seals via `reconfigure` into a different member set, checks a duplicated
/// seal commit for idempotence, and hands over to the new membership.
fn runMembershipChangeScenario(cluster: *Cluster, next_epoch: *Cluster, seed: u64) !void {
    try cluster.init(seed, 1, .{ 1, 2, 3 });
    try cluster.elect(0);
    _ = try cluster.appendAt(0, 11);
    try cluster.drainShuffled();

    const seal_slot = cluster.nodes[0].reconfigure(
        2,
        &.{ 2, 3, 4 },
        "handover:next",
        &cluster.effects[0],
    ) catch |err| return cluster.fail("reconfigure returned {t}", .{err});
    try cluster.applyEffects(0);
    try cluster.drainShuffled();
    try cluster.settle(settle_rounds);

    try cluster.expectSealAgreement(seal_slot, 2, &.{ 2, 3, 4 }, "handover:next");
    try cluster.expectNothingAfterSeal(seal_slot);
    try cluster.expectDuplicateSealCommitIsIdempotent(seal_slot);
    for (0..node_count) |index| {
        try cluster.expectReplayPreservesSeal(index, 1, 2);
    }

    // The departing node 1 stays sealed while nodes 2, 3, and 4 start the
    // new configuration and decide without it.
    const stop = cluster.nodes[0].isReconfigured().?;
    try next_epoch.initFromStop(seed, &stop);
    try next_epoch.elect(1);
    _ = try next_epoch.appendAt(1, 21);
    try next_epoch.drainShuffled();
    try next_epoch.expectEpochDecides(2, &.{21});
}

test "reconfiguration: checkpoint seal survives drop, duplicate, and reorder" {
    const cluster = try std.testing.allocator.create(Cluster);
    defer std.testing.allocator.destroy(cluster);
    const next_epoch = try std.testing.allocator.create(Cluster);
    defer std.testing.allocator.destroy(next_epoch);
    var seed: u64 = 1;
    while (seed <= scenario_seeds) : (seed += 1) {
        try runCheckpointScenario(cluster, next_epoch, seed);
    }
}

test "reconfiguration: membership handover reaches the new configuration" {
    const cluster = try std.testing.allocator.create(Cluster);
    defer std.testing.allocator.destroy(cluster);
    const next_epoch = try std.testing.allocator.create(Cluster);
    defer std.testing.allocator.destroy(next_epoch);
    var seed: u64 = 1;
    while (seed <= scenario_seeds) : (seed += 1) {
        try runMembershipChangeScenario(cluster, next_epoch, seed);
    }
}
