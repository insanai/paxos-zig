//! Deterministic, seed-driven simulation of the Paxos protocol core.
//!
//! Each run drives a small cluster through a randomized schedule of message
//! deliveries, drops, duplications, reorderings, partitions, crashes, and
//! restarts. Nodes persist durable writes to an in-memory journal that models
//! the host write-ahead contract, including crashes that land before, during,
//! or after persistence but before every message is sent. Safety invariants
//! are checked after every transition and the run ends with a fault-free
//! quiescence phase that must converge, catching liveness regressions.
//!
//! Failures print the seed, the step index, and a trailing action trace so a
//! run can be replayed exactly with `zig build sim -- --seed=N --steps=M`.

const std = @import("std");
const paxos = @import("paxos");

/// Fault probabilities in permille, applied per step or per transition.
pub const Faults = struct {
    /// Chance that a delivered message is dropped instead.
    drop_permille: u16 = 60,
    /// Chance that a delivered message stays queued and arrives again.
    duplicate_permille: u16 = 40,
    /// Chance that a transition ends in a crash before its effects settle.
    crash_permille: u16 = 20,
    /// Chance factor for cutting or healing one link in the partition matrix.
    link_permille: u16 = 25,
};

/// Returns a simulator for one comptime protocol configuration.
pub fn Simulator(comptime proto_options: paxos.Options) type {
    return struct {
        const Self = @This();
        pub const P = paxos.Protocol(u64, proto_options);
        const max_nodes = proto_options.max_members;
        const max_slots = proto_options.window_slots;
        const journal_capacity = 16_384;
        const network_capacity = 2_048;
        const trace_capacity = 48;
        const quiesce_rounds = 400;
        const noop: u64 = 0;

        pub const Config = struct {
            seed: u64,
            steps: u32 = 512,
            node_count: u16,
            priorities: [max_nodes]u32 = [_]u32{0} ** max_nodes,
            faults: Faults = .{},
            verbose: bool = false,
        };

        const Journal = struct {
            entries: [journal_capacity]P.Write,
            count: usize,
        };

        const ActionKind = enum {
            deliver,
            drop,
            duplicate,
            tick,
            propose,
            crash,
            restart,
            cut,
            heal,
            reconnect,
        };

        const Action = struct {
            step: u32,
            kind: ActionKind,
            node: u8 = 0,
            aux: u32 = 0,
        };

        config: Config,
        prng: std.Random.DefaultPrng,
        membership: P.Membership,
        nodes: [max_nodes]P.Node,
        effects: [max_nodes]P.Effects,
        journals: [max_nodes]Journal,
        alive: [max_nodes]bool,
        shadow_promised: [max_nodes]paxos.Ballot,
        shadow_decided: [max_nodes]paxos.Slot,
        cut_links: [max_nodes][max_nodes]bool,
        network: [network_capacity]P.Envelope,
        network_count: usize,
        golden: [max_slots]?u64,
        issued: u64,
        step_index: u32,
        trace: [trace_capacity]Action,
        trace_count: usize,

        /// Runs one seeded scenario to completion, including quiescence.
        pub fn run(self: *Self, config: Config) !void {
            std.debug.assert(config.node_count >= 1);
            std.debug.assert(config.node_count <= max_nodes);
            try self.initRun(config);
            while (self.step_index < config.steps) : (self.step_index += 1) {
                try self.stepOnce();
            }
            try self.quiesce();
        }

        fn initRun(self: *Self, config: Config) !void {
            self.config = config;
            self.prng = std.Random.DefaultPrng.init(config.seed);
            var ids: [max_nodes]paxos.NodeId = undefined;
            for (0..config.node_count) |index| ids[index] = @intCast(index + 1);
            try self.membership.init(ids[0..config.node_count]);
            for (0..config.node_count) |index| {
                try self.nodes[index].initWithPriority(
                    ids[index],
                    &self.membership,
                    config.priorities[index],
                );
                self.effects[index].init();
                self.journals[index].count = 0;
                self.alive[index] = true;
                self.shadow_promised[index] = paxos.Ballot.zero;
                self.shadow_decided[index] = 0;
            }
            self.cut_links = [_][max_nodes]bool{[_]bool{false} ** max_nodes} ** max_nodes;
            self.network_count = 0;
            self.golden = [_]?u64{null} ** max_slots;
            self.issued = 0;
            self.step_index = 0;
            self.trace_count = 0;
        }

        fn stepOnce(self: *Self) !void {
            const roll = self.prng.random().uintLessThan(u16, 1000);
            if (roll < 550) return self.deliverRandom();
            if (roll < 780) return self.tickRandom();
            if (roll < 860) return self.proposeAtLeader();
            if (roll < 880) return self.crashRandom();
            if (roll < 930) return self.restartRandom();
            if (roll < 930 + self.config.faults.link_permille) return self.flipLink(true);
            if (roll < 975) return self.flipLink(false);
            return self.reconnectRandom();
        }

        fn deliverRandom(self: *Self) !void {
            if (self.network_count == 0) return self.tickRandom();
            const random = self.prng.random();
            const index = random.uintLessThan(usize, self.network_count);
            const envelope = self.network[index];
            if (self.chance(self.config.faults.drop_permille)) {
                _ = self.networkRemove(index);
                self.record(.{ .step = self.step_index, .kind = .drop });
                return;
            }
            if (self.chance(self.config.faults.duplicate_permille)) {
                self.record(.{ .step = self.step_index, .kind = .duplicate });
            } else {
                _ = self.networkRemove(index);
            }
            const to_index: usize = envelope.to - 1;
            const from_index: usize = envelope.from - 1;
            if (self.cut_links[from_index][to_index]) return;
            if (!self.alive[to_index]) return;

            self.record(.{
                .step = self.step_index,
                .kind = .deliver,
                .node = @intCast(envelope.to),
                .aux = @intFromEnum(envelope.message),
            });
            self.nodes[to_index].step(envelope, &self.effects[to_index]) catch |err| {
                return self.fail("step returned {t}", err);
            };
            if (try self.applyEffects(to_index, true)) return;
            try self.checkInvariants(to_index);
        }

        fn tickRandom(self: *Self) !void {
            const index = self.randomLiveNode() orelse return;
            self.record(.{
                .step = self.step_index,
                .kind = .tick,
                .node = @intCast(index + 1),
            });
            self.nodes[index].tick(noop, &self.effects[index]) catch |err| {
                return self.fail("tick returned {t}", err);
            };
            if (try self.applyEffects(index, true)) return;
            try self.checkInvariants(index);
        }

        fn proposeAtLeader(self: *Self) !void {
            const index = self.findLeader() orelse return;
            const random = self.prng.random();
            const batched = random.uintLessThan(u16, 1000) < 250;
            const value = self.issued + 1;
            var count: u64 = 1;
            if (batched) {
                var slots: [2]paxos.Slot = undefined;
                _ = self.nodes[index].proposeBatch(
                    &.{ value, value + 1 },
                    &slots,
                    &self.effects[index],
                ) catch |err| return self.ignoreProposeError(err);
                count = 2;
            } else {
                _ = self.nodes[index].propose(value, &self.effects[index]) catch |err| {
                    return self.ignoreProposeError(err);
                };
            }
            self.issued += count;
            self.record(.{
                .step = self.step_index,
                .kind = .propose,
                .node = @intCast(index + 1),
                .aux = @intCast(count),
            });
            if (try self.applyEffects(index, true)) return;
            try self.checkInvariants(index);
        }

        fn ignoreProposeError(self: *Self, err: anyerror) !void {
            return switch (err) {
                error.NotLeader, error.WindowFull => {},
                else => self.fail("propose returned {t}", err),
            };
        }

        fn crashRandom(self: *Self) !void {
            const index = self.randomLiveNode() orelse return;
            if (self.liveCount() <= 1) return;
            self.alive[index] = false;
            self.record(.{
                .step = self.step_index,
                .kind = .crash,
                .node = @intCast(index + 1),
                .aux = 3,
            });
        }

        fn restartRandom(self: *Self) !void {
            for (0..self.config.node_count) |index| {
                if (!self.alive[index]) return self.restartNode(index);
            }
        }

        fn restartNode(self: *Self, index: usize) !void {
            var replayed = P.DurableState{};
            const journal = &self.journals[index];
            for (journal.entries[0..journal.count]) |write| {
                replayed.apply(write) catch |err| {
                    return self.fail("journal replay returned {t}", err);
                };
            }
            try self.mergeGolden(&replayed);
            self.nodes[index].restoreWithPriority(
                @intCast(index + 1),
                &self.membership,
                &replayed,
                self.config.priorities[index],
            ) catch |err| return self.fail("restore returned {t}", err);
            self.effects[index].init();
            self.alive[index] = true;
            self.shadow_promised[index] = replayed.promised;
            self.shadow_decided[index] = 0;
            self.record(.{
                .step = self.step_index,
                .kind = .restart,
                .node = @intCast(index + 1),
            });
        }

        fn reconnectRandom(self: *Self) !void {
            const index = self.randomLiveNode() orelse return;
            const random = self.prng.random();
            const peer_index = random.uintLessThan(usize, self.config.node_count);
            if (peer_index == index) return;
            self.record(.{
                .step = self.step_index,
                .kind = .reconnect,
                .node = @intCast(index + 1),
                .aux = @intCast(peer_index + 1),
            });
            self.nodes[index].reconnected(
                @intCast(peer_index + 1),
                &self.effects[index],
            ) catch |err| return self.fail("reconnected returned {t}", err);
            if (try self.applyEffects(index, true)) return;
            try self.checkInvariants(index);
        }

        fn flipLink(self: *Self, cut: bool) !void {
            if (self.config.node_count < 2) return;
            const random = self.prng.random();
            const a = random.uintLessThan(usize, self.config.node_count);
            const b = random.uintLessThan(usize, self.config.node_count);
            if (a == b) return;
            self.cut_links[a][b] = cut;
            self.cut_links[b][a] = cut;
            self.record(.{
                .step = self.step_index,
                .kind = if (cut) .cut else .heal,
                .node = @intCast(a + 1),
                .aux = @intCast(b + 1),
            });
        }

        /// Persists writes, then queues messages, unless a crash intervenes.
        /// Returns true when the node crashed and its effects must not be
        /// treated as observed state.
        fn applyEffects(self: *Self, index: usize, allow_crash: bool) !bool {
            const effects = &self.effects[index];
            if (allow_crash and self.chance(self.config.faults.crash_permille)) {
                try self.crashDuringEffects(index);
                return true;
            }
            try self.persistWrites(index, effects.writesSlice().len);
            effects.confirmWritesDurable();
            self.enqueue(effects.messagesSlice());
            return false;
        }

        /// Models a crash at one of the three host commit points: before any
        /// write, after a durable prefix of writes, or after all writes with
        /// only a prefix of the messages sent.
        fn crashDuringEffects(self: *Self, index: usize) !void {
            const effects = &self.effects[index];
            const random = self.prng.random();
            const variant = random.uintLessThan(u8, 3);
            switch (variant) {
                0 => {},
                1 => {
                    const writes = effects.writesSlice();
                    const kept = random.uintAtMost(usize, writes.len);
                    try self.persistWrites(index, kept);
                },
                else => {
                    try self.persistWrites(index, effects.writesSlice().len);
                    effects.confirmWritesDurable();
                    const messages = effects.messagesSlice();
                    const sent = random.uintAtMost(usize, messages.len);
                    self.enqueue(messages[0..sent]);
                },
            }
            self.alive[index] = false;
            self.record(.{
                .step = self.step_index,
                .kind = .crash,
                .node = @intCast(index + 1),
                .aux = variant,
            });
        }

        fn persistWrites(self: *Self, index: usize, count: usize) !void {
            const journal = &self.journals[index];
            const writes = self.effects[index].writesSlice()[0..count];
            if (journal.count + writes.len > journal_capacity) {
                return self.fail("journal capacity exhausted at {d}", journal.count);
            }
            for (writes) |write| {
                journal.entries[journal.count] = write;
                journal.count += 1;
            }
        }

        fn enqueue(self: *Self, messages: []const P.Envelope) void {
            for (messages) |envelope| {
                if (self.network_count == network_capacity) return;
                self.network[self.network_count] = envelope;
                self.network_count += 1;
            }
        }

        fn networkRemove(self: *Self, index: usize) P.Envelope {
            const envelope = self.network[index];
            self.network_count -= 1;
            self.network[index] = self.network[self.network_count];
            return envelope;
        }

        /// Safety oracle, run after every observed transition.
        fn checkInvariants(self: *Self, index: usize) !void {
            const node = &self.nodes[index];
            if (node.durable.promised.lessThan(self.shadow_promised[index])) {
                return self.fail("promised ballot regressed on node {d}", index + 1);
            }
            self.shadow_promised[index] = node.durable.promised;
            if (node.decidedThrough() < self.shadow_decided[index]) {
                return self.fail("decided prefix regressed on node {d}", index + 1);
            }
            self.shadow_decided[index] = node.decidedThrough();
            try self.mergeGolden(&node.durable);
        }

        /// Agreement and validity: the first durable commit for a slot fixes
        /// its value forever, and every value was proposed or is the no-op.
        fn mergeGolden(self: *Self, durable: *const P.DurableState) !void {
            for (&durable.cells) |*cell| {
                const value = cell.committed orelse continue;
                const slot_index: usize = @intCast(cell.slot - 1);
                if (value != noop and value > self.issued) {
                    return self.fail("unproposed value committed in slot {d}", cell.slot);
                }
                if (self.golden[slot_index]) |chosen| {
                    if (chosen != value) {
                        return self.fail("conflicting decisions in slot {d}", cell.slot);
                    }
                } else {
                    self.golden[slot_index] = value;
                }
            }
        }

        /// Fault-free convergence phase: heal everything and require the
        /// cluster to elect a leader and agree on one decided prefix.
        fn quiesce(self: *Self) !void {
            for (0..self.config.node_count) |index| {
                if (!self.alive[index]) try self.restartNode(index);
            }
            self.cut_links = [_][max_nodes]bool{[_]bool{false} ** max_nodes} ** max_nodes;
            var round: u32 = 0;
            while (round < quiesce_rounds) : (round += 1) {
                for (0..self.config.node_count) |index| {
                    self.nodes[index].tick(noop, &self.effects[index]) catch |err| {
                        return self.fail("quiescent tick returned {t}", err);
                    };
                    _ = try self.applyEffects(index, false);
                    try self.checkInvariants(index);
                }
                try self.drainAll();
                if (self.converged()) return;
            }
            return self.fail("no convergence after {d} quiescent rounds", quiesce_rounds);
        }

        fn drainAll(self: *Self) !void {
            while (self.network_count > 0) {
                const envelope = self.networkRemove(0);
                const to_index: usize = envelope.to - 1;
                self.nodes[to_index].step(envelope, &self.effects[to_index]) catch |err| {
                    return self.fail("quiescent step returned {t}", err);
                };
                _ = try self.applyEffects(to_index, false);
                try self.checkInvariants(to_index);
            }
        }

        fn converged(self: *const Self) bool {
            var has_leader = false;
            const decided = self.nodes[0].decidedThrough();
            for (0..self.config.node_count) |index| {
                if (self.nodes[index].role == .leader) has_leader = true;
                if (self.nodes[index].decidedThrough() != decided) return false;
            }
            if (!has_leader) return false;
            for (self.golden, 0..) |entry, slot_index| {
                if (entry != null and slot_index + 1 > decided) return false;
            }
            return true;
        }

        fn findLeader(self: *const Self) ?usize {
            for (0..self.config.node_count) |index| {
                if (self.alive[index] and self.nodes[index].role == .leader) return index;
            }
            return null;
        }

        fn randomLiveNode(self: *Self) ?usize {
            const live = self.liveCount();
            if (live == 0) return null;
            var target = self.prng.random().uintLessThan(usize, live);
            for (0..self.config.node_count) |index| {
                if (!self.alive[index]) continue;
                if (target == 0) return index;
                target -= 1;
            }
            unreachable;
        }

        fn liveCount(self: *const Self) usize {
            var count: usize = 0;
            for (self.alive[0..self.config.node_count]) |is_alive| {
                if (is_alive) count += 1;
            }
            return count;
        }

        fn chance(self: *Self, permille: u16) bool {
            return self.prng.random().uintLessThan(u16, 1000) < permille;
        }

        fn record(self: *Self, action: Action) void {
            self.trace[self.trace_count % trace_capacity] = action;
            self.trace_count += 1;
            if (self.config.verbose) {
                std.debug.print("step {d}: {t} node={d} aux={d}\n", .{
                    action.step, action.kind, action.node, action.aux,
                });
            }
        }

        fn fail(self: *const Self, comptime reason: []const u8, detail: anytype) anyerror {
            std.debug.print(
                "simulation failure: " ++ reason ++ "\n" ++
                    "  seed={d} steps={d} nodes={d} at step {d}\n",
                .{
                    detail,
                    self.config.seed,
                    self.config.steps,
                    self.config.node_count,
                    self.step_index,
                },
            );
            self.dumpState();
            return error.SimulationFailed;
        }

        fn dumpState(self: *const Self) void {
            for (0..self.config.node_count) |index| {
                const node = &self.nodes[index];
                std.debug.print(
                    "  node {d}: alive={} role={t} ballot=({d},{d}) " ++
                        "promised=({d},{d}) decided={d} next_slot={d}\n",
                    .{
                        index + 1,
                        self.alive[index],
                        node.role,
                        node.ballot.round,
                        node.ballot.node,
                        node.durable.promised.round,
                        node.durable.promised.node,
                        node.decidedThrough(),
                        node.next_slot,
                    },
                );
                for (&node.durable.cells) |*cell| {
                    if (cell.slot == 0 or cell.slot > 6) continue;
                    const accepted = cell.accepted;
                    const committed = cell.committed;
                    if (accepted == null and committed == null) continue;
                    std.debug.print(
                        "    slot {d}: accepted=({d},{d})@{d} committed={?d}\n",
                        .{
                            cell.slot,
                            if (accepted) |a| a.ballot.round else 0,
                            if (accepted) |a| a.ballot.node else 0,
                            if (accepted) |a| a.value else 0,
                            committed,
                        },
                    );
                }
            }
            const start = self.trace_count -| trace_capacity;
            std.debug.print("  last actions:\n", .{});
            for (start..self.trace_count) |sequence| {
                const action = self.trace[sequence % trace_capacity];
                std.debug.print("    step {d}: {t} node={d} aux={d}\n", .{
                    action.step, action.kind, action.node, action.aux,
                });
            }
        }
    };
}

/// Three-node majority configuration exercised by the default test sweep.
pub const Sim3 = Simulator(.{
    .max_members = 3,
    .window_slots = 32,
    .election_timeout_ticks = 4,
    .heartbeat_interval_ticks = 2,
    .resend_interval_ticks = 3,
});

/// Five-node flexible-quorum configuration (phase one 4, phase two 2).
pub const Sim5Flexible = Simulator(.{
    .max_members = 5,
    .window_slots = 32,
    .read_quorum_size = 4,
    .write_quorum_size = 2,
    .election_timeout_ticks = 4,
    .heartbeat_interval_ticks = 2,
    .resend_interval_ticks = 3,
});

test "simulation: three-node cluster keeps agreement under faults" {
    const sim_options = @import("sim_options");
    const sim = try std.testing.allocator.create(Sim3);
    defer std.testing.allocator.destroy(sim);
    var seed: u64 = 1;
    while (seed <= sim_options.seeds) : (seed += 1) {
        try sim.run(.{
            .seed = seed,
            .steps = sim_options.steps,
            .node_count = 3,
        });
    }
}

test "simulation: five-node flexible quorums keep agreement under faults" {
    const sim_options = @import("sim_options");
    const sim = try std.testing.allocator.create(Sim5Flexible);
    defer std.testing.allocator.destroy(sim);
    const seeds = @max(sim_options.seeds / 4, 8);
    var seed: u64 = 1;
    while (seed <= seeds) : (seed += 1) {
        try sim.run(.{
            .seed = seed,
            .steps = sim_options.steps,
            .node_count = 5,
        });
    }
}

test "simulation: prioritized members keep agreement under faults" {
    const sim_options = @import("sim_options");
    const sim = try std.testing.allocator.create(Sim3);
    defer std.testing.allocator.destroy(sim);
    const seeds = @max(sim_options.seeds / 4, 8);
    var seed: u64 = 1;
    while (seed <= seeds) : (seed += 1) {
        try sim.run(.{
            .seed = seed,
            .steps = sim_options.steps,
            .node_count = 3,
            .priorities = .{ 7, 3, 0 },
        });
    }
}
