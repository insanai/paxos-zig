//! Durable-path benchmark: the first measurement of the library's actual
//! safety contract. Each node journals its `Effects.writes` to a per-node
//! append-only file and syncs it to disk before any message moves, exactly
//! as a production host must. Two host strategies are measured:
//!
//! - fsync-each: one value proposed, every transition synced individually.
//! - group8: eight values pipelined; each node buffers the group's writes
//!   and messages, syncs once, then releases the messages. Buffering the
//!   messages before the sync completes requires opting out of the debug
//!   effect-order guard; the host still enforces the contract itself.
//!
//! Numbers depend on the filesystem and disk; they are orders of magnitude
//! above the in-memory results, which is the honest cost of durability.

const std = @import("std");
const paxos = @import("paxos");

const node_count = 3;
const value_count = 512;
const sample_count = 3;
const record_size = 33;

const P = paxos.Protocol(u64, .{
    .max_members = node_count,
    .max_slots = value_count,
    .assert_effect_order = false,
});

const bench_dir = ".zig-cache/durable-bench";

const State = struct {
    membership: P.Membership,
    nodes: [node_count]P.Node,
    effects: P.Effects,
    files: [node_count]std.Io.File,
    offsets: [node_count]u64,
    fsyncs: u64,
    queue: [4_096]P.Envelope,
    queue_count: usize,
    pending: [node_count][1_024]P.Envelope,
    pending_count: [node_count]usize,
    encode_buffer: [record_size * (2 * value_count + 1)]u8,
};

var state: State = undefined;

const Sample = struct {
    nanoseconds: u64,
    fsyncs: u64,
    checksum: u64,
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const cwd = std.Io.Dir.cwd();
    cwd.createDirPath(io, bench_dir) catch {};

    try runMode(io, "fsync-each", 1);
    try runMode(io, "group8", 8);
}

fn runMode(io: std.Io, name: []const u8, window: u32) !void {
    var samples: [sample_count]Sample = undefined;
    for (&samples, 0..) |*sample, index| {
        sample.* = try runSample(io, window, index);
    }
    for (1..samples.len) |unsorted| {
        var index = unsorted;
        while (index > 0) : (index -= 1) {
            if (samples[index - 1].nanoseconds <= samples[index].nanoseconds) break;
            std.mem.swap(Sample, &samples[index - 1], &samples[index]);
        }
    }
    const median = samples[sample_count / 2];
    const per_value = @as(f64, @floatFromInt(median.nanoseconds)) /
        @as(f64, @floatFromInt(value_count));
    std.debug.print(
        \\workload:       durable-u64-3n mode={s} (Zig Multi-Paxos 0.1.0)
        \\values:         {d} nodes={d} journal=file-per-node fsync-before-send
        \\median_ns:      {d}
        \\ns_per_value:   {d:.2}
        \\fsyncs:         {d}
        \\checksum:       {d}
        \\
    , .{
        name,            value_count,
        node_count,      median.nanoseconds,
        per_value,       median.fsyncs,
        median.checksum,
    });
    std.debug.print(
        "{{\"impl\":\"paxos-zig\",\"workload\":\"durable-u64-3n\"," ++
            "\"mode\":\"{s}\",\"values\":{d},\"nodes\":{d}," ++
            "\"payload_bytes\":8,\"ns_total_median\":{d}," ++
            "\"ns_per_value\":{d:.2},\"fsyncs\":{d},\"checksum\":{d}}}\n\n",
        .{
            name,            value_count,
            node_count,      median.nanoseconds,
            per_value,       median.fsyncs,
            median.checksum,
        },
    );
    const expected = value_count * (value_count + 1) / 2;
    if (median.checksum != expected) {
        std.debug.print("SELF-CHECK FAILED for durable/{s}\n", .{name});
        std.process.exit(1);
    }
}

fn runSample(io: std.Io, window: u32, sample_index: usize) !Sample {
    const s = &state;
    try s.membership.init(&.{ 1, 2, 3 });
    inline for (0..node_count) |index| {
        try s.nodes[index].init(@intCast(index + 1), &s.membership);
    }
    try openJournals(io, sample_index);
    s.fsyncs = 0;
    s.queue_count = 0;
    s.pending_count = [_]usize{0} ** node_count;
    s.effects.init();

    try s.nodes[0].campaign(0, &s.effects);
    try settle(io, 0);
    try drain(io);
    std.debug.assert(s.nodes[0].role == .leader);

    const started = std.Io.Clock.Timestamp.now(io, .awake);
    var seq: u64 = 1;
    while (seq <= value_count) {
        const batch: u64 = @min(window, value_count - seq + 1);
        for (0..batch) |offset| {
            _ = try s.nodes[0].propose(seq + offset, &s.effects);
            try buffer(io, 0);
        }
        try flushNode(io, 0);
        try drainGrouped(io, window);
        seq += batch;
    }
    const finished = std.Io.Clock.Timestamp.now(io, .awake);

    var checksum: u64 = 0;
    for (1..value_count + 1) |slot| {
        checksum +%= s.nodes[0].committedAt(@intCast(slot)).?;
    }
    closeJournals(io);
    return .{
        .nanoseconds = @intCast(started.durationTo(finished).raw.nanoseconds),
        .fsyncs = s.fsyncs,
        .checksum = checksum,
    };
}

fn openJournals(io: std.Io, sample_index: usize) !void {
    const s = &state;
    inline for (0..node_count) |index| {
        var name_buffer: [64]u8 = undefined;
        const path = try std.fmt.bufPrint(
            &name_buffer,
            bench_dir ++ "/node{d}-sample{d}.journal",
            .{ index + 1, sample_index },
        );
        s.files[index] = try std.Io.Dir.cwd().createFile(io, path, .{
            .truncate = true,
        });
        s.offsets[index] = 0;
    }
}

fn closeJournals(io: std.Io) void {
    for (&state.files) |*file| file.close(io);
}

/// Serializes and appends one node's writes, syncs, then queues messages.
fn settle(io: std.Io, node_index: usize) !void {
    try persist(io, node_index);
    for (state.effects.messagesSlice()) |message| {
        enqueue(&state.queue, &state.queue_count, message);
    }
}

/// Group-commit path: persists writes without syncing yet and holds the
/// node's messages back until `flushNode` syncs its journal.
fn buffer(io: std.Io, node_index: usize) !void {
    try append(io, node_index);
    for (state.effects.messagesSlice()) |message| {
        enqueue(
            &state.pending[node_index],
            &state.pending_count[node_index],
            message,
        );
    }
}

fn flushNode(io: std.Io, node_index: usize) !void {
    const s = &state;
    try s.files[node_index].sync(io);
    s.fsyncs += 1;
    for (s.pending[node_index][0..s.pending_count[node_index]]) |message| {
        enqueue(&s.queue, &s.queue_count, message);
    }
    s.pending_count[node_index] = 0;
}

fn persist(io: std.Io, node_index: usize) !void {
    try append(io, node_index);
    try state.files[node_index].sync(io);
    state.fsyncs += 1;
}

fn append(io: std.Io, node_index: usize) !void {
    const s = &state;
    var length: usize = 0;
    for (s.effects.writesSlice()) |write| {
        encode(write, s.encode_buffer[length .. length + record_size]);
        length += record_size;
    }
    if (length == 0) return;
    try s.files[node_index].writePositionalAll(
        io,
        s.encode_buffer[0..length],
        s.offsets[node_index],
    );
    s.offsets[node_index] += length;
}

/// Fixed 33-byte little-endian record: tag, ballot, slot, value.
fn encode(write: P.Write, out: []u8) void {
    var ballot = paxos.Ballot.zero;
    var slot: paxos.Slot = 0;
    var value: u64 = 0;
    var tag: u8 = 0;
    switch (write) {
        .promise => |b| {
            tag = 1;
            ballot = b;
        },
        .accept => |record| {
            tag = 2;
            ballot = record.ballot;
            slot = record.slot;
            value = record.value;
        },
        .commit => |record| {
            tag = 3;
            slot = record.slot;
            value = record.value;
        },
    }
    out[0] = tag;
    std.mem.writeInt(u64, out[1..9], ballot.round, .little);
    std.mem.writeInt(u32, out[9..13], ballot.priority, .little);
    std.mem.writeInt(u32, out[13..17], ballot.node, .little);
    std.mem.writeInt(u32, out[17..21], slot, .little);
    std.mem.writeInt(u64, out[21..29], value, .little);
    std.mem.writeInt(u32, out[29..33], 0, .little);
}

fn enqueue(queue: []P.Envelope, count: *usize, message: P.Envelope) void {
    std.debug.assert(count.* < queue.len);
    queue[count.*] = message;
    count.* += 1;
}

/// Delivers queued messages; each receiving node persists before its
/// replies move. Group mode syncs once per node per delivery wave.
fn drainGrouped(io: std.Io, window: u32) !void {
    const s = &state;
    while (s.queue_count > 0) {
        var wave: [4_096]P.Envelope = undefined;
        const wave_count = s.queue_count;
        @memcpy(wave[0..wave_count], s.queue[0..wave_count]);
        s.queue_count = 0;
        var touched = [_]bool{false} ** node_count;
        for (wave[0..wave_count]) |envelope| {
            const node_index: usize = envelope.to - 1;
            try s.nodes[node_index].step(envelope, &s.effects);
            if (window == 1) {
                try settle(io, node_index);
            } else {
                try buffer(io, node_index);
                touched[node_index] = true;
            }
        }
        for (touched, 0..) |was_touched, node_index| {
            if (was_touched) try flushNode(io, node_index);
        }
    }
}

fn drain(io: std.Io) !void {
    try drainGrouped(io, 1);
}
