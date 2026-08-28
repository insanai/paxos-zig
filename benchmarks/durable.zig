//! Durable-path benchmark: the first measurement of the library's actual
//! safety contract. Each node journals its `Effects.writes` to a per-node
//! append-only file and syncs it to disk before any message moves, exactly
//! as a production host must. Two host strategies are measured:
//!
//! - fsync-each: one value proposed, every transition synced individually.
//! - group8: eight values pipelined; each node buffers the group's writes
//!   and messages, syncs once, then releases the messages.
//!
//! Both strategies copy writes and messages into host-owned batches and run
//! the barrier outside `Effects`, so this host declares its protocol through
//! `paxos.host_managed.Protocol` and owns the effect-order rule itself: the
//! pending message queue stays private until the shared sync completes, and
//! `verifyJournals` proves after every run that the written prefix rebuilds
//! the live durable state.
//!
//! Numbers depend on the filesystem and disk; they are orders of magnitude
//! above the in-memory results, which is the honest cost of durability.
//!
//! Besides the whole-run totals, the median run records the wall time of
//! every durable operation (one proposed-and-committed group, including
//! its fsyncs) and emits the additive `samples_ns_per_value` and
//! `batch_ns_series` result fields so tools/bench-gate.zig can enforce
//! durable rows statistically. Clock reads cost tens of nanoseconds
//! against multi-millisecond fsyncs, so the headline aggregates stay
//! computed from the whole-run start-to-finish time exactly as before.

const std = @import("std");
const paxos = @import("paxos");

const node_count = 3;
const value_count = 512;
const sample_count = 3;
const record_size = 37;

/// Cap on raw samples recorded per run, matching the in-memory
/// benchmark's stride and the gate's minimum enforced sample count.
const max_recorded_samples = 64;

const P = paxos.host_managed.Protocol(u64, .{
    .max_members = node_count,
    .window_slots = value_count,
});

const bench_dir = ".zig-cache/durable-bench";

const State = struct {
    membership: P.Membership,
    nodes: [node_count]P.Node,
    effects: P.Effects,
    files: [node_count]std.Io.File,
    offsets: [node_count]u64,
    dirty: [node_count]bool,
    fsyncs: u64,
    queue: [4_096]P.Envelope,
    queue_count: usize,
    pending: [node_count][1_024]P.Envelope,
    pending_count: [node_count]usize,
    encode_buffer: [record_size * (2 * value_count + 1)]u8,
    /// Wall time of each durable group per sample run, in issue order.
    batch_ns: [sample_count][value_count]u64,
    /// Holds the full per-group series as JSON; sized for the widest
    /// possible u64 entry plus its separator.
    series_buffer: [21 * value_count + 2]u8,
};

var state: State = undefined;

const Sample = struct {
    nanoseconds: u64,
    fsyncs: u64,
    checksum: u64,
    /// Index into `state.batch_ns`, so the sort below keeps each total
    /// attached to its per-group timing series.
    index: usize,
};

/// Formats at most `max_recorded_samples` evenly-strided entries of the
/// time-ordered `ns` array, each divided by `per`, as a JSON integer
/// array. Feeds the additive `samples_ns_per_value` results field; older
/// result files simply lack it and keep parsing everywhere.
fn jsonSamples(out_buffer: []u8, ns: []const u64, per: u64) ![]const u8 {
    var writer = std.Io.Writer.fixed(out_buffer);
    const stride = (ns.len + max_recorded_samples - 1) / max_recorded_samples;
    try writer.writeByte('[');
    var index: usize = 0;
    while (index < ns.len) : (index += stride) {
        if (index != 0) try writer.writeByte(',');
        try writer.print("{d}", .{ns[index] / per});
    }
    try writer.writeByte(']');
    return writer.buffered();
}

/// Formats the whole time-ordered `ns` array, each entry divided by
/// `per`, as a JSON integer array. Feeds the additive `batch_ns_series`
/// results field that the periodicity check in tools/bench-gate.zig
/// reads; older result files simply lack it and keep parsing everywhere.
fn jsonSeries(out_buffer: []u8, ns: []const u64, per: u64) ![]const u8 {
    var writer = std.Io.Writer.fixed(out_buffer);
    try writer.writeByte('[');
    for (ns, 0..) |nanoseconds, index| {
        if (index != 0) try writer.writeByte(',');
        try writer.print("{d}", .{nanoseconds / per});
    }
    try writer.writeByte(']');
    return writer.buffered();
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const cwd = std.Io.Dir.cwd();
    cwd.createDirPath(io, bench_dir) catch {};

    try runMode(io, "fsync-each", 1);
    try runMode(io, "group8", 8);
}

fn runMode(io: std.Io, name: []const u8, window: u32) !void {
    // Uniform groups keep the per-value normalization of the timing
    // arrays exact; both shipped modes divide value_count evenly.
    std.debug.assert(value_count % window == 0);
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
        \\workload:       durable-u64-3n mode={s} (Zig Multi-Paxos {s})
        \\values:         {d} nodes={d} journal=file-per-node fsync-before-send
        \\median_ns:      {d}
        \\range_ns:       {d}..{d} across {d} samples
        \\ns_per_value:   {d:.2}
        \\fsyncs:         {d}
        \\checksum:       {d}
        \\
    , .{
        name,                                  paxos.version,
        value_count,                           node_count,
        median.nanoseconds,                    samples[0].nanoseconds,
        samples[sample_count - 1].nanoseconds, sample_count,
        per_value,                             median.fsyncs,
        median.checksum,
    });
    // The median run's per-group timings, normalized per value. Serialized
    // in issue order so the series keeps its time structure.
    const groups = value_count / window;
    const timed = state.batch_ns[median.index][0..groups];
    var sample_buffer: [2048]u8 = undefined;
    const samples_json = try jsonSamples(&sample_buffer, timed, window);
    const series_json = try jsonSeries(&state.series_buffer, timed, window);
    std.debug.print(
        "{{\"impl\":\"paxos-zig\",\"workload\":\"durable-u64-3n\"," ++
            "\"mode\":\"{s}\",\"values\":{d},\"nodes\":{d}," ++
            "\"payload_bytes\":8,\"ns_total_median\":{d}," ++
            "\"ns_total_min\":{d},\"ns_total_max\":{d}," ++
            "\"ns_per_value\":{d:.2},\"fsyncs\":{d},\"checksum\":{d}," ++
            "\"samples_ns_per_value\":{s},\"batch_ns_series\":{s}}}\n\n",
        .{
            name,                   value_count,
            node_count,             median.nanoseconds,
            samples[0].nanoseconds, samples[sample_count - 1].nanoseconds,
            per_value,              median.fsyncs,
            median.checksum,        samples_json,
            series_json,
        },
    );
    const expected = value_count * (value_count + 1) / 2;
    const expected_fsyncs = groups * 6;
    if (median.checksum != expected or median.fsyncs != expected_fsyncs) {
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
    // Election setup is outside the timed workload and therefore outside its
    // fsync accounting as well.
    s.fsyncs = 0;

    // Per-group clock reads cost tens of nanoseconds against fsyncs
    // costing milliseconds; the aggregate below still spans the whole
    // run start to finish and is never derived from the group timings.
    const started = std.Io.Clock.Timestamp.now(io, .awake);
    var previous = started;
    var seq: u64 = 1;
    var batch_index: usize = 0;
    while (seq <= value_count) {
        const batch: u64 = @min(window, value_count - seq + 1);
        for (0..batch) |offset| {
            _ = try s.nodes[0].propose(seq + offset, &s.effects);
            try buffer(io, 0);
        }
        try flushNode(io, 0);
        try drainGrouped(io, window);
        seq += batch;
        const now = std.Io.Clock.Timestamp.now(io, .awake);
        s.batch_ns[sample_index][batch_index] =
            @intCast(previous.durationTo(now).raw.nanoseconds);
        previous = now;
        batch_index += 1;
    }
    const finished = std.Io.Clock.Timestamp.now(io, .awake);

    var checksum: u64 = 0;
    for (1..value_count + 1) |slot| {
        checksum +%= s.nodes[0].committedAt(@intCast(slot)).?;
    }
    for (1..node_count) |node_index| {
        var replica_checksum: u64 = 0;
        for (1..value_count + 1) |slot| {
            replica_checksum +%= s.nodes[node_index].committedAt(@intCast(slot)) orelse
                return error.IncompleteReplica;
        }
        if (replica_checksum != checksum) return error.ReplicaDivergence;
    }
    closeJournals(io);
    try verifyJournals(io, sample_index);
    return .{
        .nanoseconds = @intCast(started.durationTo(finished).raw.nanoseconds),
        .fsyncs = s.fsyncs,
        .checksum = checksum,
        .index = sample_index,
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
        s.dirty[index] = false;
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
    if (s.dirty[node_index]) {
        try s.files[node_index].sync(io);
        s.fsyncs += 1;
        s.dirty[node_index] = false;
    }
    for (s.pending[node_index][0..s.pending_count[node_index]]) |message| {
        enqueue(&s.queue, &s.queue_count, message);
    }
    s.pending_count[node_index] = 0;
}

fn persist(io: std.Io, node_index: usize) !void {
    try append(io, node_index);
    if (!state.dirty[node_index]) return;
    try state.files[node_index].sync(io);
    state.fsyncs += 1;
    state.dirty[node_index] = false;
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
    s.dirty[node_index] = true;
}

/// Reopens every journal and proves that the measured bytes reconstruct the
/// same durable state as the live node. Recovery is outside the timed region.
fn verifyJournals(io: std.Io, sample_index: usize) !void {
    const s = &state;
    inline for (0..node_count) |index| {
        var name_buffer: [64]u8 = undefined;
        const path = try std.fmt.bufPrint(
            &name_buffer,
            bench_dir ++ "/node{d}-sample{d}.journal",
            .{ index + 1, sample_index },
        );
        var file = try std.Io.Dir.cwd().openFile(io, path, .{});
        defer file.close(io);

        var replayed = P.DurableState{};
        var offset: u64 = 0;
        var bytes: [record_size]u8 = undefined;
        while (offset < s.offsets[index]) : (offset += record_size) {
            const read = try file.readPositionalAll(io, &bytes, offset);
            if (read != record_size) return error.TruncatedJournalRecord;
            try replayed.apply(try decode(&bytes));
        }
        if (offset != s.offsets[index]) return error.MisalignedJournal;
        var trailing: [1]u8 = undefined;
        if (try file.readPositionalAll(io, &trailing, offset) != 0) {
            return error.TrailingJournalData;
        }
        if (!std.meta.eql(replayed, s.nodes[index].durable)) {
            return error.JournalReplayMismatch;
        }
    }
}

/// Fixed 37-byte little-endian record: tag, ballot, 64-bit slot, value.
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
        // The durable workload never installs a chosen trim; the fixed
        // 37-byte record is part of the recorded-results protocol and
        // must not grow for a write this harness cannot produce.
        .trim_anchor => std.debug.panic(
            "trim anchors are not part of the durable workload",
            .{},
        ),
    }
    out[0] = tag;
    std.mem.writeInt(u64, out[1..9], ballot.round, .little);
    std.mem.writeInt(u32, out[9..13], ballot.priority, .little);
    std.mem.writeInt(u32, out[13..17], ballot.node, .little);
    std.mem.writeInt(u64, out[17..25], slot, .little);
    std.mem.writeInt(u64, out[25..33], value, .little);
    std.mem.writeInt(u32, out[33..37], 0, .little);
}

fn decode(bytes: *const [record_size]u8) !P.Write {
    if (std.mem.readInt(u32, bytes[33..37], .little) != 0) {
        return error.InvalidJournalRecord;
    }
    const ballot = paxos.Ballot{
        .round = std.mem.readInt(u64, bytes[1..9], .little),
        .priority = std.mem.readInt(u32, bytes[9..13], .little),
        .node = std.mem.readInt(u32, bytes[13..17], .little),
    };
    const slot = std.mem.readInt(u64, bytes[17..25], .little);
    const value = std.mem.readInt(u64, bytes[25..33], .little);
    return switch (bytes[0]) {
        1 => .{ .promise = ballot },
        2 => .{ .accept = .{ .ballot = ballot, .slot = slot, .value = value } },
        3 => .{ .commit = .{ .slot = slot, .value = value } },
        else => error.InvalidJournalRecord,
    };
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
