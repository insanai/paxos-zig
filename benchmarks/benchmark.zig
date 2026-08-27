//! Workload-matrix benchmark for the Zig Multi-Paxos core.
//!
//! Every run is an in-memory, single-process workload fixture: three or five
//! voters, no serialization, no network, no fsync. Numbers are regression
//! signals for this library, never service latency and never a language
//! comparison. The matrix varies commit mode (one value synchronously,
//! pipelined windows, batches), payload size, cluster size, and log slack so
//! the shape of the harness is visible in the results instead of baked in.
//!
//! Each run prints one human block and one machine-readable JSON line, and
//! self-checks its checksum and logical message count, exiting nonzero on
//! any mismatch so CI can use it as a correctness smoke test.

const std = @import("std");
const paxos = @import("paxos");

const sample_count = 7;

/// Cap on raw samples recorded per run: enough for the statistical gate
/// to work on a real sample vector while keeping result rows compact.
const max_recorded_samples = 64;

/// Formats at most `max_recorded_samples` evenly-strided entries of the
/// time-ordered `ns` array, each divided by `per`, as a JSON integer
/// array. Feeds the additive `samples_ns_per_value` results field; older
/// result files simply lack it and keep parsing everywhere.
fn jsonSamples(buffer: []u8, ns: []const u64, per: u64) ![]const u8 {
    var writer = std.Io.Writer.fixed(buffer);
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
fn jsonSeries(buffer: []u8, ns: []const u64, per: u64) ![]const u8 {
    var writer = std.Io.Writer.fixed(buffer);
    try writer.writeByte('[');
    for (ns, 0..) |nanoseconds, index| {
        if (index != 0) try writer.writeByte(',');
        try writer.print("{d}", .{nanoseconds / per});
    }
    try writer.writeByte(']');
    return writer.buffered();
}

const Mode = struct {
    name: []const u8,
    /// Proposals issued before draining the network once.
    window: u32,
    /// True to issue the window through `proposeBatch`.
    batched: bool = false,
};

const sync_mode = Mode{ .name = "sync", .window = 1 };

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    var failures: u32 = 0;

    const U64x3 = Bench(u64, 3, 4_096, 4_096, 32);
    failures += try U64x3.runAll(io, "u64-3n", &.{
        sync_mode,
        .{ .name = "pipeline8", .window = 8 },
        .{ .name = "pipeline64", .window = 64 },
        .{ .name = "batch16", .window = 16, .batched = true },
        .{ .name = "batch256", .window = 256, .batched = true },
    });

    // Twice the slots the workload needs: quantifies how much of the result
    // depends on arrays sized exactly to the value count.
    const U64Slack = Bench(u64, 3, 4_096, 8_192, 32);
    failures += try U64Slack.runAll(io, "u64-3n-slack", &.{sync_mode});

    const Blob64x3 = Bench(Blob(64), 3, 1_024, 1_024, 64);
    failures += try Blob64x3.runAll(io, "blob64-3n", &.{
        sync_mode,
        .{ .name = "pipeline8", .window = 8 },
    });

    const Blob1kx3 = Bench(Blob(1_024), 3, 1_024, 1_024, 16);
    failures += try Blob1kx3.runAll(io, "blob1k-3n", &.{
        sync_mode,
        .{ .name = "pipeline8", .window = 8 },
    });

    const U64x5 = Bench(u64, 5, 4_096, 4_096, 16);
    failures += try U64x5.runAll(io, "u64-5n", &.{
        sync_mode,
        .{ .name = "pipeline8", .window = 8 },
    });

    // The moving-window workload never resets: 256 window wraps on one
    // global slot line, floors advancing as the host consumes (ZDS 0011).
    // Latency percentiles across batches are the direct no-cliff check —
    // window reuse must cost the same at wrap 256 as at wrap 1.
    const Moving = MovingBench(3, 1_024, 64, 262_144);
    failures += try Moving.run(io, "u64-3n-moving");

    if (failures > 0) {
        std.debug.print("benchmark self-checks failed: {d}\n", .{failures});
        std.process.exit(1);
    }
}

/// A wrapping workload over one global slot line: batches of `batch`
/// values, floors advanced after every drain, `total` values across
/// `total / window_slots` window wraps. Checksums accumulate at release
/// time on every replica, because the window no longer retains history.
fn MovingBench(
    comptime node_count: comptime_int,
    comptime window_slots: usize,
    comptime batch: u64,
    comptime total: u64,
) type {
    const P = paxos.Protocol(u64, .{
        .max_members = node_count,
        .window_slots = window_slots,
        .recovery_chunk_slots = 256,
    });
    const queue_capacity = 8_192;
    const batch_count = total / batch;
    const moving_samples = 3;

    return struct {
        const State = struct {
            membership: P.Membership,
            nodes: [node_count]P.Node,
            disks: [node_count]P.DurableState,
            effects: P.Effects,
            queue: [queue_capacity]P.Envelope,
            queue_count: usize,
            message_count: u64,
            released: [node_count]u64,
            delivered: [node_count]u64,
            batch_ns: [batch_count]u64,
            /// Holds the full per-batch series as JSON; sized for the
            /// widest possible u64 entry plus its separator.
            series_buffer: [21 * batch_count + 2]u8,
        };

        var state: State = undefined;

        fn run(io: std.Io, workload: []const u8) !u32 {
            var totals: [moving_samples]u64 = undefined;
            for (&totals) |*nanoseconds| nanoseconds.* = try runSample(io);
            std.mem.sort(u64, &totals, {}, std.sort.asc(u64));
            const median = totals[moving_samples / 2];

            const s = &state;
            // Serialize the full time-ordered series and its strided
            // sample subset before the sort below turns the per-batch
            // measurements into order statistics.
            var sample_buffer: [2048]u8 = undefined;
            const samples_json = try jsonSamples(&sample_buffer, &s.batch_ns, batch);
            const series_json = try jsonSeries(&s.series_buffer, &s.batch_ns, batch);
            std.mem.sort(u64, &s.batch_ns, {}, std.sort.asc(u64));
            const per_batch = [4]u64{
                s.batch_ns[batch_count / 2],
                s.batch_ns[batch_count * 9 / 10],
                s.batch_ns[batch_count * 99 / 100],
                s.batch_ns[batch_count - 1],
            };
            const ns_per_value =
                @as(f64, @floatFromInt(median)) / @as(f64, @floatFromInt(total));
            std.debug.print(
                \\
                \\workload:       {s} mode=batch{d} (Zig Multi-Paxos {s})
                \\values:         {d} nodes={d} window_slots={d} wraps={d}
                \\median_ns:      {d} across {d} samples
                \\ns_per_value:   {d:.2}
                \\batch-average ns/value p50/p90/p99/max: {d}/{d}/{d}/{d}
                \\
            , .{
                workload,             batch,
                paxos.version,        total,
                node_count,           window_slots,
                total / window_slots, median,
                moving_samples,       ns_per_value,
                per_batch[0] / batch, per_batch[1] / batch,
                per_batch[2] / batch, per_batch[3] / batch,
            });
            std.debug.print(
                "{{\"impl\":\"paxos-zig\",\"workload\":\"{s}\"," ++
                    "\"mode\":\"batch{d}\",\"values\":{d},\"nodes\":{d}," ++
                    "\"payload_bytes\":8,\"window_slots\":{d},\"wraps\":{d}," ++
                    "\"ns_total_median\":{d},\"ns_per_value\":{d:.2}," ++
                    "\"batch_ns_per_value_p50\":{d},\"batch_ns_per_value_p90\":{d}," ++
                    "\"batch_ns_per_value_p99\":{d},\"batch_ns_per_value_max\":{d}," ++
                    "\"samples_ns_per_value\":{s},\"batch_ns_series\":{s}}}\n",
                .{
                    workload,             batch,
                    total,                node_count,
                    window_slots,         total / window_slots,
                    median,               ns_per_value,
                    per_batch[0] / batch, per_batch[1] / batch,
                    per_batch[2] / batch, per_batch[3] / batch,
                    samples_json,         series_json,
                },
            );
            return 0;
        }

        fn runSample(io: std.Io) !u64 {
            const s = &state;
            try s.membership.init(memberIdsOf(node_count));
            inline for (0..node_count) |index| {
                try s.nodes[index].init(@intCast(index + 1), &s.membership);
            }
            s.disks = [_]P.DurableState{.{}} ** node_count;
            s.effects.init();
            s.queue_count = 0;
            s.message_count = 0;
            s.released = [_]u64{0} ** node_count;
            s.delivered = [_]u64{0} ** node_count;

            try s.nodes[0].campaign(0, &s.effects);
            try consume(0);
            try drain();
            std.debug.assert(s.nodes[0].role == .leader);

            const started = std.Io.Clock.Timestamp.now(io, .awake);
            var previous = started;
            var seq: u64 = 1;
            var batch_index: usize = 0;
            while (seq <= total) : (seq += batch) {
                var values: [batch]u64 = undefined;
                var slots: [batch]paxos.Slot = undefined;
                for (0..batch) |offset| values[offset] = seq + offset;
                _ = try s.nodes[0].proposeBatch(&values, &slots, &s.effects);
                try consume(0);
                try drain();
                // The host has applied and journaled everything released:
                // license cell reuse so the window keeps moving.
                inline for (0..node_count) |index| {
                    try s.nodes[index].advanceMemoryFloor(
                        s.nodes[index].decidedThrough(),
                    );
                }
                const now = std.Io.Clock.Timestamp.now(io, .awake);
                s.batch_ns[batch_index] =
                    @intCast(previous.durationTo(now).raw.nanoseconds);
                previous = now;
                batch_index += 1;
            }
            const finished = std.Io.Clock.Timestamp.now(io, .awake);

            const expected = total * (total + 1) / 2;
            inline for (0..node_count) |index| {
                if (s.released[index] != expected or
                    s.delivered[index] != total)
                {
                    return error.ReplicaDivergence;
                }
            }
            return @intCast(started.durationTo(finished).raw.nanoseconds);
        }

        fn drain() !void {
            const s = &state;
            var head: usize = 0;
            while (head < s.queue_count) : (head += 1) {
                const envelope = s.queue[head];
                const node_index: usize = envelope.to - 1;
                try s.nodes[node_index].step(envelope, &s.effects);
                try consume(node_index);
            }
            s.queue_count = 0;
        }

        fn consume(node_index: usize) !void {
            const s = &state;
            for (s.effects.writesSlice()) |write| {
                try s.disks[node_index].apply(write);
            }
            s.effects.confirmWritesDurable();
            for (s.effects.committedSlice()) |committed| {
                if (committed.slot != 0) {
                    s.released[node_index] +%= committed.value;
                    s.delivered[node_index] += 1;
                }
            }
            for (s.effects.messagesSlice()) |message| {
                if (s.queue_count == s.queue.len) return error.QueueFull;
                s.queue[s.queue_count] = message;
                s.queue_count += 1;
                s.message_count += 1;
            }
        }
    };
}

fn memberIdsOf(comptime node_count: comptime_int) []const paxos.NodeId {
    const ids = comptime blk: {
        var storage: [node_count]paxos.NodeId = undefined;
        for (&storage, 1..) |*id, id_value| id.* = id_value;
        break :blk storage;
    };
    return &ids;
}

/// Fixed-size payload carrying a sequence number for checksumming.
fn Blob(comptime size: usize) type {
    return struct {
        seq: u64,
        pad: [size - 8]u8 = [_]u8{0} ** (size - 8),
    };
}

fn sequenceOf(value: anytype) u64 {
    return if (@TypeOf(value) == u64) value else value.seq;
}

fn valueFrom(comptime ValueT: type, seq: u64) ValueT {
    return if (ValueT == u64) seq else .{ .seq = seq };
}

fn Bench(
    comptime ValueT: type,
    comptime node_count: comptime_int,
    comptime value_count: u64,
    comptime window_slots: usize,
    comptime measurement_iterations: u64,
) type {
    const P = paxos.Protocol(ValueT, .{
        .max_members = node_count,
        .window_slots = window_slots,
        .recovery_chunk_slots = 256,
    });
    const queue_capacity = 8_192;
    // Leader-to-peer accept, accepted reply, and commit per value.
    const expected_messages_per_value: u64 = 3 * (node_count - 1);

    return struct {
        const State = struct {
            membership: P.Membership,
            nodes: [node_count]P.Node,
            disks: [node_count]P.DurableState,
            effects: P.Effects,
            queue: [queue_capacity]P.Envelope,
            queue_count: usize,
            message_count: u64,
            window_ns: [value_count]u64,
            window_count: usize,
        };

        var state: State = undefined;

        const Sample = struct {
            checksum: u64,
            messages: u64,
            nanoseconds: u64,
        };

        fn runAll(io: std.Io, workload: []const u8, modes: []const Mode) !u32 {
            var failures: u32 = 0;
            for (modes) |mode| {
                failures += try runMode(io, workload, mode);
            }
            return failures;
        }

        fn runMode(io: std.Io, workload: []const u8, mode: Mode) !u32 {
            // Total-time samples run without per-window clock reads; one
            // extra instrumented pass collects window-average timing so
            // timer overhead never contaminates the aggregate samples.
            var samples: [sample_count]Sample = undefined;
            for (&samples) |*sample| sample.* = try runMeasurement(io, mode);
            sortSamples(&samples);
            const median = samples[sample_count / 2];

            _ = try runSample(io, mode, true);
            // Stride the time-ordered window measurements before the
            // percentile pass sorts them into order statistics.
            var sample_buffer: [2048]u8 = undefined;
            const samples_json = try jsonSamples(
                &sample_buffer,
                state.window_ns[0..state.window_count],
                1,
            );
            var window_ns_per_value: [4]u64 = .{ 0, 0, 0, 0 };
            computePercentiles(&window_ns_per_value);
            report(workload, mode, &samples, median, window_ns_per_value, samples_json);

            const expected_checksum = value_count * (value_count + 1) / 2 *
                measurement_iterations;
            const expected_messages = expected_messages_per_value * value_count *
                measurement_iterations;
            if (median.checksum != expected_checksum or
                median.messages != expected_messages)
            {
                std.debug.print("SELF-CHECK FAILED for {s}/{s}\n", .{
                    workload, mode.name,
                });
                return 1;
            }
            return 0;
        }

        /// Aggregates enough independently initialized stable-leader runs to
        /// keep sub-millisecond fixtures from becoming clock-noise contests.
        /// Initialization and validation remain outside each timed interval.
        fn runMeasurement(io: std.Io, mode: Mode) !Sample {
            var result = Sample{ .checksum = 0, .messages = 0, .nanoseconds = 0 };
            for (0..measurement_iterations) |_| {
                const sample = try runSample(io, mode, false);
                result.checksum +%= sample.checksum;
                result.messages += sample.messages;
                result.nanoseconds += sample.nanoseconds;
            }
            return result;
        }

        fn runSample(io: std.Io, mode: Mode, instrumented: bool) !Sample {
            const s = &state;
            try s.membership.init(memberIds());
            inline for (0..node_count) |index| {
                try s.nodes[index].init(@intCast(index + 1), &s.membership);
            }
            s.disks = [_]P.DurableState{.{}} ** node_count;
            s.effects.init();
            s.queue_count = 0;
            s.message_count = 0;
            s.window_count = 0;

            try s.nodes[0].campaign(valueFrom(ValueT, 0), &s.effects);
            try consume(0);
            try drain();
            std.debug.assert(s.nodes[0].role == .leader);
            s.message_count = 0;

            const started = std.Io.Clock.Timestamp.now(io, .awake);
            var previous = started;
            var seq: u64 = 1;
            while (seq <= value_count) {
                const window: u64 = @min(mode.window, value_count - seq + 1);
                try issueWindow(mode, seq, window);
                try drain();
                if (instrumented) {
                    const now = std.Io.Clock.Timestamp.now(io, .awake);
                    const nanoseconds = previous.durationTo(now).raw.nanoseconds;
                    s.window_ns[s.window_count] = @as(u64, @intCast(nanoseconds)) / window;
                    s.window_count += 1;
                    previous = now;
                }
                seq += window;
            }
            const finished = std.Io.Clock.Timestamp.now(io, .awake);
            const total = started.durationTo(finished).raw.nanoseconds;

            var checksum: u64 = 0;
            for (1..value_count + 1) |slot| {
                checksum +%= sequenceOf(s.nodes[0].committedAt(@intCast(slot)).?);
            }
            for (0..node_count) |node_index| {
                var replica_checksum: u64 = 0;
                for (1..value_count + 1) |slot| {
                    const value = s.nodes[node_index].committedAt(@intCast(slot)) orelse
                        return error.IncompleteReplica;
                    replica_checksum +%= sequenceOf(value);
                }
                if (replica_checksum != checksum) return error.ReplicaDivergence;
                if (!std.meta.eql(s.nodes[node_index].durable, s.disks[node_index])) {
                    return error.DurableMirrorMismatch;
                }
            }
            std.mem.doNotOptimizeAway(checksum);
            return .{
                .checksum = checksum,
                .messages = s.message_count,
                .nanoseconds = @intCast(total),
            };
        }

        fn issueWindow(mode: Mode, first_seq: u64, window: u64) !void {
            const s = &state;
            if (mode.batched) {
                var values: [256]ValueT = undefined;
                var slots: [256]paxos.Slot = undefined;
                for (0..window) |offset| {
                    values[offset] = valueFrom(ValueT, first_seq + offset);
                }
                _ = try s.nodes[0].proposeBatch(
                    values[0..window],
                    slots[0..window],
                    &s.effects,
                );
                try consume(0);
            } else {
                for (0..window) |offset| {
                    _ = try s.nodes[0].propose(
                        valueFrom(ValueT, first_seq + offset),
                        &s.effects,
                    );
                    try consume(0);
                }
            }
        }

        fn drain() !void {
            const s = &state;
            var head: usize = 0;
            while (head < s.queue_count) : (head += 1) {
                const envelope = s.queue[head];
                const node_index: usize = envelope.to - 1;
                try s.nodes[node_index].step(envelope, &s.effects);
                try consume(node_index);
            }
            s.queue_count = 0;
        }

        /// The host loop: persist writes, confirm durability, then enqueue
        /// messages. The in-memory "disk" keeps the ordering observable.
        fn consume(node_index: usize) !void {
            const s = &state;
            for (s.effects.writesSlice()) |write| {
                try s.disks[node_index].apply(write);
            }
            s.effects.confirmWritesDurable();
            for (s.effects.messagesSlice()) |message| {
                if (s.queue_count == s.queue.len) return error.QueueFull;
                s.queue[s.queue_count] = message;
                s.queue_count += 1;
                s.message_count += 1;
            }
        }

        fn computePercentiles(out: *[4]u64) void {
            const s = &state;
            const windows = s.window_ns[0..s.window_count];
            std.mem.sort(u64, windows, {}, std.sort.asc(u64));
            out[0] = windows[windows.len / 2];
            out[1] = windows[windows.len * 9 / 10];
            out[2] = windows[windows.len * 99 / 100];
            out[3] = windows[windows.len - 1];
        }

        fn report(
            workload: []const u8,
            mode: Mode,
            samples: *const [sample_count]Sample,
            median: Sample,
            window_ns_per_value: [4]u64,
            samples_json: []const u8,
        ) void {
            const measured_values = value_count * measurement_iterations;
            const per_value = @as(f64, @floatFromInt(median.nanoseconds)) /
                @as(f64, @floatFromInt(measured_values));
            std.debug.print(
                \\workload:       {s} mode={s} (Zig Multi-Paxos {s})
                \\values:         {d} ({d} x {d} iterations) nodes={d} payload={d}B window_slots={d}
                \\median_ns:      {d} (min {d}, max {d} across {d} samples)
                \\ns_per_value:   {d:.2}
                \\window-average ns/value p50/p90/p99/max: {d}/{d}/{d}/{d}
                \\messages:       {d}
                \\messages/value: {d:.2}
                \\checksum:       {d}
                \\
            , .{
                workload,               mode.name,
                paxos.version,          measured_values,
                value_count,            measurement_iterations,
                node_count,             @sizeOf(ValueT),
                window_slots,           median.nanoseconds,
                samples[0].nanoseconds, samples[sample_count - 1].nanoseconds,
                sample_count,           per_value,
                window_ns_per_value[0], window_ns_per_value[1],
                window_ns_per_value[2], window_ns_per_value[3],
                median.messages,
                @as(f64, @floatFromInt(median.messages)) /
                    @as(f64, @floatFromInt(measured_values)),
                median.checksum,
            });
            std.debug.print(
                "{{\"impl\":\"paxos-zig\",\"workload\":\"{s}\",\"mode\":\"{s}\"," ++
                    "\"values\":{d},\"nodes\":{d},\"payload_bytes\":{d}," ++
                    "\"values_per_iteration\":{d},\"measurement_iterations\":{d}," ++
                    "\"max_slots\":{d},\"ns_total_median\":{d}," ++
                    "\"ns_total_min\":{d},\"ns_total_max\":{d}," ++
                    "\"ns_per_value\":{d:.2},\"window_ns_per_value_p50\":{d}," ++
                    "\"window_ns_per_value_p90\":{d}," ++
                    "\"window_ns_per_value_p99\":{d}," ++
                    "\"window_ns_per_value_max\":{d}," ++
                    "\"messages\":{d},\"checksum\":{d}," ++
                    "\"samples_ns_per_value\":{s}}}\n\n",
                .{
                    workload,                              mode.name,
                    measured_values,                       node_count,
                    @sizeOf(ValueT),                       value_count,
                    measurement_iterations,                window_slots,
                    median.nanoseconds,                    samples[0].nanoseconds,
                    samples[sample_count - 1].nanoseconds, per_value,
                    window_ns_per_value[0],                window_ns_per_value[1],
                    window_ns_per_value[2],                window_ns_per_value[3],
                    median.messages,                       median.checksum,
                    samples_json,
                },
            );
        }

        fn memberIds() []const paxos.NodeId {
            const ids = comptime blk: {
                var out: [node_count]paxos.NodeId = undefined;
                for (0..node_count) |index| out[index] = index + 1;
                break :blk out;
            };
            return &ids;
        }

        fn sortSamples(samples: *[sample_count]Sample) void {
            for (1..samples.len) |unsorted_index| {
                var index = unsorted_index;
                while (index > 0) : (index -= 1) {
                    if (samples[index - 1].nanoseconds <= samples[index].nanoseconds) break;
                    const previous = samples[index - 1];
                    samples[index - 1] = samples[index];
                    samples[index] = previous;
                }
            }
        }
    };
}
