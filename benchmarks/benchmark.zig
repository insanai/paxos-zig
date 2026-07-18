const std = @import("std");
const paxos = @import("paxos");

const node_count = 3;
const value_count = 4_096;
const sample_count = 7;

const P = paxos.Protocol(u64, .{
    .max_members = node_count,
    .max_slots = value_count,
});

const Sample = struct {
    checksum: u64,
    messages: u64,
    nanoseconds: u64,
};

pub fn main(init: std.process.Init) !void {
    var samples: [sample_count]Sample = undefined;
    for (&samples) |*sample| sample.* = try runSample(init.io);
    sortSamples(&samples);

    const result = samples[sample_count / 2];
    std.debug.print(
        \\implementation: Multi-Paxos 0.1.0 (Zig)
        \\values:         {d}
        \\median_ns:      {d}
        \\ns_per_value:   {d:.2}
        \\messages:       {d}
        \\messages/value: {d:.2}
        \\checksum:       {d}
        \\
    , .{
        value_count,
        result.nanoseconds,
        @as(f64, @floatFromInt(result.nanoseconds)) / value_count,
        result.messages,
        @as(f64, @floatFromInt(result.messages)) / value_count,
        result.checksum,
    });
}

fn runSample(io: std.Io) !Sample {
    var membership: P.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var nodes: [node_count]P.Node = undefined;
    try nodes[0].init(1, &membership);
    try nodes[1].init(2, &membership);
    try nodes[2].init(3, &membership);
    var disks = [_]P.DurableState{.{}} ** node_count;
    var queue: [32_768]P.Envelope = undefined;
    var queue_count: usize = 0;
    var effects: P.Effects = undefined;
    effects.init();
    var message_count: u64 = 0;

    try nodes[0].campaign(0, &effects);
    try append(&queue, &queue_count, &message_count, &effects);
    try drain(&nodes, &disks, &queue, &queue_count, &message_count, &effects);
    std.debug.assert(nodes[0].role == .leader);
    message_count = 0;

    const started = std.Io.Clock.Timestamp.now(io, .awake);
    for (1..value_count + 1) |value| {
        _ = try nodes[0].propose(@intCast(value), &effects);
        for (effects.writesSlice()) |write| try disks[0].apply(write);
        try append(&queue, &queue_count, &message_count, &effects);
        try drain(&nodes, &disks, &queue, &queue_count, &message_count, &effects);
    }
    const elapsed = started.untilNow(io).raw.nanoseconds;
    std.debug.assert(elapsed > 0);

    var checksum: u64 = 0;
    for (1..value_count + 1) |slot| {
        checksum +%= nodes[0].committedAt(@intCast(slot)).?;
    }
    std.mem.doNotOptimizeAway(checksum);
    return .{
        .checksum = checksum,
        .messages = message_count,
        .nanoseconds = @intCast(elapsed),
    };
}

fn drain(
    nodes: *[node_count]P.Node,
    disks: *[node_count]P.DurableState,
    queue: *[32_768]P.Envelope,
    queue_count: *usize,
    message_count: *u64,
    effects: *P.Effects,
) !void {
    var queue_head: usize = 0;
    while (queue_head < queue_count.*) : (queue_head += 1) {
        const envelope = queue[queue_head];
        const node_index: usize = switch (envelope.to) {
            1 => 0,
            2 => 1,
            3 => 2,
            else => return error.UnknownNode,
        };
        try nodes[node_index].step(envelope, effects);
        for (effects.writesSlice()) |write| try disks[node_index].apply(write);
        try append(queue, queue_count, message_count, effects);
    }
    queue_count.* = 0;
}

fn append(
    queue: *[32_768]P.Envelope,
    queue_count: *usize,
    message_count: *u64,
    effects: *const P.Effects,
) !void {
    for (effects.messagesSlice()) |message| {
        if (queue_count.* == queue.len) return error.QueueFull;
        queue[queue_count.*] = message;
        queue_count.* += 1;
        message_count.* += 1;
    }
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
