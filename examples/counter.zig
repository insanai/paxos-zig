const std = @import("std");
const paxos = @import("paxos");

const Command = struct {
    client: u32,
    request: u32,
    operation: enum { noop, add },
    amount: i64,
};

const P = paxos.Protocol(Command, .{ .max_members = 3, .max_slots = 32 });

pub fn main(_: std.process.Init) !void {
    var membership: P.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var nodes: [3]P.Node = undefined;
    try nodes[0].init(1, &membership);
    try nodes[1].init(2, &membership);
    try nodes[2].init(3, &membership);
    var disks = [_]P.DurableState{.{}} ** 3;
    var counters = [_]i64{0} ** 3;
    var queue: [1024]P.Envelope = undefined;
    var queue_count: usize = 0;
    var effects: P.Effects = undefined;
    effects.init();

    const noop = Command{
        .client = 0,
        .request = 0,
        .operation = .noop,
        .amount = 0,
    };
    try nodes[0].campaign(noop, &effects);
    try consumeEffects(0, &disks, &counters, &queue, &queue_count, &effects);
    try drain(&nodes, &disks, &counters, &queue, &queue_count, &effects);

    for (1..4) |request| {
        const command = Command{
            .client = 7,
            .request = @intCast(request),
            .operation = .add,
            .amount = 10,
        };
        const slot = try nodes[0].propose(command, &effects);
        std.debug.print("proposed request {d} in slot {d}\n", .{ request, slot });
        try consumeEffects(0, &disks, &counters, &queue, &queue_count, &effects);
        try drain(&nodes, &disks, &counters, &queue, &queue_count, &effects);
    }

    std.debug.print("replicated counters: {d}, {d}, {d}\n", .{
        counters[0],
        counters[1],
        counters[2],
    });
}

fn drain(
    nodes: *[3]P.Node,
    disks: *[3]P.DurableState,
    counters: *[3]i64,
    queue: *[1024]P.Envelope,
    queue_count: *usize,
    effects: *P.Effects,
) !void {
    while (queue_count.* > 0) {
        const envelope = queue[0];
        std.mem.copyForwards(
            P.Envelope,
            queue[0 .. queue_count.* - 1],
            queue[1..queue_count.*],
        );
        queue_count.* -= 1;

        const index = nodeIndex(envelope.to) orelse return error.UnknownNode;
        try nodes[index].step(envelope, effects);
        try consumeEffects(index, disks, counters, queue, queue_count, effects);
    }
}

fn consumeEffects(
    node_index: usize,
    disks: *[3]P.DurableState,
    counters: *[3]i64,
    queue: *[1024]P.Envelope,
    queue_count: *usize,
    effects: *const P.Effects,
) !void {
    // The safety contract is visible here: durable writes happen before sends.
    for (effects.writesSlice()) |write| try disks[node_index].apply(write);

    for (effects.messagesSlice()) |message| {
        if (queue_count.* == queue.len) return error.QueueFull;
        queue[queue_count.*] = message;
        queue_count.* += 1;
    }

    for (effects.committedSlice()) |entry| {
        if (entry.value.operation == .add) counters[node_index] += entry.value.amount;
    }
}

fn nodeIndex(id: paxos.NodeId) ?usize {
    return switch (id) {
        1 => 0,
        2 => 1,
        3 => 2,
        else => null,
    };
}
