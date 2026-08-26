//! Misuse fixture: reads protected messages without confirming durability.
//! The enforced protocol must stop this process in every optimize mode with
//! the stable diagnostic on stderr. Reaching the end of main is a failure.

const std = @import("std");
const paxos = @import("paxos");

const P = paxos.Protocol(u64, .{ .max_members = 3, .window_slots = 16 });

pub fn main() !void {
    var membership: P.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var node: P.Node = undefined;
    try node.init(2, &membership);
    var effects = P.Effects{};
    try node.step(.{
        .from = 1,
        .to = 2,
        .message = .{ .prepare = .{
            .ballot = .{ .round = 1, .node = 1 },
            .decided_through = 0,
        } },
    }, &effects);
    if (effects.writes_count == 0) return error.FixtureProducedNoWrites;

    // Invalid: the write batch was never confirmed durable.
    const messages = effects.messagesSlice();
    std.mem.doNotOptimizeAway(messages.len);
}
