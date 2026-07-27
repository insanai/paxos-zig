//! Positive fixture: the documented host sequence must run cleanly in every
//! optimize mode. Any diagnostic or abort here means the enforced check
//! misfires on a correct host.

const std = @import("std");
const paxos = @import("paxos");

const P = paxos.Protocol(u64, .{ .max_members = 3, .max_slots = 16 });

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

    // A real host would append and sync writesSlice here.
    std.mem.doNotOptimizeAway(effects.writesSlice().len);
    effects.confirmWritesDurable();
    std.mem.doNotOptimizeAway(effects.messagesSlice().len);
    effects.reset();
}
