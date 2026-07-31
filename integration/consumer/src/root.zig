const std = @import("std");
const paxos = @import("paxos");

test "package exports the paxos module" {
    try std.testing.expectEqualStrings("0.2.2", paxos.version);
    const P = paxos.Protocol(u64, .{ .max_members = 3, .max_slots = 8 });
    var membership: P.Membership = undefined;
    try membership.init(&.{ 1, 2, 3 });
    var node: P.Node = undefined;
    try node.init(1, &membership);
    try std.testing.expectEqual(@as(paxos.NodeId, 1), node.id);
}
