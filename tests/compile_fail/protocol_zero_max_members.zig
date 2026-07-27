//! Compile-fail fixture: zero max_members must be rejected.
const paxos = @import("paxos");

comptime {
    _ = paxos.Protocol(u64, .{ .max_members = 0 });
}
