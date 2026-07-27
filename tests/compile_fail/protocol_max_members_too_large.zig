//! Compile-fail fixture: max_members beyond the u16 wire bound.
const paxos = @import("paxos");

comptime {
    _ = paxos.Protocol(u64, .{ .max_members = 65536 });
}
