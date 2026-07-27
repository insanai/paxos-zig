//! Compile-fail fixture: zero heartbeat interval must be rejected.
const paxos = @import("paxos");

comptime {
    _ = paxos.Protocol(u64, .{ .heartbeat_interval_ticks = 0 });
}
