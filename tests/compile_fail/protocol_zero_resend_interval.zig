//! Compile-fail fixture: zero resend interval must be rejected.
const paxos = @import("paxos");

comptime {
    _ = paxos.Protocol(u64, .{ .resend_interval_ticks = 0 });
}
