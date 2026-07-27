//! Compile-fail fixture: zero election timeout must be rejected.
const paxos = @import("paxos");

comptime {
    _ = paxos.Protocol(u64, .{ .election_timeout_ticks = 0 });
}
