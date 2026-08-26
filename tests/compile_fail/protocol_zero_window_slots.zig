//! Compile-fail fixture: zero window_slots must be rejected.
const paxos = @import("paxos");

comptime {
    _ = paxos.Protocol(u64, .{ .window_slots = 0 });
}
