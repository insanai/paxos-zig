//! Compile-fail fixture: zero max_slots must be rejected.
const paxos = @import("paxos");

comptime {
    _ = paxos.Protocol(u64, .{ .max_slots = 0 });
}
