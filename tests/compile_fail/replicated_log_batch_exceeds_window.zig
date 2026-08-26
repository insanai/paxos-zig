//! Compile-fail fixture: max_batch above window_slots must be rejected.
const paxos = @import("paxos");

comptime {
    _ = paxos.ReplicatedLog(u64, .{ .max_batch = 65, .window_slots = 64 });
}
