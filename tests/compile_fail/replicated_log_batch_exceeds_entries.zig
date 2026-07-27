//! Compile-fail fixture: max_batch above max_entries must be rejected.
const paxos = @import("paxos");

comptime {
    _ = paxos.ReplicatedLog(u64, .{ .max_batch = 65, .max_entries = 64 });
}
