//! Compile-fail fixture: zero max_batch must be rejected.
const paxos = @import("paxos");

comptime {
    _ = paxos.ReplicatedLog(u64, .{ .max_batch = 0 });
}
