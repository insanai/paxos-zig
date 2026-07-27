//! Compile-fail fixture: zero learner capacity must be rejected.
const paxos = @import("paxos");

comptime {
    _ = paxos.Learner(u64, .{ .max_entries = 0 });
}
