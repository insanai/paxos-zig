//! Compile-fail fixture: metadata bytes beyond the u16 encoded count.
const paxos = @import("paxos");

comptime {
    _ = paxos.ReplicatedLog(u64, .{ .max_metadata_bytes = 65536 });
}
