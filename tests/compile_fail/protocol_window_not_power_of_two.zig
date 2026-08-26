//! Compile-fail fixture: the consensus window must be a power of two.
const paxos = @import("paxos");

comptime {
    _ = paxos.Protocol(u64, .{ .window_slots = 3 });
}
