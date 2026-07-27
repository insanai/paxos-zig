//! Compile-fail fixture: max_slots beyond the u32 slot bound.
const paxos = @import("paxos");

comptime {
    _ = paxos.Protocol(u64, .{ .max_slots = 1 << 32 });
}
