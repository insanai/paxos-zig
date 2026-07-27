//! Compile-fail fixture: pointer-bearing value types must be rejected.
const paxos = @import("paxos");

comptime {
    _ = paxos.Protocol(*u64, .{});
}
