//! Compile-fail fixture: derived message capacity must not overflow usize.
//! Built for a 32-bit target; on 64-bit hosts the bounds checks keep the
//! products inside usize.
const std = @import("std");
const paxos = @import("paxos");

comptime {
    _ = paxos.Protocol(u64, .{
        .max_members = 65535,
        .max_slots = std.math.maxInt(u32),
    });
}
