//! A deterministic, bounded implementation of classic and Multi-Paxos.
//!
//! The library performs no I/O and owns no threads or clocks. A `Node` consumes
//! messages and emits `Effects`. Persist `Effects.writes` before transmitting
//! `Effects.messages`; this ordering is part of the safety contract.

const protocol = @import("protocol.zig");
const replicated_log = @import("replicated_log.zig");

/// Stable identity of one voting member.
pub const NodeId = protocol.NodeId;
/// One-based log position.
pub const Slot = protocol.Slot;
/// Totally ordered Paxos proposal ballot.
pub const Ballot = protocol.Ballot;
/// Compile-time configuration of the minimal protocol core.
pub const Options = protocol.Options;
/// Bounded classic and Multi-Paxos effect machine.
pub const Protocol = protocol.Protocol;
/// Compile-time configuration of the sealed replicated-log layer.
pub const ReplicatedLogOptions = replicated_log.Options;
/// Reconfigurable command log with stop signs and snapshot epochs.
pub const ReplicatedLog = replicated_log.ReplicatedLog;

test {
    _ = @import("bit_set.zig");
    _ = @import("protocol.zig");
    _ = @import("replicated_log.zig");
}
