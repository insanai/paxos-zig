//! A bounded non-voting learner for chosen Paxos values.
//!
//! `Learner` does not interpret promises or votes and cannot make a value
//! chosen. A trusted host feeds it values already established as chosen by the
//! active configuration. It buffers reordering, rejects conflicts, and releases
//! only a contiguous prefix to the application.

const std = @import("std");
const protocol = @import("protocol.zig");

/// Compile-time capacity of the bounded chosen-value buffer.
pub const Options = struct {
    /// Maximum chosen slots retained; slots above this bound are rejected.
    max_entries: usize = 256,
};

/// Outcome of recording one chosen value.
pub const LearnResult = enum {
    /// The identical value was already recorded for this slot; no change.
    duplicate,
    /// Recorded, but an undecided gap below it still blocks release.
    buffered,
    /// Recorded, and the contiguous released prefix advanced.
    advanced,
};

pub fn Learner(comptime Value: type, comptime options: Options) type {
    comptime std.debug.assert(options.max_entries > 0);

    return struct {
        const Self = @This();

        /// One released chosen value paired with its one-based slot.
        pub const Chosen = struct {
            slot: protocol.Slot,
            value: Value,
        };

        configuration_id: u64,
        learned: [options.max_entries]?Value =
            [_]?Value{null} ** options.max_entries,
        released_through: protocol.Slot = 0,

        /// Replaces `self` with an empty learner bound to one configuration;
        /// chosen values certified under any other configuration are rejected.
        pub fn init(self: *Self, configuration_id: u64) !void {
            if (configuration_id == 0) return error.InvalidConfigurationId;
            self.* = .{ .configuration_id = configuration_id };
            self.assertValid();
        }

        /// Records one host-certified chosen value for this configuration.
        /// Duplicates are idempotent; a different value for the same slot is
        /// evidence of corruption or a violated host certification contract.
        pub fn learnChosen(
            self: *Self,
            configuration_id: u64,
            slot: protocol.Slot,
            value: Value,
        ) !LearnResult {
            self.assertValid();
            if (configuration_id != self.configuration_id) {
                return error.ConfigurationMismatch;
            }
            const index = try slotIndex(slot);
            if (self.learned[index]) |existing| {
                if (!std.meta.eql(existing, value)) return error.ConflictingChosenValue;
                return .duplicate;
            }
            self.learned[index] = value;
            const before = self.released_through;
            self.advanceReleasedPrefix();
            self.assertValid();
            return if (self.released_through > before) .advanced else .buffered;
        }

        /// Copies the contiguous chosen suffix beginning at `from_slot`.
        pub fn readChosen(
            self: *const Self,
            from_slot: protocol.Slot,
            output: []Chosen,
        ) ![]const Chosen {
            self.assertValid();
            if (from_slot == 0) return error.InvalidSlot;
            if (from_slot > self.released_through) return output[0..0];
            const count: usize = @intCast(self.released_through - from_slot + 1);
            if (output.len < count) return error.ReadBufferTooSmall;
            for (output[0..count], 0..) |*chosen, offset| {
                const slot = from_slot + @as(protocol.Slot, @intCast(offset));
                chosen.* = .{ .slot = slot, .value = self.learned[slot - 1].? };
            }
            return output[0..count];
        }

        /// Returns a released chosen value. Buffered values above the
        /// contiguous prefix stay hidden until the gap below them fills, so
        /// applications never observe out-of-order state.
        pub fn chosenAt(self: *const Self, slot: protocol.Slot) ?Value {
            const index = slotIndex(slot) catch return null;
            if (slot > self.released_through) return null;
            return self.learned[index];
        }

        fn advanceReleasedPrefix(self: *Self) void {
            while (self.released_through < options.max_entries) {
                const next = self.released_through + 1;
                if (self.learned[next - 1] == null) return;
                self.released_through = next;
            }
        }

        fn assertValid(self: *const Self) void {
            if (!std.debug.runtime_safety) return;
            std.debug.assert(self.configuration_id > 0);
            std.debug.assert(self.released_through <= options.max_entries);
            for (0..self.released_through) |index| {
                std.debug.assert(self.learned[index] != null);
            }
        }

        fn slotIndex(slot: protocol.Slot) !usize {
            if (slot == 0 or slot > options.max_entries) return error.InvalidSlot;
            return @intCast(slot - 1);
        }
    };
}

test "learner releases only a contiguous chosen prefix" {
    const L = Learner(u64, .{ .max_entries = 4 });
    var learner: L = undefined;
    try learner.init(7);

    try std.testing.expectEqual(.buffered, try learner.learnChosen(7, 2, 22));
    try std.testing.expectEqual(@as(protocol.Slot, 0), learner.released_through);
    try std.testing.expectEqual(.advanced, try learner.learnChosen(7, 1, 11));
    try std.testing.expectEqual(@as(protocol.Slot, 2), learner.released_through);
    try std.testing.expectEqual(.duplicate, try learner.learnChosen(7, 2, 22));
    try std.testing.expectError(
        error.ConflictingChosenValue,
        learner.learnChosen(7, 2, 23),
    );
}

test "learner rejects messages from another configuration" {
    const L = Learner(u64, .{ .max_entries = 2 });
    var learner: L = undefined;
    try learner.init(9);
    try std.testing.expectError(
        error.ConfigurationMismatch,
        learner.learnChosen(8, 1, 1),
    );
}
