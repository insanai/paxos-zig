//! Compile-fail fixture: a zero-bit set must be rejected.
const bitset = @import("bitset");

comptime {
    _ = bitset.BitSet(0);
}
