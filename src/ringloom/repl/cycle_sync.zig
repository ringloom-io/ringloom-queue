// SPDX-License-Identifier: Apache-2.0

//! Cycle synchronization: materialize the follower's cycle files and EOF
//! markers so follower tailers roll exactly like the master, including across
//! empty cycles.

const std = @import("std");
const Queue = @import("../queue.zig").Queue;
const Appender = @import("../appender.zig").Appender;
const Index = @import("../index.zig").Index;

pub const CycleSynchronizer = struct {
    queue: *Queue,
    appender: *Appender,

    pub fn init(queue: *Queue, appender: *Appender) CycleSynchronizer {
        return .{ .queue = queue, .appender = appender };
    }

    /// Materialize and EOF-terminate every cycle in `[from_cycle, to_cycle)`,
    /// then position the appender at the start of `to_cycle`. The next excerpt
    /// (`writeAtIndex(to_cycle/0, ...)`) finds `self.cycle == to_cycle` already.
    pub fn onCycleRoll(self: *CycleSynchronizer, from_cycle: u32, to_cycle: u32, next_expected_index: u64) !void {
        if (to_cycle <= from_cycle) return error.InvalidRollConfig;

        // 1. Seal `from_cycle` (the cycle the appender just finished, or open it).
        try self.appender.sealCurrentCycleIfAt(from_cycle);

        // 2. Materialize + seal every intermediate empty cycle.
        var c: u64 = @as(u64, from_cycle) + 1;
        while (c < to_cycle) : (c += 1) {
            try self.materializeEmptyCycle(c);
        }

        // 3. Open `to_cycle` for writing at seqnum 0.
        try self.appender.openCycleForWrite(to_cycle);

        std.debug.assert(self.appender.expectedNextIndex() == Index.compose(to_cycle, 0));
        std.debug.assert(next_expected_index == Index.compose(to_cycle, 0));
    }

    /// Creates an empty-but-closed cycle file: header, zeroed inline index, and
    /// an EOF marker at the data-region start. Idempotent.
    fn materializeEmptyCycle(self: *CycleSynchronizer, cycle: u64) !void {
        try self.appender.openCycleForWrite(cycle);
        try self.appender.sealTip();
    }
};
