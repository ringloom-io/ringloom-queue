// SPDX-License-Identifier: Apache-2.0

//! Sink-side startup helper: derive the last applied index from a queue.
//!
//! The write-at-index *write* path
//! lives on `Appender` (appender.zig); this module only provides the read-only
//! resume helper the sink calls once at startup.

const std = @import("std");
const Queue = @import("../queue.zig").Queue;
const Tailer = @import("../tailer.zig").Tailer;
const Index = @import("../index.zig").Index;

/// Returns the highest fully-published index in `queue`, or -1 if it is empty.
///
/// Walks from the lowest cycle to the tip with a `Tailer`, returning the last
/// non-null index. Walking from the lowest cycle (rather than seeking the
/// highest) is required: trailing empty cycles would make a highest-only seek
/// wrongly report an empty queue. O(records) but startup-only.
pub fn deriveLastAppliedIndex(queue: *Queue) !i64 {
    const start_index: u64 = if (queue.lowest_cycle != 0)
        Index.compose(@intCast(queue.lowest_cycle), 0)
    else
        0;

    const t = try Tailer.create(queue, start_index);
    defer t.deinit();

    var last: i64 = -1;
    while (try t.pollRaw()) |entry| {
        last = @intCast(entry.index);
    }
    return last;
}
