const std = @import("std");

const StepResult = @import("platform.zig").StepResult;
const Queue = @import("queue.zig").Queue;

/// Pollable cleaner shell for retention and cold-page reclamation work.
pub const Cleaner = struct {
    allocator: std.mem.Allocator,
    queue: ?*Queue = null,
    retention_cycles: ?u32 = null,
    retention_floor_cycle: u64 = 0,

    /// Creates an idle cleaner.
    pub fn init(allocator: std.mem.Allocator) Cleaner {
        return .{ .allocator = allocator };
    }

    /// Creates a cleaner bound to a queue's maintenance state.
    pub fn initForQueue(allocator: std.mem.Allocator, queue: *Queue, retention_cycles: ?u32) Cleaner {
        return .{
            .allocator = allocator,
            .queue = queue,
            .retention_cycles = retention_cycles,
        };
    }

    /// Releases resources owned by the cleaner.
    pub fn deinit(self: *Cleaner) void {
        _ = self;
    }

    /// Requests deletion of cycle files older than `cycle`.
    pub fn deleteCyclesBefore(self: *Cleaner, cycle: u64) void {
        if (cycle > self.retention_floor_cycle) self.retention_floor_cycle = cycle;
    }

    /// Drives bounded cleaner work.
    pub fn poll(self: *Cleaner, max_work_units: usize) !StepResult {
        if (max_work_units == 0) return .idle;
        const queue = self.queue orelse return .idle;
        return try self.applyRetention(queue, max_work_units);
    }

    fn applyRetention(self: *Cleaner, queue: *Queue, max_work_units: usize) !StepResult {
        const floor = self.computeRetentionFloor(queue);
        self.retention_floor_cycle = @max(self.retention_floor_cycle, floor);
        if (self.retention_floor_cycle == 0) return .idle;

        var cycle = queue.lowest_cycle;
        if (queue.metadata) |meta| {
            cycle = @atomicLoad(u64, &meta.lowest_cycle, .acquire);
        }
        if (cycle >= self.retention_floor_cycle) return .idle;

        var remaining = max_work_units;
        var made_progress = false;
        while (remaining > 0 and cycle < self.retention_floor_cycle) {
            if (!canDeleteCycle(queue, cycle)) break;

            const path = try queue.cyclePath(cycle);
            defer self.allocator.free(path);

            if (!canDeleteCycle(queue, cycle)) break;
            std.Io.Dir.cwd().deleteFile(queue.io, path) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
            removeQueueFilePath(queue, path);

            cycle += 1;
            publishLowestCycle(queue, cycle);
            made_progress = true;
            remaining -= 1;
        }

        if (!made_progress) return .idle;
        return if (cycle < self.retention_floor_cycle and canDeleteCycle(queue, cycle))
            .more_work
        else
            .made_progress;
    }

    fn computeRetentionFloor(self: *const Cleaner, queue: *Queue) u64 {
        var floor = self.retention_floor_cycle;
        if (self.retention_cycles) |retention| {
            const highest = if (queue.metadata) |meta|
                @atomicLoad(u64, &meta.highest_cycle, .acquire)
            else
                queue.highest_cycle;
            const keep = @as(u64, retention);
            const highest_plus_one = highest +| 1;
            const retention_floor = if (keep == 0)
                highest
            else if (highest_plus_one > keep)
                highest_plus_one - keep
            else
                0;
            floor = @max(floor, retention_floor);
        }
        return floor;
    }
};

fn canDeleteCycle(queue: *Queue, cycle: u64) bool {
    const highest = if (queue.metadata) |meta|
        @atomicLoad(u64, &meta.highest_cycle, .acquire)
    else
        queue.highest_cycle;
    if (cycle >= highest) return false;

    if (queue.preroll_cycle) |preroll_cycle| {
        if (cycle >= preroll_cycle) return false;
    }
    if (queue.appender) |appender| {
        const appender_cycle = @atomicLoad(u64, &appender.cycle, .acquire);
        if (cycle >= appender_cycle) return false;
    }
    for (queue.tailers.items) |tailer| {
        const tailer_cycle = @atomicLoad(u64, &tailer.qf_cycle_open, .acquire);
        if (tailer_cycle != std.math.maxInt(u64) and cycle >= tailer_cycle) return false;
    }
    return true;
}

fn publishLowestCycle(queue: *Queue, cycle: u64) void {
    if (queue.metadata) |meta| {
        const current = @atomicLoad(u64, &meta.lowest_cycle, .acquire);
        if (cycle > current) @atomicStore(u64, &meta.lowest_cycle, cycle, .release);
        queue.lowest_cycle = @atomicLoad(u64, &meta.lowest_cycle, .acquire);
    } else {
        queue.lowest_cycle = @max(queue.lowest_cycle, cycle);
    }
}

fn removeQueueFilePath(queue: *Queue, path: []const u8) void {
    for (queue.queuefile_paths.items, 0..) |existing, i| {
        if (std.mem.eql(u8, existing, path)) {
            queue.allocator.free(existing);
            _ = queue.queuefile_paths.swapRemove(i);
            return;
        }
    }
}
