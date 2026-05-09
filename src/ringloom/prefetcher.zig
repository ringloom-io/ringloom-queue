const std = @import("std");

const config = @import("config.zig");
const StepResult = @import("platform.zig").StepResult;
const mmap_ops = @import("mmap_ops.zig");
const RingloomError = @import("errors.zig").RingloomError;
const Queue = @import("queue.zig").Queue;

/// Tracks the next published range a tailer can safely prefetch.
pub const ReadPrefetchState = struct {
    cycle: u64 = 0,
    cursor_offset: u64 = 0,
    next_offset: u64 = 0,
    published_limit: u64 = 0,
    active: bool = false,

    /// Starts read prefetch tracking at `offset` within `cycle`.
    pub fn reset(self: *ReadPrefetchState, cycle: u64, offset: u64) void {
        self.* = .{
            .cycle = cycle,
            .cursor_offset = offset,
            .next_offset = offset,
            .published_limit = offset,
            .active = true,
        };
    }

    /// Returns the published-but-not-yet-prefetched range, when non-empty.
    pub fn publishedRange(self: *const ReadPrefetchState) ?struct { start: u64, len: u64 } {
        if (!self.active or self.published_limit <= self.next_offset) return null;
        return .{ .start = self.next_offset, .len = self.published_limit - self.next_offset };
    }
};

/// Tracks the next future range an appender prefetcher should prepare.
pub const WritePrefetchState = struct {
    cycle: u64 = 0,
    next_offset: u64 = 0,
    active: bool = false,

    /// Starts write prefetch tracking at `offset` within `cycle`.
    pub fn reset(self: *WritePrefetchState, cycle: u64, offset: u64) void {
        self.* = .{
            .cycle = cycle,
            .next_offset = offset,
            .active = true,
        };
    }
};

pub const PrefetcherOptions = struct {
    write_runway_bytes: u64 = 8 * 1024 * 1024,
    read_runway_bytes: u64 = 4 * 1024 * 1024,
};

/// Pollable prefetch helper used to keep appender and tailer windows warm.
pub const Prefetcher = struct {
    allocator: std.mem.Allocator,
    queue: ?*Queue = null,
    write: WritePrefetchState = .{},
    write_runway_bytes: u64 = 8 * 1024 * 1024,
    read_runway_bytes: u64 = 4 * 1024 * 1024,

    /// Creates an idle prefetcher shell.
    pub fn init(allocator: std.mem.Allocator) Prefetcher {
        return .{ .allocator = allocator };
    }

    /// Creates a prefetcher bound to a queue's maintenance state.
    pub fn initForQueue(allocator: std.mem.Allocator, queue: *Queue, options: PrefetcherOptions) Prefetcher {
        return .{
            .allocator = allocator,
            .queue = queue,
            .write_runway_bytes = options.write_runway_bytes,
            .read_runway_bytes = options.read_runway_bytes,
        };
    }

    /// Releases resources owned by the prefetcher.
    pub fn deinit(self: *Prefetcher) void {
        _ = self;
    }

    /// Drives bounded background prefetch work.
    pub fn poll(self: *Prefetcher, max_work_units: usize) !StepResult {
        if (max_work_units == 0) return .idle;
        const queue = self.queue orelse return .idle;

        var remaining = max_work_units;
        var made_progress = false;
        var has_more = false;

        if (queue.appender) |appender| {
            const write_step = try self.prefetchWriteRunway(appender, &remaining);
            made_progress = made_progress or write_step != .idle;
            has_more = has_more or write_step == .more_work;
        }

        if (remaining > 0) {
            const before_cycle = queue.preroll_cycle;
            const before_fd = queue.preroll_fd;
            queue.maybePreroll(try nowMs(queue.io)) catch |err| switch (err) {
                error.PathAlreadyExists => {},
                else => return err,
            };
            if (before_cycle == null and queue.preroll_cycle != null and before_fd == null and queue.preroll_fd != null) {
                made_progress = true;
                remaining -= 1;
            }
        }

        if (has_more or (made_progress and remaining == 0)) return .more_work;
        return if (made_progress) .made_progress else .idle;
    }

    /// Write-touches a future zero-filled appender mapping.
    pub fn prepareWritableWindow(
        self: *Prefetcher,
        buf: []align(std.heap.page_size_min) u8,
        page_size: usize,
    ) void {
        _ = self;
        mmap_ops.touchWritablePages(buf, page_size);
    }

    /// Applies read-ahead hints and read-touches a tailer mapping.
    pub fn prepareReadableWindow(
        self: *Prefetcher,
        buf: []align(std.heap.page_size_min) u8,
        page_size: usize,
    ) RingloomError!void {
        _ = self;
        try mmap_ops.adviseSequential(buf);
        mmap_ops.touchReadablePages(buf, page_size);
    }

    /// Applies bounded read-ahead/read-touch to a published range tracked by a tailer.
    pub fn prepareReadableRange(
        self: *Prefetcher,
        fd: ?std.posix.fd_t,
        buf: []align(config.page_alignment) u8,
        mmap_offset: u64,
        file_size: u64,
        state: *ReadPrefetchState,
        max_work_units: usize,
    ) RingloomError!StepResult {
        if (max_work_units == 0 or self.read_runway_bytes == 0) return .idle;
        if (state.publishedRange() == null) return .idle;

        const page_size = effectivePageSize(self);
        const map_end = mmap_offset +| @as(u64, buf.len);
        const published_limit = @min(state.published_limit, @min(map_end, file_size));
        if (published_limit <= state.next_offset) return .idle;

        const runway_limit = state.cursor_offset +| self.read_runway_bytes;
        const work_limit = state.next_offset +| workBudgetBytes(max_work_units, page_size);
        const target_limit = @min(published_limit, @min(runway_limit, work_limit));
        if (target_limit <= state.next_offset) return .idle;
        if (state.next_offset < mmap_offset) return error.PrefetchFailed;

        const rel_start_u64 = state.next_offset - mmap_offset;
        const len_u64 = target_limit - state.next_offset;
        if (rel_start_u64 > std.math.maxInt(usize) or len_u64 > std.math.maxInt(usize)) {
            return error.PrefetchFailed;
        }
        const rel_start: usize = @intCast(rel_start_u64);
        const len: usize = @intCast(len_u64);
        if (rel_start > buf.len or len > buf.len - rel_start) return error.PrefetchFailed;

        if (fd) |file_fd| {
            if (self.queue) |queue| {
                queue.platform.adviseReadAhead(file_fd, state.next_offset, len_u64) catch |err| switch (err) {
                    error.PlatformCapabilityUnavailable => {},
                    else => return err,
                };
            }
        }
        touchReadableRange(buf, rel_start, len, page_size);
        state.next_offset = target_limit;

        return if (state.next_offset < published_limit and state.next_offset < runway_limit)
            .more_work
        else
            .made_progress;
    }

    /// Prepares a future cycle file for appender roll handoff without touching the live appender mapping.
    pub fn prepareCycle(self: *Prefetcher, cycle: u64) !StepResult {
        const queue = self.queue orelse return .idle;
        const prepared = try queue.preparePrerollCycle(cycle);
        return if (prepared) .made_progress else .idle;
    }

    fn prefetchWriteRunway(self: *Prefetcher, appender: anytype, remaining: *usize) RingloomError!StepResult {
        if (remaining.* == 0 or self.write_runway_bytes == 0) return .idle;
        const buf = appender.buf orelse return .idle;
        if (appender.fd == null) return .idle;

        const page_size = effectivePageSize(self);
        const safe_start = alignForward(appender.tip, page_size);
        const appender_cycle = @atomicLoad(u64, &appender.cycle, .acquire);
        if (!self.write.active or self.write.cycle != appender_cycle or self.write.next_offset < safe_start) {
            self.write.reset(appender_cycle, safe_start);
        }

        const map_end = appender.mmap_offset +| appender.mmap_size;
        const runway_end = appender.tip +| self.write_runway_bytes;
        const target_limit = @min(appender.file_size, @min(map_end, runway_end));
        if (target_limit <= self.write.next_offset) return .idle;
        if (self.write.next_offset < appender.mmap_offset) return error.PrefetchFailed;

        const work_bytes = workBudgetBytes(remaining.*, page_size);
        const limit = @min(target_limit, self.write.next_offset +| work_bytes);
        const rel_start_u64 = self.write.next_offset - appender.mmap_offset;
        const len_u64 = limit - self.write.next_offset;
        if (rel_start_u64 > std.math.maxInt(usize) or len_u64 > std.math.maxInt(usize)) {
            return error.PrefetchFailed;
        }
        const rel_start: usize = @intCast(rel_start_u64);
        const len: usize = @intCast(len_u64);
        if (rel_start > buf.len or len > buf.len - rel_start) return error.PrefetchFailed;

        touchReadableRange(buf, rel_start, len, page_size);
        self.write.next_offset = limit;

        const units_used_u64 = std.math.divCeil(u64, len_u64, page_size) catch 1;
        const units_used: usize = @intCast(@min(units_used_u64, remaining.*));
        remaining.* -= units_used;

        return if (self.write.next_offset < target_limit) .more_work else .made_progress;
    }
};

fn nowMs(io: std.Io) !u64 {
    const ms = std.Io.Clock.real.now(io).toMilliseconds();
    if (ms < 0) return error.InvalidRollConfig;
    return @intCast(ms);
}

fn effectivePageSize(self: *const Prefetcher) u64 {
    if (self.queue) |queue| return queue.platform.page_size;
    return std.heap.pageSize();
}

fn workBudgetBytes(max_work_units: usize, page_size: u64) u64 {
    const units: u64 = @intCast(max_work_units);
    return std.math.mul(u64, units, page_size) catch std.math.maxInt(u64);
}

fn alignForward(value: u64, alignment: u64) u64 {
    if (alignment == 0) return value;
    const rem = value % alignment;
    return if (rem == 0) value else value + (alignment - rem);
}

fn alignForwardUsize(value: usize, alignment: usize) usize {
    if (alignment == 0) return value;
    const rem = value % alignment;
    return if (rem == 0) value else value + (alignment - rem);
}

fn touchReadableRange(
    buf: []align(config.page_alignment) const u8,
    start: usize,
    len: usize,
    page_size_u64: u64,
) void {
    if (len == 0) return;
    const page_size: usize = @intCast(@min(page_size_u64, std.math.maxInt(usize)));
    const end = start + len;

    var off = start;
    while (off < end) {
        const ptr: *const volatile u8 = @ptrCast(&buf[off]);
        _ = ptr.*;
        const next = alignForwardUsize(off + 1, page_size);
        if (next <= off) break;
        off = next;
    }
}

test "prefetch state machines are pollable shells" {
    var read: ReadPrefetchState = .{};
    read.reset(7, 128);
    try std.testing.expect(read.active);
    try std.testing.expectEqual(@as(u64, 7), read.cycle);
    try std.testing.expectEqual(@as(u64, 128), read.next_offset);
    read.published_limit = 256;
    const range = read.publishedRange().?;
    try std.testing.expectEqual(@as(u64, 128), range.start);
    try std.testing.expectEqual(@as(u64, 128), range.len);

    var prefetcher = Prefetcher.init(std.testing.allocator);
    defer prefetcher.deinit();
    try std.testing.expectEqual(StepResult.idle, try prefetcher.poll(1));
}
