const std = @import("std");

const StepResult = @import("platform.zig").StepResult;
const mmap_ops = @import("mmap_ops.zig");
const RingloomError = @import("errors.zig").RingloomError;

/// Tracks the next published range a tailer can safely prefetch.
pub const ReadPrefetchState = struct {
    cycle: u64 = 0,
    next_offset: u64 = 0,
    published_limit: u64 = 0,
    active: bool = false,

    /// Starts read prefetch tracking at `offset` within `cycle`.
    pub fn reset(self: *ReadPrefetchState, cycle: u64, offset: u64) void {
        self.* = .{
            .cycle = cycle,
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

/// Pollable prefetch helper used to keep appender and tailer windows warm.
pub const Prefetcher = struct {
    allocator: std.mem.Allocator,
    write: WritePrefetchState = .{},

    /// Creates an idle prefetcher shell.
    pub fn init(allocator: std.mem.Allocator) Prefetcher {
        return .{ .allocator = allocator };
    }

    /// Releases resources owned by the prefetcher.
    pub fn deinit(self: *Prefetcher) void {
        _ = self;
    }

    /// Drives bounded background work; currently returns idle for the shell implementation.
    pub fn poll(self: *Prefetcher, max_work_units: usize) StepResult {
        _ = self;
        _ = max_work_units;
        return .idle;
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
};

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
    try std.testing.expectEqual(StepResult.idle, prefetcher.poll(1));
}
