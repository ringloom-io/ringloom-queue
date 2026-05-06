const std = @import("std");

const StepResult = @import("platform.zig").StepResult;

pub const ReadPrefetchState = struct {
    cycle: u64 = 0,
    next_offset: u64 = 0,
    published_limit: u64 = 0,
    active: bool = false,

    pub fn reset(self: *ReadPrefetchState, cycle: u64, offset: u64) void {
        self.* = .{
            .cycle = cycle,
            .next_offset = offset,
            .published_limit = offset,
            .active = true,
        };
    }
};

pub const WritePrefetchState = struct {
    cycle: u64 = 0,
    next_offset: u64 = 0,
    active: bool = false,

    pub fn reset(self: *WritePrefetchState, cycle: u64, offset: u64) void {
        self.* = .{
            .cycle = cycle,
            .next_offset = offset,
            .active = true,
        };
    }
};

pub const Prefetcher = struct {
    allocator: std.mem.Allocator,
    write: WritePrefetchState = .{},

    pub fn init(allocator: std.mem.Allocator) Prefetcher {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Prefetcher) void {
        _ = self;
    }

    pub fn poll(self: *Prefetcher, max_work_units: usize) StepResult {
        _ = self;
        _ = max_work_units;
        return .idle;
    }
};

test "prefetch state machines are pollable shells" {
    var read: ReadPrefetchState = .{};
    read.reset(7, 128);
    try std.testing.expect(read.active);
    try std.testing.expectEqual(@as(u64, 7), read.cycle);
    try std.testing.expectEqual(@as(u64, 128), read.next_offset);

    var prefetcher = Prefetcher.init(std.testing.allocator);
    defer prefetcher.deinit();
    try std.testing.expectEqual(StepResult.idle, prefetcher.poll(1));
}
