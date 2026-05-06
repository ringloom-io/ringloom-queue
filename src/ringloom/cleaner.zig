const std = @import("std");

const StepResult = @import("platform.zig").StepResult;

pub const Cleaner = struct {
    allocator: std.mem.Allocator,
    retention_floor_cycle: u64 = 0,

    pub fn init(allocator: std.mem.Allocator) Cleaner {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Cleaner) void {
        _ = self;
    }

    pub fn poll(self: *Cleaner, max_work_units: usize) StepResult {
        _ = self;
        _ = max_work_units;
        return .idle;
    }
};

test "cleaner is a pollable shell" {
    var cleaner = Cleaner.init(std.testing.allocator);
    defer cleaner.deinit();
    try std.testing.expectEqual(StepResult.idle, cleaner.poll(1));
}
