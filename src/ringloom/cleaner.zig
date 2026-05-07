const std = @import("std");

const StepResult = @import("platform.zig").StepResult;

/// Pollable cleaner shell for retention and cold-page reclamation work.
pub const Cleaner = struct {
    allocator: std.mem.Allocator,
    retention_floor_cycle: u64 = 0,

    /// Creates an idle cleaner.
    pub fn init(allocator: std.mem.Allocator) Cleaner {
        return .{ .allocator = allocator };
    }

    /// Releases resources owned by the cleaner.
    pub fn deinit(self: *Cleaner) void {
        _ = self;
    }

    /// Drives bounded cleaner work; currently returns idle for the shell implementation.
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
