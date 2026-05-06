const std = @import("std");
const builtin = @import("builtin");

const RingloomError = @import("errors.zig").RingloomError;

pub const fd_t = std.posix.fd_t;

pub const StepResult = enum(u8) {
    idle,
    made_progress,
    more_work,
};

pub const Platform = struct {
    supports_map_populate: bool,
    supports_madv_populate_write: bool,
    supports_huge_pages: bool,
    page_size: usize,

    pub fn detect() Platform {
        return .{
            .supports_map_populate = builtin.os.tag == .linux,
            .supports_madv_populate_write = builtin.os.tag == .linux,
            .supports_huge_pages = builtin.os.tag == .linux,
            .page_size = std.heap.pageSize(),
        };
    }

    pub fn preallocate(self: Platform, fd: fd_t, offset: u64, len: u64) RingloomError!void {
        _ = self;
        _ = fd;
        _ = offset;
        _ = len;
        return error.PlatformCapabilityUnavailable;
    }

    pub fn adviseReadAhead(self: Platform, fd: fd_t, offset: u64, len: u64) RingloomError!void {
        _ = self;
        _ = fd;
        _ = offset;
        _ = len;
        return error.PlatformCapabilityUnavailable;
    }

    pub fn adviseDontNeed(self: Platform, fd: fd_t, offset: u64, len: u64) RingloomError!void {
        _ = self;
        _ = fd;
        _ = offset;
        _ = len;
        return error.PlatformCapabilityUnavailable;
    }
};

test "platform detects basic capabilities" {
    const platform = Platform.detect();
    try std.testing.expect(platform.page_size >= std.heap.page_size_min);
    if (builtin.os.tag == .linux) {
        try std.testing.expect(platform.supports_huge_pages);
    }
}
