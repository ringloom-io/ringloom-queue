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
    supports_preallocation: bool,
    supports_fadvise: bool,
    page_size: usize,

    pub fn detect() Platform {
        return .{
            .supports_map_populate = builtin.os.tag == .linux,
            .supports_madv_populate_write = builtin.os.tag == .linux,
            .supports_huge_pages = builtin.os.tag == .linux,
            .supports_preallocation = builtin.os.tag == .linux,
            .supports_fadvise = builtin.os.tag == .linux,
            .page_size = std.heap.pageSize(),
        };
    }

    pub fn preallocate(self: Platform, fd: fd_t, offset: u64, len: u64) RingloomError!void {
        if (!self.supports_preallocation) return error.PlatformCapabilityUnavailable;
        if (builtin.os.tag != .linux) return error.PlatformCapabilityUnavailable;

        const signed_offset = try toSignedOffset(offset, error.PreallocateFailed);
        const signed_len = try toSignedOffset(len, error.PreallocateFailed);
        const linux = std.os.linux;
        return checkedLinux(linux.fallocate(fd, 0, signed_offset, signed_len), .preallocate);
    }

    pub fn adviseReadAhead(self: Platform, fd: fd_t, offset: u64, len: u64) RingloomError!void {
        if (!self.supports_fadvise) return error.PlatformCapabilityUnavailable;
        if (builtin.os.tag != .linux) return error.PlatformCapabilityUnavailable;

        const signed_offset = try toSignedOffset(offset, error.PrefetchFailed);
        const signed_len = try toSignedOffset(len, error.PrefetchFailed);
        const linux = std.os.linux;
        return checkedLinux(
            linux.fadvise(fd, signed_offset, signed_len, linux.POSIX_FADV.WILLNEED),
            .prefetch,
        );
    }

    pub fn adviseDontNeed(self: Platform, fd: fd_t, offset: u64, len: u64) RingloomError!void {
        if (!self.supports_fadvise) return error.PlatformCapabilityUnavailable;
        if (builtin.os.tag != .linux) return error.PlatformCapabilityUnavailable;

        const signed_offset = try toSignedOffset(offset, error.CleanerFailed);
        const signed_len = try toSignedOffset(len, error.CleanerFailed);
        const linux = std.os.linux;
        return checkedLinux(
            linux.fadvise(fd, signed_offset, signed_len, linux.POSIX_FADV.DONTNEED),
            .cleaner,
        );
    }

    pub fn truncate(self: Platform, fd: fd_t, size: u64) RingloomError!void {
        _ = self;
        const signed_size = try toSignedOffset(size, error.PreallocateFailed);
        if (builtin.os.tag == .linux) {
            return checkedLinux(std.os.linux.ftruncate(fd, signed_size), .preallocate);
        }
        return error.PlatformCapabilityUnavailable;
    }
};

const LinuxOp = enum {
    preallocate,
    prefetch,
    cleaner,
};

fn toSignedOffset(value: u64, comptime err: RingloomError) RingloomError!i64 {
    if (value > @as(u64, @intCast(std.math.maxInt(i64)))) return err;
    return @intCast(value);
}

fn checkedLinux(rc: usize, op: LinuxOp) RingloomError!void {
    switch (std.posix.errno(rc)) {
        .SUCCESS => return,
        .NOSYS, .OPNOTSUPP, .NXIO => return error.PlatformCapabilityUnavailable,
        .NOSPC => return error.PreallocateFailed,
        .INTR => return switch (op) {
            .preallocate => error.PreallocateFailed,
            .prefetch => error.PrefetchFailed,
            .cleaner => error.CleanerFailed,
        },
        else => return switch (op) {
            .preallocate => error.PreallocateFailed,
            .prefetch => error.PrefetchFailed,
            .cleaner => error.CleanerFailed,
        },
    }
}

test "platform detects basic capabilities" {
    const platform = Platform.detect();
    try std.testing.expect(platform.page_size >= std.heap.page_size_min);
    if (builtin.os.tag == .linux) {
        try std.testing.expect(platform.supports_huge_pages);
        try std.testing.expect(platform.supports_preallocation);
        try std.testing.expect(platform.supports_fadvise);
    }
}
