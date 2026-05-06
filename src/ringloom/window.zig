const std = @import("std");

const config = @import("config.zig");
const mmap_ops = @import("mmap_ops.zig");
const RingloomError = @import("errors.zig").RingloomError;

pub const WindowParams = struct {
    offset: u64,
    size: u64,
    tip_in_window: u64,
};

pub fn computeWindow(tip: u64, file_size: u64, blocksize: u64) WindowParams {
    if (blocksize == 0 or file_size == 0) {
        return .{ .offset = 0, .size = 0, .tip_in_window = 0 };
    }

    const clamped_tip = @min(tip, file_size - 1);
    const block_index = clamped_tip / blocksize;
    const window_block = if (block_index > 0) block_index - 1 else 0;
    const offset = window_block * blocksize;
    const max_size = 2 * blocksize;
    const size = @min(max_size, file_size - offset);

    return .{
        .offset = offset,
        .size = size,
        .tip_in_window = tip - offset,
    };
}

pub inline fn needsRemap(current_offset: u64, current_size: u64, new: WindowParams) bool {
    return new.size == 0 or new.offset != current_offset or new.size != current_size;
}

pub inline fn shouldPremap(tip_in_window: u64, window_size: u64) bool {
    return window_size != 0 and tip_in_window > (window_size >> 1);
}

pub const MmapWindow = struct {
    buf: ?[]align(config.page_alignment) u8 = null,
    offset: u64 = 0,
    size: u64 = 0,
    fd: std.posix.fd_t = -1,

    pub fn ensureMapped(
        self: *MmapWindow,
        params: WindowParams,
        prot: mmap_ops.Protection,
        flags: mmap_ops.MapFlags,
    ) RingloomError!void {
        if (self.buf != null and !needsRemap(self.offset, self.size, params)) return;
        if (params.size == 0 or params.size > std.math.maxInt(usize)) return error.MmapFailed;

        const new_buf = try mmap_ops.mapFileWithFallback(
            self.fd,
            params.offset,
            @intCast(params.size),
            prot,
            flags,
        );
        if (self.buf) |old_buf| mmap_ops.unmapFile(old_buf);
        self.buf = new_buf;
        self.offset = params.offset;
        self.size = params.size;
    }

    pub fn ptrAt(self: *const MmapWindow, abs_offset: u64) ?[*]u8 {
        const buf = self.buf orelse return null;
        if (abs_offset < self.offset) return null;
        const rel = abs_offset - self.offset;
        if (rel >= self.size or rel >= buf.len) return null;
        return buf.ptr + @as(usize, @intCast(rel));
    }

    pub fn sliceAt(self: *const MmapWindow, abs_offset: u64, len: usize) ?[]u8 {
        const buf = self.buf orelse return null;
        if (abs_offset < self.offset) return null;
        const rel = abs_offset - self.offset;
        if (rel > self.size or @as(u64, len) > self.size - rel) return null;
        if (rel > std.math.maxInt(usize)) return null;
        const start: usize = @intCast(rel);
        if (start > buf.len or len > buf.len - start) return null;
        return buf[start..][0..len];
    }

    pub fn unmap(self: *MmapWindow) void {
        if (self.buf) |old_buf| {
            mmap_ops.unmapFile(old_buf);
            self.buf = null;
        }
        self.offset = 0;
        self.size = 0;
    }
};

pub const PremappedWindow = struct {
    current: MmapWindow = .{},
    next: ?MmapWindow = null,

    pub fn deinit(self: *PremappedWindow) void {
        self.current.unmap();
        if (self.next) |*next| next.unmap();
        self.next = null;
    }

    pub fn setNext(self: *PremappedWindow, next: MmapWindow) void {
        if (self.next) |*old| old.unmap();
        self.next = next;
    }

    pub fn ensureCurrentMapped(
        self: *PremappedWindow,
        params: WindowParams,
        prot: mmap_ops.Protection,
        flags: mmap_ops.MapFlags,
    ) RingloomError!void {
        if (!needsRemap(self.current.offset, self.current.size, params) and self.current.buf != null) {
            return;
        }

        if (self.next) |*next| {
            if (!needsRemap(next.offset, next.size, params) and next.buf != null) {
                self.current.unmap();
                self.current = next.*;
                self.next = null;
                return;
            }
            next.unmap();
            self.next = null;
        }

        try self.current.ensureMapped(params, prot, flags);
    }
};

test "computeWindow at file start" {
    const p = computeWindow(0, 8 * 1024 * 1024, 2 * 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 0), p.offset);
    try std.testing.expectEqual(@as(u64, 4 * 1024 * 1024), p.size);
    try std.testing.expectEqual(@as(u64, 0), p.tip_in_window);
}

test "computeWindow mid-file keeps previous block mapped" {
    const p = computeWindow(5 * 1024 * 1024, 16 * 1024 * 1024, 2 * 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 2 * 1024 * 1024), p.offset);
    try std.testing.expectEqual(@as(u64, 4 * 1024 * 1024), p.size);
    try std.testing.expectEqual(@as(u64, 3 * 1024 * 1024), p.tip_in_window);
}

test "shouldPremap triggers past 50 percent boundary" {
    try std.testing.expect(!shouldPremap(1024, 4096));
    try std.testing.expect(shouldPremap(2049, 4096));
}

test "MmapWindow maps slices by absolute offset" {
    const io = std.testing.io;
    const cwd = std.Io.Dir.cwd();
    const path = ".zig-cache/ringloom-window-test.ringloom";
    const file = try cwd.createFile(io, path, .{ .read = true });
    defer file.close(io);
    defer cwd.deleteFile(io, path) catch {};
    try @import("platform.zig").Platform.detect().truncate(file.handle, 2 * std.heap.page_size_min);

    var win: MmapWindow = .{ .fd = file.handle };
    defer win.unmap();

    try win.ensureMapped(.{
        .offset = 0,
        .size = 2 * std.heap.page_size_min,
        .tip_in_window = 0,
    }, .read_write, .{});

    const slice = win.sliceAt(16, 4).?;
    @memcpy(slice, "test");
    try std.testing.expectEqualStrings("test", win.sliceAt(16, 4).?);
    try std.testing.expectEqual(@as(?[]u8, null), win.sliceAt(2 * std.heap.page_size_min, 1));
}
