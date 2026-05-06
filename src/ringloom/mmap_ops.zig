const std = @import("std");

const config = @import("config.zig");
const RingloomError = @import("errors.zig").RingloomError;
const SharedMetadata = @import("metadata.zig").SharedMetadata;

const posix = std.posix;

pub const Protection = enum {
    read_only,
    read_write,

    fn toPosix(self: Protection) posix.PROT {
        return switch (self) {
            .read_only => .{ .READ = true },
            .read_write => .{ .READ = true, .WRITE = true },
        };
    }
};

pub const MapFlags = struct {
    type: posix.system.MAP_TYPE = .SHARED,
    populate: bool = false,
    huge_tlb: bool = false,

    fn toPosix(self: MapFlags) posix.MAP {
        var flags: posix.MAP = .{ .TYPE = self.type };
        if (comptime @hasField(posix.MAP, "POPULATE")) {
            flags.POPULATE = self.populate;
        }
        if (comptime @hasField(posix.MAP, "HUGETLB")) {
            flags.HUGETLB = self.huge_tlb;
        }
        return flags;
    }

    pub fn withoutHugePages(self: MapFlags) MapFlags {
        var flags = self;
        flags.huge_tlb = false;
        return flags;
    }
};

pub const MetadataMap = struct {
    buf: []align(config.page_alignment) u8,
    metadata: *SharedMetadata,

    pub fn unmap(self: MetadataMap) void {
        unmapFile(self.buf);
    }
};

pub fn mapFile(
    fd: posix.fd_t,
    offset: u64,
    size: usize,
    prot: Protection,
    flags: MapFlags,
) RingloomError![]align(config.page_alignment) u8 {
    if (size == 0) return error.MmapFailed;
    if (offset % std.heap.pageSize() != 0) return error.MmapFailed;

    return posix.mmap(
        null,
        size,
        prot.toPosix(),
        flags.toPosix(),
        fd,
        offset,
    ) catch error.MmapFailed;
}

pub fn mapFileWithFallback(
    fd: posix.fd_t,
    offset: u64,
    size: usize,
    prot: Protection,
    flags: MapFlags,
) RingloomError![]align(config.page_alignment) u8 {
    return mapFile(fd, offset, size, prot, flags) catch |err| {
        if (err == error.MmapFailed and flags.huge_tlb) {
            return try mapFile(fd, offset, size, prot, flags.withoutHugePages());
        }
        return err;
    };
}

pub fn unmapFile(buf: []align(config.page_alignment) const u8) void {
    posix.munmap(buf);
}

pub fn remapFile(
    old_buf: []align(config.page_alignment) u8,
    fd: posix.fd_t,
    new_offset: u64,
    new_size: usize,
    prot: Protection,
    flags: MapFlags,
) RingloomError![]align(config.page_alignment) u8 {
    const new_buf = try mapFileWithFallback(fd, new_offset, new_size, prot, flags);
    unmapFile(old_buf);
    return new_buf;
}

pub fn adviseSequential(buf: []align(config.page_alignment) u8) RingloomError!void {
    posix.madvise(buf.ptr, buf.len, posix.MADV.SEQUENTIAL) catch return error.MadviseFailed;
}

pub fn adviseWillNeed(buf: []align(config.page_alignment) u8) RingloomError!void {
    posix.madvise(buf.ptr, buf.len, posix.MADV.WILLNEED) catch return error.MadviseFailed;
}

pub fn adviseDontNeed(buf: []align(config.page_alignment) u8) RingloomError!void {
    posix.madvise(buf.ptr, buf.len, posix.MADV.DONTNEED) catch return error.MadviseFailed;
}

pub fn touchWritablePages(buf: []align(config.page_alignment) u8, page_size: usize) void {
    var off: usize = 0;
    while (off < buf.len) : (off += page_size) {
        const ptr: *volatile u8 = @ptrCast(&buf[off]);
        ptr.* = 0;
    }
}

pub fn touchReadablePages(buf: []align(config.page_alignment) const u8, page_size: usize) void {
    var off: usize = 0;
    while (off < buf.len) : (off += page_size) {
        const ptr: *const volatile u8 = @ptrCast(&buf[off]);
        _ = ptr.*;
    }
}

pub fn mapSharedMetadata(fd: posix.fd_t, prot: Protection) RingloomError!MetadataMap {
    const buf = try mapFile(fd, 0, @sizeOf(SharedMetadata), prot, .{});
    return .{
        .buf = buf,
        .metadata = @ptrCast(@alignCast(buf.ptr)),
    };
}

test "mapFile and unmapFile round-trip" {
    const io = std.testing.io;
    const cwd = std.Io.Dir.cwd();
    const path = ".zig-cache/ringloom-mmap-test.ringloom";
    const file = try cwd.createFile(io, path, .{ .read = true });
    defer file.close(io);
    defer cwd.deleteFile(io, path) catch {};
    try @import("platform.zig").Platform.detect().truncate(file.handle, std.heap.page_size_min);

    const buf = try mapFile(file.handle, 0, std.heap.page_size_min, .read_write, .{});
    defer unmapFile(buf);

    buf[0] = 0xab;
    try std.testing.expectEqual(@as(u8, 0xab), buf[0]);
}

test "mapFileWithFallback maps without huge pages when regular mapping works" {
    const io = std.testing.io;
    const cwd = std.Io.Dir.cwd();
    const path = ".zig-cache/ringloom-mmap-fallback.ringloom";
    const file = try cwd.createFile(io, path, .{ .read = true });
    defer file.close(io);
    defer cwd.deleteFile(io, path) catch {};
    try @import("platform.zig").Platform.detect().truncate(file.handle, std.heap.page_size_min);

    const buf = try mapFileWithFallback(file.handle, 0, std.heap.page_size_min, .read_write, .{
        .populate = true,
        .huge_tlb = false,
    });
    defer unmapFile(buf);
    touchWritablePages(buf, std.heap.pageSize());
}

test "metadata map casts fixed layout shared metadata" {
    const io = std.testing.io;
    const cwd = std.Io.Dir.cwd();
    const path = ".zig-cache/ringloom-metadata-map.ringloom";
    const file = try cwd.createFile(io, path, .{ .read = true });
    defer file.close(io);
    defer cwd.deleteFile(io, path) catch {};
    try @import("platform.zig").Platform.detect().truncate(file.handle, @sizeOf(SharedMetadata));

    const mapped = try mapSharedMetadata(file.handle, .read_write);
    defer mapped.unmap();

    mapped.metadata.* = SharedMetadata.init(86_400, 256, 4096, 0);
    try std.testing.expectEqual(@as(u32, 4096), mapped.metadata.index_count);
}
