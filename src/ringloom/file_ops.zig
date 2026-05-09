// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

const Platform = @import("platform.zig").Platform;
const RingloomError = @import("errors.zig").RingloomError;

/// Rounds `value` up to the next multiple of `alignment`.
pub inline fn alignUp(value: u64, alignment: u64) u64 {
    if (alignment == 0) return value;
    const rem = value % alignment;
    return if (rem == 0) value else value + (alignment - rem);
}

/// Returns true when the appender is within one block of the mapped file end.
pub inline fn needsExtension(tip: u64, file_size: u64, blocksize: u64) bool {
    return blocksize != 0 and tip +| blocksize >= file_size;
}

/// Preallocates and truncates a file to the requested size.
pub fn extendFile(platform: Platform, fd: std.posix.fd_t, new_size: u64) RingloomError!void {
    try platform.preallocate(fd, 0, new_size);
    try platform.truncate(fd, new_size);
}

/// Extends a file even when platform block preallocation is unavailable.
pub fn extendFilePermissive(platform: Platform, fd: std.posix.fd_t, new_size: u64) RingloomError!void {
    platform.preallocate(fd, 0, new_size) catch |err| switch (err) {
        error.PlatformCapabilityUnavailable => {},
        else => return err,
    };
    try platform.truncate(fd, new_size);
}

/// Extends a file only when `required_size` exceeds `current_size`.
pub fn ensureFileSize(
    platform: Platform,
    fd: std.posix.fd_t,
    current_size: *u64,
    required_size: u64,
    blocksize: u64,
) RingloomError!void {
    if (required_size <= current_size.*) return;
    const new_size = alignUp(required_size, blocksize);
    try extendFile(platform, fd, new_size);
    current_size.* = new_size;
}

/// Best-effort variant of `ensureFileSize` for platforms without preallocation.
pub fn ensureFileSizePermissive(
    platform: Platform,
    fd: std.posix.fd_t,
    current_size: *u64,
    required_size: u64,
    blocksize: u64,
) RingloomError!void {
    if (required_size <= current_size.*) return;
    const new_size = alignUp(required_size, blocksize);
    try extendFilePermissive(platform, fd, new_size);
    current_size.* = new_size;
}

test "alignUp and needsExtension" {
    try std.testing.expectEqual(@as(u64, 4096), alignUp(1, 4096));
    try std.testing.expectEqual(@as(u64, 4096), alignUp(4096, 4096));
    try std.testing.expect(needsExtension(3072, 4096, 1024));
    try std.testing.expect(!needsExtension(1024, 4096, 1024));
}

test "ensureFileSizePermissive extends a file" {
    const io = std.testing.io;
    const cwd = std.Io.Dir.cwd();
    const path = ".zig-cache/ringloom-extend-test.ringloom";
    const file = try cwd.createFile(io, path, .{ .read = true });
    defer file.close(io);
    defer cwd.deleteFile(io, path) catch {};

    var size: u64 = 0;
    try ensureFileSizePermissive(Platform.detect(), file.handle, &size, 5000, 4096);
    try std.testing.expectEqual(@as(u64, 8192), size);
    try std.testing.expectEqual(@as(u64, 8192), (try file.stat(io)).size);
}
