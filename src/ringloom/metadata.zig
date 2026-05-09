// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

pub const metadata_magic: u32 = 0x4d515a42;
pub const queue_file_magic: u32 = 0x43515a42;
pub const format_version: u16 = 1;

/// Fixed 512-byte metadata file shared by appenders and tailers.
pub const SharedMetadata = extern struct {
    magic: u32 = metadata_magic,
    version: u16 = format_version,
    flags: u16 = 0,
    roll_length_secs: u32,
    index_spacing: u32,
    index_count: u32,
    _pad0: u32 = 0,
    epoch_ms: u64,
    highest_cycle: u64 align(8),
    lowest_cycle: u64 align(8),
    modcount: u64 align(8),
    write_position: u64 align(8),
    appender_lock: u64 align(8),
    _reserved: [440]u8 = [_]u8{0} ** 440,

    /// Creates a zeroed metadata record with the immutable roll configuration set.
    pub fn init(roll_length_secs: u32, index_spacing: u32, index_count: u32, epoch_ms: u64) SharedMetadata {
        return .{
            .roll_length_secs = roll_length_secs,
            .index_spacing = index_spacing,
            .index_count = index_count,
            .epoch_ms = epoch_ms,
            .highest_cycle = 0,
            .lowest_cycle = 0,
            .modcount = 0,
            .write_position = 0,
            .appender_lock = 0,
        };
    }
};

/// Fixed 64-byte header at the start of every cycle data file.
pub const QueueFileHeader = extern struct {
    magic: u32 = queue_file_magic,
    version: u16 = format_version,
    flags: u16 = 0,
    roll_length_secs: u32,
    index_spacing: u32,
    index_count: u32,
    _pad0: u32 = 0,
    epoch_ms: u64,
    created_cycle: u32,
    _reserved: [28]u8 = [_]u8{0} ** 28,

    /// Creates a queue-file header that mirrors the shared roll configuration.
    pub fn init(
        roll_length_secs: u32,
        index_spacing: u32,
        index_count: u32,
        epoch_ms: u64,
        created_cycle: u32,
    ) QueueFileHeader {
        return .{
            .roll_length_secs = roll_length_secs,
            .index_spacing = index_spacing,
            .index_count = index_count,
            .epoch_ms = epoch_ms,
            .created_cycle = created_cycle,
        };
    }
};

comptime {
    std.debug.assert(@sizeOf(SharedMetadata) == 512);
    std.debug.assert(@alignOf(SharedMetadata) == 8);
    std.debug.assert(@offsetOf(SharedMetadata, "epoch_ms") == 24);
    std.debug.assert(@offsetOf(SharedMetadata, "highest_cycle") == 32);
    std.debug.assert(@offsetOf(SharedMetadata, "appender_lock") == 64);
    std.debug.assert(@offsetOf(SharedMetadata, "_reserved") == 72);

    std.debug.assert(@sizeOf(QueueFileHeader) == 64);
    std.debug.assert(@alignOf(QueueFileHeader) == 8);
    std.debug.assert(@offsetOf(QueueFileHeader, "epoch_ms") == 24);
    std.debug.assert(@offsetOf(QueueFileHeader, "created_cycle") == 32);
    std.debug.assert(@offsetOf(QueueFileHeader, "_reserved") == 36);
}

test "metadata and queue file headers have fixed layout" {
    try std.testing.expectEqual(@as(usize, 512), @sizeOf(SharedMetadata));
    try std.testing.expectEqual(@as(usize, 64), @sizeOf(QueueFileHeader));
    try std.testing.expectEqual(@as(usize, 56), @offsetOf(SharedMetadata, "write_position"));
    try std.testing.expectEqual(@as(usize, 32), @offsetOf(QueueFileHeader, "created_cycle"));
}

test "header initializers populate protocol fields" {
    const meta = SharedMetadata.init(86_400, 256, 4096, 0);
    try std.testing.expectEqual(metadata_magic, meta.magic);
    try std.testing.expectEqual(format_version, meta.version);
    try std.testing.expectEqual(@as(u64, 0), meta.appender_lock);

    const qfh = QueueFileHeader.init(86_400, 256, 4096, 0, 42);
    try std.testing.expectEqual(queue_file_magic, qfh.magic);
    try std.testing.expectEqual(@as(u32, 42), qfh.created_cycle);
}
