// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

/// Namespace for the 4-byte entry header that frames every on-disk message.
pub const Header = struct {
    /// Zero-filled space that has not been claimed by an appender.
    pub const UNALLOCATED: u32 = 0x00000000;
    /// Temporary claim marker while an appender is copying the payload.
    pub const WORKING: u32 = 0x80000000;
    /// Internal metadata entry marker; the lower 30 bits still hold a length.
    pub const METADATA: u32 = 0x40000000;
    /// Cycle terminator written at roll boundaries.
    pub const EOF: u32 = 0xc0000000;

    /// Mask for the 30-bit payload length.
    pub const SIZE_MASK: u32 = 0x3fffffff;
    /// Mask for the two high-order state bits.
    pub const META_MASK: u32 = 0xc0000000;
    /// Serialized header size in bytes.
    pub const HEADER_SIZE: usize = @sizeOf(u32);

    /// Returns true when no entry has been published at this offset.
    pub inline fn isUnallocated(h: u32) bool {
        return h == UNALLOCATED;
    }

    /// Extracts the high-order state bits.
    pub inline fn metaType(h: u32) u32 {
        return h & META_MASK;
    }

    /// Extracts the payload length without the state bits.
    pub inline fn dataLength(h: u32) u30 {
        return @truncate(h & SIZE_MASK);
    }

    /// Returns true while a writer owns this slot but has not published it.
    pub inline fn isWorking(h: u32) bool {
        return metaType(h) == WORKING;
    }

    /// Returns true for internal queue metadata entries.
    pub inline fn isMetadata(h: u32) bool {
        return metaType(h) == METADATA;
    }

    /// Returns true for the end-of-cycle marker.
    pub inline fn isEof(h: u32) bool {
        return metaType(h) == EOF;
    }

    /// Returns true for a published user payload entry.
    pub inline fn isData(h: u32) bool {
        return h != UNALLOCATED and metaType(h) == 0;
    }

    /// Builds a published user payload header.
    pub inline fn dataHeader(size: u30) u32 {
        return @as(u32, size);
    }

    /// Builds an internal metadata payload header.
    pub inline fn metadataHeader(size: u30) u32 {
        return METADATA | @as(u32, size);
    }

    /// Builds the in-progress claim marker.
    pub inline fn workingHeader() u32 {
        return WORKING;
    }

    /// Returns the zero-padding needed before the next 4-byte-aligned header.
    pub inline fn paddingFor(payload_size: usize) usize {
        return (0 -% payload_size) & 0x03;
    }

    /// Returns the full serialized entry size, including header and padding.
    pub inline fn entrySize(payload_size: usize) usize {
        return HEADER_SIZE + payload_size + paddingFor(payload_size);
    }
};

test "header constants and bit helpers" {
    try std.testing.expect(Header.isUnallocated(Header.UNALLOCATED));
    try std.testing.expect(Header.isWorking(Header.WORKING));
    try std.testing.expect(Header.isMetadata(Header.metadataHeader(12)));
    try std.testing.expect(Header.isEof(Header.EOF));
    try std.testing.expect(Header.isData(Header.dataHeader(42)));
    try std.testing.expectEqual(@as(u30, 42), Header.dataLength(Header.metadataHeader(42)));
    try std.testing.expectEqual(@as(u32, Header.WORKING), Header.workingHeader());
}

test "entry padding is four-byte aligned" {
    try std.testing.expectEqual(@as(usize, 3), Header.paddingFor(1));
    try std.testing.expectEqual(@as(usize, 2), Header.paddingFor(2));
    try std.testing.expectEqual(@as(usize, 1), Header.paddingFor(3));
    try std.testing.expectEqual(@as(usize, 0), Header.paddingFor(4));
    try std.testing.expectEqual(@as(usize, 12), Header.entrySize(5));
}
