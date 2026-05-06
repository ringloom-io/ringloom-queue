const std = @import("std");

pub const Header = struct {
    pub const UNALLOCATED: u32 = 0x00000000;
    pub const WORKING: u32 = 0x80000000;
    pub const METADATA: u32 = 0x40000000;
    pub const EOF: u32 = 0xc0000000;

    pub const SIZE_MASK: u32 = 0x3fffffff;
    pub const META_MASK: u32 = 0xc0000000;
    pub const HEADER_SIZE: usize = @sizeOf(u32);

    pub inline fn isUnallocated(h: u32) bool {
        return h == UNALLOCATED;
    }

    pub inline fn metaType(h: u32) u32 {
        return h & META_MASK;
    }

    pub inline fn dataLength(h: u32) u30 {
        return @truncate(h & SIZE_MASK);
    }

    pub inline fn isWorking(h: u32) bool {
        return metaType(h) == WORKING;
    }

    pub inline fn isMetadata(h: u32) bool {
        return metaType(h) == METADATA;
    }

    pub inline fn isEof(h: u32) bool {
        return metaType(h) == EOF;
    }

    pub inline fn isData(h: u32) bool {
        return h != UNALLOCATED and metaType(h) == 0;
    }

    pub inline fn dataHeader(size: u30) u32 {
        return @as(u32, size);
    }

    pub inline fn metadataHeader(size: u30) u32 {
        return METADATA | @as(u32, size);
    }

    pub inline fn workingHeader() u32 {
        return WORKING;
    }

    pub inline fn paddingFor(payload_size: usize) usize {
        return (0 -% payload_size) & 0x03;
    }

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
