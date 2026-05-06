const std = @import("std");

const config = @import("config.zig");
const QueueFileHeader = @import("metadata.zig").QueueFileHeader;

pub const Index = struct {
    pub const cycle_shift: u6 = 32;
    pub const seqnum_mask: u64 = 0x00000000ffffffff;

    pub inline fn cycle(index: u64) u32 {
        return @truncate(index >> cycle_shift);
    }

    pub inline fn seqnum(index: u64) u32 {
        return @truncate(index & seqnum_mask);
    }

    pub inline fn compose(cyc: u32, seq: u32) u64 {
        return (@as(u64, cyc) << cycle_shift) | @as(u64, seq);
    }

    pub inline fn cycleStart(cyc: u32) u64 {
        return @as(u64, cyc) << cycle_shift;
    }

    pub inline fn nextCycleStart(index: u64) u64 {
        return cycleStart(cycle(index) + 1);
    }
};

pub const SeekPoint = struct {
    offset: u64,
    seqnum: u32,
};

pub const IndexRegion = struct {
    entries: [*]align(8) u64,
    count: u32,
    spacing: u32,

    pub inline fn slotFor(self: IndexRegion, seqnum: u32) ?u32 {
        if (self.spacing == 0) return null;
        const slot = seqnum / self.spacing;
        if (slot >= self.count) return null;
        if (seqnum % self.spacing != 0) return null;
        return slot;
    }

    pub inline fn lookup(self: IndexRegion, slot: u32) ?u64 {
        if (slot >= self.count) return null;
        const val = @atomicLoad(u64, &self.entries[slot], .acquire);
        if (val == 0) return null;
        return val;
    }

    pub inline fn store(self: IndexRegion, slot: u32, offset: u64) void {
        if (slot >= self.count) return;
        @atomicStore(u64, &self.entries[slot], offset, .release);
    }

    pub inline fn dataRegionOffset(self: IndexRegion) u64 {
        return @sizeOf(QueueFileHeader) + @as(u64, self.count) * @sizeOf(u64);
    }

    pub fn seekOffset(self: IndexRegion, target_seqnum: u32, data_start: u64) SeekPoint {
        if (self.count == 0 or self.spacing == 0) return .{ .offset = data_start, .seqnum = 0 };

        const target_slot = target_seqnum / self.spacing;
        var slot = @min(target_slot, self.count - 1);
        while (true) {
            if (self.lookup(slot)) |offset| {
                return .{ .offset = offset, .seqnum = slot * self.spacing };
            }
            if (slot == 0) break;
            slot -= 1;
        }

        return .{ .offset = data_start, .seqnum = 0 };
    }

    pub fn fromMmap(mmap_base: [*]align(config.page_alignment) u8, header: *const QueueFileHeader) IndexRegion {
        const index_base = mmap_base + @sizeOf(QueueFileHeader);
        return .{
            .entries = @ptrCast(@alignCast(index_base)),
            .count = header.index_count,
            .spacing = header.index_spacing,
        };
    }
};

test "index compose and decompose" {
    const value = Index.compose(0x4a05, 3);
    try std.testing.expectEqual(@as(u32, 0x4a05), Index.cycle(value));
    try std.testing.expectEqual(@as(u32, 3), Index.seqnum(value));
    try std.testing.expectEqual(Index.compose(0x4a06, 0), Index.nextCycleStart(value));
}

test "index region slot lookup store and seek" {
    var entries: [4]u64 align(8) = [_]u64{0} ** 4;
    const region: IndexRegion = .{
        .entries = entries[0..].ptr,
        .count = 4,
        .spacing = 2,
    };

    try std.testing.expectEqual(@as(?u32, 0), region.slotFor(0));
    try std.testing.expectEqual(@as(?u32, null), region.slotFor(1));
    try std.testing.expectEqual(@as(?u32, 2), region.slotFor(4));
    try std.testing.expectEqual(@as(?u32, null), region.slotFor(8));
    try std.testing.expectEqual(@as(?u64, null), region.lookup(0));

    region.store(0, 128);
    region.store(2, 512);
    try std.testing.expectEqual(@as(?u64, 128), region.lookup(0));
    try std.testing.expectEqual(@as(?u64, 512), region.lookup(2));

    const point = region.seekOffset(5, 64);
    try std.testing.expectEqual(@as(u64, 512), point.offset);
    try std.testing.expectEqual(@as(u32, 4), point.seqnum);
}
