const std = @import("std");

const config = @import("config.zig");
const Index = @import("index.zig").Index;
const RollScheme = @import("roll.zig").RollScheme;
const javaFormatToStrftime = @import("roll.zig").javaFormatToStrftime;
const rollCycleFromMs = @import("roll.zig").cycleFromMs;
const SharedMetadata = @import("metadata.zig").SharedMetadata;
const Appender = @import("appender.zig").Appender;
const Prefetcher = @import("prefetcher.zig").Prefetcher;
const Cleaner = @import("cleaner.zig").Cleaner;
const Platform = @import("platform.zig").Platform;
const Tailer = @import("tailer.zig").Tailer;

pub const Queue = struct {
    allocator: std.mem.Allocator,
    dirname: []const u8,

    blocksize: u32 = config.default_blocksize,
    create: bool = false,

    metadata_fd: ?std.posix.fd_t = null,
    metadata_mmap: ?[]align(config.page_alignment) u8 = null,
    metadata: ?*SharedMetadata = null,

    highest_cycle: u64 = 0,
    lowest_cycle: u64 = 0,
    modcount: u64 = 0,

    roll_length_ms: u64 = 0,
    roll_epoch: i64 = 0,
    roll_format: ?[]const u8 = null,
    roll_name: ?[]const u8 = null,
    roll_strftime: ?[]const u8 = null,

    index_count: u32 = 0,
    index_spacing: u32 = 0,
    cycle_shift: u6 = Index.cycle_shift,
    seqnum_mask: u64 = Index.seqnum_mask,

    preroll_fd: ?std.posix.fd_t = null,
    preroll_mmap: ?[]align(config.page_alignment) u8 = null,

    platform: Platform,
    prefetcher: ?*Prefetcher = null,
    cleaner: ?*Cleaner = null,

    tailers: std.ArrayList(*Tailer) = .empty,
    appender: ?*Appender = null,

    last_error: ?[]const u8 = null,

    pub fn init(allocator: std.mem.Allocator, dirname: []const u8) !*Queue {
        const queue = try allocator.create(Queue);
        queue.* = .{
            .allocator = allocator,
            .dirname = try allocator.dupe(u8, dirname),
            .platform = Platform.detect(),
        };
        return queue;
    }

    pub fn setRollScheme(self: *Queue, scheme: RollScheme) !void {
        if (self.roll_name) |n| self.allocator.free(n);
        if (self.roll_format) |f| self.allocator.free(f);
        if (self.roll_strftime) |s| self.allocator.free(s);

        self.roll_name = try self.allocator.dupe(u8, scheme.name);
        self.roll_format = try self.allocator.dupe(u8, scheme.format_str);
        self.roll_length_ms = scheme.rollLengthMs();
        self.index_count = scheme.index_count;
        self.index_spacing = scheme.index_spacing;

        var sf_buf: [128]u8 = undefined;
        var cl_buf: [128]u8 = undefined;
        const result = try javaFormatToStrftime(scheme.format_str, &sf_buf, &cl_buf);
        self.roll_strftime = try self.allocator.dupe(u8, result.strftime);
    }

    pub fn doubleBlocksize(self: *Queue) void {
        self.blocksize <<= 1;
    }

    pub fn cycleFromMs(self: *const Queue, ms: i64) u64 {
        return rollCycleFromMs(ms, self.roll_epoch, self.roll_length_ms);
    }

    pub fn registerTailer(self: *Queue, tailer: *Tailer) !void {
        try self.tailers.append(self.allocator, tailer);
    }

    pub fn unregisterTailerPrefetch(self: *Queue, tailer: *Tailer) void {
        _ = self;
        _ = tailer;
    }

    pub fn deinit(self: *Queue) void {
        while (self.tailers.pop()) |tailer| {
            tailer.deinit();
        }
        self.tailers.deinit(self.allocator);

        if (self.appender) |appender| {
            appender.deinit();
            self.allocator.destroy(appender);
            self.appender = null;
        }
        if (self.prefetcher) |prefetcher| {
            prefetcher.deinit();
            self.allocator.destroy(prefetcher);
            self.prefetcher = null;
        }
        if (self.cleaner) |cleaner| {
            cleaner.deinit();
            self.allocator.destroy(cleaner);
            self.cleaner = null;
        }

        if (self.roll_format) |f| self.allocator.free(f);
        if (self.roll_name) |n| self.allocator.free(n);
        if (self.roll_strftime) |s| self.allocator.free(s);
        if (self.last_error) |e| self.allocator.free(e);
        self.allocator.free(self.dirname);
        self.allocator.destroy(self);
    }
};

test "queue init set roll scheme and cycle arithmetic" {
    const allocator = std.testing.allocator;
    const scheme = @import("roll.zig").findSchemeByName("FAST_DAILY").?;

    const queue = try Queue.init(allocator, "queue-dir");
    defer queue.deinit();

    try queue.setRollScheme(scheme);
    try std.testing.expectEqualStrings("queue-dir", queue.dirname);
    try std.testing.expectEqualStrings("FAST_DAILY", queue.roll_name.?);
    try std.testing.expectEqualStrings("%Y%m%dF", queue.roll_strftime.?);
    try std.testing.expectEqual(@as(u32, 4096), queue.index_count);
    try std.testing.expectEqual(@as(u64, 1), queue.cycleFromMs(86_400_000));

    queue.doubleBlocksize();
    try std.testing.expectEqual(@as(u32, config.default_blocksize * 2), queue.blocksize);
}

test "queue owns registered tailers" {
    const allocator = std.testing.allocator;
    const queue = try Queue.init(allocator, "queue-dir");
    defer queue.deinit();

    const tailer = try allocator.create(Tailer);
    tailer.* = Tailer.init(queue, Index.compose(2, 3));
    try queue.registerTailer(tailer);
    try std.testing.expectEqual(@as(usize, 1), queue.tailers.items.len);
}
