const std = @import("std");

const appender_mod = @import("appender.zig");
const codec_mod = @import("codec.zig");
const CoreQueue = @import("queue.zig").Queue;
const Index = @import("index.zig").Index;
const RollScheme = @import("roll.zig").RollScheme;
const roll = @import("roll.zig");
const tailer_mod = @import("tailer.zig");
const Version = @import("queue.zig").Version;
const StepResult = @import("platform.zig").StepResult;

pub const QueueConfig = struct {
    dir: []const u8,
    version: Version = .v1,
    roll_scheme: ?RollScheme = null,
    create: bool = false,
    use_huge_pages: bool = false,
    lock_appender_windows: bool = false,
    enable_prefetcher: bool = true,
    prefetch_runway_bytes: u64 = 8 * 1024 * 1024,
    read_prefetch_runway_bytes: u64 = 4 * 1024 * 1024,
    enable_cleaner: bool = true,
    spawn_helper_threads: bool = true,
    retention_cycles: ?u32 = null,
    preroll_ms: u64 = 1000,
    allocator: std.mem.Allocator = std.heap.page_allocator,
};

pub const Diagnostics = struct {
    appends: u64 = 0,
    rolls: u64 = 0,
    cas_retries: u64 = 0,
    synchronous_cycle_opens: u64 = 0,
    preroll_misses: u64 = 0,
    highest_cycle: u64 = 0,
    lowest_cycle: u64 = 0,
    modcount: u64 = 0,
    prefetcher_enabled: bool = false,
    cleaner_enabled: bool = false,
};

pub const BackoffPolicy = enum {
    spin,
    yield,
};

pub fn Queue(comptime MessageType: type) type {
    return struct {
        const Self = @This();

        inner: *CoreQueue,
        codec: codec_mod.Codec(MessageType),

        pub fn open(config: QueueConfig, comptime codec: codec_mod.Codec(MessageType)) !Self {
            _ = config.version;
            _ = config.lock_appender_windows;

            const inner = try CoreQueue.init(config.allocator, config.dir);
            errdefer inner.deinit();

            inner.setCreate(config.create);
            if (config.roll_scheme) |scheme| {
                try inner.setRollScheme(scheme);
            } else if (config.create) {
                try inner.setRollScheme(roll.default_scheme);
            }
            inner.setUseHugePages(config.use_huge_pages);
            inner.setPrefetcher(config.enable_prefetcher, config.prefetch_runway_bytes);
            inner.setReadPrefetcher(config.read_prefetch_runway_bytes);
            inner.setCleaner(config.enable_cleaner, config.retention_cycles);
            inner.setHelperThreads(config.spawn_helper_threads);
            inner.setPrerollMs(config.preroll_ms);
            try inner.open();

            return .{
                .inner = inner,
                .codec = codec,
            };
        }

        pub fn deinit(self: *Self) void {
            self.inner.deinit();
        }

        pub fn append(self: *Self, msg: MessageType) !u64 {
            return self.appendWithTimestamp(msg, try nowMs(self.inner.io));
        }

        pub fn appendWithTimestamp(self: *Self, msg: MessageType, ts_ms: u64) !u64 {
            const app = try self.inner.openAppender();
            return app.appendWithCodec(MessageType, self.codec, msg, ts_ms);
        }

        pub fn tailer(self: *Self, start_index: u64) !Tailer(MessageType) {
            const actual_start = if (start_index == 0 and self.inner.lowest_cycle != 0)
                Index.compose(@intCast(self.inner.lowest_cycle), 0)
            else
                start_index;
            return .{
                .inner = try self.inner.openTailer(actual_start),
                .codec = self.codec,
            };
        }

        pub fn truncateBefore(self: *Self, cycle: u32) !void {
            if (self.inner.cleaner) |cleaner| {
                cleaner.retention_floor_cycle = cycle;
                return;
            }
            return error.CleanerFailed;
        }

        pub fn diagnostics(self: *const Self) Diagnostics {
            var result: Diagnostics = .{
                .highest_cycle = self.inner.highest_cycle,
                .lowest_cycle = self.inner.lowest_cycle,
                .modcount = self.inner.modcount,
                .prefetcher_enabled = self.inner.prefetcher != null,
                .cleaner_enabled = self.inner.cleaner != null,
            };
            if (self.inner.appender) |app| {
                result.appends = app.diagnostics.appends;
                result.rolls = app.diagnostics.rolls;
                result.cas_retries = app.diagnostics.cas_retries;
                result.synchronous_cycle_opens = app.diagnostics.synchronous_cycle_opens;
                result.preroll_misses = app.diagnostics.preroll_misses;
            }
            return result;
        }

        pub fn maintenancePoll(self: *Self, max_work_units: u32) !StepResult {
            return self.inner.maintenancePoll(max_work_units);
        }

        pub fn getVersion(self: *const Self) Version {
            return self.inner.getVersion();
        }

        pub fn getRollScheme(self: *const Self) RollScheme {
            return .{
                .name = self.inner.roll_name orelse "",
                .format_str = self.inner.roll_format orelse "",
                .roll_length_secs = self.inner.roll_length_secs,
                .index_count = self.inner.index_count,
                .index_spacing = self.inner.index_spacing,
            };
        }
    };
}

pub fn Tailer(comptime MessageType: type) type {
    return struct {
        const Self = @This();

        pub const Entry = tailer_mod.Entry(MessageType);

        inner: *tailer_mod.Tailer,
        codec: codec_mod.Codec(MessageType),

        pub fn poll(self: *Self) !?Entry {
            return self.inner.pollWithCodec(MessageType, self.codec);
        }

        pub fn collect(self: *Self, backoff: BackoffPolicy) !Entry {
            while (true) {
                if (try self.poll()) |entry| return entry;
                switch (backoff) {
                    .spin => std.atomic.spinLoopHint(),
                    .yield => std.Thread.yield() catch {},
                }
            }
        }

        pub fn prefetchPoll(self: *Self, max_work_units: u32) !StepResult {
            return self.inner.prefetchPoll(max_work_units);
        }

        pub fn seekTo(self: *Self, index: u64) !void {
            try self.inner.seekTo(index);
        }

        pub fn getState(self: *const Self) tailer_mod.TailerState {
            return self.inner.state;
        }

        pub fn getIndex(self: *const Self) u64 {
            return self.inner.qf_index;
        }

        pub fn deinit(self: *Self) void {
            self.inner.deinit();
        }
    };
}

pub fn hexDump(writer: anytype, label: []const u8, data: []const u8) !void {
    try writer.print("-- {s} ({d} bytes) --\n", .{ label, data.len });
    var offset: usize = 0;
    while (offset < data.len) {
        const end = @min(offset + 16, data.len);
        try writer.print("{x:0>8}  ", .{offset});
        for (data[offset..end]) |b| try writer.print("{x:0>2} ", .{b});
        for (0..(16 - (end - offset))) |_| try writer.print("   ", .{});
        try writer.print(" |", .{});
        for (data[offset..end]) |b| {
            const c: u8 = if (b >= 0x20 and b < 0x7f) b else '.';
            try writer.print("{c}", .{c});
        }
        try writer.print("|\n", .{});
        offset = end;
    }
}

fn nowMs(io: std.Io) !u64 {
    const ms = std.Io.Clock.real.now(io).toMilliseconds();
    if (ms < 0) return error.InvalidRollConfig;
    return @intCast(ms);
}

test "public raw queue appends and polls messages" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    var queue = try Queue([]const u8).open(.{
        .dir = path,
        .create = true,
        .roll_scheme = roll.findSchemeByName("TEST4_SECONDLY").?,
        .allocator = allocator,
        .spawn_helper_threads = false,
    }, codec_mod.RawCodec);
    defer queue.deinit();

    const idx = try queue.appendWithTimestamp("hello", 0);
    try std.testing.expectEqual(Index.compose(0, 0), idx);

    var tailer = try queue.tailer(0);
    defer tailer.deinit();

    const entry = (try tailer.poll()).?;
    try std.testing.expectEqual(idx, entry.index);
    try std.testing.expectEqualStrings("hello", entry.message);
}

test "hexDump writes stable ASCII output" {
    var buf: [256]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buf);
    try hexDump(&writer, "payload", "abc");
    try std.testing.expect(std.mem.indexOf(u8, writer.buffered(), "payload") != null);
    try std.testing.expect(std.mem.indexOf(u8, writer.buffered(), "61 62 63") != null);
}

fn tmpQueuePath(allocator: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
}
