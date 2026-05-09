// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const builtin = @import("builtin");

const api = @import("api.zig");
const appender_mod = @import("appender.zig");
const c_api = @import("c_api.zig");
const codec = @import("codec.zig");
const Header = @import("header.zig").Header;
const Index = @import("index.zig").Index;
const IndexRegion = @import("index.zig").IndexRegion;
const metadata = @import("metadata.zig");
const Queue = @import("queue.zig").Queue;
const roll = @import("roll.zig");
const tailer_mod = @import("tailer.zig");

test "validation: create write close reopen read via public API" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    {
        var queue = try api.Queue([]const u8).open(.{
            .dir = path,
            .create = true,
            .roll_scheme = roll.findSchemeByName("TEST4_SECONDLY").?,
            .allocator = allocator,
            .spawn_helper_threads = false,
        }, codec.RawCodec);
        defer queue.deinit();

        try std.testing.expectEqual(Index.compose(0, 0), try queue.appendWithTimestamp("message-0", 0));
        try std.testing.expectEqual(Index.compose(0, 1), try queue.appendWithTimestamp("message-1", 0));
        try std.testing.expectEqual(Index.compose(0, 2), try queue.appendWithTimestamp("message-2", 0));
    }

    var reopened = try api.Queue([]const u8).open(.{
        .dir = path,
        .create = false,
        .allocator = allocator,
        .spawn_helper_threads = false,
    }, codec.RawCodec);
    defer reopened.deinit();

    var tailer = try reopened.tailer(0);
    defer tailer.deinit();

    const first = (try tailer.poll()).?;
    try std.testing.expectEqual(Index.compose(0, 0), first.index);
    try std.testing.expectEqualStrings("message-0", first.message);
    try std.testing.expectEqualStrings("message-1", (try tailer.poll()).?.message);
    try std.testing.expectEqualStrings("message-2", (try tailer.poll()).?.message);
    try std.testing.expect(try tailer.poll() == null);
}

test "validation: live metadata mmap bytes match v1 file format" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    const queue = try Queue.init(allocator, path);
    defer queue.deinit();
    queue.setCreate(true);
    try queue.setRollSchemeName("TEST4_SECONDLY");
    try queue.open();

    const stat = try tmp.dir.statFile(std.testing.io, "metadata.ringloom", .{});
    try std.testing.expectEqual(@as(u64, @sizeOf(metadata.SharedMetadata)), stat.size);

    const raw = std.mem.asBytes(queue.metadata.?);
    try std.testing.expectEqual(metadata.metadata_magic, std.mem.readInt(u32, raw[0..4], .little));
    try std.testing.expectEqual(metadata.format_version, std.mem.readInt(u16, raw[4..6], .little));
    try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, raw[8..12], .little));
    try std.testing.expectEqual(@as(u32, 4), std.mem.readInt(u32, raw[12..16], .little));
    try std.testing.expectEqual(@as(u32, 32), std.mem.readInt(u32, raw[16..20], .little));
}

test "validation: queue file header index entry payload and padding bytes" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    const queue = try Queue.init(allocator, path);
    defer queue.deinit();
    queue.setCreate(true);
    try queue.setRollSchemeName("TEST4_SECONDLY");
    try queue.open();

    const app = try appender_mod.Appender.open(queue);
    const idx = try app.appendWithTimestamp("abc", 0);
    try std.testing.expectEqual(Index.compose(0, 0), idx);

    const buf = app.buf.?;
    const qfh: *const metadata.QueueFileHeader = @ptrCast(@alignCast(buf.ptr));
    try std.testing.expectEqual(metadata.queue_file_magic, qfh.magic);
    try std.testing.expectEqual(metadata.format_version, qfh.version);
    try std.testing.expectEqual(@as(u32, 1), qfh.roll_length_secs);
    try std.testing.expectEqual(@as(u32, 4), qfh.index_spacing);
    try std.testing.expectEqual(@as(u32, 32), qfh.index_count);
    try std.testing.expectEqual(@as(u32, 0), qfh.created_cycle);

    const data_start: usize = @intCast(appender_mod.dataStartOffset(queue));
    const data_header = std.mem.readInt(u32, buf[data_start..][0..4], .little);
    try std.testing.expect(Header.isData(data_header));
    try std.testing.expectEqual(@as(u30, 3), Header.dataLength(data_header));
    try std.testing.expectEqualStrings("abc", buf[data_start + Header.HEADER_SIZE ..][0..3]);
    try std.testing.expectEqual(@as(u8, 0), buf[data_start + Header.HEADER_SIZE + 3]);

    const region = IndexRegion.fromMmap(buf.ptr, qfh);
    try std.testing.expectEqual(@as(?u64, @intCast(data_start)), region.lookup(0));
}

test "validation: roll writes EOF and tailer reads across cycles" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    var queue = try api.Queue([]const u8).open(.{
        .dir = path,
        .create = true,
        .roll_scheme = roll.findSchemeByName("TEST4_SECONDLY").?,
        .allocator = allocator,
        .spawn_helper_threads = false,
        .preroll_ms = 0,
    }, codec.RawCodec);
    defer queue.deinit();

    try std.testing.expectEqual(Index.compose(0, 0), try queue.appendWithTimestamp("cycle-0", 0));
    const eof_offset = queue.inner.appender.?.tip;
    try std.testing.expectEqual(Index.compose(1, 0), try queue.appendWithTimestamp("cycle-1", 1000));

    const old_path = try queue.inner.cyclePath(0);
    defer allocator.free(old_path);
    const old_file = try std.Io.Dir.cwd().openFile(std.testing.io, old_path, .{ .mode = .read_only });
    defer old_file.close(std.testing.io);
    const map = try @import("mmap_ops.zig").mapFile(old_file.handle, 0, @intCast(Queue.qf_disk_sz), .read_only, .{});
    defer @import("mmap_ops.zig").unmapFile(map);
    const eof = std.mem.readInt(u32, map[@intCast(eof_offset)..][0..4], .little);
    try std.testing.expectEqual(Header.EOF, eof);

    const tailer = try tailer_mod.Tailer.create(queue.inner, Index.compose(0, 0));
    defer tailer.deinit();
    try std.testing.expectEqualStrings("cycle-0", (try tailer.pollRaw()).?.payload);
    try std.testing.expectEqualStrings("cycle-1", (try tailer.pollRaw()).?.payload);
    try std.testing.expect(try tailer.pollRaw() == null);
}

test "validation: poll readiness and read prefetch stay within published limit" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    var queue = try api.Queue([]const u8).open(.{
        .dir = path,
        .create = true,
        .roll_scheme = roll.findSchemeByName("TEST4_SECONDLY").?,
        .allocator = allocator,
        .spawn_helper_threads = false,
        .enable_prefetcher = true,
    }, codec.RawCodec);
    defer queue.deinit();

    var tailer = try queue.tailer(0);
    defer tailer.deinit();
    try std.testing.expect(try tailer.poll() == null);

    _ = try queue.appendWithTimestamp("poll-test", 0);
    const entry = (try tailer.poll()).?;
    try std.testing.expectEqualStrings("poll-test", entry.message);
    try std.testing.expectEqual(@import("platform.zig").StepResult.idle, try tailer.prefetchPoll(8));

    const published = @atomicLoad(u64, &queue.inner.metadata.?.write_position, .acquire);
    try std.testing.expect(tailer.inner.read_prefetch.next_offset <= published);
    try std.testing.expect(tailer.inner.read_prefetch.published_limit <= published);
}

test "validation: multiple pre-opened tailers poll the same queue concurrently" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    const queue = try Queue.init(allocator, path);
    defer queue.deinit();
    queue.setCreate(true);
    try queue.setRollSchemeName("TEST4_SECONDLY");
    try queue.open();

    const app = try appender_mod.Appender.open(queue);
    const messages = 48;
    var i: usize = 0;
    while (i < messages) : (i += 1) {
        var msg_buf: [32]u8 = undefined;
        const msg = try std.fmt.bufPrint(&msg_buf, "msg-{d}", .{i});
        _ = try app.appendWithTimestamp(msg, 0);
    }

    const reader_count = 3;
    var tailers: [reader_count]*tailer_mod.Tailer = undefined;
    for (&tailers) |*tailer| {
        tailer.* = try tailer_mod.Tailer.create(queue, 0);
        try tailer.*.seekTo(0);
    }
    defer for (tailers) |tailer| tailer.deinit();

    var counts = [_]u32{0} ** reader_count;
    var threads: [reader_count]std.Thread = undefined;
    for (&threads, 0..) |*thread, thread_index| {
        thread.* = try std.Thread.spawn(.{}, struct {
            fn run(tailer: *tailer_mod.Tailer, count: *u32) void {
                while (true) {
                    const maybe_entry = tailer.pollRaw() catch return;
                    if (maybe_entry) |_| {
                        count.* += 1;
                    } else {
                        return;
                    }
                }
            }
        }.run, .{ tailers[thread_index], &counts[thread_index] });
    }

    for (&threads) |*thread| thread.join();
    for (counts) |count| try std.testing.expectEqual(@as(u32, messages), count);
}

test "validation: two C handles contend through shared appender lease and maintenance is pollable" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    var opts_create = c_api.ringloom_queue_options{
        .dir = path.ptr,
        .dir_len = path.len,
        .create = true,
        .enable_prefetcher = true,
        .enable_cleaner = true,
        .spawn_helper_threads = false,
    };
    var q1: ?*c_api.ringloom_queue_t = null;
    try std.testing.expectEqual(cCode(.ok), c_api.ringloom_queue_open(&opts_create, &q1));
    defer c_api.ringloom_queue_close(q1);

    var opts_open = opts_create;
    opts_open.create = false;
    var q2: ?*c_api.ringloom_queue_t = null;
    try std.testing.expectEqual(cCode(.ok), c_api.ringloom_queue_open(&opts_open, &q2));
    defer c_api.ringloom_queue_close(q2);

    var app1: ?*c_api.ringloom_appender_t = null;
    try std.testing.expectEqual(cCode(.ok), c_api.ringloom_appender_open(q1, &app1));
    defer c_api.ringloom_appender_close(app1);

    var app2: ?*c_api.ringloom_appender_t = null;
    try std.testing.expectEqual(cCode(.appender_already_open), c_api.ringloom_appender_open(q2, &app2));
    try std.testing.expect(app2 == null);

    var step: c_api.ringloom_step_result_t = .idle;
    try std.testing.expectEqual(cCode(.ok), c_api.ringloom_queue_maintenance_poll(q1, 1, &step));
    try std.testing.expect(@intFromEnum(step) >= @intFromEnum(c_api.ringloom_step_result_t.idle));
}

test "validation: child process writer is readable after reopen" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    {
        var queue = try api.Queue([]const u8).open(.{
            .dir = path,
            .create = true,
            .roll_scheme = roll.findSchemeByName("TEST4_SECONDLY").?,
            .allocator = allocator,
            .spawn_helper_threads = false,
        }, codec.RawCodec);
        queue.deinit();
    }

    const linux = std.os.linux;
    const fork_rc = linux.fork();
    if (linux.errno(fork_rc) != .SUCCESS) return error.ForkFailed;
    if (fork_rc == 0) {
        childWrite(path) catch std.process.exit(42);
        std.process.exit(0);
    }

    var status: u32 = 0;
    const wait_rc = linux.waitpid(@intCast(fork_rc), &status, 0);
    if (linux.errno(wait_rc) != .SUCCESS) return error.WaitFailed;
    try std.testing.expectEqual(@as(u32, 0), status);

    var queue = try api.Queue([]const u8).open(.{
        .dir = path,
        .create = false,
        .allocator = allocator,
        .spawn_helper_threads = false,
    }, codec.RawCodec);
    defer queue.deinit();

    var tailer = try queue.tailer(0);
    defer tailer.deinit();
    try std.testing.expectEqualStrings("child-0", (try tailer.poll()).?.message);
    try std.testing.expectEqualStrings("child-1", (try tailer.poll()).?.message);
    try std.testing.expectEqualStrings("child-2", (try tailer.poll()).?.message);
    try std.testing.expectEqualStrings("child-3", (try tailer.poll()).?.message);
}

test "property: codec header and index invariants over deterministic random data" {
    const seed: u64 = 0x7461_736b_3130;
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var payload: [128]u8 = undefined;
    var copy: [128]u8 = undefined;
    var i: usize = 0;
    while (i < 512) : (i += 1) {
        const len = random.intRangeAtMost(usize, 1, payload.len);
        random.bytes(payload[0..len]);
        const written = try codec.RawCodec.writePayload(&copy, payload[0..len]);
        const parsed = try codec.RawCodec.parsePayload(written);
        try expectEqualSlicesWithSeed(seed, payload[0..len], parsed);

        const size = random.intRangeAtMost(u30, 1, @intCast(Header.SIZE_MASK));
        const data_header = Header.dataHeader(size);
        try expectWithSeed(seed, Header.isData(data_header));
        try std.testing.expectEqual(size, Header.dataLength(data_header));
        const metadata_header = Header.metadataHeader(size);
        try expectWithSeed(seed, Header.isMetadata(metadata_header));
        try std.testing.expectEqual(size, Header.dataLength(metadata_header));
    }

    var entries: [16]u64 align(8) = [_]u64{0} ** 16;
    const region = IndexRegion{
        .entries = entries[0..].ptr,
        .count = entries.len,
        .spacing = 4,
    };
    var offset: u64 = 256;
    entries[0] = offset;
    for (entries[1..], 1..) |*entry, slot| {
        if (random.int(u1) == 1) {
            offset += random.intRangeAtMost(u64, 4, 128);
            entry.* = offset + slot;
        }
    }

    var target_seqnum: u32 = 0;
    while (target_seqnum < 80) : (target_seqnum += 1) {
        const point = region.seekOffset(target_seqnum, 256);
        var expected_slot: usize = @min(target_seqnum / region.spacing, entries.len - 1);
        while (expected_slot > 0 and entries[expected_slot] == 0) expected_slot -= 1;
        const expected_offset = if (entries[expected_slot] == 0) 256 else entries[expected_slot];
        try std.testing.expectEqual(expected_offset, point.offset);
        try std.testing.expectEqual(@as(u32, @intCast(expected_slot)) * region.spacing, point.seqnum);
    }
}

test "fuzz: header parser and raw codec smoke" {
    try std.testing.fuzz({}, struct {
        fn testOne(_: void, smith: *std.testing.Smith) !void {
            var bytes: [128]u8 = undefined;
            const len: usize = @intCast(smith.slice(&bytes));
            const input = bytes[0..len];

            if (input.len >= 4) {
                const raw = std.mem.readInt(u32, input[0..4], .little);
                _ = Header.dataLength(raw);
                _ = Header.isWorking(raw);
                _ = Header.isMetadata(raw);
                _ = Header.isEof(raw);
                _ = Header.isData(raw);
            }

            var out: [128]u8 = undefined;
            const written = try codec.RawCodec.writePayload(&out, input);
            const parsed = try codec.RawCodec.parsePayload(written);
            try std.testing.expectEqualSlices(u8, input, parsed);
        }
    }.testOne, .{
        .corpus = &.{
            "",
            "abc",
            "\x00\x00\x00\x00",
            "\x00\x00\x00\x80",
            "\xff\xff\xff\xff",
        },
    });
}

fn childWrite(path: []const u8) !void {
    var queue = try api.Queue([]const u8).open(.{
        .dir = path,
        .create = false,
        .allocator = std.heap.page_allocator,
        .spawn_helper_threads = false,
    }, codec.RawCodec);
    defer queue.deinit();

    try std.testing.expectEqual(Index.compose(0, 0), try queue.appendWithTimestamp("child-0", 0));
    try std.testing.expectEqual(Index.compose(0, 1), try queue.appendWithTimestamp("child-1", 0));
    try std.testing.expectEqual(Index.compose(0, 2), try queue.appendWithTimestamp("child-2", 0));
    try std.testing.expectEqual(Index.compose(0, 3), try queue.appendWithTimestamp("child-3", 0));
}

fn expectWithSeed(seed: u64, condition: bool) !void {
    if (!condition) {
        std.debug.print("random seed: 0x{x}\n", .{seed});
        return error.TestUnexpectedResult;
    }
}

fn expectEqualSlicesWithSeed(seed: u64, expected: []const u8, actual: []const u8) !void {
    if (!std.mem.eql(u8, expected, actual)) {
        std.debug.print("random seed: 0x{x}\n", .{seed});
        return error.TestExpectedEqual;
    }
}

fn cCode(err: c_api.ringloom_error_t) c_int {
    return @intFromEnum(err);
}

fn tmpQueuePath(allocator: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
}
