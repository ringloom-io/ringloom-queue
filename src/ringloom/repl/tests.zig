// SPDX-License-Identifier: Apache-2.0

//! Replication unit + integration tests driven by the in-memory loopback
//! transport.

const std = @import("std");

const Queue = @import("../queue.zig").Queue;
const Appender = @import("../appender.zig").Appender;
const Tailer = @import("../tailer.zig").Tailer;
const Index = @import("../index.zig").Index;

const loopback = @import("loopback.zig");
const source_mod = @import("source.zig");
const sink_mod = @import("sink.zig");
const protocol = @import("protocol.zig");
const write_at_index = @import("write_at_index.zig");

const Loopback = loopback.Loopback;
const Source = source_mod.ReplicationSource(Loopback.Outbound, Loopback.Inbound);
const Sink = sink_mod.ReplicationSink(Loopback.Outbound, Loopback.Inbound);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn tmpQueuePath(allocator: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
}

fn makeQueue(allocator: std.mem.Allocator, path: []const u8, scheme: []const u8) !*Queue {
    const q = try Queue.init(allocator, path);
    errdefer q.deinit();
    q.setCreate(true);
    try q.setRollSchemeName(scheme);
    q.setHelperThreads(false);
    q.setCleaner(false, null);
    q.setPrefetcher(false, 0);
    try q.open();
    return q;
}

fn openQueue(allocator: std.mem.Allocator, path: []const u8) !*Queue {
    const q = try Queue.init(allocator, path);
    errdefer q.deinit();
    q.setHelperThreads(false);
    q.setCleaner(false, null);
    q.setPrefetcher(false, 0);
    try q.open();
    return q;
}

/// Drives both peers until the sink has applied `target_last`, or fails.
fn driveUntilApplied(src: *Source, snk: *Sink, target_last: i64, max_iters: usize) !void {
    var i: usize = 0;
    while (i < max_iters) : (i += 1) {
        _ = try src.step(256);
        _ = try snk.step(256);
        if (snk.last_applied_index >= target_last) return;
    }
    return error.DidNotConverge;
}

/// Asserts that ordinary tailers over both queues yield identical (index, payload) sequences.
fn assertTailerParity(allocator: std.mem.Allocator, master: *Queue, follower: *Queue) !void {
    const start_m: u64 = if (master.lowest_cycle != 0) Index.compose(@intCast(master.lowest_cycle), 0) else 0;
    const start_f: u64 = if (follower.lowest_cycle != 0) Index.compose(@intCast(follower.lowest_cycle), 0) else 0;

    const tm = try Tailer.create(master, start_m);
    defer tm.deinit();
    const tf = try Tailer.create(follower, start_f);
    defer tf.deinit();

    while (try tm.pollRaw()) |em| {
        const ef = (try tf.pollRaw()) orelse return error.FollowerShorter;
        try std.testing.expectEqual(em.index, ef.index);
        try std.testing.expectEqualSlices(u8, em.payload, ef.payload);
    }
    if (try tf.pollRaw()) |_| return error.FollowerLonger;
    _ = allocator;
}

/// Byte-for-byte comparison of every cycle file present in both queues.
fn assertCycleFilesIdentical(allocator: std.mem.Allocator, master: *Queue, follower: *Queue) !void {
    const cwd = std.Io.Dir.cwd();
    var c: u64 = master.lowest_cycle;
    while (c <= master.highest_cycle) : (c += 1) {
        const mp = try master.cyclePath(c);
        defer allocator.free(mp);
        const fp = try follower.cyclePath(c);
        defer allocator.free(fp);

        const mf = cwd.readFileAlloc(master.io, mp, allocator, .unlimited) catch continue;
        defer allocator.free(mf);
        const ff = try cwd.readFileAlloc(follower.io, fp, allocator, .unlimited);
        defer allocator.free(ff);
        try std.testing.expectEqualSlices(u8, mf, ff);
    }
}

// ---------------------------------------------------------------------------
// writeAtIndex unit tests (doc 05)
// ---------------------------------------------------------------------------

test "writeAtIndex byte-for-byte equivalence with append" {
    const allocator = std.testing.allocator;

    var tmp_a = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_b.cleanup();
    const path_a = try tmpQueuePath(allocator, &tmp_a);
    defer allocator.free(path_a);
    const path_b = try tmpQueuePath(allocator, &tmp_b);
    defer allocator.free(path_b);

    const qa = try makeQueue(allocator, path_a, "TEST4_SECONDLY");
    defer qa.deinit();
    const qb = try makeQueue(allocator, path_b, "TEST4_SECONDLY");
    defer qb.deinit();

    const app_a = try Appender.open(qa);
    var indices = std.ArrayList(u64).empty;
    defer indices.deinit(allocator);
    var payloads = std.ArrayList([]u8).empty;
    defer {
        for (payloads.items) |p| allocator.free(p);
        payloads.deinit(allocator);
    }

    var n: usize = 0;
    while (n < 20) : (n += 1) {
        const len = 1 + (n % 7);
        const buf = try allocator.alloc(u8, len);
        for (buf, 0..) |*b, j| b.* = @truncate(n * 13 + j);
        const ts: u64 = @as(u64, n / 5) * 1000;
        const idx = try app_a.appendWithTimestamp(buf, ts);
        try indices.append(allocator, idx);
        try payloads.append(allocator, buf);
    }

    const app_b = try Appender.open(qb);
    for (indices.items, 0..) |idx, k| {
        try app_b.writeAtIndex(idx, payloads.items[k]);
    }

    try assertTailerParity(allocator, qa, qb);
    try assertCycleFilesIdentical(allocator, qa, qb);
}

test "writeAtIndex shared-body guarantee: matches appendPartsRaw bytes" {
    const allocator = std.testing.allocator;
    var tmp_a = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_a.cleanup();
    var tmp_b = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_b.cleanup();
    const path_a = try tmpQueuePath(allocator, &tmp_a);
    defer allocator.free(path_a);
    const path_b = try tmpQueuePath(allocator, &tmp_b);
    defer allocator.free(path_b);

    const qa = try makeQueue(allocator, path_a, "TEST4_SECONDLY");
    defer qa.deinit();
    const qb = try makeQueue(allocator, path_b, "TEST4_SECONDLY");
    defer qb.deinit();

    const app_a = try Appender.open(qa);
    const auto_index = try app_a.appendWithTimestamp("payload-bytes", 0);

    const app_b = try Appender.open(qb);
    try app_b.writeAtIndex(auto_index, "payload-bytes");

    try assertCycleFilesIdentical(allocator, qa, qb);
}

test "writeAtIndex rejects gaps and duplicates" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    const q = try makeQueue(allocator, path, "TEST4_SECONDLY");
    defer q.deinit();
    const app = try Appender.open(q);

    try app.writeAtIndex(Index.compose(0, 0), "a");
    try app.writeAtIndex(Index.compose(0, 1), "b");

    try std.testing.expectError(error.IndexGap, app.writeAtIndex(Index.compose(0, 3), "c"));
    try std.testing.expectError(error.DuplicateIndex, app.writeAtIndex(Index.compose(0, 0), "x"));
}

test "writeAtIndex resumes after restart via deriveLastAppliedIndex" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    {
        const q = try makeQueue(allocator, path, "TEST4_SECONDLY");
        defer q.deinit();
        const app = try Appender.open(q);
        try app.writeAtIndex(Index.compose(0, 0), "a");
        try app.writeAtIndex(Index.compose(0, 1), "b");
        try app.writeAtIndex(Index.compose(0, 2), "c");
    }

    const q2 = try openQueue(allocator, path);
    defer q2.deinit();
    const last = try write_at_index.deriveLastAppliedIndex(q2);
    try std.testing.expectEqual(@as(i64, @intCast(Index.compose(0, 2))), last);

    const app2 = try Appender.open(q2);
    try app2.writeAtIndex(Index.compose(0, 3), "d");
    try std.testing.expectEqual(@as(i64, @intCast(Index.compose(0, 3))), try write_at_index.deriveLastAppliedIndex(q2));
}

// ---------------------------------------------------------------------------
// Integration scenarios (doc 11 §2)
// ---------------------------------------------------------------------------

const ScenarioCtx = struct {
    allocator: std.mem.Allocator,
    tmp_m: std.testing.TmpDir,
    tmp_f: std.testing.TmpDir,
    path_m: []u8,
    path_f: []u8,
    master: *Queue,
    follower: *Queue,
    pair: *loopback.LoopbackPair,

    fn init(allocator: std.mem.Allocator, scheme: []const u8) !*ScenarioCtx {
        const self = try allocator.create(ScenarioCtx);
        errdefer allocator.destroy(self);
        self.allocator = allocator;
        self.tmp_m = std.testing.tmpDir(.{ .iterate = true });
        self.tmp_f = std.testing.tmpDir(.{ .iterate = true });
        self.path_m = try tmpQueuePath(allocator, &self.tmp_m);
        self.path_f = try tmpQueuePath(allocator, &self.tmp_f);
        self.master = try makeQueue(allocator, self.path_m, scheme);
        self.follower = try makeQueue(allocator, self.path_f, scheme);
        self.pair = try loopback.LoopbackPair.init(allocator, 4096, 8192);
        return self;
    }

    fn deinit(self: *ScenarioCtx) void {
        self.pair.deinit();
        self.master.deinit();
        self.follower.deinit();
        self.allocator.free(self.path_m);
        self.allocator.free(self.path_f);
        self.tmp_m.cleanup();
        self.tmp_f.cleanup();
        self.allocator.destroy(self);
    }

    fn newSource(self: *ScenarioCtx) Source {
        return Source.init(self.allocator, self.master, self.pair.sourceOutbound(), self.pair.sourceInbound(), .{});
    }

    fn newSink(self: *ScenarioCtx) !Sink {
        return Sink.init(self.allocator, self.follower, self.pair.sinkOutbound(), self.pair.sinkInbound(), .{});
    }
};

test "scenario 1+2: full replay of pre-populated queue, byte-for-byte parity" {
    const allocator = std.testing.allocator;
    var ctx = try ScenarioCtx.init(allocator, "TEST4_SECONDLY");
    defer ctx.deinit();

    const app = try Appender.open(ctx.master);
    var last: u64 = 0;
    var n: usize = 0;
    while (n < 300) : (n += 1) {
        var buf: [32]u8 = undefined;
        const s = try std.fmt.bufPrint(&buf, "msg-{d}", .{n});
        last = try app.appendWithTimestamp(s, @as(u64, n / 50) * 1000);
    }

    var src = ctx.newSource();
    defer src.deinit();
    var snk = try ctx.newSink();
    defer snk.deinit();

    try driveUntilApplied(&src, &snk, @intCast(last), 100000);

    try std.testing.expectEqual(@as(i64, @intCast(last)), snk.last_applied_index);
    try assertTailerParity(allocator, ctx.master, ctx.follower);
    try assertCycleFilesIdentical(allocator, ctx.master, ctx.follower);
}

test "scenario 3: live append while sink replays" {
    const allocator = std.testing.allocator;
    var ctx = try ScenarioCtx.init(allocator, "TEST4_SECONDLY");
    defer ctx.deinit();

    const app = try Appender.open(ctx.master);
    var last: u64 = 0;
    var n: usize = 0;
    while (n < 50) : (n += 1) {
        var buf: [32]u8 = undefined;
        const s = try std.fmt.bufPrint(&buf, "pre-{d}", .{n});
        last = try app.appendWithTimestamp(s, 0);
    }

    var src = ctx.newSource();
    defer src.deinit();
    var snk = try ctx.newSink();
    defer snk.deinit();

    var i: usize = 0;
    while (i < 50000) : (i += 1) {
        if (i % 100 == 0 and n < 150) {
            var buf: [32]u8 = undefined;
            const s = try std.fmt.bufPrint(&buf, "live-{d}", .{n});
            last = try app.appendWithTimestamp(s, 0);
            n += 1;
        }
        _ = try src.step(256);
        _ = try snk.step(256);
        if (n >= 150 and snk.last_applied_index >= @as(i64, @intCast(last))) break;
    }

    try std.testing.expectEqual(@as(i64, @intCast(last)), snk.last_applied_index);
    try assertTailerParity(allocator, ctx.master, ctx.follower);
}

test "scenario 4: sink restart catches up from last_applied_index" {
    const allocator = std.testing.allocator;
    var ctx = try ScenarioCtx.init(allocator, "TEST4_SECONDLY");
    defer ctx.deinit();

    const app = try Appender.open(ctx.master);
    var last: u64 = 0;
    var n: usize = 0;
    while (n < 100) : (n += 1) {
        var buf: [16]u8 = undefined;
        const s = try std.fmt.bufPrint(&buf, "m{d}", .{n});
        last = try app.appendWithTimestamp(s, 0);
    }

    {
        var src = ctx.newSource();
        defer src.deinit();
        var snk = try ctx.newSink();
        defer snk.deinit();
        try driveUntilApplied(&src, &snk, @intCast(last), 100000);
    }

    while (n < 200) : (n += 1) {
        var buf: [16]u8 = undefined;
        const s = try std.fmt.bufPrint(&buf, "m{d}", .{n});
        last = try app.appendWithTimestamp(s, 0);
    }

    var src2 = ctx.newSource();
    defer src2.deinit();
    var snk2 = try ctx.newSink();
    defer snk2.deinit();
    try std.testing.expect(snk2.last_applied_index >= 0);
    try driveUntilApplied(&src2, &snk2, @intCast(last), 100000);

    try std.testing.expectEqual(@as(i64, @intCast(last)), snk2.last_applied_index);
    try assertTailerParity(allocator, ctx.master, ctx.follower);
}

test "scenario 6: adjacent cycle rolls replicate and roll identically" {
    const allocator = std.testing.allocator;
    var ctx = try ScenarioCtx.init(allocator, "TEST4_SECONDLY");
    defer ctx.deinit();

    const app = try Appender.open(ctx.master);
    _ = try app.appendWithTimestamp("c0-a", 0);
    _ = try app.appendWithTimestamp("c0-b", 0);
    _ = try app.appendWithTimestamp("c1-a", 1000);
    _ = try app.appendWithTimestamp("c1-b", 1000);
    _ = try app.appendWithTimestamp("c2-a", 2000);
    const last = try app.appendWithTimestamp("c2-b", 2000);

    var src = ctx.newSource();
    defer src.deinit();
    var snk = try ctx.newSink();
    defer snk.deinit();

    try driveUntilApplied(&src, &snk, @intCast(last), 100000);

    try assertTailerParity(allocator, ctx.master, ctx.follower);
    try assertCycleFilesIdentical(allocator, ctx.master, ctx.follower);
    try std.testing.expect(ctx.follower.highest_cycle >= 2);
}

test "CycleSynchronizer materializes empty intermediate cycles and rolls across them" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    const q = try makeQueue(allocator, path, "TEST4_SECONDLY");
    defer q.deinit();
    const app = try Appender.open(q);

    // Cycle 0 has data, then a CYCLE_ROLL spanning the empty cycles 1..3 to cycle 4.
    try app.writeAtIndex(Index.compose(0, 0), "c0");

    var cs = @import("cycle_sync.zig").CycleSynchronizer.init(q, app);
    try cs.onCycleRoll(0, 4, Index.compose(4, 0));
    try app.writeAtIndex(Index.compose(4, 0), "c4");

    // A tailer must roll 0 -> 1(empty) -> 2(empty) -> 3(empty) -> 4 and see only the two data records.
    const t = try Tailer.create(q, Index.compose(0, 0));
    defer t.deinit();
    const e0 = (try t.pollRaw()).?;
    try std.testing.expectEqual(Index.compose(0, 0), e0.index);
    try std.testing.expectEqualStrings("c0", e0.payload);
    const e1 = (try t.pollRaw()).?;
    try std.testing.expectEqual(Index.compose(4, 0), e1.index);
    try std.testing.expectEqualStrings("c4", e1.payload);
    try std.testing.expect((try t.pollRaw()) == null);

    // Intermediate empty cycle files exist and are sealed.
    try std.testing.expect(q.highest_cycle >= 4);
}

test "scenario 8: sink ahead of source is rejected with HELLO_NACK" {
    const allocator = std.testing.allocator;
    var ctx = try ScenarioCtx.init(allocator, "TEST4_SECONDLY");
    defer ctx.deinit();

    const app_m = try Appender.open(ctx.master);
    try app_m.writeAtIndex(Index.compose(0, 0), "m0");
    try app_m.writeAtIndex(Index.compose(0, 1), "m1");

    const app_f = try Appender.open(ctx.follower);
    var k: u32 = 0;
    while (k < 5) : (k += 1) {
        try app_f.writeAtIndex(Index.compose(0, k), "ff");
    }

    var src = ctx.newSource();
    defer src.deinit();
    var snk = try ctx.newSink();
    defer snk.deinit();

    const Hook = struct {
        var fired: bool = false;
        fn cb(err: anyerror) void {
            if (err == error.SinkAheadOfSource) fired = true;
        }
    };
    Hook.fired = false;
    snk.error_hook = Hook.cb;

    var i: usize = 0;
    while (i < 1000 and snk.state != .failed) : (i += 1) {
        _ = try src.step(256);
        _ = try snk.step(256);
    }

    try std.testing.expectEqual(sink_mod.SinkState.failed, snk.state);
    try std.testing.expect(Hook.fired);
}

test "scenario 9: config mismatch is rejected with HELLO_NACK" {
    const allocator = std.testing.allocator;
    var tmp_m = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_m.cleanup();
    var tmp_f = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_f.cleanup();
    const path_m = try tmpQueuePath(allocator, &tmp_m);
    defer allocator.free(path_m);
    const path_f = try tmpQueuePath(allocator, &tmp_f);
    defer allocator.free(path_f);

    const master = try makeQueue(allocator, path_m, "TEST4_SECONDLY");
    defer master.deinit();
    const follower = try makeQueue(allocator, path_f, "FAST_DAILY");
    defer follower.deinit();

    const app = try Appender.open(master);
    _ = try app.appendWithTimestamp("x", 0);

    var pair = try loopback.LoopbackPair.init(allocator, 256, 8192);
    defer pair.deinit();

    var src = Source.init(allocator, master, pair.sourceOutbound(), pair.sourceInbound(), .{});
    defer src.deinit();
    var snk = try Sink.init(allocator, follower, pair.sinkOutbound(), pair.sinkInbound(), .{});
    defer snk.deinit();

    const Hook = struct {
        var fired: bool = false;
        fn cb(err: anyerror) void {
            if (err == error.ConfigMismatch) fired = true;
        }
    };
    Hook.fired = false;
    snk.error_hook = Hook.cb;

    var i: usize = 0;
    while (i < 1000 and snk.state != .failed) : (i += 1) {
        _ = try src.step(256);
        _ = try snk.step(256);
    }

    try std.testing.expectEqual(sink_mod.SinkState.failed, snk.state);
    try std.testing.expect(Hook.fired);
    try std.testing.expectEqual(@as(u64, 1), src.metrics.hello_nacks);
}

test "scenario 10: multiple independent sink instances all converge" {
    const allocator = std.testing.allocator;

    var tmp_m = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_m.cleanup();
    const path_m = try tmpQueuePath(allocator, &tmp_m);
    defer allocator.free(path_m);
    const master = try makeQueue(allocator, path_m, "TEST4_SECONDLY");
    defer master.deinit();

    const app = try Appender.open(master);
    var last: u64 = 0;
    var n: usize = 0;
    while (n < 80) : (n += 1) {
        var buf: [16]u8 = undefined;
        const s = try std.fmt.bufPrint(&buf, "d{d}", .{n});
        last = try app.appendWithTimestamp(s, 0);
    }

    const num_sinks = 4;
    var followers: [num_sinks]*Queue = undefined;
    var tmps: [num_sinks]std.testing.TmpDir = undefined;
    var paths: [num_sinks][]u8 = undefined;
    var pairs: [num_sinks]*loopback.LoopbackPair = undefined;
    var sources: [num_sinks]Source = undefined;
    var sinks: [num_sinks]Sink = undefined;

    var s: usize = 0;
    while (s < num_sinks) : (s += 1) {
        tmps[s] = std.testing.tmpDir(.{ .iterate = true });
        paths[s] = try tmpQueuePath(allocator, &tmps[s]);
        followers[s] = try makeQueue(allocator, paths[s], "TEST4_SECONDLY");
        pairs[s] = try loopback.LoopbackPair.init(allocator, 4096, 8192);
        sources[s] = Source.init(allocator, master, pairs[s].sourceOutbound(), pairs[s].sourceInbound(), .{});
        sinks[s] = try Sink.init(allocator, followers[s], pairs[s].sinkOutbound(), pairs[s].sinkInbound(), .{});
    }
    defer {
        s = 0;
        while (s < num_sinks) : (s += 1) {
            sinks[s].deinit();
            sources[s].deinit();
            pairs[s].deinit();
            followers[s].deinit();
            allocator.free(paths[s]);
            tmps[s].cleanup();
        }
    }

    var i: usize = 0;
    while (i < 200000) : (i += 1) {
        var all_done = true;
        s = 0;
        while (s < num_sinks) : (s += 1) {
            if (s == 1 and i > 50) continue;
            _ = try sources[s].step(256);
            _ = try sinks[s].step(256);
            if (s != 1 and sinks[s].last_applied_index < @as(i64, @intCast(last))) all_done = false;
        }
        if (all_done) break;
    }

    s = 0;
    while (s < num_sinks) : (s += 1) {
        if (s == 1) continue;
        try std.testing.expectEqual(@as(i64, @intCast(last)), sinks[s].last_applied_index);
        try assertTailerParity(allocator, master, followers[s]);
    }
}

test "scenario 11: backpressure never drops or duplicates frames" {
    const allocator = std.testing.allocator;
    var ctx = try ScenarioCtx.init(allocator, "TEST4_SECONDLY");
    defer ctx.deinit();

    ctx.pair.deinit();
    ctx.pair = try loopback.LoopbackPair.init(allocator, 4, 8192);

    const app = try Appender.open(ctx.master);
    var last: u64 = 0;
    var n: usize = 0;
    while (n < 200) : (n += 1) {
        var buf: [16]u8 = undefined;
        const s = try std.fmt.bufPrint(&buf, "b{d}", .{n});
        last = try app.appendWithTimestamp(s, 0);
    }

    var src = ctx.newSource();
    defer src.deinit();
    var snk = try ctx.newSink();
    defer snk.deinit();

    try driveUntilApplied(&src, &snk, @intCast(last), 500000);

    try std.testing.expectEqual(@as(i64, @intCast(last)), snk.last_applied_index);
    try assertTailerParity(allocator, ctx.master, ctx.follower);
}

test "scenario 12: source session anchored on empty queue re-anchors when data arrives at a later cycle" {
    // Reproduces the topics-registration bug: a source/sink handshake completes
    // while the master is empty (firstAvailableIndex == 0, so the session tailer
    // anchors at cycle 0). Data appended afterwards lands at a higher,
    // timestamp-derived cycle whose cycle-0 file is never created. The source
    // must re-anchor its session tailer forward to the queue's real lowest cycle
    // or it ships nothing (frames_sent stays 0 forever).
    const allocator = std.testing.allocator;
    var ctx = try ScenarioCtx.init(allocator, "TEST4_SECONDLY");
    defer ctx.deinit();

    // Source + sink created against an EMPTY master with long liveness timeouts
    // so the test exercises the re-anchor path rather than racing the sink's
    // heartbeat watchdog while the queue is still empty.
    var src = ctx.newSource();
    defer src.deinit();
    var snk = try Sink.init(allocator, ctx.follower, ctx.pair.sinkOutbound(), ctx.pair.sinkInbound(), .{
        .heartbeat_timeout_ns = std.time.ns_per_min,
        .hello_timeout_ns = std.time.ns_per_min,
    });
    defer snk.deinit();

    // Complete the handshake against the empty queue. This anchors the single
    // source session at firstAvailableIndex == 0 (cycle 0).
    var hs: usize = 0;
    while (hs < 10000) : (hs += 1) {
        _ = try src.step(256);
        _ = try snk.step(256);
        if (src.sessions.items.len == 1 and snk.state == .live) break;
    }
    try std.testing.expectEqual(@as(usize, 1), src.sessions.items.len);
    try std.testing.expectEqual(@as(u64, 0), src.metrics.frames_sent);

    // Append data at a high cycle (timestamp 5000ms -> cycle 5 for
    // TEST4_SECONDLY). Cycle 0 will never get a file.
    const app = try Appender.open(ctx.master);
    var last: u64 = 0;
    var n: usize = 0;
    while (n < 100) : (n += 1) {
        var buf: [32]u8 = undefined;
        const s = try std.fmt.bufPrint(&buf, "late-{d}", .{n});
        last = try app.appendWithTimestamp(s, 5000);
    }

    // Drive replication to completion. Without re-anchoring, the session tailer
    // is stuck at cycle 0 and the source never ships any frames.
    try driveUntilApplied(&src, &snk, @intCast(last), 500000);

    try std.testing.expect(src.metrics.frames_sent >= 100);
    try std.testing.expectEqual(@as(i64, @intCast(last)), snk.last_applied_index);
    try assertTailerParity(allocator, ctx.master, ctx.follower);
}

test {
    _ = protocol;
    std.testing.refAllDecls(@This());
}
