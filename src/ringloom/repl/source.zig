// SPDX-License-Identifier: Apache-2.0

//! Replication source (master side): tails the master queue and ships excerpts
//! as a pollable, comptime-generic state machine.
//!
//! `ReplicationSource(Outbound, Inbound)` is generic over the concrete transport
//! channel types so `offer`/`poll`/`nextFrame` bind statically and inline — no
//! runtime vtable on the hot path (doc 04).

const std = @import("std");

const protocol = @import("protocol.zig");
const codec = @import("codec.zig");
const transport = @import("transport.zig");
const session = @import("session.zig");
const write_at_index = @import("write_at_index.zig");

const FrameCodec = codec.FrameCodec;
const OfferResult = transport.OfferResult;
const Queue = @import("../queue.zig").Queue;
const Tailer = @import("../tailer.zig").Tailer;
const Index = @import("../index.zig").Index;
const StepResult = @import("../platform.zig").StepResult;

pub const SourceState = enum { new, starting, awaiting_hello, syncing, live, closing, closed, failed };

pub const SourceMetrics = struct {
    hwm_index: u64 = 0,
    frames_sent: u64 = 0,
    bytes_sent: u64 = 0,
    backpressure_nanos: u64 = 0,
    active_sessions: u32 = 0,
    cycles_rolled: u64 = 0,
    hello_nacks: u64 = 0,
    decode_errors: u64 = 0,
    unexpected_frames: u64 = 0,
};

pub const SourceConfig = struct {
    node_salt: u32 = 1,
    max_frame_bytes: usize = 2 * 1024 * 1024,
    control_fragment_limit: u32 = 16,
    heartbeat_interval_ns: u64 = 100 * std.time.ns_per_ms,
    backpressure_watchdog_ns: u64 = 30 * std.time.ns_per_s,
    backpressure_fatal_ns: u64 = 0, // 0 = disabled
};

pub fn ReplicationSource(comptime Outbound: type, comptime Inbound: type) type {
    comptime transport.AssertOutbound(Outbound);
    comptime transport.AssertInbound(Inbound);

    return struct {
        const Self = @This();

        pub const Session = struct {
            id: u64,
            state: SourceState = .new,
            tailer: *Tailer,
            mode: protocol.Mode,
            prev_cycle: ?u32 = null,
            hwm_index: u64 = 0,
            last_ack_index: u64 = 0,
            last_frame_sent_ns: u64 = 0,
            backpressure_since_ns: ?u64 = null,
            send_buf: []u8,
            pending_len: usize = 0, // one-slot retry buffer (0 = empty)
        };

        allocator: std.mem.Allocator,
        queue: *Queue,
        outbound: *Outbound,
        inbound: *Inbound,
        local_config: session.QueueConfigId,
        id_gen: session.SessionIdGen,
        cfg: SourceConfig,
        sessions: std.ArrayList(Session) = .empty,
        metrics: SourceMetrics = .{},
        now_override_ns: ?u64 = null,
        error_hook: ?*const fn (session_id: u64, err: anyerror) void = null,

        pub fn init(
            allocator: std.mem.Allocator,
            queue: *Queue,
            outbound: *Outbound,
            inbound: *Inbound,
            cfg: SourceConfig,
        ) Self {
            return .{
                .allocator = allocator,
                .queue = queue,
                .outbound = outbound,
                .inbound = inbound,
                .local_config = session.QueueConfigId.fromQueue(queue),
                .id_gen = session.SessionIdGen.init(cfg.node_salt),
                .cfg = cfg,
            };
        }

        pub fn deinit(self: *Self) void {
            for (self.sessions.items) |*s| {
                s.tailer.deinit();
                self.allocator.free(s.send_buf);
            }
            self.sessions.deinit(self.allocator);
            self.* = undefined;
        }

        fn now(self: *const Self) u64 {
            if (self.now_override_ns) |v| return v;
            const ns = std.Io.Clock.awake.now(self.queue.io).toNanoseconds();
            return if (ns < 0) 0 else @intCast(ns);
        }

        /// One bounded, non-blocking pass.
        pub fn step(self: *Self, max_work_units: u32) !StepResult {
            var progressed = false;

            const n = self.inbound.poll(self.cfg.control_fragment_limit);
            if (n > 0) {
                progressed = true;
                while (self.inbound.nextFrame()) |frame| self.handleControlFrame(frame);
            }

            for (self.sessions.items) |*s| {
                if (s.state != .live and s.state != .syncing) continue;
                const did = self.pumpSession(s, max_work_units) catch |err| {
                    self.fail(s, err);
                    continue;
                };
                progressed = progressed or did;
            }

            try self.maybeHeartbeats(self.now());

            self.metrics.active_sessions = @intCast(self.sessions.items.len);
            return if (progressed) .made_progress else .idle;
        }

        fn pumpSession(self: *Self, s: *Session, budget: u32) !bool {
            var produced = false;
            var i: u32 = 0;

            // Retry a previously back-pressured frame before pulling new data.
            if (s.pending_len != 0) {
                if (!self.offer(s, s.send_buf[0..s.pending_len])) return false;
                s.pending_len = 0;
                produced = true;
            }

            while (i < budget) : (i += 1) {
                const raw = (try s.tailer.pollRaw()) orelse break;
                const cyc: u32 = @intCast(Index.cycle(raw.index));

                if (s.prev_cycle) |pc| {
                    if (cyc != pc) {
                        if (!try self.offerCycleRoll(s, pc, cyc)) return produced;
                    }
                }

                const flags: u32 = if (s.mode != .live) protocol.Flags.CATCHUP else 0;
                const len = try FrameCodec.encodeExcerpt(s.send_buf, s.id, flags, raw.index, raw.payload);
                if (!self.offer(s, s.send_buf[0..len])) {
                    // Hold the un-offered excerpt for retry; do not advance.
                    s.pending_len = len;
                    return produced;
                }

                s.prev_cycle = cyc;
                s.hwm_index = raw.index;
                if (raw.index > self.metrics.hwm_index) self.metrics.hwm_index = raw.index;
                self.metrics.frames_sent += 1;
                self.metrics.bytes_sent += raw.payload.len;
                produced = true;
            }

            // Re-anchor a session whose tailer is stuck below the queue's
            // current lowest cycle. This happens when the session was created
            // against an empty queue (firstAvailableIndex == 0, cycle 0) and
            // data later arrives at a higher, date-derived cycle: the tailer
            // blocks on a cycle file that is never created. Seek it forward to
            // the queue's real first available cycle so live data is picked up.
            //
            // prev_cycle is left untouched: the session has sent no excerpts
            // yet, so emitting a CYCLE_ROLL would force the sink to materialize
            // every intermediate empty cycle (potentially tens of thousands of
            // files for date-derived cycles). A sink that has applied nothing
            // re-bases its expected_next to the first real excerpt (sink.applyOne).
            if (!produced and s.tailer.state == .awaiting_queue_file) {
                const tailer_cycle = Index.cycle(s.tailer.qf_index);
                const lowest_cycle: u32 = @intCast(self.queue.lowest_cycle);
                if (lowest_cycle > tailer_cycle) {
                    s.tailer.seekTo(Index.compose(lowest_cycle, 0)) catch |err| {
                        std.log.scoped(.repl_src).warn("re-anchor seekTo cycle {d} failed: {}", .{ lowest_cycle, err });
                    };
                }
            }

            if (!produced and s.mode != .live and s.state == .syncing) {
                s.mode = .live;
                s.state = .live;
            }
            return produced;
        }


        fn offerCycleRoll(self: *Self, s: *Session, from_cycle: u32, to_cycle: u32) !bool {
            const next_expected = Index.compose(to_cycle, 0);
            const len = try FrameCodec.encodeCycleRoll(s.send_buf, s.id, from_cycle, to_cycle, next_expected);
            if (!self.offer(s, s.send_buf[0..len])) {
                s.pending_len = len;
                return false;
            }
            self.metrics.cycles_rolled += 1;
            return true;
        }

        fn offer(self: *Self, s: *Session, frame: []const u8) bool {
            const rc = self.outbound.offer(frame);
            if (OfferResult.accepted(rc)) {
                s.last_frame_sent_ns = self.now();
                if (s.backpressure_since_ns) |t| {
                    self.metrics.backpressure_nanos += self.now() - t;
                    s.backpressure_since_ns = null;
                }
                return true;
            }
            switch (@as(OfferResult, @enumFromInt(rc))) {
                .back_pressured => {
                    if (s.backpressure_since_ns == null) s.backpressure_since_ns = self.now();
                    self.checkBackpressureWatchdog(s);
                    return false;
                },
                .not_connected, .closed => {
                    self.teardown(s);
                    return false;
                },
                else => {
                    self.fail(s, error.TransportError);
                    return false;
                },
            }
        }

        fn checkBackpressureWatchdog(self: *Self, s: *Session) void {
            const since = s.backpressure_since_ns orelse return;
            const stalled = self.now() - since;
            if (self.cfg.backpressure_fatal_ns != 0 and stalled >= self.cfg.backpressure_fatal_ns) {
                self.teardown(s);
            } else if (stalled >= self.cfg.backpressure_watchdog_ns) {
                if (self.error_hook) |hook| hook(s.id, error.BackpressureWatchdog);
            }
        }

        fn maybeHeartbeats(self: *Self, now_ns: u64) !void {
            for (self.sessions.items) |*s| {
                if (s.state != .live and s.state != .syncing) continue;
                if (s.pending_len != 0) continue;
                if (now_ns - s.last_frame_sent_ns >= self.cfg.heartbeat_interval_ns) {
                    const len = try FrameCodec.encodeHeartbeat(s.send_buf, s.id, s.hwm_index, now_ns);
                    _ = self.offer(s, s.send_buf[0..len]);
                }
            }
        }

        // ---- control-frame dispatch (pull model) ----------------------------

        fn handleControlFrame(self: *Self, frame: []const u8) void {
            const hdr = FrameCodec.peekHeader(frame) catch {
                self.metrics.decode_errors += 1;
                return;
            };
            switch (hdr.frame_type) {
                .hello => self.onHello(FrameCodec.decodeHello(frame) catch {
                    self.metrics.decode_errors += 1;
                    return;
                }),
                .ack => self.onAck(hdr.session_id, FrameCodec.decodeAck(frame) catch return),
                .reset => self.onReset(hdr.session_id),
                .close => self.onClose(hdr.session_id),
                else => self.metrics.unexpected_frames += 1,
            }
        }

        fn onHello(self: *Self, h: protocol.HelloView) void {
            if (self.local_config.checkCompatible(h)) |reason| {
                self.sendNack(reason);
                self.metrics.hello_nacks += 1;
                return;
            }

            const first = self.firstAvailableIndex();
            const last = self.lastIndex() catch {
                self.sendNack(.internal_error);
                return;
            };

            var mode: protocol.Mode = undefined;
            var start: u64 = undefined;
            const la = h.last_applied_index;
            if (la < 0) {
                mode = .full_replay;
                start = first;
            } else if (last < 0 or @as(u64, @intCast(la)) > @as(u64, @intCast(last))) {
                self.sendNack(.sink_ahead_of_source);
                self.metrics.hello_nacks += 1;
                return;
            } else {
                const next = @as(u64, @intCast(la)) + 1;
                if (next < first) {
                    self.sendNack(.index_not_available);
                    self.metrics.hello_nacks += 1;
                    return;
                }
                mode = if (next > @as(u64, @intCast(last))) .live else .catchup;
                start = next;
            }

            const s = self.newSession(start, mode) catch {
                self.sendNack(.internal_error);
                return;
            };
            self.sendHelloAck(s, first, if (last < 0) 0 else @intCast(last), mode);
            s.state = if (mode == .live) .live else .syncing;
        }

        fn onAck(self: *Self, session_id: u64, a: protocol.AckFrame) void {
            const s = self.findSession(session_id) orelse return;
            s.last_ack_index = a.last_applied_index;
        }

        fn onReset(self: *Self, session_id: u64) void {
            const s = self.findSession(session_id) orelse return;
            self.teardown(s);
        }

        fn onClose(self: *Self, session_id: u64) void {
            const s = self.findSession(session_id) orelse return;
            self.teardown(s);
        }

        // ---- session management ---------------------------------------------

        fn newSession(self: *Self, start: u64, mode: protocol.Mode) !*Session {
            const send_buf = try self.allocator.alloc(u8, self.cfg.max_frame_bytes);
            errdefer self.allocator.free(send_buf);
            const tailer = try Tailer.create(self.queue, start);
            errdefer tailer.deinit();

            try self.sessions.append(self.allocator, .{
                .id = self.id_gen.next(),
                .tailer = tailer,
                .mode = mode,
                .send_buf = send_buf,
            });
            return &self.sessions.items[self.sessions.items.len - 1];
        }

        fn findSession(self: *Self, session_id: u64) ?*Session {
            for (self.sessions.items) |*s| {
                if (s.id == session_id) return s;
            }
            return null;
        }

        fn teardown(self: *Self, s: *Session) void {
            const idx = self.indexOf(s) orelse return;
            s.tailer.deinit();
            self.allocator.free(s.send_buf);
            _ = self.sessions.swapRemove(idx);
        }

        fn fail(self: *Self, s: *Session, err: anyerror) void {
            if (self.error_hook) |hook| hook(s.id, err);
            self.teardown(s);
        }

        fn indexOf(self: *Self, s: *Session) ?usize {
            for (self.sessions.items, 0..) |*item, i| {
                if (item == s) return i;
            }
            return null;
        }

        // ---- control-frame senders ------------------------------------------

        fn sendNack(self: *Self, reason: protocol.NackReason) void {
            var buf: [128]u8 = undefined;
            const len = FrameCodec.encodeHelloNack(&buf, reason, "") catch return;
            _ = self.outbound.offer(buf[0..len]);
        }

        fn sendHelloAck(self: *Self, s: *Session, first: u64, last: u64, mode: protocol.Mode) void {
            const len = FrameCodec.encodeHelloAck(s.send_buf, .{
                .session_id = s.id,
                .source_first_available_index = first,
                .source_last_index = last,
                .mode = mode,
            }) catch return;
            _ = self.outbound.offer(s.send_buf[0..len]);
            s.last_frame_sent_ns = self.now();
        }

        // ---- queue position helpers -----------------------------------------

        fn firstAvailableIndex(self: *Self) u64 {
            return if (self.queue.lowest_cycle != 0)
                Index.compose(@intCast(self.queue.lowest_cycle), 0)
            else
                0;
        }

        fn lastIndex(self: *Self) !i64 {
            return write_at_index.deriveLastAppliedIndex(self.queue);
        }
    };
}

test "ReplicationSource compiles for the loopback transport" {
    const Loopback = @import("loopback.zig").Loopback;
    const S = ReplicationSource(Loopback.Outbound, Loopback.Inbound);
    try std.testing.expect(@hasDecl(S, "step"));
}
