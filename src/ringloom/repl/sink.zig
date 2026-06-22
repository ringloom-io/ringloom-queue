// SPDX-License-Identifier: Apache-2.0

//! Replication sink (follower side): applies excerpts via `writeAtIndex` as a
//! pollable, comptime-generic state machine. It is the single writer of the
//! follower queue.

const std = @import("std");

const protocol = @import("protocol.zig");
const codec = @import("codec.zig");
const transport = @import("transport.zig");
const session = @import("session.zig");
const cycle_sync = @import("cycle_sync.zig");
const write_at_index = @import("write_at_index.zig");

const FrameCodec = codec.FrameCodec;
const Queue = @import("../queue.zig").Queue;
const Appender = @import("../appender.zig").Appender;
const Index = @import("../index.zig").Index;
const StepResult = @import("../platform.zig").StepResult;

pub const SinkState = enum {
    new,
    starting,
    connecting,
    awaiting_hello_ack,
    syncing,
    live,
    resetting,
    closing,
    closed,
    failed,
};

pub const SinkMetrics = struct {
    last_applied_index: i64 = -1,
    frames_applied: u64 = 0,
    bytes_applied: u64 = 0,
    replay_reset_count: u64 = 0,
    current_session_id: u64 = 0,
    lag_from_source_hwm: u64 = 0,
    gaps_detected: u64 = 0,
    decode_errors: u64 = 0,
};

pub const SinkConfig = struct {
    node_id: [16]u8 = [_]u8{0} ** 16,
    queue_id: [16]u8 = [_]u8{0} ** 16,
    max_frame_bytes: usize = 2 * 1024 * 1024,
    ack_interval_ns: u64 = 50 * std.time.ns_per_ms,
    force_ack_interval_ns: u64 = 1 * std.time.ns_per_s,
    heartbeat_timeout_ns: u64 = 500 * std.time.ns_per_ms,
    hello_timeout_ns: u64 = 5 * std.time.ns_per_s,
    backoff: session.BackoffPolicy = .{},
};

pub fn ReplicationSink(comptime Outbound: type, comptime Inbound: type) type {
    comptime transport.AssertOutbound(Outbound);
    comptime transport.AssertInbound(Inbound);

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        queue: *Queue,
        appender: *Appender,
        cycle_sync: cycle_sync.CycleSynchronizer,
        outbound: *Outbound,
        inbound: *Inbound,
        local_config: session.QueueConfigId,
        cfg: SinkConfig,

        state: SinkState = .new,
        session_id: u64 = 0,
        last_applied_index: i64 = -1,
        expected_next: u64 = 0,
        last_ack_sent_ns: u64 = 0,
        last_ack_index: i64 = -1,
        last_inbound_ns: u64 = 0,
        hello_sent_ns: u64 = 0,
        source_hwm: u64 = 0,
        reconnect: session.BackoffState,
        scratch: []u8,
        metrics: SinkMetrics = .{},
        now_override_ns: ?u64 = null,
        error_hook: ?*const fn (err: anyerror) void = null,

        pub fn init(allocator: std.mem.Allocator, queue: *Queue, outbound: *Outbound, inbound: *Inbound, cfg: SinkConfig) !Self {
            const appender = try queue.openAppender();
            errdefer appender.deinit();
            const scratch = try allocator.alloc(u8, cfg.max_frame_bytes);
            errdefer allocator.free(scratch);

            var self = Self{
                .allocator = allocator,
                .queue = queue,
                .appender = appender,
                .cycle_sync = cycle_sync.CycleSynchronizer.init(queue, appender),
                .outbound = outbound,
                .inbound = inbound,
                .local_config = session.QueueConfigId.fromQueue(queue),
                .cfg = cfg,
                .reconnect = .{ .policy = cfg.backoff },
                .scratch = scratch,
            };
            self.last_applied_index = try write_at_index.deriveLastAppliedIndex(queue);
            self.expected_next = self.computeExpectedNext();
            self.metrics.last_applied_index = self.last_applied_index;
            self.state = .starting;
            return self;
        }

        pub fn deinit(self: *Self) void {
            self.appender.deinit();
            self.allocator.free(self.scratch);
            self.* = undefined;
        }

        fn now(self: *const Self) u64 {
            if (self.now_override_ns) |v| return v;
            const ns = std.Io.Clock.awake.now(self.queue.io).toNanoseconds();
            return if (ns < 0) 0 else @intCast(ns);
        }

        fn computeExpectedNext(self: *Self) u64 {
            if (self.last_applied_index < 0) {
                return if (self.queue.lowest_cycle != 0)
                    Index.compose(@intCast(self.queue.lowest_cycle), 0)
                else
                    0;
            }
            return @as(u64, @intCast(self.last_applied_index)) + 1;
        }

        /// One bounded, non-blocking pass.
        pub fn step(self: *Self, max_work_units: u32) !StepResult {
            switch (self.state) {
                .starting, .connecting => {
                    try self.tryConnectAndHello();
                    return .made_progress;
                },
                .resetting => {
                    self.beginReconnect();
                    return .made_progress;
                },
                .closed, .failed => return .idle,
                else => {},
            }

            const n = self.inbound.poll(max_work_units);
            if (n > 0) {
                self.last_inbound_ns = self.now();
                while (self.inbound.nextFrame()) |frame| self.applyFrame(frame);
            }

            if (self.state == .awaiting_hello_ack and
                self.now() - self.hello_sent_ns > self.cfg.hello_timeout_ns)
            {
                self.state = .resetting;
            }

            if ((self.state == .live or self.state == .syncing) and
                self.now() - self.last_inbound_ns > self.cfg.heartbeat_timeout_ns)
            {
                self.requestReset(.operator_request);
            }

            try self.maybeAck(self.now());

            return if (n > 0) .made_progress else .idle;
        }

        fn tryConnectAndHello(self: *Self) !void {
            if (!self.outbound.isConnected()) {
                self.reconnect.schedule(self.now());
                self.state = .connecting;
                return;
            }
            const h = protocol.HelloFrame{
                .sink_node_id = self.cfg.node_id,
                .queue_id = self.cfg.queue_id,
                .last_applied_index = self.last_applied_index,
                .epoch_ms = self.local_config.epoch_ms,
                .roll_length_secs = self.local_config.roll_length_secs,
                .index_count = self.local_config.index_count,
                .index_spacing = self.local_config.index_spacing,
                .block_size = self.local_config.block_size,
                .format_version = self.local_config.format_version,
                .roll_name = self.local_config.roll_name,
            };
            const len = try FrameCodec.encodeHello(self.scratch, h);
            _ = self.outbound.offer(self.scratch[0..len]);
            self.state = .awaiting_hello_ack;
            self.hello_sent_ns = self.now();
        }

        fn beginReconnect(self: *Self) void {
            if (!self.reconnect.ready(self.now())) return;
            // Re-derive from the local queue in case of a crash between write and reset.
            self.last_applied_index = write_at_index.deriveLastAppliedIndex(self.queue) catch self.last_applied_index;
            self.expected_next = self.computeExpectedNext();
            self.session_id = 0;
            self.state = .connecting;
        }

        // ---- frame apply path (pull model) ----------------------------------

        fn applyFrame(self: *Self, frame: []const u8) void {
            const hdr = FrameCodec.readHeader(frame) catch {
                self.metrics.decode_errors += 1;
                self.requestReset(.corrupt_frame);
                return;
            };

            if (self.session_id != 0 and hdr.session_id != self.session_id and
                hdr.frame_type != .hello_ack and hdr.frame_type != .hello_nack) return;

            switch (hdr.frame_type) {
                .hello_ack => self.onHelloAck(frame),
                .hello_nack => self.onHelloNack(frame),
                .excerpt => self.applyExcerpt(frame),
                .excerpt_batch => self.applyBatch(frame),
                .cycle_roll => self.applyCycleRoll(frame),
                .heartbeat => self.onHeartbeat(frame),
                .close => self.onClose(),
                else => {},
            }
        }

        fn onHelloAck(self: *Self, frame: []const u8) void {
            const a = FrameCodec.decodeHelloAck(frame) catch {
                self.metrics.decode_errors += 1;
                return;
            };
            if (self.session_id != 0 and a.session_id != self.session_id) {
                // A second source for an already-active session: ignore.
                if (self.error_hook) |hook| hook(error.SecondSource);
                return;
            }
            self.session_id = a.session_id;
            self.metrics.current_session_id = a.session_id;
            self.reconnect.reset();
            // A fresh follower (nothing applied yet) anchors to wherever the
            // source begins streaming, which need not be index 0 (the master's
            // earliest retained cycle may be non-zero).
            if (self.last_applied_index < 0) {
                self.expected_next = a.source_first_available_index;
            }
            self.state = if (a.mode == .live) .live else .syncing;
            self.last_inbound_ns = self.now();
        }

        fn onHelloNack(self: *Self, frame: []const u8) void {
            const nack = FrameCodec.decodeHelloNack(frame) catch {
                self.metrics.decode_errors += 1;
                return;
            };
            // Fatal: never silently overwrite or reset the local queue.
            if (self.error_hook) |hook| hook(nackError(nack.reason));
            self.state = .failed;
        }

        fn applyExcerpt(self: *Self, frame: []const u8) void {
            const e = FrameCodec.decodeExcerpt(frame) catch {
                self.requestReset(.corrupt_frame);
                return;
            };
            self.applyOne(e.index, e.payload);
        }

        fn applyBatch(self: *Self, frame: []const u8) void {
            var view = FrameCodec.decodeExcerptBatch(frame) catch {
                self.requestReset(.corrupt_frame);
                return;
            };
            while (view.next()) |rec| {
                if (self.state != .live and self.state != .syncing) return;
                self.applyOne(rec.index, rec.payload);
            }
        }

        fn applyOne(self: *Self, index: u64, payload: []const u8) void {
            if (self.last_applied_index >= 0 and index <= @as(u64, @intCast(self.last_applied_index))) return;
            if (index != self.expected_next) {
                // A fresh replica that has applied nothing may receive its first
                // excerpt at a cycle the empty-queue HELLO_ACK could not predict
                // (the source's queue was empty at handshake and data later
                // arrived at a date-derived cycle). With nothing applied there is
                // no divergence risk, so anchor to the source's real start instead
                // of resetting — but only at a cycle boundary, never mid-cycle.
                if (self.last_applied_index < 0 and Index.seqnum(index) == 0) {
                    self.expected_next = index;
                } else {
                    self.metrics.gaps_detected += 1;
                    self.requestReset(.gap_detected);
                    return;
                }
            }
            self.appender.writeAtIndex(index, payload) catch |err| {
                switch (err) {
                    error.DuplicateIndex => return,
                    error.IndexGap => {
                        self.metrics.gaps_detected += 1;
                        self.requestReset(.gap_detected);
                        return;
                    },
                    else => {
                        if (self.error_hook) |hook| hook(err);
                        self.requestReset(.corrupt_frame);
                        return;
                    },
                }
            };
            self.last_applied_index = @intCast(index);
            self.expected_next = index + 1;
            self.metrics.last_applied_index = self.last_applied_index;
            self.metrics.frames_applied += 1;
            self.metrics.bytes_applied += payload.len;
        }

        fn applyCycleRoll(self: *Self, frame: []const u8) void {
            const cr = FrameCodec.decodeCycleRoll(frame) catch {
                self.requestReset(.corrupt_frame);
                return;
            };
            self.cycle_sync.onCycleRoll(cr.from_cycle, cr.to_cycle, cr.next_expected_index) catch {
                self.requestReset(.corrupt_frame);
                return;
            };
            self.expected_next = cr.next_expected_index;
        }

        fn onHeartbeat(self: *Self, frame: []const u8) void {
            const hb = FrameCodec.decodeHeartbeat(frame) catch return;
            self.source_hwm = hb.hwm_index;
            const applied: u64 = if (self.last_applied_index < 0) 0 else @intCast(self.last_applied_index);
            self.metrics.lag_from_source_hwm = if (hb.hwm_index > applied) hb.hwm_index - applied else 0;
        }

        fn onClose(self: *Self) void {
            self.state = .closing;
        }

        fn maybeAck(self: *Self, now_ns: u64) !void {
            if (self.state != .live and self.state != .syncing) return;
            const advanced = self.last_applied_index != self.last_ack_index;
            const due = (advanced and now_ns - self.last_ack_sent_ns >= self.cfg.ack_interval_ns) or
                (now_ns - self.last_ack_sent_ns >= self.cfg.force_ack_interval_ns);
            if (!due) return;
            const idx: u64 = if (self.last_applied_index < 0) 0 else @intCast(self.last_applied_index);
            const len = try FrameCodec.encodeAck(self.scratch, self.session_id, idx, now_ns);
            _ = self.outbound.offer(self.scratch[0..len]);
            self.last_ack_sent_ns = now_ns;
            self.last_ack_index = self.last_applied_index;
        }

        fn requestReset(self: *Self, reason: protocol.ResetReason) void {
            const len = FrameCodec.encodeReset(self.scratch, self.session_id, reason) catch 0;
            if (len > 0) _ = self.outbound.offer(self.scratch[0..len]);
            self.metrics.replay_reset_count += 1;
            self.session_id = 0;
            self.reconnect.schedule(self.now());
            self.state = .resetting;
        }
    };
}

fn nackError(reason: protocol.NackReason) anyerror {
    return switch (reason) {
        .index_not_available => error.IndexNotAvailable,
        .sink_ahead_of_source => error.SinkAheadOfSource,
        .config_mismatch => error.ConfigMismatch,
        .version_incompatible => error.VersionIncompatible,
        .queue_id_mismatch => error.QueueIdMismatch,
        else => error.HelloNack,
    };
}

test "ReplicationSink compiles for the loopback transport" {
    const Loopback = @import("loopback.zig").Loopback;
    const S = ReplicationSink(Loopback.Outbound, Loopback.Inbound);
    try std.testing.expect(@hasDecl(S, "step"));
}
