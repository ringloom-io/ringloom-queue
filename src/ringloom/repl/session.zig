// SPDX-License-Identifier: Apache-2.0

//! Shared replication session primitives: session-id generation, reconnect
//! backoff, and config compatibility.

const std = @import("std");
const protocol = @import("protocol.zig");
const Queue = @import("../queue.zig").Queue;

/// Monotonic, nonzero, per-source session-id generator.
pub const SessionIdGen = struct {
    node_salt: u32,
    counter: u32 = 0,

    pub fn init(node_salt: u32) SessionIdGen {
        return .{ .node_salt = node_salt };
    }

    pub fn next(self: *SessionIdGen) u64 {
        self.counter += 1;
        return (@as(u64, self.node_salt) << 32) | @as(u64, self.counter);
    }
};

pub const BackoffPolicy = struct {
    min_ms: u64 = 50,
    max_ms: u64 = 5000,
    factor: u32 = 2,
};

/// Exponential reconnect backoff with an absolute next-attempt deadline.
pub const BackoffState = struct {
    policy: BackoffPolicy = .{},
    current_ms: u64 = 0,
    next_attempt_ns: u64 = 0,

    pub fn schedule(self: *BackoffState, now: u64) void {
        self.current_ms = if (self.current_ms == 0)
            self.policy.min_ms
        else
            @min(self.current_ms * self.policy.factor, self.policy.max_ms);
        self.next_attempt_ns = now + self.current_ms * std.time.ns_per_ms;
    }

    pub fn reset(self: *BackoffState) void {
        self.current_ms = 0;
        self.next_attempt_ns = 0;
    }

    pub fn ready(self: *const BackoffState, now: u64) bool {
        return now >= self.next_attempt_ns;
    }
};

/// The subset of queue configuration that must match between master and
/// follower for replication to be byte-compatible.
pub const QueueConfigId = struct {
    epoch_ms: u64 = 0,
    roll_length_secs: u32 = 0,
    index_count: u32 = 0,
    index_spacing: u32 = 0,
    block_size: u32 = 0,
    format_version: u16 = 0,
    roll_name: []const u8 = "",

    /// Builds the config identity from an open queue's effective configuration.
    pub fn fromQueue(queue: *const Queue) QueueConfigId {
        return .{
            .epoch_ms = if (queue.roll_epoch < 0) 0 else @intCast(queue.roll_epoch),
            .roll_length_secs = queue.roll_length_secs,
            .index_count = queue.index_count,
            .index_spacing = queue.index_spacing,
            .block_size = @intCast(Queue.qf_disk_sz),
            .format_version = @import("../metadata.zig").format_version,
            .roll_name = queue.roll_name orelse "",
        };
    }

    /// Returns null when compatible, or the NACK reason describing the mismatch.
    pub fn checkCompatible(self: QueueConfigId, h: protocol.HelloView) ?protocol.NackReason {
        if (h.format_version != self.format_version) return .version_incompatible;
        if (h.epoch_ms != self.epoch_ms or
            h.roll_length_secs != self.roll_length_secs or
            h.index_count != self.index_count or
            h.index_spacing != self.index_spacing or
            h.block_size != self.block_size or
            !std.mem.eql(u8, h.roll_name, self.roll_name))
            return .config_mismatch;
        return null;
    }
};

test "session id is nonzero and monotonic" {
    var gen = SessionIdGen.init(0xABCD);
    const a = gen.next();
    const b = gen.next();
    try std.testing.expect(a != 0);
    try std.testing.expect(b > a);
    try std.testing.expectEqual(@as(u64, 0xABCD) << 32 | 1, a);
}

test "backoff grows then resets" {
    var bs = BackoffState{ .policy = .{ .min_ms = 50, .max_ms = 400, .factor = 2 } };
    bs.schedule(0);
    try std.testing.expectEqual(@as(u64, 50), bs.current_ms);
    bs.schedule(0);
    try std.testing.expectEqual(@as(u64, 100), bs.current_ms);
    bs.schedule(0);
    bs.schedule(0);
    bs.schedule(0);
    try std.testing.expectEqual(@as(u64, 400), bs.current_ms); // capped
    bs.reset();
    try std.testing.expectEqual(@as(u64, 0), bs.current_ms);
}

test "backoff readiness honors the deadline" {
    var bs = BackoffState{ .policy = .{ .min_ms = 10, .max_ms = 100, .factor = 2 } };
    bs.schedule(1000);
    try std.testing.expect(!bs.ready(1000));
    try std.testing.expect(bs.ready(1000 + 10 * std.time.ns_per_ms));
}

test "config compatibility detects mismatches" {
    const local = QueueConfigId{
        .epoch_ms = 100,
        .roll_length_secs = 86400,
        .index_count = 4096,
        .index_spacing = 16,
        .block_size = 1 << 20,
        .format_version = 1,
        .roll_name = "DAILY",
    };
    var h: protocol.HelloView = .{
        .epoch_ms = 100,
        .roll_length_secs = 86400,
        .index_count = 4096,
        .index_spacing = 16,
        .block_size = 1 << 20,
        .format_version = 1,
        .roll_name = "DAILY",
    };
    try std.testing.expect(local.checkCompatible(h) == null);
    h.format_version = 2;
    try std.testing.expectEqual(protocol.NackReason.version_incompatible, local.checkCompatible(h).?);
    h.format_version = 1;
    h.roll_length_secs = 3600;
    try std.testing.expectEqual(protocol.NackReason.config_mismatch, local.checkCompatible(h).?);
}
