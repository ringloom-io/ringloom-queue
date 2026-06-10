// SPDX-License-Identifier: Apache-2.0

//! Deterministic in-memory loopback transport for tests.
//!
//! Concrete `Outbound`/`Inbound` types
//! so tests exercise the fully monomorphized (indirect-call-free) hot path.
//! NOT production code; not part of `mod.zig`'s production surface.

const std = @import("std");
const transport = @import("transport.zig");

/// Bounded SPSC ring of whole frames with explicit connection state.
pub const FrameRing = struct {
    allocator: std.mem.Allocator,
    slots: [][]u8, // each slot is a max_frame-sized owned buffer
    lens: []usize,
    head: usize = 0, // next write slot
    tail: usize = 0, // next read slot
    count: usize = 0, // buffered (offered, not yet consumed)
    readable: usize = 0, // frames marked readable by poll
    connected: bool = true,
    drop_remaining: usize = 0,

    pub fn init(allocator: std.mem.Allocator, capacity_frames: usize, max_frame: usize) !FrameRing {
        const slots = try allocator.alloc([]u8, capacity_frames);
        errdefer allocator.free(slots);
        var made: usize = 0;
        errdefer for (slots[0..made]) |s| allocator.free(s);
        while (made < capacity_frames) : (made += 1) {
            slots[made] = try allocator.alloc(u8, max_frame);
        }
        const lens = try allocator.alloc(usize, capacity_frames);
        return .{ .allocator = allocator, .slots = slots, .lens = lens };
    }

    pub fn deinit(self: *FrameRing) void {
        for (self.slots) |s| self.allocator.free(s);
        self.allocator.free(self.slots);
        self.allocator.free(self.lens);
        self.* = undefined;
    }

    fn capacity(self: *const FrameRing) usize {
        return self.slots.len;
    }

    /// Enqueue a frame copy. Returns the offer result (>=0 position, or negative).
    fn push(self: *FrameRing, frame: []const u8) i64 {
        if (!self.connected) return @intFromEnum(transport.OfferResult.not_connected);
        if (self.drop_remaining > 0) {
            // A dropped frame surfaces as a disconnect, never a silent gap.
            self.drop_remaining -= 1;
            self.connected = false;
            return @intFromEnum(transport.OfferResult.not_connected);
        }
        if (self.count == self.capacity()) return @intFromEnum(transport.OfferResult.back_pressured);
        if (frame.len > self.slots[self.head].len) return @intFromEnum(transport.OfferResult.max_position_exceeded);
        @memcpy(self.slots[self.head][0..frame.len], frame);
        self.lens[self.head] = frame.len;
        self.head = (self.head + 1) % self.capacity();
        self.count += 1;
        return @intCast(self.count);
    }

    fn isFull(self: *const FrameRing) bool {
        return self.count == self.capacity();
    }
};

pub const Loopback = struct {
    pub const Outbound = struct {
        ring: *FrameRing,

        pub fn offer(self: *Outbound, frame: []const u8) i64 {
            return self.ring.push(frame);
        }
        pub fn isConnected(self: *Outbound) bool {
            return self.ring.connected;
        }
        pub fn isBackPressured(self: *Outbound) bool {
            return self.ring.isFull();
        }
    };

    pub const Inbound = struct {
        ring: *FrameRing,

        pub fn poll(self: *Inbound, fragment_limit: u32) u32 {
            if (!self.ring.connected) return 0;
            const want = @min(@as(usize, fragment_limit), self.ring.count);
            self.ring.readable = want;
            return @intCast(want);
        }
        pub fn nextFrame(self: *Inbound) ?[]const u8 {
            if (self.ring.readable == 0 or self.ring.count == 0) return null;
            const slot = self.ring.tail;
            const len = self.ring.lens[slot];
            self.ring.tail = (self.ring.tail + 1) % self.ring.slots.len;
            self.ring.count -= 1;
            self.ring.readable -= 1;
            return self.ring.slots[slot][0..len];
        }
        pub fn isConnected(self: *Inbound) bool {
            return self.ring.connected;
        }
    };
};

comptime {
    transport.AssertOutbound(Loopback.Outbound);
    transport.AssertInbound(Loopback.Inbound);
}

/// A bidirectional pair: source⇄sink, each direction a bounded ring.
pub const LoopbackPair = struct {
    allocator: std.mem.Allocator,
    s2k: FrameRing, // source -> sink
    k2s: FrameRing, // sink -> source
    src_out: Loopback.Outbound,
    src_in: Loopback.Inbound,
    sink_out: Loopback.Outbound,
    sink_in: Loopback.Inbound,

    pub fn init(allocator: std.mem.Allocator, capacity_frames: usize, max_frame: usize) !*LoopbackPair {
        const self = try allocator.create(LoopbackPair);
        errdefer allocator.destroy(self);
        self.allocator = allocator;
        self.s2k = try FrameRing.init(allocator, capacity_frames, max_frame);
        errdefer self.s2k.deinit();
        self.k2s = try FrameRing.init(allocator, capacity_frames, max_frame);
        self.src_out = .{ .ring = &self.s2k };
        self.sink_in = .{ .ring = &self.s2k };
        self.sink_out = .{ .ring = &self.k2s };
        self.src_in = .{ .ring = &self.k2s };
        return self;
    }

    pub fn deinit(self: *LoopbackPair) void {
        self.s2k.deinit();
        self.k2s.deinit();
        const a = self.allocator;
        a.destroy(self);
    }

    pub fn sourceOutbound(self: *LoopbackPair) *Loopback.Outbound {
        return &self.src_out;
    }
    pub fn sourceInbound(self: *LoopbackPair) *Loopback.Inbound {
        return &self.src_in;
    }
    pub fn sinkOutbound(self: *LoopbackPair) *Loopback.Outbound {
        return &self.sink_out;
    }
    pub fn sinkInbound(self: *LoopbackPair) *Loopback.Inbound {
        return &self.sink_in;
    }

    /// Drop the next `n` source→sink frames; each surfaces as a disconnect.
    pub fn dropNext(self: *LoopbackPair, n: usize) void {
        self.s2k.drop_remaining = n;
    }
    pub fn disconnect(self: *LoopbackPair) void {
        self.s2k.connected = false;
        self.k2s.connected = false;
    }
    pub fn reconnect(self: *LoopbackPair) void {
        self.s2k.connected = true;
        self.k2s.connected = true;
        self.s2k.drop_remaining = 0;
        self.k2s.drop_remaining = 0;
    }
};

test "loopback delivers frames in order through the pull model" {
    const a = std.testing.allocator;
    var pair = try LoopbackPair.init(a, 8, 64);
    defer pair.deinit();

    const out = pair.sourceOutbound();
    try std.testing.expect(transport.OfferResult.accepted(out.offer("one")));
    try std.testing.expect(transport.OfferResult.accepted(out.offer("two")));

    const in = pair.sinkInbound();
    try std.testing.expectEqual(@as(u32, 2), in.poll(16));
    try std.testing.expectEqualStrings("one", in.nextFrame().?);
    try std.testing.expectEqualStrings("two", in.nextFrame().?);
    try std.testing.expect(in.nextFrame() == null);
}

test "loopback applies backpressure when full" {
    const a = std.testing.allocator;
    var pair = try LoopbackPair.init(a, 2, 32);
    defer pair.deinit();
    const out = pair.sourceOutbound();
    try std.testing.expect(transport.OfferResult.accepted(out.offer("a")));
    try std.testing.expect(transport.OfferResult.accepted(out.offer("b")));
    try std.testing.expect(out.isBackPressured());
    try std.testing.expectEqual(@as(i64, @intFromEnum(transport.OfferResult.back_pressured)), out.offer("c"));
}

test "disconnect/reconnect toggles connection state" {
    const a = std.testing.allocator;
    var pair = try LoopbackPair.init(a, 4, 32);
    defer pair.deinit();
    pair.disconnect();
    try std.testing.expect(!pair.sourceOutbound().isConnected());
    try std.testing.expectEqual(@as(i64, @intFromEnum(transport.OfferResult.not_connected)), pair.sourceOutbound().offer("x"));
    pair.reconnect();
    try std.testing.expect(pair.sourceOutbound().isConnected());
}
