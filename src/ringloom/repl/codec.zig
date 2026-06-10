// SPDX-License-Identifier: Apache-2.0

//! Allocation-free encoder/decoder for replication frames.
//!
//! Every encoder writes a full frame
//! (16-byte header + body) into a caller-owned buffer and returns the total
//! length; every decoder borrows the supplied slice and validates bounds,
//! returning `CodecError` rather than reading out of bounds.

const std = @import("std");

const p = @import("protocol.zig");
const Header = @import("../header.zig").Header;

pub const CodecError = p.CodecError;

/// Largest excerpt payload the protocol can carry (matches the on-disk header).
pub const MAX_PAYLOAD: usize = Header.SIZE_MASK;

fn w8(buf: []u8, off: usize, v: u8) void {
    buf[off] = v;
}
fn w16(buf: []u8, off: usize, v: u16) void {
    std.mem.writeInt(u16, buf[off..][0..2], v, .little);
}
fn w32(buf: []u8, off: usize, v: u32) void {
    std.mem.writeInt(u32, buf[off..][0..4], v, .little);
}
fn w64(buf: []u8, off: usize, v: u64) void {
    std.mem.writeInt(u64, buf[off..][0..8], v, .little);
}
fn r8(buf: []const u8, off: usize) u8 {
    return buf[off];
}
fn r16(buf: []const u8, off: usize) u16 {
    return std.mem.readInt(u16, buf[off..][0..2], .little);
}
fn r32(buf: []const u8, off: usize) u32 {
    return std.mem.readInt(u32, buf[off..][0..4], .little);
}
fn r64(buf: []const u8, off: usize) u64 {
    return std.mem.readInt(u64, buf[off..][0..8], .little);
}

/// Iterator over the records inside a decoded `EXCERPT_BATCH` frame.
pub const BatchView = struct {
    first_index: u64,
    count: u16,
    emitted: u16 = 0,
    cursor: usize = 0,
    bytes: []const u8, // remaining records: u32 length + payload, repeated

    /// Returns the next `(index, payload)` or null when the batch is drained.
    pub fn next(self: *BatchView) ?p.ExcerptView {
        if (self.emitted >= self.count) return null;
        if (self.cursor + 4 > self.bytes.len) return null;
        const len = r32(self.bytes, self.cursor);
        const start = self.cursor + 4;
        if (start + len > self.bytes.len) return null;
        const payload = self.bytes[start .. start + len];
        const idx = self.first_index + self.emitted;
        self.emitted += 1;
        self.cursor = start + len;
        return .{ .index = idx, .payload = payload };
    }
};

/// Incremental builder coalescing contiguous excerpts into one batch frame.
pub const BatchBuilder = struct {
    buf: []u8,
    len: usize,
    n: u16 = 0,

    /// Begins a batch frame for `first_index`; reserves the 16-byte header and
    /// the 10-byte batch prefix. Caller must ensure `buf.len >= 26`.
    pub fn begin(buf: []u8, session_id: u64, flags: u32, first_index: u64) BatchBuilder {
        writeHeaderUnchecked(buf, .{ .frame_type = .excerpt_batch, .flags = flags, .session_id = session_id });
        w64(buf, p.HEADER_SIZE + 0, first_index);
        w16(buf, p.HEADER_SIZE + 8, 0);
        return .{ .buf = buf, .len = p.HEADER_SIZE + 10 };
    }

    /// Appends one record; returns false (without mutating) if it would not fit
    /// or the batch is already at `maxInt(u16)` records.
    pub fn tryAppend(self: *BatchBuilder, payload: []const u8) bool {
        if (self.n == std.math.maxInt(u16)) return false;
        const need = 4 + payload.len;
        if (self.len + need > self.buf.len) return false;
        w32(self.buf, self.len, @intCast(payload.len));
        @memcpy(self.buf[self.len + 4 ..][0..payload.len], payload);
        self.len += need;
        self.n += 1;
        return true;
    }

    pub fn count(self: *const BatchBuilder) u16 {
        return self.n;
    }

    /// Finalizes the batch by stamping the record count; returns total length.
    pub fn finish(self: *BatchBuilder) usize {
        w16(self.buf, p.HEADER_SIZE + 8, self.n);
        return self.len;
    }
};

/// Stateless frame encoder/decoder.
pub const FrameCodec = struct {
    /// Writes the 16-byte common header; returns 16. Errors if buffer is short.
    pub fn writeHeader(buf: []u8, h: p.FrameHeader) CodecError!usize {
        if (buf.len < p.HEADER_SIZE) return error.BufferTooSmall;
        writeHeaderUnchecked(buf, h);
        return p.HEADER_SIZE;
    }

    /// Validates magic/version and returns the decoded common header.
    pub fn readHeader(buf: []const u8) CodecError!p.FrameHeader {
        if (buf.len < p.HEADER_SIZE) return error.TruncatedFrame;
        if (r16(buf, 0) != p.MAGIC) return error.BadMagic;
        if (r8(buf, 2) != p.PROTOCOL_VERSION) return error.UnsupportedVersion;
        return .{
            .frame_type = @enumFromInt(r8(buf, 3)),
            .flags = r32(buf, 4),
            .session_id = r64(buf, 8),
        };
    }

    /// Cheap header peek (same as readHeader; provided for call-site clarity).
    pub fn peekHeader(buf: []const u8) CodecError!p.FrameHeader {
        return readHeader(buf);
    }

    // ---- encoders -------------------------------------------------------

    pub fn encodeHello(buf: []u8, h: p.HelloFrame) CodecError!usize {
        const total = p.HEADER_SIZE + 72 + h.roll_name.len;
        if (buf.len < total) return error.BufferTooSmall;
        if (h.roll_name.len > std.math.maxInt(u16)) return error.BufferTooSmall;
        writeHeaderUnchecked(buf, .{ .frame_type = .hello, .flags = 0, .session_id = 0 });
        const b = p.HEADER_SIZE;
        w32(buf, b + 0, p.PROTOCOL_VERSION);
        @memcpy(buf[b + 4 ..][0..16], &h.sink_node_id);
        @memcpy(buf[b + 20 ..][0..16], &h.queue_id);
        w64(buf, b + 36, @bitCast(h.last_applied_index));
        w64(buf, b + 44, h.epoch_ms);
        w32(buf, b + 52, h.roll_length_secs);
        w32(buf, b + 56, h.index_count);
        w32(buf, b + 60, h.index_spacing);
        w32(buf, b + 64, h.block_size);
        w16(buf, b + 68, h.format_version);
        w16(buf, b + 70, @intCast(h.roll_name.len));
        @memcpy(buf[b + 72 ..][0..h.roll_name.len], h.roll_name);
        return total;
    }

    pub fn encodeHelloAck(buf: []u8, a: p.HelloAckFrame) CodecError!usize {
        const total = p.HEADER_SIZE + 45;
        if (buf.len < total) return error.BufferTooSmall;
        writeHeaderUnchecked(buf, .{ .frame_type = .hello_ack, .flags = 0, .session_id = 0 });
        const b = p.HEADER_SIZE;
        w32(buf, b + 0, p.PROTOCOL_VERSION);
        @memcpy(buf[b + 4 ..][0..16], &a.source_node_id);
        w64(buf, b + 20, a.session_id);
        w64(buf, b + 28, a.source_first_available_index);
        w64(buf, b + 36, a.source_last_index);
        w8(buf, b + 44, @intFromEnum(a.mode));
        return total;
    }

    pub fn encodeHelloNack(buf: []u8, reason: p.NackReason, message: []const u8) CodecError!usize {
        const total = p.HEADER_SIZE + 6 + message.len;
        if (buf.len < total) return error.BufferTooSmall;
        if (message.len > std.math.maxInt(u16)) return error.BufferTooSmall;
        writeHeaderUnchecked(buf, .{ .frame_type = .hello_nack, .flags = 0, .session_id = 0 });
        const b = p.HEADER_SIZE;
        w32(buf, b + 0, @intFromEnum(reason));
        w16(buf, b + 4, @intCast(message.len));
        @memcpy(buf[b + 6 ..][0..message.len], message);
        return total;
    }

    pub fn encodeExcerpt(buf: []u8, session_id: u64, flags: u32, index: u64, payload: []const u8) CodecError!usize {
        const total = p.HEADER_SIZE + 12 + payload.len;
        if (buf.len < total) return error.BufferTooSmall;
        if (payload.len > MAX_PAYLOAD) return error.BufferTooSmall;
        writeHeaderUnchecked(buf, .{ .frame_type = .excerpt, .flags = flags, .session_id = session_id });
        const b = p.HEADER_SIZE;
        w64(buf, b + 0, index);
        w32(buf, b + 8, @intCast(payload.len));
        @memcpy(buf[b + 12 ..][0..payload.len], payload);
        return total;
    }

    pub fn encodeCycleRoll(buf: []u8, session_id: u64, from_cycle: u32, to_cycle: u32, next_expected_index: u64) CodecError!usize {
        const total = p.HEADER_SIZE + 16;
        if (buf.len < total) return error.BufferTooSmall;
        writeHeaderUnchecked(buf, .{ .frame_type = .cycle_roll, .flags = 0, .session_id = session_id });
        const b = p.HEADER_SIZE;
        w32(buf, b + 0, from_cycle);
        w32(buf, b + 4, to_cycle);
        w64(buf, b + 8, next_expected_index);
        return total;
    }

    pub fn encodeHeartbeat(buf: []u8, session_id: u64, hwm: u64, wall_ns: u64) CodecError!usize {
        const total = p.HEADER_SIZE + 16;
        if (buf.len < total) return error.BufferTooSmall;
        writeHeaderUnchecked(buf, .{ .frame_type = .heartbeat, .flags = 0, .session_id = session_id });
        const b = p.HEADER_SIZE;
        w64(buf, b + 0, hwm);
        w64(buf, b + 8, wall_ns);
        return total;
    }

    pub fn encodeAck(buf: []u8, session_id: u64, last_applied: u64, wall_ns: u64) CodecError!usize {
        const total = p.HEADER_SIZE + 16;
        if (buf.len < total) return error.BufferTooSmall;
        writeHeaderUnchecked(buf, .{ .frame_type = .ack, .flags = 0, .session_id = session_id });
        const b = p.HEADER_SIZE;
        w64(buf, b + 0, last_applied);
        w64(buf, b + 8, wall_ns);
        return total;
    }

    pub fn encodeReset(buf: []u8, session_id: u64, reason: p.ResetReason) CodecError!usize {
        const total = p.HEADER_SIZE + 4;
        if (buf.len < total) return error.BufferTooSmall;
        writeHeaderUnchecked(buf, .{ .frame_type = .reset, .flags = 0, .session_id = session_id });
        w32(buf, p.HEADER_SIZE + 0, @intFromEnum(reason));
        return total;
    }

    pub fn encodeClose(buf: []u8, session_id: u64, reason: u32) CodecError!usize {
        const total = p.HEADER_SIZE + 4;
        if (buf.len < total) return error.BufferTooSmall;
        writeHeaderUnchecked(buf, .{ .frame_type = .close, .flags = 0, .session_id = session_id });
        w32(buf, p.HEADER_SIZE + 0, reason);
        return total;
    }

    // ---- decoders (borrow the input frame) ------------------------------

    pub fn decodeHello(frame: []const u8) CodecError!p.HelloView {
        const b = body(frame);
        if (b.len < 72) return error.TruncatedFrame;
        const name_len = r16(b, 70);
        if (72 + @as(usize, name_len) > b.len) return error.TruncatedFrame;
        var h: p.HelloView = .{};
        @memcpy(&h.sink_node_id, b[4..20]);
        @memcpy(&h.queue_id, b[20..36]);
        h.last_applied_index = @bitCast(r64(b, 36));
        h.epoch_ms = r64(b, 44);
        h.roll_length_secs = r32(b, 52);
        h.index_count = r32(b, 56);
        h.index_spacing = r32(b, 60);
        h.block_size = r32(b, 64);
        h.format_version = r16(b, 68);
        h.roll_name = b[72 .. 72 + name_len];
        return h;
    }

    pub fn decodeHelloAck(frame: []const u8) CodecError!p.HelloAckFrame {
        const b = body(frame);
        if (b.len < 45) return error.TruncatedFrame;
        var a: p.HelloAckFrame = .{};
        @memcpy(&a.source_node_id, b[4..20]);
        a.session_id = r64(b, 20);
        a.source_first_available_index = r64(b, 28);
        a.source_last_index = r64(b, 36);
        a.mode = @enumFromInt(r8(b, 44));
        return a;
    }

    pub fn decodeHelloNack(frame: []const u8) CodecError!p.NackView {
        const b = body(frame);
        if (b.len < 6) return error.TruncatedFrame;
        const msg_len = r16(b, 4);
        if (6 + @as(usize, msg_len) > b.len) return error.TruncatedFrame;
        return .{
            .reason = @enumFromInt(r32(b, 0)),
            .message = b[6 .. 6 + msg_len],
        };
    }

    pub fn decodeExcerpt(frame: []const u8) CodecError!p.ExcerptView {
        const b = body(frame);
        if (b.len < 12) return error.TruncatedFrame;
        const len = r32(b, 8);
        if (12 + @as(usize, len) > b.len) return error.TruncatedFrame;
        return .{ .index = r64(b, 0), .payload = b[12 .. 12 + len] };
    }

    pub fn decodeExcerptBatch(frame: []const u8) CodecError!BatchView {
        const b = body(frame);
        if (b.len < 10) return error.TruncatedFrame;
        return .{
            .first_index = r64(b, 0),
            .count = r16(b, 8),
            .bytes = b[10..],
        };
    }

    pub fn decodeCycleRoll(frame: []const u8) CodecError!p.CycleRollFrame {
        const b = body(frame);
        if (b.len < 16) return error.TruncatedFrame;
        return .{ .from_cycle = r32(b, 0), .to_cycle = r32(b, 4), .next_expected_index = r64(b, 8) };
    }

    pub fn decodeHeartbeat(frame: []const u8) CodecError!p.HeartbeatFrame {
        const b = body(frame);
        if (b.len < 16) return error.TruncatedFrame;
        return .{ .hwm_index = r64(b, 0), .wall_clock_nanos = r64(b, 8) };
    }

    pub fn decodeAck(frame: []const u8) CodecError!p.AckFrame {
        const b = body(frame);
        if (b.len < 16) return error.TruncatedFrame;
        return .{ .last_applied_index = r64(b, 0), .wall_clock_nanos = r64(b, 8) };
    }

    pub fn decodeReset(frame: []const u8) CodecError!p.ResetFrame {
        const b = body(frame);
        if (b.len < 4) return error.TruncatedFrame;
        return .{ .reason = @enumFromInt(r32(b, 0)) };
    }

    pub fn decodeClose(frame: []const u8) CodecError!p.CloseFrame {
        const b = body(frame);
        if (b.len < 4) return error.TruncatedFrame;
        return .{ .reason = r32(b, 0) };
    }
};

fn writeHeaderUnchecked(buf: []u8, h: p.FrameHeader) void {
    w16(buf, 0, p.MAGIC);
    w8(buf, 2, p.PROTOCOL_VERSION);
    w8(buf, 3, @intFromEnum(h.frame_type));
    w32(buf, 4, h.flags);
    w64(buf, 8, h.session_id);
}

/// Returns the body slice of a frame whose header has already been validated by
/// the caller, or an empty slice when the frame is header-only sized.
fn body(frame: []const u8) []const u8 {
    if (frame.len <= p.HEADER_SIZE) return frame[0..0];
    return frame[p.HEADER_SIZE..];
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

test "header round-trips" {
    var buf: [64]u8 = undefined;
    _ = try FrameCodec.writeHeader(&buf, .{ .frame_type = .excerpt, .flags = 0x5, .session_id = 0xABCD });
    const h = try FrameCodec.readHeader(&buf);
    try std.testing.expectEqual(p.FrameType.excerpt, h.frame_type);
    try std.testing.expectEqual(@as(u32, 0x5), h.flags);
    try std.testing.expectEqual(@as(u64, 0xABCD), h.session_id);
}

test "readHeader rejects bad magic and version" {
    var buf: [16]u8 = [_]u8{0} ** 16;
    try std.testing.expectError(error.BadMagic, FrameCodec.readHeader(&buf));
    std.mem.writeInt(u16, buf[0..2], p.MAGIC, .little);
    buf[2] = 99;
    try std.testing.expectError(error.UnsupportedVersion, FrameCodec.readHeader(&buf));
}

test "excerpt round-trips at min and max-ish sizes" {
    const allocator = std.testing.allocator;
    const payload = try allocator.alloc(u8, 4096);
    defer allocator.free(payload);
    for (payload, 0..) |*c, i| c.* = @truncate(i);

    const buf = try allocator.alloc(u8, payload.len + 64);
    defer allocator.free(buf);

    const n = try FrameCodec.encodeExcerpt(buf, 0x99, p.Flags.CATCHUP, 0x1234_5678, payload);
    const h = try FrameCodec.readHeader(buf[0..n]);
    try std.testing.expectEqual(p.FrameType.excerpt, h.frame_type);
    try std.testing.expectEqual(@as(u32, p.Flags.CATCHUP), h.flags);
    const e = try FrameCodec.decodeExcerpt(buf[0..n]);
    try std.testing.expectEqual(@as(u64, 0x1234_5678), e.index);
    try std.testing.expectEqualSlices(u8, payload, e.payload);
}

test "hello round-trips with roll name" {
    var buf: [256]u8 = undefined;
    const h_in: p.HelloFrame = .{
        .sink_node_id = [_]u8{1} ** 16,
        .queue_id = [_]u8{2} ** 16,
        .last_applied_index = -1,
        .epoch_ms = 12345,
        .roll_length_secs = 86400,
        .index_count = 4096,
        .index_spacing = 16,
        .block_size = 2 * 1024 * 1024,
        .format_version = 1,
        .roll_name = "DAILY",
    };
    const n = try FrameCodec.encodeHello(&buf, h_in);
    const out = try FrameCodec.decodeHello(buf[0..n]);
    try std.testing.expectEqual(h_in.last_applied_index, out.last_applied_index);
    try std.testing.expectEqual(h_in.index_count, out.index_count);
    try std.testing.expectEqualStrings("DAILY", out.roll_name);
    try std.testing.expectEqualSlices(u8, &h_in.queue_id, &out.queue_id);
}

test "hello_ack/nack/cycle_roll/heartbeat/ack/reset/close round-trip" {
    var buf: [128]u8 = undefined;

    var n = try FrameCodec.encodeHelloAck(&buf, .{ .session_id = 7, .source_first_available_index = 1, .source_last_index = 99, .mode = .catchup });
    const ack = try FrameCodec.decodeHelloAck(buf[0..n]);
    try std.testing.expectEqual(@as(u64, 7), ack.session_id);
    try std.testing.expectEqual(p.Mode.catchup, ack.mode);

    n = try FrameCodec.encodeHelloNack(&buf, .config_mismatch, "bad roll");
    const nack = try FrameCodec.decodeHelloNack(buf[0..n]);
    try std.testing.expectEqual(p.NackReason.config_mismatch, nack.reason);
    try std.testing.expectEqualStrings("bad roll", nack.message);

    n = try FrameCodec.encodeCycleRoll(&buf, 7, 3, 5, 0x5_0000_0000);
    const cr = try FrameCodec.decodeCycleRoll(buf[0..n]);
    try std.testing.expectEqual(@as(u32, 3), cr.from_cycle);
    try std.testing.expectEqual(@as(u32, 5), cr.to_cycle);
    try std.testing.expectEqual(@as(u64, 0x5_0000_0000), cr.next_expected_index);

    n = try FrameCodec.encodeHeartbeat(&buf, 7, 42, 1000);
    const hb = try FrameCodec.decodeHeartbeat(buf[0..n]);
    try std.testing.expectEqual(@as(u64, 42), hb.hwm_index);

    n = try FrameCodec.encodeAck(&buf, 7, 41, 2000);
    const a = try FrameCodec.decodeAck(buf[0..n]);
    try std.testing.expectEqual(@as(u64, 41), a.last_applied_index);

    n = try FrameCodec.encodeReset(&buf, 7, .gap_detected);
    const rs = try FrameCodec.decodeReset(buf[0..n]);
    try std.testing.expectEqual(p.ResetReason.gap_detected, rs.reason);

    n = try FrameCodec.encodeClose(&buf, 7, 0);
    const cl = try FrameCodec.decodeClose(buf[0..n]);
    try std.testing.expectEqual(@as(u32, 0), cl.reason);
}

test "batch builder fills to capacity and decodes contiguously" {
    var buf: [64]u8 = undefined; // header(16)+prefix(10)=26; each rec=4+2=6
    var bb = BatchBuilder.begin(&buf, 7, 0, 100);
    try std.testing.expect(bb.tryAppend("ab"));
    try std.testing.expect(bb.tryAppend("cd"));
    try std.testing.expect(bb.tryAppend("ef"));
    // 26 + 3*6 = 44; remaining 20 bytes -> 3 more fit (6 each = 18)
    try std.testing.expect(bb.tryAppend("gh"));
    try std.testing.expect(bb.tryAppend("ij"));
    try std.testing.expect(bb.tryAppend("kl"));
    try std.testing.expect(!bb.tryAppend("mn")); // 44+18=62, +6=68 > 64
    const n = bb.finish();
    try std.testing.expectEqual(@as(u16, 6), bb.count());

    var view = try FrameCodec.decodeExcerptBatch(buf[0..n]);
    var expect_idx: u64 = 100;
    const want = [_][]const u8{ "ab", "cd", "ef", "gh", "ij", "kl" };
    var i: usize = 0;
    while (view.next()) |rec| : (i += 1) {
        try std.testing.expectEqual(expect_idx, rec.index);
        try std.testing.expectEqualStrings(want[i], rec.payload);
        expect_idx += 1;
    }
    try std.testing.expectEqual(@as(usize, 6), i);
}

test "truncation fuzz never panics and reports clean errors" {
    var buf: [256]u8 = undefined;
    const n = try FrameCodec.encodeExcerpt(&buf, 1, 0, 5, "hello world");
    // Every strict prefix shorter than the full frame must error cleanly.
    var len: usize = 0;
    while (len < n) : (len += 1) {
        const slice = buf[0..len];
        if (FrameCodec.readHeader(slice)) |h| {
            if (h.frame_type == .excerpt) {
                _ = FrameCodec.decodeExcerpt(slice) catch {};
            }
        } else |_| {}
    }
}

test "frame codec is allocation-free" {
    var buf: [128]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(buf[0..0]);
    _ = fba.allocator();
    var frame: [128]u8 = undefined;
    const n = try FrameCodec.encodeExcerpt(&frame, 1, 0, 9, "payload");
    const e = try FrameCodec.decodeExcerpt(frame[0..n]);
    try std.testing.expectEqualStrings("payload", e.payload);
}
