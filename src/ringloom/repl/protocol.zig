// SPDX-License-Identifier: Apache-2.0

//! Replication wire-protocol constants, frame types, and helper structs.
//!
//! All multi-byte integers are
//! little-endian; the transport delivers whole frames, so the protocol carries
//! no outer length prefix.

const std = @import("std");

/// Frame magic: 'R','L' little-endian.
pub const MAGIC: u16 = 0x524C;
/// Protocol version negotiated in HELLO/HELLO_ACK.
pub const PROTOCOL_VERSION: u8 = 1;
/// Fixed common frame-header size in bytes.
pub const HEADER_SIZE: usize = 16;

/// Discriminator byte identifying each frame body layout.
pub const FrameType = enum(u8) {
    hello = 0x01,
    hello_ack = 0x02,
    hello_nack = 0x03,
    excerpt = 0x10,
    excerpt_batch = 0x11,
    cycle_roll = 0x20,
    heartbeat = 0x30,
    ack = 0x31,
    reset = 0x40,
    close = 0xF0,
    _,
};

/// Frame `flags` bitfield values.
pub const Flags = struct {
    pub const END_OF_BATCH: u32 = 1 << 0;
    pub const META_EXCERPT: u32 = 1 << 1; // reserved; v1 sources MUST NOT set
    pub const CATCHUP: u32 = 1 << 2;
};

/// Reason carried by HELLO_NACK; all are fatal to the sink.
pub const NackReason = enum(u32) {
    index_not_available = 1,
    sink_ahead_of_source = 2,
    config_mismatch = 3,
    version_incompatible = 4,
    queue_id_mismatch = 5,
    internal_error = 6,
    _,
};

/// Reason carried by RESET (sink → source).
pub const ResetReason = enum(u32) {
    gap_detected = 1,
    corrupt_frame = 2,
    operator_request = 3,
    _,
};

/// Replay mode chosen by the source on HELLO; informational to the sink.
pub const Mode = enum(u8) {
    full_replay = 0,
    catchup = 1,
    live = 2,
    _,
};

/// Decoded common 16-byte frame header.
pub const FrameHeader = struct {
    frame_type: FrameType,
    flags: u32 = 0,
    session_id: u64 = 0,
};

/// HELLO body (sink → source). `roll_name` borrows the input buffer when decoded.
pub const HelloFrame = struct {
    sink_node_id: [16]u8 = [_]u8{0} ** 16,
    queue_id: [16]u8 = [_]u8{0} ** 16,
    last_applied_index: i64 = -1,
    epoch_ms: u64 = 0,
    roll_length_secs: u32 = 0,
    index_count: u32 = 0,
    index_spacing: u32 = 0,
    block_size: u32 = 0,
    format_version: u16 = 0,
    roll_name: []const u8 = "",
};

/// HELLO decoded view (alias; `roll_name` borrows the frame buffer).
pub const HelloView = HelloFrame;

/// HELLO_ACK body (source → sink).
pub const HelloAckFrame = struct {
    source_node_id: [16]u8 = [_]u8{0} ** 16,
    session_id: u64 = 0,
    source_first_available_index: u64 = 0,
    source_last_index: u64 = 0,
    mode: Mode = .live,
};

/// HELLO_NACK decoded view; `message` borrows the frame buffer.
pub const NackView = struct {
    reason: NackReason,
    message: []const u8 = "",
};

/// Single decoded excerpt; `payload` borrows the frame buffer.
pub const ExcerptView = struct {
    index: u64,
    payload: []const u8,
};

pub const CycleRollFrame = struct {
    from_cycle: u32,
    to_cycle: u32,
    next_expected_index: u64,
};

pub const HeartbeatFrame = struct {
    hwm_index: u64,
    wall_clock_nanos: u64,
};

pub const AckFrame = struct {
    last_applied_index: u64,
    wall_clock_nanos: u64,
};

pub const ResetFrame = struct {
    reason: ResetReason,
};

pub const CloseFrame = struct {
    reason: u32,
};

/// Errors surfaced by the frame codec.
pub const CodecError = error{
    BufferTooSmall,
    TruncatedFrame,
    BadMagic,
    UnsupportedVersion,
    BadFrameType,
};

test "frame type round-trips through its integer value" {
    try std.testing.expectEqual(@as(u8, 0x10), @intFromEnum(FrameType.excerpt));
    try std.testing.expectEqual(FrameType.hello, @as(FrameType, @enumFromInt(0x01)));
}
