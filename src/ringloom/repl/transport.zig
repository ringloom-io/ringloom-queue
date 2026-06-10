// SPDX-License-Identifier: Apache-2.0

//! Transport SPI: zero-overhead, comptime-generic channel contracts.
//!
//! The replication library does not ship a
//! transport; consumers supply concrete `Outbound`/`Inbound` types that satisfy
//! the duck-typed contracts asserted here. Source/sink are generic over those
//! types so `offer`/`poll`/`nextFrame` bind statically and inline — no runtime
//! vtable except the `CTransport` adapter at the C ABI boundary.

const std = @import("std");

/// Negative results returned by `Outbound.offer`. Any value `>= 0` means the
/// frame was accepted at that transport position.
pub const OfferResult = enum(i64) {
    back_pressured = -1,
    not_connected = -2,
    admin_action = -3,
    closed = -4,
    max_position_exceeded = -5,

    /// True when `n` (an `offer` return value) indicates acceptance.
    pub fn accepted(n: i64) bool {
        return n >= 0;
    }
};

pub const ChannelId = struct { name: []const u8, stream_id: u32 };

pub const DisconnectReason = enum(u32) { remote_closed, timeout, transport_error, local_close };

/// Compile-time assertion that `T` satisfies the outbound contract.
pub fn AssertOutbound(comptime T: type) void {
    if (!@hasDecl(T, "offer") or !@hasDecl(T, "isConnected") or !@hasDecl(T, "isBackPressured"))
        @compileError(@typeName(T) ++ " is not a valid Outbound transport (needs offer/isConnected/isBackPressured)");
}

/// Compile-time assertion that `T` satisfies the inbound (pull-model) contract.
pub fn AssertInbound(comptime T: type) void {
    if (!@hasDecl(T, "poll") or !@hasDecl(T, "nextFrame") or !@hasDecl(T, "isConnected"))
        @compileError(@typeName(T) ++ " is not a valid Inbound transport (needs poll/nextFrame/isConnected)");
}

/// Runtime vtable for an outbound channel — used only when the concrete type is
/// not known at comptime (C ABI / FFM). Native Zig consumers never touch this.
/// These are Zig-internal indirect calls; the real C function pointers live in
/// the C ABI channel structs, which the adapter forwards to.
pub const OutboundVTable = struct {
    offer: *const fn (ctx: *anyopaque, frame: []const u8) i64,
    is_connected: *const fn (ctx: *anyopaque) bool,
    is_back_pressured: *const fn (ctx: *anyopaque) bool,
};

/// Runtime vtable for an inbound channel (pull model).
pub const InboundVTable = struct {
    poll: *const fn (ctx: *anyopaque, fragment_limit: u32) u32,
    next_frame: *const fn (ctx: *anyopaque, out_len: *usize) ?[*]const u8,
    is_connected: *const fn (ctx: *anyopaque) bool,
};

/// Adapter turning a *comptime-known* outbound vtable into a satisfying channel
/// type; because `vt` is comptime the calls devirtualize and inline.
pub fn ComptimeOutbound(comptime vt: OutboundVTable) type {
    return struct {
        ctx: *anyopaque,
        pub fn offer(self: *@This(), f: []const u8) i64 {
            return vt.offer(self.ctx, f);
        }
        pub fn isConnected(self: *@This()) bool {
            return vt.is_connected(self.ctx);
        }
        pub fn isBackPressured(self: *@This()) bool {
            return vt.is_back_pressured(self.ctx);
        }
    };
}

/// Runtime-erased C ABI transport: the single place a per-call indirect branch
/// is unavoidable. Per-frame decode + writeAtIndex still stay monomorphized in
/// the generic core; only the transport primitives are indirect.
pub const CTransport = struct {
    pub const Out = struct {
        ctx: *anyopaque,
        vt: *const OutboundVTable,
        pub fn offer(self: *Out, frame: []const u8) i64 {
            return self.vt.offer(self.ctx, frame);
        }
        pub fn isConnected(self: *Out) bool {
            return self.vt.is_connected(self.ctx);
        }
        pub fn isBackPressured(self: *Out) bool {
            return self.vt.is_back_pressured(self.ctx);
        }
    };

    pub const In = struct {
        ctx: *anyopaque,
        vt: *const InboundVTable,
        pub fn poll(self: *In, fragment_limit: u32) u32 {
            return self.vt.poll(self.ctx, fragment_limit);
        }
        pub fn nextFrame(self: *In) ?[]const u8 {
            var len: usize = 0;
            const ptr = self.vt.next_frame(self.ctx, &len) orelse return null;
            return ptr[0..len];
        }
        pub fn isConnected(self: *In) bool {
            return self.vt.is_connected(self.ctx);
        }
    };
};

comptime {
    AssertOutbound(CTransport.Out);
    AssertInbound(CTransport.In);
}

test "OfferResult.accepted classifies returns" {
    try std.testing.expect(OfferResult.accepted(0));
    try std.testing.expect(OfferResult.accepted(123));
    try std.testing.expect(!OfferResult.accepted(@intFromEnum(OfferResult.back_pressured)));
    try std.testing.expect(!OfferResult.accepted(-2));
}
