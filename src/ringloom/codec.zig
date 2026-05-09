// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

const Header = @import("header.zig").Header;

/// Errors raised by codec helpers before bytes enter or leave the mmap buffer.
pub const CodecError = error{
    BufferTooSmall,
    MessageTooLarge,
    ParseFailed,
};

/// Compile-time codec contract for translating messages to and from payload bytes.
pub fn Codec(comptime MessageType: type) type {
    return struct {
        pub const Message = MessageType;

        parse: *const fn (buf: []const u8) ?MessageType,
        serialized_size: *const fn (msg: MessageType) usize,
        write: *const fn (buf: []u8, msg: MessageType, size: usize) void,

        /// Returns the serialized payload size after enforcing the 30-bit header limit.
        pub fn payloadSize(self: @This(), msg: MessageType) CodecError!usize {
            const size = self.serialized_size(msg);
            if (size > Header.SIZE_MASK) return error.MessageTooLarge;
            return size;
        }

        /// Returns the full entry size, including the 4-byte header and padding.
        pub fn entrySize(self: @This(), msg: MessageType) CodecError!usize {
            return Header.entrySize(try self.payloadSize(msg));
        }

        /// Writes a message into the provided payload buffer and returns the used slice.
        pub fn writePayload(self: @This(), buf: []u8, msg: MessageType) CodecError![]u8 {
            const size = try self.payloadSize(msg);
            if (buf.len < size) return error.BufferTooSmall;
            self.write(buf[0..size], msg, size);
            return buf[0..size];
        }

        /// Parses a payload slice or reports `ParseFailed` when the codec rejects it.
        pub fn parsePayload(self: @This(), buf: []const u8) CodecError!MessageType {
            return self.parse(buf) orelse error.ParseFailed;
        }
    };
}

/// Zero-copy codec for opaque byte slices.
pub const RawCodec = Codec([]const u8){
    .parse = struct {
        fn f(buf: []const u8) ?[]const u8 {
            return buf;
        }
    }.f,
    .serialized_size = struct {
        fn f(msg: []const u8) usize {
            return msg.len;
        }
    }.f,
    .write = struct {
        fn f(buf: []u8, msg: []const u8, size: usize) void {
            std.debug.assert(buf.len >= size);
            std.debug.assert(msg.len >= size);
            @memcpy(buf[0..size], msg[0..size]);
        }
    }.f,
};

/// Zero-copy codec for UTF-8 text payloads.
pub const TextCodec = Codec([]const u8){
    .parse = struct {
        fn f(buf: []const u8) ?[]const u8 {
            if (!std.unicode.utf8ValidateSlice(buf)) return null;
            return buf;
        }
    }.f,
    .serialized_size = RawCodec.serialized_size,
    .write = RawCodec.write,
};

/// Default codec used by untyped APIs.
pub const DefaultRawCodec = RawCodec;

/// Builds a fixed-layout codec for value types that can be copied as bytes.
pub fn StructCodec(comptime T: type) Codec(T) {
    return .{
        .parse = struct {
            fn f(buf: []const u8) ?T {
                if (buf.len < @sizeOf(T)) return null;
                return std.mem.bytesToValue(T, buf[0..@sizeOf(T)]);
            }
        }.f,
        .serialized_size = struct {
            fn f(_: T) usize {
                return @sizeOf(T);
            }
        }.f,
        .write = struct {
            fn f(buf: []u8, msg: T, size: usize) void {
                std.debug.assert(size == @sizeOf(T));
                std.debug.assert(buf.len >= size);
                const bytes = std.mem.asBytes(&msg);
                @memcpy(buf[0..size], bytes);
            }
        }.f,
    };
}

/// Callback result used by dispatching tailer loops.
pub const DispatchAction = enum {
    @"continue",
    stop,
};

/// Type-safe callback wrapper that parses a payload before dispatch.
pub fn Dispatcher(comptime MessageType: type) type {
    return struct {
        pub const DispatchFn = *const fn (ctx: *anyopaque, index: u64, msg: MessageType) DispatchAction;

        dispatch_fn: DispatchFn,
        context: *anyopaque,

        pub fn dispatch(self: @This(), index: u64, msg: MessageType) DispatchAction {
            return self.dispatch_fn(self.context, index, msg);
        }

        pub fn dispatchPayload(
            self: @This(),
            comptime codec: Codec(MessageType),
            index: u64,
            payload: []const u8,
        ) CodecError!DispatchAction {
            const msg = try codec.parsePayload(payload);
            return self.dispatch(index, msg);
        }
    };
}

/// Runtime vtable codec for FFI or dynamically selected message encodings.
pub const RuntimeCodec = struct {
    parse_fn: *const fn (data: [*]const u8, len: usize) ?*anyopaque,
    free_fn: ?*const fn (obj: *anyopaque) void = null,
    sizeof_fn: *const fn (obj: *anyopaque) usize,
    write_fn: *const fn (buf: [*]u8, obj: *anyopaque, len: usize) void,

    pub fn parse(self: RuntimeCodec, data: []const u8) ?*anyopaque {
        return self.parse_fn(data.ptr, data.len);
    }

    pub fn serializedSize(self: RuntimeCodec, obj: *anyopaque) CodecError!usize {
        const size = self.sizeof_fn(obj);
        if (size > Header.SIZE_MASK) return error.MessageTooLarge;
        return size;
    }

    pub fn entrySize(self: RuntimeCodec, obj: *anyopaque) CodecError!usize {
        return Header.entrySize(try self.serializedSize(obj));
    }

    pub fn write(self: RuntimeCodec, buf: []u8, obj: *anyopaque) CodecError![]u8 {
        const size = try self.serializedSize(obj);
        if (buf.len < size) return error.BufferTooSmall;
        self.write_fn(buf.ptr, obj, size);
        return buf[0..size];
    }

    pub fn free(self: RuntimeCodec, obj: *anyopaque) void {
        if (self.free_fn) |f| f(obj);
    }
};

test "raw codec is zero-copy and size checked" {
    const msg = "hello";
    try std.testing.expectEqual(msg.ptr, RawCodec.parse(msg).?.ptr);
    try std.testing.expectEqual(@as(usize, 5), RawCodec.serialized_size(msg));
    try std.testing.expectEqual(@as(usize, 12), try RawCodec.entrySize(msg));

    var out: [5]u8 = undefined;
    const written = try RawCodec.writePayload(&out, msg);
    try std.testing.expectEqualStrings(msg, written);
    try std.testing.expectError(error.BufferTooSmall, RawCodec.writePayload(out[0..4], msg));
}

test "text codec validates utf8" {
    const text = "hello";
    try std.testing.expectEqual(text.ptr, TextCodec.parse(text).?.ptr);
    try std.testing.expect(TextCodec.parse(&.{ 0xff, 0xfe }) == null);

    var out: [5]u8 = undefined;
    try std.testing.expectEqualStrings(text, try TextCodec.writePayload(&out, text));
}

test "structured codec round trip and truncated parse" {
    const Trade = extern struct {
        price: f64,
        quantity: u32,
        symbol: [8]u8,
    };
    const trade_codec = StructCodec(Trade);
    const msg = Trade{
        .price = 123.45,
        .quantity = 100,
        .symbol = "AAPL\x00\x00\x00\x00".*,
    };

    try std.testing.expectEqual(@sizeOf(Trade), trade_codec.serialized_size(msg));
    var buf: [@sizeOf(Trade)]u8 = undefined;
    const written = try trade_codec.writePayload(&buf, msg);
    try std.testing.expectEqual(@sizeOf(Trade), written.len);

    const parsed = try trade_codec.parsePayload(written);
    try std.testing.expectEqual(msg.price, parsed.price);
    try std.testing.expectEqual(msg.quantity, parsed.quantity);
    try std.testing.expectEqualSlices(u8, &msg.symbol, &parsed.symbol);
    try std.testing.expect(trade_codec.parse(written[0 .. written.len - 1]) == null);
}

test "empty payload round trip" {
    const empty = "";
    var buf: [1]u8 = undefined;
    const written = try RawCodec.writePayload(&buf, empty);
    try std.testing.expectEqual(@as(usize, 0), written.len);
    try std.testing.expectEqual(@as(usize, Header.HEADER_SIZE), try RawCodec.entrySize(empty));
    try std.testing.expectEqual(@as(usize, 0), RawCodec.parse(written).?.len);
}

test "dispatcher parses payload before callback" {
    const Context = struct {
        seen_index: u64 = 0,
        seen_len: usize = 0,
        fn dispatch(ctx: *anyopaque, index: u64, msg: []const u8) DispatchAction {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.seen_index = index;
            self.seen_len = msg.len;
            return .stop;
        }
    };

    var ctx: Context = .{};
    const dispatcher = Dispatcher([]const u8){
        .dispatch_fn = Context.dispatch,
        .context = &ctx,
    };
    try std.testing.expectEqual(DispatchAction.stop, try dispatcher.dispatchPayload(RawCodec, 7, "abc"));
    try std.testing.expectEqual(@as(u64, 7), ctx.seen_index);
    try std.testing.expectEqual(@as(usize, 3), ctx.seen_len);
}

test "runtime codec validates size and writes" {
    const RuntimeMessage = struct {
        bytes: []const u8,

        fn parse(data: [*]const u8, len: usize) ?*anyopaque {
            return @ptrCast(@constCast(data[0..len].ptr));
        }

        fn size(obj: *anyopaque) usize {
            const msg: *@This() = @ptrCast(@alignCast(obj));
            return msg.bytes.len;
        }

        fn write(buf: [*]u8, obj: *anyopaque, len: usize) void {
            const msg: *@This() = @ptrCast(@alignCast(obj));
            @memcpy(buf[0..len], msg.bytes[0..len]);
        }
    };

    var msg = RuntimeMessage{ .bytes = "runtime" };
    const runtime = RuntimeCodec{
        .parse_fn = RuntimeMessage.parse,
        .sizeof_fn = RuntimeMessage.size,
        .write_fn = RuntimeMessage.write,
    };

    var out: [7]u8 = undefined;
    try std.testing.expectEqualStrings("runtime", try runtime.write(&out, &msg));
    try std.testing.expectEqual(@as(usize, 12), try runtime.entrySize(&msg));
    try std.testing.expect(runtime.parse(&out) != null);
}
