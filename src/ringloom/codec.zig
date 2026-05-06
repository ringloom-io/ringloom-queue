const std = @import("std");

pub fn Codec(comptime T: type) type {
    comptime {
        if (!@hasDecl(T, "Message")) @compileError("Codec must define a 'Message' type");
        if (!@hasDecl(T, "parse")) @compileError("Codec must define 'parse'");
        if (!@hasDecl(T, "encodedSize")) @compileError("Codec must define 'encodedSize'");
        if (!@hasDecl(T, "write")) @compileError("Codec must define 'write'");
    }
    return T;
}

pub const DispatchAction = enum {
    @"continue",
    stop,
};

pub fn Dispatcher(comptime MessageType: type) type {
    return struct {
        pub const DispatchFn = *const fn (ctx: *anyopaque, index: u64, msg: MessageType) DispatchAction;

        dispatch_fn: DispatchFn,
        context: *anyopaque,

        pub fn dispatch(self: @This(), index: u64, msg: MessageType) DispatchAction {
            return self.dispatch_fn(self.context, index, msg);
        }
    };
}

pub const DefaultRawCodec = struct {
    pub const Message = []const u8;

    pub fn parse(data: []const u8) ?Message {
        return data;
    }

    pub fn encodedSize(msg: Message) usize {
        return msg.len;
    }

    pub fn write(buf: []u8, msg: Message) void {
        std.debug.assert(buf.len >= msg.len);
        @memcpy(buf[0..msg.len], msg);
    }
};

pub const RuntimeCodec = struct {
    parse_fn: *const fn (data: [*]const u8, len: usize) ?*anyopaque,
    free_fn: ?*const fn (obj: *anyopaque) void = null,
    sizeof_fn: *const fn (obj: *anyopaque) usize,
    write_fn: *const fn (buf: [*]u8, obj: *anyopaque, len: usize) void,

    pub fn parse(self: RuntimeCodec, data: []const u8) ?*anyopaque {
        return self.parse_fn(data.ptr, data.len);
    }

    pub fn encodedSize(self: RuntimeCodec, obj: *anyopaque) usize {
        return self.sizeof_fn(obj);
    }

    pub fn write(self: RuntimeCodec, buf: []u8, obj: *anyopaque) void {
        self.write_fn(buf.ptr, obj, buf.len);
    }

    pub fn free(self: RuntimeCodec, obj: *anyopaque) void {
        if (self.free_fn) |f| f(obj);
    }
};

test "default raw codec is zero-copy" {
    const Raw = Codec(DefaultRawCodec);
    const msg = "hello";
    try std.testing.expectEqual(msg.ptr, Raw.parse(msg).?.ptr);
    try std.testing.expectEqual(@as(usize, 5), Raw.encodedSize(msg));

    var out: [5]u8 = undefined;
    Raw.write(&out, msg);
    try std.testing.expectEqualStrings(msg, &out);
}

test "dispatcher invokes callback" {
    const Context = struct {
        seen_index: u64 = 0,
        fn dispatch(ctx: *anyopaque, index: u64, msg: []const u8) DispatchAction {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.seen_index = index + msg.len;
            return .stop;
        }
    };

    var ctx: Context = .{};
    const dispatcher = Dispatcher([]const u8){
        .dispatch_fn = Context.dispatch,
        .context = &ctx,
    };
    try std.testing.expectEqual(DispatchAction.stop, dispatcher.dispatch(7, "abc"));
    try std.testing.expectEqual(@as(u64, 10), ctx.seen_index);
}
