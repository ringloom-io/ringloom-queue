const std = @import("std");

const Appender = @import("appender.zig").Appender;
const codec = @import("codec.zig");
const CoreQueue = @import("queue.zig").Queue;
const roll = @import("roll.zig");
const Tailer = @import("tailer.zig").Tailer;

/// C ABI version for struct-size and behavior compatibility checks.
pub const abi_version: u32 = 1;

/// Opaque C queue handle.
pub const ringloom_queue_t = opaque {};
/// Opaque C appender handle.
pub const ringloom_appender_t = opaque {};
/// Opaque C tailer handle.
pub const ringloom_tailer_t = opaque {};

const QueueHandle = struct {
    queue: *CoreQueue,
};

const AppenderHandle = struct {
    appender: *Appender,
};

const TailerHandle = struct {
    tailer: *Tailer,
};

/// Stable C error codes; these are not Zig error-set ordinals.
pub const ringloom_error_t = enum(c_int) {
    ok = 0,
    ok_not_ready = 1,
    invalid_argument = -1,
    out_of_memory = -2,
    queue_not_found = -3,
    metadata_corrupt = -4,
    mmap_failed = -5,
    appender_already_open = -6,
    message_too_large = -7,
    empty_payload = -8,
    parse_failed = -9,
    internal_error = -255,
};

/// C representation of bounded maintenance poll progress.
pub const ringloom_step_result_t = enum(c_int) {
    idle = 0,
    progress = 1,
    more_work = 2,
};

/// Options passed to `ringloom_queue_open`.
pub const ringloom_queue_options = extern struct {
    size: u32 = @sizeOf(ringloom_queue_options),
    dir: ?[*]const u8 = null,
    dir_len: usize = 0,
    create: bool = false,
    roll_scheme_name: ?[*]const u8 = null,
    roll_scheme_name_len: usize = 0,
    enable_prefetcher: bool = true,
    enable_cleaner: bool = true,
    spawn_helper_threads: bool = false,
    retention_cycles: u32 = 0,
    retention_cycles_set: bool = false,
};

/// Borrowed payload view returned by `ringloom_tailer_poll`.
pub const ringloom_message_view = extern struct {
    size: u32 = @sizeOf(ringloom_message_view),
    index: u64 = 0,
    data: ?*const anyopaque = null,
    data_len: usize = 0,
};

/// Returns the C ABI version supported by this build.
pub export fn ringloom_abi_version() u32 {
    return abi_version;
}

/// Returns a static, null-terminated string for a C error code.
pub export fn ringloom_strerror(err: c_int) [*:0]const u8 {
    return switch (err) {
        0 => "ok",
        1 => "not ready",
        -1 => "invalid argument",
        -2 => "out of memory",
        -3 => "queue not found",
        -4 => "metadata corrupt",
        -5 => "mmap failed",
        -6 => "appender already open",
        -7 => "message too large",
        -8 => "empty payload",
        -9 => "parse failed",
        else => "internal error",
    };
}

/// Opens or creates a queue and stores an opaque handle in `out`.
pub export fn ringloom_queue_open(opts: ?*const ringloom_queue_options, out: ?*?*ringloom_queue_t) c_int {
    const options = opts orelse return code(.invalid_argument);
    const out_ptr = out orelse return code(.invalid_argument);
    out_ptr.* = null;
    if (options.dir == null or options.dir_len == 0) return code(.invalid_argument);
    if (options.size < @offsetOf(ringloom_queue_options, "spawn_helper_threads") + @sizeOf(bool)) {
        return code(.invalid_argument);
    }

    const allocator = std.heap.page_allocator;
    const dir = options.dir.?[0..options.dir_len];

    const queue = CoreQueue.init(allocator, dir) catch |err| return errorCode(err);

    queue.setCreate(options.create);
    if (options.roll_scheme_name) |name_ptr| {
        const name = name_ptr[0..options.roll_scheme_name_len];
        queue.setRollSchemeName(name) catch |err| {
            queue.deinit();
            return errorCode(err);
        };
    } else if (options.create) {
        queue.setRollScheme(roll.default_scheme) catch |err| {
            queue.deinit();
            return errorCode(err);
        };
    }
    queue.setPrefetcher(options.enable_prefetcher, queue.prefetch_runway_bytes);
    const retention_cycles: ?u32 = if (options.size >= @offsetOf(ringloom_queue_options, "retention_cycles_set") + @sizeOf(bool) and
        options.retention_cycles_set)
        options.retention_cycles
    else
        null;
    queue.setCleaner(options.enable_cleaner, retention_cycles);
    queue.setHelperThreads(options.spawn_helper_threads);
    queue.open() catch |err| {
        queue.deinit();
        return errorCode(err);
    };

    const handle = allocator.create(QueueHandle) catch |err| {
        queue.deinit();
        return errorCode(err);
    };
    handle.* = .{ .queue = queue };
    out_ptr.* = @ptrCast(handle);
    return code(.ok);
}

/// Closes a queue handle and all resources it owns.
pub export fn ringloom_queue_close(q: ?*ringloom_queue_t) void {
    const handle = queueHandle(q) orelse return;
    const allocator = handle.queue.allocator;
    handle.queue.deinit();
    allocator.destroy(handle);
}

/// Opens a C appender handle; returns `appender_already_open` for duplicates.
pub export fn ringloom_appender_open(q: ?*ringloom_queue_t, out: ?*?*ringloom_appender_t) c_int {
    const queue_handle = queueHandle(q) orelse return code(.invalid_argument);
    const out_ptr = out orelse return code(.invalid_argument);
    out_ptr.* = null;
    if (queue_handle.queue.appender != null) return code(.appender_already_open);

    const appender = Appender.open(queue_handle.queue) catch |err| return errorCode(err);
    const handle = queue_handle.queue.allocator.create(AppenderHandle) catch |err| {
        appender.close();
        return errorCode(err);
    };
    handle.* = .{ .appender = appender };
    out_ptr.* = @ptrCast(handle);
    return code(.ok);
}

/// Appends raw bytes and optionally returns the assigned public index.
pub export fn ringloom_appender_append(
    a: ?*ringloom_appender_t,
    data: ?*const anyopaque,
    len: usize,
    index_out: ?*u64,
) c_int {
    const handle = appenderHandle(a) orelse return code(.invalid_argument);
    if (data == null and len != 0) return code(.invalid_argument);
    const bytes = if (len == 0) &[_]u8{} else @as([*]const u8, @ptrCast(data.?))[0..len];
    const idx = handle.appender.appendWithTimestamp(bytes, nowMs(handle.appender.queue.io) catch |err| return errorCode(err)) catch |err| return errorCode(err);
    if (index_out) |out| out.* = idx;
    return code(.ok);
}

/// Closes a C appender handle.
pub export fn ringloom_appender_close(a: ?*ringloom_appender_t) void {
    const handle = appenderHandle(a) orelse return;
    const allocator = handle.appender.queue.allocator;
    handle.appender.close();
    allocator.destroy(handle);
}

/// Opens a C tailer handle at `start_index`.
pub export fn ringloom_tailer_open(q: ?*ringloom_queue_t, start_index: u64, out: ?*?*ringloom_tailer_t) c_int {
    const queue_handle = queueHandle(q) orelse return code(.invalid_argument);
    const out_ptr = out orelse return code(.invalid_argument);
    out_ptr.* = null;

    const actual_start = if (start_index == 0 and queue_handle.queue.lowest_cycle != 0)
        @import("index.zig").Index.compose(@intCast(queue_handle.queue.lowest_cycle), 0)
    else
        start_index;
    const tailer = Tailer.create(queue_handle.queue, actual_start) catch |err| return errorCode(err);
    const handle = queue_handle.queue.allocator.create(TailerHandle) catch |err| {
        tailer.deinit();
        return errorCode(err);
    };
    handle.* = .{ .tailer = tailer };
    out_ptr.* = @ptrCast(handle);
    return code(.ok);
}

/// Non-blocking poll; returned data is borrowed until the next tailer call or close.
pub export fn ringloom_tailer_poll(t: ?*ringloom_tailer_t, out: ?*ringloom_message_view) c_int {
    const handle = tailerHandle(t) orelse return code(.invalid_argument);
    const out_ptr = out orelse return code(.invalid_argument);
    if (out_ptr.size < @sizeOf(ringloom_message_view)) return code(.invalid_argument);

    const entry = handle.tailer.pollWithCodec([]const u8, codec.RawCodec) catch |err| return errorCode(err);
    if (entry) |msg| {
        out_ptr.index = msg.index;
        out_ptr.data = msg.message.ptr;
        out_ptr.data_len = msg.message.len;
        return code(.ok);
    }
    out_ptr.data = null;
    out_ptr.data_len = 0;
    return code(.ok_not_ready);
}

/// Drives bounded read-side prefetch work for a tailer.
pub export fn ringloom_tailer_prefetch_poll(
    t: ?*ringloom_tailer_t,
    max_work_units: u32,
    out: ?*ringloom_step_result_t,
) c_int {
    const handle = tailerHandle(t) orelse return code(.invalid_argument);
    const step = handle.tailer.prefetchPoll(max_work_units) catch |err| return errorCode(err);
    if (out) |out_ptr| out_ptr.* = stepCode(step);
    return code(.ok);
}

/// Closes a C tailer handle.
pub export fn ringloom_tailer_close(t: ?*ringloom_tailer_t) void {
    const handle = tailerHandle(t) orelse return;
    const allocator = handle.tailer.queue.allocator;
    handle.tailer.deinit();
    allocator.destroy(handle);
}

/// Drives bounded queue maintenance work.
pub export fn ringloom_queue_maintenance_poll(
    q: ?*ringloom_queue_t,
    max_work_units: u32,
    out: ?*ringloom_step_result_t,
) c_int {
    const handle = queueHandle(q) orelse return code(.invalid_argument);
    const step = handle.queue.maintenancePoll(max_work_units) catch |err| return errorCode(err);
    if (out) |out_ptr| out_ptr.* = stepCode(step);
    return code(.ok);
}

/// Alias for queue-level maintenance polling.
pub export fn ringloom_queue_prefetch_poll(
    q: ?*ringloom_queue_t,
    max_work_units: u32,
    out: ?*ringloom_step_result_t,
) c_int {
    return ringloom_queue_maintenance_poll(q, max_work_units, out);
}

/// Alias for queue-level maintenance polling.
pub export fn ringloom_queue_cleaner_poll(
    q: ?*ringloom_queue_t,
    max_work_units: u32,
    out: ?*ringloom_step_result_t,
) c_int {
    return ringloom_queue_maintenance_poll(q, max_work_units, out);
}

fn queueHandle(q: ?*ringloom_queue_t) ?*QueueHandle {
    return @ptrCast(@alignCast(q orelse return null));
}

fn appenderHandle(a: ?*ringloom_appender_t) ?*AppenderHandle {
    return @ptrCast(@alignCast(a orelse return null));
}

fn tailerHandle(t: ?*ringloom_tailer_t) ?*TailerHandle {
    return @ptrCast(@alignCast(t orelse return null));
}

fn stepCode(step: @import("platform.zig").StepResult) ringloom_step_result_t {
    return switch (step) {
        .idle => .idle,
        .made_progress => .progress,
        .more_work => .more_work,
    };
}

fn code(err: ringloom_error_t) c_int {
    return @intFromEnum(err);
}

fn errorCode(err: anyerror) c_int {
    return switch (err) {
        error.OutOfMemory => code(.out_of_memory),
        error.QueueNotFound, error.FileNotFound => code(.queue_not_found),
        error.MetadataCorrupt, error.InvalidMagic, error.InvalidVersion, error.InvalidQueueFileHeader, error.QueueFileMagicMismatch, error.QueueFileVersionMismatch => code(.metadata_corrupt),
        error.MmapFailed => code(.mmap_failed),
        error.AppenderAlreadyOpen => code(.appender_already_open),
        error.MessageTooLarge => code(.message_too_large),
        error.EmptyPayload => code(.empty_payload),
        error.ParseFailed => code(.parse_failed),
        else => code(.internal_error),
    };
}

fn nowMs(io: std.Io) !u64 {
    const ms = std.Io.Clock.real.now(io).toMilliseconds();
    if (ms < 0) return error.InvalidRollConfig;
    return @intCast(ms);
}

test "C ABI opens queue appends and polls raw view" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
    defer allocator.free(path);

    const scheme = "TEST4_SECONDLY";
    var opts = ringloom_queue_options{
        .dir = path.ptr,
        .dir_len = path.len,
        .create = true,
        .roll_scheme_name = scheme.ptr,
        .roll_scheme_name_len = scheme.len,
    };
    var q: ?*ringloom_queue_t = null;
    try std.testing.expectEqual(code(.ok), ringloom_queue_open(&opts, &q));
    defer ringloom_queue_close(q);

    var app: ?*ringloom_appender_t = null;
    try std.testing.expectEqual(code(.ok), ringloom_appender_open(q, &app));

    var idx: u64 = 0;
    try std.testing.expectEqual(code(.ok), ringloom_appender_append(app, "hello".ptr, 5, &idx));
    ringloom_appender_close(app);

    var tailer: ?*ringloom_tailer_t = null;
    try std.testing.expectEqual(code(.ok), ringloom_tailer_open(q, 0, &tailer));
    defer ringloom_tailer_close(tailer);

    var view: ringloom_message_view = .{};
    try std.testing.expectEqual(code(.ok), ringloom_tailer_poll(tailer, &view));
    try std.testing.expectEqual(idx, view.index);
    const bytes: [*]const u8 = @ptrCast(view.data.?);
    try std.testing.expectEqualStrings("hello", bytes[0..view.data_len]);
}
