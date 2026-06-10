// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

const Appender = @import("appender.zig").Appender;
const codec = @import("codec.zig");
const CoreQueue = @import("queue.zig").Queue;
const roll = @import("roll.zig");
const Tailer = @import("tailer.zig").Tailer;
const repl = @import("repl/mod.zig");
const repl_transport = repl.transport;

const CTransport = repl_transport.CTransport;
const SourceType = repl.ReplicationSource(CTransport.Out, CTransport.In);
const SinkType = repl.ReplicationSink(CTransport.Out, CTransport.In);

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
    repl_config_mismatch = -20,
    repl_sink_ahead = -21,
    repl_index_unavailable = -22,
    repl_gap_detected = -23,
    repl_corrupt_frame = -24,
    repl_transport_error = -25,
    repl_version_incompatible = -26,
    repl_queue_id_mismatch = -27,
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
        -20 => "replication config mismatch",
        -21 => "replication sink ahead of source",
        -22 => "replication index not available",
        -23 => "replication index gap detected",
        -24 => "replication corrupt frame",
        -25 => "replication transport error",
        -26 => "replication version incompatible",
        -27 => "replication queue id mismatch",
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

// ---------------------------------------------------------------------------
// Replication C ABI
// ---------------------------------------------------------------------------

/// Opaque replication source handle.
pub const ringloom_repl_source_t = opaque {};
/// Opaque replication sink handle.
pub const ringloom_repl_sink_t = opaque {};

pub const ringloom_offer_fn = *const fn (ctx: ?*anyopaque, buf: [*]const u8, len: usize) callconv(.c) i64;
pub const ringloom_is_connected_fn = *const fn (ctx: ?*anyopaque) callconv(.c) bool;
pub const ringloom_is_backpressured_fn = *const fn (ctx: ?*anyopaque) callconv(.c) bool;
pub const ringloom_close_fn = *const fn (ctx: ?*anyopaque) callconv(.c) void;
pub const ringloom_poll_fn = *const fn (ctx: ?*anyopaque, fragment_limit: u32) callconv(.c) u32;
pub const ringloom_next_frame_fn = *const fn (ctx: ?*anyopaque, out_len: *usize) callconv(.c) ?[*]const u8;

/// Outbound channel supplied by the consumer (function pointers + context).
pub const ringloom_outbound_channel = extern struct {
    size: u32 = @sizeOf(ringloom_outbound_channel),
    ctx: ?*anyopaque = null,
    offer: ringloom_offer_fn,
    is_connected: ringloom_is_connected_fn,
    is_backpressured: ringloom_is_backpressured_fn,
    close: ?ringloom_close_fn = null,
};

/// Inbound channel (pull model) supplied by the consumer.
pub const ringloom_inbound_channel = extern struct {
    size: u32 = @sizeOf(ringloom_inbound_channel),
    ctx: ?*anyopaque = null,
    poll: ringloom_poll_fn,
    next_frame: ringloom_next_frame_fn,
    is_connected: ringloom_is_connected_fn,
    close: ?ringloom_close_fn = null,
};

pub const ringloom_repl_source_options = extern struct {
    size: u32 = @sizeOf(ringloom_repl_source_options),
    queue: ?*ringloom_queue_t = null,
    queue_id: [16]u8 = [_]u8{0} ** 16,
    node_id: [16]u8 = [_]u8{0} ** 16,
    outbound: ringloom_outbound_channel,
    inbound: ringloom_inbound_channel,
    heartbeat_interval_ms: u64 = 100,
    batching_max_frames: u32 = 1,
    batching_max_window_nanos: u64 = 0,
    backpressure_watchdog_ms: u64 = 30_000,
    backpressure_fatal_ms: u64 = 0,
    error_ctx: ?*anyopaque = null,
    on_error: ?*const fn (ctx: ?*anyopaque, session_id: u64, code: c_int) callconv(.c) void = null,
};

pub const ringloom_repl_sink_options = extern struct {
    size: u32 = @sizeOf(ringloom_repl_sink_options),
    queue: ?*ringloom_queue_t = null,
    queue_id: [16]u8 = [_]u8{0} ** 16,
    node_id: [16]u8 = [_]u8{0} ** 16,
    outbound: ringloom_outbound_channel,
    inbound: ringloom_inbound_channel,
    ack_interval_ms: u64 = 100,
    force_ack_interval_ms: u64 = 1000,
    heartbeat_timeout_ms: u64 = 500,
    reconnect_backoff_min_ms: u64 = 50,
    reconnect_backoff_max_ms: u64 = 5000,
    error_ctx: ?*anyopaque = null,
    on_error: ?*const fn (ctx: ?*anyopaque, session_id: u64, code: c_int) callconv(.c) void = null,
};

pub const ringloom_repl_source_metrics_t = extern struct {
    size: u32 = @sizeOf(ringloom_repl_source_metrics_t),
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

pub const ringloom_repl_sink_metrics_t = extern struct {
    size: u32 = @sizeOf(ringloom_repl_sink_metrics_t),
    last_applied_index: i64 = -1,
    frames_applied: u64 = 0,
    bytes_applied: u64 = 0,
    replay_reset_count: u64 = 0,
    current_session_id: u64 = 0,
    lag_from_source_hwm: u64 = 0,
    gaps_detected: u64 = 0,
    decode_errors: u64 = 0,
};

const SourceHandle = struct {
    allocator: std.mem.Allocator,
    out_channel: ringloom_outbound_channel,
    in_channel: ringloom_inbound_channel,
    out: CTransport.Out,
    in: CTransport.In,
    source: SourceType,
};

const SinkHandle = struct {
    allocator: std.mem.Allocator,
    out_channel: ringloom_outbound_channel,
    in_channel: ringloom_inbound_channel,
    out: CTransport.Out,
    in: CTransport.In,
    sink: SinkType,
};

/// Forwards the internal (comptime) transport vtable to the consumer's C
/// function pointers. The indirect call lives here, at the boundary only.
const CChannelAdapters = struct {
    fn outOffer(ctx: *anyopaque, frame: []const u8) i64 {
        const ch: *const ringloom_outbound_channel = @ptrCast(@alignCast(ctx));
        return ch.offer(ch.ctx, frame.ptr, frame.len);
    }
    fn outConnected(ctx: *anyopaque) bool {
        const ch: *const ringloom_outbound_channel = @ptrCast(@alignCast(ctx));
        return ch.is_connected(ch.ctx);
    }
    fn outBackPressured(ctx: *anyopaque) bool {
        const ch: *const ringloom_outbound_channel = @ptrCast(@alignCast(ctx));
        return ch.is_backpressured(ch.ctx);
    }
    fn inPoll(ctx: *anyopaque, fragment_limit: u32) u32 {
        const ch: *const ringloom_inbound_channel = @ptrCast(@alignCast(ctx));
        return ch.poll(ch.ctx, fragment_limit);
    }
    fn inNextFrame(ctx: *anyopaque, out_len: *usize) ?[*]const u8 {
        const ch: *const ringloom_inbound_channel = @ptrCast(@alignCast(ctx));
        return ch.next_frame(ch.ctx, out_len);
    }
    fn inConnected(ctx: *anyopaque) bool {
        const ch: *const ringloom_inbound_channel = @ptrCast(@alignCast(ctx));
        return ch.is_connected(ch.ctx);
    }
};

const c_out_vtable = repl_transport.OutboundVTable{
    .offer = CChannelAdapters.outOffer,
    .is_connected = CChannelAdapters.outConnected,
    .is_back_pressured = CChannelAdapters.outBackPressured,
};

const c_in_vtable = repl_transport.InboundVTable{
    .poll = CChannelAdapters.inPoll,
    .next_frame = CChannelAdapters.inNextFrame,
    .is_connected = CChannelAdapters.inConnected,
};

fn replSourceHandle(s: ?*ringloom_repl_source_t) ?*SourceHandle {
    return @ptrCast(@alignCast(s orelse return null));
}

fn replSinkHandle(s: ?*ringloom_repl_sink_t) ?*SinkHandle {
    return @ptrCast(@alignCast(s orelse return null));
}

fn fieldCovered(size: u32, comptime T: type, comptime field: []const u8) bool {
    return size >= @offsetOf(T, field) + @sizeOf(@FieldType(T, field));
}

fn msToNs(ms: u64) u64 {
    return ms *| std.time.ns_per_ms;
}

fn saltFromNodeId(node_id: [16]u8) u32 {
    return std.mem.readInt(u32, node_id[0..4], .little);
}

/// Opens a replication source bound to `queue`; stores the handle in `out`.
pub export fn ringloom_repl_source_open(opts: ?*const ringloom_repl_source_options, out: ?*?*ringloom_repl_source_t) c_int {
    const o = opts orelse return code(.invalid_argument);
    const out_ptr = out orelse return code(.invalid_argument);
    out_ptr.* = null;
    if (o.size < @offsetOf(ringloom_repl_source_options, "inbound") + @sizeOf(ringloom_inbound_channel)) {
        return code(.invalid_argument);
    }
    const queue_handle = queueHandle(o.queue) orelse return code(.invalid_argument);
    const allocator = queue_handle.queue.allocator;

    const handle = allocator.create(SourceHandle) catch |err| return errorCode(err);
    errdefer allocator.destroy(handle);

    handle.allocator = allocator;
    handle.out_channel = o.outbound;
    handle.in_channel = o.inbound;
    handle.out = .{ .ctx = @ptrCast(&handle.out_channel), .vt = &c_out_vtable };
    handle.in = .{ .ctx = @ptrCast(&handle.in_channel), .vt = &c_in_vtable };

    var cfg = repl.SourceConfig{ .node_salt = saltFromNodeId(o.node_id) };
    if (fieldCovered(o.size, ringloom_repl_source_options, "heartbeat_interval_ms"))
        cfg.heartbeat_interval_ns = msToNs(o.heartbeat_interval_ms);
    if (fieldCovered(o.size, ringloom_repl_source_options, "backpressure_watchdog_ms"))
        cfg.backpressure_watchdog_ns = msToNs(o.backpressure_watchdog_ms);
    if (fieldCovered(o.size, ringloom_repl_source_options, "backpressure_fatal_ms"))
        cfg.backpressure_fatal_ns = msToNs(o.backpressure_fatal_ms);

    handle.source = SourceType.init(allocator, queue_handle.queue, &handle.out, &handle.in, cfg);
    out_ptr.* = @ptrCast(handle);
    return code(.ok);
}

/// Drives one bounded, non-blocking pass of the source state machine.
pub export fn ringloom_repl_source_step(s: ?*ringloom_repl_source_t, max_work_units: u32, out: ?*ringloom_step_result_t) c_int {
    const handle = replSourceHandle(s) orelse return code(.invalid_argument);
    const step = handle.source.step(max_work_units) catch |err| return errorCode(err);
    if (out) |out_ptr| out_ptr.* = stepCode(step);
    return code(.ok);
}

/// Copies a snapshot of the source metrics into `out`.
pub export fn ringloom_repl_source_metrics(s: ?*ringloom_repl_source_t, out: ?*ringloom_repl_source_metrics_t) c_int {
    const handle = replSourceHandle(s) orelse return code(.invalid_argument);
    const out_ptr = out orelse return code(.invalid_argument);
    if (out_ptr.size < @sizeOf(ringloom_repl_source_metrics_t)) return code(.invalid_argument);
    const m = handle.source.metrics;
    out_ptr.* = .{
        .hwm_index = m.hwm_index,
        .frames_sent = m.frames_sent,
        .bytes_sent = m.bytes_sent,
        .backpressure_nanos = m.backpressure_nanos,
        .active_sessions = m.active_sessions,
        .cycles_rolled = m.cycles_rolled,
        .hello_nacks = m.hello_nacks,
        .decode_errors = m.decode_errors,
        .unexpected_frames = m.unexpected_frames,
    };
    return code(.ok);
}

/// Closes a replication source handle (does not close the bound queue).
pub export fn ringloom_repl_source_close(s: ?*ringloom_repl_source_t) void {
    const handle = replSourceHandle(s) orelse return;
    const allocator = handle.allocator;
    handle.source.deinit();
    allocator.destroy(handle);
}

/// Opens a replication sink bound to `queue`; stores the handle in `out`.
pub export fn ringloom_repl_sink_open(opts: ?*const ringloom_repl_sink_options, out: ?*?*ringloom_repl_sink_t) c_int {
    const o = opts orelse return code(.invalid_argument);
    const out_ptr = out orelse return code(.invalid_argument);
    out_ptr.* = null;
    if (o.size < @offsetOf(ringloom_repl_sink_options, "inbound") + @sizeOf(ringloom_inbound_channel)) {
        return code(.invalid_argument);
    }
    const queue_handle = queueHandle(o.queue) orelse return code(.invalid_argument);
    const allocator = queue_handle.queue.allocator;

    const handle = allocator.create(SinkHandle) catch |err| return errorCode(err);
    errdefer allocator.destroy(handle);

    handle.allocator = allocator;
    handle.out_channel = o.outbound;
    handle.in_channel = o.inbound;
    handle.out = .{ .ctx = @ptrCast(&handle.out_channel), .vt = &c_out_vtable };
    handle.in = .{ .ctx = @ptrCast(&handle.in_channel), .vt = &c_in_vtable };

    var cfg = repl.SinkConfig{ .node_id = o.node_id, .queue_id = o.queue_id };
    if (fieldCovered(o.size, ringloom_repl_sink_options, "ack_interval_ms"))
        cfg.ack_interval_ns = msToNs(o.ack_interval_ms);
    if (fieldCovered(o.size, ringloom_repl_sink_options, "force_ack_interval_ms"))
        cfg.force_ack_interval_ns = msToNs(o.force_ack_interval_ms);
    if (fieldCovered(o.size, ringloom_repl_sink_options, "heartbeat_timeout_ms"))
        cfg.heartbeat_timeout_ns = msToNs(o.heartbeat_timeout_ms);
    if (fieldCovered(o.size, ringloom_repl_sink_options, "reconnect_backoff_min_ms"))
        cfg.backoff.min_ms = o.reconnect_backoff_min_ms;
    if (fieldCovered(o.size, ringloom_repl_sink_options, "reconnect_backoff_max_ms"))
        cfg.backoff.max_ms = o.reconnect_backoff_max_ms;

    handle.sink = SinkType.init(allocator, queue_handle.queue, &handle.out, &handle.in, cfg) catch |err| return errorCode(err);
    out_ptr.* = @ptrCast(handle);
    return code(.ok);
}

/// Drives one bounded, non-blocking pass of the sink state machine.
pub export fn ringloom_repl_sink_step(s: ?*ringloom_repl_sink_t, max_work_units: u32, out: ?*ringloom_step_result_t) c_int {
    const handle = replSinkHandle(s) orelse return code(.invalid_argument);
    const step = handle.sink.step(max_work_units) catch |err| return errorCode(err);
    if (out) |out_ptr| out_ptr.* = stepCode(step);
    return code(.ok);
}

/// Reports the last index applied to the follower queue (-1 when none).
pub export fn ringloom_repl_sink_last_applied_index(s: ?*ringloom_repl_sink_t, out: ?*i64) c_int {
    const handle = replSinkHandle(s) orelse return code(.invalid_argument);
    const out_ptr = out orelse return code(.invalid_argument);
    out_ptr.* = handle.sink.last_applied_index;
    return code(.ok);
}

/// Copies a snapshot of the sink metrics into `out`.
pub export fn ringloom_repl_sink_metrics(s: ?*ringloom_repl_sink_t, out: ?*ringloom_repl_sink_metrics_t) c_int {
    const handle = replSinkHandle(s) orelse return code(.invalid_argument);
    const out_ptr = out orelse return code(.invalid_argument);
    if (out_ptr.size < @sizeOf(ringloom_repl_sink_metrics_t)) return code(.invalid_argument);
    const m = handle.sink.metrics;
    out_ptr.* = .{
        .last_applied_index = m.last_applied_index,
        .frames_applied = m.frames_applied,
        .bytes_applied = m.bytes_applied,
        .replay_reset_count = m.replay_reset_count,
        .current_session_id = m.current_session_id,
        .lag_from_source_hwm = m.lag_from_source_hwm,
        .gaps_detected = m.gaps_detected,
        .decode_errors = m.decode_errors,
    };
    return code(.ok);
}

/// Closes a replication sink handle (does not close the bound queue).
pub export fn ringloom_repl_sink_close(s: ?*ringloom_repl_sink_t) void {
    const handle = replSinkHandle(s) orelse return;
    const allocator = handle.allocator;
    handle.sink.deinit();
    allocator.destroy(handle);
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
        error.ConfigMismatch => code(.repl_config_mismatch),
        error.SinkAheadOfSource => code(.repl_sink_ahead),
        error.IndexNotAvailable => code(.repl_index_unavailable),
        error.IndexGap, error.DuplicateIndex => code(.repl_gap_detected),
        error.BadMagic, error.UnsupportedVersion, error.TruncatedFrame, error.BufferTooSmall => code(.repl_corrupt_frame),
        error.TransportError, error.BackpressureWatchdog, error.SecondSource => code(.repl_transport_error),
        error.VersionIncompatible => code(.repl_version_incompatible),
        error.QueueIdMismatch => code(.repl_queue_id_mismatch),
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

const CTestRing = struct {
    const cap = 512;
    const slot = 8192;
    bufs: [][slot]u8,
    lens: [cap]usize = [_]usize{0} ** cap,
    head: usize = 0,
    tail: usize = 0,
    count: usize = 0,
    readable: usize = 0,

    fn offer(ctx: ?*anyopaque, buf: [*]const u8, len: usize) callconv(.c) i64 {
        const self: *CTestRing = @ptrCast(@alignCast(ctx.?));
        if (self.count >= cap or len > slot) return -1;
        @memcpy(self.bufs[self.tail][0..len], buf[0..len]);
        self.lens[self.tail] = len;
        self.tail = (self.tail + 1) % cap;
        self.count += 1;
        return 0;
    }
    fn connected(ctx: ?*anyopaque) callconv(.c) bool {
        _ = ctx;
        return true;
    }
    fn backpressured(ctx: ?*anyopaque) callconv(.c) bool {
        const self: *CTestRing = @ptrCast(@alignCast(ctx.?));
        return self.count >= cap;
    }
    fn poll(ctx: ?*anyopaque, limit: u32) callconv(.c) u32 {
        const self: *CTestRing = @ptrCast(@alignCast(ctx.?));
        self.readable = @min(self.count, limit);
        return @intCast(self.readable);
    }
    fn nextFrame(ctx: ?*anyopaque, out_len: *usize) callconv(.c) ?[*]const u8 {
        const self: *CTestRing = @ptrCast(@alignCast(ctx.?));
        if (self.readable == 0) return null;
        const idx = self.head;
        out_len.* = self.lens[idx];
        self.head = (self.head + 1) % cap;
        self.count -= 1;
        self.readable -= 1;
        return &self.bufs[idx];
    }
};

test "C ABI replication loopback replicates master to follower" {
    const allocator = std.testing.allocator;

    var tmp_m = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_m.cleanup();
    var tmp_f = std.testing.tmpDir(.{ .iterate = true });
    defer tmp_f.cleanup();
    const master_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp_m.sub_path[0..]});
    defer allocator.free(master_path);
    const follower_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp_f.sub_path[0..]});
    defer allocator.free(follower_path);

    const scheme = "TEST4_SECONDLY";
    var master_opts = ringloom_queue_options{
        .dir = master_path.ptr,
        .dir_len = master_path.len,
        .create = true,
        .roll_scheme_name = scheme.ptr,
        .roll_scheme_name_len = scheme.len,
    };
    var follower_opts = master_opts;
    follower_opts.dir = follower_path.ptr;
    follower_opts.dir_len = follower_path.len;

    var master_q: ?*ringloom_queue_t = null;
    var follower_q: ?*ringloom_queue_t = null;
    try std.testing.expectEqual(code(.ok), ringloom_queue_open(&master_opts, &master_q));
    defer ringloom_queue_close(master_q);
    try std.testing.expectEqual(code(.ok), ringloom_queue_open(&follower_opts, &follower_q));
    defer ringloom_queue_close(follower_q);

    var app: ?*ringloom_appender_t = null;
    try std.testing.expectEqual(code(.ok), ringloom_appender_open(master_q, &app));
    var last_index: u64 = 0;
    const payloads = [_][]const u8{ "alpha", "beta", "gamma", "delta", "epsilon" };
    for (payloads) |p| {
        try std.testing.expectEqual(code(.ok), ringloom_appender_append(app, p.ptr, p.len, &last_index));
    }
    ringloom_appender_close(app);

    const s2k_bufs = try allocator.alloc([CTestRing.slot]u8, CTestRing.cap);
    defer allocator.free(s2k_bufs);
    const k2s_bufs = try allocator.alloc([CTestRing.slot]u8, CTestRing.cap);
    defer allocator.free(k2s_bufs);
    var s2k = CTestRing{ .bufs = s2k_bufs };
    var k2s = CTestRing{ .bufs = k2s_bufs };

    const src_out = ringloom_outbound_channel{ .ctx = &s2k, .offer = CTestRing.offer, .is_connected = CTestRing.connected, .is_backpressured = CTestRing.backpressured };
    const src_in = ringloom_inbound_channel{ .ctx = &k2s, .poll = CTestRing.poll, .next_frame = CTestRing.nextFrame, .is_connected = CTestRing.connected };
    const snk_out = ringloom_outbound_channel{ .ctx = &k2s, .offer = CTestRing.offer, .is_connected = CTestRing.connected, .is_backpressured = CTestRing.backpressured };
    const snk_in = ringloom_inbound_channel{ .ctx = &s2k, .poll = CTestRing.poll, .next_frame = CTestRing.nextFrame, .is_connected = CTestRing.connected };

    var src_opts = ringloom_repl_source_options{ .queue = master_q, .outbound = src_out, .inbound = src_in };
    var snk_opts = ringloom_repl_sink_options{ .queue = follower_q, .outbound = snk_out, .inbound = snk_in };

    var src: ?*ringloom_repl_source_t = null;
    var snk: ?*ringloom_repl_sink_t = null;
    try std.testing.expectEqual(code(.ok), ringloom_repl_source_open(&src_opts, &src));
    defer ringloom_repl_source_close(src);
    try std.testing.expectEqual(code(.ok), ringloom_repl_sink_open(&snk_opts, &snk));
    defer ringloom_repl_sink_close(snk);

    var applied: i64 = -1;
    var iterations: usize = 0;
    while (iterations < 100000) : (iterations += 1) {
        var sr: ringloom_step_result_t = .idle;
        var kr: ringloom_step_result_t = .idle;
        try std.testing.expectEqual(code(.ok), ringloom_repl_sink_step(snk, 64, &kr));
        try std.testing.expectEqual(code(.ok), ringloom_repl_source_step(src, 64, &sr));
        try std.testing.expectEqual(code(.ok), ringloom_repl_sink_last_applied_index(snk, &applied));
        if (applied == @as(i64, @intCast(last_index))) break;
    }
    try std.testing.expectEqual(@as(i64, @intCast(last_index)), applied);

    var metrics: ringloom_repl_sink_metrics_t = .{};
    try std.testing.expectEqual(code(.ok), ringloom_repl_sink_metrics(snk, &metrics));
    try std.testing.expectEqual(@as(u64, payloads.len), metrics.frames_applied);

    var tailer: ?*ringloom_tailer_t = null;
    try std.testing.expectEqual(code(.ok), ringloom_tailer_open(follower_q, 0, &tailer));
    defer ringloom_tailer_close(tailer);
    for (payloads) |p| {
        var view: ringloom_message_view = .{};
        try std.testing.expectEqual(code(.ok), ringloom_tailer_poll(tailer, &view));
        const bytes: [*]const u8 = @ptrCast(view.data.?);
        try std.testing.expectEqualStrings(p, bytes[0..view.data_len]);
    }
}
