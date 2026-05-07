//! Opt-in benchmark binary for ringloom-queue.
//!
//! Measures latency (p50/p95/p99/p99.9) and throughput for both the appender
//! hot path and the tailer poll path. Warmup, message count, and message size
//! are all configurable via CLI args.
//!
//! Usage (via the build system):
//!     zig build bench -- [--warmup=N] [--count=N] [--size=N] [--no-tailer] [--keep-dir] [--help]

const std = @import("std");
const ringloom = @import("ringloom_queue");

const BenchOptions = struct {
    warmup: usize = 10_000,
    count: usize = 1_000_000,
    size: usize = 64,
    skip_tailer: bool = false,
    keep_dir: bool = false,
};

const BenchmarkPlan = struct {
    scheme: ringloom.RollScheme,
    entry_size: u64,
    messages_per_cycle: u64,
    cycles_needed: u64,
    roll_length_ms: u64,
    base_ts_ms: u64,
};

const LatencyStats = struct {
    samples: usize,
    min_ns: u64,
    max_ns: u64,
    mean_ns: u64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
};

const RunResult = struct {
    label: []const u8,
    elapsed_ns: u64,
    payload_size: usize,
    stats: LatencyStats,
    first_index: u64 = 0,

    fn throughputMsgsPerSec(self: RunResult) f64 {
        if (self.elapsed_ns == 0) return 0;
        return @as(f64, @floatFromInt(self.stats.samples)) * 1e9 / @as(f64, @floatFromInt(self.elapsed_ns));
    }

    fn throughputMiBPerSec(self: RunResult) f64 {
        const total_bytes: f64 = @floatFromInt(self.stats.samples * self.payload_size);
        const seconds: f64 = @as(f64, @floatFromInt(self.elapsed_ns)) / 1e9;
        if (seconds == 0) return 0;
        return total_bytes / seconds / (1024.0 * 1024.0);
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const opts = parseArgs(init) catch |err| switch (err) {
        error.HelpRequested => {
            try printHelp(io);
            return;
        },
        else => return err,
    };

    const plan = try buildPlan(opts);

    const cwd = std.Io.Dir.cwd();
    const path = try std.fmt.allocPrint(
        allocator,
        ".zig-cache/tmp/ringloom-bench-{d}",
        .{@as(i128, std.Io.Clock.awake.now(io).toNanoseconds())},
    );
    defer allocator.free(path);
    cwd.deleteTree(io, path) catch {};
    defer if (!opts.keep_dir) cwd.deleteTree(io, path) catch {};

    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = path,
        .create = true,
        .roll_scheme = plan.scheme,
        .allocator = allocator,
        .spawn_helper_threads = true,
        .enable_prefetcher = true,
        .preroll_ms = 0,
    }, ringloom.RawCodec);
    defer queue.deinit();

    const payload = try allocator.alloc(u8, opts.size);
    defer allocator.free(payload);
    @memset(payload, 'A');
    if (opts.size > 0) payload[0] = '!';
    if (opts.size > 1) payload[opts.size - 1] = '!';

    const latencies = try allocator.alloc(u64, opts.count);
    defer allocator.free(latencies);

    const clock_overhead_ns = measureClockOverhead(io);

    try printPreamble(io, opts, plan, clock_overhead_ns);

    const append_result = try runAppendBench(&queue, payload, latencies, opts, plan, io);

    const maybe_tailer = if (!opts.skip_tailer)
        try runTailerBench(&queue, append_result.first_index, opts, latencies, io)
    else
        null;

    try printResultsTable(io, opts, append_result, maybe_tailer);

    try printDiagnostics(io, queue.diagnostics());
}

// -----------------------------------------------------------------------------
// CLI parsing & help
// -----------------------------------------------------------------------------

fn parseArgs(init: std.process.Init) !BenchOptions {
    var opts: BenchOptions = .{};
    var it = init.minimal.args.iterate();
    _ = it.next(); // skip program name
    while (it.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            return error.HelpRequested;
        } else if (std.mem.eql(u8, arg, "--no-tailer")) {
            opts.skip_tailer = true;
        } else if (std.mem.eql(u8, arg, "--keep-dir")) {
            opts.keep_dir = true;
        } else if (parseUsizeFlag(arg, "--warmup=")) |v| {
            opts.warmup = v;
        } else if (parseUsizeFlag(arg, "--count=")) |v| {
            opts.count = v;
        } else if (parseUsizeFlag(arg, "--size=")) |v| {
            opts.size = v;
        } else {
            std.log.err("unknown argument: {s}", .{arg});
            return error.InvalidArgument;
        }
    }
    if (opts.count == 0) return error.InvalidArgument;
    if (opts.size == 0) return error.InvalidArgument;
    return opts;
}

fn parseUsizeFlag(arg: []const u8, prefix: []const u8) ?usize {
    if (!std.mem.startsWith(u8, arg, prefix)) return null;
    return std.fmt.parseInt(usize, arg[prefix.len..], 10) catch null;
}

fn printHelp(io: std.Io) !void {
    const text =
        \\ringloom-queue benchmark
        \\
        \\Options:
        \\  --warmup=N      Warmup append count (default: 10000)
        \\  --count=N       Measured message count (default: 1000000)
        \\  --size=N        Payload size in bytes (default: 64)
        \\  --no-tailer     Skip the tailer poll benchmark
        \\  --keep-dir      Do not delete the temporary queue directory on exit
        \\  -h, --help      Show this message
        \\
    ;
    var buf: [1024]u8 = undefined;
    var w: std.Io.File.Writer = .init(.stdout(), io, &buf);
    try w.interface.writeAll(text);
    try w.interface.flush();
}

// -----------------------------------------------------------------------------
// Capacity planning
// -----------------------------------------------------------------------------

const HEADER_SIZE: u64 = 4; // ringloom.Header.HEADER_SIZE
const QUEUE_FILE_HEADER_SIZE: u64 = 64;

fn entrySize(payload_size: usize) u64 {
    const padding: u64 = (0 -% @as(u64, payload_size)) & 0x03;
    return HEADER_SIZE + @as(u64, payload_size) + padding;
}

fn buildPlan(opts: BenchOptions) !BenchmarkPlan {
    const scheme = ringloom.roll.findSchemeByName("FAST_DAILY").?;
    const data_start = QUEUE_FILE_HEADER_SIZE + @as(u64, scheme.index_count) * 8;
    const entry_size = entrySize(opts.size);
    const capacity = ringloom.config.default_qf_disk_size;
    if (data_start >= capacity or entry_size > capacity - data_start) {
        std.log.err(
            "payload size {d} does not fit in a queue file with scheme {s}",
            .{ opts.size, scheme.name },
        );
        return error.BenchmarkConfigTooLarge;
    }

    const usable_bytes = capacity - data_start;
    const messages_per_cycle = @divFloor(usable_bytes, entry_size);
    if (messages_per_cycle == 0) return error.BenchmarkConfigTooLarge;

    const total_messages = try std.math.add(u64, opts.warmup, opts.count);
    const cycles_needed = if (total_messages == 0) 1 else @divFloor(total_messages + messages_per_cycle - 1, messages_per_cycle);
    if (cycles_needed > std.math.maxInt(u32)) {
        std.log.err(
            "configuration would need {d} cycles, exceeding the 32-bit cycle index limit",
            .{cycles_needed},
        );
        return error.BenchmarkConfigTooLarge;
    }

    const roll_length_ms = scheme.rollLengthMs();
    const max_cycle_offset = try std.math.mul(u64, cycles_needed - 1, roll_length_ms);
    const max_timestamp = try std.math.add(u64, 0, max_cycle_offset);
    _ = max_timestamp;

    return .{
        .scheme = scheme,
        .entry_size = entry_size,
        .messages_per_cycle = messages_per_cycle,
        .cycles_needed = cycles_needed,
        .roll_length_ms = roll_length_ms,
        .base_ts_ms = 0,
    };
}

fn timestampForOrdinal(plan: BenchmarkPlan, ordinal: u64) u64 {
    const cycle = @divFloor(ordinal, plan.messages_per_cycle);
    return plan.base_ts_ms + cycle * plan.roll_length_ms;
}

// -----------------------------------------------------------------------------
// Benchmarks
// -----------------------------------------------------------------------------

fn runAppendBench(
    queue: *ringloom.Queue([]const u8),
    payload: []const u8,
    latencies: []u64,
    opts: BenchOptions,
    plan: BenchmarkPlan,
    io: std.Io,
) !RunResult {
    // Warmup: untimed appends to fault in mappings and trigger prefetcher work.
    // When the configured payload count exceeds a single queue file, the
    // benchmark advances the append timestamp by one roll interval after the
    // per-cycle capacity is reached so the run spans multiple queue files.
    var i: usize = 0;
    while (i < opts.warmup) : (i += 1) {
        _ = try queue.appendWithTimestamp(payload, timestampForOrdinal(plan, i));
    }

    var first_measured_index: ?u64 = null;
    const wall_start = std.Io.Clock.awake.now(io);
    i = 0;
    while (i < opts.count) : (i += 1) {
        const ordinal = @as(u64, opts.warmup) + i;
        const t0 = std.Io.Clock.awake.now(io);
        const idx = try queue.appendWithTimestamp(payload, timestampForOrdinal(plan, ordinal));
        const t1 = std.Io.Clock.awake.now(io);
        if (first_measured_index == null) first_measured_index = idx;
        latencies[i] = durationNs(t0, t1);
    }
    const wall_elapsed = durationNs(wall_start, std.Io.Clock.awake.now(io));

    return .{
        .label = "append",
        .elapsed_ns = wall_elapsed,
        .payload_size = opts.size,
        .stats = computeStats(latencies),
        .first_index = first_measured_index.?,
    };
}

fn runTailerBench(
    queue: *ringloom.Queue([]const u8),
    first_measured_index: u64,
    opts: BenchOptions,
    latencies: []u64,
    io: std.Io,
) !RunResult {
    var tailer = try queue.tailer(first_measured_index);
    defer tailer.deinit();

    const wall_start = std.Io.Clock.awake.now(io);
    var i: usize = 0;
    while (i < opts.count) {
        const t0 = std.Io.Clock.awake.now(io);
        const maybe_entry = try tailer.poll();
        const t1 = std.Io.Clock.awake.now(io);
        if (maybe_entry == null) {
            // Should not happen: the queue is fully written before this loop.
            std.atomic.spinLoopHint();
            continue;
        }
        latencies[i] = durationNs(t0, t1);
        i += 1;
    }
    const wall_elapsed = durationNs(wall_start, std.Io.Clock.awake.now(io));

    return .{
        .label = "tailer poll",
        .elapsed_ns = wall_elapsed,
        .payload_size = opts.size,
        .stats = computeStats(latencies),
    };
}

// -----------------------------------------------------------------------------
// Stats
// -----------------------------------------------------------------------------

fn durationNs(a: std.Io.Timestamp, b: std.Io.Timestamp) u64 {
    const ns: i96 = a.durationTo(b).toNanoseconds();
    if (ns < 0) return 0;
    return @intCast(ns);
}

fn computeStats(latencies: []u64) LatencyStats {
    std.mem.sort(u64, latencies, {}, std.sort.asc(u64));
    var sum: u128 = 0;
    for (latencies) |v| sum += v;
    const n = latencies.len;
    const mean: u64 = if (n == 0) 0 else @intCast(sum / n);
    return .{
        .samples = n,
        .min_ns = if (n == 0) 0 else latencies[0],
        .max_ns = if (n == 0) 0 else latencies[n - 1],
        .mean_ns = mean,
        .p50_ns = percentile(latencies, 0.50),
        .p95_ns = percentile(latencies, 0.95),
        .p99_ns = percentile(latencies, 0.99),
        .p999_ns = percentile(latencies, 0.999),
    };
}

fn percentile(sorted: []const u64, p: f64) u64 {
    if (sorted.len == 0) return 0;
    // Nearest-rank percentile, clamped to the last element.
    const rank = @as(f64, @floatFromInt(sorted.len)) * p;
    var idx: usize = @intFromFloat(@ceil(rank));
    if (idx == 0) idx = 1;
    if (idx > sorted.len) idx = sorted.len;
    return sorted[idx - 1];
}

fn measureClockOverhead(io: std.Io) u64 {
    var samples: [4096]u64 = undefined;
    for (&samples) |*s| {
        const a = std.Io.Clock.awake.now(io);
        const b = std.Io.Clock.awake.now(io);
        s.* = durationNs(a, b);
    }
    std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
    return samples[samples.len / 2];
}

// -----------------------------------------------------------------------------
// Output
// -----------------------------------------------------------------------------

const RowKind = enum {
    samples,
    msgs_per_sec,
    mib_per_sec,
    ns_min,
    ns_mean,
    ns_p50,
    ns_p95,
    ns_p99,
    ns_p999,
    ns_max,
};

fn printPreamble(io: std.Io, opts: BenchOptions, plan: BenchmarkPlan, clock_overhead_ns: u64) !void {
    var buf: [1536]u8 = undefined;
    var w: std.Io.File.Writer = .init(.stdout(), io, &buf);
    const out = &w.interface;
    try out.print(
        \\
        \\ringloom-queue benchmark
        \\  warmup messages : {d}
        \\  measured count  : {d}
        \\  payload size    : {d} bytes
        \\  tailer bench    : {s}
        \\  roll scheme     : {s}
        \\  entry size      : {d} bytes
        \\  msgs / cycle    : {d}
        \\  cycles needed   : {d}
        \\  clock overhead  : ~{d} ns / pair (median)
        \\
        \\
    , .{
        opts.warmup,
        opts.count,
        opts.size,
        if (opts.skip_tailer) "skipped" else "enabled",
        plan.scheme.name,
        plan.entry_size,
        plan.messages_per_cycle,
        plan.cycles_needed,
        clock_overhead_ns,
    });
    try out.flush();
}

fn printResultsTable(
    io: std.Io,
    opts: BenchOptions,
    append_result: RunResult,
    maybe_tailer: ?RunResult,
) !void {
    _ = opts;
    var buf: [4096]u8 = undefined;
    var w: std.Io.File.Writer = .init(.stdout(), io, &buf);
    const out = &w.interface;

    const Row = struct { name: []const u8, kind: RowKind };
    const rows = [_]Row{
        .{ .name = "samples", .kind = .samples },
        .{ .name = "throughput (msgs/s)", .kind = .msgs_per_sec },
        .{ .name = "throughput (MiB/s)", .kind = .mib_per_sec },
        .{ .name = "min (ns)", .kind = .ns_min },
        .{ .name = "mean (ns)", .kind = .ns_mean },
        .{ .name = "p50 (ns)", .kind = .ns_p50 },
        .{ .name = "p95 (ns)", .kind = .ns_p95 },
        .{ .name = "p99 (ns)", .kind = .ns_p99 },
        .{ .name = "p99.9 (ns)", .kind = .ns_p999 },
        .{ .name = "max (ns)", .kind = .ns_max },
    };

    const col_w: usize = 24;
    const metric_w: usize = 22;

    // Header.
    try out.print("+", .{});
    try writeDashes(out, metric_w + 2);
    try out.print("+", .{});
    try writeDashes(out, col_w + 2);
    if (maybe_tailer != null) {
        try out.print("+", .{});
        try writeDashes(out, col_w + 2);
    }
    try out.print("+\n", .{});

    try out.print("| {s:<22} | {s:<24} ", .{ "metric", append_result.label });
    if (maybe_tailer) |t| try out.print("| {s:<24} ", .{t.label});
    try out.print("|\n", .{});

    try out.print("+", .{});
    try writeDashes(out, metric_w + 2);
    try out.print("+", .{});
    try writeDashes(out, col_w + 2);
    if (maybe_tailer != null) {
        try out.print("+", .{});
        try writeDashes(out, col_w + 2);
    }
    try out.print("+\n", .{});

    for (rows) |row| {
        try out.print("| {s:<22} | ", .{row.name});
        try formatCell(out, row.kind, append_result);
        try out.print(" ", .{});
        if (maybe_tailer) |t| {
            try out.print("| ", .{});
            try formatCell(out, row.kind, t);
            try out.print(" ", .{});
        }
        try out.print("|\n", .{});
    }

    try out.print("+", .{});
    try writeDashes(out, metric_w + 2);
    try out.print("+", .{});
    try writeDashes(out, col_w + 2);
    if (maybe_tailer != null) {
        try out.print("+", .{});
        try writeDashes(out, col_w + 2);
    }
    try out.print("+\n", .{});

    try out.flush();
}

fn writeDashes(out: *std.Io.Writer, n: usize) !void {
    try out.splatByteAll('-', n);
}

fn formatCell(
    out: *std.Io.Writer,
    kind: RowKind,
    r: RunResult,
) !void {
    switch (kind) {
        .samples => try out.print("{d:>24}", .{r.stats.samples}),
        .msgs_per_sec => try out.print("{d:>24.0}", .{r.throughputMsgsPerSec()}),
        .mib_per_sec => try out.print("{d:>24.2}", .{r.throughputMiBPerSec()}),
        .ns_min => try out.print("{d:>24}", .{r.stats.min_ns}),
        .ns_mean => try out.print("{d:>24}", .{r.stats.mean_ns}),
        .ns_p50 => try out.print("{d:>24}", .{r.stats.p50_ns}),
        .ns_p95 => try out.print("{d:>24}", .{r.stats.p95_ns}),
        .ns_p99 => try out.print("{d:>24}", .{r.stats.p99_ns}),
        .ns_p999 => try out.print("{d:>24}", .{r.stats.p999_ns}),
        .ns_max => try out.print("{d:>24}", .{r.stats.max_ns}),
    }
}

fn printDiagnostics(io: std.Io, d: ringloom.Diagnostics) !void {
    var buf: [1024]u8 = undefined;
    var w: std.Io.File.Writer = .init(.stdout(), io, &buf);
    const out = &w.interface;
    try out.print(
        \\
        \\queue diagnostics:
        \\  appends                  : {d}
        \\  rolls                    : {d}
        \\  cas_retries              : {d}
        \\  synchronous_cycle_opens  : {d}
        \\  preroll_misses           : {d}
        \\  prefetcher_enabled       : {}
        \\  cleaner_enabled          : {}
        \\
    , .{
        d.appends,
        d.rolls,
        d.cas_retries,
        d.synchronous_cycle_opens,
        d.preroll_misses,
        d.prefetcher_enabled,
        d.cleaner_enabled,
    });
    try out.flush();
}
