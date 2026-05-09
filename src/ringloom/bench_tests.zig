//! Opt-in benchmark binary for ringloom-queue.
//!
//! Measures latency (p50/p95/p99/p99.9) and throughput for both the appender
//! hot path and the tailer poll path. Warmup, message count, and message size
//! are all configurable via CLI args.
//!
//! Usage (via the build system):
//!     zig build bench -- [--warmup=N] [--count=N] [--size=N] [--no-tailer] [--keep-dir] [--no-maintenance-threads] [--no-tailer-prefetch] [--retention-cycles=N] [--help]

const std = @import("std");
const ringloom = @import("ringloom_queue");

const BenchOptions = struct {
    warmup: usize = 10_000,
    count: usize = 1_000_000,
    size: usize = 64,
    skip_tailer: bool = false,
    keep_dir: bool = false,
    maintenance_threads: bool = true,
    tailer_prefetch: bool = true,
    retention_cycles: ?u32 = null,
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
    p9999_ns: u64,
};

const no_preroll_cycle = std.math.maxInt(u64);
const tailer_prefetch_poll_units: u32 = 64;
const tailer_prefetch_initial_wait_yields: usize = 4096;

const RunResult = struct {
    label: []const u8,
    elapsed_ns: u64,
    payload_size: usize,
    stats: LatencyStats,
    first_index: u64 = 0,
    prefetch_enabled: bool = false,
    prefetch_progress: u64 = 0,
    prefetch_idle: u64 = 0,
    prefetch_cleaner_progress: u64 = 0,
    prefetch_errors: u32 = 0,

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
        .spawn_helper_threads = false,
        .enable_prefetcher = true,
        .enable_cleaner = true,
        .retention_cycles = opts.retention_cycles,
        .preroll_ms = 0,
    }, ringloom.RawCodec);
    defer queue.deinit();

    var maintenance = BenchMaintenance.init(&queue);
    if (opts.maintenance_threads) try maintenance.start();
    defer maintenance.stopAndJoin();

    const payload = try allocator.alloc(u8, opts.size);
    defer allocator.free(payload);
    @memset(payload, 'A');
    if (opts.size > 0) payload[0] = '!';
    if (opts.size > 1) payload[opts.size - 1] = '!';

    const latencies = try allocator.alloc(u64, opts.count);
    defer allocator.free(latencies);

    const clock_overhead_ns = measureClockOverhead(io);

    try printPreamble(io, opts, plan, clock_overhead_ns);

    const append_result = try runAppendBench(&queue, &maintenance, payload, latencies, opts, plan, io);
    maintenance.stopAndJoin();

    const maybe_tailer = if (!opts.skip_tailer)
        try runTailerBench(&queue, append_result.first_index, opts, latencies, io)
    else
        null;

    try printResultsTable(io, opts, append_result, maybe_tailer);

    try printDiagnostics(io, queue.diagnostics());
    try printMaintenanceDiagnostics(io, maintenance);
    try printTailerPrefetchDiagnostics(io, maybe_tailer);
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
        } else if (std.mem.eql(u8, arg, "--no-maintenance-threads")) {
            opts.maintenance_threads = false;
        } else if (std.mem.eql(u8, arg, "--no-tailer-prefetch")) {
            opts.tailer_prefetch = false;
        } else if (parseUsizeFlag(arg, "--warmup=")) |v| {
            opts.warmup = v;
        } else if (parseUsizeFlag(arg, "--count=")) |v| {
            opts.count = v;
        } else if (parseUsizeFlag(arg, "--size=")) |v| {
            opts.size = v;
        } else if (parseUsizeFlag(arg, "--retention-cycles=")) |v| {
            if (v > std.math.maxInt(u32)) return error.InvalidArgument;
            opts.retention_cycles = @intCast(v);
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
        \\  --no-maintenance-threads
        \\                  Disable benchmark-owned prefetcher/cleaner threads
        \\  --no-tailer-prefetch
        \\                  Disable tailer read prefetch during tailer benchmark
        \\  --retention-cycles=N
        \\                  Enable cleaner retention during the append benchmark
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
    maintenance: *BenchMaintenance,
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
    maintenance.publishPrerollTarget(plan, 0);
    while (i < opts.warmup) : (i += 1) {
        maintenance.publishPrerollTarget(plan, i);
        _ = try queue.appendWithTimestamp(payload, timestampForOrdinal(plan, i));
    }

    var first_measured_index: ?u64 = null;
    const wall_start = std.Io.Clock.awake.now(io);
    i = 0;
    while (i < opts.count) : (i += 1) {
        const ordinal = @as(u64, opts.warmup) + i;
        maintenance.publishPrerollTarget(plan, ordinal);
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

    var tailer_prefetch = TailerPrefetch.init(&tailer);
    defer tailer_prefetch.stopAndJoin();
    if (opts.tailer_prefetch) {
        try tailer_prefetch.start();
        tailer_prefetch.waitForInitialProgress();
    }

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

    tailer_prefetch.stopAndJoin();

    return .{
        .label = "tailer poll",
        .elapsed_ns = wall_elapsed,
        .payload_size = opts.size,
        .stats = computeStats(latencies),
        .prefetch_enabled = opts.tailer_prefetch,
        .prefetch_progress = tailer_prefetch.progress.load(.acquire),
        .prefetch_idle = tailer_prefetch.idle.load(.acquire),
        .prefetch_cleaner_progress = tailer_prefetch.cleaner_progress.load(.acquire),
        .prefetch_errors = tailer_prefetch.errors.load(.acquire),
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
        .p9999_ns = percentile(latencies, 0.9999),
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

const BenchMaintenance = struct {
    queue: *ringloom.Queue([]const u8),
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    next_preroll_cycle: std.atomic.Value(u64) = std.atomic.Value(u64).init(no_preroll_cycle),
    prefetch_thread: ?std.Thread = null,
    cleaner_thread: ?std.Thread = null,
    prefetch_progress: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    cleaner_progress: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    errors: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    fn init(queue: *ringloom.Queue([]const u8)) BenchMaintenance {
        return .{ .queue = queue };
    }

    fn start(self: *BenchMaintenance) !void {
        self.stop.store(false, .release);
        self.prefetch_thread = try std.Thread.spawn(.{}, prefetchLoop, .{self});
        errdefer self.stopAndJoin();
        self.cleaner_thread = try std.Thread.spawn(.{}, cleanerLoop, .{self});
    }

    fn stopAndJoin(self: *BenchMaintenance) void {
        self.stop.store(true, .release);
        if (self.prefetch_thread) |thread| {
            thread.join();
            self.prefetch_thread = null;
        }
        if (self.cleaner_thread) |thread| {
            thread.join();
            self.cleaner_thread = null;
        }
    }

    fn publishPrerollTarget(self: *BenchMaintenance, plan: BenchmarkPlan, ordinal: usize) void {
        const current_cycle = @divFloor(@as(u64, ordinal), plan.messages_per_cycle);
        const next_cycle = current_cycle + 1;
        const target = if (next_cycle < plan.cycles_needed) next_cycle else no_preroll_cycle;
        self.next_preroll_cycle.store(target, .release);
    }

    fn prefetchLoop(self: *BenchMaintenance) void {
        while (!self.stop.load(.acquire)) {
            const cycle = self.next_preroll_cycle.load(.acquire);
            if (cycle != no_preroll_cycle) {
                if (self.queue.inner.prefetcher) |prefetcher| {
                    const step = prefetcher.prepareCycle(cycle) catch {
                        _ = self.errors.fetchAdd(1, .monotonic);
                        self.stop.store(true, .release);
                        return;
                    };
                    if (step != .idle) _ = self.prefetch_progress.fetchAdd(1, .monotonic);
                }
            }
            std.Thread.yield() catch {};
        }
    }

    fn cleanerLoop(self: *BenchMaintenance) void {
        while (!self.stop.load(.acquire)) {
            if (self.queue.inner.cleaner) |cleaner| {
                const step = cleaner.poll(16) catch {
                    _ = self.errors.fetchAdd(1, .monotonic);
                    self.stop.store(true, .release);
                    return;
                };
                if (step != .idle) _ = self.cleaner_progress.fetchAdd(1, .monotonic);
            }
            std.Thread.yield() catch {};
        }
    }
};

const TailerPrefetch = struct {
    tailer: *ringloom.PublicTailer([]const u8),
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    thread: ?std.Thread = null,
    progress: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    idle: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    cleaner_progress: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    errors: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    fn init(tailer: *ringloom.PublicTailer([]const u8)) TailerPrefetch {
        return .{ .tailer = tailer };
    }

    fn start(self: *TailerPrefetch) !void {
        self.stop.store(false, .release);
        self.thread = try std.Thread.spawn(.{}, loop, .{self});
    }

    fn stopAndJoin(self: *TailerPrefetch) void {
        self.stop.store(true, .release);
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    fn waitForInitialProgress(self: *TailerPrefetch) void {
        var yields: usize = 0;
        while (yields < tailer_prefetch_initial_wait_yields) : (yields += 1) {
            if (self.progress.load(.acquire) != 0 or self.errors.load(.acquire) != 0) return;
            std.Thread.yield() catch {};
        }
    }

    fn loop(self: *TailerPrefetch) void {
        while (!self.stop.load(.acquire)) {
            const step = self.tailer.prefetchPoll(tailer_prefetch_poll_units) catch {
                _ = self.errors.fetchAdd(1, .monotonic);
                self.stop.store(true, .release);
                return;
            };
            switch (step) {
                .idle => {
                    _ = self.idle.fetchAdd(1, .monotonic);
                    self.pollCleaner();
                    std.Thread.yield() catch {};
                },
                .made_progress => {
                    _ = self.progress.fetchAdd(1, .monotonic);
                    self.pollCleaner();
                    std.Thread.yield() catch {};
                },
                .more_work => {
                    _ = self.progress.fetchAdd(1, .monotonic);
                    self.pollCleaner();
                },
            }
        }
    }

    fn pollCleaner(self: *TailerPrefetch) void {
        if (self.tailer.inner.queue.cleaner) |cleaner| {
            const step = cleaner.poll(4) catch {
                _ = self.errors.fetchAdd(1, .monotonic);
                self.stop.store(true, .release);
                return;
            };
            if (step != .idle) _ = self.cleaner_progress.fetchAdd(1, .monotonic);
        }
    }
};

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
    ns_p9999,
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
        \\  tailer prefetch : {s}
        \\  maint threads   : {s}
        \\  roll scheme     : {s}
        \\  entry size      : {d} bytes
        \\  msgs / cycle    : {d}
        \\  cycles needed   : {d}
        \\  clock overhead  : ~{d} ns / pair (median)
        \\
    , .{
        opts.warmup,
        opts.count,
        opts.size,
        if (opts.skip_tailer) "skipped" else "enabled",
        if (opts.tailer_prefetch and !opts.skip_tailer) "threaded" else "disabled",
        if (opts.maintenance_threads) "enabled" else "disabled",
        plan.scheme.name,
        plan.entry_size,
        plan.messages_per_cycle,
        plan.cycles_needed,
        clock_overhead_ns,
    });
    if (opts.retention_cycles) |retention| {
        try out.print("  retention       : keep {d} cycles\n\n", .{retention});
    } else {
        try out.print("  retention       : disabled\n\n", .{});
    }
    try out.flush();
}

fn printTailerPrefetchDiagnostics(io: std.Io, maybe_tailer: ?RunResult) !void {
    const tailer = maybe_tailer orelse return;
    if (!tailer.prefetch_enabled) return;

    var buf: [512]u8 = undefined;
    var w: std.Io.File.Writer = .init(.stdout(), io, &buf);
    const out = &w.interface;
    try out.print(
        \\
        \\tailer prefetch helper:
        \\  progress polls           : {d}
        \\  idle polls               : {d}
        \\  cleaner progress polls   : {d}
        \\  helper errors            : {d}
        \\
    , .{
        tailer.prefetch_progress,
        tailer.prefetch_idle,
        tailer.prefetch_cleaner_progress,
        tailer.prefetch_errors,
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
        .{ .name = "p99.99 (ns)", .kind = .ns_p9999 },
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
        .ns_p9999 => try out.print("{d:>24}", .{r.stats.p9999_ns}),
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

fn printMaintenanceDiagnostics(io: std.Io, maintenance: BenchMaintenance) !void {
    var buf: [512]u8 = undefined;
    var w: std.Io.File.Writer = .init(.stdout(), io, &buf);
    const out = &w.interface;
    try out.print(
        \\
        \\benchmark maintenance threads:
        \\  prefetch progress polls   : {d}
        \\  cleaner progress polls    : {d}
        \\  helper errors             : {d}
        \\
    , .{
        maintenance.prefetch_progress.load(.acquire),
        maintenance.cleaner_progress.load(.acquire),
        maintenance.errors.load(.acquire),
    });
    try out.flush();
}
