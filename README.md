# ringloom-queue

`ringloom-queue` is a memory-mapped, append-only queue for Zig. It stores messages in fixed-size rolling cycle files, exposes a single-writer appender and independent tailers, and provides pollable maintenance helpers for prefetching, pre-roll creation, deferred cleanup, and retention.

The public API is type-safe: choose a message type and a codec, then open a `Queue(MessageType)`. Built-in codecs include `RawCodec`/`DefaultRawCodec` for byte slices, `TextCodec` for UTF-8 byte slices, and `StructCodec(T)` for fixed-layout value types.

## Build and test

```sh
zig build test --summary all
zig build bench -- --warmup=1000000 --count=10000000 --size=64
```

## Opening a queue

```zig
const std = @import("std");
const ringloom = @import("ringloom_queue");

pub fn main() !void {
    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = "data/events",
        .create = true,
        .roll_scheme = ringloom.roll.findSchemeByName("FAST_DAILY"),
        .allocator = std.heap.page_allocator,
    }, ringloom.RawCodec);
    defer queue.deinit();

    _ = try queue.append("hello ringloom");
}
```

Important `QueueConfig` fields:

| Field | Purpose |
| --- | --- |
| `dir` | Queue directory containing `metadata.ringloom` and cycle files. |
| `create` | Create a missing queue when `true`; otherwise open an existing queue. |
| `roll_scheme` | Optional cycle naming/duration/index layout. Defaults to `FAST_DAILY` when creating. |
| `allocator` | Allocator used for queue-owned paths, helpers, and handles. |
| `use_huge_pages` | Request best-effort huge-page mappings where supported. |
| `enable_prefetcher` | Enable pollable prefetch/pre-roll helper state. |
| `enable_cleaner` | Enable retention and deferred cleanup helper state. |
| `retention_cycles` | Optional number of recent cycles to keep. `null` keeps files indefinitely. |
| `preroll_ms` | How early to prepare the next appender cycle before a time roll. |

## Appending messages

Use `append()` for wall-clock timestamps, or `appendWithTimestamp()` when the caller already has an event timestamp in UTC milliseconds.

```zig
const index = try queue.append("payload");
_ = index;

const event_ts_ms: u64 = 1_771_000_000_000;
const historical_index = try queue.appendWithTimestamp("historical payload", event_ts_ms);
_ = historical_index;
```

Only one appender lease may be open for a queue at a time. The public `append` helpers open/reuse the queue appender internally.

## Maintenance

Maintenance is intentionally pollable and bounded. A call to `queue.inner.maintenancePoll(max_work_units)` performs up to that much prefetcher/cleaner work and returns a `StepResult`:

- `.idle` when no work was available
- `.made_progress` when work completed
- `.more_work` when more work is immediately available

The public `spawn_helper_threads` option records whether native helper threads are allowed, but applications that need dedicated maintenance threads should drive polling explicitly.

```zig
const std = @import("std");
const ringloom = @import("ringloom_queue");

const Maintenance = struct {
    queue: *ringloom.Queue([]const u8),
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    thread: ?std.Thread = null,

    fn start(self: *Maintenance) !void {
        self.stop.store(false, .release);
        self.thread = try std.Thread.spawn(.{}, loop, .{self});
    }

    fn stopAndJoin(self: *Maintenance) void {
        self.stop.store(true, .release);
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    fn loop(self: *Maintenance) void {
        while (!self.stop.load(.acquire)) {
            const step = self.queue.inner.maintenancePoll(16) catch return;
            if (step == .idle) std.Thread.yield() catch {};
        }
    }
};

var queue = try ringloom.Queue([]const u8).open(.{
    .dir = "data/events",
    .create = true,
    .enable_prefetcher = true,
    .prefetch_runway_bytes = 8 * 1024 * 1024,
    .read_prefetch_runway_bytes = 4 * 1024 * 1024,
    .enable_cleaner = true,
    .retention_cycles = 8,
    .preroll_ms = 1000,
    .spawn_helper_threads = false,
}, ringloom.RawCodec);
defer queue.deinit();

var maintenance = Maintenance{ .queue = &queue };
try maintenance.start();
defer maintenance.stopAndJoin();
```

Retention is conservative: the cleaner avoids deleting the current/highest cycle, a pre-roll cycle, an open appender cycle, and cycles still open by registered local tailers.

## Tailers

Tailers are independent read cursors. `tailer(start_index)` returns a cursor positioned at the first message at or after `start_index`.

```zig
var tailer = try queue.tailer(0);
defer tailer.deinit();

while (true) {
    if (try tailer.poll()) |entry| {
        std.debug.print("index={} payload={s}\n", .{ entry.index, entry.message });
    } else {
        std.Thread.yield() catch {};
    }
}
```

For blocking reads, use `collect()` with a backoff policy:

```zig
const entry = try tailer.collect(.yield);
std.debug.print("read index={}\n", .{entry.index});
```

Tailers can also drive read-side prefetch explicitly:

```zig
while (true) {
    _ = try tailer.prefetchPoll(64);
    if (try tailer.poll()) |entry| {
        // process entry.message
        _ = entry;
    } else {
        std.Thread.yield() catch {};
    }
}
```

For lowest jitter, run tailer prefetch from a separate helper thread and stop/join that helper before closing the tailer. The benchmark does this by default; pass `--no-tailer-prefetch` to compare against a single-threaded tailer benchmark.

## Benchmarks

The benchmark binary exercises append and tailer hot paths:

```sh
zig build bench -- --warmup=1000000 --count=10000000 --size=64
```

Useful options:

| Option | Purpose |
| --- | --- |
| `--warmup=N` | Untimed append warmup count. |
| `--count=N` | Measured append/tailer sample count. |
| `--size=N` | Payload size in bytes. |
| `--no-tailer` | Skip the tailer benchmark. |
| `--no-maintenance-threads` | Disable benchmark-owned appender prefetch/cleaner threads. |
| `--no-tailer-prefetch` | Disable the benchmark tailer prefetch helper. |
| `--retention-cycles=N` | Enable cleaner retention during the append phase. |
| `--keep-dir` | Keep the temporary queue directory after the run. |

## License

`ringloom-queue` is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
