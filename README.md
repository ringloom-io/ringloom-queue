# ringloom-queue

`ringloom-queue` is a high-performance, memory-mapped, append-only queue for low-latency systems. It stores messages in fixed-size rolling cycle files, exposes a single active appender with independent tailers, and keeps maintenance work explicit through bounded polling APIs.

The queue is designed for workloads where predictable latency, stable file formats, and allocation-free hot paths matter more than broker-style routing or general-purpose middleware features.

## Why ringloom-queue

- **Memory-mapped append path** — payloads are written directly into mapped queue files with fixed 4-byte message headers.
- **Single writer, many readers** — one active appender owns writes while each tailer maintains its own independent read cursor.
- **Rolling cycle files** — queues are split into fixed-size time-based cycle files with deterministic names and inline indexes.
- **Pollable maintenance** — prefetching, pre-roll creation, deferred cleanup, and retention are driven by bounded poll calls.
- **Stable interoperability layer** — a C ABI exposes opaque handles, stable status codes, raw-byte appends, tailer polling, and maintenance helpers.
- **Java integration** — Java 25 FFM bindings package the native shared library inside the jar for the build platform.

## Highlights

- Append-only queue directory containing `metadata.ringloom` plus rolling cycle files.
- Fixed-layout metadata and queue-file headers readable by direct mmap casts.
- Flat inline `u64` index arrays for efficient tailer seeks.
- Built-in Zig codecs for raw bytes, UTF-8 text, and fixed-layout structs.
- Best-effort platform preallocation and mmap/page-fault reduction helpers.
- Conservative retention that avoids deleting active appender, pre-roll, current, or tailer-owned cycles.
- Unit, validation, C ABI, Java binding, and benchmark coverage.

## Architecture at a glance

```text
┌──────────────┐      mmap writes      ┌──────────────────────┐
│ Appender     │ ───────────────────▶ │ rolling cycle files  │
│ single owner │                      │ + metadata mmap      │
└──────────────┘                      └──────────────────────┘
                                                ▲
                                                │ mmap reads
                     ┌──────────────────────────┴──────────────────────────┐
                     │                                                     │
              ┌──────────────┐                                      ┌──────────────┐
              │ Tailer A     │                                      │ Tailer B     │
              │ independent  │                                      │ independent  │
              └──────────────┘                                      └──────────────┘
```

Core components:

| Component | Purpose |
|---|---|
| `Queue` | Owns metadata, roll configuration, cycle discovery, maintenance helpers, and handle lifecycle. |
| `Appender` | Single active writer that appends payloads, publishes headers, updates indexes, and rolls cycles. |
| `Tailer` | Independent non-blocking reader with seek support and optional read-side prefetching. |
| `Prefetcher` | Pollable helper state for preparing write and read mappings/pages before they are needed. |
| `Cleaner` | Pollable helper state for deferred resource cleanup and retention. |
| `c_api` | Stable raw-byte C ABI for FFI bindings and non-Zig runtimes. |

## Getting started

### Requirements

- Zig 0.16.x or a compatible development build
- Linux or macOS
- Optional for Java bindings:
  - Java 25+
  - Gradle 9+

### Common commands

| Command | What it does |
|---|---|
| `zig build test` | Run Zig unit and validation tests. |
| `zig build test --summary all` | Run tests with a full Zig build summary. |
| `zig build c-abi -Doptimize=ReleaseSmall` | Build the C ABI shared library as `zig-out/lib/libringloom_queue.so` on Linux. |
| `zig build bench -- --warmup=1000000 --count=10000000 --size=64` | Run the append/tailer benchmark binary. |
| `gradle -p bindings/java test` | Run Java binding tests. |
| `gradle -p bindings/java jar` | Build the Java jar and embed the ReleaseSmall native shared library. |
| `gradle -p bindings/java javadoc` | Generate Java binding API documentation. |

## Repository layout

| Path | Contents |
|---|---|
| `src/root.zig` | Public Zig module root and re-exports. |
| `src/ringloom/` | Queue core, appender, tailer, codecs, metadata, mmap helpers, cleaner, prefetcher, C ABI, and tests. |
| `docs/` | Architecture, file-format, public API, testing, and binding specifications. |
| `bindings/java/` | Java 25 FFM bindings, Gradle build, tests, and binding README. |
| `build.zig` | Zig build graph for tests, benchmarks, and the C ABI shared library. |

## Zig API overview

The type-safe Zig API is generic over a message type and codec. Built-in codecs include:

| Codec | Message type | Purpose |
|---|---|---|
| `RawCodec` / `DefaultRawCodec` | `[]const u8` | Raw byte payloads with no validation. |
| `TextCodec` | `[]const u8` | UTF-8 byte payloads with validation. |
| `StructCodec(T)` | `T` | Fixed-layout value types. |

### Opening a queue and appending

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

    const index = try queue.append("hello ringloom");
    _ = index;
}
```

Important queue configuration fields:

| Field | Purpose |
|---|---|
| `dir` | Queue directory containing metadata and cycle files. |
| `create` | Create a missing queue when `true`; otherwise open an existing queue. |
| `roll_scheme` | Optional cycle naming, duration, and index layout. Defaults to `FAST_DAILY` when creating. |
| `allocator` | Allocator used for queue-owned paths, helper state, and handles. |
| `use_huge_pages` | Request best-effort huge-page mappings where supported. |
| `enable_prefetcher` | Enable pollable prefetch and pre-roll helper state. |
| `enable_cleaner` | Enable retention and deferred cleanup helper state. |
| `retention_cycles` | Optional number of recent cycles to keep; `null` keeps files indefinitely. |
| `preroll_ms` | How early to prepare the next appender cycle before a time roll. |

### Tailers

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

For blocking-style reads, use `collect()` with a backoff policy:

```zig
const entry = try tailer.collect(.yield);
std.debug.print("read index={}\n", .{entry.index});
```

## Maintenance model

Maintenance is intentionally explicit and bounded. A call to `queue.inner.maintenancePoll(max_work_units)` performs up to that much prefetcher and cleaner work and returns a `StepResult`:

| Result | Meaning |
|---|---|
| `.idle` | No work was available. |
| `.made_progress` | Work completed. |
| `.more_work` | More work is immediately available. |

Applications that need dedicated maintenance workers should run their own thread or event-loop task and call `maintenancePoll()` periodically. The public `spawn_helper_threads` flag is retained as configuration metadata for compatibility; the C ABI path does not create native helper threads.

Tailers can also drive read-side prefetch explicitly with `tailer.prefetchPoll(max_work_units)`.

## C ABI

The C ABI is a thin raw-byte shim over the same queue core. It is intended for C, C++, JVM, Python extensions, Rust FFI, and other runtimes.

Build it with:

```sh
zig build c-abi -Doptimize=ReleaseSmall
```

The ABI exposes:

- `ringloom_abi_version()`
- `ringloom_queue_open()` / `ringloom_queue_close()`
- `ringloom_appender_open()` / `ringloom_appender_append()` / `ringloom_appender_close()`
- `ringloom_tailer_open()` / `ringloom_tailer_poll()` / `ringloom_tailer_close()`
- `ringloom_tailer_prefetch_poll()`
- `ringloom_queue_prefetch_poll()`
- `ringloom_queue_cleaner_poll()`
- `ringloom_queue_maintenance_poll()`

Message views returned by `ringloom_tailer_poll()` borrow mmap memory owned by the tailer. Copy bytes before polling the same tailer again if the payload must outlive the borrowed view.

## Java bindings

Java bindings live in `bindings/java` and use the Java Foreign Function & Memory API.

Build the jar:

```sh
gradle -p bindings/java jar
```

The Gradle build embeds the current platform's `ringloom_queue` shared library into the jar under:

```text
io/ringloom/queue/native/<os>-<arch>/<mapped-library-name>
```

Runtime loading order:

1. `-Dringloom.queue.nativeLibPath=/absolute/path/to/libringloom_queue.so`
2. `-Dringloom.queue.nativeLibDir=/directory/containing/the/library`
3. Embedded classpath resource for the current platform

Example:

```java
try (RingloomQueue queue = RingloomQueue.open(
         QueueConfig.create("data/events").withRollSchemeName("FAST_DAILY"));
     RingloomAppender appender = queue.openAppender()) {

    long index = appender.appendString("hello ringloom");

    try (RingloomTailer tailer = queue.openTailer(0)) {
        RingloomMessageView view = new RingloomMessageView();
        if (tailer.poll(view)) {
            System.out.println(view.index() + ": " + view.payloadString());
        }
    }
}
```

Java applications using these bindings must enable native access, for example `--enable-native-access=ALL-UNNAMED`.

See [`bindings/java/README.md`](bindings/java/README.md) and [`docs/11-java-bindings.md`](docs/11-java-bindings.md) for more details.

## Benchmarks

The benchmark binary exercises append and tailer hot paths:

```sh
zig build bench -- --warmup=1000000 --count=10000000 --size=64
```

Useful options:

| Option | Purpose |
|---|---|
| `--warmup=N` | Untimed append warmup count. |
| `--count=N` | Measured append/tailer sample count. |
| `--size=N` | Payload size in bytes. |
| `--no-tailer` | Skip the tailer benchmark. |
| `--no-maintenance-threads` | Disable benchmark-owned appender prefetch and cleaner threads. |
| `--no-tailer-prefetch` | Disable the benchmark tailer prefetch helper. |
| `--retention-cycles=N` | Enable cleaner retention during the append phase. |
| `--keep-dir` | Keep the temporary queue directory after the run. |

## Documentation

- [`docs/00-index.md`](docs/00-index.md) — documentation map and quick reference.
- [`docs/01-architecture-overview.md`](docs/01-architecture-overview.md) — architecture, concurrency model, and design decisions.
- [`docs/02-file-format.md`](docs/02-file-format.md) — metadata, queue file, message header, and index layout specification.
- [`docs/09-zig-task-public-api.md`](docs/09-zig-task-public-api.md) — Zig public API and C ABI design.
- [`docs/10-zig-task-testing.md`](docs/10-zig-task-testing.md) — testing and validation strategy.
- [`docs/11-java-bindings.md`](docs/11-java-bindings.md) — Java binding API and packaging specification.

## Project status

`ringloom-queue` includes the core queue implementation, public Zig API, C ABI, Java bindings, tests, and benchmarks. The project is evolving around broader platform coverage, additional binding polish, operational examples, and production hardening.

## License

`ringloom-queue` is licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE).
