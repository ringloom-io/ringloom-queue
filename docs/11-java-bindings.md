# Java Bindings Specification

## 1. Scope

The Java bindings expose the ringloom-queue C ABI as a small Java 25 library under `bindings/java`. The binding is intentionally polling-first, single-writer aware, and explicit about borrowed mmap memory lifetimes.

Goals:

- Provide an ergonomic Java API for opening queues, appending raw byte payloads, polling tailers, and driving maintenance.
- Use the Java Foreign Function & Memory API instead of JNI glue.
- Package the native `ringloom_queue` shared library inside the jar for the build host platform.
- Build the embedded native library with Zig `ReleaseSmall` optimization.
- Preserve the C ABI ownership model: opaque handles, stable status codes, no native helper threads owned by the binding, and borrowed message views.

Non-goals for the first binding version:

- Typed Zig codec support. The Java API exposes the C ABI raw-byte codec.
- Cross-compiling and packaging multiple platform libraries in one build.
- Blocking reads or Java-owned notifier integration.
- Native helper-thread management. Java applications own scheduling and call poll methods explicitly.

## 2. Build and packaging contract

The Java project lives in `bindings/java` and uses Gradle's `java` plugin.

The `jar` task must:

1. Run `zig build c-abi -Doptimize=ReleaseSmall` from the repository root, unless `ringloomQueue.embeddedNativeLibDir` or `ringloom.queue.nativeLibDir` points at an existing native library directory.
2. Stage `System.mapLibraryName("ringloom_queue")` from `zig-out/lib` into generated resources.
3. Package the staged library at:
   `io/ringloom/queue/native/<os>-<arch>/<mapped-library-name>`

Supported resource platform identifiers:

| OS / arch | Identifier |
|---|---|
| Linux x86_64 / amd64 | `linux-x86_64` |
| Linux aarch64 / arm64 | `linux-aarch64` |
| macOS x86_64 / amd64 | `macos-x86_64` |
| macOS aarch64 / arm64 | `macos-aarch64` |

Runtime native loading order:

1. `ringloom.queue.nativeLibPath` — absolute path to a specific shared library.
2. `ringloom.queue.nativeLibDir` — directory containing the mapped library name.
3. Embedded classpath resource for the current platform, extracted to a temporary file and loaded with `System.load`.

Tests and applications using the Java FFM API must run with native access enabled, for example:

```sh
java --enable-native-access=ALL-UNNAMED ...
```

## 3. Public Java package

All public classes are in package `io.ringloom.queue`.

### 3.1 `QueueConfig`

Immutable record that maps to `ringloom_queue_options`.

Fields:

| Java field | Native field | Notes |
|---|---|---|
| `dir` | `dir`, `dir_len` | Required UTF-8 path string. |
| `create` | `create` | Create a missing queue when true. |
| `rollSchemeName` | `roll_scheme_name`, `roll_scheme_name_len` | Optional built-in roll scheme name. |
| `enablePrefetcher` | `enable_prefetcher` | Enables pollable prefetch state. |
| `enableCleaner` | `enable_cleaner` | Enables pollable cleaner state. |
| `spawnHelperThreads` | `spawn_helper_threads` | Recorded for compatibility; the C ABI does not create threads. |
| `retentionCycles` | `retention_cycles`, `retention_cycles_set` | Optional number of recent cycles to keep. |

Factory methods:

- `QueueConfig.create(String dir)` / `create(Path dir)`
- `QueueConfig.open(String dir)` / `open(Path dir)`

Copy methods:

- `withCreate(boolean)`
- `withRollSchemeName(String)`
- `withPrefetcher(boolean)`
- `withCleaner(boolean)`
- `withHelperThreads(boolean)`
- `withRetentionCycles(Integer)`

### 3.2 `RingloomQueue`

`RingloomQueue` owns a native `ringloom_queue_t*` and implements `AutoCloseable`.

Operations:

| Method | Native call | Semantics |
|---|---|---|
| `open(QueueConfig)` | `ringloom_queue_open` | Opens or creates a queue. |
| `create(String)` | `ringloom_queue_open` | Convenience create with defaults. |
| `openAppender()` | `ringloom_appender_open` | Opens the single appender lease. |
| `openTailer(long startIndex)` | `ringloom_tailer_open` | Opens an independent tailer. |
| `maintenancePoll(int)` | `ringloom_queue_maintenance_poll` | Bounded prefetcher + cleaner work. |
| `prefetchPoll(int)` | `ringloom_queue_prefetch_poll` | Alias for queue maintenance in ABI v1. |
| `cleanerPoll(int)` | `ringloom_queue_cleaner_poll` | Alias for queue maintenance in ABI v1. |
| `close()` | `ringloom_queue_close` | Idempotent close of the native queue handle. |

Applications should close appenders and tailers before closing the parent queue. Opening a tailer at index `0` starts at the queue's current lowest cycle; if the queue is empty, open the tailer after the first append or reopen/seek with a concrete saved index once data exists.

### 3.3 `RingloomAppender`

`RingloomAppender` owns a native `ringloom_appender_t*` and implements `AutoCloseable`.

Operations:

| Method | Native call | Semantics |
|---|---|---|
| `append(byte[])` | `ringloom_appender_append` | Copies Java bytes into native call memory, appends, returns index. |
| `append(MemorySegment)` | `ringloom_appender_append` | Appends from an off-heap native segment, returns index. |
| `appendString(String)` | `ringloom_appender_append` | UTF-8 convenience method. |
| `close()` | `ringloom_appender_close` | Idempotent appender close. |

The appender is single-writer. Java code should not concurrently call append on the same appender instance.

### 3.4 `RingloomTailer`

`RingloomTailer` owns a native `ringloom_tailer_t*` and implements `AutoCloseable`.

Operations:

| Method | Native call | Semantics |
|---|---|---|
| `poll(RingloomMessageView out)` | `ringloom_tailer_poll` | Non-blocking poll into a reusable borrowed view. Returns false when empty. |
| `poll()` | `ringloom_tailer_poll` | Allocates a view and returns `Optional`. |
| `pollCopy()` | `ringloom_tailer_poll` | Copies payload bytes into an owned `RingloomMessage`. |
| `prefetchPoll(int)` | `ringloom_tailer_prefetch_poll` | Bounded read-side prefetch work. |
| `close()` | `ringloom_tailer_close` | Idempotent tailer close. |

### 3.5 Message types

`RingloomMessageView` is a borrowed mmap view containing:

- `index()` — public queue index.
- `payloadSegment()` — native memory segment borrowed from the tailer's mapped queue file.
- `payloadLength()` — payload byte length.
- `payloadBytes()` — owned Java copy.
- `payloadString()` — UTF-8 convenience copy.

Borrowed payload lifetime ends at the next `poll` on the same tailer or when the tailer is closed.

`RingloomMessage` is an owned record: `record RingloomMessage(long index, byte[] payload)`.

### 3.6 Status and step results

`RingloomStatus` exposes the stable native C ABI integer constants. Ergonomic API methods throw on errors except for `OK_NOT_READY`, which `RingloomTailer.poll(...)` converts to `false`.

`RingloomQueueException` is thrown for native failures that are not converted to `IllegalArgumentException` or `OutOfMemoryError`.

`StepResult` maps native bounded-work results:

| Native value | Java enum |
|---|---|
| `0` | `IDLE` |
| `1` | `MADE_PROGRESS` |
| `2` | `MORE_WORK` |

## 4. Native layout constants

The binding uses explicit offsets for the ABI v1 structs on supported 64-bit platforms.

`ringloom_queue_options`:

| Field | Offset | Layout |
|---|---:|---|
| `size` | 0 | `int` |
| `dir` | 8 | address |
| `dir_len` | 16 | `long` / `size_t` |
| `create` | 24 | boolean |
| `roll_scheme_name` | 32 | address |
| `roll_scheme_name_len` | 40 | `long` / `size_t` |
| `enable_prefetcher` | 48 | boolean |
| `enable_cleaner` | 49 | boolean |
| `spawn_helper_threads` | 50 | boolean |
| `retention_cycles` | 52 | int |
| `retention_cycles_set` | 56 | boolean |
| total size | 64 | 8-byte alignment |

`ringloom_message_view`:

| Field | Offset | Layout |
|---|---:|---|
| `size` | 0 | `int` |
| `index` | 8 | `long` / `uint64_t` |
| `data` | 16 | address |
| `data_len` | 24 | `long` / `size_t` |
| total size | 32 | 8-byte alignment |

## 5. Example

```java
try (RingloomQueue queue = RingloomQueue.open(
         QueueConfig.create("data/events").withRollSchemeName("FAST_DAILY"));
     RingloomAppender appender = queue.openAppender()) {

    long index = appender.appendString("hello ringloom");

    try (RingloomTailer tailer = queue.openTailer(0)) {
        RingloomMessageView view = new RingloomMessageView();
        if (tailer.poll(view)) {
            System.out.println("index=" + view.index() + " payload=" + view.payloadString());
        }
    }

    StepResult step = queue.maintenancePoll(64);
}
```
