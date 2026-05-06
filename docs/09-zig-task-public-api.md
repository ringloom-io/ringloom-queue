# Task 7: Public API Design

## 1. Overview

The ringloom-queue public API is designed as an idiomatic Zig library. It exposes a
single-writer, multiple-reader, memory-mapped IPC queue with the lowest possible
latency and zero allocations on the hot path.

**Key principles:**

| Principle | How it is realised |
|---|---|
| Comptime generics | `Queue(MessageType)` / `Tailer(MessageType)` — monomorphised at compile time for zero function-pointer overhead |
| Zero hot-path allocations | All data lives in mmap'd `.ringloom` files; the append and poll paths never call an allocator |
| Error unions | Every fallible operation returns `!T`; no error codes, no global error string |
| No global state | Each `Queue` instance is fully independent |
| C ABI shim | Opaque handles, stable error codes, borrowed message views, and bounded poll functions for non-Zig clients |

---

## 2. Core Types

```zig
const std = @import("std");

/// A 64-bit index that uniquely identifies a message within the queue.
/// The upper bits encode the cycle (file), the lower bits encode the
/// sequence number within that cycle.
pub const Index = u64;

/// On-disk format version.
pub const Version = enum(u8) {
    unknown = 0,
    v1 = 1,
};

/// State machine for a tailer's position within the queue.
pub const TailerState = enum(u8) {
    /// Waiting for the next entry to appear in the current cycle file.
    awaiting_entry,
    /// Currently processing an entry.
    busy,
    /// Current cycle file exhausted; waiting for the next roll.
    awaiting_queuefile,
    /// stat() on a cycle file failed.
    err_stat,
    /// mmap() on a cycle file failed.
    err_mmap,
    /// poll()/collect() has not been called yet.
    not_yet_polled,
    /// Attempted to extend a cycle file and failed.
    extend_fail,
    /// An entry was successfully collected via collect().
    collected,
};

/// Describes how the queue rolls to a new cycle file.
pub const RollScheme = struct {
    /// Length of each cycle in milliseconds (e.g. 86_400_000 for daily).
    cycle_ms: u64,
    /// strftime-compatible format string for the cycle file name.
    date_format: []const u8,
    /// File extension for cycle files.
    extension: []const u8 = ".ringloom",
};
```

---

## 3. QueueConfig (Builder Pattern)

`QueueConfig` aggregates every knob needed to open or create a queue.
All fields except `dir` have sensible defaults so the simplest call site is
just `.{ .dir = "/tmp/my-queue" }`.

```zig
pub const QueueConfig = struct {
    /// Path to the queue directory.  Must already exist unless `create` is set.
    dir: []const u8,

    /// On-disk format version.
    version: Version = .v1,

    /// Override the default roll scheme (daily, `.ringloom` extension).
    roll_scheme: ?RollScheme = null,

    /// If true, create the directory and `metadata.ringloom` when they do not exist.
    create: bool = false,

    /// Request MAP_HUGETLB for cycle file mappings.
    use_huge_pages: bool = false,

    /// Lock the current and next appender windows in RAM when permitted by
    /// RLIMIT_MEMLOCK. Best-effort; failure is reported through diagnostics.
    lock_appender_windows: bool = false,

    /// Start the background prefetcher that preallocates, maps, and write-touches
    /// future appender pages before the hot path reaches them.
    enable_prefetcher: bool = true,

    /// Minimum prepared writable runway ahead of the appender.
    prefetch_runway_bytes: u64 = 8 * 1024 * 1024,

    /// Minimum read-prefetched runway ahead of each tailer, bounded by published data.
    read_prefetch_runway_bytes: u64 = 4 * 1024 * 1024,

    /// Start the background cleaner for deferred unmap/page-cache/retention work.
    enable_cleaner: bool = true,

    /// Native Zig convenience: spawn helper threads for prefetcher/cleaner.
    /// The C ABI forces this off and exposes pollable maintenance instead.
    spawn_helper_threads: bool = true,

    /// Retention in cycle files. Null means keep files indefinitely.
    retention_cycles: ?u32 = null,

    /// How many milliseconds before the next roll boundary to pre-create the
    /// next cycle file, avoiding latency spikes at roll time.
    preroll_ms: u64 = 1000,

    /// Allocator used for metadata bookkeeping (never on the hot path).
    allocator: std.mem.Allocator = std.heap.page_allocator,
};
```

---

## 4. Queue(comptime MessageType) — Generic Queue

`Queue` is the central type.  It is parameterised over the user's message type
so that the codec, serialisation sizes, and poll/append paths are all resolved
at comptime and fully inlined.

```zig
pub fn Queue(comptime MessageType: type) type {
    return struct {
        const Self = @This();

        // ── internal state (not part of the public contract) ──────────
        allocator: std.mem.Allocator,
        dir: []const u8,
        version: Version,
        roll: RollScheme,
        codec: Codec(MessageType),
        // … mmap bookkeeping, platform helpers, prefetcher/cleaner state, etc.

        // ── lifecycle ─────────────────────────────────────────────────

        /// Open (or create) a queue rooted at `config.dir`.
        /// The `codec` translates between `MessageType` and the raw bytes
        /// stored on disk.
        pub fn open(config: QueueConfig, codec: Codec(MessageType)) !Self {
            // 1. Resolve / create directory
            // 2. Read or write metadata.ringloom
            // 3. mmap the current cycle file
            // 4. Initialise platform helper state
            _ = .{ config, codec };
            @compileError("stub");
        }

        /// Release all resources: unmap files, close fds, free metadata.
        pub fn deinit(self: *Self) void {
            _ = self;
        }

        // ── writer (single thread only) ───────────────────────────────

        /// Append a message and return the assigned index.
        /// This is the **hot path** — no allocator calls, no syscalls
        /// in the polling profile, and no expected page faults when the
        /// prefetcher keeps the writable runway prepared.
        pub fn append(self: *Self, msg: MessageType) !Index {
            _ = .{ self, msg };
            @compileError("stub");
        }

        /// Append with an explicit wall-clock timestamp (milliseconds
        /// since epoch).  Useful for replaying captured data.
        pub fn appendWithTimestamp(self: *Self, msg: MessageType, ts_ms: u64) !Index {
            _ = .{ self, msg, ts_ms };
            @compileError("stub");
        }

        // ── reader (thread-safe — each tailer is independent) ─────────

        /// Create a new tailer starting at `start_index`.
        /// Pass `0` to read from the very beginning of the queue.
        pub fn tailer(self: *Self, start_index: Index) !Tailer(MessageType) {
            _ = .{ self, start_index };
            @compileError("stub");
        }

        // ── housekeeping (never called by append hot path) ─────────────

        /// Request deletion of cycle files older than `cycle`.
        /// The cleaner performs the actual unlink asynchronously.
        pub fn truncateBefore(self: *Self, cycle: u32) !void {
            _ = .{ self, cycle };
            @compileError("stub");
        }

        /// Return prefetch/cleaner diagnostics such as prefetch misses,
        /// synchronous fallback count, and cleaner reclaim counts.
        pub fn diagnostics(self: *const Self) Diagnostics {
            _ = self;
            @compileError("stub");
        }

        /// Drive queue-level prefetcher and cleaner work without requiring
        /// library-owned helper threads.
        pub fn maintenancePoll(self: *Self, max_work_units: u32) !StepResult {
            _ = .{ self, max_work_units };
            @compileError("stub");
        }

        // ── metadata access ───────────────────────────────────────────

        pub fn getVersion(self: *const Self) Version {
            return self.version;
        }

        pub fn getRollScheme(self: *const Self) RollScheme {
            return self.roll;
        }
    };
}
```

### Threading contract

| Operation | Thread safety |
|---|---|
| `append` / `appendWithTimestamp` | **Single writer only.** No internal locking. |
| `tailer` | Safe to call from any thread. |
| `Tailer.poll` / `Tailer.collect` | Each tailer instance must be used by a single thread at a time, but different tailers may run concurrently. |
| `getVersion` / `getRollScheme` | Immutable after `open`; safe from any thread. |

All shared state between writer and readers is synchronised with
**acquire/release** atomic ordering on the sequence counter in the mapped file
header.

---

## 5. Tailer(comptime MessageType) — Iterator Pattern

A `Tailer` is a lightweight cursor over the queue.  It never allocates — the
returned `Entry.message` borrows directly from the mmap'd region and is valid
until the next call to `poll` or `collect`.

```zig
pub fn Tailer(comptime MessageType: type) type {
    return struct {
        const Self = @This();

        pub const Entry = struct {
            /// The unique index of this message.
            index: Index,
            /// The deserialised message.  Borrows from the mmap region —
            /// valid until the next poll/collect call on this tailer.
            message: MessageType,
            /// Size of the raw on-disk representation in bytes.
            raw_size: usize,
        };

        // ── reading ───────────────────────────────────────────────────

        /// Non-blocking poll.  Returns `null` when no new message is
        /// available yet.  This is the lowest-latency read path.
        pub fn poll(self: *Self) !?Entry {
            _ = self;
            @compileError("stub");
        }

        /// Convenience blocking loop around poll() using caller-selected backoff.
        /// The queue core itself has no kernel notification dependency.
        pub fn collect(self: *Self, backoff: BackoffPolicy) !Entry {
            _ = .{ self, backoff };
            @compileError("stub");
        }

        /// Drive read-side prefetch for this tailer within a bounded work budget.
        pub fn prefetchPoll(self: *Self, max_work_units: u32) !StepResult {
            _ = .{ self, max_work_units };
            @compileError("stub");
        }

        // ── positioning ───────────────────────────────────────────────

        /// Seek to a specific index.  The next poll/collect will return
        /// the message at (or after) `index`.
        pub fn seekTo(self: *Self, index: Index) !void {
            _ = .{ self, index };
        }

        // ── introspection ─────────────────────────────────────────────

        pub fn getState(self: *const Self) TailerState {
            return self.state;
        }

        pub fn getIndex(self: *const Self) Index {
            return self.current_index;
        }

        // ── lifecycle ─────────────────────────────────────────────────

        /// Release cycle-file mappings held by this tailer.
        pub fn deinit(self: *Self) void {
            _ = self;
        }
    };
}
```

---

## 6. Codec Interface and Built-in Codecs

A **codec** bridges the gap between the user's `MessageType` and the raw bytes
on disk.  ringloom-queue does **not** use BinaryWire or any self-describing wire
format — the codec is a simple, user-supplied pair of serialize/deserialize
functions resolved at comptime.

### 6.1 Codec definition

```zig
pub fn Codec(comptime MessageType: type) type {
    return struct {
        /// Deserialise `MessageType` from a raw byte buffer.
        parse: *const fn (buf: []const u8) ?MessageType,

        /// Return the serialised size of `msg` in bytes.
        serialized_size: *const fn (msg: MessageType) usize,

        /// Write `msg` into `buf`.  `buf.len` is guaranteed to be at
        /// least `serialized_size(msg)`.
        write: *const fn (buf: []u8, msg: MessageType) void,
    };
}
```

### 6.2 RawCodec — `[]const u8`, no validation

The simplest built-in codec.  It stores and retrieves raw byte slices with
no copying and no validation.

```zig
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
        fn f(buf: []u8, msg: []const u8) void {
            @memcpy(buf[0..msg.len], msg);
        }
    }.f,
};
```

### 6.3 TextCodec — `[]const u8`, UTF-8 validated

Identical to `RawCodec` on the write side.  On parse, it rejects buffers
that are not valid UTF-8.

```zig
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
```

### 6.4 Custom codec example — `Trade`

```zig
const Trade = struct {
    symbol: [8]u8,
    price: f64,
    quantity: u32,
};

const trade_codec = ringloom.Codec(Trade){
    .parse = struct {
        fn f(buf: []const u8) ?Trade {
            if (buf.len < @sizeOf(Trade)) return null;
            return std.mem.bytesToValue(Trade, buf[0..@sizeOf(Trade)]);
        }
    }.f,
    .serialized_size = struct {
        fn f(_: Trade) usize {
            return @sizeOf(Trade);
        }
    }.f,
    .write = struct {
        fn f(buf: []u8, msg: Trade) void {
            @memcpy(buf[0..@sizeOf(Trade)], std.mem.asBytes(&msg));
        }
    }.f,
};
```

---

## 7. Example Usage

### 7a. Writing to a queue

```zig
const std = @import("std");
const ringloom = @import("ringloom-queue");

pub fn main() !void {
    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = "/tmp/my-queue",
        .create = true,
    }, ringloom.RawCodec);
    defer queue.deinit();

    const index = try queue.append("hello world");
    std.log.info("wrote message at index 0x{x}", .{index});
}
```

### 7b. Reading — non-blocking poll

```zig
const std = @import("std");
const ringloom = @import("ringloom-queue");

pub fn main() !void {
    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = "/tmp/my-queue",
    }, ringloom.RawCodec);
    defer queue.deinit();

    var t = try queue.tailer(0);
    defer t.deinit();

    while (true) {
        if (try t.poll()) |entry| {
            std.log.info("[0x{x}] {s}", .{ entry.index, entry.message });
        }
    }
}
```

### 7c. Reading — application-owned backoff

```zig
const std = @import("std");
const ringloom = @import("ringloom-queue");

pub fn main() !void {
    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = "/tmp/my-queue",
    }, ringloom.RawCodec);
    defer queue.deinit();

    var t = try queue.tailer(0);
    defer t.deinit();

    while (true) {
        const entry = try t.collect(.low_latency_spin_then_sleep);
        std.log.info("[0x{x}] {s}", .{ entry.index, entry.message });
    }
}
```

### 7d. Structured messages with a custom codec

```zig
const std = @import("std");
const ringloom = @import("ringloom-queue");

const Trade = struct {
    symbol: [8]u8,
    price: f64,
    quantity: u32,
};

const trade_codec = ringloom.Codec(Trade){
    .parse = struct {
        fn f(buf: []const u8) ?Trade {
            if (buf.len < @sizeOf(Trade)) return null;
            return std.mem.bytesToValue(Trade, buf[0..@sizeOf(Trade)]);
        }
    }.f,
    .serialized_size = struct {
        fn f(_: Trade) usize {
            return @sizeOf(Trade);
        }
    }.f,
    .write = struct {
        fn f(buf: []u8, msg: Trade) void {
            @memcpy(buf[0..@sizeOf(Trade)], std.mem.asBytes(&msg));
        }
    }.f,
};

pub fn main() !void {
    var queue = try ringloom.Queue(Trade).open(.{
        .dir = "/tmp/trades",
        .create = true,
    }, trade_codec);
    defer queue.deinit();

    var sym: [8]u8 = undefined;
    @memcpy(sym[0..4], "AAPL");
    @memset(sym[4..], 0);

    const index = try queue.append(.{
        .symbol = sym,
        .price = 187.42,
        .quantity = 100,
    });
    std.log.info("trade written at 0x{x}", .{index});
}
```

### 7e. Resuming from a saved index

```zig
// Persist `last_index` to a file or database between runs.
const saved_index: ringloom.Index = 0x4A0500000003;

var t = try queue.tailer(saved_index);
defer t.deinit();

const entry = try t.collect(.low_latency_spin_then_sleep);
std.log.info("resumed at [0x{x}] {s}", .{ entry.index, entry.message });
```

---

## 8. Error Handling

ringloom-queue uses Zig error unions throughout.  Every fallible function returns
`!T` so errors propagate naturally with `try`.

### 8.1 Error set

```zig
pub const QueueError = error{
    /// The queue directory does not exist and `create` was not set.
    DirectoryNotFound,
    /// metadata.ringloom is missing or corrupt.
    MetadataCorrupt,
    /// On-disk version is not supported by this build.
    UnsupportedVersion,
    /// mmap failed (e.g. address space exhaustion).
    MmapFailed,
    /// A cycle file could not be extended to the required size.
    ExtendFailed,
    /// stat() on a cycle file failed unexpectedly.
    StatFailed,
    /// The codec's parse function returned null (corrupt or truncated message).
    ParseFailed,
    /// Platform preallocation/read-ahead/population failed.
    PlatformIoFailed,
    /// Attempted to write when another writer is active (detected via header flag).
    WriterConflict,
    /// The requested index is beyond the end of the queue.
    IndexOutOfRange,
};
```

### 8.2 Example: handling errors explicitly

```zig
const queue = ringloom.Queue([]const u8).open(.{
    .dir = "/tmp/my-queue",
}, ringloom.RawCodec) catch |err| switch (err) {
    error.DirectoryNotFound => {
        std.log.err("queue directory does not exist", .{});
        return err;
    },
    error.MetadataCorrupt => {
        std.log.err("metadata.ringloom is corrupt — delete and recreate", .{});
        return err;
    },
    else => return err,
};
```

---

## 9. Debug Output

ringloom-queue uses `std.log.scoped` for all diagnostic output.  No `printf`,
no global debug flags.

### 9.1 Scoped logger

```zig
const log = std.log.scoped(.ringloom_queue);

pub fn open(config: QueueConfig, codec: anytype) !Self {
    log.info("opening queue dir={s} version={}", .{ config.dir, @intFromEnum(config.version) });
    // ...
}
```

### 9.2 Hex dump utility

For low-level debugging, a simple hex dump replaces the old wire-tracing
infrastructure:

```zig
pub fn hexDump(writer: anytype, label: []const u8, data: []const u8) !void {
    try writer.print("── {s} ({d} bytes) ──\n", .{ label, data.len });
    var offset: usize = 0;
    while (offset < data.len) {
        const end = @min(offset + 16, data.len);
        try writer.print("{x:0>8}  ", .{offset});
        for (data[offset..end]) |b| {
            try writer.print("{x:0>2} ", .{b});
        }
        // padding for short lines
        for (0..(16 - (end - offset))) |_| {
            try writer.print("   ", .{});
        }
        try writer.print(" |", .{});
        for (data[offset..end]) |b| {
            const c: u8 = if (b >= 0x20 and b < 0x7f) b else '.';
            try writer.print("{c}", .{c});
        }
        try writer.print("|\n", .{});
        offset = end;
    }
}
```

Users can enable or suppress ringloom-queue log messages at build time using the
standard Zig log level mechanism:

```zig
pub const std_options = .{
    .log_scope_levels = &.{
        .{ .scope = .ringloom_queue, .level = .debug },
    },
};
```

---

## 10. Design Decisions

### 10.1 Why comptime generics

| Alternative | Downside |
|---|---|
| Runtime function pointers (vtable) | Indirect call on every append/poll; cannot inline codec logic |
| `anytype` without named codec struct | Harder to document, no clear contract |
| `*anyopaque` + cast | Loses type safety entirely |

With `Queue(comptime MessageType)`, the compiler monomorphises the entire
read/write path.  The codec's `parse`, `serialized_size`, and `write`
functions are inlined directly into `append` and `poll`, eliminating all
function-pointer overhead and enabling the optimiser to reason about the full
call chain.

### 10.2 C ABI shim

The C ABI is a thin, stable, polling-first shim over the same queue core. It is
intended for clients written in C, C++, Python extensions, JVM/JNI, kdb+, Rust
FFI, and other runtimes.

Key rules:

| Rule | Contract |
|---|---|
| Handles | All objects are opaque pointers (`ringloom_queue_t`, `ringloom_tailer_t`, `ringloom_appender_t`) |
| Errors | Stable integer `ringloom_error_t`; never expose Zig error-set ordinals |
| ABI version | `ringloom_abi_version()` and struct `size` fields support forward compatibility |
| Threads | The C ABI must not create library-owned threads; builds should hard-disable spawn paths |
| Maintenance | The embedder drives prefetcher/pretoucher/cleaner work via bounded poll calls |
| Message lifetime | Returned message views borrow mmap memory until the next call on the same tailer or tailer close |
| Panics | C-callable paths must convert failures to error codes; no Zig panic may cross FFI |
| Allocator | Use explicit init options for allocator hooks, or the documented default C allocator |

```c
typedef struct ringloom_queue ringloom_queue_t;
typedef struct ringloom_tailer ringloom_tailer_t;
typedef struct ringloom_appender ringloom_appender_t;

typedef enum ringloom_step_result {
    RINGLOOM_STEP_IDLE = 0,
    RINGLOOM_STEP_PROGRESS = 1,
    RINGLOOM_STEP_MORE_WORK = 2,
} ringloom_step_result_t;

typedef struct ringloom_message_view {
    uint32_t size;       /* sizeof(ringloom_message_view) */
    uint64_t index;
    const void *data;    /* borrowed mmap pointer */
    size_t data_len;
} ringloom_message_view_t;

uint32_t ringloom_abi_version(void);
const char *ringloom_strerror(int err);

int ringloom_queue_open(const struct ringloom_queue_options *opts, ringloom_queue_t **out);
void ringloom_queue_close(ringloom_queue_t *q);

int ringloom_appender_open(ringloom_queue_t *q, ringloom_appender_t **out);
int ringloom_appender_append(ringloom_appender_t *a, const void *data, size_t len, uint64_t *index_out);
void ringloom_appender_close(ringloom_appender_t *a);

int ringloom_tailer_open(ringloom_queue_t *q, uint64_t start_index, ringloom_tailer_t **out);
int ringloom_tailer_poll(ringloom_tailer_t *t, ringloom_message_view_t *out); /* RINGLOOM_OK_NOT_READY when empty */
int ringloom_tailer_prefetch_poll(ringloom_tailer_t *t, uint32_t max_work_units, ringloom_step_result_t *out);
void ringloom_tailer_close(ringloom_tailer_t *t);

int ringloom_queue_prefetch_poll(ringloom_queue_t *q, uint32_t max_work_units, ringloom_step_result_t *out);
int ringloom_queue_cleaner_poll(ringloom_queue_t *q, uint32_t max_work_units, ringloom_step_result_t *out);
int ringloom_queue_maintenance_poll(ringloom_queue_t *q, uint32_t max_work_units, ringloom_step_result_t *out);
```

The borrowed `ringloom_message_view.data` pointer is valid until the next
`ringloom_tailer_poll`, `ringloom_tailer_close`, or any other API call that explicitly
advances/remaps the same tailer. Queue-level maintenance must not unmap a window
currently owned by a live local tailer cursor. If an embedding needs longer
lifetimes, it must copy the bytes before polling that tailer again.

Example C-style event loop:

```c
for (;;) {
    ringloom_message_view_t msg = { .size = sizeof(msg) };
    int rc = ringloom_tailer_poll(tailer, &msg);
    if (rc == RINGLOOM_OK) {
        handle_message(msg.data, msg.data_len);
        continue;
    }
    if (rc != RINGLOOM_OK_NOT_READY) fail(rc);

    ringloom_step_result_t step;
    fail_if_error(ringloom_tailer_prefetch_poll(tailer, 256, &step));
    fail_if_error(ringloom_queue_maintenance_poll(queue, 256, &step));

    app_sleep_or_wait();
}
```

### 10.3 Memory ownership model

| Resource | Owner | Lifetime |
|---|---|---|
| Queue directory metadata | `Queue` instance | `open` → `deinit` |
| mmap'd cycle files | `Queue` instance | Mapped on demand, unmapped at `deinit` |
| `Tailer` cursors | Caller | Created via `queue.tailer()`, caller calls `deinit` |
| `Entry.message` | Borrows from mmap | Valid until next `poll`/`collect` on the same tailer |

The hot path (append and poll) never touches the allocator.  All data flows
through the mmap'd region.  The allocator is only used during `open` (to
allocate internal bookkeeping) and `deinit` (to free it).

### 10.4 No global state

Every `Queue` instance is fully self-contained.  There is no process-wide
registry, no singleton, no thread-local storage.  Multiple independent queues
can coexist in the same process without interference.

### 10.5 Acquire/release ordering

The writer publishes each message by storing the new sequence number with
`@atomicStore(.release)`.  Readers observe it with `@atomicLoad(.acquire)`.
This is the minimum ordering that guarantees the message body is visible before
the reader sees the updated sequence number, and it avoids the cost of
sequential consistency (`seq_cst`) fences on x86 and ARM.

---

## 11. Summary

| Aspect | ringloom-queue approach |
|---|---|
| Language | Zig API plus C ABI shim |
| Generics | `Queue(comptime MessageType)` — comptime monomorphisation |
| Codec | User-supplied `Codec(T)` struct; built-in `RawCodec`, `TextCodec` |
| Writer | Single-threaded `append` / `appendWithTimestamp` |
| Reader | `Tailer.poll` (non-blocking) or optional Zig `Tailer.collect` backoff loop |
| File format | `.ringloom` cycle files, `metadata.ringloom` |
| Error handling | Zig error unions (`!T`) |
| Allocations | Zero on hot path; allocator used only during open/close |
| Logging | `std.log.scoped(.ringloom_queue)` |
| Atomics | Acquire/release ordering for writer↔reader synchronisation |
| Global state | None |
