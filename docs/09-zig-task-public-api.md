# Task 7: Public API Design

## 1. Overview

The brz-queue public API is designed as an idiomatic Zig library. It exposes a
single-writer, multiple-reader, memory-mapped IPC queue with the lowest possible
latency and zero allocations on the hot path.

**Key principles:**

| Principle | How it is realised |
|---|---|
| Comptime generics | `Queue(MessageType)` / `Tailer(MessageType)` — monomorphised at compile time for zero function-pointer overhead |
| Zero hot-path allocations | All data lives in mmap'd `.brz` files; the append and poll paths never call an allocator |
| Error unions | Every fallible operation returns `!T`; no error codes, no global error string |
| No global state | Each `Queue` instance is fully independent |
| No C ABI layer | Pure Zig library — a C-compatible shim can be added later if needed |

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
    extension: []const u8 = ".brz",
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

    /// Override the default roll scheme (daily, `.brz` extension).
    roll_scheme: ?RollScheme = null,

    /// If true, create the directory and `metadata.brz` when they do not exist.
    create: bool = false,

    /// Request MAP_HUGETLB for cycle file mappings.
    use_huge_pages: bool = false,

    /// How many milliseconds before the next roll boundary to pre-create the
    /// next cycle file, avoiding latency spikes at roll time.
    preroll_ms: u64 = 1000,

    /// Use io_uring for blocking tailer wakeup (the `collect` API).
    /// Falls back to poll/futex when false or when the kernel is too old.
    enable_io_uring: bool = true,

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
        // … mmap bookkeeping, ring pointers, io_uring fd, etc.

        // ── lifecycle ─────────────────────────────────────────────────

        /// Open (or create) a queue rooted at `config.dir`.
        /// The `codec` translates between `MessageType` and the raw bytes
        /// stored on disk.
        pub fn open(config: QueueConfig, codec: Codec(MessageType)) !Self {
            // 1. Resolve / create directory
            // 2. Read or write metadata.brz
            // 3. mmap the current cycle file
            // 4. Optionally initialise io_uring
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
        /// (the mmap page fault is the only implicit kernel interaction).
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

        /// Blocking wait using io_uring (or fallback).  Returns the next
        /// message, sleeping until one is appended.
        pub fn collect(self: *Self) !Entry {
            _ = self;
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
on disk.  brz-queue does **not** use BinaryWire or any self-describing wire
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

const trade_codec = brz.Codec(Trade){
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
const brz = @import("brz-queue");

pub fn main() !void {
    var queue = try brz.Queue([]const u8).open(.{
        .dir = "/tmp/my-queue",
        .create = true,
    }, brz.RawCodec);
    defer queue.deinit();

    const index = try queue.append("hello world");
    std.log.info("wrote message at index 0x{x}", .{index});
}
```

### 7b. Reading — non-blocking poll

```zig
const std = @import("std");
const brz = @import("brz-queue");

pub fn main() !void {
    var queue = try brz.Queue([]const u8).open(.{
        .dir = "/tmp/my-queue",
    }, brz.RawCodec);
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

### 7c. Reading — blocking with io_uring

```zig
const std = @import("std");
const brz = @import("brz-queue");

pub fn main() !void {
    var queue = try brz.Queue([]const u8).open(.{
        .dir = "/tmp/my-queue",
        .enable_io_uring = true,
    }, brz.RawCodec);
    defer queue.deinit();

    var t = try queue.tailer(0);
    defer t.deinit();

    while (true) {
        const entry = try t.collect(); // blocks until message available
        std.log.info("[0x{x}] {s}", .{ entry.index, entry.message });
    }
}
```

### 7d. Structured messages with a custom codec

```zig
const std = @import("std");
const brz = @import("brz-queue");

const Trade = struct {
    symbol: [8]u8,
    price: f64,
    quantity: u32,
};

const trade_codec = brz.Codec(Trade){
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
    var queue = try brz.Queue(Trade).open(.{
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
const saved_index: brz.Index = 0x4A0500000003;

var t = try queue.tailer(saved_index);
defer t.deinit();

const entry = try t.collect();
std.log.info("resumed at [0x{x}] {s}", .{ entry.index, entry.message });
```

---

## 8. Error Handling

brz-queue uses Zig error unions throughout.  Every fallible function returns
`!T` so errors propagate naturally with `try`.

### 8.1 Error set

```zig
pub const QueueError = error{
    /// The queue directory does not exist and `create` was not set.
    DirectoryNotFound,
    /// metadata.brz is missing or corrupt.
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
    /// io_uring setup failed (kernel too old, resource limit).
    IoUringInitFailed,
    /// Attempted to write when another writer is active (detected via header flag).
    WriterConflict,
    /// The requested index is beyond the end of the queue.
    IndexOutOfRange,
};
```

### 8.2 Example: handling errors explicitly

```zig
const queue = brz.Queue([]const u8).open(.{
    .dir = "/tmp/my-queue",
}, brz.RawCodec) catch |err| switch (err) {
    error.DirectoryNotFound => {
        std.log.err("queue directory does not exist", .{});
        return err;
    },
    error.MetadataCorrupt => {
        std.log.err("metadata.brz is corrupt — delete and recreate", .{});
        return err;
    },
    else => return err,
};
```

---

## 9. Debug Output

brz-queue uses `std.log.scoped` for all diagnostic output.  No `printf`,
no global debug flags.

### 9.1 Scoped logger

```zig
const log = std.log.scoped(.brz_queue);

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

Users can enable or suppress brz-queue log messages at build time using the
standard Zig log level mechanism:

```zig
pub const std_options = .{
    .log_scope_levels = &.{
        .{ .scope = .brz_queue, .level = .debug },
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

### 10.2 Why no C ABI

brz-queue is a **pure Zig** library.  Exposing a C ABI adds constraints
(stable struct layout, manual memory management, global error state) that
conflict with idiomatic Zig design.  If C/Python/kdb interop is needed in
the future, a thin wrapper crate can be written on top without polluting the
core API.

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

| Aspect | brz-queue approach |
|---|---|
| Language | Pure Zig, no C ABI |
| Generics | `Queue(comptime MessageType)` — comptime monomorphisation |
| Codec | User-supplied `Codec(T)` struct; built-in `RawCodec`, `TextCodec` |
| Writer | Single-threaded `append` / `appendWithTimestamp` |
| Reader | `Tailer.poll` (non-blocking) or `Tailer.collect` (io_uring blocking) |
| File format | `.brz` cycle files, `metadata.brz` |
| Error handling | Zig error unions (`!T`) |
| Allocations | Zero on hot path; allocator used only during open/close |
| Logging | `std.log.scoped(.brz_queue)` |
| Atomics | Acquire/release ordering for writer↔reader synchronisation |
| Global state | None |