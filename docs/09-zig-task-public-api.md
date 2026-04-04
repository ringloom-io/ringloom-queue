# Task 7: Public API and Language Bindings

## Overview

This task covers the design of the public Zig API for the Chronicle Queue reimplementation,
equivalent to the C library's `libchronicle.h`. The goal is to provide an idiomatic Zig
interface that leverages the type system for safety and ergonomics, while also exposing a
C ABI layer for backwards compatibility with existing Python and kdb+ bindings.

The C API uses opaque pointers (`queue_t*`, `tailer_t*`), global error strings, and
`void*` callback signatures — all patterns that Zig can improve upon with generics,
error unions, and comptime type safety.

---

## 1. Idiomatic Zig API Design

### 1.1 Core Types

Replace the C library's opaque pointer + free-function pairs with Zig structs that own
their resources and clean up via `deinit()`.

```zig
// src/chronicle.zig — Public API

const std = @import("std");

pub const RollScheme = @import("roll_scheme.zig").RollScheme;
pub const Wire = @import("wire.zig");
pub const Index = u64;

pub const TailerState = enum(u8) {
    awaiting_entry = 0,
    busy = 1,
    awaiting_queuefile = 2,
    err_stat = 3,
    err_mmap = 4,
    not_yet_polled = 5,
    extend_fail = 6,
    collected = 7,
};

pub const Version = enum(u8) {
    unknown = 0,
    v5 = 5,
};
```

### 1.2 Queue Struct with Builder Pattern

The C API uses a multi-step init → set → open sequence:

```c
queue_t* q = chronicle_init(dir);
chronicle_set_version(q, 5);
chronicle_set_roll_scheme(q, "FAST_DAILY");
chronicle_set_encoder(q, &sizeof_fn, &write_fn);
chronicle_set_create(q, 1);
chronicle_open(q);
```

The Zig equivalent uses a configuration struct and a single `open()` call:

```zig
pub const QueueConfig = struct {
    /// Directory containing queue files
    dir: []const u8,

    /// Protocol version. If `.unknown`, auto-detected from existing queue files.
    version: Version = .unknown,

    /// Roll scheme name (e.g. "FAST_DAILY", "DAILY"). null = auto-detect.
    roll_scheme: ?RollScheme = null,

    /// Custom roll date format. Overrides the roll_scheme format if set.
    roll_date_format: ?[]const u8 = null,

    /// If true, create the queue directory and metadata if it doesn't exist.
    create: bool = false,

    /// Allocator for internal buffers, mmap bookkeeping, tailer lists.
    allocator: std.mem.Allocator,
};
```

### 1.3 Queue with Comptime Generic Message Type

Instead of `void*` callbacks, use Zig's comptime generics. The user provides a
`MessageType` and a `Codec` that knows how to serialize/deserialize it:

```zig
/// A Codec tells the Queue how to transform between raw bytes and user messages.
///
/// For simple string queues, use `chronicle.StringCodec`.
/// For BinaryWire messages, use `chronicle.WireCodec`.
pub fn Codec(comptime T: type) type {
    return struct {
        /// Deserialize bytes from the queue into a user message.
        /// The returned value borrows from `data` — it is only valid until
        /// the next call to `poll()` or `collect()` on the same tailer.
        parse: *const fn (data: []const u8) error{ParseError}!T,

        /// Return the serialized size of a message (for pre-allocation).
        serialized_size: *const fn (msg: T) usize,

        /// Serialize a message into the provided buffer.
        /// `buf.len` is guaranteed to be >= `serialized_size(msg)`.
        write: *const fn (buf: []u8, msg: T) void,
    };
}

pub fn Queue(comptime MessageType: type) type {
    return struct {
        const Self = @This();

        // --- internal fields ---
        allocator: std.mem.Allocator,
        dir: []const u8,
        version: Version,
        roll: RollSchemeInfo,
        codec: Codec(MessageType),
        // ... mmap state, directory listing, tailer linked list, etc.

        /// Open a queue with the given configuration and codec.
        pub fn open(config: QueueConfig, codec: Codec(MessageType)) !Self {
            // 1. stat directory, detect version from existing files
            // 2. mmap metadata.cq4t
            // 3. parse roll config from metadata
            // 4. populate highest_cycle / lowest_cycle / modcount
            _ = config;
            _ = codec;
            @panic("TODO: implement");
        }

        /// Close the queue and release all resources (munmap, close fds).
        pub fn deinit(self: *Self) void {
            _ = self;
            @panic("TODO: implement");
        }

        /// Append a message to the queue using the current wall-clock time.
        /// Returns the index (cycle << shift | seqnum) of the written entry.
        pub fn append(self: *Self, msg: MessageType) !Index {
            return self.appendWithTimestamp(msg, std.time.milliTimestamp());
        }

        /// Append a message with an explicit timestamp (milliseconds since epoch).
        /// This is essential for deterministic testing and replay.
        pub fn appendWithTimestamp(self: *Self, msg: MessageType, timestamp_ms: i64) !Index {
            _ = self;
            _ = msg;
            _ = timestamp_ms;
            @panic("TODO: implement");
        }

        /// Create a tailer starting from the given index (0 = beginning).
        pub fn tailer(self: *Self, start_index: Index) !Tailer(MessageType) {
            _ = self;
            _ = start_index;
            @panic("TODO: implement");
        }

        /// Peek all tailers, advancing any that have new data available.
        pub fn peek(self: *Self) void {
            _ = self;
            @panic("TODO: implement");
        }

        /// Return the queue version (4 or 5).
        pub fn getVersion(self: *const Self) Version {
            return self.version;
        }

        /// Return the roll scheme name, e.g. "FAST_DAILY".
        pub fn getRollScheme(self: *const Self) ?[]const u8 {
            _ = self;
            @panic("TODO: implement");
        }

        /// Return the filename for a given cycle number.
        pub fn getCyclePath(self: *const Self, cycle: u32) ![]const u8 {
            _ = self;
            _ = cycle;
            @panic("TODO: implement");
        }
    };
}
```

### 1.4 Tailer Struct — Iterator Pattern

The C API has two consumption models: callback-based (`cdispatch_f`) and blocking
(`chronicle_collect`/`chronicle_return`). In Zig, we unify these into an **iterator**:

```zig
pub fn Tailer(comptime MessageType: type) type {
    return struct {
        const Self = @This();

        state: TailerState,
        current_index: Index,
        // ... internal mmap state, queue back-pointer, etc.

        /// A single entry read from the queue.
        pub const Entry = struct {
            index: Index,
            message: MessageType,
            raw_size: usize,
        };

        /// Attempt to read the next entry without blocking.
        /// Returns `null` if no entry is available yet.
        pub fn poll(self: *Self) !?Entry {
            _ = self;
            @panic("TODO: implement");
        }

        /// Block until the next entry is available, then return it.
        /// Equivalent to the C `chronicle_collect` function.
        ///
        /// Uses exponential backoff internally (usleep with increasing delay
        /// after 20 unsuccessful polls, then re-checks modcount).
        pub fn collect(self: *Self) !Entry {
            var delay_count: u64 = 0;
            while (true) {
                if (try self.poll()) |entry| {
                    return entry;
                }
                delay_count += 1;
                if (delay_count > 20) {
                    std.time.sleep(delay_count * std.time.ns_per_us);
                    // re-check directory listing modcount
                }
            }
        }

        /// Return the current tailer state.
        pub fn getState(self: *const Self) TailerState {
            return self.state;
        }

        /// Return the index of the last entry read (or the start position).
        pub fn getIndex(self: *const Self) Index {
            return self.current_index;
        }

        /// Close the tailer, releasing its mmap region.
        pub fn deinit(self: *Self) void {
            _ = self;
            @panic("TODO: implement");
        }
    };
}
```

There is no need for a separate `chronicle_return` in Zig. The C library uses it to
call `parser_free` on the deserialized object. In the Zig API, the `Entry.message`
borrows from the memory-mapped buffer and requires no explicit free. If the codec
allocates (e.g. for deep copies), the codec's `parse` function should use an arena
allocator owned by the tailer, which is reset on each `poll()` call.

---

## 2. Zig Generics for Message Types

### 2.1 The `Queue(comptime MessageType)` Pattern

The key insight is that `void*` in the C API is a type erasure mechanism. Zig's comptime
generics remove the need for this entirely:

```zig
// --- String queue (simplest case) ---
const StringQueue = chronicle.Queue([]const u8);

// --- Structured message queue ---
const TradeMessage = struct {
    symbol: []const u8,
    price: f64,
    quantity: u64,
    timestamp: i64,
};
const TradeQueue = chronicle.Queue(TradeMessage);
```

### 2.2 Built-in Codecs

Provide common codecs out of the box:

```zig
/// Codec for queues where each entry is plain UTF-8 text.
/// This is the equivalent of the C library's `wire_parse_textonly` +
/// `chronicle_encoder_default_*` functions.
pub const StringCodec = Codec([]const u8){
    .parse = struct {
        fn parse(data: []const u8) error{ParseError}![]const u8 {
            return data;
        }
    }.parse,
    .serialized_size = struct {
        fn size(msg: []const u8) usize {
            return msg.len;
        }
    }.size,
    .write = struct {
        fn write(buf: []u8, msg: []const u8) void {
            @memcpy(buf[0..msg.len], msg);
        }
    }.write,
};

/// Codec that uses BinaryWire framing (event_name → text).
/// Wraps the text in a wire text field, matching Java Chronicle Queue's
/// default text message format.
pub const WireTextCodec = Codec([]const u8){
    .parse = wireParseText,
    .serialized_size = wireTextSize,
    .write = wireTextWrite,
};
```

### 2.3 Custom Codec Example

Users implement their own codec for structured types:

```zig
const TradeCodec = chronicle.Codec(TradeMessage){
    .parse = struct {
        fn parse(data: []const u8) !TradeMessage {
            // Use Wire.parse to walk BinaryWire fields
            var reader = Wire.Reader.init(data);
            return TradeMessage{
                .symbol = try reader.readField("symbol").text(),
                .price = try reader.readField("price").float64(),
                .quantity = try reader.readField("quantity").uint64(),
                .timestamp = try reader.readField("timestamp").int64(),
            };
        }
    }.parse,
    .serialized_size = struct {
        fn size(msg: TradeMessage) usize {
            var pad = Wire.Pad.init();
            pad.fieldText("symbol", msg.symbol);
            pad.fieldFloat64("price", msg.price);
            pad.fieldUint64("quantity", msg.quantity);
            pad.fieldInt64("timestamp", msg.timestamp);
            return pad.size();
        }
    }.size,
    .write = struct {
        fn write(buf: []u8, msg: TradeMessage) void {
            var pad = Wire.Pad.initBuf(buf);
            pad.fieldText("symbol", msg.symbol);
            pad.fieldFloat64("price", msg.price);
            pad.fieldUint64("quantity", msg.quantity);
            pad.fieldInt64("timestamp", msg.timestamp);
        }
    }.write,
};
```

---

## 3. Error Handling

### 3.1 Replace Global Error String with Error Unions

The C library stores errors in a global `cerr_msg` string and returns `-1` or `NULL`:

```c
// C: caller must check return AND call chronicle_strerror()
int rc = chronicle_open(queue);
if (rc != 0) {
    printf("Error: %s\n", chronicle_strerror());
}
```

The Zig API uses error unions — the idiomatic way to propagate errors:

```zig
pub const ChronicleError = error{
    /// The queue directory does not exist and `create` was not set.
    DirNotFound,
    /// The path exists but is not a directory.
    NotADirectory,
    /// Could not detect queue version from existing files and none was specified.
    VersionDetectFailed,
    /// The requested roll scheme name is not recognized.
    InvalidRollScheme,
    /// fstat() failed on a queue file.
    StatFailed,
    /// mmap() failed — possibly fatal, check system limits.
    MmapFailed,
    /// Attempted to append a message larger than 30-bit length field allows.
    MessageTooLarge,
    /// The wire protocol parser encountered invalid data.
    ParseError,
    /// A CAS operation on the working header timed out (contention).
    LockTimeout,
    /// Memory allocation failure.
    OutOfMemory,
    /// Queue file on disk needs extending but the extend operation failed.
    ExtendFailed,
};
```

Usage becomes clean and composable:

```zig
var queue = try StringQueue.open(.{
    .dir = "/var/chronicle/trades",
    .version = .v5,
    .roll_scheme = .fast_daily,
    .allocator = allocator,
}, chronicle.StringCodec);
defer queue.deinit();

const index = try queue.append("hello world");
```

### 3.2 Error Context for Diagnostics

For cases where callers need detailed error context (like the C `chronicle_strerror()`),
attach context to errors using Zig's `@errorReturnTrace()` in debug builds, and consider
a separate diagnostic channel:

```zig
/// Optional: retrieve detailed error information after a failed operation.
/// This is primarily for the C ABI compatibility layer. In pure Zig code,
/// prefer using error unions directly.
pub fn lastErrorMessage() []const u8 {
    return thread_local_error_message orelse "no error";
}

threadlocal var thread_local_error_message: ?[]const u8 = null;
```

---

## 4. C ABI Compatibility Layer

### 4.1 Exporting Functions from Zig

To maintain backwards compatibility with the existing Python (ctypes) and kdb+ bindings,
expose a C ABI using Zig's `export` keyword. This replaces the C `.so` with a Zig-built
`.so` that has the exact same symbol names.

```zig
// src/c_api.zig — C ABI compatibility layer
//
// This file wraps the idiomatic Zig API in extern "C" functions matching
// the original libchronicle.h signatures.

const std = @import("std");
const chronicle = @import("chronicle.zig");

/// We use the page allocator for C ABI allocations since the C caller
/// cannot provide a Zig allocator.
const c_allocator = std.heap.page_allocator;

// Opaque handles — the C caller sees these as void pointers.
// Internally they wrap a type-erased Queue and Tailer.
const CQueue = chronicle.Queue(CObj);
const CTailer = chronicle.Tailer(CObj);

// The C object type — an opaque pointer, matching `typedef void* COBJ`.
const CObj = *anyopaque;

// ─── Callback types matching libchronicle.h ───
const CParseF = *const fn ([*]u8, c_int) callconv(.C) ?CObj;
const CParseFreeF = *const fn (?CObj) callconv(.C) void;
const CSizeofF = *const fn (?CObj) callconv(.C) usize;
const CAppendF = *const fn ([*]u8, ?CObj, usize) callconv(.C) void;
const CDispatchF = *const fn (?*anyopaque, u64, ?CObj) callconv(.C) c_int;

// ─── Lifecycle ───

export fn chronicle_init(dir: [*:0]const u8) ?*CQueue {
    const queue = c_allocator.create(CQueue) catch return null;
    queue.* = CQueue.init(.{
        .dir = std.mem.sliceTo(dir, 0),
        .allocator = c_allocator,
    });
    return queue;
}

export fn chronicle_set_version(queue: *CQueue, version: c_int) void {
    queue.setVersion(switch (version) {
        5 => .v5,
        else => return,
    });
}

export fn chronicle_set_roll_scheme(queue: *CQueue, scheme: [*:0]const u8) c_int {
    queue.setRollScheme(std.mem.sliceTo(scheme, 0)) catch return -1;
    return 0;
}

export fn chronicle_set_create(queue: *CQueue, create: c_int) void {
    queue.config.create = (create != 0);
}

export fn chronicle_open(queue: *CQueue) c_int {
    queue.openFromConfig() catch |err| {
        setLastError(err);
        return -1;
    };
    return 0;
}

export fn chronicle_cleanup(queue: *CQueue) c_int {
    queue.deinit();
    c_allocator.destroy(queue);
    return 0;
}

export fn chronicle_strerror() [*:0]const u8 {
    return @ptrCast(chronicle.lastErrorMessage().ptr);
}

// ─── Appending ───

export fn chronicle_append(queue: *CQueue, msg: ?CObj) u64 {
    return queue.append(msg) catch return 0;
}

export fn chronicle_append_ts(queue: *CQueue, msg: ?CObj, ms: c_long) u64 {
    return queue.appendWithTimestamp(msg, ms) catch return 0;
}

// ─── Tailer ───

export fn chronicle_tailer(
    queue: *CQueue,
    dispatcher: ?CDispatchF,
    ctx: ?*anyopaque,
    index: u64,
) ?*CTailer {
    _ = dispatcher;
    _ = ctx;
    const t = queue.tailer(index) catch return null;
    const tailer_ptr = c_allocator.create(CTailer) catch return null;
    tailer_ptr.* = t;
    return tailer_ptr;
}

export fn chronicle_tailer_close(tailer: *CTailer) void {
    tailer.deinit();
    c_allocator.destroy(tailer);
}

export fn chronicle_tailer_state(tailer: *CTailer) c_int {
    return @intFromEnum(tailer.getState());
}

export fn chronicle_tailer_index(tailer: *CTailer) u64 {
    return tailer.getIndex();
}

// ─── Polling ───

export fn chronicle_peek() void {
    // Poll all known queues — iterate global queue list
}

export fn chronicle_peek_queue(queue: *CQueue) void {
    queue.peek();
}

export fn chronicle_peek_tailer(tailer: *CTailer) c_int {
    _ = tailer.poll() catch return -1;
    return @intFromEnum(tailer.getState());
}

// ─── Collect (blocking read) ───

const Collected = extern struct {
    msg: ?CObj,
    sz: usize,
    index: u64,
};

export fn chronicle_collect(tailer: *CTailer, result: *Collected) ?CObj {
    const entry = tailer.collect() catch return null;
    result.msg = @ptrCast(@constCast(entry.message));
    result.sz = entry.raw_size;
    result.index = entry.index;
    return result.msg;
}

export fn chronicle_return(tailer: *CTailer, result: *Collected) void {
    _ = tailer;
    _ = result;
    // In the Zig implementation, collected entries borrow from mmap.
    // No explicit free is needed unless the codec allocates.
}

// ─── Debug ───

export fn chronicle_debug() void {
    // Use std.log to print queue state
}

fn setLastError(err: anyerror) void {
    chronicle.thread_local_error_message = @errorName(err);
}
```

### 4.2 Build Configuration

The `build.zig` should produce both a static library and a shared object:

```zig
// In build.zig
const lib = b.addSharedLibrary(.{
    .name = "chronicle",
    .root_source_file = b.path("src/c_api.zig"),
    .target = target,
    .optimize = optimize,
});
// Ensure C ABI symbols are exported
lib.rdynamic = true;
b.installArtifact(lib);
```

### 4.3 Python Binding Compatibility

The existing Python bindings (`bindings/python/libchronicle.py`) use `ctypes` to load
`libchronicle.so` and call the `chronicle_*` functions. Because the Zig C ABI layer
exports identical symbol names with identical signatures, the Python bindings work
**without modification**:

```python
# Existing Python code — unchanged
from ctypes import cdll
cx = cdll.LoadLibrary("libchronicle.so")  # Now built from Zig
q = cx.chronicle_init(b"/var/chronicle/trades")
cx.chronicle_set_version(q, 5)
cx.chronicle_open(q)
```

The same applies to the kdb+ bindings in `bindings/kdb/shmipc.c` which link against
`libchronicle.so` at the C level.

---

## 5. The Collect API — Iterator and Blocking Patterns

### 5.1 Non-Blocking Iterator (Primary Pattern)

The recommended Zig pattern is the non-blocking `poll()` iterator, which integrates
naturally with event loops:

```zig
var t = try queue.tailer(0);
defer t.deinit();

while (true) {
    if (try t.poll()) |entry| {
        std.log.info("index={x} message={s}", .{ entry.index, entry.message });
    } else {
        // No data available — do other work, sleep, etc.
        std.time.sleep(1 * std.time.ns_per_ms);
    }
}
```

### 5.2 Blocking Collect (C API Compatibility)

The `collect()` method provides a blocking interface matching the C `chronicle_collect`
behavior, with internal exponential backoff:

```zig
// Blocks until an entry is available
const entry = try t.collect();
std.log.info("got: {s}", .{entry.message});
```

### 5.3 Bounded Iterator for Batch Processing

For processing a known range or draining available entries:

```zig
/// Read up to `max_entries` currently available entries without blocking.
pub fn pollBatch(
    self: *Self,
    max_entries: usize,
    results: []Entry,
) !usize {
    var count: usize = 0;
    while (count < max_entries and count < results.len) {
        if (try self.poll()) |entry| {
            results[count] = entry;
            count += 1;
        } else break;
    }
    return count;
}
```

### 5.4 Why Not async/await

Zig removed its built-in async/await support after 0.11. The iterator/poll pattern is
the standard approach in modern Zig. For integration with external event loops (e.g.
`epoll`/`io_uring`), the non-blocking `poll()` method is the correct primitive — callers
can wrap it in their own async framework as needed.

---

## 6. Debug Output

### 6.1 Replace printf with std.log

The C library uses `printf` throughout for debug output, gated by a global `debug` flag.
The Zig reimplementation should use `std.log`, which provides:

- Scoped log namespaces (e.g. `.chronicle`, `.wire`, `.tailer`)
- Compile-time log level filtering (zero cost when disabled)
- User-overridable log function via `pub const std_options`

```zig
const log = std.log.scoped(.chronicle);

pub fn peek(self: *Self) void {
    log.debug("peek: checking modcount, highest_cycle={d}", .{self.highest_cycle});
    // ...
    log.info("peek: advanced tailer to cycle={d} seqnum={d}", .{ cycle, seqnum });
}
```

### 6.2 Wire Protocol Tracing

The C library has a global `wire_trace` flag. Replace with a scoped logger:

```zig
const wire_log = std.log.scoped(.chronicle_wire);

pub fn parse(base: []const u8, callbacks: *WireCallbacks) void {
    wire_log.debug("parsing {d} bytes", .{base.len});
    // ...
    wire_log.debug("  event_name: {s}", .{name});
}
```

### 6.3 Hex Dump Utility

Port the C `formatbuf` / `printbuf` / `wirepad_dump` functions using `std.fmt`:

```zig
/// Format a byte slice as a hex dump, matching the C library's output format.
/// 00000000 e5 68 65 6c 6c 6f 00 00                          .hello..
pub fn hexDump(writer: anytype, data: []const u8) !void {
    var offset: usize = 0;
    while (offset < data.len) {
        try writer.print("{x:0>8} ", .{offset});
        const chunk = @min(16, data.len - offset);
        // hex bytes
        for (0..16) |i| {
            if (i == 8) try writer.writeByte(' ');
            if (i < chunk) {
                try writer.print("{x:0>2} ", .{data[offset + i]});
            } else {
                try writer.writeAll("   ");
            }
        }
        // ASCII
        for (0..chunk) |i| {
            const c = data[offset + i];
            try writer.writeByte(if (c >= 0x20 and c < 0x7f) c else '.');
        }
        try writer.writeByte('\n');
        offset += 16;
    }
}
```

---

## 7. Example Usage

### 7.1 Writing to a Queue

```zig
const std = @import("std");
const chronicle = @import("chronicle");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Open a v5 queue with FAST_HOURLY rolling, creating if needed
    var queue = try chronicle.Queue([]const u8).open(.{
        .dir = "/tmp/my-queue",
        .version = .v5,
        .roll_scheme = .fast_hourly,
        .create = true,
        .allocator = allocator,
    }, chronicle.WireTextCodec);
    defer queue.deinit();

    // Append some messages
    const idx1 = try queue.append("hello from zig");
    const idx2 = try queue.append("second message");
    const idx3 = try queue.append("a much longer item that will need encoding as variable length text");

    std.log.info("wrote 3 messages: {x}, {x}, {x}", .{ idx1, idx2, idx3 });
}
```

### 7.2 Reading from a Queue (Non-Blocking Poll)

```zig
const std = @import("std");
const chronicle = @import("chronicle");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Open an existing queue (version and roll scheme auto-detected)
    var queue = try chronicle.Queue([]const u8).open(.{
        .dir = "/tmp/my-queue",
        .allocator = allocator,
    }, chronicle.WireTextCodec);
    defer queue.deinit();

    std.log.info("queue version: {}", .{queue.getVersion()});

    // Create a tailer starting from the beginning
    var t = try queue.tailer(0);
    defer t.deinit();

    // Poll for messages in a loop
    var count: usize = 0;
    while (count < 100) {
        if (try t.poll()) |entry| {
            std.log.info("[{x}] {s}", .{ entry.index, entry.message });
            count += 1;
        } else {
            // No data — yield briefly, then re-check the directory listing
            std.time.sleep(500 * std.time.ns_per_ms);
            queue.peek();
        }
    }
}
```

### 7.3 Reading from a Queue (Blocking Collect)

```zig
const std = @import("std");
const chronicle = @import("chronicle");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var queue = try chronicle.Queue([]const u8).open(.{
        .dir = "/tmp/my-queue",
        .allocator = allocator,
    }, chronicle.WireTextCodec);
    defer queue.deinit();

    var t = try queue.tailer(0);
    defer t.deinit();

    // Block until each entry is available
    while (true) {
        const entry = try t.collect();
        std.log.info("[{x}] {s} ({d} bytes)", .{
            entry.index,
            entry.message,
            entry.raw_size,
        });
    }
}
```

### 7.4 Structured Messages with Custom Codec

```zig
const std = @import("std");
const chronicle = @import("chronicle");
const Wire = chronicle.Wire;

const Trade = struct {
    symbol: []const u8,
    price: f64,
    quantity: u64,
};

const trade_codec = chronicle.Codec(Trade){
    .parse = struct {
        fn f(data: []const u8) !Trade {
            var r = Wire.Reader.init(data);
            return Trade{
                .symbol = try r.fieldText("symbol"),
                .price = try r.fieldFloat64("price"),
                .quantity = try r.fieldUint64("quantity"),
            };
        }
    }.f,
    .serialized_size = struct {
        fn f(msg: Trade) usize {
            return Wire.sizeFieldText("symbol", msg.symbol) +
                Wire.sizeFieldFloat64("price") +
                Wire.sizeFieldUint64("quantity");
        }
    }.f,
    .write = struct {
        fn f(buf: []u8, msg: Trade) void {
            var w = Wire.Writer.init(buf);
            w.fieldText("symbol", msg.symbol);
            w.fieldFloat64("price", msg.price);
            w.fieldUint64("quantity", msg.quantity);
        }
    }.f,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var queue = try chronicle.Queue(Trade).open(.{
        .dir = "/tmp/trades",
        .version = .v5,
        .roll_scheme = .fast_daily,
        .create = true,
        .allocator = allocator,
    }, trade_codec);
    defer queue.deinit();

    _ = try queue.append(.{
        .symbol = "AAPL",
        .price = 150.25,
        .quantity = 100,
    });

    var t = try queue.tailer(0);
    defer t.deinit();

    const entry = try t.collect();
    std.log.info("trade: {s} @ {d:.2} x {d}", .{
        entry.message.symbol,
        entry.message.price,
        entry.message.quantity,
    });
}
```

### 7.5 Resuming from a Specific Index

```zig
// Resume reading from a known index (e.g. persisted from a previous run).
// Index encoding: upper bits = cycle, lower bits = sequence number.
// For v5 FAST_DAILY: 0x4A0500000003 = cycle 0x4A05 (18949), seqnum 3.
const resume_index: u64 = 0x4A0500000003;
var t = try queue.tailer(resume_index);
defer t.deinit();

const entry = try t.collect();
// entry.index == 0x4A0500000003, the 4th entry in cycle 18949
```

---

## 8. API Summary — C vs Zig

| C API | Zig API | Notes |
|---|---|---|
| `chronicle_init(dir)` | `Queue(T).open(config, codec)` | Config struct replaces multiple set calls |
| `chronicle_set_version(q, v)` | `config.version = .v5` | Set before open via config |
| `chronicle_set_roll_scheme(q, s)` | `config.roll_scheme = .fast_daily` | Enum instead of string |
| `chronicle_set_encoder(q, sf, wf)` | `codec` param to `open()` | Comptime generic codec |
| `chronicle_set_decoder(q, pf, ff)` | `codec` param to `open()` | Single codec handles both directions |
| `chronicle_set_create(q, 1)` | `config.create = true` | Bool instead of int |
| `chronicle_open(q)` | `Queue(T).open(config, codec)` | Combined init + open |
| `chronicle_cleanup(q)` | `queue.deinit()` | Idiomatic Zig resource cleanup |
| `chronicle_strerror()` | Error union (`!T`) | Errors are values, not global state |
| `chronicle_tailer(q, cb, ctx, idx)` | `queue.tailer(idx)` | Returns iterator, no callback needed |
| `chronicle_tailer_close(t)` | `tailer.deinit()` | — |
| `chronicle_collect(t, &res)` | `tailer.collect()` | Returns `Entry` struct directly |
| `chronicle_return(t, &res)` | *(not needed)* | Entries borrow from mmap |
| `chronicle_peek()` | `queue.peek()` | Per-queue, not global |
| `chronicle_peek_tailer(t)` | `tailer.poll()` | Returns `?Entry` |
| `chronicle_append(q, msg)` | `queue.append(msg)` | Type-safe, no void pointer |
| `chronicle_append_ts(q, msg, ms)` | `queue.appendWithTimestamp(msg, ms)` | — |
| `chronicle_debug()` | `std.log` scoped logging | Compile-time filtered |
| `chronicle_get_version(q)` | `queue.getVersion()` | Returns `Version` enum |
| `chronicle_get_cycle_fn(q, c)` | `queue.getCyclePath(cycle)` | Returns slice, caller frees |

---

## 9. Design Decisions and Rationale

### 9.1 Why Comptime Generics Over Runtime Dispatch

The C library uses function pointers for serialization (`csizeof_f`, `cappend_f`,
`cparse_f`). This is runtime dispatch with `void*` — no type safety, easy to mismatch.

Zig's `Queue(comptime MessageType)` approach:
- **Zero-cost**: codec functions are known at compile time and inlined
- **Type-safe**: impossible to pass a `Trade` to a `Queue([]const u8)`
- **Self-documenting**: the queue's message type is part of its type signature
- **Testable**: each `Queue(T)` instantiation is independently testable

The tradeoff is that each message type creates a separate compiled Queue. This is
acceptable because in practice each process uses one or two message types.

### 9.2 Memory Ownership Model

The C library has confusing ownership: `cparse_f` allocates, `cparsefree_f` frees,
and the user must call `chronicle_return()` after `chronicle_collect()`. Missing a
`chronicle_return` call leaks memory.

The Zig design avoids this:
- `poll()` returns an `Entry` whose `.message` borrows from the mmap region
- The borrow is valid until the next `poll()` or `deinit()` call
- If the codec needs to allocate (deep copy), it uses an arena owned by the tailer
- The arena is reset on each `poll()`, so no manual free is needed

### 9.3 Global State Elimination

The C library uses global state:
- `cerr_msg` — global error string
- `chronicle_peek()` — iterates a global linked list of all queues

The Zig API eliminates all global state:
- Errors are returned as error unions per-call
- `peek()` is a method on `Queue`, not a global function
- The C ABI layer uses `threadlocal` for the error string (for Python compat)