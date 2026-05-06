# Task 1: Core Types and Constants

## Overview

This document specifies the core types, constants, and foundational data structures
for **ringloom-queue** — a clean-room, high-performance, lock-free, memory-mapped IPC queue
in Zig. It covers the 4-byte header protocol, tailer state machine, fixed-layout
`extern struct` file headers, flat inline index, roll scheme table, platform
capabilities, pollable helper state machines, and the primary structs (`Queue`,
`Tailer`, `Prefetcher`, `Cleaner`).

All designs follow idiomatic Zig patterns: error unions, optional types, slices,
explicit allocator passing, and comptime generics. No wire protocol (BinaryWire) is
used — all structures are fixed-layout `extern struct` types that map directly onto
mmap'd memory with zero parsing overhead.

**Key architectural principles:**

- Single writer, multiple readers. The appender is **not** thread-safe. Tailers are independently thread-safe (no shared mutable state between tailers).
- Acquire/release atomic ordering (not SeqCst) on all shared fields.
- Tiered CAS backoff: spin → yield → exponential sleep (capped at 1 ms).
- Zero allocations on the hot path.
- File extensions: `.ringloom` for data files, `metadata.ringloom` for shared metadata.
- Pre-roll file creation with platform preallocation.
- Non-blocking tailer polling in the core; applications own blocking/wakeup policy.

---

## 1. Header Constants

The ringloom-queue protocol uses a 4-byte (32-bit) header word before every entry
(data or metadata). The top two bits encode the entry type, and the lower 30 bits
encode a length.

### Bit Layout

```
bits [29:0]   bit 30    bit 31    meaning
─────────────────────────────────────────────────────────
  0            0         0        UNALLOCATED  (0x00000000)
  size         0         0        data payload (size in bytes)
  size         1         0        METADATA     (0x40000000)
  0            0         1        WORKING      (0x80000000) — lock flag only, no PID
  0            1         1        EOF          (0xC0000000)
```

> **Change from previous design:** `WORKING` is now a simple lock flag. It does
> not encode a PID in the lower 30 bits. The `workingHeader()` helper takes no
> arguments.

### Zig Implementation

```zig
// src/ringloom/header.zig

/// The 4-byte ringloom-queue header constants.
/// This struct serves as a namespace — it is never instantiated.
pub const Header = struct {
    pub const UNALLOCATED: u32 = 0x00000000;
    pub const WORKING: u32 = 0x80000000;     // Lock flag only — no PID
    pub const METADATA: u32 = 0x40000000;
    pub const EOF: u32 = 0xC0000000;

    /// Mask to extract the 30-bit length field.
    pub const SIZE_MASK: u32 = 0x3FFFFFFF;

    /// Mask to extract the 2-bit meta field (top two bits).
    pub const META_MASK: u32 = 0xC0000000;

    /// Returns true if the header word represents unallocated space.
    pub inline fn isUnallocated(h: u32) bool {
        return h == UNALLOCATED;
    }

    /// Returns the meta-type bits (top 2 bits).
    pub inline fn metaType(h: u32) u32 {
        return h & META_MASK;
    }

    /// Returns the 30-bit data length field.
    pub inline fn dataLength(h: u32) u30 {
        return @truncate(h & SIZE_MASK);
    }

    /// Returns true if this header indicates a writer is actively working.
    pub inline fn isWorking(h: u32) bool {
        return metaType(h) == WORKING;
    }

    /// Returns true if this header is a metadata entry.
    pub inline fn isMetadata(h: u32) bool {
        return metaType(h) == METADATA;
    }

    /// Returns true if this header is an EOF marker.
    pub inline fn isEof(h: u32) bool {
        return metaType(h) == EOF;
    }

    /// Build a data header from a payload size.
    pub inline fn dataHeader(size: u30) u32 {
        return @as(u32, size);
    }

    /// Build a metadata header from a payload size.
    pub inline fn metadataHeader(size: u30) u32 {
        return METADATA | @as(u32, size);
    }

    /// Build a WORKING header. No PID — just the lock flag.
    pub inline fn workingHeader() u32 {
        return WORKING;
    }
};
```

### Design Notes

- Using `u30` via `@truncate` lets the compiler enforce that no one accidentally
  passes a value that would collide with the meta bits.
- All functions are `inline` since they are trivial bit operations used in hot paths.
- The `Header` struct serves as a namespace — it is never instantiated.
- `workingHeader()` takes no arguments. The old PID-encoded design added complexity
  without benefit in a single-writer model.

---

## 2. Tailer State Enum

The tailer state enum encodes the result of each peek/poll cycle. Tailers transition
between these states as they scan the queue.

### Zig Implementation

```zig
// src/ringloom/tailer.zig

/// The state of a tailer after a peek/poll operation.
pub const TailerState = enum(u8) {
    /// Waiting for the next entry to appear at the current position.
    awaiting_entry = 0,
    /// A writer holds the working lock at the current position.
    busy = 1,
    /// The queue file for the current cycle does not exist yet.
    awaiting_queue_file = 2,
    /// An fstat() call failed.
    err_stat = 3,
    /// An mmap() call failed (likely fatal).
    err_mmap = 4,
    /// The tailer has not been polled yet.
    not_yet_polled = 5,
    /// The appender needs to extend the queue file on disk.
    extend_needed = 6,
    /// A value was collected (synchronous read).
    collected = 7,

    pub fn description(self: TailerState) []const u8 {
        return switch (self) {
            .awaiting_entry => "AWAITING_ENTRY",
            .busy => "BUSY",
            .awaiting_queue_file => "AWAITING_QUEUEFILE",
            .err_stat => "E_STAT",
            .err_mmap => "E_MMAP",
            .not_yet_polled => "NOT_YET_POLLED",
            .extend_needed => "EXTEND_NEEDED",
            .collected => "COLLECTED",
        };
    }
};
```

> **Polling note:** When a tailer is in `awaiting_entry`, `poll()` returns
> immediately. The caller decides whether to spin, yield, sleep, or use an
> application-owned notifier before polling again. The queue core does not own a
> kernel wakeup mechanism.

### Parse Queue Block State

The inner block parser has its own distinct state enum:

```zig
/// Return state of the inner parse_queue_block scanner.
pub const ParseBlockState = enum(u8) {
    /// Stopped at an unallocated header — awaiting next entry.
    awaiting_entry = 0,
    /// Hit a WORKING header — a writer holds the lock.
    busy = 1,
    /// Hit an EOF marker — end of this queue file.
    reached_eof = 2,
    /// Data extends beyond the mapped window — need to extend or remap.
    need_extend = 3,
    /// Hit data but no data parser was provided.
    null_item = 4,
    /// An item was collected via the synchronous API.
    collected = 7,
};
```

---

## 3. Collected Value Struct

The `Collected` struct is used by the synchronous read API to return a parsed
message, its size, and its 64-bit index.

### Zig Implementation

```zig
/// A collected message returned by the synchronous read API.
pub fn Collected(comptime T: type) type {
    return struct {
        /// The deserialized message object.
        msg: ?T = null,
        /// The raw size of the payload in bytes.
        size: usize = 0,
        /// The 64-bit index (upper 32 = cycle, lower 32 = seqnum).
        index: u64 = 0,
    };
}

/// When using opaque/untyped messages (raw byte-slice access):
pub const RawCollected = struct {
    msg: ?[]const u8 = null,
    size: usize = 0,
    index: u64 = 0,
};
```

### Design Notes

- The generic `Collected(T)` pattern allows type-safe collection when the message
  type is known at compile time.
- `RawCollected` is the untyped equivalent for raw byte-slice access.
- In the common zero-copy case, `msg` points directly into the mmap buffer.

---

## 4. Codec Interface (Comptime Generics)

The codec defines how to serialize and deserialize messages. No wire protocol is
used — the codec works directly on raw byte slices from the mmap buffer.

### Zig Approach: Comptime Interface

```zig
// src/ringloom/codec.zig

/// A Codec defines how to serialize and deserialize messages for ringloom-queue.
/// Implement this interface on your message type.
///
/// Example:
///   const MyCodec = struct {
///       pub const Message = MyStruct;
///       pub fn parse(data: []const u8) ?Message { ... }
///       pub fn encodedSize(msg: Message) usize { ... }
///       pub fn write(buf: []u8, msg: Message) void { ... }
///       pub fn free(msg: *Message) void { ... }  // optional
///   };
pub fn Codec(comptime T: type) type {
    // Validate that T has the required declarations at compile time
    comptime {
        if (!@hasDecl(T, "Message")) @compileError("Codec must define a 'Message' type");
        if (!@hasDecl(T, "parse")) @compileError("Codec must define 'parse'");
        if (!@hasDecl(T, "encodedSize")) @compileError("Codec must define 'encodedSize'");
        if (!@hasDecl(T, "write")) @compileError("Codec must define 'write'");
    }
    return T;
}

/// A Dispatcher receives decoded messages from a tailer.
pub fn Dispatcher(comptime MessageType: type) type {
    return struct {
        pub const DispatchFn = *const fn (ctx: *anyopaque, index: u64, msg: MessageType) DispatchAction;

        dispatch_fn: DispatchFn,
        context: *anyopaque,

        pub fn dispatch(self: @This(), index: u64, msg: MessageType) DispatchAction {
            return self.dispatch_fn(self.context, index, msg);
        }
    };
}

/// What the dispatcher tells the tailer to do after processing a message.
pub const DispatchAction = enum {
    /// Continue processing more messages.
    @"continue",
    /// Stop processing after this message.
    stop,
};
```

### Default Raw Codec

A built-in codec that treats payloads as opaque byte slices with zero-copy semantics:

```zig
pub const DefaultRawCodec = struct {
    pub const Message = []const u8;

    /// Zero-copy parse: returns a slice directly into the mmap buffer.
    pub fn parse(data: []const u8) ?Message {
        return data;
    }

    pub fn encodedSize(msg: Message) usize {
        return msg.len;
    }

    pub fn write(buf: []u8, msg: Message) void {
        @memcpy(buf[0..msg.len], msg);
    }
};
```

### Alternative: Runtime Vtable

If runtime polymorphism is needed (e.g., swapping codecs without recompilation),
use a vtable struct with function pointers:

```zig
/// Runtime-polymorphic codec using function pointers.
pub const RuntimeCodec = struct {
    parse_fn: *const fn (data: [*]const u8, len: usize) ?*anyopaque,
    free_fn: ?*const fn (obj: *anyopaque) void,
    sizeof_fn: *const fn (obj: *anyopaque) usize,
    write_fn: *const fn (buf: [*]u8, obj: *anyopaque, len: usize) void,

    pub fn parse(self: RuntimeCodec, data: []const u8) ?*anyopaque {
        return self.parse_fn(data.ptr, data.len);
    }

    pub fn encodedSize(self: RuntimeCodec, obj: *anyopaque) usize {
        return self.sizeof_fn(obj);
    }

    pub fn write(self: RuntimeCodec, buf: []u8, obj: *anyopaque) void {
        self.write_fn(buf.ptr, obj, buf.len);
    }

    pub fn free(self: RuntimeCodec, obj: *anyopaque) void {
        if (self.free_fn) |f| f(obj);
    }
};
```

---

## 5. Roll Scheme Table

ringloom-queue rolls (rotates) its data files on a schedule. Each roll scheme defines:
a name, a Java date format string (used for filename generation), a roll period,
and index parameters.

### Zig Implementation

```zig
// src/ringloom/roll.zig

pub const RollScheme = struct {
    name: []const u8,
    /// Java SimpleDateFormat pattern (e.g. "yyyyMMdd-HH'F'").
    format_str: []const u8,
    /// Roll period in seconds.
    roll_length_secs: u32,
    /// Number of index entries per queue file.
    index_count: u32,
    /// Spacing between indexed entries (every Nth entry is indexed).
    index_spacing: u32,

    /// Roll period in milliseconds (what the queue stores internally).
    pub fn rollLengthMs(self: RollScheme) u64 {
        return @as(u64, self.roll_length_secs) * 1000;
    }
};

/// Complete table of known roll schemes (27 entries).
pub const roll_schemes = [_]RollScheme{
    // ── Sub-hourly ──────────────────────────────────────────────────────────
    .{ .name = "FIVE_MINUTELY",          .format_str = "yyyyMMdd-HHmm'V'",     .roll_length_secs = 5 * 60,       .index_count = 2048,   .index_spacing = 256 },
    .{ .name = "TEN_MINUTELY",           .format_str = "yyyyMMdd-HHmm'X'",     .roll_length_secs = 10 * 60,      .index_count = 2048,   .index_spacing = 256 },
    .{ .name = "TWENTY_MINUTELY",        .format_str = "yyyyMMdd-HHmm'XX'",    .roll_length_secs = 20 * 60,      .index_count = 2048,   .index_spacing = 256 },
    .{ .name = "HALF_HOURLY",            .format_str = "yyyyMMdd-HHmm'H'",     .roll_length_secs = 30 * 60,      .index_count = 2048,   .index_spacing = 256 },
    .{ .name = "FAST_HOURLY",            .format_str = "yyyyMMdd-HH'F'",       .roll_length_secs = 3600,         .index_count = 4096,   .index_spacing = 256 },
    .{ .name = "TWO_HOURLY",             .format_str = "yyyyMMdd-HH'II'",      .roll_length_secs = 2 * 3600,     .index_count = 4096,   .index_spacing = 256 },
    .{ .name = "FOUR_HOURLY",            .format_str = "yyyyMMdd-HH'IV'",      .roll_length_secs = 4 * 3600,     .index_count = 4096,   .index_spacing = 256 },
    .{ .name = "SIX_HOURLY",             .format_str = "yyyyMMdd-HH'VI'",      .roll_length_secs = 6 * 3600,     .index_count = 4096,   .index_spacing = 256 },
    .{ .name = "FAST_DAILY",             .format_str = "yyyyMMdd'F'",           .roll_length_secs = 86400,        .index_count = 4096,   .index_spacing = 256 },
    // ── Standard ────────────────────────────────────────────────────────────
    .{ .name = "MINUTELY",               .format_str = "yyyyMMdd-HHmm",        .roll_length_secs = 60,           .index_count = 2048,   .index_spacing = 16 },
    .{ .name = "HOURLY",                 .format_str = "yyyyMMdd-HH",          .roll_length_secs = 3600,         .index_count = 4096,   .index_spacing = 16 },
    .{ .name = "DAILY",                  .format_str = "yyyyMMdd",             .roll_length_secs = 86400,        .index_count = 8192,   .index_spacing = 64 },
    // ── Large / minimal roll ────────────────────────────────────────────────
    .{ .name = "LARGE_HOURLY",           .format_str = "yyyyMMdd-HH'L'",       .roll_length_secs = 3600,         .index_count = 8192,   .index_spacing = 64 },
    .{ .name = "LARGE_DAILY",            .format_str = "yyyyMMdd'L'",           .roll_length_secs = 86400,        .index_count = 32768,  .index_spacing = 128 },
    .{ .name = "XLARGE_DAILY",           .format_str = "yyyyMMdd'X'",           .roll_length_secs = 86400,        .index_count = 32768,  .index_spacing = 256 },
    .{ .name = "HUGE_DAILY",             .format_str = "yyyyMMdd'H'",           .roll_length_secs = 86400,        .index_count = 32768,  .index_spacing = 1024 },
    // ── Small / sparse ──────────────────────────────────────────────────────
    .{ .name = "SMALL_DAILY",            .format_str = "yyyyMMdd'S'",           .roll_length_secs = 86400,        .index_count = 8192,   .index_spacing = 8 },
    .{ .name = "LARGE_HOURLY_SPARSE",    .format_str = "yyyyMMdd-HH'LS'",      .roll_length_secs = 3600,         .index_count = 4096,   .index_spacing = 1024 },
    .{ .name = "LARGE_HOURLY_XSPARSE",   .format_str = "yyyyMMdd-HH'LX'",     .roll_length_secs = 3600,         .index_count = 2048,   .index_spacing = 1048576 },
    .{ .name = "HUGE_DAILY_XSPARSE",     .format_str = "yyyyMMdd'HX'",         .roll_length_secs = 86400,        .index_count = 16384,  .index_spacing = 1048576 },
    // ── Test / benchmark ────────────────────────────────────────────────────
    .{ .name = "TEST_SECONDLY",          .format_str = "yyyyMMdd-HHmmss'T'",   .roll_length_secs = 1,            .index_count = 32768,  .index_spacing = 4 },
    .{ .name = "TEST4_SECONDLY",         .format_str = "yyyyMMdd-HHmmss'T4'",  .roll_length_secs = 1,            .index_count = 32,     .index_spacing = 4 },
    .{ .name = "TEST_HOURLY",            .format_str = "yyyyMMdd-HH'T'",       .roll_length_secs = 3600,         .index_count = 16,     .index_spacing = 4 },
    .{ .name = "TEST_DAILY",             .format_str = "yyyyMMdd'T1'",          .roll_length_secs = 86400,        .index_count = 8,      .index_spacing = 1 },
    .{ .name = "TEST2_DAILY",            .format_str = "yyyyMMdd'T2'",          .roll_length_secs = 86400,        .index_count = 16,     .index_spacing = 2 },
    .{ .name = "TEST4_DAILY",            .format_str = "yyyyMMdd'T4'",          .roll_length_secs = 86400,        .index_count = 32,     .index_spacing = 4 },
    .{ .name = "TEST8_DAILY",            .format_str = "yyyyMMdd'T8'",          .roll_length_secs = 86400,        .index_count = 128,    .index_spacing = 8 },
};

/// Look up a roll scheme by name. Returns null if not found.
pub fn findSchemeByName(name: []const u8) ?RollScheme {
    for (roll_schemes) |s| {
        if (std.mem.eql(u8, s.name, name)) return s;
    }
    return null;
}

/// Look up a roll scheme by its Java date format string. Returns null if not found.
pub fn findSchemeByFormat(format_str: []const u8) ?RollScheme {
    for (roll_schemes) |s| {
        if (std.mem.eql(u8, s.format_str, format_str)) return s;
    }
    return null;
}
```

---

## 6. Java Date Format → strftime Conversion

ringloom-queue uses Java `SimpleDateFormat` patterns (e.g. `"yyyyMMdd-HH'F'"`) from
the roll scheme table for filename generation. These must be converted to
`strftime` patterns. Since all format strings are comptime-known constants, the
conversion can happen entirely at comptime.

### Conversion Table

| Java token | strftime | Meaning       |
|------------|----------|---------------|
| `yyyy`     | `%Y`     | 4-digit year  |
| `MM`       | `%m`     | 2-digit month |
| `dd`       | `%d`     | 2-digit day   |
| `HH`       | `%H`     | 24-hour hour  |
| `mm`       | `%M`     | Minute        |
| `ss`       | `%S`     | Second        |
| `'...'`    | literal  | Quoted text   |
| `-`        | `-`      | Literal dash  |

### Zig Implementation

```zig
// src/ringloom/roll.zig (continued)

pub const ConversionError = error{
    UnrecognizedToken,
    UnterminatedQuote,
    BufferOverflow,
};

/// Convert a Java SimpleDateFormat string to a C strftime format string.
/// Also returns a "cleaned" version of the Java format with quotes removed,
/// suitable for use as a filename template.
///
/// At comptime, this returns a struct with both strings as comptime-known slices.
/// At runtime, it writes into the provided buffers.
pub fn javaFormatToStrftime(
    java_fmt: []const u8,
    strftime_buf: []u8,
    clean_buf: []u8,
) ConversionError!struct { strftime: []const u8, clean: []const u8 } {
    var si: usize = 0; // strftime output index
    var ci: usize = 0; // clean output index
    var fi: usize = 0; // java format input index
    var in_quote = false;

    while (fi < java_fmt.len) {
        if (in_quote and java_fmt[fi] != '\'') {
            if (si >= strftime_buf.len or ci >= clean_buf.len) return ConversionError.BufferOverflow;
            strftime_buf[si] = java_fmt[fi];
            si += 1;
            clean_buf[ci] = java_fmt[fi];
            ci += 1;
            fi += 1;
        } else if (java_fmt[fi] == '\'') {
            in_quote = !in_quote;
            fi += 1;
        } else if (java_fmt[fi] == '-') {
            if (si >= strftime_buf.len or ci >= clean_buf.len) return ConversionError.BufferOverflow;
            strftime_buf[si] = '-';
            si += 1;
            clean_buf[ci] = '-';
            ci += 1;
            fi += 1;
        } else if (fi + 4 <= java_fmt.len and std.mem.eql(u8, java_fmt[fi..][0..4], "yyyy")) {
            if (si + 2 > strftime_buf.len) return ConversionError.BufferOverflow;
            strftime_buf[si] = '%';
            strftime_buf[si + 1] = 'Y';
            si += 2;
            @memcpy(clean_buf[ci..][0..4], "yyyy");
            ci += 4;
            fi += 4;
        } else if (fi + 2 <= java_fmt.len and std.mem.eql(u8, java_fmt[fi..][0..2], "MM")) {
            if (si + 2 > strftime_buf.len) return ConversionError.BufferOverflow;
            strftime_buf[si] = '%';
            strftime_buf[si + 1] = 'm';
            si += 2;
            @memcpy(clean_buf[ci..][0..2], "MM");
            ci += 2;
            fi += 2;
        } else if (fi + 2 <= java_fmt.len and std.mem.eql(u8, java_fmt[fi..][0..2], "dd")) {
            if (si + 2 > strftime_buf.len) return ConversionError.BufferOverflow;
            strftime_buf[si] = '%';
            strftime_buf[si + 1] = 'd';
            si += 2;
            @memcpy(clean_buf[ci..][0..2], "dd");
            ci += 2;
            fi += 2;
        } else if (fi + 2 <= java_fmt.len and std.mem.eql(u8, java_fmt[fi..][0..2], "HH")) {
            if (si + 2 > strftime_buf.len) return ConversionError.BufferOverflow;
            strftime_buf[si] = '%';
            strftime_buf[si + 1] = 'H';
            si += 2;
            @memcpy(clean_buf[ci..][0..2], "HH");
            ci += 2;
            fi += 2;
        } else if (fi + 2 <= java_fmt.len and std.mem.eql(u8, java_fmt[fi..][0..2], "mm")) {
            if (si + 2 > strftime_buf.len) return ConversionError.BufferOverflow;
            strftime_buf[si] = '%';
            strftime_buf[si + 1] = 'M';
            si += 2;
            @memcpy(clean_buf[ci..][0..2], "mm");
            ci += 2;
            fi += 2;
        } else if (fi + 2 <= java_fmt.len and std.mem.eql(u8, java_fmt[fi..][0..2], "ss")) {
            if (si + 2 > strftime_buf.len) return ConversionError.BufferOverflow;
            strftime_buf[si] = '%';
            strftime_buf[si + 1] = 'S';
            si += 2;
            @memcpy(clean_buf[ci..][0..2], "ss");
            ci += 2;
            fi += 2;
        } else {
            return ConversionError.UnrecognizedToken;
        }
    }

    if (in_quote) return ConversionError.UnterminatedQuote;

    return .{
        .strftime = strftime_buf[0..si],
        .clean = clean_buf[0..ci],
    };
}

/// Comptime version: returns a struct with the two format strings as comptime slices.
/// Use this when the Java format is a comptime-known constant (i.e. from the roll table).
pub fn comptimeJavaToStrftime(comptime java_fmt: []const u8) struct {
    strftime_fmt: []const u8,
    clean_fmt: []const u8,
} {
    comptime {
        var strftime_buf: [256]u8 = undefined;
        var clean_buf: [256]u8 = undefined;
        const result = javaFormatToStrftime(java_fmt, &strftime_buf, &clean_buf) catch |err| {
            @compileError(@tagName(err) ++ " in format: " ++ java_fmt);
        };
        const sf = result.strftime;
        const cl = result.clean;
        return .{
            .strftime_fmt = sf[0..sf.len].*,
            .clean_fmt = cl[0..cl.len].*,
        };
    }
}
```

### Filename Generation

Generate the `.ringloom` filename for a given cycle number:

```zig
/// Generate the .ringloom filename for a given cycle number.
/// cycle * roll_length_ms / 1000 = seconds since epoch → format with strftime pattern.
pub fn getCycleFn(
    allocator: std.mem.Allocator,
    dirname: []const u8,
    cycle: u64,
    roll_length_ms: u64,
    strftime_pattern: [*:0]const u8,
) ![]u8 {
    const raw_time: i64 = @intCast(cycle * (roll_length_ms / 1000));
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = @intCast(raw_time) };
    const day = epoch_seconds.getEpochDay();
    const year_day = day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_secs = epoch_seconds.getDaySeconds();

    // Since we only need %Y %m %d %H %M %S and literals,
    // build the string directly without calling libc strftime.
    var date_buf: [64]u8 = undefined;
    const date_str = formatDate(
        &date_buf,
        strftime_pattern,
        year_day.year,
        month_day.month.numeric(),
        month_day.day_index + 1,
        day_secs.getHoursIntoDay(),
        day_secs.getMinutesIntoHour(),
        day_secs.getSecondsIntoMinute(),
    );

    return std.fmt.allocPrint(allocator, "{s}/{s}.ringloom", .{ dirname, date_str });
}
```

### Tests for Format Conversion

```zig
test "java format conversion" {
    var sf_buf: [64]u8 = undefined;
    var cl_buf: [64]u8 = undefined;

    // DAILY: "yyyyMMdd" → "%Y%m%d"
    {
        const r = try javaFormatToStrftime("yyyyMMdd", &sf_buf, &cl_buf);
        try std.testing.expectEqualStrings("%Y%m%d", r.strftime);
        try std.testing.expectEqualStrings("yyyyMMdd", r.clean);
    }

    // FAST_HOURLY: "yyyyMMdd-HH'F'" → "%Y%m%d-%HF"
    {
        const r = try javaFormatToStrftime("yyyyMMdd-HH'F'", &sf_buf, &cl_buf);
        try std.testing.expectEqualStrings("%Y%m%d-%HF", r.strftime);
        try std.testing.expectEqualStrings("yyyyMMdd-HHF", r.clean);
    }

    // TEST_SECONDLY: "yyyyMMdd-HHmmss'T'" → "%Y%m%d-%H%M%ST"
    {
        const r = try javaFormatToStrftime("yyyyMMdd-HHmmss'T'", &sf_buf, &cl_buf);
        try std.testing.expectEqualStrings("%Y%m%d-%H%M%ST", r.strftime);
    }
}
```

---

## 7. 64-Bit Index Layout

Indices are 64-bit values with the cycle in the upper 32 bits and the
sequence number in the lower 32 bits.

```zig
// src/ringloom/index.zig

pub const Index = struct {
    /// Number of bits to shift for the cycle. Always 32.
    pub const cycle_shift: u6 = 32;

    /// Mask to extract the sequence number (lower 32 bits).
    pub const seqnum_mask: u64 = 0x00000000FFFFFFFF;

    /// Extract the cycle from a 64-bit index.
    pub inline fn cycle(index: u64) u32 {
        return @truncate(index >> cycle_shift);
    }

    /// Extract the sequence number from a 64-bit index.
    pub inline fn seqnum(index: u64) u32 {
        return @truncate(index & seqnum_mask);
    }

    /// Compose a 64-bit index from cycle and sequence number.
    pub inline fn compose(cyc: u32, seq: u32) u64 {
        return (@as(u64, cyc) << cycle_shift) | @as(u64, seq);
    }

    /// Return the index pointing to the start of the given cycle (seqnum = 0).
    pub inline fn cycleStart(cyc: u32) u64 {
        return @as(u64, cyc) << cycle_shift;
    }

    /// Return the index for the next cycle (current cycle + 1, seqnum = 0).
    pub inline fn nextCycleStart(index: u64) u64 {
        return cycleStart(cycle(index) + 1);
    }
};
```

---

## 8. Fixed-Layout File Structures

This is a key architectural element of ringloom-queue. Instead of a wire protocol that
requires parsing, all file headers are **fixed-layout `extern struct` types** that
map directly onto mmap'd memory. To read a field, cast the mmap pointer to the
struct type and access the field — zero parsing, zero allocation.

### SharedMetadata (512 bytes)

The shared metadata file (`metadata.ringloom`) is memory-mapped and shared between the
writer and all readers. Atomic fields are accessed with acquire/release ordering.

```zig
// src/ringloom/metadata.zig

pub const SharedMetadata = extern struct {
    /// Magic number: "BZQM" bytes as a little-endian u32 (0x4D515A42).
    magic: u32 = 0x4D515A42,
    /// Format version.
    version: u16 = 1,
    /// Reserved flags.
    flags: u16 = 0,
    /// Roll period in seconds (from roll scheme).
    roll_length_secs: u32,
    /// Index spacing (from roll scheme).
    index_spacing: u32,
    /// Index count per queue file (from roll scheme).
    index_count: u32,
    /// Roll epoch in milliseconds since Unix epoch (usually 0).
    epoch_ms: u64,
    /// Highest cycle number written. Accessed atomically (acquire/release).
    highest_cycle: u64 align(8),
    /// Lowest cycle number still available. Accessed atomically (acquire/release).
    lowest_cycle: u64 align(8),
    /// Monotonically increasing modification counter. Accessed atomically.
    modcount: u64 align(8),
    /// Published byte offset in the active cycle. Accessed atomically.
    write_position: u64 align(8),
    /// Appender lifecycle lease (0 = unlocked, non-zero = owner token).
    appender_lock: u64 align(8),
    /// Reserved for future use. Pads struct to exactly 512 bytes.
    _reserved: [440]u8 = [_]u8{0} ** 440,
};

comptime {
    std.debug.assert(@sizeOf(SharedMetadata) == 512);
}
```

**Usage pattern:**

```zig
// Map the metadata file
const meta_mmap = try std.posix.mmap(null, 512, PROT.READ | PROT.WRITE, .{ .TYPE = .SHARED }, meta_fd, 0);
const metadata: *SharedMetadata = @ptrCast(@alignCast(meta_mmap.ptr));

// Read highest cycle with acquire ordering
const hc = @atomicLoad(u64, &metadata.highest_cycle, .acquire);

// Update highest cycle with release ordering
@atomicStore(u64, &metadata.highest_cycle, new_cycle, .release);

// Increment modcount with atomic RMW
_ = @atomicRmw(u64, &metadata.modcount, .Add, 1, .release);
```

### QueueFileHeader (64 bytes)

Each `.ringloom` data file starts with a 64-byte fixed header, followed by the index
region, followed by the data region.

```zig
// src/ringloom/metadata.zig (continued)

pub const QueueFileHeader = extern struct {
    /// Magic number: "BZQC" bytes as a little-endian u32 (0x43515A42).
    magic: u32 = 0x43515A42,
    /// Format version.
    version: u16 = 1,
    /// Reserved flags.
    flags: u16 = 0,
    /// Roll period in seconds (redundant with metadata, for standalone validation).
    roll_length_secs: u32,
    /// Index spacing.
    index_spacing: u32,
    /// Index count (number of u64 slots in the index region).
    index_count: u32,
    /// Roll epoch in milliseconds since Unix epoch.
    epoch_ms: u64,
    /// Cycle number this file represents.
    created_cycle: u32,
    /// Reserved for future use. Pads struct to exactly 64 bytes.
    _reserved: [28]u8 = [_]u8{0} ** 28,
};

comptime {
    std.debug.assert(@sizeOf(QueueFileHeader) == 64);
}
```

**File layout:**

```
Offset 0                    → QueueFileHeader (64 bytes)
Offset 64                   → Index region (index_count × 8 bytes)
Offset 64 + index_count × 8 → Data region (entries with 4-byte headers)
```

### How Fixed Layouts Eliminate Parsing

In a traditional wire-protocol design, reading a header requires:
1. Read tag bytes to identify the field
2. Read length prefix
3. Decode the value
4. Repeat for each field

With `extern struct`, the layout is fixed at compile time. The compiler knows the
exact byte offset of every field. Reading `metadata.highest_cycle` compiles down
to a single memory load at a known offset — no branching, no parsing, no allocation.

---

## 9. Flat Index Region Types

The index region is a flat array of `u64` entries embedded directly in the queue
file, between the `QueueFileHeader` and the data region. Each index slot maps
to a sequence number range and stores the byte offset of the corresponding entry
in the data region.

```zig
// src/ringloom/index.zig (continued)

pub const IndexRegion = struct {
    /// Pointer to the base of the index array in the mmap'd file.
    entries: [*]align(8) volatile u64,
    /// Number of index slots.
    count: u32,
    /// Every Nth entry is indexed (index_spacing from roll scheme).
    spacing: u32,

    /// Compute which index slot a given sequence number maps to.
    /// Returns null if the seqnum doesn't align to a slot boundary
    /// or exceeds the index capacity.
    pub inline fn slotFor(self: IndexRegion, seqnum: u32) ?u32 {
        if (self.spacing == 0) return null;
        const slot = seqnum / self.spacing;
        if (slot >= self.count) return null;
        // Only exact multiples of spacing get indexed
        if (seqnum % self.spacing != 0) return null;
        return slot;
    }

    /// Look up the data-region byte offset stored in the given index slot.
    /// Returns null if the slot contains 0 (not yet written).
    pub inline fn lookup(self: IndexRegion, slot: u32) ?u64 {
        if (slot >= self.count) return null;
        const val = @atomicLoad(u64, &self.entries[slot], .acquire);
        if (val == 0) return null;
        return val;
    }

    /// Store a data-region byte offset into the given index slot.
    pub inline fn store(self: IndexRegion, slot: u32, offset: u64) void {
        if (slot >= self.count) return;
        @atomicStore(u64, &self.entries[slot], offset, .release);
    }

    /// Compute the byte offset where the data region starts (after the index).
    /// This is relative to the start of the file.
    pub inline fn dataRegionOffset(self: IndexRegion) u64 {
        return @sizeOf(QueueFileHeader) + @as(u64, self.count) * @sizeOf(u64);
    }

    /// Initialize an IndexRegion from a mapped queue file.
    pub fn fromMmap(mmap_base: [*]align(std.mem.page_size) u8, header: *const QueueFileHeader) IndexRegion {
        const index_base = mmap_base + @sizeOf(QueueFileHeader);
        return .{
            .entries = @ptrCast(@alignCast(index_base)),
            .count = header.index_count,
            .spacing = header.index_spacing,
        };
    }
};
```

**Advantages of the flat index:**

- No separate index file — the index is inline in the data file.
- Trivial to mmap alongside the data region.
- Lock-free reads via atomic loads.
- Cache-friendly linear layout.

---

## 10. Queue Struct

The `Queue` struct is the central handle for a ringloom-queue instance.

### Zig Implementation

```zig
// src/ringloom/queue.zig

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;

const Header = @import("header.zig").Header;
const Index = @import("index.zig").Index;
const IndexRegion = @import("index.zig").IndexRegion;
const TailerState = @import("tailer.zig").TailerState;
const RollScheme = @import("roll.zig").RollScheme;
const SharedMetadata = @import("metadata.zig").SharedMetadata;
const QueueFileHeader = @import("metadata.zig").QueueFileHeader;
const Appender = @import("appender.zig").Appender;
const Prefetcher = @import("prefetcher.zig").Prefetcher;
const Cleaner = @import("cleaner.zig").Cleaner;
const Platform = @import("platform.zig").Platform;

pub const Queue = struct {
    allocator: Allocator,

    // ── Identity ────────────────────────────────────────────────────────────
    /// Directory path of the queue.
    dirname: []const u8,

    // ── Configuration ───────────────────────────────────────────────────────
    /// Block size for mmap windows. Must be a power of two. Default: 2 MiB.
    blocksize: u32 = 2 * 1024 * 1024,
    /// Whether this queue has permission to create new files.
    create: bool = false,

    // ── Shared Metadata (replaces dirlist_* fields) ─────────────────────────
    /// File descriptor for metadata.ringloom.
    metadata_fd: ?posix.fd_t = null,
    /// Memory-mapped region for metadata.ringloom (512 bytes).
    metadata_mmap: ?[]align(std.mem.page_size) u8 = null,
    /// Typed pointer into the mmap — cast of metadata_mmap. Zero-cost access.
    metadata: ?*SharedMetadata = null,

    // ── Cycle tracking (cache of metadata atomics) ──────────────────────────
    highest_cycle: u64 = 0,
    lowest_cycle: u64 = 0,
    modcount: u64 = 0,

    // ── Roll configuration ──────────────────────────────────────────────────
    /// Roll period in milliseconds.
    roll_length_ms: u64 = 0,
    /// Roll epoch offset in milliseconds (usually 0).
    roll_epoch: i64 = -1,
    /// Original Java date format string (e.g. "yyyyMMdd").
    roll_format: ?[]const u8 = null,
    /// Human-readable roll scheme name (e.g. "DAILY").
    roll_name: ?[]const u8 = null,
    /// Converted strftime format string (e.g. "%Y%m%d").
    roll_strftime: ?[]const u8 = null,

    // ── Index configuration ─────────────────────────────────────────────────
    index_count: u32 = 0,
    index_spacing: u32 = 0,

    // ── Index layout (constant) ─────────────────────────────────────────────
    /// Always 32 in current implementations.
    cycle_shift: u6 = 32,
    /// Always 0x00000000FFFFFFFF.
    seqnum_mask: u64 = Index.seqnum_mask,

    // ── Pre-roll (next-cycle file pre-creation) ─────────────────────────────
    /// File descriptor for the pre-rolled next-cycle file (or null).
    preroll_fd: ?posix.fd_t = null,
    /// Memory-mapped buffer for the pre-rolled file (or null).
    preroll_mmap: ?[]align(std.mem.page_size) u8 = null,

    // ── Platform and latency helpers ────────────────────────────────────────
    /// Detected OS capability layer for preallocation, advice, and page touch.
    platform: Platform = .detect(),
    /// Optional page prefetcher for appender and tailer windows.
    prefetcher: ?*Prefetcher = null,
    /// Optional background cleaner for old mappings, page-cache hints, and retention.
    cleaner: ?*Cleaner = null,

    // ── Tailers ─────────────────────────────────────────────────────────────
    /// All reader tailers attached to this queue.
    tailers: std.ArrayList(*Tailer),
    /// The appender (created lazily on first append). NOT thread-safe.
    appender: ?*Appender = null,

    // ── Error state ─────────────────────────────────────────────────────────
    last_error: ?[]const u8 = null,

    /// Initialize a new queue handle. Does NOT open any files.
    /// Call `open()` after configuring version, roll scheme, etc.
    pub fn init(allocator: Allocator, dirname: []const u8) !*Queue {
        const queue = try allocator.create(Queue);
        queue.* = .{
            .allocator = allocator,
            .dirname = try allocator.dupe(u8, dirname),
            .tailers = std.ArrayList(*Tailer).init(allocator),
        };
        return queue;
    }

    /// Apply a roll scheme to this queue.
    pub fn setRollScheme(self: *Queue, scheme: RollScheme) !void {
        if (self.roll_name) |n| self.allocator.free(n);
        if (self.roll_format) |f| self.allocator.free(f);
        if (self.roll_strftime) |s| self.allocator.free(s);

        self.roll_name = try self.allocator.dupe(u8, scheme.name);
        self.roll_format = try self.allocator.dupe(u8, scheme.format_str);
        self.roll_length_ms = scheme.rollLengthMs();
        self.index_count = scheme.index_count;
        self.index_spacing = scheme.index_spacing;

        // Convert Java format → strftime
        var sf_buf: [128]u8 = undefined;
        var cl_buf: [128]u8 = undefined;
        const result = try @import("roll.zig").javaFormatToStrftime(
            scheme.format_str,
            &sf_buf,
            &cl_buf,
        );
        self.roll_strftime = try self.allocator.dupe(u8, result.strftime);
    }

    /// Double the block size (called when a single entry exceeds current blocksize).
    pub fn doubleBlocksize(self: *Queue) void {
        self.blocksize = self.blocksize << 1;
    }

    /// Compute cycle number from a millisecond timestamp.
    pub fn cycleFromMs(self: *const Queue, ms: i64) u64 {
        const epoch = if (self.roll_epoch == -1) @as(i64, 0) else self.roll_epoch;
        return @intCast(@divTrunc(ms - epoch, @as(i64, @intCast(self.roll_length_ms))));
    }

    /// Get current wall-clock time in milliseconds since Unix epoch.
    pub fn clockMs(_: *const Queue) i64 {
        return std.time.milliTimestamp();
    }

    /// Release all resources.
    pub fn deinit(self: *Queue) void {
        // Close all tailers
        while (self.tailers.items.len > 0) {
            const tailer = self.tailers.items[self.tailers.items.len - 1];
            tailer.deinit();
            _ = self.tailers.pop();
        }
        self.tailers.deinit();

        // Close appender
        if (self.appender) |a| {
            a.deinit();
            self.appender = null;
        }

        // Stop and tear down helper state
        if (self.uring_ctx) |ctx| {
            ctx.deinit();
            self.allocator.destroy(ctx);
            self.uring_ctx = null;
        }

        // Unmap and close pre-roll file
        if (self.preroll_mmap) |m| {
            posix.munmap(m);
        }
        if (self.preroll_fd) |fd| {
            posix.close(fd);
        }

        // Unmap and close shared metadata
        if (self.metadata_mmap) |m| {
            posix.munmap(m);
        }
        if (self.metadata_fd) |fd| {
            posix.close(fd);
        }

        // Free strings
        if (self.roll_format) |f| self.allocator.free(f);
        if (self.roll_name) |n| self.allocator.free(n);
        if (self.roll_strftime) |s| self.allocator.free(s);
        self.allocator.free(self.dirname);

        self.allocator.destroy(self);
    }
};
```

### Key Design Decisions

| Aspect | Design |
|--------|--------|
| Shared metadata | Single `*SharedMetadata` pointer cast from mmap — replaces `DirlistFields` and all `dirlist_*` fields |
| Pre-roll | `preroll_fd` + `preroll_mmap` for the next-cycle file, created ahead of time via platform preallocation |
| Polling | Core readers expose non-blocking `poll()`; blocking/wakeup policy belongs to the application |
| Writer model | Single writer — appender is NOT thread-safe, no lock needed for append path |
| Reader model | Multiple readers — each tailer is independently thread-safe, no shared mutable state between tailers |
| String storage | `[]const u8` slices with `allocator.dupe`/`allocator.free` |
| Optional types | `?[]const u8`, `?*Tailer`, `?posix.fd_t` instead of sentinel values |

---

## 11. Tailer Struct

The `Tailer` struct tracks the state of a single reader (or the appender) as it
scans through queue files.

### Zig Implementation

```zig
// src/ringloom/tailer.zig (continued)

const std = @import("std");
const posix = std.posix;
const Queue = @import("queue.zig").Queue;

pub const MmapProtection = enum {
    read_only,
    read_write,
};

pub const Tailer = struct {
    // ── Dispatch ────────────────────────────────────────────────────────────
    /// Index of the last dispatched message. Messages with index <= this are skipped.
    dispatch_after: u64 = 0,
    /// Current state of the tailer.
    state: TailerState = .not_yet_polled,
    /// Callback for delivering messages (null for appender).
    dispatcher: ?*const fn (ctx: *anyopaque, index: u64, msg: *anyopaque) i32 = null,
    /// User context passed to dispatcher.
    dispatch_ctx: ?*anyopaque = null,
    /// Pointer to a Collected struct for synchronous reads (set during collect()).
    collect: ?*RawCollected = null,

    // ── mmap protection ─────────────────────────────────────────────────────
    mmap_protection: MmapProtection = .read_only,

    // ── Currently open queue file ───────────────────────────────────────────
    /// Cycle number of the currently open queue file.
    qf_cycle_open: u64 = 0,
    /// Filename of the currently open queue file.
    qf_filename: ?[]const u8 = null,
    /// File descriptor of the currently open queue file.
    qf_fd: ?posix.fd_t = null,
    /// Cached file size from fstat.
    qf_file_size: u64 = 0,

    // ── Position tracking ───────────────────────────────────────────────────
    /// Byte offset of the next header to read (from start of file).
    qf_tip: u64 = 0,
    /// 64-bit index of the next entry at qf_tip.
    qf_index: u64 = 0,

    // ── mmap window ─────────────────────────────────────────────────────────
    /// Currently mapped buffer, or null if not mapped.
    qf_buf: ?[]align(std.mem.page_size) u8 = null,
    /// File offset where the current mmap window starts.
    qf_mmapoff: u64 = 0,
    /// Size of the current mmap window.
    qf_mmapsz: u64 = 0,

    // ── Read prefetch ───────────────────────────────────────────────────────
    /// Next read range to pre-map/advise/touch, bounded by published data.
    read_prefetch: ReadPrefetchState = .{},

    // ── Parent ──────────────────────────────────────────────────────────────
    /// Back-pointer to the owning queue.
    queue: *Queue,

    /// Clean up this tailer's resources (close fd, unmap, unregister prefetch).
    pub fn deinit(self: *Tailer) void {
        self.queue.unregisterTailerPrefetch(self);

        // Unmap queue file
        if (self.qf_buf) |buf| {
            posix.munmap(buf);
            self.qf_buf = null;
        }

        // Close queue file fd
        if (self.qf_fd) |fd| {
            posix.close(fd);
            self.qf_fd = null;
        }

        // Free filename
        if (self.qf_filename) |fn_slice| {
            self.queue.allocator.free(fn_slice);
            self.qf_filename = null;
        }

        self.queue.allocator.destroy(self);
    }

    /// Translate a file-absolute byte offset to a pointer within the mmap window.
    /// Returns null if the offset is outside the current window.
    pub fn offsetToPtr(self: *const Tailer, file_offset: u64) ?[*]u8 {
        if (self.qf_buf == null) return null;
        if (file_offset < self.qf_mmapoff) return null;
        const delta = file_offset - self.qf_mmapoff;
        if (delta >= self.qf_mmapsz) return null;
        return self.qf_buf.?.ptr + delta;
    }
};
```

### Thread Safety Notes

- **Appender:** NOT thread-safe. Only one active thread/process may append to a queue
  at a time; the appender lease is acquired outside the hot path. The 4-byte header
  transition (`UNALLOCATED → WORKING → DATA`) is still atomic so readers and recovery
  never observe a partially published payload.
- **Reader tailers:** Each tailer is independently thread-safe — it has its own mmap
  window, tip, index, and state. No mutable state is shared between tailers. Multiple
  tailers can read the same queue concurrently from different threads.
- **Shared metadata access:** Tailers read `highest_cycle` and `modcount` from the
  shared metadata file using atomic loads with `.acquire` ordering.

---

## 12. Platform and Pollable Helper Types

The core queue uses a small platform capability layer and pollable maintenance
state machines. The platform layer hides Linux/macOS differences; helper state
machines do bounded work and can either be driven by native Zig helper threads or
by an embedding application through the C ABI.

```zig
// src/ringloom/platform.zig

const std = @import("std");
const posix = std.posix;

pub const StepResult = enum(u8) {
    idle,
    made_progress,
    more_work,
};

pub const Platform = struct {
    supports_map_populate: bool,
    supports_madv_populate_write: bool,
    supports_huge_pages: bool,
    page_size: usize,

    pub fn detect() Platform {
        return .{
            .supports_map_populate = builtin.os.tag == .linux,
            .supports_madv_populate_write = builtin.os.tag == .linux,
            .supports_huge_pages = builtin.os.tag == .linux,
            .page_size = std.heap.pageSize(),
        };
    }

    pub fn preallocate(self: Platform, fd: posix.fd_t, offset: u64, len: u64) !void;
    pub fn adviseReadAhead(self: Platform, fd: posix.fd_t, offset: u64, len: u64) !void;
    pub fn adviseDontNeed(self: Platform, fd: posix.fd_t, offset: u64, len: u64) !void;
};
```

### Helper Poll Flow

1. Appender and tailers publish their current positions in process-local helper state.
2. `Prefetcher.poll(max_work_units)` prepares write and read windows within its budget.
3. `Cleaner.poll(max_work_units)` reclaims old local mappings and applies retention within its budget.
4. Native Zig helper threads are thin loops around these poll calls.
5. C ABI users call the same poll functions from application-owned threads/event loops.

Each poll call returns `StepResult` so the caller can decide whether to call
again immediately, defer, or sleep.

---

## 13. Behaviour Constants and Defaults

```zig
// src/ringloom/config.zig

/// Shared metadata filename (replaces the old directory listing file).
pub const metadata_filename = "metadata.ringloom";

/// Queue data file extension.
pub const queue_file_extension = ".ringloom";

/// Number of cycles to look back when patching missing EOF markers.
/// An appender starts seeking from (highest_cycle - patch_cycles).
pub const patch_cycles: u32 = 3;

/// Default initial size for new queue files on disk (bytes).
/// Used with platform preallocation.
pub const default_qf_disk_size: u64 = 83_754_496;

/// Default mmap block size (1 MiB). Must be a power of two.
pub const default_blocksize: u32 = 2 * 1024 * 1024;

/// Maximum payload size for a single entry (bytes).
pub const max_data_size: usize = 1000;

/// How far ahead (in milliseconds) to pre-create the next-cycle file.
/// The appender creates the next roll file this many ms before the roll boundary.
pub const default_preroll_ms: u64 = 1000;

/// Maximum backoff sleep duration for tiered CAS backoff (nanoseconds).
/// Backoff strategy: spin → yield → exponential sleep (capped at this value).
pub const max_cas_backoff_ns: u64 = 1_000_000; // 1 ms
```

---

## 14. Error Set

```zig
// src/ringloom/errors.zig

pub const RingloomError = error{
    // ── File I/O ────────────────────────────────────────────────────
    DirStatFailed,
    NotADirectory,
    OpenFailed,
    StatFailed,
    MmapFailed,
    MunmapFailed,
    SeekFailed,
    WriteFailed,
    RenameFailed,
    PreallocateFailed,

    // ── Queue state ─────────────────────────────────────────────────
    QueueIsNull,
    MetadataMagicMismatch,
    MetadataVersionMismatch,
    QueueFileMagicMismatch,
    QueueFileVersionMismatch,
    CreateNotPermitted,
    CreateRequiresRollScheme,
    CreateRequiresEmptyDir,

    // ── Roll / format ───────────────────────────────────────────────
    RollFormatNotRecognized,
    RollFormatMissing,
    RollLengthMissing,
    RollEpochMissing,
    UnrecognizedFormatToken,
    UnterminatedQuote,

    // ── Metadata ────────────────────────────────────────────────────
    MetadataParseFailed,
    MetadataFieldsMissing,
    MetadataReopenFailed,

    // ── Data ────────────────────────────────────────────────────────
    MessageTooLarge,
    WriteConflict,

    // ── Index ───────────────────────────────────────────────────────
    IndexSlotOutOfBounds,
    IndexRegionCorrupted,

    // ── Platform / helpers ──────────────────────────────────────────
    PlatformCapabilityUnavailable,
    PreallocateFailed,
    PrefetchFailed,
    CleanerFailed,

    // ── Pre-roll ────────────────────────────────────────────────────
    PrerollCreateFailed,
    PrerollPreallocateFailed,

    // ── Memory ──────────────────────────────────────────────────────
    OutOfMemory,
};
```

### Usage Pattern

```zig
pub fn open(self: *Queue) RingloomError!void {
    const stat = std.posix.fstat(fd) catch return RingloomError.StatFailed;
    // ...
}

// Caller:
queue.open() catch |err| {
    std.log.err("Failed to open queue: {}", .{err});
    return err;
};
```

---

## 15. Suggested File Layout

```
src/
└── ringloom/
    ├── header.zig       — Header constants and bit manipulation
    ├── index.zig        — 64-bit index layout (cycle/seqnum), IndexRegion
    ├── metadata.zig     — SharedMetadata, QueueFileHeader (extern structs)
    ├── roll.zig         — RollScheme, roll_schemes table, format conversion
    ├── tailer.zig       — TailerState enum, Tailer struct, ParseBlockState
    ├── queue.zig        — Queue struct
    ├── codec.zig        — Codec interface, Dispatcher, DefaultRawCodec
    ├── config.zig       — Constants and tuning parameters
    ├── errors.zig       — RingloomError error set
    ├── platform.zig     — Linux/macOS preallocation, advice, page touch
    ├── prefetcher.zig   — Pollable write/read prefetch state machine
    ├── cleaner.zig      — Pollable cleanup/retention state machine
    └── root.zig         — pub usingnamespace or re-exports
```

---

## 16. Summary Checklist

| Component | Zig module | Status |
|-----------|------------|--------|
| Header constants | `header.zig` | ☐ |
| TailerState enum | `tailer.zig` | ☐ |
| ParseBlockState enum | `tailer.zig` | ☐ |
| Collected struct | `tailer.zig` | ☐ |
| Codec interface | `codec.zig` | ☐ |
| RollScheme + table (27 schemes) | `roll.zig` | ☐ |
| Java→strftime conversion | `roll.zig` | ☐ |
| 64-bit index helpers | `index.zig` | ☐ |
| SharedMetadata (512-byte extern struct) | `metadata.zig` | ☐ |
| QueueFileHeader (64-byte extern struct) | `metadata.zig` | ☐ |
| IndexRegion (flat u64 array) | `index.zig` | ☐ |
| Queue struct | `queue.zig` | ☐ |
| Tailer struct | `tailer.zig` | ☐ |
| Platform capabilities | `platform.zig` | ☐ |
| Prefetcher state machine | `prefetcher.zig` | ☐ |
| Cleaner state machine | `cleaner.zig` | ☐ |
| Behaviour constants | `config.zig` | ☐ |
| Error set | `errors.zig` | ☐ |
