# Task 1: Core Types and Constants

## Overview

This document specifies the core types, constants, and foundational data structures
required to reimplement **libchronicle** in Zig. It covers the 4-byte header protocol,
tailer state machine, roll scheme table, and the two primary structs (`Queue` and `Tailer`).
All designs follow idiomatic Zig patterns: error unions, optional types, slices, and
explicit allocator passing.

---

## 1. Header Constants

The Chronicle Queue protocol uses a 4-byte (32-bit) header word before every entry
(data or metadata). The top two bits encode the entry type, and the lower 30 bits
encode a length or PID.

### Bit Layout

```
bits [29:0]   bit 30    bit 31    meaning
─────────────────────────────────────────────────────────
  0            0         0        HD_UNALLOCATED (0x00000000)
  size         0         0        data payload (size in bytes)
  size         1         0        HD_METADATA  (0x40000000)
  pid          0         1        HD_WORKING   (0x80000000)
  0            1         1        HD_EOF       (0xC0000000)
```

### Zig Implementation

```zig
// src/chronicle/header.zig

/// The 4-byte Chronicle Queue header constants.
pub const Header = struct {
    pub const UNALLOCATED: u32 = 0x00000000;
    pub const WORKING: u32 = 0x80000000;
    pub const METADATA: u32 = 0x40000000;
    pub const EOF: u32 = 0xC0000000;

    /// Mask to extract the 30-bit length/pid field.
    pub const MASK_LENGTH: u32 = 0x3FFFFFFF;

    /// Mask to extract the 2-bit meta field (top two bits). Same numeric value as EOF.
    pub const MASK_META: u32 = 0xC0000000;

    /// Returns true if the header word represents unallocated space.
    pub inline fn isUnallocated(h: u32) bool {
        return h == UNALLOCATED;
    }

    /// Returns the meta-type bits (top 2 bits).
    pub inline fn metaType(h: u32) u32 {
        return h & MASK_META;
    }

    /// Returns the 30-bit length field.
    pub inline fn dataLength(h: u32) u30 {
        return @truncate(h & MASK_LENGTH);
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

    /// Build a WORKING header stamped with the given PID (lower 30 bits).
    pub inline fn workingHeader(pid: u30) u32 {
        return WORKING | @as(u32, pid);
    }
};
```

### Design Notes

- Using `u30` via `@truncate` lets the compiler enforce that no one accidentally
  passes a value that would collide with the meta bits.
- All functions are `inline` since they are trivial bit operations used in hot paths.
- The `Header` struct serves as a namespace — it is never instantiated.

---

## 2. Tailer State Enum

In the C implementation, `tailstate_t` is an enum encoding the result of each
peek/poll cycle. The Zig version uses a proper tagged enum.

### C Reference

```c
typedef enum {
    TS_AWAITING_ENTRY,     // 0 — awaiting next entry
    TS_BUSY,               // 1 — hit working header
    TS_AWAITING_QUEUEFILE, // 2 — queue file missing, awaiting creation
    TS_E_STAT,             // 3 — fstat failed
    TS_E_MMAP,             // 4 — mmap failed (probably fatal)
    TS_PEEK,               // 5 — not yet polled
    TS_EXTEND_FAIL,        // 6 — queue file needs extending
    TS_COLLECTED,          // 7 — a value was collected
} tailstate_t;
```

### Zig Implementation

```zig
// src/chronicle/tailer.zig

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
            .not_yet_polled => "PEEK?",
            .extend_needed => "EXTEND_FAIL",
            .collected => "COLLECTED",
        };
    }
};
```

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

The `collected_t` struct is used by the synchronous `chronicle_collect` API to
return a parsed message, its size, and its 64-bit index.

### Zig Implementation

```zig
/// A collected message returned by the synchronous read API.
pub fn Collected(comptime T: type) type {
    return struct {
        /// The deserialized message object.
        msg: ?T = null,
        /// The raw size of the wire payload in bytes.
        size: usize = 0,
        /// The 64-bit chronicle index (upper 32 = cycle, lower 32 = seqnum).
        index: u64 = 0,
    };
}

/// When using opaque/untyped messages (equivalent to C's `void* msg`):
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

---

## 4. Function Pointer Types (Codec Interface)

The C library uses five function pointer typedefs to plug in custom serialization:

| C typedef     | Purpose                                      |
|---------------|----------------------------------------------|
| `cparse_f`    | Deserialize bytes → user object              |
| `cparsefree_f`| Free a deserialized object                   |
| `csizeof_f`   | Return serialized size of user object        |
| `cappend_f`   | Serialize user object into byte buffer       |
| `cdispatch_f` | Deliver (index, object) to application code  |

### Zig Approach: Comptime Interface

Rather than using raw function pointers, Zig's idiomatic pattern is a comptime
interface (sometimes called a "duck-typed generic" or vtable pattern). This gives
us type safety and potential inlining.

```zig
// src/chronicle/codec.zig

/// A Codec defines how to serialize and deserialize messages for a Chronicle Queue.
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
/// Equivalent to C's cdispatch_f + DISPATCH_CTX.
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

### Alternative: Runtime Vtable

If runtime polymorphism is needed (e.g., swapping codecs without recompilation),
use a vtable struct with function pointers:

```zig
/// Runtime-polymorphic codec using function pointers (mirrors the C design).
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

### Default Text Codec

The C library provides a default codec that treats payloads as opaque bytes (or
extracts text via wire parsing). Provide a built-in equivalent:

```zig
pub const DefaultTextCodec = struct {
    pub const Message = []const u8;

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

---

## 5. Roll Scheme Table

Chronicle Queue rolls (rotates) its data files on a schedule. Each roll scheme
defines: a name, a Java date format string, a roll period, and index parameters.

### C Reference (27 schemes)

```c
struct ROLL_SCHEME {
    char*    name;
    char*    formatstr;       // Java SimpleDateFormat pattern
    uint32_t roll_length_secs;
    uint32_t entries;         // index_count
    uint32_t index;           // index_spacing
};
```

### Zig Implementation

```zig
// src/chronicle/roll.zig

pub const RollScheme = struct {
    name: []const u8,
    /// Java SimpleDateFormat pattern (e.g. "yyyyMMdd-HH'F'").
    format_str: []const u8,
    /// Roll period in seconds.
    roll_length_secs: u32,
    /// Number of entries per index page (index_count).
    index_count: u32,
    /// Spacing between indexed entries (index_spacing).
    index_spacing: u32,

    /// Roll period in milliseconds (what the queue stores internally).
    pub fn rollLengthMs(self: RollScheme) u64 {
        return @as(u64, self.roll_length_secs) * 1000;
    }
};

/// Complete table of known roll schemes.
/// Order matches the C implementation for compatibility.
pub const roll_schemes = [_]RollScheme{
    // ── In use by cq5 ──────────────────────────────────────────────────────────
    .{ .name = "FIVE_MINUTELY",          .format_str = "yyyyMMdd-HHmm'V'",     .roll_length_secs = 5 * 60,       .index_count = 2048,   .index_spacing = 256 },
    .{ .name = "TEN_MINUTELY",           .format_str = "yyyyMMdd-HHmm'X'",     .roll_length_secs = 10 * 60,      .index_count = 2048,   .index_spacing = 256 },
    .{ .name = "TWENTY_MINUTELY",        .format_str = "yyyyMMdd-HHmm'XX'",    .roll_length_secs = 20 * 60,      .index_count = 2048,   .index_spacing = 256 },
    .{ .name = "HALF_HOURLY",            .format_str = "yyyyMMdd-HHmm'H'",     .roll_length_secs = 30 * 60,      .index_count = 2048,   .index_spacing = 256 },
    .{ .name = "FAST_HOURLY",            .format_str = "yyyyMMdd-HH'F'",       .roll_length_secs = 3600,         .index_count = 4096,   .index_spacing = 256 },
    .{ .name = "TWO_HOURLY",             .format_str = "yyyyMMdd-HH'II'",      .roll_length_secs = 2 * 3600,     .index_count = 4096,   .index_spacing = 256 },
    .{ .name = "FOUR_HOURLY",            .format_str = "yyyyMMdd-HH'IV'",      .roll_length_secs = 4 * 3600,     .index_count = 4096,   .index_spacing = 256 },
    .{ .name = "SIX_HOURLY",             .format_str = "yyyyMMdd-HH'VI'",      .roll_length_secs = 6 * 3600,     .index_count = 4096,   .index_spacing = 256 },
    .{ .name = "FAST_DAILY",             .format_str = "yyyyMMdd'F'",           .roll_length_secs = 86400,        .index_count = 4096,   .index_spacing = 256 },
    // ── Historical cq4 ─────────────────────────────────────────────────────────
    .{ .name = "MINUTELY",               .format_str = "yyyyMMdd-HHmm",        .roll_length_secs = 60,           .index_count = 2048,   .index_spacing = 16 },
    .{ .name = "HOURLY",                 .format_str = "yyyyMMdd-HH",          .roll_length_secs = 3600,         .index_count = 4096,   .index_spacing = 16 },
    .{ .name = "DAILY",                  .format_str = "yyyyMMdd",             .roll_length_secs = 86400,        .index_count = 8192,   .index_spacing = 64 },
    // ── Large / minimal roll ────────────────────────────────────────────────────
    .{ .name = "LARGE_HOURLY",           .format_str = "yyyyMMdd-HH'L'",       .roll_length_secs = 3600,         .index_count = 8192,   .index_spacing = 64 },
    .{ .name = "LARGE_DAILY",            .format_str = "yyyyMMdd'L'",           .roll_length_secs = 86400,        .index_count = 32768,  .index_spacing = 128 },
    .{ .name = "XLARGE_DAILY",           .format_str = "yyyyMMdd'X'",           .roll_length_secs = 86400,        .index_count = 32768,  .index_spacing = 256 },
    .{ .name = "HUGE_DAILY",             .format_str = "yyyyMMdd'H'",           .roll_length_secs = 86400,        .index_count = 32768,  .index_spacing = 1024 },
    // ── Small / test ────────────────────────────────────────────────────────────
    .{ .name = "SMALL_DAILY",            .format_str = "yyyyMMdd'S'",           .roll_length_secs = 86400,        .index_count = 8192,   .index_spacing = 8 },
    .{ .name = "LARGE_HOURLY_SPARSE",    .format_str = "yyyyMMdd-HH'LS'",      .roll_length_secs = 3600,         .index_count = 4096,   .index_spacing = 1024 },
    .{ .name = "LARGE_HOURLY_XSPARSE",   .format_str = "yyyyMMdd-HH'LX'",     .roll_length_secs = 3600,         .index_count = 2048,   .index_spacing = 1048576 },
    .{ .name = "HUGE_DAILY_XSPARSE",     .format_str = "yyyyMMdd'HX'",         .roll_length_secs = 86400,        .index_count = 16384,  .index_spacing = 1048576 },
    // ── Test / benchmark ────────────────────────────────────────────────────────
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

> **Note on C values:** The C code uses expressions like `2<<10` which equals 2048
> (not 1024). This is `2 * (1 << 10)` = `2 * 1024`. The Zig table uses the
> evaluated constants for clarity.

---

## 6. Java Date Format → strftime Conversion

Chronicle Queue stores Java `SimpleDateFormat` patterns like `"yyyyMMdd-HH'F'"`.
These must be converted to C `strftime` patterns for filename generation. The C
function `chronicle_apply_roll_scheme` does this conversion, and also strips
single-quoted literals.

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

The C code converts at runtime into a heap-allocated string. In Zig, since all
format strings are comptime-known constants from the roll scheme table, we can
do this conversion at comptime and avoid any allocation:

```zig
// src/chronicle/roll.zig (continued)

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
            // Inside quoted literal — copy verbatim to both outputs
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
/// Use this when the Java format is a comptime-known constant.
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
        // Copy to comptime-persistent arrays
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

The C function `chronicle_get_cycle_fn` converts a cycle number to a filename:

```zig
/// Generate the .cq4 filename for a given cycle number.
/// cycle * roll_length_ms / 1000 = seconds since epoch → format with strftime.
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

    // Use std.fmt to build "dirname/FORMATTED_DATE.cq4"
    // For strftime interop, we may need to call the C strftime via @cImport
    // or reimplement the limited subset we need.
    //
    // A practical approach: since we only need %Y %m %d %H %M %S and literals,
    // we can build the string directly:
    var date_buf: [64]u8 = undefined;
    const date_str = formatChronicleDate(
        &date_buf,
        strftime_pattern,
        year_day.year,
        month_day.month.numeric(),
        month_day.day_index + 1,
        day_secs.getHoursIntoDay(),
        day_secs.getMinutesIntoHour(),
        day_secs.getSecondsIntoMinute(),
    );

    return std.fmt.allocPrint(allocator, "{s}/{s}.cq4", .{ dirname, date_str });
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

Chronicle indices are 64-bit values with the cycle in the upper bits and the
sequence number in the lower bits.

```zig
// src/chronicle/index.zig

pub const Index = struct {
    /// Number of bits to shift for the cycle. Always 32 in current implementations.
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

## 8. Queue Struct

The `queue_t` struct is the central handle for a Chronicle Queue instance.

### C Reference (abbreviated)

The C struct contains: dirname, blocksize, version, create flag, directory listing
mmap state, glob of queue files, cycle tracking, roll config, index config,
codec function pointers, tailer linked list, appender, and a global next pointer.

### Zig Implementation

```zig
// src/chronicle/queue.zig

const std = @import("std");
const Allocator = std.mem.Allocator;

const Header = @import("header.zig").Header;
const Index = @import("index.zig").Index;
const TailerState = @import("tailer.zig").TailerState;
const RollScheme = @import("roll.zig").RollScheme;

/// Pointers into the memory-mapped directory listing for live-updated fields.
pub const DirlistFields = struct {
    /// Pointer to the 8-byte highest_cycle value in the mmap.
    highest_cycle: ?[*]align(1) volatile u64 = null,
    /// Pointer to the 8-byte lowest_cycle value in the mmap.
    lowest_cycle: ?[*]align(1) volatile u64 = null,
    /// Pointer to the 8-byte modcount value in the mmap (atomically incremented).
    modcount: ?[*]align(1) volatile u64 = null,
};

/// Queue version (v5 format).
pub const QueueVersion = enum(u8) {
    unset = 0,
    v5 = 5,
};

pub const Queue = struct {
    allocator: Allocator,

    // ── Identity ────────────────────────────────────────────────────────────
    /// Directory path of the queue.
    dirname: []const u8,

    // ── Configuration ───────────────────────────────────────────────────────
    /// Block size for mmap windows. Must be a power of two. Default: 1 MiB.
    blocksize: u32 = 1024 * 1024,
    /// Queue format version.
    version: QueueVersion = .unset,
    /// Whether this queue has permission to create new files.
    create: bool = false,

    // ── Directory Listing ───────────────────────────────────────────────────
    /// Filename of the directory listing file.
    /// Filename is "metadata.cq4t".
    dirlist_name: ?[]const u8 = null,
    /// File descriptor for the directory listing.
    dirlist_fd: ?std.posix.fd_t = null,
    /// Size of the directory listing file (from fstat).
    dirlist_file_size: u64 = 0,
    /// Memory-mapped buffer for the directory listing.
    dirlist_mmap: ?[]align(std.mem.page_size) u8 = null,
    /// Pointers into the mmap for live-updated fields.
    dirlist_fields: DirlistFields = .{},

    // ── Queue file tracking ─────────────────────────────────────────────────
    /// Glob pattern for finding .cq4 files.
    queuefile_pattern: ?[]const u8 = null,

    // ── Cycle tracking (observed from directory listing) ────────────────────
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

    // ── Tailers ─────────────────────────────────────────────────────────────
    /// All reader tailers attached to this queue.
    tailers: std.ArrayList(*Tailer),
    /// The shared appender tailer (created lazily on first append).
    appender: ?*Tailer = null,

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
        // Free old strings if any
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
        const ts = std.time.milliTimestamp();
        return ts;
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

        // Unmap directory listing
        if (self.dirlist_mmap) |m| {
            std.posix.munmap(m);
        }
        if (self.dirlist_fd) |fd| {
            std.posix.close(fd);
        }

        // Free strings
        if (self.dirlist_name) |n| self.allocator.free(n);
        if (self.queuefile_pattern) |p| self.allocator.free(p);
        if (self.roll_format) |f| self.allocator.free(f);
        if (self.roll_name) |n| self.allocator.free(n);
        if (self.roll_strftime) |s| self.allocator.free(s);
        self.allocator.free(self.dirname);

        self.allocator.destroy(self);
    }
};
```

### Key Differences from C

| C pattern | Zig pattern |
|-----------|-------------|
| `char*` strings with `strdup`/`free` | `[]const u8` slices with `allocator.dupe`/`allocator.free` |
| `NULL` pointers | Optional types (`?[]const u8`, `?*Tailer`) |
| `bzero` + `malloc` | `allocator.create` + struct literal with defaults |
| Global `queue_head` linked list | Not needed — see section 10 below |
| `glob_t` for file discovery | `std.fs.Dir.iterate()` or `std.fs.Dir.openDir` |
| `int fd` (0 is valid) | `std.posix.fd_t` wrapped in optional |
| `struct stat` | `std.posix.Stat` or just store the size field |

---

## 9. Tailer Struct

The `tailer_t` struct tracks the state of a single reader (or the appender) as it
scans through queue files.

### Zig Implementation

```zig
// src/chronicle/tailer.zig (continued)

const std = @import("std");
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
    qf_fd: ?std.posix.fd_t = null,
    /// Cached file size from fstat.
    qf_file_size: u64 = 0,

    // ── Position tracking ───────────────────────────────────────────────────
    /// Byte offset of the next header to read (from start of file).
    qf_tip: u64 = 0,
    /// Sequence-number portion of the next entry at qf_tip.
    qf_index: u64 = 0,

    // ── mmap window ─────────────────────────────────────────────────────────
    /// Currently mapped buffer, or null if not mapped.
    qf_buf: ?[]align(std.mem.page_size) u8 = null,
    /// File offset where the current mmap window starts.
    qf_mmapoff: u64 = 0,
    /// Size of the current mmap window.
    qf_mmapsz: u64 = 0,

    // ── Parent ──────────────────────────────────────────────────────────────
    /// Back-pointer to the owning queue.
    queue: *Queue,

    /// Clean up this tailer's resources (close fd, unmap).
    pub fn deinit(self: *Tailer) void {
        if (self.qf_buf) |buf| {
            std.posix.munmap(buf);
            self.qf_buf = null;
        }
        if (self.qf_fd) |fd| {
            std.posix.close(fd);
            self.qf_fd = null;
        }
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

### Key Differences from C

| C pattern | Zig pattern |
|-----------|-------------|
| Doubly-linked list (`next`/`prev`) | `std.ArrayList(*Tailer)` on the Queue |
| `PROT_READ` / `PROT_READ\|PROT_WRITE` | `MmapProtection` enum |
| `int qf_fd` with 0 = closed | `?std.posix.fd_t` (null = closed) |
| `unsigned char* qf_buf` | `?[]align(page_size) u8` |
| `struct stat qf_statbuf` | Store just `qf_file_size: u64` |

---

## 10. Global Queue Registry

The C code uses a global singly-linked list (`queue_head → queue.next → ...`) to
track all open queues, enabling `chronicle_peek()` to iterate all of them. Zig has
no implicit globals, so this must be handled explicitly.

### Recommended Approach

Use a module-level `QueueRegistry` that the caller manages:

```zig
// src/chronicle/registry.zig

const std = @import("std");
const Queue = @import("queue.zig").Queue;

/// Global registry of all open queues. Replaces the C `queue_head` linked list.
/// The caller is responsible for creating and passing this around.
pub const QueueRegistry = struct {
    queues: std.ArrayList(*Queue),

    pub fn init(allocator: std.mem.Allocator) QueueRegistry {
        return .{
            .queues = std.ArrayList(*Queue).init(allocator),
        };
    }

    pub fn deinit(self: *QueueRegistry) void {
        // Note: does NOT deinit the queues themselves — caller is responsible.
        self.queues.deinit();
    }

    pub fn register(self: *QueueRegistry, queue: *Queue) !void {
        try self.queues.append(queue);
    }

    pub fn unregister(self: *QueueRegistry, queue: *Queue) void {
        for (self.queues.items, 0..) |q, i| {
            if (q == queue) {
                _ = self.queues.swapRemove(i);
                return;
            }
        }
    }

    /// Poll all registered queues (equivalent to C's chronicle_peek()).
    pub fn peekAll(self: *QueueRegistry) void {
        for (self.queues.items) |queue| {
            queue.peek();
        }
    }
};
```

If you prefer the simplicity of a process-global, you can use a `var` at module
scope, but this is discouraged in Zig:

```zig
// Less idiomatic, but simpler for porting:
var global_registry: ?QueueRegistry = null;

pub fn getGlobalRegistry(allocator: std.mem.Allocator) *QueueRegistry {
    if (global_registry == null) {
        global_registry = QueueRegistry.init(allocator);
    }
    return &global_registry.?;
}
```

---

## 11. Error Handling Strategy

The C library uses a global `cerr_msg` string and returns `-1` or `NULL` on error.
Zig has first-class error unions, which are far more expressive and safe.

### Error Set Definition

```zig
// src/chronicle/errors.zig

pub const ChronicleError = error{
    // ── File I/O ────────────────────────────────────────────────────
    DirStatFailed,
    NotADirectory,
    OpenFailed,
    StatFailed,
    MmapFailed,
    SeekFailed,
    WriteFailed,
    RenameFailed,
    GlobFailed,

    // ── Queue state ─────────────────────────────────────────────────
    QueueIsNull,
    VersionDetectFailed,
    VersionMismatch,
    CreateNotPermitted,
    CreateRequiresVersion,
    CreateRequiresEmptyDir,
    CreateRequiresRollScheme,

    // ── Roll / format ───────────────────────────────────────────────
    RollFormatNotRecognized,
    RollFormatMissing,
    RollLengthMissing,
    RollEpochMissing,
    UnrecognizedFormatToken,
    UnterminatedQuote,

    // ── Directory listing ───────────────────────────────────────────
    DirlistParseFailed,
    DirlistFieldsMissing,
    DirlistReopenFailed,

    // ── Data ────────────────────────────────────────────────────────
    MessageTooLarge,
    WriteConflict,

    // ── Memory ──────────────────────────────────────────────────────
    OutOfMemory,
};
```

### Usage Pattern

```zig
pub fn open(self: *Queue) ChronicleError!void {
    // ...
    const stat = std.posix.fstat(fd) catch return ChronicleError.StatFailed;
    // ...
}

// Caller:
queue.open() catch |err| {
    std.log.err("Failed to open queue: {}", .{err});
    return err;
};
```

Functions that can fail return `ChronicleError!ReturnType`. Functions that cannot
fail return the value directly. This replaces the C pattern of checking `-1` and
calling `chronicle_strerror()`.

---

## 12. Behaviour Constants and Defaults

The C implementation has several tuning constants defined as global variables or
inline values. Define these as named constants:

```zig
// src/chronicle/config.zig

/// Number of cycles to look back when patching missing EOF markers.
/// An appender starts seeking from (highest_cycle - patch_cycles).
pub const patch_cycles: u32 = 3;

/// Default initial size for new queue files on disk (bytes).
pub const default_qf_disk_size: u64 = 83_754_496;

/// Default mmap block size (1 MiB). Must be a power of two.
pub const default_blocksize: u32 = 1024 * 1024;

/// Maximum data size constant from the C header.
pub const max_data_size: usize = 1000;

/// Directory listing filename for v5 queues.
pub const v5_dirlist_name = "metadata.cq4t";

/// Queue data file extension.
pub const queue_file_extension = ".cq4";
```

---

## 13. Suggested File Layout

```
src/
└── chronicle/
    ├── header.zig       — Header constants and bit manipulation
    ├── index.zig        — 64-bit index layout (cycle/seqnum)
    ├── roll.zig         — RollScheme, roll_schemes table, format conversion
    ├── tailer.zig       — TailerState enum, Tailer struct, ParseBlockState
    ├── queue.zig        — Queue struct, DirlistFields, QueueVersion
    ├── codec.zig        — Codec interface, Dispatcher, DefaultTextCodec
    ├── config.zig       — Constants and tuning parameters
    ├── errors.zig       — ChronicleError error set
    ├── registry.zig     — QueueRegistry (replaces global linked list)
    └── root.zig         — pub usingnamespace or re-exports
```

---

## 14. Summary Checklist

| Component | C source | Zig module | Status |
|-----------|----------|------------|--------|
| Header constants | `libchronicle.h` | `header.zig` | ☐ |
| TailerState enum | `libchronicle.h` | `tailer.zig` | ☐ |
| ParseBlockState enum | `libchronicle.c:188` | `tailer.zig` | ☐ |
| collected_t | `libchronicle.h` | `tailer.zig` | ☐ |
| Function pointer types | `libchronicle.h` | `codec.zig` | ☐ |
| ROLL_SCHEME + table | `libchronicle.c:436` | `roll.zig` | ☐ |
| Java→strftime conversion | `libchronicle.c:471-535` | `roll.zig` | ☐ |
| queue_t struct | `libchronicle.c:140-185` | `queue.zig` | ☐ |
| tailer_t struct | `libchronicle.c:110-138` | `tailer.zig` | ☐ |
| dirlist_fields_t | `libchronicle.c:104-108` | `queue.zig` | ☐ |
| Global queue list | `libchronicle.c:200` | `registry.zig` | ☐ |
| Error handling | `libchronicle.c:85-98` | `errors.zig` | ☐ |
| Config constants | scattered | `config.zig` | ☐ |
| 64-bit index helpers | `libchronicle.c:170-171` | `index.zig` | ☐ |