# Task 2: BinaryWire Serialization Protocol

## Overview

This document specifies the Zig reimplementation of the **BinaryWire** serialization
protocol used by Chronicle Queue for metadata messages, directory listings, index
structures, and optionally for data payloads. The C implementation is split across
`wire.c` and `wire.h`, with buffer utilities in `buffer.c`/`buffer.h`.

BinaryWire is a binary, self-describing wire format. Every value is preceded by a
control byte that identifies its type. Structures are nested using length-prefixed
containers. Field names and event names are encoded inline, making the format
fully self-describing without an external schema.

---

## 1. Control Byte Map

The first byte of each wire element determines how to decode the following bytes.
Here is the complete table as implemented in `wire.c`:

| Byte Range      | Constant          | Meaning                                         |
|-----------------|-------------------|-------------------------------------------------|
| `0x00 – 0x7F`  | (inline uint8)    | Unsigned integer value 0–127                    |
| `0x82`          | `BYTES_LENGTH32`  | Nested structure: 4-byte LE length, then body   |
| `0x8D`          | `I64_ARRAY`       | Index array: 2×u64 header, then N×u64 entries   |
| `0x8E`          | `PADDING_32`      | 4-byte LE count of additional bytes to skip     |
| `0x8F`          | `PADDING`         | Single padding byte (skip 1 byte total)         |
| `0x90`          | `FLOAT32`         | 4-byte LE IEEE 754 float                        |
| `0xA5`          | `INT16`           | 2-byte LE signed/unsigned 16-bit integer        |
| `0xA6`          | `INT32`           | 4-byte LE signed/unsigned 32-bit integer        |
| `0xA7`          | `INT64`           | 8-byte LE signed/unsigned 64-bit integer        |
| `0xB6`          | `TYPE_PREFIX`     | Type annotation: stop-bit length, then UTF-8    |
| `0xB8`          | `TEXT`            | Text value: stop-bit length, then UTF-8 bytes   |
| `0xB9`          | `EVENT_NAME`      | Event/message name: stop-bit length, then UTF-8 |
| `0xC0 – 0xDF`  | (small field)     | Field name: length = `control - 0xC0`, inline   |
| `0xE0 – 0xFF`  | (small text)      | Text value: length = `control - 0xE0`, inline   |

### Zig Constants

```zig
// src/chronicle/wire.zig

pub const WireCode = struct {
    // Nested structure with 4-byte length prefix
    pub const BYTES_LENGTH32: u8 = 0x82;
    // 64-bit integer array (for index structures)
    pub const I64_ARRAY: u8 = 0x8D;
    // Multi-byte padding (4-byte length + skip)
    pub const PADDING_32: u8 = 0x8E;
    // Single-byte padding
    pub const PADDING: u8 = 0x8F;
    // 32-bit IEEE 754 float
    pub const FLOAT32: u8 = 0x90;
    // 16-bit integer
    pub const INT16: u8 = 0xA5;
    // 32-bit integer
    pub const INT32: u8 = 0xA6;
    // 64-bit integer
    pub const INT64: u8 = 0xA7;
    // Type prefix annotation
    pub const TYPE_PREFIX: u8 = 0xB6;
    // Variable-length text
    pub const TEXT: u8 = 0xB8;
    // Event/message name
    pub const EVENT_NAME: u8 = 0xB9;

    // Small field name range
    pub const SMALL_FIELD_LO: u8 = 0xC0;
    pub const SMALL_FIELD_HI: u8 = 0xDF;
    // Small text value range
    pub const SMALL_TEXT_LO: u8 = 0xE0;
    pub const SMALL_TEXT_HI: u8 = 0xFF;

    /// Maximum length encodable in a small field name (0xDF - 0xC0 = 31).
    pub const SMALL_FIELD_MAX_LEN: u8 = 0x1F;
    /// Maximum length encodable in a small text value (0xFF - 0xE0 = 31).
    pub const SMALL_TEXT_MAX_LEN: u8 = 0x1F;

    /// Returns true if this control byte is an inline uint8 value (0x00–0x7F).
    pub inline fn isInlineUint(control: u8) bool {
        return control <= 0x7F;
    }

    /// Returns true if this control byte starts a small field name.
    pub inline fn isSmallField(control: u8) bool {
        return control >= SMALL_FIELD_LO and control <= SMALL_FIELD_HI;
    }

    /// Returns true if this control byte starts a small text value.
    pub inline fn isSmallText(control: u8) bool {
        return control >= SMALL_TEXT_LO; // always true for 0xE0..0xFF
    }

    /// Extract the inline length from a small field control byte.
    pub inline fn smallFieldLen(control: u8) u8 {
        return control - SMALL_FIELD_LO;
    }

    /// Extract the inline length from a small text control byte.
    pub inline fn smallTextLen(control: u8) u8 {
        return control - SMALL_TEXT_LO;
    }
};
```

---

## 2. Stop-Bit Encoding

Variable-length integers (used for string lengths in `EVENT_NAME`, `TYPE_PREFIX`,
and `TEXT`) use a stop-bit encoding where each byte contributes 7 bits of data and
the high bit indicates whether more bytes follow.

### C Reference

```c
// wire.c:29-38
int read_stop_uint(unsigned char* p, int *stopsz) {
    *stopsz = 0;
    int n = 0;
    do {
        n++;
        *stopsz = (*stopsz << 7) + (p[n-1] & 0x7F);
    } while ((p[n-1] & 0x80) != 0x00);
    return n;
}
```

**Important:** The stop-bit convention in Chronicle Wire is that the **high bit
being set (`0x80`) means "continue reading"**, and the high bit being **clear**
means "this is the last byte". This is the *opposite* of LEB128. Also note the
C code shifts accumulated value left by 7 *before* adding (big-endian style byte
order for the length), not little-endian like standard LEB128.

### Zig Implementation

```zig
// src/chronicle/wire.zig

pub const StopBit = struct {
    /// Read a stop-bit encoded unsigned integer from the buffer.
    /// Returns the decoded value and the number of bytes consumed.
    pub fn readUint(data: []const u8) error{BufferUnderflow}!struct { value: u64, bytes_read: usize } {
        var result: u64 = 0;
        var i: usize = 0;

        while (i < data.len) {
            const b = data[i];
            i += 1;
            result = (result << 7) | @as(u64, b & 0x7F);
            if (b & 0x80 == 0) {
                // High bit clear = last byte
                return .{ .value = result, .bytes_read = i };
            }
        }
        return error.BufferUnderflow;
    }

    /// Write a stop-bit encoded unsigned integer into the buffer.
    /// Returns the number of bytes written.
    pub fn writeUint(buf: []u8, value: u64) error{BufferOverflow}!usize {
        if (value <= 0x7F) {
            if (buf.len < 1) return error.BufferOverflow;
            buf[0] = @truncate(value & 0x7F);
            return 1;
        }

        // Determine how many 7-bit groups we need
        var v = value;
        var n_bytes: usize = 0;
        while (v > 0) : (v >>= 7) {
            n_bytes += 1;
        }

        if (buf.len < n_bytes) return error.BufferOverflow;

        // Write big-endian style: most significant group first
        // All bytes except the last get 0x80 set (continue flag)
        v = value;
        var i: usize = n_bytes;
        while (i > 0) {
            i -= 1;
            buf[i] = @truncate(v & 0x7F);
            if (i < n_bytes - 1) {
                buf[i] |= 0x80; // set continue bit
            }
            v >>= 7;
        }
        return n_bytes;
    }

    /// Return the number of bytes needed to encode the given value.
    pub fn encodedSize(value: u64) usize {
        if (value == 0) return 1;
        var v = value;
        var n: usize = 0;
        while (v > 0) : (v >>= 7) {
            n += 1;
        }
        return n;
    }
};
```

### Tests

```zig
test "stop-bit round-trip" {
    var buf: [16]u8 = undefined;

    // Small value: single byte
    {
        const n = try StopBit.writeUint(&buf, 42);
        try std.testing.expectEqual(@as(usize, 1), n);
        const r = try StopBit.readUint(buf[0..n]);
        try std.testing.expectEqual(@as(u64, 42), r.value);
    }

    // Value requiring two bytes
    {
        const n = try StopBit.writeUint(&buf, 200);
        try std.testing.expectEqual(@as(usize, 2), n);
        const r = try StopBit.readUint(buf[0..n]);
        try std.testing.expectEqual(@as(u64, 200), r.value);
    }

    // Larger value
    {
        const n = try StopBit.writeUint(&buf, 16384);
        const r = try StopBit.readUint(buf[0..n]);
        try std.testing.expectEqual(@as(u64, 16384), r.value);
    }
}
```

---

## 3. Wire Parser (Reader)

The C `wire_parse` function is a stateful, callback-driven parser. It walks the
byte buffer sequentially, decoding control bytes and invoking callbacks for each
discovered element.

### C Callback Structure

```c
typedef struct wirecallbacks {
    void (*event_name)(char*,int,struct wirecallbacks*);
    void (*type_prefix)(char*,int,struct wirecallbacks*);
    void (*field_uint64)(char*,int,uint64_t,struct wirecallbacks*);
    void (*field_char)(char*,int,char*,int,struct wirecallbacks*);
    void (*ptr_uint64)(char*,int,unsigned char*,struct wirecallbacks*);
    void (*ptr_uint64arr)(char*,int,uint64_t,uint64_t,unsigned char*,struct wirecallbacks*);
    void (*reset_nesting)();
    void* userdata;
} wirecallbacks_t;
```

### Zig Approach: Comptime Interface

Instead of C-style function pointers with `void* userdata`, Zig can use a comptime
interface pattern. The parser is generic over a handler type, and the compiler
statically dispatches calls. This eliminates indirection overhead and allows the
handler to carry typed state.

```zig
// src/chronicle/wire_parser.zig

const std = @import("std");
const WireCode = @import("wire.zig").WireCode;
const StopBit = @import("wire.zig").StopBit;

pub const ParseError = error{
    BufferUnderflow,
    UnknownControlByte,
    NestingOverflow,
};

/// A parsed wire element, delivered to the handler.
pub const WireEvent = union(enum) {
    /// An event/message name (0xB9).
    event_name: []const u8,
    /// A type prefix annotation (0xB6).
    type_prefix: []const u8,
    /// A field with a uint64 value (inline 0x00–0x7F, or INT16/INT32/INT64).
    field_uint64: struct {
        field: []const u8,
        value: u64,
    },
    /// A field with a text/string value (small text 0xE0–0xFF, or TEXT 0xB8).
    field_text: struct {
        field: []const u8,
        text: []const u8,
    },
    /// A field with a float32 value (0x90).
    field_float32: struct {
        field: []const u8,
        value: f32,
    },
    /// Direct pointer to a uint64 value in the buffer (for memory-mapped access).
    /// The event_name is the enclosing event, not the field name.
    ptr_uint64: struct {
        event: []const u8,
        ptr: [*]const u8,
    },
    /// Direct pointer to a uint64 array (for index structures).
    ptr_uint64_array: struct {
        field: []const u8,
        used: u64,
        capacity: u64,
        ptr: [*]const u8,
    },
    /// Nesting level increased.
    nest_enter: struct {
        field: []const u8,
    },
    /// Nesting level decreased.
    nest_exit: void,
};
```

### Generic Parser Function

```zig
/// Maximum nesting depth for wire structures.
const MAX_NEST_DEPTH = 10;

/// Parse a BinaryWire buffer and deliver events to the handler.
///
/// The handler must implement:
///   fn onWireEvent(self: *Handler, event: WireEvent) void
///
/// This design replaces the C wirecallbacks_t function-pointer struct.
pub fn wireParse(comptime Handler: type, handler: *Handler, base: []const u8) ParseError!void {
    var p: usize = 0;
    const lim = base.len;

    var field_name: []const u8 = "";
    var event_name: []const u8 = "";

    // Nesting stack: each entry is the byte offset where the current nest ends
    var nest: usize = 0;
    var pop_pos: [MAX_NEST_DEPTH]usize = undefined;
    pop_pos[0] = lim;

    while (p < lim) {
        const control = base[p];
        p += 1;

        if (WireCode.isInlineUint(control)) {
            // 0x00–0x7F: inline uint8 value
            handler.onWireEvent(.{ .field_uint64 = .{
                .field = field_name,
                .value = @as(u64, control),
            } });
        } else if (WireCode.isSmallField(control)) {
            // 0xC0–0xDF: small field name
            const len = WireCode.smallFieldLen(control);
            if (p + len > lim) return ParseError.BufferUnderflow;
            field_name = base[p..][0..len];
            p += len;
            // No event emitted — field_name is consumed by the next value
        } else if (WireCode.isSmallText(control)) {
            // 0xE0–0xFF: small text value
            const len = WireCode.smallTextLen(control);
            if (p + len > lim) return ParseError.BufferUnderflow;
            const text = base[p..][0..len];
            handler.onWireEvent(.{ .field_text = .{
                .field = field_name,
                .text = text,
            } });
            p += len;
        } else switch (control) {
            WireCode.PADDING => {
                // 0x8F: single padding byte — skip
            },
            WireCode.PADDING_32 => {
                // 0x8E: 4-byte length, then skip that many additional bytes
                if (p + 4 > lim) return ParseError.BufferUnderflow;
                const skip = std.mem.readInt(u32, base[p..][0..4], .little);
                p += 4 + skip;
            },
            WireCode.BYTES_LENGTH32 => {
                // 0x82: nested structure with 4-byte length prefix
                if (p + 4 > lim) return ParseError.BufferUnderflow;
                const body_len = std.mem.readInt(u32, base[p..][0..4], .little);
                p += 4;
                handler.onWireEvent(.{ .nest_enter = .{ .field = field_name } });
                nest += 1;
                if (nest >= MAX_NEST_DEPTH) return ParseError.NestingOverflow;
                pop_pos[nest] = p + body_len;
            },
            WireCode.I64_ARRAY => {
                // 0x8D: two u64 headers (capacity, used), then capacity * u64 entries
                if (p + 16 > lim) return ParseError.BufferUnderflow;
                const capacity = std.mem.readInt(u64, base[p..][0..8], .little);
                const used = std.mem.readInt(u64, base[p + 8 ..][0..8], .little);
                p += 16;
                const array_bytes = 8 * capacity;
                if (p + array_bytes > lim) return ParseError.BufferUnderflow;
                handler.onWireEvent(.{ .ptr_uint64_array = .{
                    .field = field_name,
                    .used = used,
                    .capacity = capacity,
                    .ptr = base[p..].ptr,
                } });
                p += @intCast(array_bytes);
            },
            WireCode.FLOAT32 => {
                // 0x90: 4-byte LE float
                if (p + 4 > lim) return ParseError.BufferUnderflow;
                const float_val: f32 = @bitCast(std.mem.readInt(u32, base[p..][0..4], .little));
                handler.onWireEvent(.{ .field_float32 = .{
                    .field = field_name,
                    .value = float_val,
                } });
                p += 4;
            },
            WireCode.INT16 => {
                // 0xA5: 2-byte LE integer
                if (p + 2 > lim) return ParseError.BufferUnderflow;
                const val = std.mem.readInt(u16, base[p..][0..2], .little);
                handler.onWireEvent(.{ .field_uint64 = .{
                    .field = field_name,
                    .value = @as(u64, val),
                } });
                p += 2;
            },
            WireCode.INT32 => {
                // 0xA6: 4-byte LE integer
                if (p + 4 > lim) return ParseError.BufferUnderflow;
                const val = std.mem.readInt(u32, base[p..][0..4], .little);
                handler.onWireEvent(.{ .field_uint64 = .{
                    .field = field_name,
                    .value = @as(u64, val),
                } });
                p += 4;
            },
            WireCode.INT64 => {
                // 0xA7: 8-byte LE integer
                if (p + 8 > lim) return ParseError.BufferUnderflow;
                const val = std.mem.readInt(u64, base[p..][0..8], .little);
                // Emit both ptr_uint64 (for mmap pointer access) and field_uint64
                handler.onWireEvent(.{ .ptr_uint64 = .{
                    .event = event_name,
                    .ptr = base[p..].ptr,
                } });
                handler.onWireEvent(.{ .field_uint64 = .{
                    .field = field_name,
                    .value = val,
                } });
                p += 8;
            },
            WireCode.TYPE_PREFIX => {
                // 0xB6: type prefix string
                const sb = StopBit.readUint(base[p..]) catch return ParseError.BufferUnderflow;
                p += sb.bytes_read;
                const len: usize = @intCast(sb.value);
                if (p + len > lim) return ParseError.BufferUnderflow;
                const name = base[p..][0..len];
                handler.onWireEvent(.{ .type_prefix = name });
                p += len;
            },
            WireCode.TEXT => {
                // 0xB8: variable-length text
                const sb = StopBit.readUint(base[p..]) catch return ParseError.BufferUnderflow;
                p += sb.bytes_read;
                const len: usize = @intCast(sb.value);
                if (p + len > lim) return ParseError.BufferUnderflow;
                const text = base[p..][0..len];
                handler.onWireEvent(.{ .field_text = .{
                    .field = field_name,
                    .text = text,
                } });
                p += len;
            },
            WireCode.EVENT_NAME => {
                // 0xB9: event name
                const sb = StopBit.readUint(base[p..]) catch return ParseError.BufferUnderflow;
                p += sb.bytes_read;
                const len: usize = @intCast(sb.value);
                if (p + len > lim) return ParseError.BufferUnderflow;
                event_name = base[p..][0..len];
                field_name = event_name;
                handler.onWireEvent(.{ .event_name = event_name });
                p += len;
            },
            else => {
                return ParseError.UnknownControlByte;
            },
        }

        // Pop nesting levels when we've consumed all bytes in a nested block
        while (p >= pop_pos[nest] and nest > 0) {
            nest -= 1;
            handler.onWireEvent(.{ .nest_exit = {} });
        }
    }
}
```

### Alternative: Runtime Callback Vtable

If the comptime generic approach is too rigid (e.g., for C interop or plugin
architectures), provide a runtime vtable equivalent:

```zig
/// Runtime callback table, equivalent to the C wirecallbacks_t.
/// Each field is an optional function pointer; null means "ignore this event".
pub const WireCallbacks = struct {
    event_name: ?*const fn (name: []const u8, ctx: *anyopaque) void = null,
    type_prefix: ?*const fn (name: []const u8, ctx: *anyopaque) void = null,
    field_uint64: ?*const fn (field: []const u8, value: u64, ctx: *anyopaque) void = null,
    field_text: ?*const fn (field: []const u8, text: []const u8, ctx: *anyopaque) void = null,
    field_float32: ?*const fn (field: []const u8, value: f32, ctx: *anyopaque) void = null,
    ptr_uint64: ?*const fn (event: []const u8, ptr: [*]const u8, ctx: *anyopaque) void = null,
    ptr_uint64_array: ?*const fn (field: []const u8, used: u64, capacity: u64, ptr: [*]const u8, ctx: *anyopaque) void = null,
    reset_nesting: ?*const fn () void = null,
    userdata: *anyopaque = undefined,
};
```

Then implement a `wireParseRuntime` function that wraps the vtable in a
handler struct and delegates to `wireParse`.

---

## 4. Wire Writer (WirePad)

The C `wirepad_t` is a growable byte buffer that builds BinaryWire-encoded data.
It supports nested structures via a nesting stack, and integrates with the queue's
`qc_start`/`qc_finish` functions to produce complete queue entries (4-byte header
+ wire body).

### C Structure

```c
// wire.c:177-184
struct wirepad {
    int      sz;              // allocated capacity
    unsigned char*    pos;    // current write cursor
    unsigned char*    base;   // base of buffer

    int               nest;             // current nesting depth
    unsigned char*    nest_enter_pos[10]; // saved positions for length patching
};
```

### Zig Implementation

Use an `ArrayList(u8)` or a custom growable buffer. The key insight is that `pos`
in C is just `base + items.len`. The nesting stack saves *byte offsets* (not
pointers), which is safer and works across reallocations.

```zig
// src/chronicle/wire_writer.zig

const std = @import("std");
const Allocator = std.mem.Allocator;
const WireCode = @import("wire.zig").WireCode;
const StopBit = @import("wire.zig").StopBit;
const Header = @import("header.zig").Header;

const MAX_NEST_DEPTH = 10;

pub const WireWriter = struct {
    /// Backing buffer.
    buf: std.ArrayList(u8),

    /// Nesting stack: stores the byte offset where each nested block's
    /// length field begins (to be patched on exit).
    nest_positions: [MAX_NEST_DEPTH]usize = undefined,
    /// Current nesting depth.
    nest: usize = 0,

    pub fn init(allocator: Allocator) WireWriter {
        return .{
            .buf = std.ArrayList(u8).init(allocator),
        };
    }

    pub fn initCapacity(allocator: Allocator, initial_capacity: usize) !WireWriter {
        return .{
            .buf = try std.ArrayList(u8).initCapacity(allocator, initial_capacity),
        };
    }

    pub fn deinit(self: *WireWriter) void {
        self.buf.deinit();
    }

    /// Reset the writer to empty, keeping allocated memory.
    pub fn clear(self: *WireWriter) void {
        self.buf.clearRetainingCapacity();
        self.nest = 0;
    }

    /// Get the current written data as a byte slice.
    pub fn bytes(self: *const WireWriter) []const u8 {
        return self.buf.items;
    }

    /// Get the current position (number of bytes written).
    pub fn pos(self: *const WireWriter) usize {
        return self.buf.items.len;
    }

    // ── Low-level write helpers ─────────────────────────────────────────

    fn appendByte(self: *WireWriter, b: u8) !void {
        try self.buf.append(b);
    }

    fn appendSlice(self: *WireWriter, data: []const u8) !void {
        try self.buf.appendSlice(data);
    }

    fn appendU16LE(self: *WireWriter, v: u16) !void {
        var tmp: [2]u8 = undefined;
        std.mem.writeInt(u16, &tmp, v, .little);
        try self.appendSlice(&tmp);
    }

    fn appendU32LE(self: *WireWriter, v: u32) !void {
        var tmp: [4]u8 = undefined;
        std.mem.writeInt(u32, &tmp, v, .little);
        try self.appendSlice(&tmp);
    }

    fn appendU64LE(self: *WireWriter, v: u64) !void {
        var tmp: [8]u8 = undefined;
        std.mem.writeInt(u64, &tmp, v, .little);
        try self.appendSlice(&tmp);
    }

    fn appendF32LE(self: *WireWriter, v: f32) !void {
        const bits: u32 = @bitCast(v);
        try self.appendU32LE(bits);
    }

    /// Patch a u32 value at a previously recorded byte offset.
    fn patchU32LE(self: *WireWriter, offset: usize, v: u32) void {
        std.mem.writeInt(u32, self.buf.items[offset..][0..4], v, .little);
    }

    // ── Text and field name encoding ────────────────────────────────────

    /// Write a text value using the most compact encoding.
    /// If length <= 31, uses the small text encoding (0xE0 + len).
    /// Otherwise uses the 0xB8 variable-length text encoding.
    ///
    /// C equivalent: wirepad_text()
    pub fn text(self: *WireWriter, value: []const u8) !void {
        if (value.len <= WireCode.SMALL_TEXT_MAX_LEN) {
            try self.appendByte(WireCode.SMALL_TEXT_LO + @as(u8, @intCast(value.len)));
            try self.appendSlice(value);
        } else {
            try self.appendByte(WireCode.TEXT);
            // Stop-bit encoded length
            var len_buf: [10]u8 = undefined;
            const n = try StopBit.writeUint(&len_buf, value.len);
            try self.appendSlice(len_buf[0..n]);
            try self.appendSlice(value);
        }
    }

    /// Write a field name using the most compact encoding.
    /// If length <= 31, uses the small field encoding (0xC0 + len).
    /// Longer field names are not currently supported (the C code aborts).
    ///
    /// C equivalent: wirepad_field()
    pub fn field(self: *WireWriter, name: []const u8) !void {
        if (name.len > WireCode.SMALL_FIELD_MAX_LEN) {
            return error.FieldNameTooLong;
        }
        try self.appendByte(WireCode.SMALL_FIELD_LO + @as(u8, @intCast(name.len)));
        try self.appendSlice(name);
    }

    // ── Value encoding ──────────────────────────────────────────────────

    /// Write a uint64 value with 8-byte alignment.
    /// Inserts padding bytes (0x8F or 0x8E) so that the 8 data bytes
    /// fall on an 8-byte aligned offset within the buffer.
    ///
    /// This is critical for directory-listing fields (highestCycle, lowestCycle,
    /// modCount) which are memory-mapped and accessed atomically.
    ///
    /// C equivalent: wirepad_uint64_aligned()
    pub fn uint64Aligned(self: *WireWriter, v: u64) !void {
        // The INT64 prefix byte (0xA7) should land at offset (8N - 1) so that
        // the 8 data bytes start at offset 8N. In other words, we need
        // (current_pos + 1) to be aligned to 8.
        const needed_padding = (8 - ((self.pos() + 1) % 8)) % 8;

        if (needed_padding > 0) {
            if (needed_padding < 5) {
                // Use individual 0x8F padding bytes
                for (0..needed_padding) |_| {
                    try self.appendByte(WireCode.PADDING);
                }
            } else {
                // Use 0x8E + 4-byte count of extra bytes to skip
                try self.appendByte(WireCode.PADDING_32);
                try self.appendU32LE(@intCast(needed_padding - 5));
                // Zero-fill remaining padding bytes
                for (0..needed_padding - 5) |_| {
                    try self.appendByte(0x00);
                }
            }
        }

        try self.appendByte(WireCode.INT64);
        try self.appendU64LE(v);
    }

    /// Write a varint value using the smallest encoding that fits.
    /// Values 0–127 use inline encoding (1 byte).
    /// Values up to 0xFFFF use INT16 (3 bytes).
    /// Values up to 0xFFFFFFFF use INT32 (5 bytes).
    /// Larger values use INT64 (9 bytes).
    ///
    /// C equivalent: wirepad_varint()
    pub fn varint(self: *WireWriter, v: u64) !void {
        if (v <= 0x7F) {
            try self.appendByte(@truncate(v));
        } else if (v <= 0xFFFF) {
            try self.appendByte(WireCode.INT16);
            try self.appendU16LE(@truncate(v));
        } else if (v <= 0xFFFFFFFF) {
            try self.appendByte(WireCode.INT32);
            try self.appendU32LE(@truncate(v));
        } else {
            try self.appendByte(WireCode.INT64);
            try self.appendU64LE(v);
        }
    }

    // ── Combined field + value helpers ──────────────────────────────────

    /// Write a field name followed by a text value.
    /// C equivalent: wirepad_field_text()
    pub fn fieldText(self: *WireWriter, name: []const u8, value: []const u8) !void {
        try self.field(name);
        try self.text(value);
    }

    /// Write a field name followed by a text value (enum name).
    /// C equivalent: wirepad_field_enum()
    pub fn fieldEnum(self: *WireWriter, name: []const u8, value: []const u8) !void {
        try self.field(name);
        try self.text(value);
    }

    /// Write a field name followed by a type prefix and text value.
    /// C equivalent: wirepad_field_type_enum()
    pub fn fieldTypeEnum(self: *WireWriter, name: []const u8, type_name: []const u8, value: []const u8) !void {
        try self.field(name);
        try self.typePrefix(type_name);
        try self.text(value);
    }

    /// Write a field name followed by an aligned uint64 value.
    /// C equivalent: wirepad_field_uint64()
    pub fn fieldUint64(self: *WireWriter, name: []const u8, v: u64) !void {
        try self.field(name);
        try self.uint64Aligned(v);
    }

    /// Write a field name followed by a float value.
    /// If the value fits in float32 without loss, uses FLOAT32 encoding.
    /// C equivalent: wirepad_field_float64()
    pub fn fieldFloat64(self: *WireWriter, name: []const u8, v: f64) !void {
        try self.field(name);
        const f: f32 = @floatCast(v);
        if (@as(f64, f) == v) {
            // Fits in float32
            try self.appendByte(WireCode.FLOAT32);
            try self.appendF32LE(f);
        } else {
            // Would need float64 encoding — not implemented in C either
            // For now, use float32 with precision loss, matching C behavior
            try self.appendByte(WireCode.FLOAT32);
            try self.appendF32LE(f);
        }
    }

    /// Write a field name followed by a varint value.
    /// C equivalent: wirepad_field_varint()
    pub fn fieldVarint(self: *WireWriter, name: []const u8, v: u64) !void {
        try self.field(name);
        try self.varint(v);
    }

    // ── Event and type annotations ──────────────────────────────────────

    /// Write an event name marker (0xB9 + stop-bit length + UTF-8 name).
    /// C equivalent: wirepad_event_name()
    pub fn eventName(self: *WireWriter, name: []const u8) !void {
        try self.appendByte(WireCode.EVENT_NAME);
        // The C code uses a single-byte length (name.len & 0xFF).
        // For names up to 255 bytes, one byte of stop-bit is sufficient.
        try self.appendByte(@truncate(name.len & 0xFF));
        try self.appendSlice(name);
    }

    /// Write a type prefix annotation (0xB6 + stop-bit length + UTF-8 name).
    /// C equivalent: wirepad_type_prefix()
    pub fn typePrefix(self: *WireWriter, name: []const u8) !void {
        try self.appendByte(WireCode.TYPE_PREFIX);
        try self.appendByte(@truncate(name.len & 0xFF));
        try self.appendSlice(name);
    }

    // ── Padding ─────────────────────────────────────────────────────────

    /// Pad the buffer to the next 8-byte boundary using 0x8F (PADDING) bytes.
    /// C equivalent: wirepad_pad_to_x8()
    pub fn padToX8(self: *WireWriter) !void {
        const padding = (8 - (self.pos() % 8)) % 8;
        for (0..padding) |_| {
            try self.appendByte(WireCode.PADDING);
        }
    }

    /// Pad the buffer to the next 8-byte boundary using 0x00 bytes.
    /// C equivalent: wirepad_pad_to_x8_00()
    pub fn padToX8Zero(self: *WireWriter) !void {
        const padding = (8 - (self.pos() % 8)) % 8;
        for (0..padding) |_| {
            try self.appendByte(0x00);
        }
    }

    // ── Queue-entry framing (qc_start / qc_finish) ─────────────────────

    /// Begin a queue chronicle entry. Writes a 4-byte header placeholder
    /// with the WORKING bit set (and optionally the METADATA bit).
    /// The header length will be patched by `qcFinish()`.
    ///
    /// C equivalent: wirepad_qc_start()
    pub fn qcStart(self: *WireWriter, metadata: bool) !void {
        const header_val: u32 = Header.WORKING | (if (metadata) Header.METADATA else 0);
        self.nest_positions[self.nest] = self.pos();
        self.nest += 1;
        try self.appendU32LE(header_val);
    }

    /// Finish a queue chronicle entry. Patches the 4-byte header written by
    /// `qcStart()` with the actual body length, clearing the WORKING bit.
    ///
    /// C equivalent: wirepad_qc_finish()
    pub fn qcFinish(self: *WireWriter) void {
        self.nest -= 1;
        const entered_at = self.nest_positions[self.nest];
        const body_len: u32 = @intCast(self.pos() - entered_at - 4);

        // Read existing header to preserve METADATA bit
        var existing = std.mem.readInt(u32, self.buf.items[entered_at..][0..4], .little);
        existing = (existing & ~Header.WORKING) | body_len;
        self.patchU32LE(entered_at, existing);
    }

    // ── Nesting (0x82 BYTES_LENGTH32) ───────────────────────────────────

    /// Enter a nested structure. Writes 0x82 + a 4-byte placeholder length.
    /// Call `nestExit()` to close and patch the length.
    ///
    /// C equivalent: wirepad_nest_enter()
    pub fn nestEnter(self: *WireWriter) !void {
        try self.appendByte(WireCode.BYTES_LENGTH32);
        self.nest_positions[self.nest] = self.pos();
        self.nest += 1;
        try self.appendU32LE(0); // placeholder
    }

    /// Exit a nested structure. Patches the length written by the matching
    /// `nestEnter()`.
    ///
    /// C equivalent: wirepad_nest_exit()
    pub fn nestExit(self: *WireWriter) void {
        self.nest -= 1;
        const entered_at = self.nest_positions[self.nest];
        const body_len: u32 = @intCast(self.pos() - entered_at - 4);
        self.patchU32LE(entered_at, body_len);
    }

    // ── Integration helpers ─────────────────────────────────────────────

    /// Return the number of bytes that would be written if this writer's
    /// content were copied out (equivalent to wirepad_sizeof).
    pub fn encodedSize(self: *const WireWriter) usize {
        return self.buf.items.len;
    }

    /// Copy this writer's content into a destination buffer.
    /// Equivalent to wirepad_write().
    pub fn writeTo(self: *const WireWriter, dest: []u8) void {
        @memcpy(dest[0..self.buf.items.len], self.buf.items);
    }

    /// Run the wire parser over the content of this writer.
    /// Useful for testing / validation.
    pub fn parse(self: *const WireWriter, comptime Handler: type, handler: *Handler) !void {
        const wire_parser = @import("wire_parser.zig");
        try wire_parser.wireParse(Handler, handler, self.bytes());
    }
};
```

---

## 5. Nesting Stack Management

Both the parser and writer use a nesting stack to track hierarchical structure
boundaries.

### Parser Nesting

The parser maintains a `pop_pos` array of byte offsets. When entering a
`BYTES_LENGTH32` block, it pushes `current_pos + body_length` onto the stack.
After processing each element, it checks whether the current position has reached
or passed the top of the stack, and pops automatically.

Key invariant: `pop_pos[0]` is always set to the total buffer limit, so the
outermost level "pops" naturally at the end of the buffer.

### Writer Nesting

The writer maintains a `nest_positions` array of byte offsets where length fields
were written. On `nestEnter()`, it saves the offset of the 4-byte length placeholder.
On `nestExit()`, it calculates `current_pos - saved_pos - 4` and patches the length.

The `qcStart()`/`qcFinish()` pair uses the same stack but operates on the 4-byte
queue entry header rather than a `BYTES_LENGTH32` prefix. The `WORKING` bit is
cleared and the length is OR'd in during `qcFinish()`.

### Depth Limit

Both C and Zig implementations use a fixed nesting depth of 10. Chronicle Queue
metadata structures rarely exceed 4–5 levels, so this is safe. Consider making
it configurable or using an `ArrayList(usize)` if deeper nesting is ever needed.

---

## 6. Building a Directory Listing

The most complex use of the wire writer is constructing the `metadata.cq4t`
file. This demonstrates the full framing and nesting API.

### Structure

The directory listing consists of:
1. One **metadata** message containing the roll configuration
2. Six **data** messages containing live-updated fields

### Zig Implementation

```zig
/// Build the initial content of a directory listing file.
/// Returns a WireWriter containing the complete file content.
///
/// C equivalent: directory_listing_init() in libchronicle.c:1400-1476
pub fn buildDirectoryListing(
    allocator: Allocator,
    roll_length_ms: u64,
    roll_format: []const u8,
    roll_epoch: i64,
    initial_cycle: u64,
) !WireWriter {
    var w = try WireWriter.initCapacity(allocator, 1024);
    errdefer w.deinit();

    const epoch_val: u64 = if (roll_epoch == -1) 0 else @intCast(roll_epoch);

    // ── Metadata message: roll configuration ────────────────────────────
    try w.qcStart(true); // metadata = true
    {
        try w.eventName("header");
        try w.typePrefix("STStore");
        try w.nestEnter(); // header body
        {
            try w.fieldTypeEnum("wireType", "WireType", "BINARY_LIGHT");

            try w.field("metadata");
            try w.typePrefix("SCQMeta");
            try w.nestEnter(); // metadata body
            {
                try w.field("roll");
                try w.typePrefix("SCQSRoll");
                try w.nestEnter(); // roll body
                {
                    try w.fieldVarint("length", roll_length_ms);
                    try w.fieldText("format", roll_format);
                    try w.fieldVarint("epoch", epoch_val);
                }
                w.nestExit(); // roll

                try w.fieldVarint("deltaCheckpointInterval", 64);
                try w.fieldVarint("sourceId", 0);
            }
            w.nestExit(); // metadata

            try w.padToX8();
        }
        w.nestExit(); // header
    }
    w.qcFinish();

    // ── Data messages: live fields ──────────────────────────────────────

    // listing.highestCycle
    try w.qcStart(false);
    try w.eventName("listing.highestCycle");
    try w.uint64Aligned(initial_cycle);
    w.qcFinish();

    // listing.lowestCycle
    try w.qcStart(false);
    try w.eventName("listing.lowestCycle");
    try w.uint64Aligned(initial_cycle);
    w.qcFinish();

    // listing.modCount
    try w.qcStart(false);
    try w.eventName("listing.modCount");
    try w.uint64Aligned(1); // initial modcount = 1
    w.qcFinish();

    // chronicle.write.lock
    try w.qcStart(false);
    try w.eventName("chronicle.write.lock");
    try w.uint64Aligned(0x8000000000000000);
    w.qcFinish();

    // chronicle.lastIndexReplicated
    try w.qcStart(false);
    try w.eventName("chronicle.lastIndexReplicated");
    try w.uint64Aligned(@as(u64, @bitCast(@as(i64, -1))));
    w.qcFinish();

    // chronicle.lastAcknowledgedIndexReplicated
    try w.qcStart(false);
    try w.eventName("chronicle.lastAcknowledgedIndexReplicated");
    try w.uint64Aligned(@as(u64, @bitCast(@as(i64, -1))));
    w.qcFinish();

    return w;
}
```

---

## 7. Text-Only Parser Helper

The C function `wire_parse_textonly` is a `cparse_f` implementation that parses
a wire buffer expecting a single text field and returns a heap-allocated copy of
the text. This is the default decoder for data messages.

### Zig Implementation

```zig
/// Parse a wire buffer and extract the first text field value.
/// Returns a heap-allocated copy of the text, or null if no text field was found.
///
/// C equivalent: wire_parse_textonly()
pub fn parseTextOnly(allocator: Allocator, data: []const u8) !?[]u8 {
    const TextHandler = struct {
        result: ?[]u8 = null,
        alloc: Allocator,

        pub fn onWireEvent(self: *@This(), event: WireEvent) void {
            switch (event) {
                .field_text => |ft| {
                    // Free previous result if multiple text fields
                    if (self.result) |prev| self.alloc.free(prev);
                    self.result = self.alloc.dupe(u8, ft.text) catch null;
                },
                else => {},
            }
        }
    };

    var handler = TextHandler{ .alloc = allocator };
    const wire_parser = @import("wire_parser.zig");
    try wire_parser.wireParse(TextHandler, &handler, data);
    return handler.result;
}
```

---

## 8. WireWriter as a Codec

The C code provides `wirepad_sizeof` and `wirepad_write` so that a `wirepad_t*`
can be used directly as the message object for `chronicle_append`. In Zig, this
is modeled as a codec implementation:

```zig
/// Codec that treats a WireWriter as the message type.
/// Use this to append pre-built wire structures to a queue.
pub const WireWriterCodec = struct {
    pub const Message = *const WireWriter;

    pub fn parse(data: []const u8) ?Message {
        _ = data;
        // WireWriter messages are write-only; parsing returns null.
        return null;
    }

    pub fn encodedSize(msg: Message) usize {
        return msg.encodedSize();
    }

    pub fn write(buf: []u8, msg: Message) void {
        msg.writeTo(buf);
    }
};
```

---

## 9. Buffer Formatting Utilities

The C `buffer.c` provides `printbuf` and `formatbuf` for debugging. These are
straightforward to reimplement in Zig.

### Zig Implementation

```zig
// src/chronicle/buffer.zig

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Format a byte buffer as a hex/ASCII dump suitable for debug output.
/// Produces lines like:
///   00000000 c7 6d 65 73 73 61 67 65  eb 48 65 6c 6c 6f 20 57 ·message ·Hello W
///
/// Caller owns the returned slice and must free it.
///
/// C equivalent: formatbuf()
pub fn formatBuf(allocator: Allocator, data: []const u8) ![]u8 {
    const hex_digits = "0123456789abcdef";

    const lines = if (data.len == 0) 1 else (data.len + 15) / 16;
    const line_width = 76; // 8 (offset) + 1 (space) + 48 (hex) + 1 (gap) + 17 (ascii) + 1 (newline)
    const total_size = lines * line_width + 1; // +1 for trailing null or safety

    var result = try allocator.alloc(u8, total_size);
    @memset(result, ' ');

    for (0..lines) |i| {
        const line_start = i * line_width;

        // Write 8-character hex offset
        var offset: u32 = @intCast(i * 16);
        var k: usize = 8;
        while (k > 0) {
            k -= 1;
            result[line_start + k] = hex_digits[offset & 0xF];
            offset >>= 4;
        }

        // Hex and ASCII columns
        for (0..16) |j| {
            const byte_idx = i * 16 + j;
            if (byte_idx >= data.len) break;

            const b = data[byte_idx];
            const gap: usize = if (j > 7) 1 else 0;
            const hex_pos = line_start + 9 + j * 3 + gap;
            const ascii_pos = line_start + 9 + 49 + j + gap;

            result[hex_pos] = hex_digits[(b >> 4) & 0x0F];
            result[hex_pos + 1] = hex_digits[b & 0x0F];

            result[ascii_pos] = if (b >= 32 and b < 127) b else '.';
        }

        // Newline at end of each line
        result[line_start + line_width - 1] = '\n';
    }

    return result[0 .. lines * line_width];
}

/// Print a byte buffer as an escaped C string to stderr (for debugging).
///
/// C equivalent: printbuf()
pub fn printBuf(data: []const u8) void {
    const stderr = std.io.getStdErr().writer();
    stderr.print("unsigned char* buf=\"", .{}) catch return;
    for (data) |c| {
        switch (c) {
            '\n' => stderr.writeAll("\\n") catch return,
            '\r' => stderr.writeAll("\\r") catch return,
            '\t' => stderr.writeAll("\\t") catch return,
            '\\' => stderr.writeAll("\\\\") catch return,
            else => {
                if (c < 0x20 or c > 0x7f) {
                    stderr.print("\\{o:0>3}", .{c}) catch return;
                } else {
                    stderr.writeByte(c) catch return;
                }
            },
        }
    }
    stderr.writeAll("\"\n") catch return;
}
```

---

## 10. Parsing Directory Listing and Queue File Headers

The wire parser is used in several integration points with libchronicle. These are
callback-based in C; in Zig, each becomes a specific handler struct.

### Directory Listing Parser

When the queue opens, it parses the directory listing to extract:
- `listing.highestCycle`, `listing.lowestCycle`, `listing.modCount` — as **pointers**
  into the mmap buffer (for live atomic reads)
- `length`, `epoch`, `format` — roll configuration values

```zig
/// Handler for parsing the directory listing wire data.
/// Extracts both mmap pointers (for live fields) and roll config values.
///
/// C equivalent: handle_dirlist_ptr, handle_dirlist_uint64, handle_dirlist_text
pub const DirlistParseHandler = struct {
    /// Output: mmap pointers for live fields
    highest_cycle_ptr: ?[*]const u8 = null,
    lowest_cycle_ptr: ?[*]const u8 = null,
    modcount_ptr: ?[*]const u8 = null,

    /// Output: roll configuration
    roll_length: ?u64 = null,
    roll_epoch: ?u64 = null,
    roll_format: ?[]const u8 = null,

    pub fn onWireEvent(self: *DirlistParseHandler, event: WireEvent) void {
        switch (event) {
            .ptr_uint64 => |p| {
                if (std.mem.eql(u8, p.event, "listing.highestCycle")) {
                    self.highest_cycle_ptr = p.ptr;
                } else if (std.mem.eql(u8, p.event, "listing.lowestCycle")) {
                    self.lowest_cycle_ptr = p.ptr;
                } else if (std.mem.eql(u8, p.event, "listing.modCount")) {
                    self.modcount_ptr = p.ptr;
                }
            },
            .field_uint64 => |f| {
                if (std.mem.eql(u8, f.field, "length")) {
                    self.roll_length = f.value;
                } else if (std.mem.eql(u8, f.field, "epoch")) {
                    self.roll_epoch = f.value;
                }
            },
            .field_text => |f| {
                if (std.mem.eql(u8, f.field, "format")) {
                    self.roll_format = f.text; // Points into mmap — no allocation
                }
            },
            else => {},
        }
    }
};
```

---

## 11. Integration with parse_queue_block

The wire parser is called from within `parse_queue_block` in two contexts:

1. **Metadata entries**: When the block parser encounters a header with the
   `HD_METADATA` bit set, it calls `wire_parse(base+4, sz, &hcbs)` to extract
   metadata fields (index2index pointers, index pages, roll config).

2. **Data entries**: When the block parser encounters a data header, it calls
   `parse_data_cb` which may in turn use `wire_parse` if the data format is
   BinaryWire (rather than raw bytes).

In the Zig design, `parse_queue_block` accepts a comptime handler type for metadata
and a function pointer (or comptime handler) for data:

```zig
/// Signature for the data callback used by parseQueueBlock.
/// Returns the next parse state — either .awaiting_entry to continue,
/// or .collected to signal a synchronous read completed.
pub const DataCallback = *const fn (
    data: []const u8,
    index: u64,
    userdata: *anyopaque,
) ParseBlockState;

/// Scan a memory-mapped block of a queue file, decoding 4-byte headers
/// and dispatching metadata to the wire parser and data to a callback.
///
/// Updates `tip` and `index` in place as entries are consumed.
///
/// C equivalent: parse_queue_block()
pub fn parseQueueBlock(
    comptime MetaHandler: type,
    meta_handler: *MetaHandler,
    data_callback: ?DataCallback,
    data_userdata: ?*anyopaque,
    buf: []const u8,
    tip: *usize,
    index: *u64,
    version: u8,
) ParseBlockState {
    var base = tip.*;
    var idx = index.*;
    var result: ParseBlockState = .awaiting_entry;

    while (result == .awaiting_entry) {
        // Need at least 4 bytes for the header
        if (base + 4 > buf.len) return .need_extend;

        const header = std.mem.readInt(u32, buf[base..][0..4], .little);

        // Memory fence: no speculative reads before the header is resolved
        @fence(.seq_cst);

        if (header == Header.UNALLOCATED) {
            return .awaiting_entry;
        } else if (Header.metaType(header) == Header.WORKING) {
            return .busy;
        } else if (Header.metaType(header) == Header.METADATA) {
            const sz: usize = @intCast(Header.dataLength(header));
            if (base + 4 + sz > buf.len) return .need_extend;
            // Parse metadata with wire parser
            const wire_parser = @import("wire_parser.zig");
            wire_parser.wireParse(MetaHandler, meta_handler, buf[base + 4 ..][0..sz]) catch {};
        } else if (Header.metaType(header) == Header.EOF) {
            return .reached_eof;
        } else {
            // Data entry
            const sz: usize = @intCast(Header.dataLength(header));
            if (data_callback) |cb| {
                if (base + 4 + sz > buf.len) return .need_extend;
                result = cb(buf[base + 4 ..][0..sz], idx, data_userdata.?);
            } else {
                return .null_item;
            }
            idx += 1;
            index.* = idx;
        }

        // Advance past this entry, with v5 4-byte alignment padding
        const sz: usize = @intCast(Header.dataLength(header));
        const pad4: usize = if (version < 5) 0 else ((4 -% sz) & 0x03);
        base = base + 4 + sz + pad4;
        tip.* = base;
    }

    return result;
}
```

---

## 12. Error Handling in Wire Operations

### Parser Errors

The wire parser can encounter:
- `BufferUnderflow` — not enough bytes for the declared structure
- `UnknownControlByte` — unrecognized control byte
- `NestingOverflow` — exceeded maximum nesting depth

In the C code, unknown control bytes print a message and skip to the end of the
buffer. In Zig, we return a proper error. Callers that need C-like resilience can
`catch` the error and log it.

### Writer Errors

The wire writer can fail with:
- `OutOfMemory` — the backing `ArrayList` could not grow
- `FieldNameTooLong` — a field name exceeds 31 bytes (small field limit)
- `BufferOverflow` — for stop-bit encoding into a fixed buffer

Since the writer uses `ArrayList(u8)`, most operations propagate `Allocator.Error`
(which is `error{OutOfMemory}`). The `!void` return type on all writer methods
carries this naturally.

---

## 13. Suggested File Layout

```
src/
└── chronicle/
    ├── wire.zig            — WireCode constants, StopBit encoding
    ├── wire_parser.zig     — wireParse() generic parser, WireEvent union
    ├── wire_writer.zig     — WireWriter struct (replaces wirepad_t)
    ├── wire_handlers.zig   — DirlistParseHandler, QueueFileHeaderHandler
    ├── wire_helpers.zig    — parseTextOnly(), WireWriterCodec
    └── buffer.zig          — formatBuf(), printBuf() debug utilities
```

---

## 14. Testing Strategy

### Unit Tests for Stop-Bit Encoding

Round-trip tests for boundary values: 0, 127, 128, 255, 256, 16383, 16384,
`maxInt(u64)`.

### Unit Tests for WireWriter

Build known structures and compare byte output against the C implementation's
output. Use `printbuf`-style golden byte strings from C test fixtures.

### Round-Trip Tests

Write a structure with `WireWriter`, then parse it with `wireParse`, and verify
the handler receives the expected events in the expected order.

### Integration Tests

Build a complete directory listing with `buildDirectoryListing`, parse it back
with `DirlistParseHandler`, and verify that all roll config values and mmap
pointer offsets are correct.

### Fuzz Tests

Feed random bytes to `wireParse` and ensure it either succeeds or returns a
clean `ParseError` — never panics or accesses out-of-bounds memory. Zig's
built-in fuzz testing (`std.testing.fuzz`) is well-suited for this.

---

## 15. Summary Checklist

| Component | C source | Zig module | Status |
|-----------|----------|------------|--------|
| WireCode constants | `wire.c:41–175` cases | `wire.zig` | ☐ |
| Stop-bit encoding | `wire.c:29–38` | `wire.zig` | ☐ |
| Wire parser (generic) | `wire.c:41–175` | `wire_parser.zig` | ☐ |
| WireEvent tagged union | `wire.h` callbacks | `wire_parser.zig` | ☐ |
| WireCallbacks (runtime) | `wire.h` wirecallbacks_t | `wire_parser.zig` | ☐ |
| WireWriter struct | `wire.c:177–184` | `wire_writer.zig` | ☐ |
| WireWriter.text | `wire.c:218–233` | `wire_writer.zig` | ☐ |
| WireWriter.field | `wire.c:235–248` | `wire_writer.zig` | ☐ |
| WireWriter.uint64Aligned | `wire.c:250–278` | `wire_writer.zig` | ☐ |
| WireWriter.varint | `wire.c:280–308` | `wire_writer.zig` | ☐ |
| WireWriter.fieldText | `wire.c:310–313` | `wire_writer.zig` | ☐ |
| WireWriter.fieldEnum | `wire.c:315–318` | `wire_writer.zig` | ☐ |
| WireWriter.fieldTypeEnum | `wire.c:320–324` | `wire_writer.zig` | ☐ |
| WireWriter.fieldUint64 | `wire.c:326–329` | `wire_writer.zig` | ☐ |
| WireWriter.fieldFloat64 | `wire.c:331–343` | `wire_writer.zig` | ☐ |
| WireWriter.fieldVarint | `wire.c:345–348` | `wire_writer.zig` | ☐ |
| WireWriter.padToX8 | `wire.c:350–356` | `wire_writer.zig` | ☐ |
| WireWriter.eventName | `wire.c:366–377` | `wire_writer.zig` | ☐ |
| WireWriter.typePrefix | `wire.c:379–390` | `wire_writer.zig` | ☐ |
| WireWriter.qcStart | `wire.c:395–402` | `wire_writer.zig` | ☐ |
| WireWriter.qcFinish | `wire.c:404–416` | `wire_writer.zig` | ☐ |
| WireWriter.nestEnter | `wire.c:418–426` | `wire_writer.zig` | ☐ |
| WireWriter.nestExit | `wire.c:428–436` | `wire_writer.zig` | ☐ |
| parseTextOnly | `wire.c:478–495` | `wire_helpers.zig` | ☐ |
| WireWriterCodec | `wire.c:466–474` | `wire_helpers.zig` | ☐ |
| DirlistParseHandler | `libchronicle.c:691–722` | `wire_handlers.zig` | ☐ |
| QueueFileHeaderHandler | `libchronicle.c:724–746` | `wire_handlers.zig` | ☐ |
| buildDirectoryListing | `libchronicle.c:1400–1476` | `wire_writer.zig` | ☐ |
| formatBuf | `buffer.c:55–89` | `buffer.zig` | ☐ |
| printBuf | `buffer.c:24–51` | `buffer.zig` | ☐ |
| parseQueueBlock (wire integration) | `libchronicle.c:605–651` | `wire_parser.zig` | ☐ |