# Task 2: Serialization & Codec Interface

## Overview

The ringloom-queue on-disk format uses **fixed-layout binary structures** (`extern struct`) for all internal
metadata — queue headers, directory listings, message headers, and roll metadata (see docs 02 and 03).
These structures are parsed by pointer-casting directly from the mmap'd region: zero deserialization,
zero copies, zero overhead.

**User message payloads** are serialized via a **comptime codec interface**. The codec is the _only_
customization point for message encoding. Internal structures have no codec overhead whatsoever.

This document covers:

1. How message payloads are laid out in the data region
2. The comptime codec interface contract
3. Built-in codecs (raw bytes, text, structured messages)
4. How the codec integrates with the appender and tailer hot paths

---

## 1. Message Layout in Data Region

Each message entry in a queue data file consists of:

```
[4-byte header][N bytes payload][0–3 bytes padding to 4-byte alignment]
```

- **4-byte header** — a packed `extern struct` containing the message size and metadata flags
  (see doc 03 for the header format). Parsed by pointer cast, never by the codec.
- **N bytes payload** — raw bytes produced by `codec.write()`. The codec owns this region entirely.
- **Padding** — zero-filled bytes to bring the next entry to a 4-byte aligned offset. Managed by
  the appender, invisible to the codec.

The codec operates on:

- **Write path**: a `[]u8` slice pointing directly into mmap memory (after the 4-byte header).
- **Read path**: a `[]const u8` slice pointing directly into mmap memory (after the 4-byte header).

Both are **zero-copy** — the codec reads/writes the mmap buffer in place.

---

## 2. The Codec Interface (Comptime Generics)

The codec is a comptime-generic struct parameterized on the user's message type:

```zig
pub fn Codec(comptime MessageType: type) type {
    return struct {
        /// Parse raw bytes from the mmap buffer into a message.
        /// Must NOT allocate. The returned message may reference the input slice
        /// (e.g., returning a sub-slice for []const u8 message types).
        parse: *const fn (buf: []const u8) ?MessageType,

        /// Return the serialized size of a message in bytes.
        /// Must be callable BEFORE write — the appender needs this to compute the
        /// total entry size for the CAS claim on the write offset.
        serialized_size: *const fn (msg: MessageType) usize,

        /// Write a message into the mmap buffer.
        /// Must NOT allocate. Writes exactly `serialized_size(msg)` bytes starting
        /// at buf[0]. The `size` parameter equals `serialized_size(msg)` and is
        /// provided to avoid redundant recomputation.
        write: *const fn (buf: []u8, msg: MessageType, size: usize) void,
    };
}
```

### Key Constraints

| Constraint              | Rationale                                                                 |
|-------------------------|---------------------------------------------------------------------------|
| **Zero allocations**    | `parse` and `write` must NOT heap-allocate. They operate on mmap memory.  |
| **Zero copy on read**   | `parse` may return a value that references the input slice directly.      |
| **Deterministic size**  | `serialized_size` must be callable before `write`, enabling the appender to compute the total entry size for the atomic CAS header claim. |
| **No side effects**     | Codec functions are pure transforms between message values and byte spans.|

### Why Comptime Generics

The codec uses comptime generics (monomorphization) rather than a runtime vtable because:

- The compiler monomorphizes `Appender(MessageType)` and `Tailer(MessageType)` at comptime,
  eliminating all function-pointer indirection.
- Codec functions are eligible for inlining directly into the hot path.
- There is no dynamic dispatch overhead on every message read/write.

This pattern is inspired by Zig's `std.io.Reader` / `std.io.Writer` interface pattern.

---

## 3. Built-in Codecs

### 3a. Raw Bytes Codec

Passes `[]const u8` through unchanged. The simplest possible codec:

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
        fn f(buf: []u8, msg: []const u8, size: usize) void {
            @memcpy(buf[0..size], msg[0..size]);
        }
    }.f,
};
```

Use case: opaque binary blobs, pre-serialized data, or when the caller manages its own encoding.

### 3b. Text Codec

Semantically identical to the raw bytes codec but typed as a text message. Could optionally
validate UTF-8 on parse:

```zig
pub const TextCodec = Codec([]const u8){
    .parse = struct {
        fn f(buf: []const u8) ?[]const u8 {
            // Optional: validate UTF-8
            if (!std.unicode.utf8ValidateSlice(buf)) return null;
            return buf;
        }
    }.f,
    .serialized_size = struct {
        fn f(msg: []const u8) usize {
            return msg.len;
        }
    }.f,
    .write = struct {
        fn f(buf: []u8, msg: []const u8, size: usize) void {
            @memcpy(buf[0..size], msg[0..size]);
        }
    }.f,
};
```

Use case: human-readable log messages, text-based protocols.

### 3c. Structured Message Codec (Example)

For fixed-layout user structs, the codec can use pointer-cast serialization — the same technique
the queue uses for its own internal metadata:

```zig
const Trade = struct {
    price: f64,
    quantity: u32,
    symbol: [8]u8,
};

const trade_codec = Codec(Trade){
    .parse = struct {
        fn f(buf: []const u8) ?Trade {
            if (buf.len < @sizeOf(Trade)) return null;
            return @as(*align(1) const Trade, @ptrCast(buf.ptr)).*;
        }
    }.f,
    .serialized_size = struct {
        fn f(_: Trade) usize {
            return @sizeOf(Trade);
        }
    }.f,
    .write = struct {
        fn f(buf: []u8, msg: Trade, _: usize) void {
            @as(*align(1) Trade, @ptrCast(buf.ptr)).* = msg;
        }
    }.f,
};
```

Notes:

- `@sizeOf(Trade)` is a comptime-known constant, so `serialized_size` compiles to a constant return.
- `align(1)` is required because mmap offsets are only guaranteed to be 4-byte aligned (after the
  header), not necessarily aligned to the struct's natural alignment.
- For cross-platform compatibility, prefer `extern struct` with explicit field layout if the queue
  files will be read by different architectures or compilers.

### 3d. Variable-Length Messages

For variable-length encodings (e.g., protobuf, msgpack, custom TLV), the codec still works. The
`serialized_size` function simply returns the actual encoded length rather than a compile-time
constant:

```zig
const var_codec = Codec(MyMessage){
    .parse = struct {
        fn f(buf: []const u8) ?MyMessage {
            // Decode variable-length fields from buf
            return decodeMyMessage(buf);
        }
    }.f,
    .serialized_size = struct {
        fn f(msg: MyMessage) usize {
            // Compute actual encoded size
            return computeEncodedSize(msg);
        }
    }.f,
    .write = struct {
        fn f(buf: []u8, msg: MyMessage, size: usize) void {
            // Encode into buf[0..size]
            encodeMyMessage(buf[0..size], msg);
        }
    }.f,
};
```

---

## 4. Codec Integration with Hot Path

### 4a. Appender (Write Path)

The appender's `append(msg)` function follows this sequence:

```
1. payload_size = codec.serialized_size(msg)
2. entry_size  = 4 + payload_size + pad_to_4(payload_size)
3. CAS to claim `entry_size` bytes at the current write offset
4. codec.write(mmap_slice[offset+4 ..], msg, payload_size)
5. Publish: write the 4-byte header (atomic store with release semantics)
```

Steps 1 and 4 are the only points where the codec is invoked. Everything else — the CAS claim,
padding computation, header write — is handled by the queue infrastructure with fixed-layout
structures. The codec is never in the contention path (the CAS in step 3 operates on the write
offset, not on codec output).

### 4b. Tailer (Read Path)

The tailer's read loop:

```
1. Read the 4-byte header at the current read offset (atomic load with acquire semantics)
2. If not-ready → spin/wait
3. payload_size = header.data_length()
4. message = codec.parse(mmap_slice[offset+4 .. offset+4+payload_size])
5. Dispatch message to callback / return to caller
6. Advance read offset by 4 + payload_size + pad_to_4(payload_size)
```

Step 4 is the only codec invocation. The header read (step 1) is a pointer cast, not a codec
operation.

### 4c. Performance Characteristics

Both paths are:

- **Zero-copy**: the codec reads/writes mmap memory directly. No intermediate buffers.
- **Zero-allocation**: no heap activity on the hot path.
- **Inlined**: comptime monomorphization means the codec functions are inlined into the appender
  and tailer loops. For a fixed-size struct codec, `serialized_size` compiles down to a constant
  and `write`/`parse` compile down to a single `memcpy` or pointer dereference.

---

## 5. Design Notes

### Fixed-Layout Internals vs. Codec Payloads

The queue has two distinct serialization strategies:

| Component           | Strategy                   | Overhead |
|---------------------|----------------------------|----------|
| Queue metadata      | `extern struct` + ptr cast | Zero     |
| Directory listing   | `extern struct` + ptr cast | Zero     |
| Message headers     | `extern struct` + ptr cast | Zero     |
| Message payloads    | Codec interface            | Codec-dependent (zero for fixed structs) |

The codec boundary is deliberately narrow: it covers only the user's message payload bytes. All
queue-internal structures bypass it entirely.

### Codec Composability

Codecs can be composed. For example, a compression codec could wrap an inner codec:

```zig
fn CompressedCodec(comptime Inner: type, comptime InnerCodec: Codec(Inner)) type {
    // ... compress on write, decompress on parse
}
```

This is left to userspace — the core library provides the interface, not a compression implementation.

---

## 6. Migrating from BinaryWire

The previous architecture used a self-describing BinaryWire serialization protocol for both internal
metadata and user messages. This has been completely removed. In the new architecture:

- **Internal metadata** uses fixed `extern struct` layouts — no serialization overhead.
- **User payloads** use the codec interface described above.

If interop with external systems that expect a self-describing wire format is needed, such a format
can be implemented as a **user-space codec** — it is no longer part of the core library. The codec
interface is flexible enough to support any encoding scheme, including self-describing formats, as
long as the codec functions do not allocate.

---

## 7. Testing Strategy

### Round-Trip Property

For every codec and message type, verify:

```zig
test "codec round-trip" {
    const msg = Trade{ .price = 123.45, .quantity = 100, .symbol = "AAPL\x00\x00\x00\x00".* };
    const size = trade_codec.serialized_size(msg);

    var buf: [256]u8 = undefined;
    trade_codec.write(&buf, msg, size);

    const parsed = trade_codec.parse(buf[0..size]) orelse return error.ParseFailed;
    try std.testing.expectEqual(msg.price, parsed.price);
    try std.testing.expectEqual(msg.quantity, parsed.quantity);
    try std.testing.expectEqualSlices(u8, &msg.symbol, &parsed.symbol);
}
```

### Size Property

Verify that `serialized_size(msg)` matches the number of bytes actually needed:

```zig
test "serialized_size matches write" {
    const msg = Trade{ .price = 99.99, .quantity = 50, .symbol = "MSFT\x00\x00\x00\x00".* };
    const size = trade_codec.serialized_size(msg);
    try std.testing.expectEqual(@sizeOf(Trade), size);
}
```

### Zero-Allocation Verification

Use Zig's `std.testing.allocator` (which tracks allocations) in integration tests to verify that
the appender/tailer hot paths perform zero heap allocations when using a well-behaved codec.

### Edge Cases

- Empty payload (`serialized_size` returns 0)
- Maximum payload size (up to `2^30 - 1` bytes, the header's length field limit)
- Misaligned reads (verify `align(1)` pointer casts work correctly)
- Truncated buffers (`parse` must return `null`, not crash)

---

## 8. Summary Checklist

- [ ] `Codec(comptime MessageType)` generic struct defined
- [ ] `RawCodec` (pass-through `[]const u8`) implemented
- [ ] `TextCodec` (with optional UTF-8 validation) implemented
- [ ] Example structured-message codec documented and tested
- [ ] Appender calls `serialized_size` → CAS → `write` → publish header
- [ ] Tailer calls header read → `parse` → dispatch
- [ ] Round-trip tests for each built-in codec
- [ ] Size-consistency tests for each built-in codec
- [ ] Zero-allocation verification on hot paths
- [ ] No references to BinaryWire, stop-bit encoding, nesting stacks, or self-describing formats in core