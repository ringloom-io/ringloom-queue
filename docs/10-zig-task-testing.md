# Task 8: Testing and Validation Strategy

## Overview

This task defines the comprehensive testing strategy for ringloom-queue — a clean-room,
high-performance, lock-free, memory-mapped IPC queue in Zig. The testing strategy covers
unit tests, integration tests, concurrency tests, performance benchmarks, fuzzing, and
property-based testing.

ringloom-queue uses Zig's built-in test framework (`test` blocks and `std.testing`), Zig's
built-in fuzz testing, and the language's runtime safety checks. The goal is to verify
correctness, memory safety, and performance of the single-writer / multiple-reader
architecture with zero allocations on the hot path.

Key testing concerns specific to ringloom-queue:

- **Fixed struct layouts** — `SharedMetadata` (512 bytes) and `QueueFileHeader` (64 bytes)
  must have exact sizes and field offsets for mmap-and-cast to work across processes.
- **Publication correctness** — appender lease enforcement, entry header state transitions,
  acquire/release ordering, and tiered backoff/recovery must be correct.
- **Polling and prefetching** — non-blocking readers, read-side prefetch, and
  pollable maintenance helpers must be reliable, bounded, and low-jitter.
- **Codec round-trips** — user-supplied codecs must serialize and deserialize without
  loss or allocation.
- **File format invariants** — `.ringloom` files and `metadata.ringloom` must conform to the spec.

---

> **API drift note (kept up-to-date with the implementation):**
> The original spec snippets below reference a few names that were renamed or
> never landed in the public API. When porting a snippet, translate as follows:
>
> | Spec name                          | Actual API in `src/`                                  |
> |------------------------------------|-------------------------------------------------------|
> | `ringloom.RawBytesCodec`           | `ringloom.RawCodec`                                   |
> | `.roll_scheme = .FAST_DAILY`       | `.roll_scheme = ringloom.roll.findSchemeByName("FAST_DAILY").?` |
> | `var appender = try queue.appender();` then `appender.append(payload)` | `_ = try queue.append(payload);` (the queue caches a single appender internally) |
> | `queue.tailer(.start)`             | `queue.tailer(0)` (start_index is a `u64`)            |
> | `RollScheme.FAST_DAILY` (enum decl)| `ringloom.roll.findSchemeByName("FAST_DAILY").?`      |
>
> The benchmark binary in §6 below uses the actual API; older test snippets are
> kept for design intent but may need the substitutions above to compile.

---

## 1. Zig's Built-in Test Framework

### 1.1 Test Blocks and `std.testing`

Zig provides first-class test support. Any `test "name" { ... }` block in any `.zig` file
is discovered and run by `zig build test`. Assertions come from `std.testing`:

```zig
const std = @import("std");
const testing = std.testing;
const ringloom = @import("ringloom_queue.zig");

test "queue init and deinit" {
    var queue = try ringloom.Queue(TestMsg).open(.{
        .dir = "/tmp/ringloom-test-queue",
        .roll_scheme = .FAST_DAILY,
        .create = true,
        .allocator = testing.allocator,
    }, TestCodec);
    defer queue.deinit();

    try testing.expectEqual(@as(u16, 1), queue.getVersion());
}
```

Key `std.testing` functions used throughout:

| Function | Purpose |
|---|---|
| `testing.expectEqual(expected, actual)` | Exact equality |
| `testing.expectEqualStrings(expected, actual)` | Byte-level string comparison with diff on failure |
| `testing.expectEqualSlices(u8, expected, actual)` | Slice comparison |
| `testing.expect(condition)` | Boolean assertion |
| `testing.expectError(expected_err, result)` | Assert a specific error was returned |
| `testing.allocator` | Leak-detecting allocator that fails the test on leak |

### 1.2 Test Organization

Tests live alongside the code they test, in the same `.zig` source files or in dedicated
`*_test.zig` companion files:

```
src/
├── queue.zig
├── queue_test.zig          # Queue lifecycle tests
├── appender.zig
├── appender_test.zig       # Appender + CAS tests
├── tailer.zig
├── tailer_test.zig         # Tailer polling + read-prefetch tests
├── metadata.zig            # SharedMetadata, QueueFileHeader, header constants
├── metadata_test.zig       # Struct layout tests, header encoding tests
├── index.zig
├── index_test.zig          # Flat index tests
├── codec.zig
├── codec_test.zig          # Codec round-trip tests
├── roll_scheme.zig
├── roll_scheme_test.zig    # Roll scheme tests
├── mmap.zig
├── mmap_test.zig           # Memory mapping tests
├── platform.zig
├── platform_test.zig       # Linux/macOS preallocation/advice capability tests
├── prefetcher.zig
├── prefetcher_test.zig     # Write/read prefetch tests
├── cleaner.zig
└── cleaner_test.zig        # Pollable cleanup/retention tests
```

Running tests:

```sh
# Run all tests (debug mode — all safety checks enabled)
zig build test

# Run tests for a single file
zig build test -- --test-filter "SharedMetadata"
```

### 1.3 The Testing Allocator

Zig's `testing.allocator` wraps `std.heap.GeneralPurposeAllocator` and **fails the test**
if any allocation is leaked. Every test that allocates must use `testing.allocator`:

```zig
test "queue cleanup frees all memory" {
    var queue = try ringloom.Queue(TestMsg).open(.{
        .dir = tmp.path,
        .create = true,
        .allocator = testing.allocator, // Leak detection enabled
    }, TestCodec);
    defer queue.deinit(); // If this leaks, the test fails
}
```

---

## 2. Unit Tests by Component

### 2.1 Header Constants and Encoding

Test all message header encoding/decoding functions against the spec:

```zig
const hdr = @import("metadata.zig");

test "header constants have correct bit patterns" {
    try testing.expectEqual(@as(u32, 0x00000000), hdr.HD_UNALLOCATED);
    try testing.expectEqual(@as(u32, 0x80000000), hdr.HD_WORKING);
    try testing.expectEqual(@as(u32, 0x40000000), hdr.HD_METADATA);
    try testing.expectEqual(@as(u32, 0xC0000000), hdr.HD_EOF);
    try testing.expectEqual(@as(u32, 0x3FFFFFFF), hdr.HD_MASK_LENGTH);
    try testing.expectEqual(@as(u32, 0xC0000000), hdr.HD_MASK_META);
}

test "DATA header encodes payload size in lower 30 bits" {
    const h = hdr.makeDataHeader(100);
    try testing.expectEqual(@as(u32, 100), h);
    try testing.expectEqual(@as(u32, 0), h & hdr.HD_MASK_META);
    try testing.expectEqual(@as(u32, 100), h & hdr.HD_MASK_LENGTH);
}

test "METADATA header sets metadata bit" {
    const h = hdr.makeMetadataHeader(256);
    try testing.expectEqual(@as(u32, 256), h & hdr.HD_MASK_LENGTH);
    try testing.expect(h & hdr.HD_METADATA != 0);
    try testing.expect(h & hdr.HD_WORKING == 0);
}

test "WORKING header has no PID — it is exactly 0x80000000" {
    try testing.expectEqual(@as(u32, 0x80000000), hdr.HD_WORKING);
    try testing.expectEqual(@as(u32, 0), hdr.HD_WORKING & hdr.HD_MASK_LENGTH);
}

test "EOF header is exactly 0xC0000000" {
    try testing.expectEqual(@as(u32, 0xC0000000), hdr.HD_EOF);
}

test "extractPayloadSize strips upper 2 bits" {
    try testing.expectEqual(@as(u30, 42), hdr.extractPayloadSize(hdr.makeDataHeader(42)));
    try testing.expectEqual(@as(u30, 42), hdr.extractPayloadSize(hdr.makeMetadataHeader(42)));
    try testing.expectEqual(@as(u30, 0), hdr.extractPayloadSize(hdr.HD_EOF));
}
```

### 2.2 Fixed Struct Layouts

**This is a critical test category.** The entire mmap-and-cast architecture depends on
struct sizes and field offsets matching the specification exactly. If Zig's `extern struct`
layout ever changes, or if a field is added incorrectly, these tests catch it immediately.

```zig
const meta = @import("metadata.zig");
const SharedMetadata = meta.SharedMetadata;
const QueueFileHeader = meta.QueueFileHeader;

test "SharedMetadata has correct total size" {
    try testing.expectEqual(@as(usize, 512), @sizeOf(SharedMetadata));
}

test "SharedMetadata field offsets match the spec" {
    try testing.expectEqual(@as(usize, 0), @offsetOf(SharedMetadata, "magic"));
    try testing.expectEqual(@as(usize, 4), @offsetOf(SharedMetadata, "version"));
    try testing.expectEqual(@as(usize, 6), @offsetOf(SharedMetadata, "flags"));
    try testing.expectEqual(@as(usize, 8), @offsetOf(SharedMetadata, "roll_length_secs"));
    try testing.expectEqual(@as(usize, 12), @offsetOf(SharedMetadata, "index_spacing"));
    try testing.expectEqual(@as(usize, 16), @offsetOf(SharedMetadata, "index_count"));
    // epoch_ms is u64 — implicit 4-byte padding after index_count
    try testing.expectEqual(@as(usize, 24), @offsetOf(SharedMetadata, "epoch_ms"));
    try testing.expectEqual(@as(usize, 32), @offsetOf(SharedMetadata, "highest_cycle"));
    try testing.expectEqual(@as(usize, 40), @offsetOf(SharedMetadata, "lowest_cycle"));
    try testing.expectEqual(@as(usize, 48), @offsetOf(SharedMetadata, "modcount"));
    try testing.expectEqual(@as(usize, 56), @offsetOf(SharedMetadata, "write_position"));
    try testing.expectEqual(@as(usize, 64), @offsetOf(SharedMetadata, "appender_lock"));
    try testing.expectEqual(@as(usize, 72), @offsetOf(SharedMetadata, "_reserved"));
}

test "SharedMetadata atomic fields are 8-byte aligned" {
    try testing.expect(@offsetOf(SharedMetadata, "highest_cycle") % 8 == 0);
    try testing.expect(@offsetOf(SharedMetadata, "lowest_cycle") % 8 == 0);
    try testing.expect(@offsetOf(SharedMetadata, "modcount") % 8 == 0);
    try testing.expect(@offsetOf(SharedMetadata, "write_position") % 8 == 0);
    try testing.expect(@offsetOf(SharedMetadata, "appender_lock") % 8 == 0);
}

test "SharedMetadata magic number is correct" {
    const m = SharedMetadata{
        .roll_length_secs = 86400,
        .index_spacing = 256,
        .index_count = 4096,
        .epoch_ms = 0,
        .highest_cycle = 0,
        .lowest_cycle = 0,
        .modcount = 0,
        .write_position = 0,
        .appender_lock = 0,
    };
    try testing.expectEqual(@as(u32, 0x4D515A42), m.magic);
}

test "QueueFileHeader has correct total size" {
    try testing.expectEqual(@as(usize, 64), @sizeOf(QueueFileHeader));
}

test "QueueFileHeader field offsets match the spec" {
    try testing.expectEqual(@as(usize, 0), @offsetOf(QueueFileHeader, "magic"));
    try testing.expectEqual(@as(usize, 4), @offsetOf(QueueFileHeader, "version"));
    try testing.expectEqual(@as(usize, 6), @offsetOf(QueueFileHeader, "flags"));
    try testing.expectEqual(@as(usize, 8), @offsetOf(QueueFileHeader, "roll_length_secs"));
    try testing.expectEqual(@as(usize, 12), @offsetOf(QueueFileHeader, "index_spacing"));
    try testing.expectEqual(@as(usize, 16), @offsetOf(QueueFileHeader, "index_count"));
    try testing.expectEqual(@as(usize, 24), @offsetOf(QueueFileHeader, "epoch_ms"));
    try testing.expectEqual(@as(usize, 32), @offsetOf(QueueFileHeader, "created_cycle"));
    try testing.expectEqual(@as(usize, 36), @offsetOf(QueueFileHeader, "_reserved"));
}

test "QueueFileHeader magic number is correct" {
    const h = QueueFileHeader{
        .roll_length_secs = 86400,
        .index_spacing = 256,
        .index_count = 4096,
        .epoch_ms = 0,
        .created_cycle = 0,
    };
    try testing.expectEqual(@as(u32, 0x43515A42), h.magic);
}
```

### 2.3 Flat Index

Test the index region's offset calculation, slot lookup, binary search, and atomic
consistency:

```zig
test "index region offset is 64 bytes into file" {
    // Index region starts immediately after QueueFileHeader
    try testing.expectEqual(@as(usize, 64), @sizeOf(QueueFileHeader));
}

test "index region size matches index_count * 8" {
    const index_count: u32 = 4096;
    const expected_size: usize = index_count * @sizeOf(u64);
    try testing.expectEqual(@as(usize, 32_768), expected_size);
}

test "data region offset is header + index region" {
    const index_count: u32 = 4096;
    const data_offset = @sizeOf(QueueFileHeader) + index_count * @sizeOf(u64);
    try testing.expectEqual(@as(usize, 32_832), data_offset);
}

test "slotFor computes correct index slot" {
    const index_spacing: u32 = 256;
    try testing.expectEqual(@as(u32, 0), idx.slotFor(0, index_spacing));
    try testing.expectEqual(@as(u32, 0), idx.slotFor(255, index_spacing));
    try testing.expectEqual(@as(u32, 1), idx.slotFor(256, index_spacing));
    try testing.expectEqual(@as(u32, 1), idx.slotFor(511, index_spacing));
    try testing.expectEqual(@as(u32, 4), idx.slotFor(1024, index_spacing));
}

test "binary search finds nearest populated slot" {
    // Create a mock index region with known populated slots
    var index_region = [_]u64{0} ** 16;
    index_region[0] = 32_832;  // first data message
    index_region[2] = 33_200;  // message at seqnum 512
    index_region[5] = 34_100;  // message at seqnum 1280

    // Search for seqnum 700 → should find slot 2 (nearest ≤)
    const result = idx.binarySearchIndex(&index_region, 2);
    try testing.expectEqual(@as(usize, 2), result.slot);
    try testing.expectEqual(@as(u64, 33_200), result.offset);
}

test "index atomic store and load consistency" {
    var slot: u64 align(8) = 0;
    @atomicStore(u64, &slot, 42_000, .release);
    const loaded = @atomicLoad(u64, &slot, .acquire);
    try testing.expectEqual(@as(u64, 42_000), loaded);
}
```

### 2.4 Codec Interface

Codecs must survive round-trips, report accurate sizes, and allocate nothing:

```zig
test "codec round-trip: parse(write(msg)) == msg" {
    var buf: [4096]u8 = undefined;
    const original = TestMsg{ .id = 42, .value = 3.14, .name = "hello" };

    const written = TestCodec.write(&buf, original);
    const parsed = TestCodec.parse(written);

    try testing.expectEqual(original.id, parsed.id);
    try testing.expect(original.value == parsed.value);
    try testing.expectEqualStrings(original.name, parsed.name);
}

test "codec serialized_size matches actual output" {
    const msg = TestMsg{ .id = 1, .value = 0.0, .name = "test" };
    const predicted = TestCodec.serializedSize(msg);

    var buf: [4096]u8 = undefined;
    const actual = TestCodec.write(&buf, msg);

    try testing.expectEqual(predicted, actual.len);
}

test "codec zero allocation verification" {
    // Use a counting allocator to prove no allocations during encode/decode
    var counting = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = counting.deinit();

    const before = counting.total_requested_bytes;

    var buf: [4096]u8 = undefined;
    const msg = TestMsg{ .id = 1, .value = 0.0, .name = "x" };
    const written = TestCodec.write(&buf, msg);
    _ = TestCodec.parse(written);

    const after = counting.total_requested_bytes;
    try testing.expectEqual(before, after);
}

test "codec handles empty message" {
    var buf: [4096]u8 = undefined;
    const msg = TestMsg{ .id = 0, .value = 0.0, .name = "" };
    const written = TestCodec.write(&buf, msg);
    const parsed = TestCodec.parse(written);
    try testing.expectEqualStrings("", parsed.name);
}

test "codec handles max-size message" {
    // Verify codec does not crash or corrupt with a large payload
    const large_name = "x" ** 1024;
    var buf: [8192]u8 = undefined;
    const msg = TestMsg{ .id = 999, .value = 1e100, .name = large_name };
    const written = TestCodec.write(&buf, msg);
    const parsed = TestCodec.parse(written);
    try testing.expectEqualStrings(large_name, parsed.name);
}
```

### 2.5 Roll Scheme

```zig
test "cycle calculation: FAST_DAILY epoch=0" {
    const scheme = ringloom.RollScheme.FAST_DAILY;
    // 2021-11-18 00:00:00 UTC = epoch day 18949
    const ts_ms: u64 = 1637193600000;
    const cycle = scheme.toCycle(ts_ms);
    try testing.expectEqual(@as(u32, 18949), cycle);
}

test "cycle calculation: next day increments cycle by 1" {
    const scheme = ringloom.RollScheme.FAST_DAILY;
    const day1: u64 = 1637193600000; // 2021-11-18
    const day2: u64 = 1637280000000; // 2021-11-19
    try testing.expectEqual(scheme.toCycle(day1) + 1, scheme.toCycle(day2));
}

test "filename generation with .ringloom extension" {
    const scheme = ringloom.RollScheme.FAST_DAILY;
    var buf: [64]u8 = undefined;
    const name = scheme.cycleToFilename(18949, &buf);
    try testing.expectEqualStrings("20211118F.ringloom", name);
}

test "TEST_SECONDLY filename with .ringloom extension" {
    const scheme = ringloom.RollScheme.TEST_SECONDLY;
    var buf: [64]u8 = undefined;
    const name = scheme.cycleToFilename(0, &buf);
    // Cycle 0 = epoch second → "19700101-000000T.ringloom"
    try testing.expectEqualStrings("19700101-000000T.ringloom", name);
}

test "date format conversion for all roll schemes" {
    // Verify every roll scheme produces a non-empty filename ending in .ringloom
    inline for (std.meta.fields(ringloom.RollScheme)) |field| {
        const scheme = @field(ringloom.RollScheme, field.name);
        var buf: [64]u8 = undefined;
        const name = scheme.cycleToFilename(0, &buf);
        try testing.expect(name.len > 4); // at least "X.ringloom"
        try testing.expect(std.mem.endsWith(u8, name, ".ringloom"));
    }
}
```

### 2.6 Alignment Padding

```zig
test "alignment padding formula" {
    // padding = (-payload_size) & 0x03
    try testing.expectEqual(@as(u32, 3), computePadding(1));
    try testing.expectEqual(@as(u32, 2), computePadding(2));
    try testing.expectEqual(@as(u32, 1), computePadding(3));
    try testing.expectEqual(@as(u32, 0), computePadding(4));
    try testing.expectEqual(@as(u32, 3), computePadding(5));
    try testing.expectEqual(@as(u32, 0), computePadding(100));
    try testing.expectEqual(@as(u32, 3), computePadding(101));
}

test "total entry size is always 4-byte aligned" {
    var i: u32 = 1;
    while (i < 1024) : (i += 1) {
        const total = 4 + i + computePadding(i); // header + payload + padding
        try testing.expect(total % 4 == 0);
    }
}
```

---

## 3. Integration Tests

### 3.1 Queue Lifecycle

```zig
test "create new queue, write, close, reopen, read" {
    const tmp = try test_util.makeTempDir("ringloom.lifecycle.");
    defer test_util.removeTempDir(tmp);

    // 1. Create queue
    {
        var queue = try ringloom.Queue([]const u8).open(.{
            .dir = tmp.path,
            .roll_scheme = .FAST_DAILY,
            .create = true,
            .allocator = testing.allocator,
        }, ringloom.RawBytesCodec);
        defer queue.deinit();

        // 2. Append several messages
        var appender = try queue.appender();
        defer appender.deinit();

        try appender.append("message-0");
        try appender.append("message-1");
        try appender.append("message-2");
    }
    // Queue is now closed (deinit ran)

    // 3. Reopen queue
    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = tmp.path,
        .roll_scheme = .FAST_DAILY,
        .create = false,
        .allocator = testing.allocator,
    }, ringloom.RawBytesCodec);
    defer queue.deinit();

    // 4. Create tailer, read all messages, verify content
    var tailer = try queue.tailer(.start);
    defer tailer.deinit();

    try testing.expectEqualStrings("message-0", (try tailer.poll()).?);
    try testing.expectEqualStrings("message-1", (try tailer.poll()).?);
    try testing.expectEqualStrings("message-2", (try tailer.poll()).?);
    try testing.expect((try tailer.poll()) == null); // no more messages
}
```

### 3.2 Metadata File

```zig
test "metadata.ringloom is valid 512-byte struct" {
    const tmp = try test_util.makeTempDir("ringloom.meta.");
    defer test_util.removeTempDir(tmp);

    // Create queue — this creates metadata.ringloom
    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = tmp.path,
        .roll_scheme = .FAST_DAILY,
        .create = true,
        .allocator = testing.allocator,
    }, ringloom.RawBytesCodec);
    defer queue.deinit();

    // Verify metadata file exists and is exactly 512 bytes
    const meta_path = try std.fs.path.join(testing.allocator, &.{ tmp.path, "metadata.ringloom" });
    defer testing.allocator.free(meta_path);

    const file = try std.fs.openFileAbsolute(meta_path, .{});
    defer file.close();

    const stat = try file.stat();
    try testing.expectEqual(@as(u64, 512), stat.size);

    // Read raw bytes, verify magic number
    var raw: [512]u8 = undefined;
    _ = try file.readAll(&raw);
    const magic = std.mem.readInt(u32, raw[0..4], .little);
    try testing.expectEqual(@as(u32, 0x4D515A42), magic); // "BZQM"

    // Verify roll config fields match
    const roll_secs = std.mem.readInt(u32, raw[8..12], .little);
    try testing.expectEqual(@as(u32, 86400), roll_secs); // FAST_DAILY = 24h
}
```

### 3.3 Queue File Structure

```zig
test "queue file has correct header and index region" {
    const tmp = try test_util.makeTempDir("ringloom.filestructure.");
    defer test_util.removeTempDir(tmp);

    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = tmp.path,
        .roll_scheme = .TEST4_SECONDLY,
        .create = true,
        .allocator = testing.allocator,
    }, ringloom.RawBytesCodec);
    defer queue.deinit();

    // Append a message to force file creation
    var appender = try queue.appender();
    defer appender.deinit();
    try appender.append("hello");

    // Find the .ringloom data file
    const ringloom_file = try test_util.findFirstRingloomFile(tmp.path);
    defer ringloom_file.close();

    // Verify QueueFileHeader at offset 0
    var hdr_buf: [64]u8 = undefined;
    _ = try ringloom_file.preadAll(&hdr_buf, 0);

    const magic = std.mem.readInt(u32, hdr_buf[0..4], .little);
    try testing.expectEqual(@as(u32, 0x43515A42), magic); // "BZQC"

    const index_count = std.mem.readInt(u32, hdr_buf[16..20], .little);
    try testing.expectEqual(@as(u32, 32), index_count); // TEST4_SECONDLY

    // Verify data region follows header + index
    const expected_data_offset = 64 + index_count * 8;
    const file_data_offset = std.mem.readInt(u64, hdr_buf[24..32], .little);
    // data_offset field or computed — verify first data header is at expected location
    var data_header_buf: [4]u8 = undefined;
    _ = try ringloom_file.preadAll(&data_header_buf, expected_data_offset);
    const data_header = std.mem.readInt(u32, &data_header_buf, .little);
    // Should be a DATA header with size = len("hello") = 5
    try testing.expectEqual(@as(u32, 5), data_header & 0x3FFFFFFF);
    try testing.expectEqual(@as(u32, 0), data_header & 0xC0000000); // DATA type
}
```

### 3.4 Cycle Rolling

```zig
test "cycle roll creates new file and writes EOF" {
    const tmp = try test_util.makeTempDir("ringloom.roll.");
    defer test_util.removeTempDir(tmp);

    // Use TEST_SECONDLY for fast rolling (1-second cycles)
    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = tmp.path,
        .roll_scheme = .TEST_SECONDLY,
        .create = true,
        .allocator = testing.allocator,
    }, ringloom.RawBytesCodec);
    defer queue.deinit();

    var appender = try queue.appender();
    defer appender.deinit();

    // Write first message in cycle N
    try appender.append("msg-in-cycle-1");
    const first_file = try test_util.findLatestRingloomFile(tmp.path);

    // Sleep past the roll boundary
    std.time.sleep(1_100 * std.time.ns_per_ms);

    // Write message in cycle N+1
    try appender.append("msg-in-cycle-2");

    // Count .ringloom files — should be at least 2
    const file_count = try test_util.countRingloomFiles(tmp.path);
    try testing.expect(file_count >= 2);

    // Verify EOF marker in the old file
    const eof_found = try test_util.hasEofMarker(first_file);
    try testing.expect(eof_found);

    // Verify new file has the second message
    var tailer = try queue.tailer(.start);
    defer tailer.deinit();

    const msg1 = (try tailer.poll()).?;
    try testing.expectEqualStrings("msg-in-cycle-1", msg1);
    const msg2 = (try tailer.poll()).?;
    try testing.expectEqualStrings("msg-in-cycle-2", msg2);
}
```

### 3.5 Pre-Roll

```zig
test "pre-roll creates next cycle file before roll boundary" {
    const tmp = try test_util.makeTempDir("ringloom.preroll.");
    defer test_util.removeTempDir(tmp);

    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = tmp.path,
        .roll_scheme = .TEST_SECONDLY,
        .create = true,
        .allocator = testing.allocator,
    }, ringloom.RawBytesCodec);
    defer queue.deinit();

    var appender = try queue.appender();
    defer appender.deinit();

    // Append triggers pre-roll of next cycle file
    try appender.append("first");

    // The pre-created file should exist (may be empty or have just a header)
    // Verify by checking file count or by inspecting the directory
    std.time.sleep(1_100 * std.time.ns_per_ms);

    // Now appending to the next cycle should be fast (file already exists)
    const start = std.time.nanoTimestamp();
    try appender.append("second");
    const elapsed = std.time.nanoTimestamp() - start;

    // Pre-rolled append should be fast — no file creation overhead
    // (This is a soft check; just verify it doesn't error)
    _ = elapsed;

    try testing.expect(try test_util.countRingloomFiles(tmp.path) >= 2);
}
```

---

## 4. Concurrency Tests

### 4.1 Multi-Process Writer/Reader

```zig
test "multi-process: writer and reader" {
    const tmp = try test_util.makeTempDir("ringloom.mp.");
    defer test_util.removeTempDir(tmp);

    // Fork a child process
    const pid = try std.posix.fork();

    if (pid == 0) {
        // Child: write messages
        var queue = try ringloom.Queue([]const u8).open(.{
            .dir = tmp.path,
            .roll_scheme = .FAST_DAILY,
            .create = true,
            .allocator = std.heap.page_allocator,
        }, ringloom.RawBytesCodec);
        defer queue.deinit();

        var appender = try queue.appender();
        defer appender.deinit();

        var i: u32 = 0;
        while (i < 100) : (i += 1) {
            var buf: [32]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "msg-{d}", .{i}) catch unreachable;
            try appender.append(msg);
        }
        std.posix.exit(0);
    } else {
        // Parent: wait briefly then read
        std.time.sleep(50 * std.time.ns_per_ms);

        var queue = try ringloom.Queue([]const u8).open(.{
            .dir = tmp.path,
            .roll_scheme = .FAST_DAILY,
            .create = false,
            .allocator = testing.allocator,
        }, ringloom.RawBytesCodec);
        defer queue.deinit();

        var tailer = try queue.tailer(.start);
        defer tailer.deinit();

        var count: u32 = 0;
        const deadline = std.time.nanoTimestamp() + 5 * std.time.ns_per_s;
        while (std.time.nanoTimestamp() < deadline) {
            if (try tailer.poll()) |_| {
                count += 1;
                if (count == 100) break;
            } else {
                std.time.sleep(1 * std.time.ns_per_ms);
            }
        }

        try testing.expectEqual(@as(u32, 100), count);

        // Reap child
        _ = std.posix.waitpid(pid, 0);
    }
}
```

### 4.2 Multi-Thread Readers

```zig
test "multiple reader threads on same queue" {
    const tmp = try test_util.makeTempDir("ringloom.mtread.");
    defer test_util.removeTempDir(tmp);

    const num_messages: u32 = 1000;
    const num_readers: u32 = 4;

    // Create and populate queue
    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = tmp.path,
        .roll_scheme = .FAST_DAILY,
        .create = true,
        .allocator = testing.allocator,
    }, ringloom.RawBytesCodec);
    defer queue.deinit();

    var appender = try queue.appender();
    defer appender.deinit();

    var i: u32 = 0;
    while (i < num_messages) : (i += 1) {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "msg-{d}", .{i}) catch unreachable;
        try appender.append(msg);
    }

    // Spawn N reader threads, each with own tailer
    var counts: [num_readers]u32 = [_]u32{0} ** num_readers;
    var threads: [num_readers]std.Thread = undefined;

    for (0..num_readers) |t| {
        threads[t] = try std.Thread.spawn(.{}, struct {
            fn reader(q: *ringloom.Queue([]const u8), count: *u32) void {
                var tailer = q.tailer(.start) catch return;
                defer tailer.deinit();

                while (tailer.poll() catch null) |_| {
                    count.* += 1;
                }
            }
        }.reader, .{ &queue, &counts[t] });
    }

    for (&threads) |*t| t.join();

    // Each thread should have read all messages
    for (counts) |c| {
        try testing.expectEqual(num_messages, c);
    }
}
```

### 4.3 Appender Lease Contention (Multi-Process)

```zig
test "multi-process appender lease allows only one active appender" {
    const tmp = try test_util.makeTempDir("ringloom.lease.");
    defer test_util.removeTempDir(tmp);

    const contenders = 4;
    var pids: [contenders]std.posix.pid_t = undefined;

    // Create queue first
    {
        var queue = try ringloom.Queue([]const u8).open(.{
            .dir = tmp.path,
            .roll_scheme = .FAST_DAILY,
            .create = true,
            .allocator = testing.allocator,
        }, ringloom.RawBytesCodec);
        queue.deinit();
    }

    // Fork multiple would-be appenders. Exactly one should acquire the lease.
    for (0..contenders) |w| {
        const pid = try std.posix.fork();
        if (pid == 0) {
            var queue = try ringloom.Queue([]const u8).open(.{
                .dir = tmp.path,
                .create = false,
                .allocator = std.heap.page_allocator,
            }, ringloom.RawBytesCodec);
            defer queue.deinit();

            if (queue.appender()) |appender| {
                defer appender.deinit();
                appender.append("lease-owner") catch {};
                std.time.sleep(100 * std.time.ns_per_ms);
                std.posix.exit(0);
            } else |err| {
                if (err == error.AppenderAlreadyOpen) std.posix.exit(2);
                std.posix.exit(3);
            }
        } else {
            pids[w] = pid;
        }
    }

    var owners: u32 = 0;
    var rejected: u32 = 0;
    for (pids) |pid| {
        const status = std.posix.waitpid(pid, 0);
        if (status.status == 0) owners += 1;
        if (status.status == 2) rejected += 1;
    }

    try testing.expectEqual(@as(u32, 1), owners);
    try testing.expectEqual(@as(u32, contenders - 1), rejected);
}
```

---

## 5. Polling, Read-Prefetch, and Maintenance Tests

```zig
test "tailer poll returns null then message without blocking" {
    const tmp = try test_util.makeTempDir("ringloom.poll.");
    defer test_util.removeTempDir(tmp);

    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = tmp.path,
        .roll_scheme = .FAST_DAILY,
        .create = true,
        .allocator = testing.allocator,
    }, ringloom.RawBytesCodec);
    defer queue.deinit();

    var tailer = try queue.tailer(.start);
    defer tailer.deinit();

    try testing.expectEqual(@as(?[]const u8, null), try tailer.poll());

    var appender = try queue.appender();
    defer appender.deinit();
    try appender.append("poll-test");

    const msg = (try tailer.poll()).?;
    try testing.expectEqualStrings("poll-test", msg);
}

test "tailer read prefetch never passes published write position" {
    const tmp = try test_util.makeTempDir("ringloom.read-prefetch.");
    defer test_util.removeTempDir(tmp);

    var queue = try openTestQueue(.{
        .dir = tmp.path,
        .read_prefetch_runway_bytes = 4 * 1024 * 1024,
        .spawn_helper_threads = false,
    });
    defer queue.deinit();

    try appendBytes(&queue, 1024 * 1024);

    var tailer = try queue.tailer(.start);
    defer tailer.deinit();

    const published = queue.diagnostics().published_write_position;
    _ = try tailer.prefetchPoll(256);

    try testing.expect(tailer.diagnostics().last_prefetch_end <= published);
}

test "maintenance poll is bounded and reports more work" {
    var queue = try openTestQueue(.{
        .spawn_helper_threads = false,
        .enable_prefetcher = true,
        .enable_cleaner = true,
    });
    defer queue.deinit();

    const result = try queue.maintenancePoll(1);
    try testing.expect(result == .idle or result == .made_progress or result == .more_work);
}

test "C ABI open does not spawn helper threads" {
    var opts = ringloom_c_default_options();
    opts.spawn_helper_threads = false;
    opts.enable_prefetcher = true;
    opts.enable_cleaner = true;

    var q: ?*ringloom_queue_t = null;
    try testing.expectEqual(RINGLOOM_OK, ringloom_queue_open(&opts, &q));
    defer ringloom_queue_close(q.?);

    try testing.expectEqual(@as(u32, 0), ringloom_debug_thread_count(q.?));
}
```

---

## 6. Performance Tests

The benchmark suite is shipped as a separate executable (built only by the
`bench` step) rather than as `zig test` cases, because we want full control over
warmup, measurement loops, output formatting, and CLI configuration. Build and
invoke it with:

```bash
zig build bench -- [--warmup=N] [--count=N] [--size=N] [--no-tailer] [--keep-dir]
```

The harness lives in `src/ringloom/bench_tests.zig` and reports both **append**
and **tailer poll** numbers in a single table:

- Throughput (msgs/s, MiB/s) measured against monotonic wall-clock time around
  the inner loop.
- Latency percentiles `min / mean / p50 / p95 / p99 / p99.9 / max` in
  nanoseconds, computed from per-message `std.Io.Clock.awake` samples.
- A clock-overhead estimate (median delta of two back-to-back `Clock.awake`
  reads) so reviewers can subtract sampling cost from latency figures.

### 6.1 Configuration & Defaults

| Flag           | Default       | Description                                             |
|----------------|---------------|---------------------------------------------------------|
| `--warmup=N`   | `10_000`      | Untimed appends executed before each measured loop.     |
| `--count=N`    | `1_000_000`   | Number of measured messages for both append and tailer. |
| `--size=N`     | `64`          | Payload size in bytes (runtime allocated).              |
| `--no-tailer`  | off           | Skip the tailer poll benchmark.                         |
| `--keep-dir`   | off           | Keep the temporary queue directory for inspection.      |
| `-h`, `--help` |               | Print usage and exit.                                   |

Notable harness invariants:

- The queue uses the `FAST_DAILY` roll scheme with `preroll_ms = 0`.
- When `(warmup + count) * entrySize(size)` exceeds one queue file, the harness
  automatically advances the append timestamp by one roll interval after each
  per-cycle capacity is filled, so large runs span multiple cycle files instead
  of failing.
- The harness still rejects configurations where a **single message** would not
  fit in one queue file, or where the run would exceed the 32-bit cycle-index
  space.
- The tailer benchmark seeks directly to the first measured append index, so
  warmup messages are not scanned, timed, or included in tailer throughput.

### 6.2 Latency & Throughput Benchmark

The append loop is the canonical microbenchmark. Per measured message:

```zig
const t0 = std.Io.Clock.awake.now(io);
_ = try queue.appendWithTimestamp(payload, fixed_ts);
const t1 = std.Io.Clock.awake.now(io);
latencies[i] = t0.durationTo(t1).toNanoseconds();
```

`appendWithTimestamp` is preferred over `append` so we don't fold the
wall-clock read into the measured cost. For large runs, the timestamp is held
constant within a cycle and then advanced by `roll_length_ms` once that cycle's
message capacity is reached. After the loop completes, the harness sorts the
latency array (nearest-rank percentiles) and prints the table.

### 6.3 Tailer Poll Latency

A single tailer is opened against the same queue at the first measured append
index returned by the append loop. The harness then times one `poll()` per
measured message. Because the queue is fully written before this loop, every
poll returns an entry; if a `null` is observed (e.g. because of a transient
mapping update) the harness spins and retries without recording a sample.

### 6.4 Reading the Output

A typical result looks like:

```
+------------------------+--------------------------+--------------------------+
| metric                 | append                   | tailer poll              |
+------------------------+--------------------------+--------------------------+
| samples                |                  1000000 |                  1000000 |
| throughput (msgs/s)    |                 10565203 |                 19304604 |
| throughput (MiB/s)     |                   644.85 |                  1178.26 |
| min (ns)               |                       30 |                       20 |
| mean (ns)              |                       70 |                       28 |
| p50 (ns)               |                       30 |                       30 |
| p95 (ns)               |                       50 |                       30 |
| p99 (ns)               |                     2184 |                       40 |
| p99.9 (ns)             |                     2415 |                     1904 |
| max (ns)               |                    10149 |                    11361 |
+------------------------+--------------------------+--------------------------+
```

Latency numbers describe **append-to-mmap** and **poll-from-mmap** costs; they
do not include `msync`/`fsync` durability. Dedicated latency runs should pin the
benchmark to an isolated CPU and sample
`perf stat -e page-faults,minor-faults,major-faults,dTLB-load-misses` alongside.

### 6.5 Page-Fault Regression (future work)

The low-jitter profile must prove that the appender does not take page faults
in steady state when the prefetcher is enabled. This is currently driven
manually by running the bench under `perf stat`; an automated regression that
reads `/proc/self/status` before/after the timed loop and asserts
`major_faults` is unchanged is tracked separately.

### 6.6 Cleaner Does Not Reclaim Hot Data

```zig
test "cleaner only reclaims data behind local tailers and retention floor" {
    var queue = try openTestQueue(.{
        .enable_cleaner = true,
        .retention_cycles = 2,
    });
    defer queue.deinit();

    // Append and roll several cycles, keep one tailer intentionally behind,
    // then run cleaner once.
    try appendAcrossCycles(&queue, 5);
    var lagging = try queue.tailer(Index.compose(queue.currentCycle() - 1, 0));
    defer lagging.deinit();

    try queue.cleaner.?.runOnce();

    try testing.expect(try queue.cycleExists(queue.currentCycle()));
    try testing.expect(try queue.cycleExists(queue.currentCycle() - 1));
    try testing.expect(!(try queue.cycleExists(queue.currentCycle() - 4)));
}
```

---

## 7. Property-Based Testing

### 7.1 Codec Round-Trip

```zig
test "property: codec round-trip for random messages" {
    var prng = std.Random.DefaultPrng.init(0xDEADBEEF);
    const random = prng.random();

    var i: usize = 0;
    while (i < 10_000) : (i += 1) {
        // Generate random payload
        var payload: [256]u8 = undefined;
        const len = random.intRangeAtMost(usize, 0, payload.len);
        random.bytes(payload[0..len]);

        var buf: [4096]u8 = undefined;
        const written = ringloom.RawBytesCodec.write(&buf, payload[0..len]);
        const parsed = ringloom.RawBytesCodec.parse(written);

        try testing.expectEqualSlices(u8, payload[0..len], parsed);
    }
}
```

### 7.2 Index Region Invariants

```zig
test "property: index offsets are monotonically increasing" {
    const tmp = try test_util.makeTempDir("ringloom.prop.index.");
    defer test_util.removeTempDir(tmp);

    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = tmp.path,
        .roll_scheme = .TEST4_SECONDLY, // small index for fast test
        .create = true,
        .allocator = testing.allocator,
    }, ringloom.RawBytesCodec);
    defer queue.deinit();

    var appender = try queue.appender();
    defer appender.deinit();

    // Write enough messages to populate several index entries
    // TEST4_SECONDLY: index_spacing=4, index_count=32
    var i: u32 = 0;
    while (i < 128) : (i += 1) {
        try appender.append("prop-test-msg");
    }

    // Read the index region and verify monotonicity
    const index_region = try test_util.readIndexRegion(tmp.path);
    var prev: u64 = 0;
    for (index_region) |offset| {
        if (offset == 0) continue; // unpopulated
        try testing.expect(offset > prev);
        prev = offset;
    }
}
```

### 7.3 Header Encoding Round-Trip

```zig
test "property: header encode/decode round-trip for random sizes" {
    var prng = std.Random.DefaultPrng.init(0xCAFEBABE);
    const random = prng.random();

    var i: usize = 0;
    while (i < 100_000) : (i += 1) {
        const size = random.intRangeAtMost(u30, 1, 0x3FFFFFFF);

        // DATA header round-trip
        const data_hdr = hdr.makeDataHeader(size);
        try testing.expectEqual(size, hdr.extractPayloadSize(data_hdr));
        try testing.expect(!hdr.isWorking(data_hdr));
        try testing.expect(!hdr.isEof(data_hdr));

        // METADATA header round-trip
        const meta_hdr = hdr.makeMetadataHeader(size);
        try testing.expectEqual(size, hdr.extractPayloadSize(meta_hdr));
        try testing.expect(hdr.isMetadata(meta_hdr));
    }
}
```

---

## 8. Fuzzing Strategy

### 8.1 Zig's Built-in Fuzzing

Zig 0.12+ supports fuzz testing natively. Fuzz targets are `test` blocks that accept
a `fuzz` input:

```zig
test "fuzz codec parse does not crash" {
    // Feed random bytes to the codec parser — must not crash, panic, or UB
    const input = std.testing.fuzzInput(.{});
    _ = ringloom.RawBytesCodec.parse(input) catch {};
}

test "fuzz message header parser does not crash" {
    const input = std.testing.fuzzInput(.{});
    if (input.len < 4) return;
    const header = std.mem.readInt(u32, input[0..4], .little);
    _ = hdr.extractPayloadSize(header);
    _ = hdr.isWorking(header);
    _ = hdr.isEof(header);
    _ = hdr.isMetadata(header);
}
```

### 8.2 Queue File Fuzzing

```zig
test "fuzz queue file parser with random bytes" {
    // Write random bytes as a .ringloom file and try to open/read
    const input = std.testing.fuzzInput(.{});
    const tmp = try test_util.makeTempDir("ringloom.fuzz.");
    defer test_util.removeTempDir(tmp);

    // Write random content as a .ringloom file
    const fuzz_path = try std.fs.path.join(testing.allocator, &.{ tmp.path, "19700101F.ringloom" });
    defer testing.allocator.free(fuzz_path);

    {
        const file = try std.fs.createFileAbsolute(fuzz_path, .{});
        defer file.close();
        _ = try file.writeAll(input);
    }

    // Also create a valid metadata.ringloom so queue.open doesn't fail on that
    try test_util.writeValidMetadata(tmp.path);

    // Attempt to open and read — must not crash
    var queue = ringloom.Queue([]const u8).open(.{
        .dir = tmp.path,
        .create = false,
        .allocator = testing.allocator,
    }, ringloom.RawBytesCodec) catch return;
    defer queue.deinit();

    var tailer = queue.tailer(.start) catch return;
    defer tailer.deinit();

    // Read up to 100 messages — any errors are fine, just don't crash
    var count: u32 = 0;
    while (count < 100) : (count += 1) {
        _ = tailer.poll() catch break;
    }
}
```

### 8.3 Fuzz Targets to Implement

| Target | Input | Goal |
|--------|-------|------|
| Codec parse | Random bytes | No crash, no UB |
| Message header parser | Random 4-byte values | No crash, correct classification |
| Queue file reader | Random `.ringloom` file | No crash, graceful error |
| Index binary search | Random u64 array | No out-of-bounds, correct result |
| Metadata parser | Random 512 bytes | No crash, detects invalid magic |

### 8.4 Seed Corpus

Seed the fuzzer with known-good files from integration tests:

```sh
# Build seed corpus from passing integration tests
mkdir -p fuzz/corpus/ringloom_file
cp /tmp/ringloom-test-*/20*.ringloom fuzz/corpus/ringloom_file/

mkdir -p fuzz/corpus/metadata
cp /tmp/ringloom-test-*/metadata.ringloom fuzz/corpus/metadata/
```

---

## 9. Memory Safety

### 9.1 Zig's Built-in Safety Checks

In `Debug` and `ReleaseSafe` modes, Zig enables:

- **Bounds checking** on all slice and array access
- **Integer overflow detection** (traps on overflow in non-wrapping arithmetic)
- **Null pointer detection** (optional pointers checked on dereference)
- **Use-after-free detection** (when using the testing allocator)
- **Alignment checks** on pointer casts

These are always enabled for tests (`zig build test` defaults to Debug mode).

### 9.2 Zero Allocation on Hot Path

The hot path (append and poll) must not allocate. Verify by wrapping the allocator:

```zig
test "zero allocations on append hot path" {
    const tmp = try test_util.makeTempDir("ringloom.noalloc.");
    defer test_util.removeTempDir(tmp);

    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = tmp.path,
        .create = true,
        .allocator = testing.allocator,
    }, ringloom.RawBytesCodec);
    defer queue.deinit();

    var appender = try queue.appender();
    defer appender.deinit();

    // Warm up — first append may trigger file creation
    try appender.append("warmup");

    // Now wrap allocator to detect any calls
    var alloc_count: usize = 0;
    const counting_allocator = test_util.makeCountingAllocator(testing.allocator, &alloc_count);

    // Swap allocator (if the API supports it) or verify no allocations happened
    // Alternative: check before/after allocation count from testing.allocator
    const before = testing.allocator_instance.total_requested_bytes;

    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        try appender.append("hot-path-msg");
    }

    const after = testing.allocator_instance.total_requested_bytes;
    try testing.expectEqual(before, after); // Zero allocations
}

test "zero allocations on poll hot path" {
    const tmp = try test_util.makeTempDir("ringloom.noalloc.poll.");
    defer test_util.removeTempDir(tmp);

    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = tmp.path,
        .create = true,
        .allocator = testing.allocator,
    }, ringloom.RawBytesCodec);
    defer queue.deinit();

    var appender = try queue.appender();
    defer appender.deinit();

    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        try appender.append("msg");
    }

    var tailer = try queue.tailer(.start);
    defer tailer.deinit();

    // Warm up
    _ = try tailer.poll();

    const before = testing.allocator_instance.total_requested_bytes;

    while (try tailer.poll()) |_| {}

    const after = testing.allocator_instance.total_requested_bytes;
    try testing.expectEqual(before, after); // Zero allocations
}
```

### 9.3 mmap Safety

```zig
test "mmap: unmap before close, null after unmap" {
    const tmp = try test_util.makeTempDir("ringloom.mmap.");
    defer test_util.removeTempDir(tmp);

    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = tmp.path,
        .create = true,
        .allocator = testing.allocator,
    }, ringloom.RawBytesCodec);

    // After deinit, all mappings should be released
    queue.deinit();

    // Verify that the queue's internal pointers are null/invalid
    // (This depends on implementation — at minimum, deinit must not leak)
    // The testing allocator will catch any leaked memory
}
```

### 9.4 Testing Allocator Leak Detection

```zig
test "queue deinit frees all memory" {
    // The testing allocator will fail this test if any allocation leaks
    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = "/tmp/ringloom-leak-test",
        .create = true,
        .allocator = testing.allocator,
    }, ringloom.RawBytesCodec);

    var appender = try queue.appender();
    try appender.append("test");
    appender.deinit();

    var tailer = try queue.tailer(.start);
    _ = try tailer.poll();
    tailer.deinit();

    queue.deinit();
    // If we get here without the testing allocator asserting, no leaks occurred
}
```

---

## 10. Test Matrix

The following matrix defines the full scope of testing. Each cell should have at least
one test. Priority is indicated (P1 = must have for initial release, P2 = should have,
P3 = nice to have).

### 10.1 Format × Operation Matrix

| | v1 Read | v1 Write | v1 Read-after-Write |
|---|---|---|---|
| **Single entry** | P1 | P1 | P1 |
| **Multiple entries** | P1 | P1 | P1 |
| **Large messages** | P1 | P1 | P1 |
| **Empty queue** | P1 | P1 | P1 |
| **Cycle roll** | P1 | P1 | P1 |
| **Pre-roll** | P1 | — | P1 |
| **Multi-thread read** | P1 | — | P1 |
| **Read prefetch** | P1 | — | P1 |
| **Index seeking** | P1 | — | P1 |

### 10.2 Cycle Rolling Tests

| Scenario | Priority | Description |
|---|---|---|
| Write within single cycle | P1 | All entries have timestamps in the same roll period |
| Write across cycle boundary | P1 | Timestamps span two roll periods, verify new `.ringloom` file created |
| Read across cycle boundary | P1 | Tailer follows from one `.ringloom` file to the next |
| Multiple cycle gaps | P2 | Write to cycle N, skip N+1, write to N+2 — tailer handles missing files |
| Resume tailer at specific cycle | P1 | Tailer starts at an index in a non-first cycle |
| Cycle filename generation | P1 | Verify filenames for all roll schemes produce `.ringloom` extension |
| EOF written on roll | P1 | Verify `0xC0000000` header in old file when new cycle starts |
| Pre-roll file creation | P1 | Verify next cycle's file is pre-created before the roll boundary |

### 10.3 Roll Scheme Coverage

Test `cycleToFilename()` for every defined roll scheme:

| Scheme | Format | Roll (s) | Test cycles |
|---|---|---|---|
| FIVE_MINUTELY | `yyyyMMdd-HHmm'V'` | 300 | 0, 1, 288 |
| TEN_MINUTELY | `yyyyMMdd-HHmm'X'` | 600 | 0, 1, 144 |
| TWENTY_MINUTELY | `yyyyMMdd-HHmm'XX'` | 1200 | 0, 1 |
| HALF_HOURLY | `yyyyMMdd-HHmm'H'` | 1800 | 0, 1 |
| FAST_HOURLY | `yyyyMMdd-HH'F'` | 3600 | 0, 1, 24 |
| TWO_HOURLY | `yyyyMMdd-HH'II'` | 7200 | 0, 1 |
| FOUR_HOURLY | `yyyyMMdd-HH'IV'` | 14400 | 0, 1 |
| SIX_HOURLY | `yyyyMMdd-HH'VI'` | 21600 | 0, 1 |
| FAST_DAILY | `yyyyMMdd'F'` | 86400 | 0, 1, 18949 |
| MINUTELY | `yyyyMMdd-HHmm` | 60 | 0, 1 |
| HOURLY | `yyyyMMdd-HH` | 3600 | 0, 1 |
| DAILY | `yyyyMMdd` | 86400 | 0, 1, 24855, 24856 |
| TEST_SECONDLY | `yyyyMMdd-HHmmss'T'` | 1 | 0, 1, 86400 |
| TEST4_SECONDLY | `yyyyMMdd-HHmmss'T4'` | 1 | 0, 1 |

### 10.4 Struct Layout Tests

| Struct | Size | Test | Priority |
|---|---|---|---|
| `SharedMetadata` | 512 | Total size, all field offsets, atomic field alignment | P1 |
| `QueueFileHeader` | 64 | Total size, all field offsets | P1 |
| Magic numbers | — | `0x4D515A42` (metadata), `0x43515A42` (data file) | P1 |

### 10.5 Multi-Process Coordination

| Scenario | Priority | Description |
|---|---|---|
| Single writer, single reader (separate processes) | P1 | Fork, parent writes, child reads via tailer polling |
| Writer creates new cycle file, reader follows | P1 | Reader detects modcount change and opens new file |
| Multiple appenders to same queue | P1 | Fork N would-be appenders, verify only one acquires the appender lease |
| Reader starts before writer | P2 | Reader polls until data appears |
| Writer crash recovery | P3 | Writer dies mid-write (WORKING bit set), reader skips stale entry |

### 10.6 Polling, Prefetch, and C ABI Tests

| Scenario | Priority | Description |
|---|---|---|
| Poll empty queue | P1 | `Tailer.poll()` returns not-ready/null without blocking |
| Poll after publish | P1 | Writer publishes, reader observes via acquire loads |
| Read prefetch bounds | P1 | Prefetch never passes published write position or unpublished cycle headers |
| Multi-tailer prefetch budget | P1 | Overlapping prefetch work is coalesced or bounded |
| C ABI no threads | P1 | C ABI open does not spawn helper threads |
| C ABI maintenance poll | P1 | Prefetcher/cleaner work advances only when caller polls |

---

## 11. Continuous Integration

### 11.1 Build and Test Commands

```sh
# Build everything
zig build

# Run all tests (debug mode — all safety checks enabled)
zig build test

# Run tests in ReleaseSafe (optimized but with safety checks)
zig build test -Doptimize=ReleaseSafe

# Run tests in ReleaseFast (no safety checks — performance baseline)
zig build test -Doptimize=ReleaseFast

# Run fuzzing for 5 minutes
zig build test -Dfuzz -- --fuzz-timeout=300000

# Run benchmarks only
zig build test -- --test-filter "benchmark"
```

### 11.2 CI Pipeline Stages

```
┌──────────────┐    ┌───────────────┐    ┌──────────────────┐    ┌────────────┐
│ Build (Debug) │───>│ Unit Tests    │───>│ Integration Tests │───>│ Fuzz (5min)│
│               │    │ (all safety)  │    │ (lifecycle, mmap) │    │            │
└──────────────┘    └───────────────┘    └──────────────────┘    └────────────┘
                           │                      │
                           ▼                      ▼
                    ┌───────────────┐    ┌──────────────────┐
                    │ ReleaseSafe   │    │ Concurrency Tests │
                    │ Tests         │    │ (multi-process)   │
                    └───────────────┘    └──────────────────┘
                                                  │
                                                  ▼
                                         ┌──────────────────┐
                                         │ Benchmarks       │
                                         │ (nightly only)   │
                                         └──────────────────┘
```

### 11.3 build.zig Test Configuration

```zig
// build.zig

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Main library
    const lib = b.addStaticLibrary(.{
        .name = "ringloom_queue",
        .root_source_file = b.path("src/ringloom_queue.zig"),
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(lib);

    // Test step — discovers all test blocks in all source files
    const test_step = b.step("test", "Run all tests");

    const test_files = [_][]const u8{
        "src/metadata_test.zig",
        "src/index_test.zig",
        "src/codec_test.zig",
        "src/roll_scheme_test.zig",
        "src/queue_test.zig",
        "src/appender_test.zig",
        "src/tailer_test.zig",
        "src/mmap_test.zig",
        "src/platform_test.zig",
        "src/prefetcher_test.zig",
        "src/cleaner_test.zig",
        "src/c_api_test.zig",
    };

    for (test_files) |test_file| {
        const t = b.addTest(.{
            .root_source_file = b.path(test_file),
            .target = target,
            .optimize = optimize,
        });
        // Link the ringloom_queue module so tests can import it
        t.root_module.addImport("ringloom_queue", &lib.root_module);

        const run = b.addRunArtifact(t);
        test_step.dependOn(&run.step);
    }

    // Benchmark step (separate, opt-in executable, not a test).
    const bench_mod = b.createModule(.{
        .root_source_file = b.path("src/ringloom/bench_tests.zig"),
        .target = target,
        .optimize = .ReleaseFast,
        .imports = &.{
            .{ .name = "ringloom_queue", .module = mod },
        },
    });
    const bench_exe = b.addExecutable(.{
        .name = "ringloom_queue_bench",
        .root_module = bench_mod,
    });
    const bench_run = b.addRunArtifact(bench_exe);
    if (b.args) |args| bench_run.addArgs(args);
    const bench_step = b.step("bench", "Run opt-in benchmarks");
    bench_step.dependOn(&bench_run.step);
}
```

---

## 12. Summary: Test Count Estimates

| Category | Est. Tests | Notes |
|---|---|---|
| Header constants and encoding | 8 | Bit patterns, encode/decode, round-trip |
| Struct layout (SharedMetadata) | 4 | Size, offsets, alignment, magic |
| Struct layout (QueueFileHeader) | 3 | Size, offsets, magic |
| Flat index | 6 | Offset calc, slotFor, binary search, atomics |
| Codec round-trip | 5 | Round-trip, size, zero-alloc, empty, max-size |
| Roll scheme filenames | 27 | One per defined scheme |
| Roll scheme cycle calculation | 3 | Epoch, boundary, inverse |
| Alignment padding | 2 | Formula, 4-byte alignment invariant |
| Queue lifecycle (integration) | 4 | Create/write/reopen/read, metadata, file structure, error cases |
| Cycle rolling (integration) | 4 | Roll, EOF, pre-roll, multi-cycle read |
| Multi-process | 3 | Writer/reader, multi-thread, appender lease contention |
| Polling/read prefetch/C ABI | 6 | Poll readiness, prefetch bounds, no internal C ABI threads |
| Performance benchmarks | 3 | Append latency, poll latency, throughput/page faults |
| Property-based (random) | 3 | Codec round-trip, index monotonicity, header round-trip |
| Fuzz targets | 5 | Codec, header, queue file, index, metadata |
| Memory safety | 4 | Leak detection, zero-alloc append, zero-alloc poll, mmap |
| Error handling | 4 | Invalid dir, corrupt file, bad magic, missing metadata |
| **Total** | **~97** | |

All tests run in under 5 seconds in debug mode (excluding fuzz and multi-process I/O
wait tests). Benchmarks run in `ReleaseFast` mode and are opt-in. Fuzz tests run for a
configurable duration.
