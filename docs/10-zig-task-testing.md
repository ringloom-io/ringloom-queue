# Task 8: Testing and Validation Strategy

## Overview

This task defines the comprehensive testing strategy for the Zig reimplementation of
libchronicle. The C test suite uses cmocka for unit tests and libarchive for unpacking
Java-written queue fixtures. The Zig reimplementation will use Zig's built-in test
framework (`test` blocks and `std.testing`), Zig's built-in fuzz testing, and the
language's runtime safety checks to achieve equivalent — and stronger — coverage.

The overarching goal is **cross-compatibility**: the Zig implementation must read queues
written by Java Chronicle Queue, and Java must be able to read queues written by Zig.
Every C test case is ported, and new categories of tests are added that were not feasible
in the C codebase.

---

## 1. Zig's Built-in Test Framework

### 1.1 Test Blocks and `std.testing`

Zig provides first-class test support. Any `test "name" { ... }` block in any `.zig` file
is discovered and run by `zig build test`. Assertions come from `std.testing`:

```zig
const std = @import("std");
const testing = std.testing;
const chronicle = @import("chronicle.zig");

test "queue init and deinit" {
    var queue = try chronicle.Queue([]const u8).open(.{
        .dir = "/tmp/zig-test-queue",
        .version = .v5,
        .roll_scheme = .fast_daily,
        .create = true,
        .allocator = testing.allocator,
    }, chronicle.StringCodec);
    defer queue.deinit();

    try testing.expectEqual(chronicle.Version.v5, queue.getVersion());
}
```

Key `std.testing` functions used throughout:

| Function | Purpose |
|---|---|
| `testing.expectEqual(expected, actual)` | Exact equality |
| `testing.expectEqualStrings(expected, actual)` | Byte-level string comparison with diff on failure |
| `testing.expectEqualSlices(u8, expected, actual)` | Slice comparison (for wire bytes) |
| `testing.expect(condition)` | Boolean assertion |
| `testing.expectError(expected_err, result)` | Assert a specific error was returned |
| `testing.allocator` | Leak-detecting allocator that fails the test on leak |

### 1.2 Test Organization

Tests are organized into per-module test files alongside the source:

```
src/
├── chronicle.zig          # Public API
├── wire.zig               # BinaryWire protocol
├── wire_test.zig          # Wire protocol tests
├── queue_test.zig         # Queue lifecycle and I/O tests
├── roll_scheme.zig        # Roll scheme definitions
├── roll_scheme_test.zig   # Roll scheme cycle filename tests
├── buffer.zig             # Hex dump utility
├── buffer_test.zig        # Buffer formatting tests
├── c_api.zig              # C ABI compatibility layer
└── c_api_test.zig         # C ABI round-trip tests
```

Run all tests:

```sh
zig build test
```

Run a specific test by name filter:

```sh
zig build test -- "wire protocol"
```

### 1.3 The Testing Allocator

`std.testing.allocator` is a `GeneralPurposeAllocator` configured to detect:
- Memory leaks (any allocation not freed when the test exits)
- Double frees
- Use-after-free (in debug/safe modes)
- Buffer overflows via guard pages

Every test that allocates memory should use `testing.allocator` as the allocator passed
to queue/tailer constructors. This provides automatic leak detection without Valgrind.

---

## 2. Porting the C Test Suite

Every C test case maps to one or more Zig test blocks. The table below lists each C test
and its Zig equivalent with a description of what it verifies.

### 2.1 Queue Lifecycle Tests (from `test/test_queue.c`)

| C Test | Zig Test | What It Verifies |
|---|---|---|
| `queue_init_cleanup` | `test "queue init and cleanup"` | A queue can be initialized and immediately cleaned up without errors. No directory access needed. |
| `queue_not_exist` | `test "open nonexistent directory returns DirNotFound"` | Opening a queue pointing at a nonexistent path returns `error.DirNotFound` (C: returns -1, strerror = "dir stat fail"). |
| `queue_is_file` | `test "open path that is a file returns NotADirectory"` | If the queue path points to a regular file instead of a directory, returns `error.NotADirectory` (C: "dir is not a directory"). |
| `queue_empty_dir_no_ver` | `test "open empty dir without version fails"` | An empty directory with no queue files and no explicit version set fails with `error.VersionDetectFailed` (C: "queue should exist (no permission to create), but version detect failed"). |
| `queue_init_rollscheme` | `test "roll scheme configuration and cycle filename generation"` | Tests setting version (rejects 6, accepts 5), setting roll scheme (rejects unknown, accepts valid), and verifies `getCyclePath()` output for FAST_HOURLY, FIVE_MINUTELY, DAILY schemes including the 32-bit overflow regression (cycle 24855/24856). |
| `queue_cqv5_sample_input` | `test "read and write v5 queue with cycle rolling"` | Unpacks `cqv5-sample-input.tar.bz2`, opens queue, verifies version=5 and roll_scheme="FAST_DAILY". Reads "one"/"two"/"three"/long text at expected indices. Appends "four five", "six" at timestamp 1637267400000 (same cycle), "seven" at 1637308800000 (next day — triggers cycle roll). Verifies cycle roll from 0x4A05 to 0x4A06. Creates a second tailer at index 0x4A0500000003 and verifies it reads from that position forward. |
| `queue_cqv5_new_queue` | `test "create new v5 queue, write, close, reopen, read"` | Creates a fresh queue in a temp directory with version=5, DAILY scheme. Appends "four five", closes, reopens (version/scheme auto-detected), reads back and verifies the message and index match. |
| `queue_cqv5_new_test4_queue_nodata` | `test "create queue with TEST4_SECONDLY and no data"` | Creates a queue with TEST4_SECONDLY scheme, closes, reopens, verifies version=5 and scheme="TEST4_SECONDLY". Tests that a queue with metadata but no data entries can be correctly reopened. |

### 2.2 Queue Lifecycle Test Implementation

```zig
// src/queue_test.zig

const std = @import("std");
const testing = std.testing;
const chronicle = @import("chronicle.zig");
const test_util = @import("test_util.zig");

test "queue init and cleanup" {
    const tmp = try test_util.makeTempDir("chronicle.test.");
    defer test_util.removeTempDir(tmp);

    var queue = try chronicle.Queue([]const u8).open(.{
        .dir = tmp.path,
        .version = .v5,
        .roll_scheme = .fast_daily,
        .create = true,
        .allocator = testing.allocator,
    }, chronicle.WireTextCodec);
    queue.deinit();
}

test "open nonexistent directory returns DirNotFound" {
    const result = chronicle.Queue([]const u8).open(.{
        .dir = "/tmp/this-path-does-not-exist-zig-test",
        .allocator = testing.allocator,
    }, chronicle.WireTextCodec);

    try testing.expectError(error.DirNotFound, result);
}

test "open path that is a file returns NotADirectory" {
    const tmp = try test_util.makeTempFile("chronicle.test.");
    defer test_util.removeTempFile(tmp);

    const result = chronicle.Queue([]const u8).open(.{
        .dir = tmp.path,
        .allocator = testing.allocator,
    }, chronicle.WireTextCodec);

    try testing.expectError(error.NotADirectory, result);
}

test "open empty dir without version fails" {
    const tmp = try test_util.makeTempDir("chronicle.test.");
    defer test_util.removeTempDir(tmp);

    const result = chronicle.Queue([]const u8).open(.{
        .dir = tmp.path,
        // no version set, no create permission
        .allocator = testing.allocator,
    }, chronicle.WireTextCodec);

    try testing.expectError(error.VersionDetectFailed, result);
}

test "roll scheme configuration and cycle filename generation" {
    const tmp = try test_util.makeTempDir("chronicle.test.");
    defer test_util.removeTempDir(tmp);

    var queue = try chronicle.Queue([]const u8).open(.{
        .dir = tmp.path,
        .version = .v5,
        .roll_scheme = .fast_hourly,
        .create = true,
        .allocator = testing.allocator,
    }, chronicle.WireTextCodec);
    defer queue.deinit();

    // FAST_HOURLY: format "yyyyMMdd-HH'F'" with 3600s roll
    {
        const p0 = try queue.getCyclePath(0);
        defer testing.allocator.free(p0);
        try testing.expect(std.mem.endsWith(u8, p0, "/19700101-00F.cq4"));

        const p1 = try queue.getCyclePath(1);
        defer testing.allocator.free(p1);
        try testing.expect(std.mem.endsWith(u8, p1, "/19700101-01F.cq4"));

        const p24 = try queue.getCyclePath(24);
        defer testing.allocator.free(p24);
        try testing.expect(std.mem.endsWith(u8, p24, "/19700102-00F.cq4"));
    }
}

test "read and write v5 queue with cycle rolling" {
    const fixture = try test_util.unpackTestData("cqv5-sample-input.tar.bz2");
    defer test_util.removeTempDir(fixture);

    const queuedir = try std.fs.path.join(testing.allocator, &.{ fixture.path, "qv5" });
    defer testing.allocator.free(queuedir);

    var queue = try chronicle.Queue([]const u8).open(.{
        .dir = queuedir,
        .allocator = testing.allocator,
    }, chronicle.WireTextCodec);
    defer queue.deinit();

    try testing.expectEqual(chronicle.Version.v5, queue.getVersion());

    var tailer = try queue.tailer(0);
    defer tailer.deinit();

    // Read the 4 entries written by Java
    const e0 = try tailer.collect();
    try testing.expectEqualStrings("one", e0.message);
    try testing.expectEqual(@as(u64, 0x4A0500000000), e0.index);

    const e1 = try tailer.collect();
    try testing.expectEqualStrings("two", e1.message);
    try testing.expectEqual(@as(u64, 0x4A0500000001), e1.index);

    const e2 = try tailer.collect();
    try testing.expectEqualStrings("three", e2.message);
    try testing.expectEqual(@as(u64, 0x4A0500000002), e2.index);

    const e3 = try tailer.collect();
    try testing.expectEqualStrings(
        "a much longer item that will need encoding as variable length text",
        e3.message,
    );
    try testing.expectEqual(@as(u64, 0x4A0500000003), e3.index);

    // Write "four five" and "six" at same-day timestamp (no cycle roll)
    const idx4 = try queue.appendWithTimestamp("four five", 1637267400000);
    try testing.expectEqual(@as(u64, 0x4A0500000004), idx4);

    const idx5 = try queue.appendWithTimestamp("six", 1637267400000);
    try testing.expectEqual(@as(u64, 0x4A0500000005), idx5);

    // Write "seven" at next-day timestamp — triggers cycle roll
    const idx6 = try queue.appendWithTimestamp("seven", 1637308800000);
    try testing.expectEqual(@as(u64, 0x4A0600000000), idx6);

    // Read back our writes through the tailer
    const e4 = try tailer.collect();
    try testing.expectEqualStrings("four five", e4.message);

    const e5 = try tailer.collect();
    try testing.expectEqualStrings("six", e5.message);

    const e6 = try tailer.collect();
    try testing.expectEqualStrings("seven", e6.message);
    try testing.expectEqual(@as(u64, 0x4A0600000000), e6.index);

    tailer.deinit();

    // Second tailer starting from midpoint
    var tailer2 = try queue.tailer(0x4A0500000003);
    defer tailer2.deinit();

    const r0 = try tailer2.collect();
    try testing.expectEqual(@as(u64, 0x4A0500000003), r0.index);
    try testing.expectEqualStrings(
        "a much longer item that will need encoding as variable length text",
        r0.message,
    );

    const r1 = try tailer2.collect();
    try testing.expectEqualStrings("four five", r1.message);

    const r2 = try tailer2.collect();
    try testing.expectEqualStrings("six", r2.message);

    const r3 = try tailer2.collect();
    try testing.expectEqualStrings("seven", r3.message);
}

test "create new v5 queue, write, close, reopen, read" {
    const tmp = try test_util.makeTempDir("chronicle.test.");
    defer test_util.removeTempDir(tmp);

    // Phase 1: create and write
    const write_idx = blk: {
        var queue = try chronicle.Queue([]const u8).open(.{
            .dir = tmp.path,
            .version = .v5,
            .roll_scheme = .daily,
            .create = true,
            .allocator = testing.allocator,
        }, chronicle.WireTextCodec);
        defer queue.deinit();

        break :blk try queue.append("four five");
    };

    // Phase 2: reopen and read
    {
        var queue = try chronicle.Queue([]const u8).open(.{
            .dir = tmp.path,
            .allocator = testing.allocator,
        }, chronicle.WireTextCodec);
        defer queue.deinit();

        try testing.expectEqual(chronicle.Version.v5, queue.getVersion());

        var tailer = try queue.tailer(0);
        defer tailer.deinit();

        const entry = try tailer.collect();
        try testing.expectEqualStrings("four five", entry.message);
        try testing.expectEqual(write_idx, entry.index);
    }
}
```

### 2.3 Wire Protocol Tests (from `test/test_wire.c`)

| C Test | Zig Test | What It Verifies |
|---|---|---|
| `test_wirepad_text` | `test "wire pad: text field with padding"` | Writing "hello" + pad_to_x8_00 produces 8 bytes: `e5 68 65 6c 6c 6f 00 00`. Parsing that data back fires the text callback with "hello". |
| `test_wirepad_fields` | `test "wire pad: mixed field types"` | Writes field_text("message", "Hello World"), field_varint("number", 1234567890), field_enum("code", "SECONDS"), field_float64("price", 10.50). Byte-level comparison against known-good hex output. |
| `test_wirepad_metadata` | `test "wire pad: full metadata.cq4t construction"` | Constructs a complete metadata page with STStore header, wireType, SCQMeta, SCQSRoll, listing.highestCycle/lowestCycle/modCount, chronicle.write.lock, chronicle.lastIndexReplicated, chronicle.lastAcknowledgedIndexReplicated. Byte-for-byte comparison against 0x1A8 bytes of known-good output. |

### 2.4 Buffer Tests (from `test/test_buffer.c`)

| C Test | Zig Test | What It Verifies |
|---|---|---|
| `test_buffer_hello` | `test "hex dump formatting"` | Verifies hex dump output for "hello world\\0" (12 bytes), empty buffer (0 bytes), and a 178-byte string. Line-by-line comparison of offset, hex columns, and ASCII columns. |

---

## 3. Wire Protocol Tests — Byte-Level Verification

Wire protocol correctness is critical because the serialized bytes are a shared contract
between Zig, Java, and C implementations. Every wire test compares output byte-for-byte
against known-good reference data.

### 3.1 Testing Strategy

1. **Construction tests**: Build wire structures using the Pad/Writer API and compare
   the resulting byte buffer against a reference slice.
2. **Round-trip tests**: Serialize → deserialize → compare with original values.
3. **Parsing tests**: Feed known byte sequences to the parser and verify callback
   invocations and extracted values.

### 3.2 Wire Text Field Test

```zig
// src/wire_test.zig

const std = @import("std");
const testing = std.testing;
const Wire = @import("wire.zig");

test "wire pad: text field with padding" {
    var pad = Wire.Pad.init(testing.allocator, 1024);
    defer pad.deinit();

    try testing.expectEqual(@as(usize, 0), pad.size());

    // Write "hello" — 1 byte descriptor (0xe5 = text, length 5), 5 bytes text
    pad.text("hello");
    pad.padToAlign8Zeroed();
    try testing.expectEqual(@as(usize, 8), pad.size()); // 1 + 5 + 2 padding

    const expected = [_]u8{ 0xe5, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00 };
    try testing.expectEqualSlices(u8, &expected, pad.bytes());

    // Parse it back and verify we get "hello"
    var got_text: ?[]const u8 = null;
    pad.parse(.{
        .on_text = struct {
            fn cb(text: []const u8, ctx: *?[]const u8) void {
                ctx.* = text;
            }
        }.cb,
        .text_ctx = &got_text,
    });

    try testing.expect(got_text != null);
    try testing.expectEqualStrings("hello", got_text.?);
}

test "wire pad: mixed field types" {
    var pad = Wire.Pad.init(testing.allocator, 1024);
    defer pad.deinit();

    pad.fieldText("message", "Hello World");
    pad.fieldVarint("number", 1234567890);
    pad.fieldEnum("code", "SECONDS");
    pad.fieldFloat64("price", 10.50);

    // Reference bytes from Chronicle Wire specification
    // https://github.com/OpenHFT/Chronicle-Wire#simple-use-case
    const expected = [_]u8{
        0xc7, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, //  .message
        0xeb, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x57, //  .Hello W
        0x6f, 0x72, 0x6c, 0x64, 0xc6, 0x6e, 0x75, 0x6d, //  orld.num
        0x62, 0x65, 0x72, 0xa6, 0xd2, 0x02, 0x96, 0x49, //  ber....I
        0xc4, 0x63, 0x6f, 0x64, 0x65, 0xe7, 0x53, 0x45, //  .code.SE
        0x43, 0x4f, 0x4e, 0x44, 0x53, 0xc5, 0x70, 0x72, //  CONDS.pr
        0x69, 0x63, 0x65, 0x90, 0x00, 0x00, 0x28, 0x41, //  ice...(A
    };
    try testing.expectEqualSlices(u8, &expected, pad.bytes());
}

test "wire pad: full metadata.cq4t construction" {
    var pad = Wire.Pad.init(testing.allocator, 1024);
    defer pad.deinit();

    // Build the metadata page exactly as the C test does:
    // Single metadata message with STStore header
    pad.qcStart(.metadata);
    pad.eventName("header");
    pad.typePrefix("STStore");
    pad.nestEnter();
    {
        pad.fieldTypeEnum("wireType", "WireType", "BINARY_LIGHT");
        pad.field("metadata");
        pad.typePrefix("SCQMeta");
        pad.nestEnter();
        {
            pad.field("roll");
            pad.typePrefix("SCQSRoll");
            pad.nestEnter();
            {
                pad.fieldVarint("length", 86400000);
                pad.fieldText("format", "yyyyMMdd'F'");
                pad.fieldVarint("epoch", 0);
            }
            pad.nestExit();
            pad.fieldVarint("deltaCheckpointInterval", 64);
            pad.fieldVarint("sourceId", 0);
        }
        pad.nestExit();
        pad.padToAlign8();
    }
    pad.nestExit();
    pad.qcFinish();

    // 6 data messages for directory listing fields
    pad.qcStart(.data);
    pad.eventName("listing.highestCycle");
    pad.uint64Aligned(18941);
    pad.qcFinish();

    pad.qcStart(.data);
    pad.eventName("listing.lowestCycle");
    pad.uint64Aligned(18941);
    pad.qcFinish();

    pad.qcStart(.data);
    pad.eventName("listing.modCount");
    pad.uint64Aligned(1);
    pad.qcFinish();

    pad.qcStart(.data);
    pad.eventName("chronicle.write.lock");
    pad.uint64Aligned(0x8000000000000000);
    pad.qcFinish();

    pad.qcStart(.data);
    pad.eventName("chronicle.lastIndexReplicated");
    pad.uint64Aligned(0xFFFFFFFFFFFFFFFF);
    pad.qcFinish();

    pad.qcStart(.data);
    pad.eventName("chronicle.lastAcknowledgedIndexReplicated");
    pad.uint64Aligned(0xFFFFFFFFFFFFFFFF);
    pad.qcFinish();

    // The reference bytes — 0x1A8 (424) bytes from the C test.
    // This is the exact content of a metadata.cq4t file for a FAST_DAILY queue.
    const expected = [_]u8{
        0xac, 0x00, 0x00, 0x40, 0xb9, 0x06, 0x68, 0x65, // ...@..he
        0x61, 0x64, 0x65, 0x72, 0xb6, 0x07, 0x53, 0x54, // ader..ST
        0x53, 0x74, 0x6f, 0x72, 0x65, 0x82, 0x96, 0x00, // Store...
        0x00, 0x00, 0xc8, 0x77, 0x69, 0x72, 0x65, 0x54, // ...wireT
        0x79, 0x70, 0x65, 0xb6, 0x08, 0x57, 0x69, 0x72, // ype..Wir
        0x65, 0x54, 0x79, 0x70, 0x65, 0xec, 0x42, 0x49, // eType.BI
        0x4e, 0x41, 0x52, 0x59, 0x5f, 0x4c, 0x49, 0x47, // NARY_LIG
        0x48, 0x54, 0xc8, 0x6d, 0x65, 0x74, 0x61, 0x64, // HT.metad
        0x61, 0x74, 0x61, 0xb6, 0x07, 0x53, 0x43, 0x51, // ata..SCQ
        0x4d, 0x65, 0x74, 0x61, 0x82, 0x5d, 0x00, 0x00, // Meta.]..
        0x00, 0xc4, 0x72, 0x6f, 0x6c, 0x6c, 0xb6, 0x08, // ..roll..
        0x53, 0x43, 0x51, 0x53, 0x52, 0x6f, 0x6c, 0x6c, // SCQSRoll
        0x82, 0x26, 0x00, 0x00, 0x00, 0xc6, 0x6c, 0x65, // .&....le
        0x6e, 0x67, 0x74, 0x68, 0xa6, 0x00, 0x5c, 0x26, // ngth..\&
        0x05, 0xc6, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, // ..format
        0xeb, 0x79, 0x79, 0x79, 0x79, 0x4d, 0x4d, 0x64, // .yyyyMMd
        0x64, 0x27, 0x46, 0x27, 0xc5, 0x65, 0x70, 0x6f, // d'F'.epo
        0x63, 0x68, 0x00, 0xd7, 0x64, 0x65, 0x6c, 0x74, // ch..delt
        0x61, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, // aCheckpo
        0x69, 0x6e, 0x74, 0x49, 0x6e, 0x74, 0x65, 0x72, // intInter
        0x76, 0x61, 0x6c, 0x40, 0xc8, 0x73, 0x6f, 0x75, // val@.sou
        0x72, 0x63, 0x65, 0x49, 0x64, 0x00, 0x8f, 0x8f, // rceId...
        0x24, 0x00, 0x00, 0x00, 0xb9, 0x14, 0x6c, 0x69, // $.....li
        0x73, 0x74, 0x69, 0x6e, 0x67, 0x2e, 0x68, 0x69, // sting.hi
        0x67, 0x68, 0x65, 0x73, 0x74, 0x43, 0x79, 0x63, // ghestCyc
        0x6c, 0x65, 0x8e, 0x00, 0x00, 0x00, 0x00, 0xa7, // le......
        0xfd, 0x49, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // .I......
        0x24, 0x00, 0x00, 0x00, 0xb9, 0x13, 0x6c, 0x69, // $.....li
        0x73, 0x74, 0x69, 0x6e, 0x67, 0x2e, 0x6c, 0x6f, // sting.lo
        0x77, 0x65, 0x73, 0x74, 0x43, 0x79, 0x63, 0x6c, // westCycl
        0x65, 0x8e, 0x01, 0x00, 0x00, 0x00, 0x00, 0xa7, // e.......
        0xfd, 0x49, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // .I......
        0x1c, 0x00, 0x00, 0x00, 0xb9, 0x10, 0x6c, 0x69, // ......li
        0x73, 0x74, 0x69, 0x6e, 0x67, 0x2e, 0x6d, 0x6f, // sting.mo
        0x64, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x8f, 0xa7, // dCount..
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ........
        0x24, 0x00, 0x00, 0x00, 0xb9, 0x14, 0x63, 0x68, // $.....ch
        0x72, 0x6f, 0x6e, 0x69, 0x63, 0x6c, 0x65, 0x2e, // ronicle.
        0x77, 0x72, 0x69, 0x74, 0x65, 0x2e, 0x6c, 0x6f, // write.lo
        0x63, 0x6b, 0x8e, 0x00, 0x00, 0x00, 0x00, 0xa7, // ck......
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, // ........
        0x2c, 0x00, 0x00, 0x00, 0xb9, 0x1d, 0x63, 0x68, // ,.....ch
        0x72, 0x6f, 0x6e, 0x69, 0x63, 0x6c, 0x65, 0x2e, // ronicle.
        0x6c, 0x61, 0x73, 0x74, 0x49, 0x6e, 0x64, 0x65, // lastInde
        0x78, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, // xReplica
        0x74, 0x65, 0x64, 0x8f, 0x8f, 0x8f, 0x8f, 0xa7, // ted.....
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // ........
        0x34, 0x00, 0x00, 0x00, 0xb9, 0x29, 0x63, 0x68, // 4....)ch
        0x72, 0x6f, 0x6e, 0x69, 0x63, 0x6c, 0x65, 0x2e, // ronicle.
        0x6c, 0x61, 0x73, 0x74, 0x41, 0x63, 0x6b, 0x6e, // lastAckn
        0x6f, 0x77, 0x6c, 0x65, 0x64, 0x67, 0x65, 0x64, // owledged
        0x49, 0x6e, 0x64, 0x65, 0x78, 0x52, 0x65, 0x70, // IndexRep
        0x6c, 0x69, 0x63, 0x61, 0x74, 0x65, 0x64, 0xa7, // licated.
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // ........
    };
    try testing.expectEqualSlices(u8, &expected, pad.bytes());
}
```

### 3.3 Wire Varint Encoding Edge Cases

The BinaryWire protocol uses a variable-length integer encoding ("stop bit" encoding).
Dedicated tests should cover edge cases:

```zig
test "wire varint encoding: boundary values" {
    var pad = Wire.Pad.init(testing.allocator, 256);
    defer pad.deinit();

    // 0 encodes as single byte
    pad.varint(0);
    try testing.expectEqualSlices(u8, &[_]u8{0x00}, pad.bytes());

    pad.clear();
    // 127 (max single-byte value)
    pad.varint(127);
    try testing.expectEqualSlices(u8, &[_]u8{0x7f}, pad.bytes());

    pad.clear();
    // 128 requires two bytes
    pad.varint(128);
    try testing.expectEqualSlices(u8, &[_]u8{ 0x80, 0x01 }, pad.bytes());

    pad.clear();
    // 1234567890 — the value used in the Chronicle Wire simple use case example
    pad.varint(1234567890);
    try testing.expectEqualSlices(u8, &[_]u8{ 0xd2, 0x02, 0x96, 0x49 }, pad.bytes());
}

test "wire varint round-trip" {
    const test_values = [_]u64{
        0, 1, 127, 128, 255, 256, 16383, 16384,
        1234567890, 86400000, 0x7FFFFFFF, 0xFFFFFFFF,
    };
    for (test_values) |val| {
        var pad = Wire.Pad.init(testing.allocator, 64);
        defer pad.deinit();
        pad.varint(val);
        const decoded = Wire.readVarint(pad.bytes());
        try testing.expectEqual(val, decoded.value);
    }
}
```

---

## 4. Integration Tests with Java-Written Queue Data

### 4.1 Test Fixtures

The C test suite includes two tar.bz2 archives containing queues written by Java
Chronicle Queue:

| File | Contents | Purpose |
|---|---|---|
| `cqv5-sample-input.tar.bz2` | `qv5/` directory with v5 queue files | Read-compatibility with v5 format |

Both contain entries: "one", "two", "three", and a longer text entry.

### 4.2 Extracting Test Data in Zig

The C tests use `libarchive` for extraction. In Zig, there are several options:

**Option A: Use `std.tar` and `std.compress.bzip2` (preferred)**

Zig's standard library includes tar parsing and bzip2 decompression since 0.12:

```zig
// src/test_util.zig

const std = @import("std");
const testing = std.testing;

pub const TempDir = struct {
    path: []const u8,
    dir: std.fs.Dir,
};

/// Unpack a .tar.bz2 test fixture from the test/ directory into a temp directory.
pub fn unpackTestData(archive_name: []const u8) !TempDir {
    // Locate the archive relative to the source file
    const test_dir = comptime std.fs.path.dirname(@src().file) orelse ".";
    const archive_path = try std.fs.path.join(
        testing.allocator,
        &.{ test_dir, "..", "native", "test", archive_name },
    );
    defer testing.allocator.free(archive_path);

    // Create a temp directory
    const tmp = try makeTempDir("chronicle.test.");

    // Open and decompress
    const file = try std.fs.openFileAbsolute(archive_path, .{});
    defer file.close();

    var bzip2_stream = std.compress.bzip2.reader(file.reader());
    var tar_iter = std.tar.iterator(bzip2_stream.reader(), .{});

    while (try tar_iter.next()) |entry| {
        switch (entry.kind) {
            .directory => {
                try tmp.dir.makePath(entry.name);
            },
            .file => {
                if (std.fs.path.dirname(entry.name)) |parent| {
                    try tmp.dir.makePath(parent);
                }
                const out_file = try tmp.dir.createFile(entry.name, .{});
                defer out_file.close();
                var buf: [8192]u8 = undefined;
                while (true) {
                    const n = try entry.reader().read(&buf);
                    if (n == 0) break;
                    try out_file.writeAll(buf[0..n]);
                }
            },
            else => {},
        }
    }

    return tmp;
}

pub fn makeTempDir(prefix: []const u8) !TempDir {
    _ = prefix;
    const path = try std.fs.selfExeDir(testing.allocator);
    const tmp_path = try std.fmt.allocPrint(
        testing.allocator,
        "{s}/test_tmp_{d}",
        .{ path, std.time.nanoTimestamp() },
    );
    testing.allocator.free(path);
    try std.fs.makeDirAbsolute(tmp_path);
    const dir = try std.fs.openDirAbsolute(tmp_path, .{});
    return TempDir{ .path = tmp_path, .dir = dir };
}

pub fn removeTempDir(tmp: TempDir) void {
    tmp.dir.close();
    std.fs.deleteTreeAbsolute(tmp.path) catch {};
    testing.allocator.free(tmp.path);
}
```

**Option B: Shell out to `tar` (fallback)**

If the standard library decompression proves problematic for specific archive formats:

```zig
pub fn unpackTestDataShell(archive_name: []const u8) !TempDir {
    const tmp = try makeTempDir("chronicle.test.");

    const archive_path = try resolveTestArchive(archive_name);
    defer testing.allocator.free(archive_path);

    const result = try std.process.Child.run(.{
        .allocator = testing.allocator,
        .argv = &.{ "tar", "xjf", archive_path, "-C", tmp.path },
    });
    defer testing.allocator.free(result.stdout);
    defer testing.allocator.free(result.stderr);

    if (result.term.Exited != 0) return error.UnpackFailed;
    return tmp;
}
```

Option A is preferred because it has no external dependency and works in sandboxed CI
environments. Option B is a pragmatic fallback.

---

## 5. Cross-Compatibility Testing

### 5.1 Zig Reads Java-Written Queues

This is covered by porting `queue_cqv5_sample_input`
(section 2.1 above). This test reads a queue that was originally created by Java
Chronicle Queue and verifies the exact message content and index values.

### 5.2 Java Reads Zig-Written Queues

To verify the reverse direction, add a test that:

1. Creates a new v5 queue using the Zig implementation
2. Writes several entries with known content and timestamps
3. Reads the queue back using a Java test harness

This requires a Java test in the `java/` directory:

```zig
test "Java can read queue written by Zig" {
    const tmp = try test_util.makeTempDir("chronicle.compat.");
    defer test_util.removeTempDir(tmp);

    // Write from Zig
    {
        var queue = try chronicle.Queue([]const u8).open(.{
            .dir = tmp.path,
            .version = .v5,
            .roll_scheme = .fast_daily,
            .create = true,
            .allocator = testing.allocator,
        }, chronicle.WireTextCodec);
        defer queue.deinit();

        _ = try queue.appendWithTimestamp("hello from zig", 1637267400000);
        _ = try queue.appendWithTimestamp("second entry", 1637267400000);
    }

    // Invoke Java reader (skip if java not available)
    const result = std.process.Child.run(.{
        .allocator = testing.allocator,
        .argv = &.{
            "java", "-cp", "java/build/libs/*",
            "net.openhft.chronicle.queue.ZigCompatTest",
            tmp.path,
        },
    }) catch |err| {
        if (err == error.FileNotFound) {
            std.log.warn("Java not found, skipping cross-compat test", .{});
            return; // Skip, don't fail
        }
        return err;
    };
    defer testing.allocator.free(result.stdout);
    defer testing.allocator.free(result.stderr);

    try testing.expectEqual(@as(u8, 0), result.term.Exited);
}
```

### 5.3 Bidirectional Interleaved Access

The strongest compatibility test is interleaved read/write between implementations:

1. Java writes entries 0-9
2. Zig reads entries 0-9, verifying content
3. Zig writes entries 10-19
4. Java reads entries 10-19, verifying content
5. Both read all 20 entries

This is implemented as a shell-orchestrated integration test rather than a unit test.

---

## 6. Property-Based Testing for the Wire Protocol

Property-based testing generates random inputs and verifies that invariants hold. This is
especially valuable for the wire protocol, where hand-written tests only cover known cases.

### 6.1 Round-Trip Property

**Property**: For any valid value, `parse(serialize(value)) == value`.

```zig
test "property: varint round-trip for random values" {
    var rng = std.Random.DefaultPrng.init(0xDEADBEEF);
    const random = rng.random();

    for (0..10000) |_| {
        const value = random.int(u64) >> @intCast(random.intRangeAtMost(u6, 0, 63));

        var buf: [10]u8 = undefined;
        const written = Wire.writeVarint(&buf, value);
        const result = Wire.readVarint(buf[0..written]);

        try testing.expectEqual(value, result.value);
        try testing.expectEqual(written, result.bytes_consumed);
    }
}
```

### 6.2 Serialized Size Property

**Property**: The serialized output length of a field equals the pre-computed size.

```zig
test "property: serialized size matches actual output" {
    var rng = std.Random.DefaultPrng.init(0xCAFEBABE);
    const random = rng.random();

    for (0..1000) |_| {
        // Generate a random field name (1-20 chars)
        var name_buf: [20]u8 = undefined;
        const name_len = random.intRangeAtMost(usize, 1, 20);
        for (name_buf[0..name_len]) |*c| c.* = random.intRangeAtMost(u8, 'a', 'z');
        const name = name_buf[0..name_len];

        // Generate a random text value (0-200 chars)
        var val_buf: [200]u8 = undefined;
        const val_len = random.intRangeAtMost(usize, 0, 200);
        for (val_buf[0..val_len]) |*c| c.* = random.intRangeAtMost(u8, 0x20, 0x7e);
        const val = val_buf[0..val_len];

        const predicted_size = Wire.sizeFieldText(name, val);

        var pad = Wire.Pad.init(testing.allocator, 512);
        defer pad.deinit();
        pad.fieldText(name, val);

        try testing.expectEqual(predicted_size, pad.size());
    }
}
```

### 6.3 Alignment Property

**Property**: After `padToAlign8()`, the pad size is always a multiple of 8.

```zig
test "property: padToAlign8 always produces 8-byte-aligned size" {
    var rng = std.Random.DefaultPrng.init(42);
    const random = rng.random();

    for (0..1000) |_| {
        var pad = Wire.Pad.init(testing.allocator, 256);
        defer pad.deinit();

        // Write a random number of random-length text fields
        const field_count = random.intRangeAtMost(usize, 1, 10);
        for (0..field_count) |_| {
            var buf: [50]u8 = undefined;
            const len = random.intRangeAtMost(usize, 1, 50);
            for (buf[0..len]) |*c| c.* = random.intRangeAtMost(u8, 'a', 'z');
            pad.text(buf[0..len]);
        }

        pad.padToAlign8();
        try testing.expect(pad.size() % 8 == 0);
    }
}
```

---

## 7. Fuzzing Strategy

### 7.1 Zig's Built-in Fuzzing

Since Zig 0.14, the compiler includes a built-in fuzzer that integrates with the test
runner. Fuzz tests are written as regular `test` blocks that accept a `std.testing.FuzzInput`:

```zig
const std = @import("std");
const Wire = @import("wire.zig");

test "fuzz wire parser does not crash" {
    const input = std.testing.fuzzInput(.{});
    // Feed arbitrary bytes to the wire parser — it must not crash,
    // must not read out of bounds, and must terminate.
    Wire.parse(input.bytes) catch {};
}

test "fuzz varint decoder does not crash" {
    const input = std.testing.fuzzInput(.{});
    if (input.bytes.len == 0) return;
    _ = Wire.readVarint(input.bytes) catch {};
}
```

Run fuzz tests:

```sh
# Run for 60 seconds
zig build test -Dfuzz -- "fuzz wire parser" --fuzz-timeout=60000

# Run indefinitely until a crash is found
zig build test -Dfuzz -- "fuzz wire parser"
```

### 7.2 Porting the C Fuzz Harness

The C codebase has `fuzzmain.c` — an AFL-based fuzzer that reads a script of
`(time, bytes)` pairs, appends random data to a queue, then replays to verify. The Zig
equivalent uses the built-in fuzzer for the parsing layer and a structured test for the
append/replay logic:

```zig
test "fuzz queue append and replay" {
    const input = std.testing.fuzzInput(.{});
    if (input.bytes.len < 4) return;

    const tmp = try test_util.makeTempDir("chronicle.fuzz.");
    defer test_util.removeTempDir(tmp);

    var queue = try chronicle.Queue([]const u8).open(.{
        .dir = tmp.path,
        .version = .v5,
        .roll_scheme = .fast_hourly,
        .create = true,
        .allocator = std.testing.allocator,
    }, chronicle.StringCodec);
    defer queue.deinit();

    // Interpret fuzz input as a series of variable-length messages
    var offset: usize = 0;
    var indices = std.ArrayList(u64).init(std.testing.allocator);
    defer indices.deinit();
    var messages = std.ArrayList([]const u8).init(std.testing.allocator);
    defer messages.deinit();

    while (offset + 2 <= input.bytes.len) {
        const msg_len = @min(
            std.mem.readInt(u16, input.bytes[offset..][0..2], .little),
            @as(u16, @intCast(@min(input.bytes.len - offset - 2, 1024))),
        );
        offset += 2;
        if (offset + msg_len > input.bytes.len) break;
        const msg = input.bytes[offset .. offset + msg_len];
        offset += msg_len;

        const idx = try queue.append(msg);
        try indices.append(idx);
        try messages.append(msg);
    }

    // Replay and verify
    var tailer = try queue.tailer(0);
    defer tailer.deinit();

    for (indices.items, messages.items) |expected_idx, expected_msg| {
        const entry = try tailer.collect();
        try std.testing.expectEqual(expected_idx, entry.index);
        try std.testing.expectEqualSlices(u8, expected_msg, entry.message);
    }
}
```

### 7.3 Fuzz Targets to Implement

| Target | Input | Invariant |
|---|---|---|
| Wire parser | Arbitrary bytes | Must not crash, OOB read, or hang |
| Varint decoder | Arbitrary bytes | Must not crash, must consume ≤ 10 bytes |
| Queue block parser | Arbitrary bytes | Must not crash when interpreting as a queue data page with headers |
| Roll scheme date format parser | Arbitrary strings | Must return error or valid result, never crash |
| Metadata page parser | Arbitrary bytes | Must not crash when parsing as a metadata.cq4t page |

### 7.4 Seed Corpus

Seed the fuzzer with known-good inputs from the test fixtures:

```sh
# Extract real queue file data as seed inputs
mkdir -p fuzz_corpus/wire_parser
# Copy actual .cq4 file content regions as seed files
cp native/test/fuzz_input/* fuzz_corpus/wire_parser/
```

The existing `native/test/fuzz_input/` directory contains seeds from the C AFL setup.
These are directly reusable.

---

## 8. Memory Safety

### 8.1 Zig's Built-in Safety Checks

In `Debug` and `ReleaseSafe` build modes, Zig enables:

| Check | What It Catches | Relevant to Chronicle |
|---|---|---|
| Bounds checking | Array/slice index out of bounds | Parsing wire data, reading queue blocks |
| Integer overflow | Signed/unsigned overflow | Cycle calculation, index arithmetic |
| Null pointer dereference | Accessing null optional | Tailer/queue lifecycle |
| Use-after-free | Accessing freed memory (via `GeneralPurposeAllocator`) | Tailer accessing closed queue |
| Alignment checks | Unaligned pointer access | uint64 aligned reads from mmap |
| Unreachable code | `unreachable` hit at runtime | Unexpected wire type codes |

These are **always on in test builds**, providing coverage equivalent to running the
C code under Valgrind + ASan — but with zero configuration overhead.

### 8.2 The Testing Allocator as a Memory Verifier

The `std.testing.allocator` wraps `GeneralPurposeAllocator` with these behaviors:

- **Leak detection**: If any allocation is not freed when the test completes, the test
  fails with a detailed trace of the leaked allocation site.
- **Use-after-free detection**: Freed memory is filled with `0xAA` bytes. Accessing it
  produces detectably wrong values rather than silently reading stale data.
- **Double-free detection**: Attempting to free already-freed memory triggers a panic
  with a stack trace.

This replaces the C test suite's Valgrind testing (`make grind`) with zero overhead:

```zig
test "queue cleanup frees all memory" {
    // testing.allocator will catch any leaks automatically
    var queue = try chronicle.Queue([]const u8).open(.{
        .dir = tmp.path,
        .version = .v5,
        .roll_scheme = .daily,
        .create = true,
        .allocator = testing.allocator, // <-- leak detector
    }, chronicle.WireTextCodec);

    _ = try queue.append("test message");

    var tailer = try queue.tailer(0);
    _ = try tailer.collect();
    tailer.deinit();

    queue.deinit();
    // If anything leaks, the test fails here with:
    //   "memory leak detected -- allocation at src/chronicle.zig:142"
}
```

### 8.3 Mmap Safety

Memory-mapped regions require special care. The Zig implementation should:

1. Track all mmap regions in a list on the queue/tailer struct
2. `munmap` them all in `deinit()`
3. Set the pointer to `undefined` after munmap (triggers safety check on use-after-unmap)
4. Use `std.os.mmap` which returns a proper Zig slice with bounds checking

```zig
fn unmapQueueFile(self: *Self) void {
    if (self.qf_buf) |buf| {
        std.posix.munmap(buf);
        self.qf_buf = undefined; // use-after-unmap → safety panic
    }
}
```

---

## 9. Test Matrix

The following matrix defines the full scope of testing. Each cell should have at least
one test. Priority is indicated (P1 = must have for initial release, P2 = should have,
P3 = nice to have).

### 9.1 Format × Operation Matrix

| | v5 Read | v5 Write | v5 Read-after-Write |
|---|---|---|---|
| **Single entry** | P1 | P1 | P1 |
| **Multiple entries** | P1 | P1 | P1 |
| **Long text (variable-length)** | P1 | P1 | P1 |
| **Empty queue (metadata only)** | P1 | P1 | P1 |
| **Binary data (non-UTF8)** | P2 | P2 | P2 |

### 9.2 Cycle Rolling Tests

| Scenario | Priority | Description |
|---|---|---|
| Write within single cycle | P1 | All entries have timestamps in the same roll period |
| Write across cycle boundary | P1 | Timestamps span two roll periods, verify new .cq4 file created |
| Read across cycle boundary | P1 | Tailer follows from one .cq4 file to the next |
| Multiple cycle gaps | P2 | Write to cycle N, skip N+1, write to N+2 — tailer must handle missing files |
| Resume tailer at specific cycle | P1 | Tailer starts at an index in a non-first cycle |
| Cycle filename generation | P1 | Verify filenames for all 27 roll schemes match C/Java output |

### 9.3 Roll Scheme Coverage

Test `getCyclePath()` for every defined roll scheme:

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
| FAST_DAILY | `yyyyMMdd'F'` | 86400 | 0, 1, 18941 |
| MINUTELY | `yyyyMMdd-HHmm` | 60 | 0, 1 |
| HOURLY | `yyyyMMdd-HH` | 3600 | 0, 1 |
| DAILY | `yyyyMMdd` | 86400 | 0, 1, 24855, 24856 |
| TEST_SECONDLY | `yyyyMMdd-HHmmss'T'` | 1 | 0, 1, 86400 |
| TEST4_SECONDLY | `yyyyMMdd-HHmmss'T4'` | 1 | 0, 1 |

### 9.4 Multi-Process Coordination

| Scenario | Priority | Description |
|---|---|---|
| Single writer, single reader (separate processes) | P1 | Fork, parent writes, child reads via tailer polling |
| Writer creates new cycle file, reader follows | P1 | Reader detects modcount change and opens new file |
| Two writers to same queue | P2 | CAS contention on header working bit |
| Reader starts before writer | P2 | Reader polls until data appears |
| Writer crash recovery | P3 | Writer dies mid-write (working bit set), reader waits then skips |

Multi-process tests use `std.process.Child` to fork and coordinate:

```zig
test "multi-process: writer and reader" {
    const tmp = try test_util.makeTempDir("chronicle.mp.");
    defer test_util.removeTempDir(tmp);

    // Fork a writer process
    const writer = try std.process.Child.init(.{
        .allocator = testing.allocator,
        .argv = &.{ "zig-out/bin/test_writer", tmp.path },
    });
    defer writer.deinit();

    // Give writer time to start
    std.time.sleep(100 * std.time.ns_per_ms);

    // Read from the same queue
    var queue = try chronicle.Queue([]const u8).open(.{
        .dir = tmp.path,
        .allocator = testing.allocator,
    }, chronicle.WireTextCodec);
    defer queue.deinit();

    var tailer = try queue.tailer(0);
    defer tailer.deinit();

    // Poll with timeout
    const deadline = std.time.nanoTimestamp() + 5 * std.time.ns_per_s;
    while (std.time.nanoTimestamp() < deadline) {
        if (try tailer.poll()) |entry| {
            try testing.expectEqualStrings("hello from writer", entry.message);
            break;
        }
        std.time.sleep(10 * std.time.ns_per_ms);
        queue.peek();
    } else {
        return error.TestTimedOut;
    }
}
```

### 9.5 Wire Protocol Test Matrix

| Test Category | Cases | Priority |
|---|---|---|
| Text field encoding/decoding | Empty, short, 127-byte, 128-byte (variable length threshold), 64KB | P1 |
| Varint encoding/decoding | 0, 1, 127, 128, 255, 65535, 2^31-1, 2^32-1 | P1 |
| Field names | 1 char, max length, all printable ASCII | P1 |
| Float64 encoding | 0.0, 1.0, -1.0, NaN, Inf, 10.50, subnormals | P1 |
| Uint64 aligned | 0, 1, 2^63, 2^64-1, 0x8000000000000000 (lock bit) | P1 |
| Nesting | 0 deep, 1 deep, 3 deep (like metadata), mismatched enter/exit | P1 |
| QC start/finish framing | Metadata flag, data flag, size calculation | P1 |
| Type prefix | Short names, "STStore", "SCQMeta", "SCQSRoll", "WireType" | P1 |
| Event name | "header", "listing.highestCycle", long names | P1 |
| Padding | padToAlign8 at every offset 0-7, padToAlign8Zeroed | P2 |
| Malformed input | Truncated varint, truncated text, invalid type code | P2 |

---

## 10. Continuous Integration

### 10.1 Build and Test Commands

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
```

### 10.2 CI Pipeline Stages

```
┌──────────────┐    ┌───────────────┐    ┌──────────────────┐    ┌────────────┐
│ Build (Debug) │───>│ Unit Tests    │───>│ Integration Tests │───>│ Fuzz (5min)│
│               │    │ (all safety)  │    │ (Java fixtures)  │    │            │
└──────────────┘    └───────────────┘    └──────────────────┘    └────────────┘
                           │
                           ▼
                    ┌───────────────┐
                    │ ReleaseSafe   │
                    │ Tests         │
                    └───────────────┘
```

### 10.3 build.zig Test Configuration

```zig
// build.zig

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Main library
    const lib = b.addStaticLibrary(.{
        .name = "chronicle",
        .root_source_file = b.path("src/chronicle.zig"),
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(lib);

    // C ABI shared library
    const clib = b.addSharedLibrary(.{
        .name = "chronicle",
        .root_source_file = b.path("src/c_api.zig"),
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(clib);

    // Test step — discovers all test blocks in all source files
    const test_step = b.step("test", "Run all tests");

    const test_files = [_][]const u8{
        "src/wire_test.zig",
        "src/queue_test.zig",
        "src/roll_scheme_test.zig",
        "src/buffer_test.zig",
        "src/c_api_test.zig",
    };

    for (test_files) |test_file| {
        const t = b.addTest(.{
            .root_source_file = b.path(test_file),
            .target = target,
            .optimize = optimize,
        });
        // Link the chronicle module so tests can import it
        t.root_module.addImport("chronicle", &lib.root_module);

        const run = b.addRunArtifact(t);
        test_step.dependOn(&run.step);
    }
}
```

---

## 11. Summary: Test Count Estimates

| Category | Est. Tests | Notes |
|---|---|---|
| Queue lifecycle (ports of C tests) | 9 | Direct ports of all cmocka tests |
| Wire protocol construction | 6 | text, fields, metadata, varint, nesting, padding |
| Wire protocol parsing | 6 | Same categories but from bytes → values |
| Wire round-trip | 4 | varint, text, fields, full metadata |
| Roll scheme filenames | 27 | One per defined scheme |
| Roll scheme 32-bit overflow | 2 | Regression test for cycle 24855/24856 |
| Buffer hex dump | 3 | Empty, short, long |
| Property-based (random) | 4 | varint round-trip, size prediction, alignment, field names |
| Fuzz targets | 5 | Wire parser, varint, queue block, date format, metadata |
| Cross-compatibility (Java) | 3 | Zig-reads-Java, Java-reads-Zig, interleaved |
| Multi-process | 3 | Single writer/reader, cycle roll, writer-starts-after |
| C ABI compatibility | 4 | init/open/append/collect through C exports |
| Error handling | 5 | Each ChronicleError variant triggered and verified |
| **Total** | **~81** | |

All tests run in under 5 seconds in debug mode (excluding fuzz and multi-process I/O
wait tests). The fuzz tests run for a configurable duration. The Java cross-compatibility
tests are skipped if Java is not installed.