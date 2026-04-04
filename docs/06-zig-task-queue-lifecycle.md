# Task 4: Queue Lifecycle Management

This document covers the Zig reimplementation of the Chronicle Queue lifecycle:
initialising a queue handle, opening an existing queue (or creating a new one),
and cleaning up all resources on close. It maps every relevant C function in
`libchronicle.c` to its idiomatic Zig equivalent.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Queue Struct Design](#2-queue-struct-design)
3. [Queue Initialisation (`chronicle_init`)](#3-queue-initialisation)
4. [Configuration Setters](#4-configuration-setters)
5. [Version Detection](#5-version-detection)
6. [Opening a Queue (`chronicle_open`)](#6-opening-a-queue)
7. [Directory Listing Init (`directory_listing_init`)](#7-directory-listing-init)
8. [Directory Listing Reopen (`directory_listing_reopen`)](#8-directory-listing-reopen)
9. [Queue File Init (`queuefile_init`)](#9-queue-file-init)
10. [Queue Cleanup (`chronicle_cleanup`)](#10-queue-cleanup)
11. [Global Queue Registry](#11-global-queue-registry)
12. [Error Handling Strategy](#12-error-handling-strategy)
13. [Roll Scheme Table](#13-roll-scheme-table)
14. [Testing Notes](#14-testing-notes)

---

## 1. Overview

In the C implementation, `chronicle_init` allocates a `queue_t`, wires up
defaults, and prepends it to a global singly-linked list (`queue_head`).
`chronicle_open` then detects the queue version, globs `.cq4` files, reads
roll configuration, and validates everything. `chronicle_cleanup` tears down
the entire queue — tailers, appender, mmaps, file descriptors, heap memory.

The Zig port replaces:

| C pattern | Zig pattern |
|---|---|
| `malloc` + `bzero` | `allocator.create(Queue)` with field defaults |
| Global linked list `queue_head` | `ArrayList(*Queue)` owned by a registry or by user code |
| `chronicle_err()` setting global string | Error unions (`error{...}`) with optional payloads |
| `free()` scattered across cleanup | `defer` / `errdefer` for deterministic cleanup |
| `strdup` | `allocator.dupe(u8, slice)` |
| `glob()` | `std.fs.Dir.iterate()` or a custom glob over the directory |
| `asprintf` | `std.fmt.allocPrint(allocator, ...)` |

---

## 2. Queue Struct Design

The C `struct queue` (lines 140-185 of `libchronicle.c`) contains ~25 fields
covering directory state, mmap'd directory listing, glob results, roll
configuration, index configuration, codec function pointers, a tailer linked
list, an appender pointer, and a next pointer for the global queue list.

### Zig struct sketch

```zig
const std = @import("std");
const posix = std.posix;

pub const Queue = struct {
    const Self = @This();

    allocator: std.mem.Allocator,

    // ── Identity ──────────────────────────────────────────────
    dirname: []const u8,

    // ── Tunables ──────────────────────────────────────────────
    blocksize: u32 = 1 << 20, // 1 MiB, must be power-of-two
    version: Version = .unknown,
    create_permitted: bool = false,

    // ── Directory listing mmap ────────────────────────────────
    dirlist_name: ?[]const u8 = null,
    dirlist_fd: ?posix.fd_t = null,
    dirlist_map: ?[]align(std.mem.page_size) u8 = null,
    dirlist_fields: DirlistFields = .{},

    // ── Globbed queue files ───────────────────────────────────
    /// Sorted list of .cq4 paths discovered in the directory.
    queuefile_paths: std.ArrayList([]const u8),

    // ── Cycle tracking (local copies from dirlist mmap) ──────
    highest_cycle: u64 = 0,
    lowest_cycle: u64 = 0,
    modcount: u64 = 0,

    // ── Roll configuration ────────────────────────────────────
    roll_length_ms: i32 = 0,
    roll_epoch: i32 = -1,
    roll_format: ?[]const u8 = null,   // Java date format e.g. "yyyyMMdd-HH'F'"
    roll_name: ?[]const u8 = null,     // scheme name e.g. "FAST_HOURLY"
    roll_strftime: ?[]const u8 = null, // C strftime equivalent e.g. "%Y%m%d-%H"

    // ── Index configuration ───────────────────────────────────
    index_count: i32 = 0,
    index_spacing: i32 = 0,

    // ── Derived constants ─────────────────────────────────────
    cycle_shift: u6 = 32,
    seqnum_mask: u64 = 0x0000_0000_FFFF_FFFF,

    // ── Codec function pointers ───────────────────────────────
    parser: ?*const fn ([]const u8) ?[]const u8 = null,
    parser_free: ?*const fn ([]const u8) void = null,
    append_sizeof: ?*const fn (*const anyopaque) usize = null,
    append_write: ?*const fn ([*]u8, *const anyopaque, usize) void = null,

    // ── Tailers ───────────────────────────────────────────────
    tailers: std.DoublyLinkedList(TailerNode) = .{},
    appender: ?*Tailer = null,

    // ── Constants ─────────────────────────────────────────────
    pub const qf_disk_sz: u64 = 83_754_496;
    pub const patch_cycles: u64 = 3;
};

pub const Version = enum(u8) {
    unknown = 0,
    v5 = 5,
};

pub const DirlistFields = struct {
    highest_cycle: ?*align(1) u64 = null,
    lowest_cycle: ?*align(1) u64 = null,
    modcount: ?*align(1) u64 = null,
};
```

Key design decisions:

- **`blocksize` is `u32`** — the C code uses `uint` and only ever holds
  powers of two up to ~64 MiB. A `u32` suffices and allows the mask
  computation `~(blocksize - 1)` to be done cleanly.
- **`version` is an enum** — catches invalid values at compile time.
- **`queuefile_paths` uses `ArrayList`** — replaces the C `glob_t`. We
  populate it by iterating the directory ourselves (see §6).
- **Codec pointers are nullable** — set via configuration; validated at open
  time.
- **Tailer list is `std.DoublyLinkedList`** — replaces hand-rolled
  `next`/`prev` pointers.

---

## 3. Queue Initialisation

The C `chronicle_init` (lines 259-287) allocates, zeroes, sets defaults, and
prepends to the global list. The Zig equivalent is `Queue.init`:

```zig
pub fn init(allocator: std.mem.Allocator, dirname: []const u8) !*Queue {
    const queue = try allocator.create(Queue);
    errdefer allocator.destroy(queue);

    const owned_dirname = try allocator.dupe(u8, dirname);
    errdefer allocator.free(owned_dirname);

    queue.* = Queue{
        .allocator = allocator,
        .dirname = owned_dirname,
        .queuefile_paths = std.ArrayList([]const u8).init(allocator),
        // All other fields use their declared defaults (blocksize = 1MiB, etc.)
    };

    return queue;
}
```

**What changed from C:**

| C | Zig |
|---|---|
| `malloc` + `bzero` | `allocator.create` (zero-init via defaults) |
| `strdup(dir)` | `allocator.dupe(u8, dirname)` |
| `queue->roll_epoch = -1` | Field default in struct declaration |
| `queue->next = queue_head; queue_head = queue;` | Handled by external `QueueRegistry` (see §11) |
| `getenv("SHMIPC_DEBUG")` | `std.posix.getenv("SHMIPC_DEBUG")` or `std.process.getEnvMap()`, stored in a config struct |

The `errdefer` on `owned_dirname` ensures that if any subsequent allocation
fails, we don't leak the string.

---

## 4. Configuration Setters

In C, these are simple setters that mutate the queue struct between `init`
and `open`. In Zig, they become methods on `Queue`:

```zig
pub fn setVersion(self: *Queue, ver: Version) void {
    self.version = ver;
}

pub fn setCreate(self: *Queue, create: bool) void {
    self.create_permitted = create;
}

pub fn setEncoder(
    self: *Queue,
    sizeof_fn: *const fn (*const anyopaque) usize,
    write_fn: *const fn ([*]u8, *const anyopaque, usize) void,
) void {
    self.append_sizeof = sizeof_fn;
    self.append_write = write_fn;
}

pub fn setDecoder(
    self: *Queue,
    parse_fn: *const fn ([]const u8) ?[]const u8,
    free_fn: ?*const fn ([]const u8) void,
) void {
    self.parser = parse_fn;
    self.parser_free = free_fn;
}
```

### `setRollScheme`

This looks up a name in the roll scheme table (see §13) and applies it.
The C version (lines 537-545) iterates the table with `strcmp`. In Zig we
use a `std.StaticStringMap` or a simple linear scan:

```zig
pub fn setRollScheme(self: *Queue, name: []const u8) !void {
    for (roll_schemes) |scheme| {
        if (std.mem.eql(u8, name, scheme.name)) {
            try self.applyRollScheme(scheme);
            return;
        }
    }
    return error.UnknownRollScheme;
}
```

### `applyRollScheme` — Java date format conversion

The C function `chronicle_apply_roll_scheme` (lines 471-535) converts a Java
date format string like `"yyyyMMdd-HH'F'"` into a strftime pattern like
`"%Y%m%d-%HF"`. It does this character-by-character, toggling an `inquote`
flag on apostrophes and replacing Java tokens with `%`-codes.

In Zig we replicate this conversion but allocate cleanly:

```zig
fn applyRollScheme(self: *Queue, scheme: RollScheme) !void {
    // Free previous strings if set
    if (self.roll_name) |old| self.allocator.free(old);
    if (self.roll_format) |old| self.allocator.free(old);
    if (self.roll_strftime) |old| self.allocator.free(old);

    self.roll_name = try self.allocator.dupe(u8, scheme.name);
    self.roll_format = try self.allocator.dupe(u8, scheme.format);
    self.roll_length_ms = @as(i32, @intCast(scheme.roll_length_secs)) * 1000;

    // Build the strftime-compatible pattern
    self.roll_strftime = try convertJavaDateFormat(self.allocator, scheme.format);
}

/// Convert Java SimpleDateFormat → POSIX strftime pattern.
///   yyyy → %Y   MM → %m   dd → %d   HH → %H   mm → %M   ss → %S
///   Single quotes toggle literal mode. Dashes are literal.
fn convertJavaDateFormat(allocator: std.mem.Allocator, java_fmt: []const u8) ![]const u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();

    var i: usize = 0;
    var in_quote = false;
    while (i < java_fmt.len) {
        const c = java_fmt[i];
        if (in_quote and c != '\'') {
            try buf.append(c);
            i += 1;
        } else if (c == '\'') {
            in_quote = !in_quote;
            i += 1;
        } else if (c == '-') {
            try buf.append('-');
            i += 1;
        } else if (i + 4 <= java_fmt.len and std.mem.eql(u8, java_fmt[i..][0..4], "yyyy")) {
            try buf.appendSlice("%Y");
            i += 4;
        } else if (i + 2 <= java_fmt.len and std.mem.eql(u8, java_fmt[i..][0..2], "MM")) {
            try buf.appendSlice("%m");
            i += 2;
        } else if (i + 2 <= java_fmt.len and std.mem.eql(u8, java_fmt[i..][0..2], "dd")) {
            try buf.appendSlice("%d");
            i += 2;
        } else if (i + 2 <= java_fmt.len and std.mem.eql(u8, java_fmt[i..][0..2], "HH")) {
            try buf.appendSlice("%H");
            i += 2;
        } else if (i + 2 <= java_fmt.len and std.mem.eql(u8, java_fmt[i..][0..2], "mm")) {
            try buf.appendSlice("%M");
            i += 2;
        } else if (i + 2 <= java_fmt.len and std.mem.eql(u8, java_fmt[i..][0..2], "ss")) {
            try buf.appendSlice("%S");
            i += 2;
        } else {
            return error.InvalidDateFormat;
        }
    }
    return try buf.toOwnedSlice();
}
```

---

## 5. Version Detection

The C `chronicle_version_detect` (lines 304-308) probes for two filenames:

```c
if (chronicle_readable(queue->dirname, "metadata.cq4t")) return 5;
return 0;
```

In Zig we use `std.fs.Dir` to attempt opening these files:

```zig
fn detectVersion(dirname: []const u8) !Version {
    var dir = try std.fs.cwd().openDir(dirname, .{});
    defer dir.close();

    // Try v5 marker
    if (dir.openFile("metadata.cq4t", .{})) |f| {
        f.close();
        return .v5;
    } else |_| {}

    return .unknown;
}
```

This avoids the C pattern of `open()` + `close()` with raw fd checks and
replaces it with Zig's `openFile` which returns an error union. We silently
discard the error (file not found) and try the next probe.

---

## 6. Opening a Queue

`chronicle_open` (lines 310-421) is the most complex lifecycle function. It:

1. Validates the directory exists (`stat`)
2. Auto-detects the version
3. Globs `.cq4` files
4. Handles create-mode vs open-mode validation
5. Builds the dirlist filename
6. Optionally creates the directory listing (`directory_listing_init`)
7. Opens and parses the directory listing (`directory_listing_reopen`)
8. Validates roll settings
9. Sets `cycle_shift = 32` and `seqnum_mask = 0xFFFFFFFF`
10. Does an initial `peek_queue` to populate cycle values

### Zig implementation sketch

```zig
pub fn open(self: *Queue) !void {
    // 1. Validate directory
    const dir_stat = std.fs.cwd().statFile(self.dirname) catch
        return error.DirStatFailed;
    if (dir_stat.kind != .directory)
        return error.NotADirectory;

    // 2. Auto-detect version
    const auto_version = try detectVersion(self.dirname);

    // 3. Glob .cq4 files
    try self.refreshQueueFiles();

    // 4. Validate version / create-mode constraints
    if (auto_version == .unknown) {
        if (!self.create_permitted)
            return error.QueueNotFoundNoCreate;
        if (self.version == .unknown)
            return error.VersionRequired;
        if (self.queuefile_paths.items.len != 0)
            return error.CreateRequiresEmptyDir;
        if (self.roll_name == null)
            return error.RollSchemeRequired;
    } else {
        if (self.version != .unknown and self.version != auto_version)
            return error.VersionMismatch;
        self.version = auto_version;
    }

    // 5. Build dirlist filename
    self.dirlist_name = try std.fmt.allocPrint(
        self.allocator,
        "{s}/{s}",
        .{
            self.dirname,
            "metadata.cq4t",
        },
    );

    // 6. Create directory listing if needed
    if (self.create_permitted and auto_version == .unknown) {
        const cycle = self.cycleFromMs(self.clockMs());
        try self.directoryListingInit(cycle);
    }

    // 7. Open + parse directory listing (read-only initially)
    try self.directoryListingReopen(.read_only);

    // 8. Validate roll settings
    if (self.roll_format == null) return error.MissingRollFormat;
    if (self.roll_length_ms == 0) return error.MissingRollLength;
    if (self.roll_epoch == -1) return error.MissingRollEpoch;

    // Cross-check detected format against known schemes
    _ = try self.setRollDateFormat(self.roll_format.?);

    // 10. Derived constants
    self.cycle_shift = 32;
    self.seqnum_mask = 0x0000_0000_FFFF_FFFF;

    // 11. Initial poll
    self.peekQueueModcount();
}
```

### `refreshQueueFiles` — replacing `glob()`

POSIX `glob()` is not available in `std.posix`. We iterate the directory and
filter for `.cq4` suffixes:

```zig
fn refreshQueueFiles(self: *Queue) !void {
    // Free old paths
    for (self.queuefile_paths.items) |p| self.allocator.free(p);
    self.queuefile_paths.clearRetainingCapacity();

    var dir = try std.fs.cwd().openDir(self.dirname, .{ .iterate = true });
    defer dir.close();

    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".cq4")) continue;

        const full = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}",
            .{ self.dirname, entry.name },
        );
        try self.queuefile_paths.append(full);
    }

    // Sort lexicographically — cycle order matches filename order
    std.mem.sort([]const u8, self.queuefile_paths.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lessThan);
}
```

---

## 7. Directory Listing Init

`directory_listing_init` (lines 1400-1476) creates the `metadata.cq4t` file
from scratch. It writes one metadata message containing the `STStore` header
with nested `SCQMeta` / `SCQSRoll` configuration, followed by six data
messages for the shared fields.

### Message structure

```
METADATA message:
  header: STStore {
    wireType: "BINARY_LIGHT"
    metadata: SCQMeta {
      roll: SCQSRoll {
        length: <roll_length_ms>
        format: "<java_date_format>"
        epoch: <roll_epoch>
      }
      deltaCheckpointInterval: 64
      sourceId: 0
    }
  }

DATA message 1: listing.highestCycle  = <cycle>      (uint64, aligned)
DATA message 2: listing.lowestCycle   = <cycle>      (uint64, aligned)
DATA message 3: listing.modCount      = 1            (uint64, aligned)
DATA message 4: chronicle.write.lock  = 0x8000000000000000
DATA message 5: chronicle.lastIndexReplicated = -1 (0xFFFFFFFFFFFFFFFF)
DATA message 6: chronicle.lastAcknowledgedIndexReplicated = -1
```

Each message is framed with a 4-byte header (metadata bit | size for metadata
messages, plain size for data messages). The uint64 values in data messages
are **8-byte aligned** so they can be atomically read/written via the mmap.

### Zig implementation sketch

The C code uses `wirepad_t` — a growable buffer with nesting support. We
build an equivalent `WirePad` in Zig (covered in a wire protocol document)
and use it here:

```zig
fn directoryListingInit(self: *Queue, cycle: u64) !void {
    const dirlist_name = self.dirlist_name orelse return error.DirlistNameNotSet;

    // Create/truncate the file
    const file = try std.fs.cwd().createFile(dirlist_name, .{
        .mode = 0o777,
        .truncate = true,
    });
    defer file.close();

    var pad = try WirePad.init(self.allocator, 1024);
    defer pad.deinit();

    const roll_epoch: i32 = if (self.roll_epoch == -1) 0 else self.roll_epoch;

    // ── Metadata message: STStore with SCQMeta/SCQSRoll ──────
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
                pad.fieldVarint("length", self.roll_length_ms);
                pad.fieldText("format", self.roll_format.?);
                pad.fieldVarint("epoch", roll_epoch);
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

    // ── Six data messages ─────────────────────────────────────
    const data_fields = [_]struct { name: []const u8, value: u64 }{
        .{ .name = "listing.highestCycle", .value = cycle },
        .{ .name = "listing.lowestCycle", .value = cycle },
        .{ .name = "listing.modCount", .value = 1 },
        .{ .name = "chronicle.write.lock", .value = 0x8000_0000_0000_0000 },
        .{ .name = "chronicle.lastIndexReplicated", .value = @bitCast(@as(i64, -1)) },
        .{ .name = "chronicle.lastAcknowledgedIndexReplicated", .value = @bitCast(@as(i64, -1)) },
    };

    for (data_fields) |df| {
        pad.qcStart(.data);
        pad.eventName(df.name);
        pad.uint64Aligned(df.value);
        pad.qcFinish();
    }

    // Write the entire pad to the file
    try file.writeAll(pad.bytes());
}
```

**Critical detail:** The `uint64Aligned` call must produce an 8-byte-aligned
value within the file so that when the file is later mmap'd, the kernel can
serve atomic loads/stores on those addresses. The C `wirepad_uint64_aligned`
function pads the current position to an 8-byte boundary, writes the `0xA7`
INT64 control byte followed by the 8-byte little-endian value. Our Zig
`WirePad` must reproduce this exact layout for interop with Java and with
readers of the shared memory.

---

## 8. Directory Listing Reopen

`directory_listing_reopen` (lines 1478-1498) opens and mmaps the directory
listing file, then parses it to locate the pointers to the three key fields
(`highest_cycle`, `lowest_cycle`, `modcount`) within the mmap region.

```zig
const DirlistMode = enum { read_only, read_write };

fn directoryListingReopen(self: *Queue, mode: DirlistMode) !void {
    const dirlist_name = self.dirlist_name orelse return error.DirlistNameNotSet;

    // Close previous mapping if any
    if (self.dirlist_map) |old_map| {
        std.posix.munmap(old_map);
        self.dirlist_map = null;
    }
    if (self.dirlist_fd) |old_fd| {
        std.posix.close(old_fd);
        self.dirlist_fd = null;
    }

    // Open with appropriate flags
    const open_flags: std.fs.File.OpenFlags = switch (mode) {
        .read_only => .{},
        .read_write => .{ .mode = .read_write },
    };
    const file = try std.fs.cwd().openFile(dirlist_name, open_flags);
    self.dirlist_fd = file.handle;
    // Note: we do NOT defer file.close() — we keep the fd alive for the mmap

    const stat = try file.stat();
    const prot: u32 = switch (mode) {
        .read_only => std.posix.PROT.READ,
        .read_write => std.posix.PROT.READ | std.posix.PROT.WRITE,
    };

    self.dirlist_map = try std.posix.mmap(
        null,
        stat.size,
        prot,
        .{ .TYPE = .SHARED },
        file.handle,
        0,
    );

    // Parse the mapping to find field pointers
    self.parseDirlist();

    // Validate that all three required pointers were found
    if (self.dirlist_fields.highest_cycle == null or
        self.dirlist_fields.lowest_cycle == null or
        self.dirlist_fields.modcount == null)
    {
        return error.DirlistFieldParseFailed;
    }
}
```

### How `parseDirlist` finds field pointers

The directory listing is a sequence of Chronicle Queue messages. The wire
parser walks each data message looking for `event_name` values matching
`"listing.highestCycle"`, `"listing.lowestCycle"`, and `"listing.modCount"`.
When found, it stores a **pointer into the mmap** (not a copy of the value)
so that future reads see the live shared-memory value. The C does this via
the `ptr_uint64` callback in `wirecallbacks_t` (see `handle_dirlist_ptr`,
lines 691-702).

In Zig the parsing callback receives a slice into the mmap buffer. We
compute the pointer to the uint64 payload:

```zig
fn parseDirlist(self: *Queue) void {
    const map = self.dirlist_map orelse return;

    var base: usize = 0;
    var index: u64 = 0;

    while (base + 4 < map.len) {
        const header = std.mem.readInt(u32, map[base..][0..4], .little);

        if (header == HD_UNALLOCATED) break;

        const meta_bits = header & HD_MASK_META;
        const sz: usize = @intCast(header & HD_MASK_LENGTH);

        if (meta_bits == HD_EOF) break;

        if (meta_bits == HD_METADATA) {
            // Skip metadata messages (already parsed for roll config)
            base += 4 + sz;
            // v5 pad to 4-byte alignment
            if (self.version == .v5) {
                const pad4 = (-%: sz) & 0x03;
                base += pad4;
            }
            continue;
        }

        // Data message — look for known event names within the wire data
        if (base + 4 + sz <= map.len) {
            const payload = map[base + 4 .. base + 4 + sz];
            self.matchDirlistField(payload);
        }

        index += 1;
        base += 4 + sz;
        if (self.version == .v5) {
            const pad4 = (-%: sz) & 0x03;
            base += pad4;
        }
    }
}

fn matchDirlistField(self: *Queue, payload: []const u8) void {
    // The wire format for a data message is:
    //   B9 <len> <event_name_bytes>   — EVENT_NAME control byte
    //   A7 <8 bytes little-endian>    — INT64 control byte
    // We search for the event name then extract the pointer to the uint64.
    // (A full wire parser would handle this generically; this is a focused
    //  extractor for the three fields we need.)

    const event_name = extractEventName(payload) orelse return;
    const uint64_ptr = findAlignedUint64(payload) orelse return;

    if (std.mem.eql(u8, event_name, "listing.highestCycle")) {
        self.dirlist_fields.highest_cycle = uint64_ptr;
    } else if (std.mem.eql(u8, event_name, "listing.lowestCycle")) {
        self.dirlist_fields.lowest_cycle = uint64_ptr;
    } else if (std.mem.eql(u8, event_name, "listing.modCount")) {
        self.dirlist_fields.modcount = uint64_ptr;
    }
}
```

The actual pointer is into the **live mmap** — any writer that updates the
field via the mmap will be visible to this reader, enabling the real-time
shared-memory IPC protocol.

---

## 9. Queue File Init

`queuefile_init` (lines 1370-1398) creates a new `.cq4` file and extends it
to `qf_disk_sz` (83,754,496 bytes) by seeking to the last byte and writing
a single byte. This pre-allocates disk space so that mmap can cover the
entire file.

```zig
fn queuefileInit(self: *Queue, path: []const u8) !void {
    const file = try std.fs.cwd().createFile(path, .{
        .mode = 0o777,
        .truncate = true,
        .read = true,
    });
    defer file.close();

    // Extend file to qf_disk_sz bytes by seeking and writing a single byte
    try file.seekTo(Queue.qf_disk_sz - 1);
    try file.writer().writeByte(0);

    // TODO: write queue file header (metadata with roll config)
    // TODO: write index2index structure
}
```

**Note:** The C code has two TODO comments here — the queuefile header and
index2index are not yet written. The Zig port should eventually write:
1. A metadata message with the roll configuration (matching what Java writes)
2. An index2index page for the indexing structure

For now, the file is created as a sparse allocation that will be populated
by the appender.

---

## 10. Queue Cleanup

`chronicle_cleanup` (lines 1326-1368) tears down everything:

1. Unlinks the queue from the global list
2. Closes all tailers (each: unmap, close fd, free)
3. Closes the appender (same as tailer)
4. Unmaps the directory listing
5. Closes the dirlist fd
6. Frees all owned strings
7. Frees the glob results
8. Frees the queue struct itself

In Zig, we use `deinit` and lean on the allocator:

```zig
pub fn deinit(self: *Queue) void {
    // Close all tailers
    while (self.tailers.first) |node| {
        const tailer = &node.data;
        tailer.close(); // handles munmap, close(fd), unlink from list
    }

    // Close the appender
    if (self.appender) |app| {
        app.close();
        self.appender = null;
    }

    // Unmap directory listing
    if (self.dirlist_map) |map| {
        std.posix.munmap(map);
    }
    if (self.dirlist_fd) |fd| {
        std.posix.close(fd);
    }

    // Free all owned strings
    if (self.dirlist_name) |n| self.allocator.free(n);
    if (self.roll_format) |s| self.allocator.free(s);
    if (self.roll_name) |s| self.allocator.free(s);
    if (self.roll_strftime) |s| self.allocator.free(s);

    // Free globbed paths
    for (self.queuefile_paths.items) |p| self.allocator.free(p);
    self.queuefile_paths.deinit();

    // Free dirname and the queue struct itself
    self.allocator.free(self.dirname);
    self.allocator.destroy(self);
}
```

**Zig advantage:** By storing the allocator in the struct, `deinit` can free
everything without the caller needing to pass the allocator back. The
`errdefer` pattern in `init` / `open` ensures partial failures also clean up.

---

## 11. Global Queue Registry

The C code uses a global singly-linked list (`queue_head`) so that
`chronicle_peek()` can iterate all open queues. This is inherently
thread-unsafe and creates hidden global state.

In Zig, we replace this with an explicit `QueueRegistry`:

```zig
pub const QueueRegistry = struct {
    queues: std.ArrayList(*Queue),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) QueueRegistry {
        return .{
            .queues = std.ArrayList(*Queue).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *QueueRegistry) void {
        // Note: does NOT deinit the queues themselves — caller owns them
        self.queues.deinit();
    }

    pub fn register(self: *QueueRegistry, queue: *Queue) !void {
        try self.queues.append(queue);
    }

    pub fn unregister(self: *QueueRegistry, queue: *Queue) void {
        for (self.queues.items, 0..) |q, i| {
            if (q == queue) {
                _ = self.queues.orderedRemove(i);
                return;
            }
        }
    }

    /// Poll all registered queues (equivalent to C chronicle_peek)
    pub fn peekAll(self: *QueueRegistry) void {
        for (self.queues.items) |queue| {
            queue.peekQueue();
        }
    }
};
```

Usage pattern:

```zig
var registry = QueueRegistry.init(allocator);
defer registry.deinit();

var queue = try Queue.init(allocator, "/tmp/myqueue");
try registry.register(queue);
defer {
    registry.unregister(queue);
    queue.deinit();
}

try queue.open();

// Later, in event loop:
registry.peekAll();
```

This keeps the queue collection explicit and testable, with no hidden global
state. For single-queue programs that don't need a registry, the user can
simply call `queue.peekQueue()` directly.

---

## 12. Error Handling Strategy

The C library uses a single global error string:

```c
static const char* cerr_msg;
int chronicle_err(const char* msg) {
    printf("chronicle error: '%s\n", msg);
    cerr_msg = msg;
    return -1;
}
```

This has several problems:
- Not thread-safe
- Loses context from nested errors
- Caller must remember to check return value AND call `chronicle_strerror()`

In Zig, we use **error unions** — the canonical Zig error handling pattern:

```zig
pub const QueueError = error{
    // Init / open errors
    DirStatFailed,
    NotADirectory,
    QueueNotFoundNoCreate,
    VersionRequired,
    CreateRequiresEmptyDir,
    RollSchemeRequired,
    VersionMismatch,

    MissingRollFormat,
    MissingRollLength,
    MissingRollEpoch,
    UnknownRollScheme,
    InvalidDateFormat,

    // Directory listing errors
    DirlistNameNotSet,
    DirlistFieldParseFailed,
    DirlistOpenFailed,
    DirlistMmapFailed,

    // Queue file errors
    QueuefileCreateFailed,
    QueuefileLseekFailed,
    QueuefileWriteFailed,

    // General
    OutOfMemory,
    AllocFailed,
};
```

Functions return `QueueError!T` where `T` is the success type. The caller
can use `try` to propagate, `catch` to handle, or `catch |err| switch (err)`
for exhaustive matching.

For diagnostic logging (replacing the `printf` calls), we use `std.log`:

```zig
const log = std.log.scoped(.chronicle);

// In functions:
log.info("opening dir {s}", .{self.dirname});
log.err("dir stat failed for {s}", .{self.dirname});
```

This integrates with Zig's log level filtering — debug builds get verbose
output, release builds are quiet by default.

---

## 13. Roll Scheme Table

The C code defines ~27 roll schemes in a static array (lines 436-465). In
Zig, we define this as a comptime-known array of structs:

```zig
pub const RollScheme = struct {
    name: []const u8,
    format: []const u8,      // Java SimpleDateFormat
    roll_length_secs: u32,
    index_count: u32,
    index_spacing: u32,
};

pub const roll_schemes = [_]RollScheme{
    // ── In use by cq5 ─────────────────────────────────────────
    .{ .name = "FIVE_MINUTELY",        .format = "yyyyMMdd-HHmm'V'",    .roll_length_secs = 5 * 60,      .index_count = 2 << 10, .index_spacing = 256 },
    .{ .name = "TEN_MINUTELY",         .format = "yyyyMMdd-HHmm'X'",    .roll_length_secs = 10 * 60,     .index_count = 2 << 10, .index_spacing = 256 },
    .{ .name = "TWENTY_MINUTELY",      .format = "yyyyMMdd-HHmm'XX'",   .roll_length_secs = 20 * 60,     .index_count = 2 << 10, .index_spacing = 256 },
    .{ .name = "HALF_HOURLY",          .format = "yyyyMMdd-HHmm'H'",    .roll_length_secs = 30 * 60,     .index_count = 2 << 10, .index_spacing = 256 },
    .{ .name = "FAST_HOURLY",          .format = "yyyyMMdd-HH'F'",      .roll_length_secs = 60 * 60,     .index_count = 4 << 10, .index_spacing = 256 },
    .{ .name = "TWO_HOURLY",           .format = "yyyyMMdd-HH'II'",     .roll_length_secs = 2 * 60 * 60, .index_count = 4 << 10, .index_spacing = 256 },
    .{ .name = "FOUR_HOURLY",          .format = "yyyyMMdd-HH'IV'",     .roll_length_secs = 4 * 60 * 60, .index_count = 4 << 10, .index_spacing = 256 },
    .{ .name = "SIX_HOURLY",           .format = "yyyyMMdd-HH'VI'",     .roll_length_secs = 6 * 60 * 60, .index_count = 4 << 10, .index_spacing = 256 },
    .{ .name = "FAST_DAILY",           .format = "yyyyMMdd'F'",         .roll_length_secs = 24 * 60 * 60,.index_count = 4 << 10, .index_spacing = 256 },
    // ── Used historically by cq4 ──────────────────────────────
    .{ .name = "MINUTELY",             .format = "yyyyMMdd-HHmm",       .roll_length_secs = 60,          .index_count = 2 << 10, .index_spacing = 16 },
    .{ .name = "HOURLY",               .format = "yyyyMMdd-HH",         .roll_length_secs = 60 * 60,     .index_count = 4 << 10, .index_spacing = 16 },
    .{ .name = "DAILY",                .format = "yyyyMMdd",            .roll_length_secs = 24 * 60 * 60,.index_count = 8 << 10, .index_spacing = 64 },
    // ── Large rolls ───────────────────────────────────────────
    .{ .name = "LARGE_HOURLY",         .format = "yyyyMMdd-HH'L'",      .roll_length_secs = 60 * 60,     .index_count = 8 << 10, .index_spacing = 64 },
    .{ .name = "LARGE_DAILY",          .format = "yyyyMMdd'L'",         .roll_length_secs = 24 * 60 * 60,.index_count = 32 << 10,.index_spacing = 128 },
    .{ .name = "XLARGE_DAILY",         .format = "yyyyMMdd'X'",         .roll_length_secs = 24 * 60 * 60,.index_count = 32 << 10,.index_spacing = 256 },
    .{ .name = "HUGE_DAILY",           .format = "yyyyMMdd'H'",         .roll_length_secs = 24 * 60 * 60,.index_count = 32 << 10,.index_spacing = 1024 },
    // ── Test / benchmark ──────────────────────────────────────
    .{ .name = "SMALL_DAILY",          .format = "yyyyMMdd'S'",         .roll_length_secs = 24 * 60 * 60,.index_count = 8 << 10, .index_spacing = 8 },
    .{ .name = "LARGE_HOURLY_SPARSE",  .format = "yyyyMMdd-HH'LS'",    .roll_length_secs = 60 * 60,     .index_count = 4 << 10, .index_spacing = 1024 },
    .{ .name = "LARGE_HOURLY_XSPARSE", .format = "yyyyMMdd-HH'LX'",    .roll_length_secs = 60 * 60,     .index_count = 2 << 10, .index_spacing = 1 << 20 },
    .{ .name = "HUGE_DAILY_XSPARSE",   .format = "yyyyMMdd'HX'",       .roll_length_secs = 24 * 60 * 60,.index_count = 16 << 10,.index_spacing = 1 << 20 },
    .{ .name = "TEST_SECONDLY",        .format = "yyyyMMdd-HHmmss'T'",  .roll_length_secs = 1,           .index_count = 32 << 10,.index_spacing = 4 },
    .{ .name = "TEST4_SECONDLY",       .format = "yyyyMMdd-HHmmss'T4'", .roll_length_secs = 1,           .index_count = 32,      .index_spacing = 4 },
    .{ .name = "TEST_HOURLY",          .format = "yyyyMMdd-HH'T'",      .roll_length_secs = 60 * 60,     .index_count = 16,      .index_spacing = 4 },
    .{ .name = "TEST_DAILY",           .format = "yyyyMMdd'T1'",        .roll_length_secs = 24 * 60 * 60,.index_count = 8,       .index_spacing = 1 },
    .{ .name = "TEST2_DAILY",          .format = "yyyyMMdd'T2'",        .roll_length_secs = 24 * 60 * 60,.index_count = 16,      .index_spacing = 2 },
    .{ .name = "TEST4_DAILY",          .format = "yyyyMMdd'T4'",        .roll_length_secs = 24 * 60 * 60,.index_count = 32,      .index_spacing = 4 },
    .{ .name = "TEST8_DAILY",          .format = "yyyyMMdd'T8'",        .roll_length_secs = 24 * 60 * 60,.index_count = 128,     .index_spacing = 8 },
};
```

Since these are `comptime`-known, lookups can be done at compile time for
known scheme names, or at runtime via linear scan for user-supplied names.

---

## 14. Testing Notes

### Unit tests for lifecycle

```zig
const testing = std.testing;

test "Queue init and deinit" {
    const queue = try Queue.init(testing.allocator, "/tmp/test_chronicle");
    defer queue.deinit();

    try testing.expectEqual(@as(u32, 1 << 20), queue.blocksize);
    try testing.expectEqual(Version.unknown, queue.version);
    try testing.expectEqualStrings("/tmp/test_chronicle", queue.dirname);
}

test "Java date format conversion" {
    const result = try convertJavaDateFormat(testing.allocator, "yyyyMMdd-HH'F'");
    defer testing.allocator.free(result);
    try testing.expectEqualStrings("%Y%m%d-%HF", result);
}

test "Roll scheme lookup" {
    const queue = try Queue.init(testing.allocator, "/tmp/test");
    defer queue.deinit();

    try queue.setRollScheme("FAST_HOURLY");
    try testing.expectEqualStrings("FAST_HOURLY", queue.roll_name.?);
    try testing.expectEqual(@as(i32, 3_600_000), queue.roll_length_ms);
}
```

### Integration tests

- Create a temporary directory, init a v5 queue with create, open it, verify
  `metadata.cq4t` exists and can be parsed.
- Open a queue directory created by Java Chronicle Queue, verify version
  detection and roll config reading.
- Test cleanup: verify no leaked file descriptors (check `/proc/self/fd`).

### Memory leak detection

Zig's `testing.allocator` tracks all allocations and fails the test if any
are leaked. This replaces the need for Valgrind in most cases.

---

## Summary of C → Zig Mapping

| C function | Zig method | Key changes |
|---|---|---|
| `chronicle_init` | `Queue.init` | `errdefer` for partial failures |
| `chronicle_open` | `Queue.open` | Error union instead of -1 return |
| `chronicle_cleanup` | `Queue.deinit` | Allocator-based, no global list walk |
| `chronicle_set_version` | `Queue.setVersion` | Enum instead of int |
| `chronicle_set_roll_scheme` | `Queue.setRollScheme` | Returns `error.UnknownRollScheme` |
| `chronicle_set_create` | `Queue.setCreate` | `bool` instead of `int` |
| `chronicle_set_encoder` | `Queue.setEncoder` | Typed function pointers |
| `chronicle_set_decoder` | `Queue.setDecoder` | Typed function pointers |
| `chronicle_version_detect` | `detectVersion` | Uses `std.fs.Dir.openFile` |
| `directory_listing_init` | `Queue.directoryListingInit` | `WirePad` builder |
| `directory_listing_reopen` | `Queue.directoryListingReopen` | `std.posix.mmap` |
| `queuefile_init` | `Queue.queuefileInit` | `std.fs.File` seek + write |
| `chronicle_apply_roll_scheme` | `Queue.applyRollScheme` | Pure Zig string conversion |
| `chronicle_get_cycle_fn` | `Queue.getCycleFn` | `std.fmt.allocPrint` |
| `chronicle_err` / `chronicle_strerror` | Error unions | No global state |
| Global `queue_head` list | `QueueRegistry` | Explicit, testable |