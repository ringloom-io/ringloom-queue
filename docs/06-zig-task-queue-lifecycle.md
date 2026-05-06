# Task 4: Queue Lifecycle Management

## Table of Contents

1. [Overview](#1-overview)
2. [Queue Struct Design](#2-queue-struct-design)
3. [Version Detection](#3-version-detection)
4. [Queue Opening](#4-queue-opening)
5. [Metadata File Creation](#5-metadata-file-creation)
6. [Queue File Creation](#6-queue-file-creation)
7. [Pre-Roll File Creation](#7-pre-roll-file-creation)
8. [Prefetcher Lifecycle](#8-prefetcher-lifecycle)
9. [Cleaner Lifecycle](#9-cleaner-lifecycle)
10. [Pollable Maintenance and Platform Setup](#10-pollable-maintenance-and-platform-setup)
11. [Queue File Enumeration](#11-queue-file-enumeration)
12. [Configuration](#12-configuration)
13. [Queue Cleanup (deinit)](#13-queue-cleanup-deinit)
14. [Error Handling](#14-error-handling)
15. [Testing Strategy](#15-testing-strategy)

---

## 1. Overview

Queue lifecycle covers the full sequence of operations from creating or opening
a queue through to tearing it down:

- **Initialization** — allocate the `Queue` struct, set defaults
- **Version detection** — probe for `metadata.brz` to determine format version
- **Metadata file creation/reading** — create or mmap the fixed 512-byte
  `SharedMetadata` extern struct (zero parsing — direct pointer cast)
- **Queue file management** — create `.brz` data files with platform
  preallocation, write the 64-byte `QueueFileHeader`
- **Roll scheme configuration** — set roll period, index count, index spacing
- **Pre-roll file preparation** — pre-create the next cycle's `.brz` file
  within a configurable `preroll_ms` window so the hot-path roll is a pointer
  swap with zero syscalls
- **Pollable helpers** — initialize prefetcher/cleaner state machines; native Zig
  may wrap them in helper threads, while C ABI users drive them manually
- **Cleanup** — tear down all mappings, fds, helper state, and platform
  resources in reverse order

The design is **single writer, multiple readers**. All metadata is accessed
through mmap'd `extern struct` pointers — there is no wire format, no parsing,
no serialization layer. Atomic fields use acquire/release ordering.

---

## 2. Queue Struct Design

### Queue struct

```zig
const std = @import("std");
const posix = std.posix;

pub const Queue = struct {
    const Self = @This();

    // --- Allocator ---
    allocator: std.mem.Allocator,

    // --- Directory ---
    dirname: []const u8,

    // --- Core config ---
    blocksize: u64 = 2 * 1024 * 1024, // 2 MiB default
    version: Version = .unknown,
    create_permitted: bool = false,

    // --- Shared metadata (mmap'd extern struct, direct pointer cast) ---
    metadata: ?*SharedMetadata = null,
    metadata_fd: ?posix.fd_t = null,
    metadata_mmap: ?[]align(std.mem.page_size) u8 = null,

    // --- Queue data files ---
    queuefile_paths: std.ArrayList([]const u8),
    highest_cycle: u64 = 0,
    lowest_cycle: u64 = 0,
    modcount: u64 = 0,

    // --- Roll configuration (copied from metadata on open) ---
    roll_length_ms: u64 = 86_400_000, // 24 hours default
    roll_length_secs: u32 = 86_400,
    roll_epoch: u64 = 0,
    roll_format: []const u8 = "yyyyMMdd'F'",
    roll_name: []const u8 = "FAST_DAILY",
    roll_strftime: []const u8 = "%Y%m%dF",

    // --- Index config ---
    index_count: u32 = 4096,
    index_spacing: u32 = 256,

    // --- Cycle / seqnum bit layout ---
    cycle_shift: u6 = 32,
    seqnum_mask: u64 = 0x00000000FFFFFFFF,

    // --- Pre-roll state (NEW) ---
    preroll_fd: ?posix.fd_t = null,
    preroll_mmap: ?[]align(std.mem.page_size) u8 = null,
    preroll_cycle: ?u32 = null,
    preroll_ms: u64 = 500, // pre-roll window: 500 ms before cycle end

    // --- Latency helpers ---
    enable_prefetcher: bool = true,
    prefetch_runway_bytes: u64 = 8 * 1024 * 1024,
    read_prefetch_runway_bytes: u64 = 4 * 1024 * 1024,
    enable_cleaner: bool = true,
    spawn_helper_threads: bool = true, // forced false for C ABI builds/opens
    retention_cycles: ?u32 = null,
    cleaner: ?Cleaner = null,
    prefetcher: ?Prefetcher = null,
    platform: Platform = .detect(),

    // --- Huge page config (NEW) ---
    use_huge_pages: bool = false,

    // --- Active components ---
    tailers: std.ArrayList(*Tailer),
    appender: ?*Appender = null,

    // --- Constants ---
    pub const qf_disk_sz: u64 = 83_754_496; // ~79.9 MiB pre-allocation
    pub const metadata_file_sz: u64 = 512;   // one disk sector
};
```

### Design notes

The metadata pointer gives zero-cost field access — no parsing, no
deserialization, no offset calculations:

```zig
// Direct struct field access through mmap'd pointer
const cycle = @atomicLoad(u64, &self.metadata.?.highest_cycle, .acquire);
```

### Version enum

```zig
pub const Version = enum(u8) {
    unknown = 0,
    v1 = 1, // brz-queue native format (.brz files, extern struct metadata)
};
```

Only two variants. `unknown` means no queue detected at the directory path.
`v1` is the brz-queue native format.

---

## 3. Version Detection

Version detection probes the queue directory for the `metadata.brz` file. If
present, optionally validate the magic number to confirm it is a valid
brz-queue metadata file.

```zig
fn detectVersion(dirname: []const u8) !Version {
    var dir = try std.fs.cwd().openDir(dirname, .{});
    defer dir.close();

    if (dir.openFile("metadata.brz", .{})) |f| {
        defer f.close();

        // Optionally read and validate magic number
        var magic_buf: [4]u8 = undefined;
        const bytes_read = try f.readAll(&magic_buf);
        if (bytes_read == 4) {
            const magic = std.mem.readInt(u32, &magic_buf, .little);
            if (magic == 0x4D515A42) return .v1;
        }
        return .unknown; // file exists but magic doesn't match
    } else |_| {}

    return .unknown;
}
```

If `metadata.brz` does not exist, the directory is not a brz-queue.

---

## 4. Queue Opening

Opening an existing queue reads and validates the metadata file, scans for
existing data files, and initializes platform/helper state.

### Steps

1. Validate directory exists
2. Detect version from `metadata.brz`
3. Open and mmap `metadata.brz` → cast to `*SharedMetadata`
4. Validate magic number (`0x4D515A42`)
5. Read roll config directly from struct fields (zero parsing!)
6. Scan directory for existing `.brz` data files
7. Detect platform capabilities and initialize prefetcher/cleaner state machines
8. Set up pre-roll state
9. Start helper threads only for the native Zig API when configured
10. Validate configuration consistency

### Zig implementation sketch

```zig
pub fn open(self: *Self) !void {
    // 1. Validate directory exists
    var dir = std.fs.cwd().openDir(self.dirname, .{}) catch |err| {
        if (err == error.FileNotFound and self.create_permitted) {
            try std.fs.cwd().makePath(self.dirname);
            return try self.createNew();
        }
        return err;
    };
    defer dir.close();

    // 2. Detect version
    self.version = try detectVersion(self.dirname);

    if (self.version == .unknown) {
        if (self.create_permitted) {
            return try self.createNew();
        }
        return error.QueueNotFound;
    }

    // 3. Open and mmap metadata.brz
    const meta_path = try std.fs.path.join(self.allocator, &.{ self.dirname, "metadata.brz" });
    defer self.allocator.free(meta_path);

    self.metadata_fd = try std.posix.open(
        meta_path,
        .{ .ACCMODE = .RDWR },
        0,
    );
    self.metadata_mmap = try mapFile(
        self.metadata_fd.?,
        0,
        @intCast(Queue.metadata_file_sz),
        .read_write,
        .{ .TYPE = .SHARED },
    );

    // Cast to *SharedMetadata — zero parsing
    self.metadata = @ptrCast(@alignCast(self.metadata_mmap.?.ptr));

    // 4. Validate magic
    if (self.metadata.?.magic != 0x4D515A42) {
        return error.InvalidMagic;
    }

    // 5. Read roll config directly from struct fields
    self.roll_length_secs = self.metadata.?.roll_length_secs;
    self.roll_length_ms = @as(u64, self.roll_length_secs) * 1000;
    self.index_count = self.metadata.?.index_count;
    self.index_spacing = self.metadata.?.index_spacing;
    self.roll_epoch = self.metadata.?.epoch_ms;

    // Read atomic fields
    self.highest_cycle = @atomicLoad(u64, &self.metadata.?.highest_cycle, .acquire);
    self.lowest_cycle = @atomicLoad(u64, &self.metadata.?.lowest_cycle, .acquire);
    self.modcount = @atomicLoad(u64, &self.metadata.?.modcount, .acquire);

    // 6. Scan directory for existing .brz data files
    try self.refreshQueueFiles();

    // 7. Initialize platform and pollable helper state
    self.platform = Platform.detect();
    try self.initPrefetcher();
    try self.initCleaner();

    // 8. Pre-roll state starts empty — will be filled by maybePreroll()
    self.preroll_fd = null;
    self.preroll_mmap = null;
    self.preroll_cycle = null;

    // 9. Optionally start native Zig helper threads
    if (self.spawn_helper_threads) {
        try self.startHelperThreads();
    }

    // 10. Validate configuration
    if (self.roll_length_secs == 0) return error.InvalidRollConfig;
    if (self.index_count == 0) return error.InvalidRollConfig;
}
```

The entire open sequence is: mmap → pointer cast → read struct fields. No
parsing, no deserialization, no offset scanning.

### Appender lease

Opening a queue does not acquire writer ownership. The lease is acquired when an
appender is created and released when that appender closes:

```zig
pub fn acquireAppenderLease(self: *Self, owner_token: u64) !void {
    const prev = @cmpxchgStrong(
        u64,
        &self.metadata.?.appender_lock,
        0,
        owner_token,
        .acquire,
        .monotonic,
    );
    if (prev != null) return error.AppenderAlreadyOpen;
}

pub fn releaseAppenderLease(self: *Self, owner_token: u64) !void {
    const prev = @cmpxchgStrong(
        u64,
        &self.metadata.?.appender_lock,
        owner_token,
        0,
        .release,
        .monotonic,
    );
    if (prev != null) return error.AppenderLeaseLost;
}
```

The lease CAS happens outside the append hot path. It prevents multiple appenders
from corrupting `write_position` without adding per-message synchronization.

---

## 5. Metadata File Creation

When creating a new queue, the metadata file is a single 512-byte file with
all fields at known offsets.

### Steps

1. Create `metadata.brz` file
2. Preallocate 512 bytes (one disk sector)
3. mmap it read-write
4. Cast to `*SharedMetadata`, fill in fields
5. `msync` to flush

### Zig implementation sketch

```zig
fn metadataInit(self: *Self) !void {
    const meta_path = try std.fs.path.join(
        self.allocator,
        &.{ self.dirname, "metadata.brz" },
    );
    defer self.allocator.free(meta_path);

    // 1. Create the file
    const fd = try std.posix.open(
        meta_path,
        .{ .ACCMODE = .RDWR, .CREAT = true, .EXCL = true },
        0o644,
    );
    errdefer std.posix.close(fd);

    // 2. Pre-allocate 512 bytes with the platform implementation.
    //    Linux uses fallocate; macOS uses F_PREALLOCATE + ftruncate.
    try self.platform.preallocate(fd, 0, Queue.metadata_file_sz);
    try std.posix.ftruncate(fd, Queue.metadata_file_sz);

    // 3. mmap read-write
    const mmap_buf = try mapFile(
        fd,
        0,
        @intCast(Queue.metadata_file_sz),
        .read_write,
        .{ .TYPE = .SHARED },
    );

    // 4. Cast to *SharedMetadata and fill in fields
    const meta: *SharedMetadata = @ptrCast(@alignCast(mmap_buf.ptr));
    meta.magic = 0x4D515A42;
    meta.version = 1;
    meta.flags = 0;
    meta.roll_length_secs = self.roll_length_secs;
    meta.index_spacing = self.index_spacing;
    meta.index_count = self.index_count;
    meta.epoch_ms = self.roll_epoch;

    // Atomic fields — use release stores so readers see consistent state
    @atomicStore(u64, &meta.highest_cycle, 0, .release);
    @atomicStore(u64, &meta.lowest_cycle, 0, .release);
    @atomicStore(u64, &meta.modcount, 0, .release);
    @atomicStore(u64, &meta.write_position, 0, .release);
    @atomicStore(u64, &meta.appender_lock, 0, .release); // unlocked

    // Zero the reserved region (preallocation should zero, but be explicit)
    @memset(&meta._reserved, 0);

    // 5. msync to flush to disk
    try std.posix.msync(@alignCast(mmap_buf), .{ .SYNC = true });

    // Store in Queue struct
    self.metadata_fd = fd;
    self.metadata_mmap = mmap_buf;
    self.metadata = meta;
}
```

### Design properties

- **512 bytes, fixed layout** — one extern struct, one disk sector
- **No encoding step** — just write struct fields directly
- **No alignment calculations** — the compiler handles it via `align(8)`
- **Atomic-ready** — fields are at known, aligned offsets from the start

---

## 6. Queue File Creation

Each `.brz` data file stores messages for one cycle. The file layout is:

```
[QueueFileHeader: 64 bytes] [Index: index_count × 8 bytes] [Data region...]
```

### Steps

1. Create the file
2. `platform.preallocate(fd, 0, qf_disk_sz)` — pre-allocate disk blocks
3. mmap the first block
4. Cast offset 0 to `*QueueFileHeader`, fill in fields
5. Zero-initialize the index region
6. Data region starts after the index

### Zig implementation sketch

```zig
fn queuefileInit(self: *Self, path: []const u8, cycle: u32) !posix.fd_t {
    // 1. Create the file
    const fd = try std.posix.open(
        path,
        .{ .ACCMODE = .RDWR, .CREAT = true, .EXCL = true },
        0o644,
    );
    errdefer std.posix.close(fd);

    // 2. Pre-allocate with the platform implementation — reserves real disk blocks
    try self.platform.preallocate(fd, 0, Queue.qf_disk_sz);
    try std.posix.ftruncate(fd, Queue.qf_disk_sz);

    // 3. mmap the header + index region
    const header_plus_index_sz = 64 + @as(u64, self.index_count) * 8;
    const map_sz = std.mem.alignForward(u64, header_plus_index_sz, std.mem.page_size);
    const buf = try mapFile(fd, 0, @intCast(map_sz), .read_write, .{ .TYPE = .SHARED });

    // 4. Cast to *QueueFileHeader and fill in fields
    const hdr: *QueueFileHeader = @ptrCast(@alignCast(buf.ptr));
    hdr.magic = 0x43515A42;     // "BZQC"
    hdr.version = 1;
    hdr.flags = 0;
    hdr.roll_length_secs = self.roll_length_secs;
    hdr.index_spacing = self.index_spacing;
    hdr.index_count = self.index_count;
    hdr.epoch_ms = self.roll_epoch;
    hdr.created_cycle = cycle;
    @memset(&hdr._reserved, 0);

    // 5. Zero-initialize the index region (offset 64 to 64 + index_count × 8)
    //    Preallocation should zero the file, but be explicit for clarity
    const index_base = buf.ptr + 64;
    const index_byte_len = @as(usize, self.index_count) * 8;
    @memset(index_base[0..index_byte_len], 0);

    // 6. Flush header + index to disk
    try std.posix.msync(@alignCast(buf), .{ .SYNC = true });

    // Unmap the initialization mapping (appender will map its own window)
    try unmapFile(buf);

    return fd;
}
```

### Data region

The data region starts at offset `64 + index_count × 8`. For the default
`FAST_DAILY` scheme (`index_count = 4096`), the data region starts at
offset `64 + 4096 × 8 = 32,832` (32 KiB + 64 bytes).

---

## 7. Pre-Roll File Creation

Pre-rolling creates the next cycle's `.brz` file **before** the current cycle
ends, so that the actual roll on the hot path is a pointer swap — zero syscalls.

### Concept

The appender periodically calls `maybePreroll()`. If the current time is within
`preroll_ms` milliseconds of the cycle boundary, and no pre-roll file exists
yet, create and mmap the next cycle's file.

When the actual roll happens (in the appender's write path), if
`preroll_cycle` matches the new cycle, just swap the fd and mmap pointers.
No `open()`, no preallocation, no `mmap()` on the critical path.

### Zig implementation sketch

```zig
pub fn maybePreroll(self: *Queue, current_time_ms: u64) !void {
    const roll_length_ms = @as(u64, self.roll_length_secs) * 1000;
    const current_cycle: u32 = @intCast(
        (current_time_ms - self.roll_epoch) / roll_length_ms,
    );
    const next_cycle = current_cycle + 1;
    const cycle_end_ms = (@as(u64, next_cycle)) * roll_length_ms + self.roll_epoch;

    // Are we within the pre-roll window?
    if (cycle_end_ms - current_time_ms < self.preroll_ms and
        self.preroll_cycle == null)
    {
        // Pre-create next cycle file
        const path = try self.cyclePath(next_cycle);
        defer self.allocator.free(path);

        const fd = try self.queuefileInit(path, next_cycle);

        self.preroll_cycle = next_cycle;
        self.preroll_fd = fd;

        // Pre-mmap for instant swap on roll
        const map_sz = std.mem.alignForward(u64, self.blocksize * 2, std.mem.page_size);
        self.preroll_mmap = try mapFile(
            fd,
            0,
            @intCast(map_sz),
            .read_write,
            .{ .TYPE = .SHARED, .POPULATE = self.platform.supports_map_populate },
        );
    }
}
```

### Roll swap (called from appender)

```zig
fn swapPrerolledFile(self: *Queue, new_cycle: u32) !void {
    if (self.preroll_cycle != null and self.preroll_cycle.? == new_cycle) {
        // Hot path: just swap pointers — zero syscalls
        // The appender takes ownership of preroll_fd and preroll_mmap
        self.appender.?.current_fd = self.preroll_fd.?;
        self.appender.?.current_mmap = self.preroll_mmap.?;

        // Clear pre-roll state
        self.preroll_fd = null;
        self.preroll_mmap = null;
        self.preroll_cycle = null;
    } else {
        // Pre-roll missed or wasn't applicable — fall back to synchronous creation
        const path = try self.cyclePath(new_cycle);
        defer self.allocator.free(path);
        _ = try self.queuefileInit(path, new_cycle);
        // Appender opens and maps normally
    }
}
```

### Timing

| Parameter | Default | Description |
|-----------|---------|-------------|
| `preroll_ms` | 500 | Milliseconds before cycle end to trigger pre-roll |

For the default `FAST_DAILY` scheme (24-hour cycles), pre-roll happens 500 ms
before midnight UTC. For `TEST_SECONDLY` (1-second cycles), 500 ms means
pre-roll starts halfway through every cycle.

---

## 8. Prefetcher Lifecycle

The prefetcher is the component that keeps page faults out of the appender and
tailer hot paths. It is optional for simple deployments, but enabled by default
in the low-jitter native Zig profile. The prefetcher is a pollable state machine:
native Zig may run it in a helper thread, while the C ABI exposes only bounded
`poll` calls so the embedding application owns scheduling.

### Responsibilities

1. Watch `metadata.write_position` and the appender's current cycle.
2. Keep at least `prefetch_runway_bytes` of writable, pre-touched space ahead of
   the appender.
3. Extend files with platform preallocation before the appender reaches the
   extension threshold.
4. Map the next appender window and populate writable PTEs using
   `MADV_POPULATE_WRITE` when available or manual write-touch otherwise.
5. Pre-create, preallocate, map, and touch the next cycle file inside the
   pre-roll window.
6. Publish a ready window/cycle through a single-producer/single-consumer handoff
   slot. The appender may swap the ready mapping without blocking.
7. Track registered local tailer positions and prepare read-only future windows
   using read-ahead hints and optional read-touch.
8. Bound read prefetch by acquire-loaded published write positions and published
   cycle headers; never prefetch unwritten future space.

The prefetcher may map and touch future regions, but it must never mutate the
appender's current mapping or write at/behind the appender's claimed write
position.

```zig
pub const Prefetcher = struct {
    queue: *Queue,
    stop: std.atomic.Value(bool) = .init(false),
    ready_write_window: SpscHandoff(MappedWindow) = .{},
    ready_read_windows: ReadPrefetchRegistry = .{},

    pub fn poll(self: *Prefetcher, max_work_units: u32) !StepResult {
        const pos = @atomicLoad(u64, &self.queue.metadata.?.write_position, .acquire);
        try self.ensureWriteRunway(pos, max_work_units);
        try self.ensureReadRunways(pos, max_work_units);
        try self.maybePreroll(self.queue.clockMs(), max_work_units);
        return self.nextStepResult();
    }

    pub fn run(self: *Prefetcher) !void {
        while (!self.stop.load(.acquire)) {
            _ = try self.poll(1024);
            std.time.sleep(50 * std.time.ns_per_us);
        }
    }
};
```

If the prefetcher falls behind, the appender can synchronously prepare the
required window as a correctness fallback. That path should increment a
diagnostic counter such as `prefetch_miss_count` so benchmarks can prove the hot
path stayed on the intended profile.

For C ABI use, `run()` is not linked or not reachable; only `poll()` is exported.
Each poll call must be bounded by `max_work_units` or a deadline so an embedding
event loop cannot be stalled by a large manual page-touch operation.

---

## 9. Cleaner Lifecycle

A cleaner is useful, but it is not part of message publication. Its job is to
make the system more predictable by moving resource cleanup, page-cache hints,
and retention work away from the appender. Like the prefetcher, it is a pollable
state machine; native Zig may run it in a helper thread, while C ABI users call
its bounded poll function from application-owned threads.

### Responsibilities

1. Unmap old windows after the appender/tailers have moved past them.
2. Close file descriptors no longer used by local components.
3. Call `madvise(MADV_DONTNEED)` on stale local mappings and
   `posix_fadvise(POSIX_FADV_DONTNEED)` on old file ranges when replay is not
   expected soon.
4. Apply configured retention by deleting cycle files older than the retention
   floor.
5. Optionally perform durability work (`sync_file_range`/`fdatasync`) if the
   queue is configured for stronger persistence.

The cleaner must be conservative. It should never reclaim the current appender
window, a prefetched handoff window, or any region still needed by local tailers.
Because cross-process tailers are not centrally registered, deletion should be
based on explicit retention policy, not inferred from local tailer positions.

```zig
pub const Cleaner = struct {
    queue: *Queue,
    pending_unmaps: LockFreeQueue(MappedWindow),

    pub fn poll(self: *Cleaner, max_work_units: u32) !StepResult {
        self.reapDeferredUnmaps(max_work_units);
        self.dropColdPageCache(max_work_units);
        try self.applyRetention(max_work_units);
        return self.nextStepResult();
    }

    pub fn run(self: *Cleaner) !void {
        while (!self.queue.closing.load(.acquire)) {
            _ = try self.poll(1024);
            std.time.sleep(10 * std.time.ns_per_ms);
        }
    }
};
```

Cleaner work can improve tail latency indirectly: old mappings and page-cache
pressure are less likely to evict the appender's prepared runway, and slow
`munmap`, `close`, delete, or flush operations do not run on the appender thread.

---

## 10. Pollable Maintenance and Platform Setup

The queue core does not initialize a kernel notification backend. Instead, it
initializes:

1. A `Platform` capability record describing available preallocation,
   population, read-ahead, page-cache, huge-page, and locking primitives.
2. A `Prefetcher` state machine for write and read page preparation.
3. A `Cleaner` state machine for deferred unmap/page-cache/retention work.

```zig
pub const StepResult = enum(u8) {
    idle,
    made_progress,
    more_work,
};

fn initMaintenance(self: *Queue) !void {
    self.platform = Platform.detect();
    self.prefetcher = try Prefetcher.init(self, .{
        .write_runway_bytes = self.prefetch_runway_bytes,
        .read_runway_bytes = self.read_prefetch_runway_bytes,
    });
    self.cleaner = try Cleaner.init(self, .{
        .retention_cycles = self.retention_cycles,
    });
}
```

Native Zig may call `startHelperThreads()` after initialization when
`spawn_helper_threads` is true. The C ABI build/path must force
`spawn_helper_threads = false` and export only bounded polling functions. This is
preferably enforced structurally: `Prefetcher.poll()` and `Cleaner.poll()` are the
core implementation, while thread spawning is a Zig-only wrapper.

---

## 11. Queue File Enumeration

Scan the queue directory for `.brz` data files (excluding `metadata.brz`),
extract cycle numbers from filenames, and sort by cycle.

```zig
fn refreshQueueFiles(self: *Self) !void {
    // Free old paths
    for (self.queuefile_paths.items) |p| {
        self.allocator.free(p);
    }
    self.queuefile_paths.clearRetainingCapacity();

    var dir = try std.fs.cwd().openDir(self.dirname, .{ .iterate = true });
    defer dir.close();

    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .file) continue;
        const name = entry.name;

        // Skip metadata file
        if (std.mem.eql(u8, name, "metadata.brz")) continue;

        // Must end with .brz
        if (!std.mem.endsWith(u8, name, ".brz")) continue;

        const owned = try self.allocator.dupe(u8, name);
        try self.queuefile_paths.append(owned);
    }

    // Sort by cycle (lexicographic on date-based filenames works correctly)
    std.mem.sort([]const u8, self.queuefile_paths.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lessThan);
}
```

### Cycle extraction

The cycle number is computed from the filename's date/time components, not
stored in the filename itself. To get the cycle from a path:

```zig
fn cycleFromPath(self: *Self, filename: []const u8) !u32 {
    // Parse the strftime-formatted portion of the filename
    // e.g. "20250101F.brz" → parse with "%Y%m%dF" → epoch seconds → cycle
    const stem = filename[0 .. filename.len - 4]; // strip ".brz"
    const epoch_secs = try parseStrftime(stem, self.roll_strftime);
    const cycle = (epoch_secs * 1000 - self.roll_epoch) / self.roll_length_ms;
    return @intCast(cycle);
}
```

---

## 12. Configuration

All configuration setters must be called **before** `open()`. They set fields
on the `Queue` struct that are used during metadata creation (for new queues)
or validated against existing metadata (for existing queues).

### setRollScheme

```zig
pub fn setRollScheme(self: *Self, name: []const u8) !void {
    const scheme = findSchemeByName(name) orelse return error.UnknownRollScheme;
    self.roll_name = scheme.name;
    self.roll_format = scheme.format_str;
    self.roll_length_secs = scheme.roll_length_secs;
    self.roll_length_ms = @as(u64, scheme.roll_length_secs) * 1000;
    self.index_count = scheme.index_count;
    self.index_spacing = scheme.index_spacing;
    self.roll_strftime = try javaFormatToStrftime(self.allocator, scheme.format_str);
}
```

### setBlocksize

```zig
pub fn setBlocksize(self: *Self, blocksize: u64) void {
    self.blocksize = blocksize;
}
```

### setPrerollMs (NEW)

```zig
pub fn setPrerollMs(self: *Self, ms: u64) void {
    self.preroll_ms = ms;
}
```

Controls how far in advance of a cycle boundary the pre-roll file is created.
Set to `0` to disable pre-rolling entirely.

### setUseHugePages (NEW)

```zig
pub fn setUseHugePages(self: *Self, enabled: bool) void {
    self.use_huge_pages = enabled;
}
```

When enabled on Linux, mmap calls include `MAP_HUGETLB` for 2 MiB huge pages.
Falls back to regular pages if the kernel can't satisfy the request. Requires
the blocksize to be 2 MiB-aligned. This option is ignored on macOS.

### setPrefetcher (NEW)

```zig
pub fn setPrefetcher(self: *Self, enabled: bool, runway_bytes: u64) void {
    self.enable_prefetcher = enabled;
    self.prefetch_runway_bytes = runway_bytes;
}
```

The prefetcher should be enabled for the minimum-jitter profile. `runway_bytes`
must be large enough to cover the worst expected burst while the prefetcher is
scheduled out; 8-64 MiB is a reasonable starting range depending on throughput.

### setReadPrefetcher (NEW)

```zig
pub fn setReadPrefetcher(self: *Self, runway_bytes: u64) void {
    self.read_prefetch_runway_bytes = runway_bytes;
}
```

Read prefetch is bounded by published data and should be budgeted for fan-out
workloads. A value of `0` disables read-touching but still allows normal tailer
mapping and `MADV_SEQUENTIAL` hints.

### setHelperThreads (NEW)

```zig
pub fn setHelperThreads(self: *Self, enabled: bool) void {
    self.spawn_helper_threads = enabled;
}
```

Native Zig can enable helper threads for convenience. The C ABI path must force
this to `false`; embedding applications drive `prefetcher.poll()` and
`cleaner.poll()` themselves.

### setCleaner (NEW)

```zig
pub fn setCleaner(self: *Self, enabled: bool, retention_cycles: ?u32) void {
    self.enable_cleaner = enabled;
    self.retention_cycles = retention_cycles;
}
```

The cleaner reclaims local mappings/page-cache and optionally deletes old cycle
files according to retention. Set `retention_cycles = null` to keep all files and
only perform local resource cleanup.

### setCreate

```zig
pub fn setCreate(self: *Self, permitted: bool) void {
    self.create_permitted = permitted;
}
```

---

## 13. Queue Cleanup (deinit)

Teardown releases all resources in reverse order of acquisition. Every mmap
is unmapped, every fd is closed, every allocation is freed.

```zig
pub fn deinit(self: *Self) void {
    // 1. Close all tailers (munmap + close fd for each)
    for (self.tailers.items) |tailer| {
        tailer.deinit();
    }
    self.tailers.deinit();

    // 2. Close appender if present
    if (self.appender) |app| {
        app.deinit();
        self.appender = null;
    }

    // 3. Stop native helper threads if they were started, then deinit helper state
    self.stopHelperThreads();
    if (self.prefetcher) |*prefetcher| {
        prefetcher.deinit();
        self.prefetcher = null;
    }
    if (self.cleaner) |*cleaner| {
        cleaner.deinit();
        self.cleaner = null;
    }

    // 4. Unmap and close pre-roll fd/mmap if present
    if (self.preroll_mmap) |buf| {
        unmapFile(buf) catch {};
        self.preroll_mmap = null;
    }
    if (self.preroll_fd) |fd| {
        std.posix.close(fd);
        self.preroll_fd = null;
    }
    self.preroll_cycle = null;

    // 5. Unmap and close metadata
    if (self.metadata_mmap) |buf| {
        unmapFile(buf) catch {};
        self.metadata_mmap = null;
    }
    self.metadata = null;
    if (self.metadata_fd) |fd| {
        std.posix.close(fd);
        self.metadata_fd = null;
    }

    // 6. Free all allocated queue file path strings
    for (self.queuefile_paths.items) |p| {
        self.allocator.free(p);
    }
    self.queuefile_paths.deinit();

    // 7. Free dirname if owned
    self.allocator.free(self.dirname);
}
```

### Ordering rationale

| Step | Why this order |
|------|----------------|
| Tailers first | Tailers hold read-only mappings into data files; close before files |
| Appender second | Appender may hold write mappings; must flush before closing |
| Helpers third | Prefetcher/cleaner may hold mappings or fds; stop them before closing shared resources |
| Pre-roll fourth | Independent fd/mmap, safe to close after appender/helpers |
| Metadata fifth | Other components may reference metadata pointer; close last |
| Strings last | Path strings may be referenced by error messages during teardown |

---

## 14. Error Handling

### Error set

```zig
pub const QueueError = error{
    // Directory errors
    QueueNotFound,
    DirectoryNotFound,
    DirectoryCreateFailed,

    // Metadata errors
    InvalidMagic,
    InvalidVersion,
    MetadataCorrupt,
    MetadataCreateFailed,

    // Queue file errors
    QueueFileCreateFailed,
    QueueFileCorrupt,
    InvalidQueueFileHeader,

    // Configuration errors
    InvalidRollConfig,
    UnknownRollScheme,
    RollConfigMismatch,

    // mmap errors
    MmapFailed,
    MunmapFailed,
    MsyncFailed,

    // Preallocation/platform errors
    PreallocateFailed,
    PlatformCapabilityUnavailable,
    DiskFull,

    // Pre-roll errors
    PrerollFailed,

    // Lock errors
    AppenderAlreadyOpen,
    AppenderLeaseLost,
};
```

### Logging

```zig
const log = std.log.scoped(.brz_queue);

// In open():
log.info("opening queue at {s}", .{self.dirname});
log.debug("detected version: {}", .{self.version});
log.debug("roll scheme: {s} ({}s cycles, {} index entries)", .{
    self.roll_name, self.roll_length_secs, self.index_count,
});

// In metadataInit():
log.info("creating metadata.brz ({} bytes)", .{Queue.metadata_file_sz});

// In queuefileInit():
log.info("creating queue file: {s} (cycle {})", .{ path, cycle });

// In maybePreroll():
log.debug("pre-rolling cycle {} ({} ms before boundary)", .{
    next_cycle, cycle_end_ms - current_time_ms,
});
```

---

## 15. Testing Strategy

### Unit tests for lifecycle

```zig
const testing = std.testing;

test "metadata creation produces 512-byte file" {
    const tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var q = try Queue.init(testing.allocator, tmp.path);
    defer q.deinit();
    q.setCreate(true);

    try q.open();

    // Verify file size
    const stat = try tmp.dir.statFile("metadata.brz");
    try testing.expectEqual(@as(u64, 512), stat.size);
}

test "metadata read-back via pointer cast" {
    const tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var q = try Queue.init(testing.allocator, tmp.path);
    defer q.deinit();
    q.setCreate(true);

    try q.open();

    // Verify fields through the metadata pointer
    try testing.expectEqual(@as(u32, 0x4D515A42), q.metadata.?.magic);
    try testing.expectEqual(@as(u16, 1), q.metadata.?.version);
    try testing.expectEqual(@as(u32, 86400), q.metadata.?.roll_length_secs);
    try testing.expectEqual(@as(u32, 4096), q.metadata.?.index_count);
    try testing.expectEqual(@as(u32, 256), q.metadata.?.index_spacing);
}

test "queue file creation with platform preallocation" {
    const tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var q = try Queue.init(testing.allocator, tmp.path);
    defer q.deinit();
    q.setCreate(true);

    try q.open();

    const fd = try q.queuefileInit(
        try std.fs.path.join(testing.allocator, &.{ tmp.path, "20250101F.brz" }),
        0,
    );
    defer std.posix.close(fd);

    // Verify pre-allocated size
    const stat = try std.posix.fstat(fd);
    try testing.expect(stat.size >= Queue.qf_disk_sz);
}

test "pre-roll file creation timing" {
    const tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var q = try Queue.init(testing.allocator, tmp.path);
    defer q.deinit();
    q.setCreate(true);
    q.setPrerollMs(1000); // 1 second window

    try q.open();

    // Simulate time 500ms before cycle boundary
    const roll_ms = @as(u64, q.roll_length_secs) * 1000;
    const cycle_boundary = roll_ms; // end of cycle 0
    const fake_time = cycle_boundary - 500;

    try q.maybePreroll(fake_time);

    // Pre-roll should have been triggered (500 < 1000ms window)
    try testing.expect(q.preroll_cycle != null);
    try testing.expectEqual(@as(u32, 1), q.preroll_cycle.?);
}

test "cycle enumeration with .brz files" {
    const tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    // Create some fake .brz files
    _ = try tmp.dir.createFile("20250101F.brz", .{});
    _ = try tmp.dir.createFile("20250102F.brz", .{});
    _ = try tmp.dir.createFile("20250103F.brz", .{});
    _ = try tmp.dir.createFile("metadata.brz", .{});

    var q = try Queue.init(testing.allocator, tmp.path);
    defer q.deinit();

    try q.refreshQueueFiles();

    // Should find 3 data files (metadata.brz excluded)
    try testing.expectEqual(@as(usize, 3), q.queuefile_paths.items.len);

    // Should be sorted
    try testing.expect(std.mem.lessThan(u8, q.queuefile_paths.items[0], q.queuefile_paths.items[1]));
    try testing.expect(std.mem.lessThan(u8, q.queuefile_paths.items[1], q.queuefile_paths.items[2]));
}

test "version detection finds v1 from metadata.brz" {
    const tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    // Write a minimal valid metadata file
    const f = try tmp.dir.createFile("metadata.brz", .{});
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, 0x4D515A42, .little);
    _ = try f.write(&buf);
    f.close();

    const version = try detectVersion(tmp.path);
    try testing.expectEqual(Version.v1, version);
}
```

### Integration tests

- **Round-trip**: create queue → write message → read message → verify contents
- **Re-open**: create queue, close it, re-open → verify metadata survives
- **Pre-roll + roll**: write messages until cycle rolls → verify pre-rolled file
  is used and no syscalls happen during the roll
- **Multi-reader**: spawn multiple reader threads → verify all see the same
  messages in order

### Memory leak detection

All tests should run under Zig's `GeneralPurposeAllocator` with leak detection
enabled. The `deinit` path must free every allocation made during `init`/`open`:

```zig
test "no memory leaks on full lifecycle" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const check = gpa.deinit();
        if (check == .leak) @panic("memory leak detected");
    }
    const allocator = gpa.allocator();

    var q = try Queue.init(allocator, "/tmp/brz-queue-leak-test");
    defer q.deinit();
    q.setCreate(true);
    try q.open();

    // ... use queue ...
}
```
