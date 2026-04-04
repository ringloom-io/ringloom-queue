# Task 4: Queue Lifecycle Management

## Table of Contents

1. [Overview](#1-overview)
2. [Queue Struct Design](#2-queue-struct-design)
3. [Version Detection](#3-version-detection)
4. [Queue Opening](#4-queue-opening)
5. [Metadata File Creation](#5-metadata-file-creation)
6. [Queue File Creation](#6-queue-file-creation)
7. [Pre-Roll File Creation](#7-pre-roll-file-creation)
8. [io_uring Setup](#8-io_uring-setup)
9. [Queue File Enumeration](#9-queue-file-enumeration)
10. [Configuration](#10-configuration)
11. [Queue Cleanup (deinit)](#11-queue-cleanup-deinit)
12. [Error Handling](#12-error-handling)
13. [Testing Strategy](#13-testing-strategy)

---

## 1. Overview

Queue lifecycle covers the full sequence of operations from creating or opening
a queue through to tearing it down:

- **Initialization** — allocate the `Queue` struct, set defaults
- **Version detection** — probe for `metadata.brz` to determine format version
- **Metadata file creation/reading** — create or mmap the fixed 512-byte
  `SharedMetadata` extern struct (zero parsing — direct pointer cast)
- **Queue file management** — create `.brz` data files with `fallocate(2)`
  pre-allocation, write the 64-byte `QueueFileHeader`
- **Roll scheme configuration** — set roll period, index count, index spacing
- **Pre-roll file preparation** — pre-create the next cycle's `.brz` file
  within a configurable `preroll_ms` window so the hot-path roll is a pointer
  swap with zero syscalls
- **io_uring setup** — initialize an io_uring context and eventfd for
  writer→reader notification
- **Cleanup** — tear down all mappings, fds, and io_uring state in reverse
  order

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
    cycle_shift: u6 = 40,
    seqnum_mask: u64 = 0xFF_FFFF_FFFF,

    // --- Pre-roll state (NEW) ---
    preroll_fd: ?posix.fd_t = null,
    preroll_mmap: ?[]align(std.mem.page_size) u8 = null,
    preroll_cycle: ?u32 = null,
    preroll_ms: u64 = 500, // pre-roll window: 500 ms before cycle end

    // --- io_uring (NEW) ---
    uring_ctx: ?IoUringContext = null,
    eventfd: ?posix.fd_t = null,

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
            if (magic == 0x425A514D) return .v1;
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
existing data files, and initializes io_uring.

### Steps

1. Validate directory exists
2. Detect version from `metadata.brz`
3. Open and mmap `metadata.brz` → cast to `*SharedMetadata`
4. Validate magic number (`0x425A514D`)
5. Read roll config directly from struct fields (zero parsing!)
6. Scan directory for existing `.brz` data files
7. Initialize io_uring context and eventfd
8. Set up pre-roll state
9. Validate configuration consistency

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
    if (self.metadata.?.magic != 0x425A514D) {
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

    // 7. Initialize io_uring context and eventfd
    try self.initIoUring();

    // 8. Pre-roll state starts empty — will be filled by maybePreroll()
    self.preroll_fd = null;
    self.preroll_mmap = null;
    self.preroll_cycle = null;

    // 9. Validate configuration
    if (self.roll_length_secs == 0) return error.InvalidRollConfig;
    if (self.index_count == 0) return error.InvalidRollConfig;
}
```

The entire open sequence is: mmap → pointer cast → read struct fields. No
parsing, no deserialization, no offset scanning.

---

## 5. Metadata File Creation

When creating a new queue, the metadata file is a single 512-byte file with
all fields at known offsets.

### Steps

1. Create `metadata.brz` file
2. `fallocate` 512 bytes (one disk sector)
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

    // 2. Pre-allocate 512 bytes with fallocate(2)
    //    This reserves real disk blocks — no sparse file tricks
    try std.posix.fallocate(fd, .{}, 0, Queue.metadata_file_sz);

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
    meta.magic = 0x425A514D;
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
    @atomicStore(u64, &meta.write_lock, 0x8000000000000000, .release); // unlocked

    // Zero the reserved region (fallocate already zeroed, but be explicit)
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
2. `fallocate(fd, 0, 0, qf_disk_sz)` — pre-allocate disk blocks
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

    // 2. Pre-allocate with fallocate(2) — reserves real disk blocks
    try std.posix.fallocate(fd, .{}, 0, Queue.qf_disk_sz);

    // 3. mmap the header + index region
    const header_plus_index_sz = 64 + @as(u64, self.index_count) * 8;
    const map_sz = std.mem.alignForward(u64, header_plus_index_sz, std.mem.page_size);
    const buf = try mapFile(fd, 0, @intCast(map_sz), .read_write, .{ .TYPE = .SHARED });

    // 4. Cast to *QueueFileHeader and fill in fields
    const hdr: *QueueFileHeader = @ptrCast(@alignCast(buf.ptr));
    hdr.magic = 0x425A5143;     // "BZQC"
    hdr.version = 1;
    hdr.flags = 0;
    hdr.roll_length_secs = self.roll_length_secs;
    hdr.index_spacing = self.index_spacing;
    hdr.index_count = self.index_count;
    hdr.epoch_ms = self.roll_epoch;
    hdr.created_cycle = cycle;
    @memset(&hdr._reserved, 0);

    // 5. Zero-initialize the index region (offset 64 to 64 + index_count × 8)
    //    fallocate zeros the file, but be explicit for clarity
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
No `open()`, no `fallocate()`, no `mmap()` on the critical path.

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
            .{ .TYPE = .SHARED, .POPULATE = true },
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

## 8. io_uring Setup

io_uring provides an efficient notification mechanism for writer→reader
communication. Instead of polling, readers submit an io_uring SQE to wait on
an eventfd, and receive a completion when a message is available.

### Initialization

```zig
fn initIoUring(self: *Queue) !void {
    // Create eventfd for writer→reader signaling
    self.eventfd = try std.posix.eventfd(0, .{ .NONBLOCK = true });
    errdefer {
        std.posix.close(self.eventfd.?);
        self.eventfd = null;
    }

    // Initialize io_uring context
    self.uring_ctx = try IoUringContext.init(.{
        .queue_depth = 64,
        .flags = 0,
    });
}
```

### How it works

| Actor | Operation |
|-------|-----------|
| **Writer** | After publishing a message (release store on the 4-byte header), writes `1` to the eventfd |
| **Reader** | Submits an io_uring `POLL_ADD` SQE on the eventfd; gets a CQE when the eventfd becomes readable |

This avoids busy-spinning in readers when no messages are available. The
eventfd write is a single syscall; the reader's io_uring poll is batched and
kernel-mediated.

### IoUringContext struct

```zig
pub const IoUringContext = struct {
    ring: std.os.linux.IoUring,

    pub fn init(opts: struct {
        queue_depth: u13 = 64,
        flags: u32 = 0,
    }) !IoUringContext {
        const ring = try std.os.linux.IoUring.init(opts.queue_depth, opts.flags);
        return .{ .ring = ring };
    }

    pub fn deinit(self: *IoUringContext) void {
        self.ring.deinit();
    }

    pub fn submitPollEventfd(self: *IoUringContext, efd: posix.fd_t) !void {
        _ = try self.ring.poll_add(0, efd, std.os.linux.POLL.IN);
        _ = try self.ring.submit();
    }
};
```

---

## 9. Queue File Enumeration

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

## 10. Configuration

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

When enabled, mmap calls include `MAP_HUGETLB` for 2 MiB huge pages. Falls
back to 4 KiB pages if the kernel can't satisfy the request. Requires the
blocksize to be 2 MiB-aligned.

### setCreate

```zig
pub fn setCreate(self: *Self, permitted: bool) void {
    self.create_permitted = permitted;
}
```

---

## 11. Queue Cleanup (deinit)

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

    // 3. Cancel pending io_uring operations and tear down ring
    if (self.uring_ctx) |*ctx| {
        ctx.deinit();
        self.uring_ctx = null;
    }

    // 4. Close eventfd
    if (self.eventfd) |efd| {
        std.posix.close(efd);
        self.eventfd = null;
    }

    // 5. Unmap and close pre-roll fd/mmap if present
    if (self.preroll_mmap) |buf| {
        unmapFile(buf) catch {};
        self.preroll_mmap = null;
    }
    if (self.preroll_fd) |fd| {
        std.posix.close(fd);
        self.preroll_fd = null;
    }
    self.preroll_cycle = null;

    // 6. Unmap and close metadata
    if (self.metadata_mmap) |buf| {
        unmapFile(buf) catch {};
        self.metadata_mmap = null;
    }
    self.metadata = null;
    if (self.metadata_fd) |fd| {
        std.posix.close(fd);
        self.metadata_fd = null;
    }

    // 7. Free all allocated queue file path strings
    for (self.queuefile_paths.items) |p| {
        self.allocator.free(p);
    }
    self.queuefile_paths.deinit();

    // 8. Free dirname if owned
    self.allocator.free(self.dirname);
}
```

### Ordering rationale

| Step | Why this order |
|------|----------------|
| Tailers first | Tailers hold read-only mappings into data files; close before files |
| Appender second | Appender may hold write mappings; must flush before closing |
| io_uring third | Cancel in-flight SQEs before closing the fds they reference |
| eventfd fourth | After io_uring is down, no one polls it |
| Pre-roll fifth | Independent fd/mmap, safe to close after appender |
| Metadata sixth | Other components may reference metadata pointer; close last |
| Strings last | Path strings may be referenced by error messages during teardown |

---

## 12. Error Handling

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

    // fallocate errors
    FallocateFailed,
    DiskFull,

    // io_uring errors
    IoUringInitFailed,
    IoUringSubmitFailed,
    EventfdCreateFailed,

    // Pre-roll errors
    PrerollFailed,

    // Lock errors
    WriteLockContention,
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

## 13. Testing Strategy

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
    try testing.expectEqual(@as(u32, 0x425A514D), q.metadata.?.magic);
    try testing.expectEqual(@as(u16, 1), q.metadata.?.version);
    try testing.expectEqual(@as(u32, 86400), q.metadata.?.roll_length_secs);
    try testing.expectEqual(@as(u32, 4096), q.metadata.?.index_count);
    try testing.expectEqual(@as(u32, 256), q.metadata.?.index_spacing);
}

test "queue file creation with fallocate" {
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
    std.mem.writeInt(u32, &buf, 0x425A514D, .little);
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
