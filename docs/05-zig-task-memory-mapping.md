# Task 3: Memory-Mapped File I/O and Atomic Operations

## Overview

Memory-mapped I/O is the core mechanism for brz-queue IPC. All data access —
reading, writing, metadata coordination — goes through mmap'd shared memory.
There are no read/write syscalls on the hot path, no serialization layers, no
allocations. A writer writes bytes into a mapped region; a reader sees those
bytes appear in its own address space, mediated only by the CPU's cache
coherence protocol and the kernel's page table.

This document specifies:

1. **mmap primitives** — `mapFile`, `unmapFile`, `remapFile`
2. **Sliding window strategy** — blocksize-aligned 2× windows with pre-mapping
3. **MAP_POPULATE** — pre-fault all pages on appender remap
4. **madvise(MADV_SEQUENTIAL)** — aggressive read-ahead for tailers
5. **MAP_HUGETLB** — optional 2 MiB huge pages for reduced TLB pressure
6. **Pre-mapping the next window** — overlap mmap with write I/O
7. **fallocate(2)** — reserve real disk blocks instead of lseek+write
8. **Atomic operations** — CAS, loads, stores with acquire/release ordering
9. **Tiered CAS backoff** — spin → yield → exponential sleep
10. **Shared metadata** — 512-byte `metadata.brz`, mmap and cast
11. **Error handling and testing**

The design follows a **single writer, multiple readers** model. The appender
gets `PROT_READ|PROT_WRITE`; tailers get `PROT_READ`. All files use the `.brz`
extension (`metadata.brz` for the shared metadata file).

---

## 1. mmap Primitives

### mapFile

Wrapper around `std.posix.mmap`. Takes an fd, offset, size, protection mode,
and flags. Returns a slice of mapped memory.

```zig
const std = @import("std");
const posix = std.posix;

pub const Protection = enum {
    read_only,
    read_write,
};

pub const MapFlags = struct {
    TYPE: posix.system.MAP_TYPE = .SHARED,
    POPULATE: bool = false,
    HUGETLB: bool = false,
};

pub const MmapError = error{
    MmapFailed,
    MunmapFailed,
    MadviseFailed,
};

pub fn mapFile(
    fd: posix.fd_t,
    offset: u64,
    size: usize,
    prot: Protection,
    flags: MapFlags,
) MmapError![]align(std.mem.page_size) u8 {
    const prot_flags = switch (prot) {
        .read_only => posix.PROT.READ,
        .read_write => posix.PROT.READ | posix.PROT.WRITE,
    };

    var mmap_flags: u32 = @intFromEnum(flags.TYPE);
    if (flags.POPULATE) mmap_flags |= posix.system.MAP.POPULATE;
    if (flags.HUGETLB) mmap_flags |= posix.system.MAP.HUGETLB;

    const result = posix.mmap(
        null,
        size,
        prot_flags,
        @bitCast(mmap_flags),
        fd,
        @intCast(offset),
    ) catch return error.MmapFailed;

    return result;
}
```

### unmapFile

```zig
pub fn unmapFile(buf: []align(std.mem.page_size) u8) MmapError!void {
    posix.munmap(buf) catch return error.MunmapFailed;
}
```

### remapFile

Unmap the old region, then map a new one with potentially different parameters.
This is not `mremap` — we explicitly unmap and re-map. The reason: `mremap` is
Linux-only, non-portable, and the two-step approach lets us change protection
flags and mmap flags between calls.

```zig
pub fn remapFile(
    old_buf: []align(std.mem.page_size) u8,
    fd: posix.fd_t,
    new_offset: u64,
    new_size: usize,
    prot: Protection,
    flags: MapFlags,
) MmapError![]align(std.mem.page_size) u8 {
    try unmapFile(old_buf);
    return try mapFile(fd, new_offset, new_size, prot, flags);
}
```

### Protection Modes

| Role     | Protection          | Rationale                              |
|----------|---------------------|----------------------------------------|
| Appender | `PROT_READ\|PROT_WRITE` | Must write message data and headers   |
| Tailer   | `PROT_READ`         | Read-only; any write is a bug          |

This enforces the single-writer contract at the kernel level. A tailer that
accidentally writes will receive a `SIGSEGV`.

---

## 2. MAP_POPULATE for Appenders

When the appender maps a new window, we use `MAP_POPULATE` to pre-fault all
pages in the mapped region:

```zig
// Appender maps with MAP_POPULATE
const flags = MapFlags{ .TYPE = .SHARED, .POPULATE = true };
const buf = try mapFile(fd, offset, size, .read_write, flags);
```

### What MAP_POPULATE does

Without `MAP_POPULATE`, each page in the mapped region starts as a PTE
(page table entry) pointing to nothing. The first access triggers a **minor
page fault**: the kernel allocates a physical frame, wires it into the page
table, and returns control. For a 2 MiB window with 4 KiB pages, that's
**512 individual minor faults** scattered across the write path.

With `MAP_POPULATE`, the kernel performs all 512 page faults during the `mmap`
call itself, as a single bulk operation. The cost is:

- **Remap is slightly slower** — the `mmap` call blocks while faulting pages
- **Writes are jitter-free** — no per-write page faults, ever

This is the right tradeoff for a latency-sensitive appender. The remap cost
is amortized (it happens once per window) and predictable. The per-write
jitter from demand paging is unpredictable and can spike to tens of
microseconds.

### Combined with fallocate

When `MAP_POPULATE` is used on a file region that was pre-allocated with
`fallocate`, the page faults are guaranteed to be minor (the disk blocks
already exist). This eliminates ALL page-fault-related latency for the
appender after the remap.

---

## 3. madvise for Tailers

After mapping a tailer window, call `madvise(MADV_SEQUENTIAL)`:

```zig
pub fn adviseSequential(buf: []align(std.mem.page_size) u8) MmapError!void {
    posix.madvise(buf.ptr, buf.len, .SEQUENTIAL) catch return error.MadviseFailed;
}
```

Usage in the tailer:

```zig
const buf = try mapFile(fd, offset, size, .read_only, .{ .TYPE = .SHARED });
try adviseSequential(buf);
```

### What MADV_SEQUENTIAL does

This hint tells the kernel:

1. **Aggressive read-ahead** — the kernel fetches pages ahead of the access
   point, so they're already in the page cache when the tailer reaches them.
2. **Drop pages behind the cursor** — pages that have been read are candidates
   for immediate reclamation. This keeps the RSS bounded.
3. **No random-access optimization** — the kernel skips building up the
   adaptive read-ahead window that it would for random I/O.

This is a perfect match for the tailer's access pattern: strictly sequential
reads from lower offsets to higher offsets, never revisiting old data.

### Why not MAP_POPULATE for tailers?

Tailers don't benefit from pre-faulting because:

- The data may not be written yet (the tailer is chasing the appender)
- `MADV_SEQUENTIAL` read-ahead achieves the same effect adaptively
- Pre-faulting a read-only region wastes work if the tailer is close to
  the write tip

---

## 4. Huge Page Support

Optional `MAP_HUGETLB` with 2 MiB pages reduces TLB pressure dramatically.

### The TLB problem

With 4 KiB pages, a 2 MiB window requires 512 TLB entries. Modern CPUs have
roughly 1024–1536 L1 dTLB entries (64 for 4K pages + a handful for 2M pages).
When the working set exceeds TLB capacity, every cache miss becomes a TLB miss
too — a page table walk that adds 7–30 ns of latency.

With 2 MiB huge pages, that same 2 MiB window requires **1 TLB entry**.

### Usage

```zig
const flags = MapFlags{
    .TYPE = .SHARED,
    .POPULATE = true,
    .HUGETLB = true,
};
const buf = mapFile(fd, offset, size, .read_write, flags) catch |err| {
    // Fallback: try without huge pages
    if (err == error.MmapFailed) {
        return try mapFile(fd, offset, size, .read_write, .{
            .TYPE = .SHARED,
            .POPULATE = true,
        });
    }
    return err;
};
```

### Requirements

- The system must have huge pages reserved:
  `echo 64 > /proc/sys/vm/nr_hugepages`
- Or transparent huge pages (THP) must be enabled
- The file offset and size must be 2 MiB-aligned
- Falls back to regular 4 KiB pages if huge pages are unavailable

### Configuration

```zig
pub const QueueConfig = struct {
    blocksize: u64 = 2 * 1024 * 1024, // 2 MiB — aligns with huge page size
    use_huge_pages: bool = false,
    // ...
};
```

When `use_huge_pages` is `true`, the `mapFile` call adds `MAP_HUGETLB` to the
flags. If the kernel can't satisfy the request, we retry without the flag.

---

## 5. Sliding Window Strategy

### Concept

brz-queue maps a **2× blocksize window** into the process address space,
aligned to a blocksize boundary. As the tip (read or write position) advances
through the file, the window slides forward.

The "tip" is an absolute file offset. The window is positioned so the tip
falls within the mapped region:

```
File:    [=====|=====|=====|=====|=====|=====|=====]
                      ^                 ^
                      |                 |
                window_start        window_end
                (2 × blocksize)

              tip is somewhere in here ↕
```

### Window Geometry

```zig
pub const WindowParams = struct {
    /// File offset where the mmap window starts (blocksize-aligned).
    offset: u64,
    /// Size of the mmap window (min(2 * blocksize, file_size - offset)).
    size: u64,
    /// Offset of the tip within the mapped window.
    tip_in_window: u64,
};

pub fn computeWindow(tip: u64, file_size: u64, blocksize: u64) WindowParams {
    // Align the window start to blocksize boundary, one block before the tip's block.
    const block_index = tip / blocksize;
    const window_block = if (block_index > 0) block_index - 1 else 0;
    const offset = window_block * blocksize;

    // Window size: 2 blocks, but clamped to file size.
    const max_size = 2 * blocksize;
    const size = @min(max_size, file_size - offset);

    return .{
        .offset = offset,
        .size = size,
        .tip_in_window = tip - offset,
    };
}
```

### Why 2× blocksize?

A message at the end of one block may straddle the block boundary. With a
2× window, the full message is always within the mapped region — no need
to check for boundary conditions or do a split read.

### needsRemap

```zig
pub fn needsRemap(current_offset: u64, current_size: u64, new: WindowParams) bool {
    return new.offset != current_offset or new.size != current_size;
}
```

### ensureMapped

```zig
pub const MmapWindow = struct {
    buf: ?[]align(std.mem.page_size) u8 = null,
    offset: u64 = 0,
    size: u64 = 0,
    fd: posix.fd_t = -1,

    pub fn ensureMapped(
        self: *MmapWindow,
        params: WindowParams,
        prot: Protection,
        flags: MapFlags,
    ) MmapError!void {
        if (!needsRemap(self.offset, self.size, params)) return;

        if (self.buf) |old_buf| {
            try unmapFile(old_buf);
        }

        self.buf = try mapFile(self.fd, params.offset, @intCast(params.size), prot, flags);
        self.offset = params.offset;
        self.size = params.size;
    }

    pub fn ptrAt(self: *const MmapWindow, abs_offset: u64) ?[*]u8 {
        const buf = self.buf orelse return null;
        const rel = abs_offset - self.offset;
        if (rel >= self.size) return null;
        return buf.ptr + rel;
    }

    pub fn sliceAt(self: *const MmapWindow, abs_offset: u64, len: usize) ?[]u8 {
        const buf = self.buf orelse return null;
        const rel = abs_offset - self.offset;
        if (rel + len > self.size) return null;
        return buf[rel..][0..len];
    }

    pub fn unmap(self: *MmapWindow) void {
        if (self.buf) |old_buf| {
            unmapFile(old_buf) catch {};
            self.buf = null;
        }
    }
};
```

---

## 6. Pre-Mapping Next Window

When the tip crosses 50% of the current window, the next window is
pre-mapped in the background. When the actual remap triggers, the
pre-mapped window is already ready — just swap pointers and unmap the
old one.

### Trigger condition

```zig
pub inline fn shouldPremap(tip_in_window: u64, window_size: u64) bool {
    return tip_in_window > (window_size >> 1);
}
```

### Implementation sketch

```zig
pub const PremappedWindow = struct {
    current: MmapWindow = .{},
    next: ?MmapWindow = null,

    /// Called on every write/read advance.
    pub fn advanceTip(
        self: *PremappedWindow,
        tip: u64,
        file_size: u64,
        blocksize: u64,
        prot: Protection,
        flags: MapFlags,
    ) MmapError!void {
        const params = computeWindow(tip, file_size, blocksize);

        // If we need a remap and next is already pre-mapped at the right offset, swap.
        if (needsRemap(self.current.offset, self.current.size, params)) {
            if (self.next) |*next| {
                if (!needsRemap(next.offset, next.size, params)) {
                    self.current.unmap();
                    self.current = next.*;
                    self.next = null;
                    return;
                }
                next.unmap();
                self.next = null;
            }
            // Fallback: do a fresh map.
            try self.current.ensureMapped(params, prot, flags);
        }

        // Check if we should pre-map the next window.
        const tip_in_window = tip - self.current.offset;
        if (shouldPremap(tip_in_window, self.current.size) and self.next == null) {
            const next_tip = self.current.offset + self.current.size;
            if (next_tip < file_size) {
                const next_params = computeWindow(next_tip, file_size, blocksize);
                var next_win = MmapWindow{ .fd = self.current.fd };
                next_win.ensureMapped(next_params, prot, flags) catch {};
                self.next = next_win;
            }
        }
    }
};
```

### Why this matters

A fresh `mmap` call takes 2–10 µs (more with `MAP_POPULATE`). By
pre-mapping while the appender still has half a window of runway, the
latency of the remap is hidden behind normal write operations. The hot
path never blocks on a fresh `mmap`.

---

## 7. File Extension with fallocate

### The old way (lseek + write)

The traditional approach extends a file by seeking to the desired size and
writing a zero byte. This creates a **sparse file**: the filesystem metadata
says the file is large, but no disk blocks are actually allocated. The first
write to each page triggers a **major page fault** (block allocation + I/O).

### The new way (fallocate)

```zig
pub fn extendFile(fd: posix.fd_t, new_size: u64) !void {
    const rc = std.os.linux.fallocate(fd, 0, 0, @intCast(new_size));
    if (rc != 0) return error.FallocateFailed;
}
```

`fallocate` actually reserves disk blocks. After `fallocate`:

- The file is non-sparse — all blocks are allocated
- First write to each page is a **minor fault only** (no block allocation)
- Combined with `MAP_POPULATE`, eliminates **all** page faults for the appender

### needsExtension

```zig
pub fn needsExtension(tip: u64, file_size: u64, blocksize: u64) bool {
    // Extend when the tip is within one blocksize of the end.
    return tip + blocksize >= file_size;
}
```

### Extension flow

```zig
pub fn ensureFileSize(fd: posix.fd_t, current_size: *u64, required_size: u64) !void {
    if (required_size <= current_size.*) return;

    // Round up to blocksize boundary.
    const blocksize: u64 = 2 * 1024 * 1024; // or from config
    const new_size = ((required_size + blocksize - 1) / blocksize) * blocksize;

    try extendFile(fd, new_size);
    current_size.* = new_size;
}
```

---

## 8. Atomic Operations with Acquire/Release Ordering

brz-queue uses acquire/release memory ordering instead of sequential
consistency (SeqCst). This is both correct and faster:

| Architecture | SeqCst store | Release store | SeqCst load | Acquire load |
|-------------|-------------|---------------|-------------|--------------|
| x86 (TSO)  | `MOV` + `MFENCE` | `MOV` (plain) | `MOV` | `MOV` (plain) |
| ARM         | `DMB ISH` + `STR` | `STLR` | `DMB ISH` + `LDR` | `LDAR` |

On x86, acquire and release are **free** — the Total Store Order (TSO) memory
model already guarantees:
- Stores are visible in program order (release is implicit)
- Loads cannot be reordered past other loads (acquire is implicit)

On ARM, acquire/release map to `LDAR`/`STLR`, which are lighter than the
full `DMB ISH` barriers that SeqCst requires.

### CAS for write slot arbitration

The appender claims a write slot by CAS-ing the header from `UNALLOCATED` to
`WORKING`. We need `.acq_rel` on success: acquire to see all prior writes to
the slot, release to publish our claim.

```zig
pub fn cmpxchg32(ptr: *volatile u32, expected: u32, desired: u32) ?u32 {
    return @cmpxchgStrong(u32, ptr, expected, desired, .acq_rel, .monotonic);
    // On success (.acq_rel):
    //   - Acquire: prevents subsequent reads from being reordered before the CAS.
    //   - Release: makes our write visible to other threads/processes.
    // On failure (.monotonic):
    //   - We don't need ordering guarantees; we'll just retry.
    // On x86: the `lock cmpxchg` instruction provides an implicit full barrier
    //         regardless of the requested ordering.
}
```

### Atomic load (reader side)

After reading a header, the reader must see the payload that the writer
stored before publishing the header:

```zig
// After reading header, before reading payload:
// On x86 with TSO, acquire load is sufficient — no explicit fence needed.
const header = @atomicLoad(u32, header_ptr, .acquire);
// All subsequent reads (payload) are guaranteed to see data written
// before the writer's release-store of this header.
```

### Atomic store (writer side)

After writing the payload, publish the header with release ordering:

```zig
// After writing payload, publish header with release:
@atomicStore(u32, header_ptr, data_header, .release);
// All prior writes (payload bytes) are guaranteed to be visible to any
// thread/process that reads this header with acquire ordering.
```

On x86, this compiles to a plain `MOV` — no fence, no `MFENCE`, no `lock`
prefix. The TSO model guarantees store-store ordering.

### Atomic u64 access for metadata fields

Metadata fields (write position, cycle count, etc.) are 64-bit and must
be accessed atomically:

```zig
pub inline fn atomicLoad64(ptr: *const volatile u64) u64 {
    return @atomicLoad(u64, ptr, .acquire);
}

pub inline fn atomicStore64(ptr: *volatile u64, val: u64) void {
    @atomicStore(u64, ptr, val, .release);
}

pub inline fn atomicFetchAdd64(ptr: *volatile u64, val: u64) u64 {
    return @atomicRmw(u64, ptr, .Add, val, .acq_rel);
}
```

### Additional atomic helpers

```zig
pub inline fn atomicLoad32(ptr: *const volatile u32) u32 {
    return @atomicLoad(u32, ptr, .acquire);
}

pub inline fn atomicStore32(ptr: *volatile u32, val: u32) void {
    @atomicStore(u32, ptr, val, .release);
}

pub inline fn atomicFetchAdd32(ptr: *volatile u32, val: u32) u32 {
    return @atomicRmw(u32, ptr, .Add, val, .acq_rel);
}
```

---

## 9. Tiered CAS Backoff

When a CAS fails (another process claimed the slot), the appender must
retry. A naive spin-loop wastes CPU and creates contention on the cache
line. The tiered backoff strategy adapts to contention level:

```zig
pub fn casBackoff(attempt: u32) void {
    if (attempt < 64) {
        // Tier 1: spin — pure CPU spin with a PAUSE hint.
        // Cost: ~1 ns per iteration.
        // Used for: very brief contention (other writer is in the middle
        //           of a CAS on the same cache line).
        std.atomic.spinLoopHint();
    } else if (attempt < 256) {
        // Tier 2: yield — give up the CPU timeslice.
        // Cost: ~1–10 µs (depends on scheduler).
        // Used for: moderate contention (other writer is doing a memcpy
        //           of payload data).
        std.Thread.yield() catch {};
    } else {
        // Tier 3: exponential backoff sleep, capped at 1 ms.
        // Cost: delay_ns (see below).
        // Used for: sustained contention or writer stall.
        // The exponential base is 1 µs, doubling each attempt beyond 256,
        // capped at 1 ms to avoid starving other work.
        const delay_ns: u64 = @min(
            @as(u64, 1000) << @min(attempt - 256, 20),
            1_000_000, // 1 ms cap
        );
        std.time.sleep(delay_ns);
    }
}
```

### Usage

```zig
pub fn appendWithBackoff(header_ptr: *volatile u32, desired: u32) void {
    var attempt: u32 = 0;
    while (true) {
        const old = cmpxchg32(header_ptr, Header.UNALLOCATED, desired);
        if (old == null) return; // CAS succeeded
        casBackoff(attempt);
        attempt +%= 1;
    }
}
```

### Why three tiers?

| Tier | Attempts | Latency | When it fires |
|------|----------|---------|---------------|
| Spin | 0–63 | ~1 ns/iter | Cache-line bouncing between cores |
| Yield | 64–255 | ~1–10 µs | Writer is mid-memcpy on payload |
| Sleep | 256+ | 1 µs → 1 ms (exp) | Writer crashed, or system under load |

The spin tier covers >99% of contention in practice (2 cores racing on the
same cache line). The yield tier handles the case where the other writer is
doing actual work. The sleep tier is a safety valve — if we're still spinning
after 256 attempts, something is wrong and we should back off hard.

---

## 10. Shared Metadata mmap

The metadata file (`metadata.brz`) is a fixed 512-byte struct, memory-mapped
directly and cast to a pointer. No parsing, no serialization.

### SharedMetadata struct

```zig
pub const SharedMetadata = extern struct {
    /// Magic number for validation.
    magic: u64,
    /// Version of the metadata format.
    version: u64,
    /// Write position (absolute byte offset in queue file).
    write_position: u64,
    /// Current cycle (which queue file is active).
    cycle: u64,
    /// Modification count (bumped on every structural change).
    modcount: u64,
    /// Blocksize in bytes.
    blocksize: u64,
    /// Reserved for future use.
    _reserved: [512 - 48]u8 = [_]u8{0} ** (512 - 48),

    comptime {
        std.debug.assert(@sizeOf(SharedMetadata) == 512);
    }
};
```

### Mapping the metadata file

```zig
const meta_buf = try mapFile(meta_fd, 0, 512, .read_write, .{ .TYPE = .SHARED });
const metadata: *SharedMetadata = @ptrCast(@alignCast(meta_buf.ptr));
```

### Accessing fields atomically

All mutable fields in `SharedMetadata` are accessed via atomic operations:

```zig
// Read write position (tailer):
const pos = atomicLoad64(&metadata.write_position);

// Update write position (appender):
atomicStore64(&metadata.write_position, new_pos);

// Bump modcount (appender, after structural change):
_ = atomicFetchAdd64(&metadata.modcount, 1);

// Read modcount (tailer, to detect changes):
const mc = atomicLoad64(&metadata.modcount);
```

Because the metadata is a shared mmap, these atomic operations work across
process boundaries — they operate on the same physical page.

---

## 11. Queue File mmap

Queue data files (`*.brz`) use the same sliding window approach described in
Section 5, but with role-specific optimizations:

### Appender

```zig
// Appender: MAP_POPULATE, read-write, pre-map next window
const flags = MapFlags{ .TYPE = .SHARED, .POPULATE = true };
try window.ensureMapped(params, .read_write, flags);
```

When `use_huge_pages` is enabled:

```zig
const flags = MapFlags{ .TYPE = .SHARED, .POPULATE = true, .HUGETLB = true };
```

### Tailer

```zig
// Tailer: read-only, sequential advice
const flags = MapFlags{ .TYPE = .SHARED };
try window.ensureMapped(params, .read_only, flags);
try adviseSequential(window.buf.?);
```

### Full appender write flow

```
1. Check needsExtension(tip, file_size, blocksize)
   → If yes: fallocate to extend the file
2. Check needsRemap(current window, computeWindow(tip))
   → If yes: swap in pre-mapped window or do fresh mmap with MAP_POPULATE
3. Check shouldPremap(tip_in_window, window_size)
   → If yes: pre-map the next window in the background
4. CAS the header: UNALLOCATED → WORKING (with tiered backoff on failure)
5. memcpy the payload into the mapped region
6. Release-store the data header (length + flags)
7. Advance the tip and release-store write_position in metadata
```

### Full tailer read flow

```
1. Acquire-load write_position from metadata
2. If tip >= write_position → no new data, return
3. Check needsRemap(current window, computeWindow(tip))
   → If yes: remap with PROT_READ, call madvise(MADV_SEQUENTIAL)
4. Acquire-load the header at tip
5. If header indicates data → read payload, advance tip
6. If header indicates EOF → advance to next cycle
```

---

## 12. Error Handling

### mmap failures

| Error | Cause | Recovery |
|-------|-------|----------|
| `ENOMEM` | No memory / too many mappings | Log, return error, caller retries |
| `EINVAL` | Bad alignment or size | Bug — assert in debug, return error in release |
| `EACCES` | Wrong protection for file mode | Bug — mismatch between open flags and prot |

Huge page fallback:

```zig
const buf = mapFile(fd, offset, size, prot, huge_flags) catch |err| {
    if (err == error.MmapFailed) {
        // Huge pages unavailable, fall back to regular pages.
        return try mapFile(fd, offset, size, prot, regular_flags);
    }
    return err;
};
```

### fallocate failures

| Error | Cause | Recovery |
|-------|-------|----------|
| `ENOSPC` | Disk full | Return error; appender must stop |
| `EOPNOTSUPP` | Filesystem doesn't support fallocate | Fall back to lseek+write |

### madvise failures

`madvise` failures are non-fatal. The queue still works, just without
read-ahead optimization. Log a warning and continue.

---

## 13. Testing Strategy

### mmap primitive tests

```zig
test "mapFile and unmapFile round-trip" {
    const tmp = try std.fs.cwd().createFile("test.brz", .{ .read = true });
    defer std.fs.cwd().deleteFile("test.brz") catch {};
    defer tmp.close();

    try tmp.setEndPos(4096);

    const buf = try mapFile(tmp.handle, 0, 4096, .read_write, .{ .TYPE = .SHARED });
    defer unmapFile(buf) catch {};

    buf[0] = 0xAB;
    try std.testing.expectEqual(@as(u8, 0xAB), buf[0]);
}
```

### MAP_POPULATE verification

Test that `MAP_POPULATE` actually pre-faults pages by checking
`/proc/self/smaps` for the Rss (resident set size) of the mapping
immediately after mmap, before any access:

```zig
test "MAP_POPULATE pre-faults pages" {
    // Map with POPULATE
    const buf = try mapFile(fd, 0, 2 * 1024 * 1024, .read_write, .{
        .TYPE = .SHARED,
        .POPULATE = true,
    });
    defer unmapFile(buf) catch {};

    // Parse /proc/self/smaps to find Rss for this mapping.
    // Rss should equal the full mapping size (2 MiB).
    const rss = try getRssForMapping(buf.ptr);
    try std.testing.expect(rss >= 2 * 1024 * 1024);
}
```

### fallocate verification

Test that `fallocate` creates non-sparse files by checking the block count:

```zig
test "fallocate allocates real blocks" {
    const file = try std.fs.cwd().createFile("test.brz", .{ .read = true });
    defer std.fs.cwd().deleteFile("test.brz") catch {};
    defer file.close();

    try extendFile(file.handle, 2 * 1024 * 1024);

    const stat = try std.posix.fstat(file.handle);
    // st_blocks is in 512-byte units. 2 MiB = 4096 blocks.
    try std.testing.expect(stat.blocks >= 4096);
}
```

### CAS correctness under contention

Multi-process test: fork N children, each tries to CAS a header in a
shared mmap'd file. Verify that exactly one succeeds per slot.

```zig
test "CAS single-winner under contention" {
    // 1. Create a shared mmap'd file with a header word = UNALLOCATED.
    // 2. Fork N child processes.
    // 3. Each child tries cmpxchg32(&header, UNALLOCATED, child_id).
    // 4. Wait for all children.
    // 5. Assert: header == exactly one child_id.
    // 6. Assert: exactly one child reported success.
}
```

### Acquire/release ordering test

Verify that a reader with acquire-load sees the full payload written
before the writer's release-store of the header:

```zig
test "acquire/release ordering" {
    // 1. Shared mmap: [header: u32][payload: [64]u8]
    // 2. Writer: fill payload with 0xFF, then release-store header = 1.
    // 3. Reader: spin on acquire-load of header until == 1, then read payload.
    // 4. Assert: ALL payload bytes are 0xFF (no torn read).
}
```

### Sliding window tests

```zig
test "computeWindow at file start" {
    const p = computeWindow(0, 8 * 1024 * 1024, 2 * 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 0), p.offset);
    try std.testing.expectEqual(@as(u64, 4 * 1024 * 1024), p.size);
    try std.testing.expectEqual(@as(u64, 0), p.tip_in_window);
}

test "computeWindow mid-file" {
    const p = computeWindow(5 * 1024 * 1024, 16 * 1024 * 1024, 2 * 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 2 * 2 * 1024 * 1024), p.offset); // block_index=2, window_block=1
    try std.testing.expectEqual(@as(u64, 4 * 1024 * 1024), p.size);
}

test "shouldPremap triggers at 50% boundary" {
    try std.testing.expect(!shouldPremap(1024, 4096));
    try std.testing.expect(shouldPremap(2049, 4096));
}
```

### Tiered backoff tests

```zig
test "casBackoff tier transitions" {
    // Tier 1: attempts 0-63 should return almost instantly (spin).
    const start1 = std.time.nanoTimestamp();
    casBackoff(0);
    const elapsed1 = std.time.nanoTimestamp() - start1;
    try std.testing.expect(elapsed1 < 1_000); // < 1 µs

    // Tier 3: attempt 276 should sleep ~1 ms (2^20 > 1M, capped).
    const start3 = std.time.nanoTimestamp();
    casBackoff(276);
    const elapsed3 = std.time.nanoTimestamp() - start3;
    try std.testing.expect(elapsed3 >= 500_000); // at least 0.5 ms
}
```

---

## 14. Suggested File Layout

```
src/
└── brz/
    ├── atomic_ops.zig      — CAS, atomic load/store, fetch-add, backoff
    ├── mmap_ops.zig        — mapFile, unmapFile, remapFile, adviseSequential
    ├── window.zig          — MmapWindow, PremappedWindow, computeWindow
    ├── file_ops.zig        — extendFile (fallocate), ensureFileSize
    ├── metadata.zig        — SharedMetadata struct, metadata mmap helpers
    └── queue.zig           — top-level queue, appender/tailer coordination
```

---

## 15. Summary Checklist

| Component | Zig module | Key detail | Status |
|-----------|------------|------------|--------|
| `mapFile` / `unmapFile` / `remapFile` | `mmap_ops.zig` | Wraps `std.posix.mmap`/`munmap` | ☐ |
| MAP_POPULATE (appender) | `mmap_ops.zig` | Pre-fault all pages on remap | ☐ |
| madvise MADV_SEQUENTIAL (tailer) | `mmap_ops.zig` | Aggressive read-ahead | ☐ |
| MAP_HUGETLB support | `mmap_ops.zig` | 2 MiB pages, fallback to 4K | ☐ |
| `computeWindow` / `needsRemap` | `window.zig` | 2× blocksize sliding window | ☐ |
| `shouldPremap` / `PremappedWindow` | `window.zig` | Pre-map at 50% boundary | ☐ |
| `extendFile` (fallocate) | `file_ops.zig` | Real block allocation | ☐ |
| `cmpxchg32` (acq_rel) | `atomic_ops.zig` | CAS for write slot arbitration | ☐ |
| `atomicLoad64` / `atomicStore64` | `atomic_ops.zig` | Acquire/release ordering | ☐ |
| `atomicFetchAdd64` | `atomic_ops.zig` | Modcount bump | ☐ |
| `casBackoff` (tiered) | `atomic_ops.zig` | Spin → yield → exp sleep | ☐ |
| `SharedMetadata` (512 B) | `metadata.zig` | mmap + cast, no parsing | ☐ |
| Queue file mmap (appender) | `queue.zig` | POPULATE + pre-map + fallocate | ☐ |
| Queue file mmap (tailer) | `queue.zig` | PROT_READ + MADV_SEQUENTIAL | ☐ |
| Huge page fallback | `mmap_ops.zig` | Retry without HUGETLB on failure | ☐ |
| Error handling | all modules | mmap, fallocate, madvise failures | ☐ |
| Unit tests (atomics) | `atomic_ops.zig` | CAS correctness, ordering | ☐ |
| Unit tests (window) | `window.zig` | Geometry, pre-map trigger | ☐ |
| Integration tests | test harness | MAP_POPULATE smaps, fallocate blocks | ☐ |
| Multi-process tests | test harness | CAS contention, acquire/release | ☐ |