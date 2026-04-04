# Task 3: Memory-Mapped File I/O and Atomic Operations

## Overview

This document specifies the Zig reimplementation of the memory-mapped file I/O
subsystem and atomic operations used by **libchronicle**. These are the lowest-level
building blocks of the library: they govern how queue files and directory listings
are mapped into process memory, how readers and writers coordinate without locks
via atomic compare-and-swap on 4-byte headers, and how the mmap window tracks the
tailer's scan position as it moves through a queue file.

The C implementation uses POSIX `mmap`/`munmap`, `open`/`close`/`fstat`/`lseek`/`write`,
and x86-specific inline assembly for `lock; cmpxchgl`, `lock; xaddl`, and `mfence`.
Zig provides portable equivalents for all of these through `std.posix`, `std.atomic`,
and the `@cmpxchgStrong`, `@atomicRmw`, and `@fence` builtins.

---

## 1. File Operations

### C Reference

The C implementation uses the following POSIX file operations:

| Operation | C Call | Where Used |
|-----------|--------|------------|
| Open file | `open(path, flags, mode)` | Opening queue files, directory listing |
| Close file | `close(fd)` | Closing queue files on cycle change |
| Get file size | `fstat(fd, &statbuf)` | Determining mmap size |
| Extend file | `lseek(fd, offset, SEEK_SET)` + `write(fd, "", 1)` | Growing queue files |
| Create file | `open(path, O_RDWR \| O_CREAT \| O_TRUNC, 0777)` | Creating new queue files |
| Rename file | `rename(tmp, final)` | Atomic file creation |

### Zig Equivalents

```zig
// src/chronicle/file_ops.zig

const std = @import("std");
const posix = std.posix;
const fs = std.fs;

pub const FileError = error{
    OpenFailed,
    StatFailed,
    SeekFailed,
    WriteFailed,
    RenameFailed,
    CloseFailed,
};

/// Open a file by its absolute or relative path with the given flags.
/// Wraps std.posix.open with error mapping.
///
/// C equivalent: open(path, flags, mode)
pub fn openFile(path: []const u8, read_write: bool) !posix.fd_t {
    const flags: posix.O = if (read_write)
        .{ .ACCMODE = .RDWR }
    else
        .{ .ACCMODE = .RDONLY };

    return posix.open(
        // std.posix.open expects a sentinel-terminated path in many cases.
        // We need to convert our slice. See openFileZ for the null-terminated
        // variant, or use std.fs.cwd().openFile() for a higher-level API.
        path,
        flags,
        0,
    ) catch return FileError.OpenFailed;
}

/// Open a file using the higher-level std.fs API (preferred in most cases).
pub fn openFileFs(dir_path: []const u8, filename: []const u8, read_write: bool) !fs.File {
    const dir = try fs.cwd().openDir(dir_path, .{});
    defer dir.close(); // only the file handle persists

    return dir.openFile(filename, .{
        .mode = if (read_write) .read_write else .read_only,
    });
}

/// Get the size of an open file.
///
/// C equivalent: fstat(fd, &statbuf) → statbuf.st_size
pub fn fileSize(fd: posix.fd_t) !u64 {
    const stat = posix.fstat(fd) catch return FileError.StatFailed;
    return @intCast(stat.size);
}

/// Extend a file to the given size by seeking to (size - 1) and writing one byte.
/// This is the pattern used by the C code to grow queue files.
///
/// C equivalent:
///   lseek(fd, extend_to - 1, SEEK_SET);
///   write(fd, "", 1);
pub fn extendFile(fd: posix.fd_t, target_size: u64) !void {
    const seek_pos = target_size - 1;
    _ = posix.lseek(fd, @intCast(seek_pos), .SET) catch return FileError.SeekFailed;
    const one_byte = [_]u8{0};
    _ = posix.write(fd, &one_byte) catch return FileError.WriteFailed;
}

/// Create a new file with the given size, filled with zeros.
/// Used for creating new queue files and directory listings.
///
/// C equivalent:
///   fd = open(fn, O_RDWR | O_CREAT | O_TRUNC, 0777);
///   lseek(fd, size - 1, SEEK_SET);
///   write(fd, "", 1);
///   close(fd);
pub fn createSizedFile(path: []const u8, size: u64) !posix.fd_t {
    const file = fs.cwd().createFile(path, .{
        .read = true,
        .truncate = true,
        .mode = 0o777,
    }) catch return FileError.OpenFailed;

    const fd = file.handle;
    try extendFile(fd, size);
    return fd;
}

/// Atomically rename a file. Used for creating queue files via a temp file
/// to avoid races with other writers.
///
/// C equivalent: rename(tmp_path, final_path)
pub fn atomicRename(old_path: []const u8, new_path: []const u8) !void {
    fs.cwd().rename(old_path, new_path) catch return FileError.RenameFailed;
}
```

### Design Notes

- The C code uses raw POSIX `open()` with integer flags. Zig's `std.posix.open`
  wraps this, but `std.fs.cwd().openFile()` is generally preferred because it
  handles path encoding and provides a `File` struct with methods.
- For file descriptors that need to be passed to `mmap`, extract `file.handle`
  to get the underlying `posix.fd_t`.
- The C code stores `struct stat` on the tailer. In Zig, we only need the file
  size, so we store a `u64` directly and call `fstat` only when we need to refresh.

---

## 2. Memory Mapping (mmap / munmap)

### C Reference

The C implementation memory-maps files using `mmap` and `munmap`:

```c
// Map a region of a file
buf = mmap(0, size, prot, MAP_SHARED, fd, offset);
if (buf == MAP_FAILED) { /* handle error */ }

// Unmap a previously mapped region
munmap(buf, size);
```

Protection flags used:
- `PROT_READ` — for reader tailers
- `PROT_READ | PROT_WRITE` — for the appender tailer and the writable directory listing

### Zig Equivalents

Zig provides `std.posix.mmap` and `std.posix.munmap` which wrap the POSIX calls
directly.

```zig
// src/chronicle/mmap_ops.zig

const std = @import("std");
const posix = std.posix;

pub const MmapError = error{
    MmapFailed,
    InvalidAlignment,
};

pub const Protection = enum {
    read_only,
    read_write,
};

/// Memory-map a region of a file.
///
/// Returns an aligned slice of bytes. The returned slice's `.ptr` and `.len`
/// correspond to the mapped region.
///
/// C equivalent: mmap(0, size, prot, MAP_SHARED, fd, offset)
///
/// Parameters:
///   fd     — file descriptor of the file to map
///   offset — byte offset into the file (must be page-aligned)
///   size   — number of bytes to map
///   prot   — read-only or read-write
pub fn mapFile(
    fd: posix.fd_t,
    offset: u64,
    size: usize,
    prot: Protection,
) MmapError![]align(std.mem.page_size) u8 {
    const prot_flags: posix.PROT = switch (prot) {
        .read_only => .{ .READ = true },
        .read_write => .{ .READ = true, .WRITE = true },
    };

    const result = posix.mmap(
        null, // let the kernel choose the address
        size,
        prot_flags,
        .{ .TYPE = .SHARED },
        fd,
        @intCast(offset),
    ) catch return MmapError.MmapFailed;

    return result;
}

/// Unmap a previously mapped region.
///
/// C equivalent: munmap(buf, size)
pub fn unmapFile(buf: []align(std.mem.page_size) u8) void {
    posix.munmap(buf);
}

/// Convenience: unmap if non-null, then map a new region.
/// Returns the new mapping. If the old mapping is null, just creates a new one.
///
/// This pattern is used throughout the tailer state machine when the mmap
/// window needs to slide forward.
pub fn remapFile(
    old_buf: ?[]align(std.mem.page_size) u8,
    fd: posix.fd_t,
    offset: u64,
    size: usize,
    prot: Protection,
) MmapError![]align(std.mem.page_size) u8 {
    if (old_buf) |buf| {
        unmapFile(buf);
    }
    return mapFile(fd, offset, size, prot);
}
```

### Page Alignment

- The `offset` parameter to `mmap` must be a multiple of the system page size
  (typically 4096 bytes). The blocksize in libchronicle is always a power of two
  and at least 1 MiB (1048576), which is always a multiple of the page size.
- Zig's `std.posix.mmap` returns `[]align(std.mem.page_size) u8`, which encodes
  the alignment in the type system. Store this typed slice on the `Tailer` struct
  to preserve alignment information.
- When the mmap offset is computed as `qf_tip & blocksize_mask`, the result is
  always blocksize-aligned, which is always page-aligned. No explicit page-alignment
  step is needed as long as blocksize ≥ page_size.

### Error Handling

In C, `mmap` returns `MAP_FAILED` (which is `(void*)-1`). The C code checks for
this and returns `TS_E_MMAP`. In Zig, `std.posix.mmap` returns an error from the
error set, which we map to `MmapError.MmapFailed`. The tailer state machine
translates this to `TailerState.err_mmap`.

---

## 3. The Blocksize-Based Windowing Strategy

### Concept

Chronicle Queue files can be very large (tens or hundreds of gigabytes). Rather
than mapping the entire file, the library maps a sliding window of `2 × blocksize`
bytes at a time, aligned to a `blocksize` boundary.

### Window Geometry

```
    file offset 0                                          file size
    ├──────────────────────────────────────────────────────────┤
    │                           │                              │
    │     ...earlier data...    │  qf_tip                      │
    │                           │    ↓                         │
    │                           ├────┬─────────────────────────┤
    │                           │████│█████████████████████████ │
    │                           ├────┴─────────────────────────┤
    │                           │                              │
    │                        mmapoff          mmapoff + mmapsz │
    │                           │←─── up to 2×blocksize ──────→│
    │                           │                              │
    └──────────────────────────────────────────────────────────┘

    qf_tip     = byte position of the next header to read/write
    mmapoff    = qf_tip & ~(blocksize - 1)        [aligned down to blocksize]
    mmapsz     = min(2 * blocksize, file_size - mmapoff)
    basep      = qf_buf + (qf_tip - mmapoff)      [pointer within the mmap]
    extent     = qf_buf + mmapsz                   [end of mapped region]
```

### Why 2× Blocksize?

A single entry's payload can be up to `blocksize` bytes long (the library
doubles the blocksize if a message exceeds it). By mapping `2 × blocksize`, we
guarantee that if `qf_tip` falls anywhere within the first block, we can always
read or write at least one full `blocksize`-length entry without running past the
end of the mapping. If the parser reaches the edge of the second block, it
returns `QB_NEED_EXTEND` (now `ParseBlockState.need_extend`), and the tailer
re-maps with a new offset.

### Implementation

```zig
// src/chronicle/window.zig

const std = @import("std");
const mmap_ops = @import("mmap_ops.zig");
const MmapError = mmap_ops.MmapError;
const Protection = mmap_ops.Protection;
const posix = std.posix;

/// Compute the mmap window parameters for a given tip position and blocksize.
pub const WindowParams = struct {
    /// File offset where the window starts (blocksize-aligned).
    offset: u64,
    /// Size of the window in bytes.
    size: u64,
    /// Offset of the tip within the window (tip - offset).
    tip_in_window: u64,
};

/// Calculate the mmap window parameters.
///
/// C equivalent: the logic in chronicle_peek_queue_tailer_r around lines 884-902
///
///   blocksize_mask = ~(blocksize - 1)
///   mmapoff = qf_tip & blocksize_mask
///   limit = min(2 * blocksize, file_size - mmapoff)
pub fn computeWindow(
    tip: u64,
    file_size: u64,
    blocksize: u32,
) WindowParams {
    const blocksize_mask: u64 = ~(@as(u64, blocksize) - 1);
    const offset = tip & blocksize_mask;

    const remaining = file_size - offset;
    const double_block = @as(u64, blocksize) * 2;
    const size = if (remaining > double_block) double_block else remaining;

    return .{
        .offset = offset,
        .size = size,
        .tip_in_window = tip - offset,
    };
}

/// Determine whether the current mmap window needs to be refreshed.
/// Returns true if the window is not mapped or the computed window differs
/// from the current one.
pub fn needsRemap(
    current_buf: ?[]align(std.mem.page_size) u8,
    current_offset: u64,
    current_size: u64,
    desired_offset: u64,
    desired_size: u64,
) bool {
    if (current_buf == null) return true;
    if (current_offset != desired_offset) return true;
    if (current_size != desired_size) return true;
    return false;
}

/// A managed mmap window that tracks its position within a file.
/// Encapsulates the mmap buffer, offset, and size fields from the C tailer.
pub const MmapWindow = struct {
    /// Currently mapped buffer, or null if not mapped.
    buf: ?[]align(std.mem.page_size) u8 = null,
    /// File offset where the current mapping starts.
    offset: u64 = 0,
    /// Size of the current mapping.
    size: u64 = 0,

    /// Unmap the current window if mapped.
    pub fn unmap(self: *MmapWindow) void {
        if (self.buf) |b| {
            mmap_ops.unmapFile(b);
            self.buf = null;
            self.offset = 0;
            self.size = 0;
        }
    }

    /// Map or remap the window to cover the given file region.
    /// Only performs the syscall if the window actually needs to change.
    ///
    /// Returns true if a remap occurred, false if the existing mapping
    /// was already correct.
    pub fn ensureMapped(
        self: *MmapWindow,
        fd: posix.fd_t,
        desired_offset: u64,
        desired_size: u64,
        prot: Protection,
    ) MmapError!bool {
        if (!needsRemap(self.buf, self.offset, self.size, desired_offset, desired_size)) {
            return false;
        }

        // Unmap old mapping
        self.unmap();

        // Create new mapping
        self.buf = try mmap_ops.mapFile(fd, desired_offset, @intCast(desired_size), prot);
        self.offset = desired_offset;
        self.size = desired_size;
        return true;
    }

    /// Get a pointer into the mapped buffer at the given file offset.
    /// Returns null if the offset is outside the current window.
    pub fn ptrAt(self: *const MmapWindow, file_offset: u64) ?[*]u8 {
        if (self.buf == null) return null;
        if (file_offset < self.offset) return null;
        const delta = file_offset - self.offset;
        if (delta >= self.size) return null;
        return self.buf.?.ptr + delta;
    }

    /// Get a slice within the mapped buffer starting at the given file offset.
    /// Returns null if the requested range exceeds the window.
    pub fn sliceAt(self: *const MmapWindow, file_offset: u64, len: usize) ?[]u8 {
        if (self.buf == null) return null;
        if (file_offset < self.offset) return null;
        const delta = file_offset - self.offset;
        if (delta + len > self.size) return null;
        const start: usize = @intCast(delta);
        return self.buf.?[start..][0..len];
    }

    /// Get the full mapped slice from the tip position to the end of the window.
    /// This is what parse_queue_block scans over.
    pub fn scanSlice(self: *const MmapWindow, tip: u64) ?[]u8 {
        if (self.buf == null) return null;
        if (tip < self.offset) return null;
        const start: usize = @intCast(tip - self.offset);
        if (start >= self.size) return null;
        return self.buf.?[start..@intCast(self.size)];
    }
};
```

---

## 4. The queue_double_blocksize Mechanism

When the inner block parser (`parse_queue_block`) returns `need_extend` without
having made any forward progress (i.e., `basep == basep_old`), it means a single
entry's header + payload spans more than the current `2 × blocksize` window. The
solution is to double the blocksize, which doubles the window size on the next
remap.

### C Reference

```c
// libchronicle.c:252-256
void queue_double_blocksize(queue_t* queue) {
    uint new_blocksize = queue->blocksize << 1;
    printf("shmipc:  doubling blocksize from %x to %x\n", queue->blocksize, new_blocksize);
    queue->blocksize = new_blocksize;
}
```

This is called from the tailer state machine:

```c
// libchronicle.c:938-940
if (s == QB_NEED_EXTEND && basep == basep_old) {
    queue_double_blocksize(queue);
}
```

And also from the appender before writing, to ensure the message fits:

```c
// libchronicle.c:1075-1076
while (write_sz > queue->blocksize)
    queue_double_blocksize(queue);
```

### Zig Implementation

```zig
// In queue.zig, on the Queue struct:

/// Double the mmap block size. Called when a single entry exceeds
/// the current window size, or when the parser cannot make progress
/// because the remaining mapped region is too small.
///
/// The blocksize must always remain a power of two.
///
/// C equivalent: queue_double_blocksize()
pub fn doubleBlocksize(self: *Queue) void {
    const old = self.blocksize;
    self.blocksize = self.blocksize << 1;
    std.log.info("chronicle: doubling blocksize from 0x{x} to 0x{x}", .{ old, self.blocksize });
}

/// Ensure the blocksize is at least large enough to hold a message of
/// the given size. Doubles repeatedly if necessary.
///
/// C equivalent: the while loop in chronicle_append_ts()
pub fn ensureBlocksizeFor(self: *Queue, write_size: usize) void {
    while (write_size > self.blocksize) {
        self.doubleBlocksize();
    }
}
```

### Blocksize Invariants

- The blocksize **must** always be a power of two. This is because the alignment
  mask is computed as `~(blocksize - 1)`, which only works correctly for powers
  of two.
- The initial blocksize is 1 MiB (1048576 = 2^20).
- The blocksize is a property of the `Queue`, not of individual tailers. Doubling
  it affects all tailers on their next remap.
- After doubling, existing mmaps are **not** immediately invalidated. They will be
  replaced on the next iteration of the tailer state machine when `needsRemap()`
  detects the window parameters have changed.

---

## 5. Atomic Compare-and-Swap (CAS)

### Purpose

The CAS operation is the core of the lock-free writing protocol. When an appender
wants to write an entry:

1. It finds the next unallocated position (header == `HD_UNALLOCATED` == 0x00000000).
2. It atomically attempts to replace the 0x00000000 header with `HD_WORKING` (0x80000000).
3. If the CAS succeeds (returned value == old value == 0x00000000), the appender
   "owns" that slot and can write the payload.
4. After writing the payload, it overwrites the header with the actual data size,
   clearing the WORKING bit.
5. If the CAS fails (another writer got there first), the appender retries.

### C Reference

```c
// libchronicle.c:216-222
static inline uint32_t lock_cmpxchgl(unsigned char *mem, uint32_t newval, uint32_t oldval) {
    __typeof (*mem) ret;
    __asm __volatile ("lock; cmpxchgl %2, %1"
    : "=a" (ret), "=m" (*mem)
    : "r" (newval), "m" (*mem), "0" (oldval));
    return (uint32_t) ret;
}
```

Note the parameter order: `lock_cmpxchgl(ptr, new_value, expected_old_value)`.
It returns the **original value** found in memory. If the return value equals
`expected_old_value`, the swap succeeded.

Usage in the appender:

```c
// libchronicle.c:1192-1194
unsigned char* ptr = (appender->qf_tip - appender->qf_mmapoff) + appender->qf_buf;
uint32_t ret = lock_cmpxchgl(ptr, HD_UNALLOCATED, HD_WORKING);
if (ret == HD_UNALLOCATED) { /* we got the lock */ }
```

**Caution:** The C code's argument order is `(ptr, oldval_to_write_if_match, newval)` —
but looking at the assembly, it's actually `(ptr, newval, oldval)` with `newval`
being what to store on success and `oldval` being what we expect to find. The
variable naming in the C code is confusing: `HD_UNALLOCATED` is the *expected*
value and `HD_WORKING` is the *desired new* value. Reading the assembly confirms:
`cmpxchgl %2, %1` compares EAX (loaded with `oldval`=`HD_UNALLOCATED`) against
`*mem`, and if equal, stores `newval`=`HD_WORKING` into `*mem`.

### Zig Implementation

Zig provides the `@cmpxchgStrong` builtin, which is a portable atomic CAS:

```zig
// src/chronicle/atomic_ops.zig

const std = @import("std");

/// Atomic compare-and-swap on a 32-bit value in memory.
///
/// Attempts to atomically replace the value at `ptr` from `expected` to `desired`.
/// Returns the value that was in memory before the operation:
///   - If it equals `expected`, the swap succeeded.
///   - If it differs, the swap failed and the returned value is what was found.
///
/// This directly replaces the C `lock_cmpxchgl` inline assembly.
///
/// C equivalent:
///   uint32_t ret = lock_cmpxchgl(ptr, new_val, expected_val);
///   // ret == expected_val means success
///
/// Memory ordering: .seq_cst matches the x86 `lock; cmpxchgl` semantics.
pub fn cmpxchg32(ptr: *volatile u32, expected: u32, desired: u32) u32 {
    // @cmpxchgStrong returns `?u32`:
    //   null  → swap succeeded, old value was `expected`
    //   value → swap failed, this is what was actually in memory
    const result = @cmpxchgStrong(
        u32,
        ptr,
        expected,
        desired,
        .seq_cst,
        .seq_cst,
    );
    return result orelse expected;
}

/// Wrapper that returns a bool for convenience.
/// Returns true if the swap succeeded.
pub fn tryCmpxchg32(ptr: *volatile u32, expected: u32, desired: u32) bool {
    return @cmpxchgStrong(
        u32,
        ptr,
        expected,
        desired,
        .seq_cst,
        .seq_cst,
    ) == null;
}
```

### Usage in the Appender

```zig
// In the append logic:

const Header = @import("header.zig").Header;
const atomic_ops = @import("atomic_ops.zig");

// Get a pointer to the 4-byte header at the current tip position
const header_ptr: *volatile u32 = @ptrCast(@alignCast(window.ptrAt(appender.qf_tip).?));

// Attempt to claim this slot: expect UNALLOCATED, write WORKING
const old_val = atomic_ops.cmpxchg32(header_ptr, Header.UNALLOCATED, Header.WORKING);

if (old_val == Header.UNALLOCATED) {
    // Success! We own this slot.
    // Memory fence before writing payload
    @fence(.seq_cst);

    // ... write payload after the 4-byte header ...

    // Memory fence after writing payload, before publishing the real header
    @fence(.seq_cst);

    // Publish: overwrite the WORKING header with the actual data size
    const final_header = Header.dataHeader(@intCast(write_size));
    @atomicStore(u32, header_ptr, final_header, .seq_cst);
} else {
    // Another writer got here first. Retry after re-polling.
    std.log.info("chronicle: write lock failed, peeking again", .{});
}
```

### Important: Pointer Alignment

The 4-byte header must be naturally aligned (4-byte boundary) for the atomic
operation to work correctly. Chronicle Queue guarantees this because:

- Queue files start at offset 0 (aligned).
- Every entry is `4 + payload_size + pad4` bytes, where `pad4` rounds up to
  the next 4-byte boundary.
- The header is always the first 4 bytes of each entry.

In Zig, the `@ptrCast(@alignCast(...))` to `*volatile u32` will assert correct
alignment at runtime in debug/safe modes. If you want to be defensive, add an
explicit check:

```zig
const raw_ptr = window.ptrAt(appender.qf_tip) orelse return error.MmapFailed;
const addr = @intFromPtr(raw_ptr);
if (addr & 0x3 != 0) {
    return error.MisalignedHeader;
}
const header_ptr: *volatile u32 = @ptrCast(@alignCast(raw_ptr));
```

---

## 6. Atomic Add (lock; xaddl)

### Purpose

The `lock; xaddl` instruction is used to atomically increment the `modcount`
field in the directory listing. When a writer updates `highestCycle` or
`lowestCycle`, it bumps `modcount` so that readers know to re-poll.

### C Reference

```c
// libchronicle.c:224-231
static inline uint32_t lock_xadd(unsigned char* mem, uint32_t val) {
    __asm__ volatile("lock; xaddl %0, %1"
    : "+r" (val), "+m" (*mem)
    : // No input-only
    : "memory"
    );
    return (uint32_t)val;
}
```

Usage:

```c
// libchronicle.c:808
lock_xadd(queue->dirlist_fields.modcount, 1);
```

Note: The `modcount` field in the directory listing is a `uint64_t` stored at an
8-byte-aligned position (ensured by `wirepad_uint64_aligned`). However, the C code
uses `lock; xaddl` (the `l` suffix = 32-bit operand), so it only atomically
increments the lower 32 bits. This is a subtle detail — for compatibility, the
Zig implementation should match this behavior, though using a 64-bit atomic add
would be more correct.

### Zig Implementation

```zig
// src/chronicle/atomic_ops.zig (continued)

/// Atomically add a value to a 32-bit integer in memory and return the
/// previous value.
///
/// This replaces the C `lock; xaddl` inline assembly.
///
/// C equivalent:
///   uint32_t old = lock_xadd(ptr, val);
///
/// Note: The C code operates on 32 bits even though modcount is logically
/// a 64-bit field. For strict compatibility, use atomicAdd32. For correctness,
/// use atomicAdd64.
pub fn atomicAdd32(ptr: *volatile u32, val: u32) u32 {
    return @atomicRmw(u32, ptr, .Add, val, .seq_cst);
}

/// Atomically add a value to a 64-bit integer in memory and return the
/// previous value.
///
/// Preferred over atomicAdd32 for the modcount field when full 64-bit
/// correctness is desired.
pub fn atomicAdd64(ptr: *volatile u64, val: u64) u64 {
    return @atomicRmw(u64, ptr, .Add, val, .seq_cst);
}
```

### Usage: Bumping modcount

```zig
// In the poke_queue_modcount equivalent:

const atomic_ops = @import("atomic_ops.zig");

pub fn pokeModcount(self: *Queue) void {
    // Write highestCycle and lowestCycle to the mmap
    if (self.dirlist_fields.highest_cycle) |ptr| {
        @atomicStore(u64, @ptrCast(@alignCast(ptr)), self.highest_cycle, .seq_cst);
    }
    if (self.dirlist_fields.lowest_cycle) |ptr| {
        @atomicStore(u64, @ptrCast(@alignCast(ptr)), self.lowest_cycle, .seq_cst);
    }

    // Atomically increment modcount
    if (self.dirlist_fields.modcount) |ptr| {
        // For strict C compatibility (32-bit xadd on lower half):
        const ptr32: *volatile u32 = @ptrCast(@alignCast(ptr));
        _ = atomic_ops.atomicAdd32(ptr32, 1);

        // For full 64-bit correctness (preferred):
        // const ptr64: *volatile u64 = @ptrCast(@alignCast(ptr));
        // _ = atomic_ops.atomicAdd64(ptr64, 1);
    }
}
```

---

## 7. Memory Fence (mfence)

### Purpose

Memory fences ensure ordering of memory operations. In the Chronicle Queue
protocol, fences are used in two critical places:

1. **Reader side:** After reading a header word, before reading the payload.
   Without a fence, the CPU could speculatively load payload bytes before
   the header read completes, potentially reading stale data from before the
   writer finished.

2. **Writer side:** After writing the payload, before publishing the final
   header with the data size. Without a fence, the header write could become
   visible to readers before all payload bytes are committed.

### C Reference

```c
// Used by the reader in parse_queue_block:
asm volatile ("mfence" ::: "memory");

// Used by the writer in chronicle_append_ts:
asm volatile ("mfence" ::: "memory");
```

### Zig Implementation

```zig
// src/chronicle/atomic_ops.zig (continued)

/// Full memory fence. Ensures all prior memory operations are globally
/// visible before any subsequent memory operations.
///
/// Replaces: asm volatile ("mfence" ::: "memory")
///
/// On x86-64, @fence(.seq_cst) compiles to an `mfence` instruction.
/// On ARM, it compiles to a `dmb ish` instruction.
/// On other architectures, the appropriate barrier is emitted.
pub inline fn memoryFence() void {
    @fence(.seq_cst);
}
```

### Where to Use Fences

In the block parser (reader side):

```zig
// After reading the 4-byte header
const header = std.mem.readInt(u32, buf[base..][0..4], .little);

// Fence: ensure we see the payload that corresponds to this header
atomic_ops.memoryFence();

// Now safe to read the payload
if (Header.isUnallocated(header)) {
    // ...
}
```

In the appender (writer side):

```zig
// After winning the CAS (writing WORKING header)
atomic_ops.memoryFence();

// Write the payload
queue.append_write(payload_ptr, msg, write_sz);

// Fence: ensure payload is committed before we publish the size header
atomic_ops.memoryFence();

// Publish the real header (clears WORKING, sets size)
const final_header = Header.dataHeader(@intCast(write_sz));
@atomicStore(u32, header_ptr, final_header, .release);
```

### Fence vs. Atomic Store Ordering

An alternative to using explicit fences is to use the ordering guarantees of
atomic loads and stores:

- Reader: `@atomicLoad(u32, header_ptr, .acquire)` ensures subsequent reads
  see writes that were visible when the header was written.
- Writer: `@atomicStore(u32, header_ptr, final_header, .release)` ensures
  all prior writes (the payload) are visible before the header becomes visible.

This acquire/release pair is semantically equivalent to the mfence approach
and may be more efficient on non-x86 architectures. However, using `.seq_cst`
everywhere matches the C implementation's behavior exactly.

Recommended approach for correctness and simplicity: **use `.seq_cst` everywhere**
initially, then optimize to acquire/release once correctness is validated.

---

## 8. File Extension for the Appender

### Problem

Queue files are pre-allocated to a fixed size (`qf_disk_sz` = 83,754,496 bytes).
When the appender approaches the end of the file (less than `2 × blocksize` bytes
remain beyond the current mmap offset), the file needs to be extended.

### C Reference

```c
// libchronicle.c:1143-1153
if (r == TS_EXTEND_FAIL) {
    uint64_t extend_to = appender->qf_statbuf.st_size + qf_disk_sz;
    if (lseek(appender->qf_fd, extend_to - 1, SEEK_SET) == -1) {
        printf("shmmain: extend queuefile %s failed at lseek: %s\n", ...);
        sleep(1);
        continue;
    }
    if (write(appender->qf_fd, "", 1) != 1) {
        printf("shmmain: extend queuefile %s failed at write: %s\n", ...);
        sleep(1);
        continue;
    }
}
```

The detection happens in the tailer state machine:

```c
// libchronicle.c:895-901
if (tailer->qf_statbuf.st_size - mmapoff < 2*queue->blocksize) {
    if (fstat(tailer->qf_fd, &tailer->qf_statbuf) < 0) return TS_E_STAT;
    // If still too small AND we are a writer, signal extend needed
    if (tailer->qf_statbuf.st_size - mmapoff < 2*queue->blocksize
        && tailer->mmap_protection != PROT_READ) {
        return TS_EXTEND_FAIL;
    }
}
```

### Zig Implementation

```zig
// src/chronicle/file_ops.zig (continued)

const config = @import("config.zig");

/// Extend a queue file by the standard increment.
///
/// Returns the new file size on success.
///
/// C equivalent: the TS_EXTEND_FAIL handling in chronicle_append_ts()
pub fn extendQueueFile(fd: posix.fd_t, current_size: u64) !u64 {
    const new_size = current_size + config.default_qf_disk_size;
    try extendFile(fd, new_size);
    return new_size;
}

/// Check whether the file needs extending for a writer tailer.
/// Returns true if extension is needed.
///
/// This check should only trigger for appender tailers (read-write protection).
pub fn needsExtension(
    file_size: u64,
    mmap_offset: u64,
    blocksize: u32,
) bool {
    return (file_size - mmap_offset) < @as(u64, blocksize) * 2;
}

/// Refresh the file size from disk (fstat) and optionally extend.
/// Returns the updated file size.
pub fn refreshAndMaybeExtend(
    fd: posix.fd_t,
    current_size: *u64,
    mmap_offset: u64,
    blocksize: u32,
    is_writer: bool,
) !?u64 {
    // Refresh stat
    current_size.* = try fileSize(fd);

    // Check if extension is still needed after refresh
    if (needsExtension(current_size.*, mmap_offset, blocksize)) {
        if (is_writer) {
            current_size.* = try extendQueueFile(fd, current_size.*);
            return current_size.*;
        }
        // Reader: cannot extend, will return TS_EXTEND_FAIL equivalent
        return null;
    }
    return current_size.*;
}
```

### Extension Flow in the Tailer State Machine

The tailer state machine (equivalent to `chronicle_peek_queue_tailer_r`) handles
extension as follows:

```zig
// Pseudocode for the tailer state machine loop:

while (true) {
    // ... open cycle file, compute window params ...

    const wp = window.computeWindow(tailer.qf_tip, tailer.qf_file_size, queue.blocksize);

    // Check if we're approaching the file size limit
    if (needsExtension(tailer.qf_file_size, wp.offset, queue.blocksize)) {
        // Refresh fstat to get latest size (another writer may have extended)
        tailer.qf_file_size = try fileSize(tailer.qf_fd.?);

        if (needsExtension(tailer.qf_file_size, wp.offset, queue.blocksize)) {
            if (tailer.mmap_protection == .read_write) {
                // Signal to the appender loop that extension is needed
                return .extend_needed;
            }
            // Reader: just use whatever size is available
        }
    }

    // ... remap window, parse block ...

    // Handle parse result
    if (parse_result == .need_extend and tip_unchanged) {
        // A single entry spans more than 2×blocksize — double it
        queue.doubleBlocksize();
        continue;
    }

    // ... handle other states ...
}
```

---

## 9. Queue File Creation

### Flow

Creating a new queue file involves:

1. Generate a temporary filename: `{desired_name}.{pid}.tmp`
2. Create the file and extend it to `qf_disk_sz` bytes
3. Optionally write the initial metadata header (TODO in C code)
4. Atomically rename the temp file to the desired name
5. If the new cycle > highest_cycle, update highest_cycle and bump modcount

### Zig Implementation

```zig
// src/chronicle/queuefile.zig

const std = @import("std");
const posix = std.posix;
const fs = std.fs;
const file_ops = @import("file_ops.zig");
const config = @import("config.zig");

pub const QueueFileError = error{
    CreateFailed,
    ExtendFailed,
    RenameFailed,
};

/// Create a new empty queue file at the given path.
/// The file is first created as a temporary file and then atomically renamed.
///
/// C equivalent: queuefile_init() in libchronicle.c:1370-1398
pub fn createQueueFile(
    allocator: std.mem.Allocator,
    final_path: []const u8,
) !void {
    // Build temp filename: "{final_path}.{pid}.tmp"
    const pid = std.os.linux.getpid();
    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.{d}.tmp", .{ final_path, pid });
    defer allocator.free(tmp_path);

    // Create and extend the temp file
    const fd = file_ops.createSizedFile(tmp_path, config.default_qf_disk_size) catch {
        return QueueFileError.CreateFailed;
    };
    posix.close(fd);

    // TODO: Write initial queue file header (metadata message with roll config)
    // TODO: Write initial index2index structure

    // Atomically rename to the desired path
    file_ops.atomicRename(tmp_path, final_path) catch {
        return QueueFileError.RenameFailed;
    };
}
```

---

## 10. The Tailer State Machine and mmap Integration

The tailer state machine (`chronicle_peek_queue_tailer_r` in C) is the most
complex function in the library. It orchestrates cycle management, file opening,
mmap windowing, and block parsing. Here is how the mmap operations integrate.

### State Machine Loop (Pseudocode)

```zig
/// Poll a tailer for new data. This is the Zig equivalent of
/// chronicle_peek_queue_tailer_r().
///
/// The function runs in a loop until it reaches a terminal state:
/// - awaiting_entry: no more data available
/// - busy: a writer is active at the current position
/// - awaiting_queue_file: the next cycle's file doesn't exist yet
/// - err_stat / err_mmap: I/O errors
/// - extend_needed: the appender needs the file extended
/// - collected: a value was synchronously collected
pub fn peekTailer(queue: *Queue, tailer: *Tailer) TailerState {
    while (true) {
        // ── Step 1: Determine the cycle from the current index ──────────
        const cycle = Index.cycle(tailer.qf_index);

        // ── Step 2: Open the cycle file if needed ───────────────────────
        if (cycle != tailer.qf_cycle_open or tailer.qf_fd == null) {
            // Close previous file and mapping
            tailer.window.unmap();
            if (tailer.qf_fd) |fd| posix.close(fd);
            if (tailer.qf_filename) |fn_slice| queue.allocator.free(fn_slice);

            // Generate filename for this cycle
            tailer.qf_filename = getCycleFn(queue, cycle) catch return .err_stat;
            tailer.qf_tip = 0;

            // Open the file
            const fd = openFile(tailer.qf_filename.?, tailer.mmap_protection == .read_write);
            if (fd) |f| {
                tailer.qf_fd = f;
            } else {
                // File doesn't exist
                if (cycle < queue.highest_cycle) {
                    // Skip this missing file — advance to next cycle
                    tailer.qf_index = Index.cycleStart(cycle + 1);
                    continue;
                }
                return .awaiting_queue_file;
            }
            tailer.qf_cycle_open = cycle;

            // Get file size
            tailer.qf_file_size = fileSize(tailer.qf_fd.?) catch return .err_stat;
        }

        // ── Step 3: Compute and apply the mmap window ───────────────────
        const wp = computeWindow(tailer.qf_tip, tailer.qf_file_size, queue.blocksize);

        // Refresh file size if approaching the end
        if (needsExtension(tailer.qf_file_size, wp.offset, queue.blocksize)) {
            tailer.qf_file_size = fileSize(tailer.qf_fd.?) catch return .err_stat;
            if (needsExtension(tailer.qf_file_size, wp.offset, queue.blocksize)) {
                if (tailer.mmap_protection == .read_write) {
                    return .extend_needed;
                }
            }
        }

        // Recompute window after possible stat refresh
        const final_wp = computeWindow(tailer.qf_tip, tailer.qf_file_size, queue.blocksize);

        // Remap if needed
        _ = tailer.window.ensureMapped(
            tailer.qf_fd.?,
            final_wp.offset,
            final_wp.size,
            tailer.mmap_protection,
        ) catch return .err_mmap;

        // ── Step 4: Parse the block ─────────────────────────────────────
        const scan_buf = tailer.window.scanSlice(tailer.qf_tip) orelse return .err_mmap;
        var tip_local: usize = 0;
        var index_local = tailer.qf_index;
        const tip_before = tip_local;

        const parse_result = parseQueueBlock(
            // ... handler, callback, buffer, etc. ...
            scan_buf,
            &tip_local,
            &index_local,
            queue.version,
        );

        // ── Step 5: Commit progress ─────────────────────────────────────
        if (tip_local != tip_before) {
            tailer.qf_tip = tailer.window.offset + @as(u64, tip_local) +
                            (tailer.qf_tip - tailer.window.offset - @as(u64, tip_before));
            tailer.qf_index = index_local;
        }

        // ── Step 6: Handle parse result ─────────────────────────────────
        if (parse_result == .need_extend and tip_local == tip_before) {
            queue.doubleBlocksize();
            continue; // retry with larger window
        }

        if (parse_result == .busy) return .busy;
        if (parse_result == .collected) return .collected;

        if (parse_result == .awaiting_entry) {
            if (cycle < queue.highest_cycle -| config.patch_cycles) {
                // Fast-forward past missing EOF
                tailer.qf_index = Index.cycleStart(cycle + 1);
                continue;
            }
            return .awaiting_entry;
        }

        if (parse_result == .reached_eof) {
            // Advance to next cycle
            tailer.qf_index = Index.nextCycleStart(tailer.qf_index);
            continue; // open next cycle file
        }
    }
}
```

### Key Relationships

The following diagram shows how the mmap window fields relate to each other
during scanning:

```
File on disk:
    0               qf_tip                              file_size
    ├───────────────────┼───────────────────────────────────┤
    │                   │                                   │

mmap window:
                    mmapoff                    mmapoff+mmapsz
                    ├──────────────────────────────┤
                    │  ↑basep          ↑extent     │
                    │  │               │           │
    qf_buf ─────────→ [mapped memory region]
                    │  │               │
                    │  tip_in_window   │
                    │  = qf_tip -      │
                    │    mmapoff        │

Pointer arithmetic:
    basep  = qf_buf + (qf_tip - mmapoff)
    extent = qf_buf + mmapsz

After parse_queue_block advances basep:
    new_tip = (basep_new - qf_buf) + mmapoff
```

---

## 11. Directory Listing mmap

The directory listing file (`metadata.cq4t`) is
memory-mapped for the lifetime of the queue. Unlike queue files which use a
sliding window, the directory listing is small enough to map entirely.

### Key Fields

Three 8-byte fields within the directory listing are accessed via direct pointers
into the mmap:

- `listing.highestCycle` — written by appenders, read by tailers
- `listing.lowestCycle` — written by appenders, read by tailers
- `listing.modCount` — atomically incremented by appenders, polled by readers

These pointers are discovered during `parse_dirlist` by the wire parser, which
reports `ptr_uint64` events for `INT64` values. The pointers point directly into
the mmap buffer, which is `MAP_SHARED`, so writes from one process are visible
to others.

### Reopening in Read-Write Mode

The directory listing is initially opened read-only. When the first append occurs,
it must be reopened read-write so the appender can update cycle bounds and bump
modcount.

```zig
/// Reopen the directory listing with the specified protection.
///
/// C equivalent: directory_listing_reopen()
pub fn reopenDirlist(self: *Queue, prot: Protection) !void {
    // Close existing mapping and fd
    if (self.dirlist_mmap) |m| {
        mmap_ops.unmapFile(m);
        self.dirlist_mmap = null;
    }
    if (self.dirlist_fd) |fd| {
        posix.close(fd);
        self.dirlist_fd = null;
    }

    // Reopen with desired access mode
    const read_write = (prot == .read_write);
    const file = try openFileFs(
        self.dirname,
        config.v5_dirlist_name,
        read_write,
    );
    self.dirlist_fd = file.handle;

    // Get file size and map
    self.dirlist_file_size = try file_ops.fileSize(file.handle);
    self.dirlist_mmap = try mmap_ops.mapFile(
        file.handle,
        0,
        @intCast(self.dirlist_file_size),
        prot,
    );

    // Re-parse to discover field pointers within the new mapping
    try self.parseDirlist();

    // Verify all required field pointers were resolved
    if (self.dirlist_fields.highest_cycle == null or
        self.dirlist_fields.lowest_cycle == null or
        self.dirlist_fields.modcount == null)
    {
        return ChronicleError.DirlistFieldsMissing;
    }
}
```

### Polling modcount

Readers poll `modcount` to detect when the directory listing has been updated:

```zig
/// Poll the shared directory listing for changes to modcount.
/// If modcount has changed, refresh highest_cycle and lowest_cycle.
///
/// C equivalent: peek_queue_modcount()
pub fn peekModcount(self: *Queue) void {
    if (self.dirlist_fields.modcount) |mc_ptr| {
        const ptr: *const volatile u64 = @ptrCast(@alignCast(mc_ptr));
        const current_modcount = @atomicLoad(u64, ptr, .seq_cst);

        if (current_modcount != self.modcount) {
            self.modcount = current_modcount;

            if (self.dirlist_fields.lowest_cycle) |lc_ptr| {
                const lc: *const volatile u64 = @ptrCast(@alignCast(lc_ptr));
                self.lowest_cycle = @atomicLoad(u64, lc, .seq_cst);
            }
            if (self.dirlist_fields.highest_cycle) |hc_ptr| {
                const hc: *const volatile u64 = @ptrCast(@alignCast(hc_ptr));
                self.highest_cycle = @atomicLoad(u64, hc, .seq_cst);
            }
        }
    }
}
```

---

## 12. Zig-Specific Considerations

### Alignment

- `std.posix.mmap` returns `[]align(std.mem.page_size) u8`. This alignment is
  preserved in the type system and propagated through slicing.
- When casting pointers from the mmap buffer to typed pointers (e.g., `*u32` for
  header access, `*u64` for modcount), use `@ptrCast(@alignCast(ptr))`. In safe
  modes, the `@alignCast` will trap on misaligned pointers.
- The `align(1)` annotation on `DirlistFields` pointer types (from Task 1) tells
  the compiler these pointers may not be naturally aligned. However, the wire
  protocol's `uint64Aligned` function guarantees 8-byte alignment for these
  specific fields, so `@alignCast` should always succeed.

### Page Size

- `std.mem.page_size` is a compile-time constant. On most Linux systems, this is
  4096 bytes. On systems with larger pages (e.g., some ARM configurations with
  64KiB pages), the blocksize must still be a multiple of the page size.
- Add a comptime assertion: `comptime { std.debug.assert(config.default_blocksize % std.mem.page_size == 0); }`

### Volatile Pointers

- Fields that are shared between processes via `MAP_SHARED` mmap must be accessed
  through `volatile` pointers or atomic operations. In Zig, use `*volatile u32`
  or `*volatile u64` for header words and directory listing fields.
- Do **not** rely on `std.mem.readInt` for shared-memory reads — it does not
  generate volatile loads. Use `@atomicLoad` or explicit volatile pointer
  dereferences.

### Error Handling for mmap Failures

- `mmap` can fail with `ENOMEM` (out of memory/address space) or `EINVAL`
  (bad alignment, size, etc.). Both result in `MmapError.MmapFailed`.
- On `mmap` failure, the tailer returns `.err_mmap`. The caller should log the
  error and retry after a delay. Repeated failures are likely fatal.
- After `munmap`, always set the buffer pointer to `null` to avoid use-after-unmap.
  The `MmapWindow.unmap()` method does this automatically.

### Memory Ordering on Non-x86 Architectures

- The C code is x86-specific (inline `mfence` assembly). The Zig `@fence(.seq_cst)`
  and `@atomicLoad`/`@atomicStore` builtins are portable and will emit the correct
  instructions on any architecture.
- On x86-64, `@fence(.seq_cst)` compiles to `mfence`. On ARM64, it compiles to
  `dmb ish`. On RISC-V, it compiles to `fence iorw, iorw`.
- The `@cmpxchgStrong` builtin compiles to `lock cmpxchg` on x86-64 and the
  appropriate LL/SC sequence on other architectures.

### Safe Shutdown

- When a queue is closed (`Queue.deinit()`), all tailers must be closed first
  (which unmaps their queue file windows), then the directory listing mmap is
  unmapped, and finally all file descriptors are closed.
- The order matters: unmap before close, and tailers before queue.
- Zig's `defer` and `errdefer` patterns are ideal for ensuring cleanup order.

---

## 13. Complete Atomic Operations Module

Bringing together all atomic operations into one module:

```zig
// src/chronicle/atomic_ops.zig

const std = @import("std");

/// Atomic compare-and-swap on a 32-bit value.
/// Returns the value found in memory before the operation.
/// If the return value equals `expected`, the swap succeeded.
///
/// C equivalent: lock_cmpxchgl()
pub fn cmpxchg32(ptr: *volatile u32, expected: u32, desired: u32) u32 {
    const result = @cmpxchgStrong(u32, ptr, expected, desired, .seq_cst, .seq_cst);
    return result orelse expected;
}

/// Atomic compare-and-swap returning a bool.
/// Returns true if the swap succeeded.
pub fn tryCmpxchg32(ptr: *volatile u32, expected: u32, desired: u32) bool {
    return @cmpxchgStrong(u32, ptr, expected, desired, .seq_cst, .seq_cst) == null;
}

/// Atomic fetch-and-add on a 32-bit value. Returns the old value.
///
/// C equivalent: lock_xadd() with xaddl (32-bit)
pub fn atomicAdd32(ptr: *volatile u32, val: u32) u32 {
    return @atomicRmw(u32, ptr, .Add, val, .seq_cst);
}

/// Atomic fetch-and-add on a 64-bit value. Returns the old value.
pub fn atomicAdd64(ptr: *volatile u64, val: u64) u64 {
    return @atomicRmw(u64, ptr, .Add, val, .seq_cst);
}

/// Atomic load of a 32-bit value with acquire semantics.
pub fn atomicLoad32(ptr: *const volatile u32) u32 {
    return @atomicLoad(u32, ptr, .seq_cst);
}

/// Atomic load of a 64-bit value with acquire semantics.
pub fn atomicLoad64(ptr: *const volatile u64) u64 {
    return @atomicLoad(u64, ptr, .seq_cst);
}

/// Atomic store of a 32-bit value with release semantics.
pub fn atomicStore32(ptr: *volatile u32, val: u32) void {
    @atomicStore(u32, ptr, val, .seq_cst);
}

/// Atomic store of a 64-bit value with release semantics.
pub fn atomicStore64(ptr: *volatile u64, val: u64) void {
    @atomicStore(u64, ptr, val, .seq_cst);
}

/// Full memory fence (sequential consistency).
///
/// C equivalent: asm volatile ("mfence" ::: "memory")
pub inline fn fence() void {
    @fence(.seq_cst);
}

/// Read a 4-byte header from shared memory with proper atomic semantics.
/// Includes the memory fence to prevent speculative reads of the payload.
pub fn readHeader(ptr: [*]const u8) u32 {
    const aligned: *const volatile u32 = @ptrCast(@alignCast(ptr));
    const header = @atomicLoad(u32, aligned, .acquire);
    // On x86, acquire load is sufficient (no speculative reordering past loads).
    // For extra safety and to match the C mfence, add a full fence:
    @fence(.seq_cst);
    return header;
}

/// Write a final header to shared memory with proper atomic semantics.
/// Ensures all prior writes (payload) are visible before the header.
pub fn writeHeader(ptr: [*]u8, header: u32) void {
    const aligned: *volatile u32 = @ptrCast(@alignCast(ptr));
    // The seq_cst store ensures all prior writes are ordered before this one.
    @atomicStore(u32, aligned, header, .seq_cst);
}
```

---

## 14. Testing Strategy

### Unit Tests for Atomic Operations

```zig
test "cmpxchg32 success" {
    var value: u32 = 0x00000000; // UNALLOCATED
    const old = cmpxchg32(&value, 0x00000000, 0x80000000); // expect 0, write WORKING
    try std.testing.expectEqual(@as(u32, 0x00000000), old); // swap succeeded
    try std.testing.expectEqual(@as(u32, 0x80000000), value); // WORKING is set
}

test "cmpxchg32 failure" {
    var value: u32 = 0x80000000; // WORKING (someone else got there first)
    const old = cmpxchg32(&value, 0x00000000, 0x80000000); // expect 0, but find WORKING
    try std.testing.expectEqual(@as(u32, 0x80000000), old); // swap failed
    try std.testing.expectEqual(@as(u32, 0x80000000), value); // value unchanged
}

test "atomicAdd32" {
    var value: u32 = 5;
    const old = atomicAdd32(&value, 3);
    try std.testing.expectEqual(@as(u32, 5), old);
    try std.testing.expectEqual(@as(u32, 8), value);
}
```

### Unit Tests for mmap Window

```zig
test "computeWindow basic" {
    // tip=0, file_size=4MB, blocksize=1MB → offset=0, size=2MB
    const wp = computeWindow(0, 4 * 1024 * 1024, 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 0), wp.offset);
    try std.testing.expectEqual(@as(u64, 2 * 1024 * 1024), wp.size);
    try std.testing.expectEqual(@as(u64, 0), wp.tip_in_window);
}

test "computeWindow mid-file" {
    // tip=1.5MB, file_size=4MB, blocksize=1MB → offset=1MB, size=2MB
    const wp = computeWindow(1536 * 1024, 4 * 1024 * 1024, 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 1024 * 1024), wp.offset);
    try std.testing.expectEqual(@as(u64, 2 * 1024 * 1024), wp.size);
    try std.testing.expectEqual(@as(u64, 512 * 1024), wp.tip_in_window);
}

test "computeWindow near end of file" {
    // tip=3.5MB, file_size=4MB, blocksize=1MB → offset=3MB, size=1MB (clamped)
    const wp = computeWindow(3584 * 1024, 4 * 1024 * 1024, 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 3 * 1024 * 1024), wp.offset);
    try std.testing.expectEqual(@as(u64, 1024 * 1024), wp.size); // only 1MB remains
}
```

### Integration Tests

1. **mmap round-trip**: Create a temporary file, write known data, mmap it
   read-only, verify the data matches.
2. **Shared mmap writes**: Map a file read-write in one mapping, write data,
   map the same file read-only in a second mapping, verify changes are visible.
3. **Atomic CAS on mmap**: Map a file read-write, perform CAS operations on
   a 4-byte header word, verify correct serialization.
4. **File extension**: Create a file, extend it, verify the new size, map the
   extended region and verify it contains zeros.
5. **Window sliding**: Create a multi-megabyte file, write entries at known
   offsets, use `MmapWindow` to slide through the file and verify all entries
   are readable.

### Multi-Process Tests

For full correctness validation, test with multiple processes:

1. One process writes entries via CAS.
2. Another process reads entries, verifying header integrity and payload content.
3. Both processes use `modcount` polling to coordinate.

These tests require spawning child processes, which can be done with
`std.process.Child`.

---

## 15. Suggested File Layout

```
src/
└── chronicle/
    ├── atomic_ops.zig    — CAS, atomic add, fences, header read/write
    ├── mmap_ops.zig      — mapFile, unmapFile, remapFile
    ├── window.zig        — MmapWindow, computeWindow, needsRemap
    ├── file_ops.zig      — openFile, fileSize, extendFile, createSizedFile
    └── queuefile.zig     — createQueueFile (wraps file_ops)
```

---

## 16. Summary Checklist

| Component | C source | Zig module | Status |
|-----------|----------|------------|--------|
| File open/close | `open()`/`close()` calls | `file_ops.zig` | ☐ |
| File stat | `fstat()` calls | `file_ops.zig` | ☐ |
| File extend | `lseek`+`write` pattern | `file_ops.zig` | ☐ |
| File create | `open(O_CREAT\|O_TRUNC)` | `file_ops.zig` | ☐ |
| Atomic rename | `rename()` | `file_ops.zig` | ☐ |
| mmap / munmap | `mmap()`/`munmap()` | `mmap_ops.zig` | ☐ |
| MmapWindow struct | `qf_buf`/`qf_mmapoff`/`qf_mmapsz` | `window.zig` | ☐ |
| computeWindow | blocksize alignment logic | `window.zig` | ☐ |
| needsRemap | window comparison logic | `window.zig` | ☐ |
| queue_double_blocksize | `libchronicle.c:252-256` | `queue.zig` | ☐ |
| lock_cmpxchgl (CAS) | `libchronicle.c:216-222` | `atomic_ops.zig` | ☐ |
| lock_xadd (atomic add) | `libchronicle.c:224-231` | `atomic_ops.zig` | ☐ |
| mfence | `asm volatile("mfence")` | `atomic_ops.zig` | ☐ |
| readHeader (atomic) | `memcpy`+`mfence` pattern | `atomic_ops.zig` | ☐ |
| writeHeader (atomic) | `memcpy`+`mfence` pattern | `atomic_ops.zig` | ☐ |
| peekModcount | `libchronicle.c:788-800` | `queue.zig` | ☐ |
| pokeModcount | `libchronicle.c:802-810` | `queue.zig` | ☐ |
| File extension check | `libchronicle.c:895-901` | `file_ops.zig` | ☐ |
| Appender extend handler | `libchronicle.c:1143-1153` | `file_ops.zig` | ☐ |
| queuefile_init | `libchronicle.c:1370-1398` | `queuefile.zig` | ☐ |
| directory_listing_reopen | `libchronicle.c:1478-1498` | `queue.zig` | ☐ |
| Tailer state machine (mmap parts) | `libchronicle.c:824-965` | tailer peek fn | ☐ |
| Page alignment assertions | N/A (implicit in C) | comptime assert | ☐ |
| Volatile pointer access | N/A (implicit in C) | explicit volatile types | ☐ |