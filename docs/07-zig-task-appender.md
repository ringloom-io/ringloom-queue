# Task 5: Appender (Writer) Implementation

> **ringloom-queue** — lock-free, memory-mapped IPC queue in Zig

---

## Table of Contents

1. [Overview](#1-overview)
2. [Appender Struct](#2-appender-struct)
3. [The Hot Path: `append(msg)`](#3-the-hot-path-appendmsg)
4. [CAS Arbitration Detail](#4-cas-arbitration-detail)
5. [Cycle Roll Handling](#5-cycle-roll-handling)
6. [Pre-Roll Integration](#6-pre-roll-integration)
7. [Flat Index Updates](#7-flat-index-updates)
8. [Publication Without Core Wakeup](#8-publication-without-core-wakeup)
9. [File Extension](#9-file-extension)
10. [EOF Patching](#10-eof-patching)
11. [Performance Characteristics](#11-performance-characteristics)
12. [Error Handling](#12-error-handling)
13. [Testing Strategy](#13-testing-strategy)

---

## 1. Overview

The appender is the write-side of ringloom-queue. It is a **single active writer** component — only one
thread/process may hold the appender lease for a queue at a time. No lock is taken in the append
loop; the lifecycle lease prevents accidental concurrent appenders before the hot path starts.

The design prioritizes:

- **Zero allocations on the hot path** — all memory is pre-mapped; payload is written directly
  into the mmap region via the comptime codec interface.
- **Minimal syscalls on the hot path** — zero in the steady-state polling profile. The queue
  core does not signal blocking readers; applications that need wakeup own that policy.
- **Predictable latency** — a background prefetcher prepares future mappings/pages and pre-roll
  ensures cycle transitions don't stall the writer when the prefetcher keeps up.

### File Conventions

| Artifact | Extension / Name |
|---|---|
| Data files | `.ringloom` (one per cycle) |
| Shared metadata | `metadata.ringloom` |

---

## 2. Appender Struct

The appender is a specialized Tailer with write permissions. It re-uses the Tailer's mmap
window management, tip tracking, and file descriptor lifecycle, and layers write-specific
state on top.

```zig
pub const Appender = struct {
    queue: *Queue,
    tailer: Tailer,           // mmap window, tip, index, fd, etc.
    cas_attempt: u32 = 0,     // backoff iteration counter
    preroll_checked: bool = false,

    pub fn init(queue: *Queue) !Appender {
        var tailer = try Tailer.init(queue, .write);
        return Appender{
            .queue = queue,
            .tailer = tailer,
        };
    }

    pub fn deinit(self: *Appender) void {
        self.tailer.deinit();
    }
};
```

Key points:

- `tailer` holds the current file descriptor, mmap base pointer, mmap window offset/length,
  tip (byte offset into the current cycle file), and cached file size.
- `cas_attempt` tracks how many consecutive CAS failures have occurred so the tiered backoff
  can choose the right strategy.
- `preroll_checked` is a per-append flag to avoid redundant pre-roll arithmetic.

---

## 3. The Hot Path: `append(msg)`

This is the most performance-critical code in the entire library. Every nanosecond matters.

### High-level flow

1. **`codec.serialized_size(msg)`** → compute payload size. This is an inline, comptime-
   dispatched function call. No allocation, no syscall.

2. **Compute total entry size**: `4 + payload + pad4`. The 4-byte header is always present.
   Padding rounds up to the next 4-byte boundary for alignment.

3. **Check modcount** — volatile `u64` read from the mmap'd `metadata.ringloom` region. If
   modcount has changed since our last check, refresh cached queue parameters (highest
   cycle, etc.). This is a single memory read — no syscall.

4. **Check for cycle roll** — compare the current wall-clock cycle number against the
   appender's cycle before claiming a data slot.
   - If a roll is needed: write an EOF marker to the current position, swap to the
     pre-rolled/pre-touched file if available, or create/map/touch a new file as a fallback.

5. **Ensure mmap window covers the current tip**. In steady state the tip is within the
   already-mapped/pre-touched region and this is a simple bounds check (no-op). At a block
   boundary, swap in a ready window from the prefetcher; synchronous mmap/populate is only a
   fallback and should be counted as a prefetch miss.

6. **CAS: `UNALLOCATED → WORKING`** — a single `lock cmpxchg` instruction on the 4-byte
   header at the tip. Uses `.monotonic` / `.monotonic` ordering; the final DATA header release
   store publishes the payload. On failure: recovery/backoff policy handles the unexpected
   busy slot (see §4).

7. **`codec.write(mmap_buf + 4, msg, size)`** — serialize the payload directly into the
   mmap region, starting 4 bytes past the header. No intermediate buffer, no allocation.

8. **Release store: write `header = DATA | size`** — this publishes the message. On x86
   this compiles to a plain `MOV` (store-release is free on x86 TSO). On ARM this would
   be an `stlr`. No `mfence` needed.

9. **Update flat index** — every `index_spacing` messages, atomically store the current
   byte offset as a `u64` into the index region. Single atomic store, no syscall.

10. **Advance tip and seqnum** — pure arithmetic on local state, followed by a release-store
    of `metadata.write_position`.

### Syscall analysis (steady state)

| Step | Operation | Syscalls |
|---|---|---|
| 1 | Payload size computation | 0 |
| 2 | Entry size arithmetic | 0 |
| 3 | Modcount check | 0 (mmap read) |
| 4 | Window bounds check | 0 (usually no-op) |
| 5 | CAS claim | 0 (atomic instruction) |
| 6 | Cycle check | 0 (arithmetic) |
| 7 | Payload write | 0 (mmap write) |
| 8 | Header publish | 0 (mmap write) |
| 9 | Index update | 0 (mmap write, every N) |
| 10 | Tip/seqnum advance | 0 |
| **Total** | | **0** |

### Full implementation sketch

```zig
pub fn append(self: *Appender, msg: anytype) !void {
    const size = codec.serializedSize(msg);
    const pad = (4 - (size % 4)) % 4;
    const total = @as(u32, 4 + size + pad);

    // Step 3: modcount refresh (volatile mmap read, no syscall)
    self.refreshModcountIfChanged();

    // Step 4: cycle roll check before claiming a data slot
    const now_ms = self.queue.clockMs();
    const current_cycle = self.queue.cycleFromMs(now_ms);
    if (current_cycle > self.tailer.cycle) {
        try self.rollCycle(current_cycle);
    }

    // Step 5: ensure mmap window covers tip
    try self.tailer.ensureWindow(self.tailer.tip, total);

    // Step 6: CAS claim
    const header_ptr = self.headerPtrAtTip();
    try self.claimSlot(header_ptr);

    // Step 7: write payload directly into mmap
    const payload_ptr = self.payloadPtrAtTip();
    codec.write(payload_ptr, msg, size);

    // Zero padding bytes
    if (pad > 0) {
        @memset(payload_ptr[size..][0..pad], 0);
    }

    // Step 8: publish header (release store — plain MOV on x86)
    @atomicStore(u32, header_ptr, Header.DATA | size, .release);

    // Step 9: flat index update
    self.maybeUpdateIndex();

    // Step 10: advance tip and seqnum
    self.tailer.tip += total;
    self.tailer.seqnum += 1;
    @atomicStore(u64, &self.queue.metadata.?.write_position, self.tailer.tip, .release);

    // Pre-roll check (cheap branch, almost always false)
    self.maybePreroll(now_ms);
}
```

---

## 4. CAS Arbitration Detail

The appender lease ensures there is only one active writer. The CAS on the entry header
marks an entry as in-progress while the payload is being copied; it is not the global
writer arbitration mechanism.

### Header constants

```zig
pub const Header = struct {
    pub const UNALLOCATED: u32 = 0x00000000;
    pub const WORKING:     u32 = 0x80000000;  // simplified — no PID encoding
    pub const EOF:         u32 = 0x00000000;  // with EOF flag in high bits
    pub const DATA:        u32 = 0x00000000;  // high bit clear, low 30 bits = size

    // Mask helpers
    pub const SIZE_MASK:   u32 = 0x3FFFFFFF;
    pub const FLAG_MASK:   u32 = 0xC0000000;
};
```

The WORKING marker is a simple `0x80000000` — no PID is encoded. This simplifies the header
format and avoids the need to parse out a PID on the reader side.

### CAS loop with tiered backoff

```zig
fn claimSlot(self: *Appender, ptr: *volatile u32) !void {
    self.cas_attempt = 0;
    while (true) {
        const result = @cmpxchgStrong(
            u32,
            ptr,
            Header.UNALLOCATED,
            Header.WORKING,
            .monotonic,  // success: claim only; final DATA header publishes
            .monotonic,  // failure: no ordering needed for retry/recovery
        );
        if (result == null) {
            // Success — we own the slot
            self.cas_attempt = 0;
            return;
        }
        // Busy/corrupt slot — normally recovery or a stale WORKING marker
        try casBackoff(&self.cas_attempt);
    }
}
```

### Tiered backoff strategy

```zig
fn casBackoff(attempt: *u32) !void {
    const n = attempt.*;
    attempt.* += 1;

    if (n < 64) {
        // Tier 1: spin with pause hint (0–64 iterations)
        // On x86 this emits PAUSE; on ARM it emits YIELD.
        // Cost: ~5-40ns per iteration.
        std.atomic.spinLoopHint();
    } else if (n < 256) {
        // Tier 2: yield to OS scheduler (64–256 iterations)
        // Allows other threads/processes to make progress.
        std.Thread.yield() catch {};
    } else {
        // Tier 3: exponential backoff capped at 1ms (256+ iterations)
        // Backs off exponentially: 1μs, 2μs, 4μs, ... 1ms max.
        const shift = @min(n - 256, 10); // cap shift at 10 → 1024μs ≈ 1ms
        const us: u64 = @as(u64, 1) << @intCast(shift);
        const capped = @min(us, 1000); // hard cap at 1ms
        std.time.sleep(capped * std.time.ns_per_us);
    }
}
```

**Why three tiers?**

| Tier | Range | Latency | Use Case |
|---|---|---|---|
| Spin | 0–63 | ~5–40 ns/iter | Brief contention (ns-scale) |
| Yield | 64–255 | ~1–10 μs | Short contention (μs-scale) |
| Exponential | 256+ | 1 μs – 1 ms | Stale WORKING marker or recovery stall |

This replaces the previous approach of sleeping for 1 second on contention, which was far
too coarse for a low-latency queue. The tiered scheme gives sub-microsecond response for
the common case (brief contention) while still backing off gracefully under sustained load.

---

## 5. Cycle Roll Handling

A cycle roll occurs when the wall-clock time crosses into a new cycle boundary (e.g., a new
day for `DAILY` roll cycle, a new hour for `HOURLY`). The appender must close the current
`.ringloom` data file and start writing to a new one.

### With pre-roll (fast path)

When the pre-roll system has already prepared the next cycle file, the roll is nearly free:

1. **Detect roll**: `current_cycle > appender_cycle` (arithmetic comparison).
2. **Write EOF marker** to the current position in the current file — a single 4-byte mmap
   write of the EOF header constant.
3. **Check pre-roll availability**: `queue.preroll_cycle == current_cycle`.
   - If true: swap the file descriptor and mmap pointers from the pre-roll state into the
     appender's tailer. **Zero syscalls** — just pointer assignments.
4. **Update `highest_cycle`** in `metadata.ringloom` — atomic store to the mmap'd metadata region.
5. **Bump `modcount`** — atomic fetch-add on the mmap'd modcount field. This notifies readers
   that queue metadata has changed.
6. **Reset tip** to the data start offset in the new file.
7. **Retry append** — return to the top of `append()` to write the message into the new file.

```zig
fn rollCycle(self: *Appender, current_header_ptr: *volatile u32, new_cycle: u64) !void {
    // Write EOF to current position
    @atomicStore(u32, current_header_ptr, Header.EOF_MARKER, .release);

    if (self.queue.preroll_cycle == new_cycle) {
        // Fast path: swap pre-rolled file into active position
        self.tailer.fd = self.queue.preroll_fd;
        self.tailer.mmap_base = self.queue.preroll_mmap;
        self.tailer.mmap_len = self.queue.preroll_mmap_len;
        self.queue.preroll_fd = null;
        self.queue.preroll_mmap = null;
        self.queue.preroll_cycle = 0;
    } else {
        // Slow path: create new cycle file on demand
        try self.createCycleFile(new_cycle);
    }

    self.tailer.cycle = new_cycle;
    self.tailer.tip = self.queue.data_start_offset;

    // Update shared metadata
    self.queue.atomicStoreHighestCycle(new_cycle);
    self.queue.bumpModcount();
}
```

### Without pre-roll (slow path — fallback)

If the pre-roll system hasn't prepared the next file (e.g., rapid successive rolls, or the
pre-roll window was missed), the appender must create the file synchronously:

1. Compute the filename: `{cycle_number}.ringloom`.
2. Create the file with platform preallocation to reserve disk blocks.
3. `mmap` and pre-touch using the best available platform populate path.
4. Write the file header.
5. Continue with the roll as above.

This path involves multiple syscalls (`open`, preallocation, `fstat`, `mmap`) and is
significantly slower. The pre-roll system exists specifically to avoid this on the hot path.

---

## 6. Pre-Roll Integration

At the end of each append, a cheap check determines whether it's time to pre-create the
next cycle file:

```zig
fn maybePreroll(self: *Appender, now_ms: i64) void {
    if (shouldPreroll(now_ms, self.queue)) {
        self.queue.maybePreroll(now_ms) catch {};
    }
}

fn shouldPreroll(now_ms: i64, queue: *Queue) bool {
    // Already pre-rolled?
    if (queue.preroll_cycle != 0) return false;

    // How close are we to the next cycle boundary?
    const cycle_end_ms = queue.cycleEndMs(queue.current_cycle);
    const remaining_ms = cycle_end_ms - now_ms;

    // Pre-roll when we're within the pre-roll window (e.g., last 10% of cycle)
    return remaining_ms <= queue.preroll_window_ms;
}
```

This is a **single branch** on the hot path — a comparison of two integers. It is almost
always `false`. When it evaluates to `true`, that one append call pays the cost of file
creation. But because the actual roll later just swaps pointers, the cost is amortized
over the many thousands of appends that follow.

### Pre-roll file creation

```zig
fn maybePreroll(queue: *Queue, now_ms: i64) !void {
    const next_cycle = queue.current_cycle + 1;
    const filename = try queue.cycleFilename(next_cycle); // e.g., "20250615.ringloom"

    // Create and pre-allocate the file
    const fd = try std.posix.open(filename, .{ .ACCMODE = .RDWR, .CREAT = true }, 0o644);
    try queue.platform.preallocate(fd, 0, queue.initial_file_size);
    try std.posix.ftruncate(fd, queue.initial_file_size);

    // mmap with platform populate/pre-touch support
    const mmap_ptr = try std.posix.mmap(
        null,
        queue.initial_file_size,
        std.posix.PROT.READ | std.posix.PROT.WRITE,
        .{ .TYPE = .SHARED, .POPULATE = true },
        fd,
        0,
    );

    // Write initial file header
    queue.writeFileHeader(mmap_ptr);

    // Store pre-roll state for the fast-path swap
    queue.preroll_fd = fd;
    queue.preroll_mmap = mmap_ptr;
    queue.preroll_mmap_len = queue.initial_file_size;
    queue.preroll_cycle = next_cycle;
}
```

---

## 7. Flat Index Updates

The appender maintains a flat inline index to allow readers to seek efficiently. Every
`index_spacing` messages, it stores the byte offset of that message's entry in the index
region of the file.

```zig
fn maybeUpdateIndex(self: *Appender) void {
    const seqnum = self.tailer.seqnum;
    const spacing = self.queue.index_spacing;

    if (seqnum % spacing == 0) {
        const slot = seqnum / spacing;
        if (slot < self.queue.index_count) {
            const index_ptr = self.indexSlotPtr(slot);
            @atomicStore(u64, index_ptr, self.tailer.tip, .release);
        }
    }
}
```

This is a single atomic `u64` store — no syscall, no allocation. With the default
`index_spacing = 256`, this executes once per 256 appends. The modulo check compiles to
a bitwise AND when the spacing is a power of two.

### Index region layout

The index region is a contiguous array of `u64` values at a fixed offset within each `.ringloom`
data file:

```
[file header][index region: N × u64][data region: entries...]
```

Readers use the index to binary-search for a target sequence number, then scan forward from
the nearest indexed offset. This avoids a full linear scan from the start of the file.

---

## 8. Publication Without Core Wakeup

After publishing the header and `write_position`, the appender increments
`metadata.modcount` and returns. It does not signal readers in the core design.
This preserves the zero-syscall steady-state append path and keeps the queue
portable across Linux and macOS.

Applications that need blocking reader wakeup can layer it outside the queue:

- Same-process readers can use application condition variables after the appender
  call returns.
- Event-loop users can combine `Tailer.poll()` with timers or runtime-specific
  reactors.
- Cross-process notification can be provided by an application-owned mechanism,
  but it is not part of the queue correctness protocol.

Any external wakeup should be treated as "poll again", not as proof that exactly
one message is available. Readers must tolerate coalesced, stale, or spurious
wakeups.

---

## 9. File Extension

When the remaining space in the current mmap window drops below `2 × blocksize`, the
prefetcher should already have extended the file using platform preallocation and prepared the next
mapping. The appender function below is a correctness fallback and should increment a
prefetch-miss diagnostic because it can add jitter.

```zig
fn extendFileIfNeeded(self: *Appender) !void {
    const remaining = self.tailer.file_size - self.tailer.tip;
    const threshold = 2 * self.queue.blocksize;

    if (remaining < threshold) {
        const new_size = self.tailer.file_size + self.queue.qf_disk_size;

        // Pre-allocate disk blocks (avoids ENOSPC on later writes)
        try self.queue.platform.preallocate(
            self.tailer.fd,
            @intCast(self.tailer.file_size),
            @intCast(new_size - self.tailer.file_size),
        );
        try std.posix.ftruncate(self.tailer.fd, new_size);

        // Update cached file size (fstat)
        const stat = try std.posix.fstat(self.tailer.fd);
        self.tailer.file_size = @intCast(stat.size);

        // Remap and pre-touch if needed to cover new region
        try self.tailer.remapWindow(self.tailer.tip);
        try self.tailer.touchWritableWindow();
    }
}
```

Platform preallocation reserves disk blocks without relying on sparse-file
growth, avoiding `ENOSPC` surprises during later mmap writes. Linux should use
`fallocate`; macOS should use `F_PREALLOCATE` followed by `ftruncate`. The remap
uses the best available platform populate/pre-touch path to pre-fault pages in
the new region.

---

## 10. EOF Patching

If a previous writer crashed while holding WORKING (i.e., between the CAS and the header
publish), the entry is left in a partially-written state. A subsequent appender detects this
and patches in an EOF marker so readers don't get stuck.

```zig
fn patchEof(self: *Appender, cycle: u32) !void {
    // Only look back a limited number of cycles
    const start_cycle = if (cycle > self.queue.patch_cycles)
        cycle - self.queue.patch_cycles
    else
        0;

    var c = start_cycle;
    while (c < cycle) : (c += 1) {
        const filename = try self.queue.cycleFilename(c);
        const fd = std.posix.open(filename, .{ .ACCMODE = .RDWR }, 0o644) catch continue;
        defer std.posix.close(fd);

        const stat = try std.posix.fstat(fd);
        const mmap_ptr = try std.posix.mmap(
            null,
            @intCast(stat.size),
            std.posix.PROT.READ | std.posix.PROT.WRITE,
            .{ .TYPE = .SHARED },
            fd,
            0,
        );
        defer std.posix.munmap(mmap_ptr);

        // Scan forward from data start, find last valid entry, write EOF after it
        var offset = self.queue.data_start_offset;
        while (offset < stat.size) {
            const header = @as(*volatile u32, @ptrCast(@alignCast(mmap_ptr + offset))).*;
            if (header == Header.UNALLOCATED or header == Header.WORKING) {
                // Patch this as EOF
                @atomicStore(u32, @ptrCast(@alignCast(mmap_ptr + offset)), Header.EOF_MARKER, .release);
                break;
            }
            const entry_size = 4 + (header & Header.SIZE_MASK);
            const padded = (entry_size + 3) & ~@as(u32, 3);
            offset += padded;
        }
    }
}
```

The `patch_cycles` parameter limits how far back the appender looks. Typically this is a
small number (e.g., 2–4 cycles) to avoid scanning ancient files on startup.

---

## 11. Performance Characteristics

| Operation | Steady-State Cost |
|---|---|
| Payload size computation | Inline function call (comptime-dispatched) |
| Modcount check | Volatile `u64` read from mmap |
| CAS claim | Single `lock cmpxchg` instruction |
| Payload write | `memcpy` into mmap (or inline codec write) |
| Header publish | Release store (plain `MOV` on x86) |
| Index update | Atomic `u64` store (every N messages) |
| Padding | 0–3 zero bytes |
| **Total syscalls (steady state)** | **0** |
| **Allocations (steady state)** | **0** |

### Memory ordering summary

| Operation | Ordering | x86 Instruction |
|---|---|---|
| CAS (success) | `.monotonic` | `lock cmpxchg` |
| CAS (failure) | `.monotonic` | (implicit in `lock cmpxchg`) |
| Header publish | `.release` | `MOV` (TSO gives release for free) |
| Index store | `.release` | `MOV` |
| Modcount read | `.acquire` (volatile) | `MOV` (TSO gives acquire for free) |

No `mfence` or `SeqCst` operations are used on the write path. The x86 TSO memory model
makes release stores and acquire loads free (they compile to plain `MOV`). On ARM/AArch64,
the compiler would emit `stlr` / `ldar` as needed.

---

## 12. Error Handling

### Recoverable errors

| Error | Cause | Recovery |
|---|---|---|
| `MmapFailed` | Kernel refuses mmap (OOM, address space exhaustion) | Return error to caller; caller can retry |
| `PreallocateFailed` | Disk full or filesystem doesn't support strict preallocation | Return error in strict low-jitter mode; weaker fallback may be configured |
| `FileCreateFailed` | Permission denied, path doesn't exist | Return error; queue directory misconfigured |
| `CycleRollFailed` | Failed to create new cycle file during slow-path roll | Return error; pre-roll avoids this in normal operation |

### Non-recoverable states

| State | Cause | Behavior |
|---|---|---|
| Corrupted header | Bit flip in mmap region | Detected by readers via invalid header; appender may overwrite |
| Stuck WORKING | Writer crashed between CAS and publish | Next appender patches EOF on startup (see §10) |

### Error propagation

The `append()` function returns `!void` — it propagates errors to the caller via Zig's
error union. The hot path itself does not allocate error objects. Errors from file operations
(preallocation, mmap, open) are propagated as-is from `std.posix` or the platform layer.

---

## 13. Testing Strategy

### Unit tests

- **Append and read-back verification**: Write a message via the appender, read it back via
  a tailer, and assert byte-for-byte equality.
- **Size computation**: Verify that `codec.serializedSize()` matches actual serialized output
  for all supported types.
- **Padding correctness**: Verify that entries are always 4-byte aligned and padding bytes
  are zero.

### Lease and header-state tests

- **Multi-process appender lease**: Fork two processes and verify exactly one can acquire
  the appender lease; the other receives `AppenderAlreadyOpen`.
- **WORKING recovery/backoff**: Seed a WORKING header and verify retry/backoff or recovery
  policy does not corrupt adjacent entries.
- **Backoff timing**: Instrument the backoff function and verify that tier transitions happen
  at the correct iteration counts (64, 256).

### Cycle roll tests

- **Roll transition correctness**: Write messages spanning a cycle boundary. Verify that:
  - EOF is written at the end of the old cycle file.
  - The new cycle file starts with a valid header.
  - No messages are lost or duplicated across the boundary.
- **Pre-roll timing verification**: Set a short cycle duration (e.g., 100ms) and verify that
  the pre-roll file is created within the pre-roll window.
- **Pre-roll swap**: Verify that when pre-roll is available, the roll completes without any
  `open`/`mmap` syscalls (use `strace` or syscall counting).

### Index tests

- **Index update verification**: Write N × `index_spacing` messages. Verify that each index
  slot contains the correct byte offset for the corresponding message.
- **Seek via index**: Use the index to seek to a message by sequence number and verify that
  the message at that offset is correct.

### EOF patching tests

- **Crash simulation**: Write a WORKING header without a subsequent DATA header (simulating
  a crash). Start a new appender and verify it patches the stale WORKING header to EOF.
- **Multiple stale cycles**: Create stale WORKING headers across several cycle files and
  verify all are patched within `patch_cycles` range.

### Stress tests

- **Throughput benchmark**: Measure messages/second for a single-writer appender with payloads
  of 64, 256, 1024, and 4096 bytes.
- **Latency benchmark**: Measure p50/p99/p999 append latency using `std.time.Timer`.
- **File extension under load**: Write enough messages to trigger multiple file extensions and
  verify no corruption at extension boundaries.
