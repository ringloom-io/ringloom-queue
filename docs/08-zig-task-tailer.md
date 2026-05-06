# Task 6: Tailer (Reader) Implementation

## Table of Contents

1. [Overview](#1-overview)
2. [Tailer Struct](#2-tailer-struct)
3. [Thread Safety Model](#3-thread-safety-model)
4. [The Hot Path: `poll()`](#4-the-hot-path-poll)
5. [Waiting and Read Prefetch](#5-waiting-and-read-prefetch)
6. [Index-Based Seeking](#6-index-based-seeking)
7. [Collect Mode (Blocking Iterator)](#7-collect-mode-blocking-iterator)
8. [Modcount Protocol](#8-modcount-protocol)
9. [Performance Characteristics](#9-performance-characteristics)
10. [Error Handling](#10-error-handling)
11. [Testing Strategy](#11-testing-strategy)

---

## 1. Overview

The tailer is the read-side of ringloom-queue. Multiple tailers can read from the same queue
concurrently, each maintaining fully independent state. There is no shared mutable state
between tailers — the only shared data is the queue's memory-mapped files, accessed
read-only by tailers with memory ordering handled by acquire loads.

The design supports two consumption patterns:

- **Polling (non-blocking)** — call `poll()` which returns immediately with a message or `null`.
- **Application-owned waiting** — callers that need blocking behavior wrap `poll()` with their own spin/yield/sleep or OS/runtime notification policy.

Key design principles:

- **Zero syscalls on the hot path** — steady-state reads are pure mmap pointer arithmetic.
- **Zero allocations on the hot path** — all buffers are pre-mapped; payloads are zero-copy slices into the mmap window.
- **Acquire loads only** — on x86 (TSO), acquire loads compile to plain MOV instructions. No fence instructions are ever emitted.
- **Pre-mapped/read-prefetched windows** — when the read tip crosses the configured runway, the next published window is pre-mapped and read-prefetched to avoid stalls at block boundaries.
- **O(log n) seeking** — the flat inline index supports binary search for fast resume from a saved position.

---

## 2. Tailer Struct

```zig
pub const Tailer = struct {
    queue: *Queue,
    dispatch_after: u64,       // skip entries at or below this index
    state: TailerState,
    mmap_protection: MmapProtection,

    // Current file state
    qf_cycle_open: u32 = 0,
    qf_filename: ?[]const u8 = null,
    qf_fd: ?posix.fd_t = null,
    qf_file_size: u64 = 0,

    // Position tracking
    qf_tip: u64 = 0,          // byte offset of next header
    qf_index: u64 = 0,        // full 64-bit index (cycle:seqnum)

    // mmap window (current)
    qf_buf: ?[]align(page_size) u8 = null,
    qf_mmapoff: u64 = 0,
    qf_mmapsz: u64 = 0,

    // Pre-mapped next window
    next_buf: ?[]align(page_size) u8 = null,
    next_mmapoff: u64 = 0,
    next_mmapsz: u64 = 0,

    // Read prefetch state
    read_prefetch: ReadPrefetchState = .{},
};
```

### Field Descriptions

| Field | Purpose |
|---|---|
| `queue` | Pointer to the parent `Queue` (shared, read-only access to metadata mmap) |
| `dispatch_after` | Index threshold — entries at or below this value are skipped (used for resumption) |
| `state` | Current tailer state (`awaiting_entry`, `busy`, `collected`, etc.) |
| `mmap_protection` | `PROT.READ` for readers (always read-only for tailers) |
| `qf_cycle_open` | Cycle number of the currently open `.ringloom` data file |
| `qf_filename` | Path to the currently open `.ringloom` data file |
| `qf_fd` | File descriptor for the current data file |
| `qf_file_size` | Size of the current data file (from fstat) |
| `qf_tip` | Byte offset within the data file where the next header is expected |
| `qf_index` | Full 64-bit index encoding both cycle and sequence number |
| `qf_buf` | Pointer to the current mmap window |
| `qf_mmapoff` | File offset where the current mmap window starts |
| `qf_mmapsz` | Size of the current mmap window |
| `next_buf` | Pointer to the pre-mapped next window (or `null` if not yet mapped) |
| `next_mmapoff` | File offset where the pre-mapped next window starts |
| `next_mmapsz` | Size of the pre-mapped next window |
| `read_prefetch` | Tracks the next published range to pre-map, advise, and optionally read-touch |

---

## 3. Thread Safety Model

Each tailer instance owns its own:

- **mmap window** — buffer pointer, offset, and size
- **File descriptor** — independently opened
- **Position** — tip and index
- **State** — tailer state machine

No shared mutable state exists between tailers. Multiple threads can each create and
operate their own tailer concurrently without any synchronization.

The only shared data is the queue's memory-mapped files. These are accessed **read-only**
by tailers. Memory ordering between the writer (which publishes entries) and readers
(which consume them) is handled entirely by acquire/release semantics:

- The **writer** uses a release store when writing the entry header (marking it as DATA).
- The **reader** uses an acquire load when reading the entry header.

On x86 (TSO architecture), acquire loads compile to plain `MOV` instructions — there is
no fence overhead. The hardware memory model already guarantees that loads are not
reordered with respect to other loads, which is exactly the ordering we need.

---

## 4. The Hot Path: `poll()`

This is the read-side critical path. In steady state, it involves **zero syscalls** and
**zero allocations** — just pointer arithmetic over mmap'd memory.

### Step 1: Modcount Check

Read the modcount from the shared metadata file (acquire load on mmap'd `u64`). If
the modcount has not changed since the last check, return `null` immediately — no new
data has been published.

```zig
const current_modcount = @atomicLoad(u64, self.queue.metadata_modcount_ptr, .acquire);
if (current_modcount == self.last_modcount) return null;
```

On x86, this is a plain `MOV` — free.

### Step 2: Cycle Management

Extract the cycle number from the current index. If it differs from the currently open
cycle:

1. Close the old file (`munmap` current and pre-mapped windows, `close` fd).
2. Open the new cycle's `.ringloom` data file.
3. `mmap` the first window with `madvise(MADV_SEQUENTIAL)` for kernel read-ahead.
4. Reset `qf_tip` to the start of the data region (past the file header).

```zig
const cycle = Index.cycle(self.qf_index);
if (cycle != self.qf_cycle_open) {
    try self.closeCycleFile();
    try self.openCycleFile(cycle);
}
```

### Step 3: Ensure mmap Window

Three cases, in order of likelihood:

1. **Tip is within current window** — continue (no syscall). This is the common case.
2. **Read-prefetched window covers the tip** — swap windows (pointer swap, defer old unmap). The new mapping has already received read-ahead hints and optional read-touch.
3. **Neither covers the tip** — full remap: `munmap` + `mmap` + read-ahead hints. This is a read-prefetch miss.

```zig
if (self.tipInWindow()) {
    // Fast path: nothing to do
} else if (self.tipInNextWindow()) {
    self.swapToNextWindow();
} else {
    try self.remapWindow();
}
```

### Step 4: Parse Message Header

```zig
const tip_offset = self.qf_tip - self.qf_mmapoff;
const header_ptr: *const volatile u32 = @ptrCast(@alignCast(self.qf_buf.?[tip_offset..].ptr));
const header = @atomicLoad(u32, header_ptr, .acquire);
```

On x86, this is a plain `MOV` — TSO guarantees load-load ordering. No `mfence` or
`LOCK` prefix needed.

### Step 5: Dispatch on Header Type

The header's top 2 bits encode the entry type:

| Header Value | Meaning | Action |
|---|---|---|
| `0x00000000` (UNALLOCATED) | No entry written here yet | Return `null` |
| `0x80000000` (WORKING) | Writer is mid-write | Return `.busy` |
| `0x00xxxxxx` (DATA) | Data entry, length in lower 30 bits | Parse payload, dispatch |
| `0x40xxxxxx` (METADATA) | Metadata entry | Skip (advance tip by `4 + size + pad4`) |
| `0xC0000000` (EOF) | End of cycle file | Advance to next cycle |

```zig
const entry_type = header & HD_MASK_META;
switch (entry_type) {
    HD_UNALLOCATED => return null,
    HD_WORKING => return .busy,
    HD_EOF => {
        self.qf_index = Index.compose(Index.cycle(self.qf_index) + 1, 0);
        return try self.poll(); // recurse into next cycle
    },
    HD_METADATA => {
        const sz = header & HD_MASK_LENGTH;
        self.qf_tip += 4 + sz + pad4(sz);
        return try self.poll(); // skip metadata, try next entry
    },
    else => {}, // DATA — fall through to payload parsing
}
```

### Step 6: Parse Payload (DATA Entries)

```zig
const payload_size = Header.dataLength(header);
const payload = self.qf_buf.?[tip_offset + 4 .. tip_offset + 4 + payload_size];
const msg = codec.parse(payload) orelse return error.ParseFailed;
```

**Zero-copy**: `payload` is a slice directly into the mmap buffer. No allocation, no
memcpy. The comptime codec's `parse` function returns a value that may reference this
slice — the caller must consume or copy the data before the next `poll()` call (which
may remap the window).

### Step 7: Read-Prefetch Next Window Check

When the tip crosses the configured read-prefetch runway, request preparation of
the next published block so that the transition is a cheap pointer swap rather
than a blocking `mmap` plus page faults:

```zig
if (shouldPremap(self.qf_tip - self.qf_mmapoff, self.qf_mmapsz)) {
    self.prefetchReadWindow();
}
```

```zig
fn shouldPremap(offset_in_window: u64, window_size: u64) bool {
    return offset_in_window >= window_size / 2;
}
```

The read-prefetched window uses `mmap` + platform read-ahead hints on the next
aligned block. It is bounded by an acquire load of `metadata.write_position` and
published cycle headers; it must never read into fallocated-but-unwritten space.
When Step 3 later detects that the tip has moved past the current window, it
swaps to the pre-mapped window in O(1) and defers old-window unmap to the cleaner.

### Step 8: Advance Tip

```zig
self.qf_tip += 4 + payload_size + pad4(payload_size);
self.qf_index += 1;
```

Where `pad4` aligns to the next 4-byte boundary:

```zig
fn pad4(sz: u32) u32 {
    return (4 -% (sz & 0x03)) & 0x03;
}
```

---

## 5. Waiting and Read Prefetch

The core tailer API is non-blocking. `poll()` returns `null` when no data is
currently available. This is deliberate: the queue data path is memory mapped, so
kernel I/O submission does not improve steady-state read latency, and appender
notification syscalls would add jitter to the writer.

```zig
pub fn collect(self: *Tailer, backoff: BackoffPolicy) !Entry {
    while (true) {
        if (try self.poll()) |entry| return entry;
        backoff.wait();
    }
}
```

### Latency Characteristics

| Scenario | Behavior |
|---|---|
| Data already available | `poll()` returns it with no waiting overhead |
| No data available | `poll()` returns `null`; caller controls spin/yield/sleep/notifier |
| Blocking convenience | Optional Zig `collect()` is just a `poll()` loop with caller-selected backoff |

### Why no core kernel wakeup

The queue avoids a built-in kernel wakeup path for three reasons:

- The mmap read path does not submit I/O, so kernel async I/O APIs do not improve
  steady-state `poll()` latency.
- Cross-platform support is simpler: Linux and macOS both support mmap and
  polling, while notification mechanisms differ.
- Any writer-to-reader wakeup syscall paid by the appender is a latency tradeoff
  that some deployments explicitly do not want.

Applications can still layer their own waiting strategy around `poll()`, such as
short spinning, `std.Thread.yield`, timers, condition variables within one
process, or application-owned `epoll`/`kqueue` integration.

### Read-prefetch poll

Tailer read prefetch may be driven by the queue prefetcher or by a per-tailer
poll call:

```zig
pub fn prefetchPoll(self: *Tailer, max_work_units: u32) !StepResult {
    const published = @atomicLoad(u64, &self.queue.metadata.?.write_position, .acquire);
    const target = self.computeReadPrefetchTarget(published);
    return try self.prepareReadWindow(target, max_work_units);
}
```

The prefetch range is clamped to published bytes, and repeated requests from
many tailers should be coalesced or budgeted so a fan-out workload does not issue
duplicate read-ahead/touch work for the same pages.

---

## 6. Index-Based Seeking

When a tailer needs to seek to a specific index (e.g., resuming from a saved position),
it can use the flat inline index to binary search for the nearest entry in O(log n),
then scan forward.

```zig
pub fn seekTo(self: *Tailer, target_index: u64) !void {
    const target_cycle = Index.cycle(target_index);
    const target_seqnum = Index.seqnum(target_index);

    // Open the target cycle file
    try self.openCycleFile(target_cycle);

    // Binary search the flat index for the nearest entry
    const index_region = self.getIndexRegion();
    const target_slot = target_seqnum / index_region.spacing;

    // Find the highest populated slot <= target_slot
    var lo: u32 = 0;
    var hi: u32 = target_slot;
    while (lo < hi) {
        const mid = lo + (hi - lo + 1) / 2;
        if (index_region.lookup(mid)) |_| {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }

    // Seek to the indexed position, or fall back to the start of the data region.
    if (index_region.lookup(lo)) |offset| {
        self.qf_tip = offset;
        self.qf_index = Index.compose(target_cycle, lo * index_region.spacing);
    } else {
        self.qf_tip = index_region.dataRegionOffset();
        self.qf_index = Index.compose(target_cycle, 0);
    }

    // Scan forward to the exact target seqnum
    while (Index.seqnum(self.qf_index) < target_seqnum) {
        _ = try self.poll(); // skip entries until we reach target
    }
    self.dispatch_after = target_index;
}
```

### Complexity Analysis

The flat inline index stores byte offsets at regular intervals (the `spacing` parameter).
With a default spacing of 256 in a healthy/recovered file:

| Phase | Cost |
|---|---|
| Binary search over index slots | O(log(n / 256)) |
| Linear scan to exact entry | O(256) worst case |
| **Total** | **O(log n)** |

If a crash left one or more missing index entries, seek still remains correct by scanning
from the nearest earlier populated slot or from the data-region start. Appender startup
recovery should repair missing index entries so the steady-state bound returns to
`index_spacing - 1`.

### Index Region Layout

The index region is stored inline at the start of each `.ringloom` data file, immediately
after the file header. Each slot is a `u64` byte offset into the data region:

```
[ file header ] [ index slot 0 ] [ index slot 1 ] ... [ index slot N ] [ data region ... ]
```

A slot value of `0` means "not yet populated." The writer fills in index slots as it
appends entries — every `spacing`-th entry gets its byte offset recorded in the
corresponding slot.

---

## 7. Collect Mode (Blocking Iterator)

Two patterns for consuming messages:

### Non-blocking Poll Loop

```zig
while (true) {
    if (try tailer.poll()) |entry| {
        processMessage(entry.message, entry.index);
    }
    // ... do other work, check conditions, etc.
}
```

Use this when the reader thread has other responsibilities or needs to interleave
queue reads with other operations.

### Blocking Collect Loop

```zig
while (true) {
    const entry = try tailer.collect(.low_latency_spin_then_sleep);
    processMessage(entry.message, entry.index);
}
```

Use this for dedicated consumer threads that should process messages as fast as
possible with minimal latency. The thread sleeps with zero CPU usage when no data
is available and wakes up within microseconds when the writer publishes.

---

## 8. Modcount Protocol

The modcount is a monotonically increasing counter stored in the shared `metadata.ringloom`
file. The writer increments modcount (release store) after publishing each entry. The
tailer reads modcount (acquire load) to detect new data.

```zig
fn checkModcount(self: *Tailer) bool {
    const current = @atomicLoad(u64, self.queue.metadata_modcount_ptr, .acquire);
    if (current == self.last_modcount) return false;
    self.last_modcount = current;
    return true;
}
```

This avoids unnecessary work: if modcount hasn't changed, the tailer skips all
downstream processing (cycle checks, mmap checks, header parsing). The acquire load
on x86 is a plain `MOV` — zero overhead.

The modcount also serves as the trigger for refreshing cycle boundaries. When modcount
changes, the tailer re-reads the highest cycle from metadata, which may have advanced
if the writer rolled to a new cycle file.

---

## 9. Performance Characteristics

| Operation | Steady-State Cost |
|---|---|
| Modcount check | Acquire load on mmap (free on x86) |
| Header read | Acquire load on mmap (free on x86) |
| Payload read | Direct slice into mmap (zero copy) |
| Codec parse | Inline comptime function call |
| Tip advance | Integer arithmetic |
| Pre-map check | Single comparison |
| **Total syscalls (steady state)** | **0** |
| **Total allocations (steady state)** | **0** |
| **Idle wait** | Outside core; depends on caller's backoff/notifier |

### When Syscalls Occur

Syscalls are **not** on the hot path. They occur only during infrequent transitions:

| Event | Syscalls |
|---|---|
| Cycle file transition | `open`, `fstat`, `mmap`, `madvise`, `close`, `munmap` |
| mmap window remap | `munmap`, `mmap`, `madvise` |
| Pre-map window creation | `mmap`, `madvise` |
| Pre-map window swap | `munmap` (old window only) |
| Caller-owned blocking wait | `sleep`, condvar, epoll/kqueue, runtime scheduler, etc. |

In a typical workload where messages arrive faster than the consumer processes them,
the tailer runs entirely in userspace with zero syscalls per message.

---

## 10. Error Handling

Errors are propagated via Zig's error union mechanism. The tailer distinguishes between:

### Recoverable Conditions

| Condition | Behavior |
|---|---|
| `poll()` returns `null` | No data available — caller decides whether to retry or wait |
| `.busy` state | Writer is mid-write — caller should retry shortly |
| Spurious external wakeup | Caller or `collect()` loop retries `poll()` |

### Fatal Errors

| Error | Cause |
|---|---|
| `error.MmapFailed` | `mmap` returned `MAP_FAILED` — out of address space or bad fd |
| `error.OpenFailed` | Could not open `.ringloom` cycle file — missing or permissions |
| `error.ParseFailed` | Codec could not parse payload — data corruption or version mismatch |
| `error.PrefetchFailed` | Read prefetch/advice failed unexpectedly |

Fatal errors bubble up to the caller. The tailer does not attempt automatic recovery
from fatal errors — the caller is responsible for deciding whether to retry, skip, or
abort.

### Cleanup on Error

When a tailer encounters a fatal error or is explicitly closed:

1. Unmap current window (`qf_buf`)
2. Unmap pre-mapped window (`next_buf`) if present
3. Close file descriptor (`qf_fd`)
4. Unregister from queue read-prefetch bookkeeping if registered

---

## 11. Testing Strategy

### Unit Tests

- **Single reader correctness** — write N entries, read N entries, verify content and indices match.
- **Header dispatch** — verify correct behavior for each header type (UNALLOCATED, WORKING, DATA, METADATA, EOF).
- **Padding** — verify `pad4` correctly aligns entry sizes to 4-byte boundaries.
- **Cycle transitions** — write past EOF marker, verify tailer opens next cycle file.

### Concurrency Tests

- **Multi-reader concurrent access** — spawn multiple tailer threads reading the same queue, verify each sees all entries in order with no corruption.
- **Writer-reader interleaving** — writer and reader running concurrently, verify acquire/release ordering guarantees are upheld.

### Waiting and Read-Prefetch Tests

- **Backoff collect** — verify optional Zig `collect()` returns when data arrives and does not change queue correctness.
- **Read-prefetch bounds** — verify prefetch never reads beyond acquire-loaded published write position.
- **Multi-tailer coalescing/budgets** — verify overlapping read-prefetch work is bounded.

### Seeking Tests

- **Index-based seeking accuracy** — write N entries, seek to random indices, verify correct entry is returned.
- **Seek across cycles** — seek to entries in previous cycle files.
- **Seek to unpopulated index slot** — verify binary search finds the nearest earlier slot and linear scan reaches the target.

### Window Management Tests

- **Pre-map window swap** — verify that crossing the 50% threshold triggers pre-mapping and that the swap is seamless.
- **Large entries spanning window boundaries** — verify correct behavior when an entry straddles two mmap windows.
- **madvise(MADV_SEQUENTIAL)** — verify the hint is applied to all new mappings (can be checked via `/proc/self/smaps`).
