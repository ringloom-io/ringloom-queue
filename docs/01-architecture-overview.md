# brz-queue Architecture Overview

## Table of Contents

1. [Introduction](#introduction)
2. [Project Summary](#project-summary)
3. [High-Level Architecture](#high-level-architecture)
4. [Core Concepts](#core-concepts)
5. [File Layout on Disk](#file-layout-on-disk)
6. [Memory-Mapped I/O Design](#memory-mapped-io-design)
7. [Message Framing and Header Protocol](#message-framing-and-header-protocol)
8. [Message Publication Protocol](#message-publication-protocol)
9. [Index Structure](#index-structure)
10. [Roll Cycle Mechanism](#roll-cycle-mechanism)
11. [Shared Metadata File](#shared-metadata-file)
12. [Data Structures](#data-structures)
13. [Appender Lifecycle](#appender-lifecycle)
14. [Tailer Lifecycle](#tailer-lifecycle)
15. [Module Decomposition](#module-decomposition)
16. [Concurrency and Memory Ordering](#concurrency-and-memory-ordering)
17. [Error Handling](#error-handling)
18. [Diagrams](#diagrams)

---

## Introduction

This document provides a comprehensive architectural description of **brz-queue**, a clean-room, high-performance, memory-mapped IPC queue implemented in Zig 0.16. The design targets the lowest possible latency with zero allocations, no steady-state syscalls, and no expected page faults on the appender hot path. brz-queue uses fixed-layout binary structures (no self-describing wire format), a single active appender lease, acquire/release publication, flat inline indexes, background page pre-touching, optional cleanup helpers, and optional `io_uring` for reader wakeup. It is designed for single-active-writer, multi-reader workloads across OS processes communicating through shared `mmap` regions.

## Project Summary

| Property | Value |
|---|---|
| **Language** | Zig |
| **License** | Apache 2.0 |
| **Format Version** | v1 (`brz`) |
| **IPC Mechanism** | Memory-mapped files (`mmap` + `MAP_SHARED`) |
| **Arbitration** | `cmpxchg` (atomic CAS via Zig builtins) |
| **Memory Ordering** | Acquire/Release (not SeqCst) |
| **File Extension** | `.brz` (data files), `metadata.brz` (shared metadata) |
| **Backoff Strategy** | Tiered: spin → yield → exponential (capped 1 ms) |
| **Index** | Flat inline array of `u64` offsets after file header |
| **Wakeup** | `io_uring` + `eventfd` (falls back to polling) |
| **Huge Pages** | Optional `MAP_HUGETLB` (2 MiB) support |
| **Pre-allocation** | `fallocate(2)` on Linux |
| **Test Framework** | Zig built-in test runner |

## High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        User Application                          │
│                                                                  │
│   ┌─────────────┐    ┌──────────────┐    ┌────────────────┐     │
│   │  Encoder     │    │   Decoder    │    │  Dispatch CB   │     │
│   │ (sizeof+write│    │ (parse)      │    │ (index, msg)   │     │
│   └──────┬───────┘    └──────┬───────┘    └───────┬────────┘     │
│          │                   │                    │              │
├──────────┼───────────────────┼────────────────────┼──────────────┤
│          ▼                   ▼                    ▼              │
│  ┌────────────────────────────────────────────────────────┐      │
│  │                   brz-queue Public API                  │      │
│  │                                                        │      │
│  │  Queue.init()       Queue.open()                       │      │
│  │  Appender.append()  Tailer.poll()                      │      │
│  │  Tailer.collect()   Queue.deinit()                     │      │
│  └────────────────────┬───────────────────────────────────┘      │
│                       │                                          │
│  ┌────────────────────┼───────────────────────────────────┐      │
│  │              Core Implementation                       │      │
│  │                    │                                   │      │
│  │  ┌────────────┐  ┌┴───────────┐  ┌─────────────────┐  │      │
│  │  │  Appender  │  │   Tailer   │  │  Metadata Mgr   │  │      │
│  │  │  (writer)  │  │  (reader)  │  │ (cycle tracker)  │  │      │
│  │  └─────┬──────┘  └─────┬──────┘  └────────┬────────┘  │      │
│  │        │               │                   │           │      │
│  │  ┌─────┴───────────────┴───────────────────┴────────┐  │      │
│  │  │    mmap / CAS / acquire-release primitives       │  │      │
│  │  └──────────────────────┬───────────────────────────┘  │      │
│  └─────────────────────────┼──────────────────────────────┘      │
│                            │                                     │
│  ┌─────────────────────────┼──────────────────────────────┐      │
│  │             io_uring notification layer                 │      │
│  │     (eventfd writer→reader wakeup, optional)           │      │
│  └─────────────────────────┬──────────────────────────────┘      │
│                            │                                     │
└────────────────────────────┼─────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Operating System / Kernel                      │
│                                                                  │
│   ┌──────────┐   ┌──────────┐   ┌────────────────────────┐      │
│   │  mmap()  │   │  open()  │   │  fallocate()           │      │
│   │MAP_SHARED│   │ O_RDWR   │   │  madvise() / io_uring  │      │
│   │pre-touch │          │   │  MAP_HUGETLB           │      │
│   └──────────┘   └──────────┘   └────────────────────────┘      │
│                                                                  │
│   ┌──────────────────────────────────────────────────────┐       │
│   │              File System (queue directory)            │       │
│   │                                                      │       │
│   │  metadata.brz   20211118F.brz   20211119F.brz        │       │
│   └──────────────────────────────────────────────────────┘       │
└──────────────────────────────────────────────────────────────────┘
```

## Core Concepts

### Queue

A brz-queue is a **directory** on disk containing:
- One shared metadata file (`metadata.brz`)
- Zero or more queue data files (`.brz`)

There is **no broker process**. The OS kernel provides persistence via `mmap` with `MAP_SHARED`, and hardware atomics provide the entry publication protocol.

### Appender

A writer. Appends messages to the queue and receives a 64-bit **index** identifying the write position. The appender is **single-threaded** and globally exclusive per queue — it is NOT thread-safe, and only one active appender thread/process may own the appender lease at a time. This is intentional for minimum jitter: cross-process write arbitration is kept out of the hot path.

### Tailer

A reader/subscriber. Reads messages sequentially starting from index 0 or a provided resume index. Multiple tailers are supported across threads and processes. Each tailer maintains its own independent mmap window and read state. Tailers never block writers.

### Index (64-bit)

Every message is identified by a 64-bit index composed of two parts:

```
┌─────────────────────────────────────────────────────────────────┐
│                        64-bit Index                              │
│                                                                  │
│  ◄──── cycle_shift bits ────►◄──── remaining bits ────►         │
│  ┌──────────────────────────┬────────────────────────────┐      │
│  │        CYCLE             │         SEQNUM             │      │
│  │   (maps to filename)     │   (position in file)       │      │
│  └──────────────────────────┴────────────────────────────┘      │
│                                                                  │
│  Example (DAILY scheme, cycle_shift=32):                         │
│  Index 0x4A0500000003                                            │
│    cycle  = 0x4A05 = 18949 → days since epoch → 20211118        │
│    seqnum = 0x00000003 → 4th message in that file                │
└─────────────────────────────────────────────────────────────────┘
```

The upper bits are the **cycle** (maps to a time-based filename) and the lower bits are the **seqnum** (sequential message number within that file).

### Roll Cycle

Determines when a new queue file is started. The cycle value is derived from the current clock:

```
cycle = (current_time_ms - roll_epoch) / roll_length_ms
```

When the cycle advances, `seqnum` resets to zero and a new `.brz` file is created.

## File Layout on Disk

```
queue_directory/
├── metadata.brz               # Fixed 512-byte shared metadata (mmap'd)
│
├── 20211118F.brz              # Queue data file for cycle N
├── 20211119F.brz              # Queue data file for cycle N+1
└── ...
```

### Metadata File (`metadata.brz`) Layout

A fixed 512-byte `extern struct SharedMetadata` — just mmap and cast the pointer. Zero parsing required.

```
Offset   Size   Field                    Description
──────   ────   ─────                    ───────────
0x0000   4      magic                    0x4D515A42 ("BZQM" bytes)
0x0004   2      version                  Format version (1)
0x0006   2      flags                    Feature flags (bitfield)
0x0008   4      roll_length_secs         Roll period in seconds
0x000C   4      index_spacing            Index every Nth data message
0x0010   4      index_count              Entries in flat index region
0x0014   4      padding                  u64 alignment padding
0x0018   8      epoch_ms                 Epoch offset in milliseconds
0x0020   8      highest_cycle            u64, atomic (acquire/release)
0x0028   8      lowest_cycle             u64, atomic (acquire/release)
0x0030   8      modcount                 u64, atomic (fetch_add)
0x0038   8      write_position           u64, atomic published offset
0x0040   8      appender_lock            u64, atomic lease; 0 = unlocked
0x0048   440    reserved                 Pad to 512 bytes total
──────────────────────────────────────────────────────────────
Total: 512 bytes (0x200)
```

All atomically-accessed `u64` fields are naturally 8-byte aligned. No wire encoding, no stop-bit encoding, no parsing.

### Queue File (`.brz`) Internal Layout

```
Offset 0x0000
┌─────────────────────────────────────────────────────────────┐
│ QueueFileHeader (64 bytes, extern struct)                     │
│   magic: u32         = 0x43515A42 ("BZQC" bytes)             │
│   version: u16       = 1                                     │
│   flags: u16         = 0                                     │
│   created_cycle: u32 = cycle number for this file            │
│   index_count: u32   = number of index slots                 │
│   index_spacing: u32 = messages between index entries        │
│   epoch_ms: u64      = roll epoch                            │
│   reserved: [28]u8   = zero                                  │
├─────────────────────────────────────────────────────────────┤
│ Index Region: flat array of u64 (index_count entries)        │
│   [0] → byte offset of data message at seqnum 0             │
│   [1] → byte offset of data message at seqnum index_spacing │
│   [2] → byte offset of data message at seqnum 2×spacing     │
│   ...                                                        │
│   [index_count-1] → ...                                      │
│   (0 = not yet written)                                      │
│                                                              │
│   Size = index_count × 8 bytes                               │
│   (e.g., 4096 × 8 = 32,768 bytes = 32 KiB)                  │
├─────────────────────────────────────────────────────────────┤
│ Data Region starts at offset 64 + (index_count × 8)         │
│                                                              │
│ [4-byte header: DATA | size] Data message 0                 │
│    User payload bytes (+ padding to 4-byte alignment)        │
├─────────────────────────────────────────────────────────────┤
│ [4-byte header: DATA | size] Data message 1                 │
│    User payload bytes (+ padding to 4-byte alignment)        │
├─────────────────────────────────────────────────────────────┤
│                        ...                                   │
├─────────────────────────────────────────────────────────────┤
│ [4-byte header: EOF]  End of file marker                     │
├─────────────────────────────────────────────────────────────┤
│ [UNALLOCATED = 0x00000000 ...]  Unallocated space            │
└─────────────────────────────────────────────────────────────┘
```

Data messages are **4-byte aligned** (padded with `(-size) & 0x03` zero bytes after each entry).

## Memory-Mapped I/O Design

### Why mmap?

The kernel guarantees that `MAP_SHARED` mappings of the same file region by multiple processes map to the **same physical pages**. This enables shared-memory IPC without explicit shared memory APIs.

### Chunked Mapping Strategy

Files are not mapped entirely. Instead, they are mapped in sliding windows of `2 × blocksize` bytes (default blocksize: 2 MiB, always a power of two and huge-page aligned):

```
File on disk:
┌──────────────────────────────────────────────────────────┐
│ Block 0       │ Block 1       │ Block 2       │ ...      │
└──────────────────────────────────────────────────────────┘

mmap window (2 × blocksize):
                ┌───────────────────────────────┐
                │ Block 1       │ Block 2       │
                └───────────────────────────────┘
                ▲                               ▲
            mmapoff                         mmapoff + mmapsz

Tip (next read/write position) is within the mapped window.
mmapoff = tip & ~(blocksize - 1)   // align down to blocksize boundary
```

Key rules:
- The mapping always covers **2 × blocksize** to guarantee any single message fits
- If a message would cross the map boundary, the window is advanced
- If a message is larger than blocksize, blocksize is **doubled**
- The mmap is only refreshed when the desired window changes

### Appender Mapping Optimizations

- **Background write pre-touch:** Future appender windows are prepared by a prefetcher thread before the appender reaches them. The prefetcher uses `fallocate`, `MAP_POPULATE`, `MADV_POPULATE_WRITE` when available, and a manual one-byte-per-page write-touch fallback for kernels that only populate read faults.
- **Pre-map next window:** When the write tip crosses the configured runway threshold, the next window is pre-mapped and pre-touched in the background. When the tip reaches the boundary, the appender swaps to an already faulted mapping — zero mmap/page-fault latency on window transition if the prefetcher kept up.
- **Optional `mlock`:** The current and next appender windows may be locked in RAM when `RLIMIT_MEMLOCK` permits. This is a tuning option, not a default, because pinning too much page cache can hurt the rest of the system.
- **`MAP_HUGETLB` (optional):** When enabled, mappings use 2 MiB huge pages to reduce TLB pressure. This is particularly effective for large blocksize values where the working set spans many pages.

### Tailer Mapping Optimizations

- **`madvise(MADV_SEQUENTIAL)`:** Tailer windows use `MADV_SEQUENTIAL` to hint the kernel that access is sequential. This enables aggressive read-ahead prefetching, keeping data pages hot ahead of the read tip.

### File Pre-allocation (`fallocate`)

When an appender needs to extend a queue file, it uses `fallocate(2)` on Linux instead of `lseek` + `write`:

```
fallocate(fd, 0, current_size, extension_size)
```

This actually allocates disk blocks so that the first write to a new region does not trigger filesystem block allocation (which would cause latency spikes). Combined with pre-touching, this removes major faults and expected minor/write faults from the appender hot path.

### Page-Fault Avoidance Strategy

The appender hot path must not rely on demand paging. Synchronous `MAP_POPULATE` in `append()` only moves the latency spike from first write to the remap syscall; it is still a spike on the appender thread. brz-queue therefore treats page preparation as background work:

1. The appender publishes its current `write_position`.
2. A prefetcher watches the write position and maintains a prepared runway ahead of it.
3. The prefetcher pre-creates future cycle files, extends current files with `fallocate`, maps future windows, faults writable pages, and optionally locks the current/next windows.
4. The appender atomically observes a ready window and swaps pointers when it reaches the boundary.
5. If the prefetcher falls behind, the appender may perform a synchronous fallback, but this is reported as a latency-profile miss.

This is the same class of technique Chronicle Queue documents as **pre-touching**: touching pages and acquiring future cycle resources before appenders need them. Chronicle exposes manual `pretouch()` and an automatic preloader; brz-queue makes the equivalent prefetcher part of the core low-jitter design.

## Message Framing and Header Protocol

Every data message is preceded by a 4-byte header word. Bits 31–30 encode the message type:

```
┌────────────────────────────────────────────────┐
│              32-bit Header Word                 │
│                                                 │
│  Bit 31    Bit 30    Bits 29-0                  │
│                                                 │
│  0         0         0x00000000  → UNALLOCATED  │
│  0         0         0x01-0x3FFF→ DATA (=size)  │
│  0         1         size        → METADATA     │
│  1         0         0x00000000  → WORKING      │
│  1         1         0x00000000  → EOF          │
└────────────────────────────────────────────────┘

Constants:
  HD_UNALLOCATED = 0x00000000
  HD_WORKING     = 0x80000000  (write lock held, no PID)
  HD_METADATA    = 0x40000000
  HD_EOF         = 0xC0000000
  HD_MASK_LENGTH = 0x3FFFFFFF
  HD_MASK_META   = 0xC0000000

Encoding:
  0x00000000                → UNALLOCATED
  0x00000001 .. 0x3FFFFFFF  → DATA (value = payload size in bytes)
  0x40000001 .. 0x7FFFFFFF  → METADATA (value & 0x3FFFFFFF = size)
  0x80000000                → WORKING (write lock held)
  0xC0000000                → EOF marker
```

Maximum payload size: `0x3FFFFFFF` = 1,073,741,823 bytes (~1 GiB).

**Key difference from legacy formats:** The WORKING state no longer encodes the writer's PID in the lower 30 bits. PID was decorative (never used for recovery or deadlock detection) and wasted bits. WORKING is now simply `0x80000000`.

## Message Publication Protocol

The single active appender owns the write position. It still uses a compare-and-swap (CAS) operation on the 4-byte header at the current tail position to transition an entry from `UNALLOCATED` to `WORKING`; this protects readers from partially written payloads and gives recovery a clear incomplete-write marker. It is not a global multi-writer arbitration mechanism.

### Writer Protocol

```
  ┌─────────────────────────────────────┐
  │ 1. Read header at tail position      │
  │    Expected: HD_UNALLOCATED (0x0)    │
  └──────────────┬──────────────────────┘
                 │
                 ▼
  ┌─────────────────────────────────────┐
  │ 2. CAS: UNALLOCATED → WORKING       │
  │    @cmpxchgStrong(..., .monotonic)  │
  │                                      │
  │    Returns old value:                │
  │    - If 0x0: slot claimed ─────────►├──┐
  │    - Otherwise: busy/recovery ──────►├──┤
  └─────────────────────────────────────┘  │
                                            │
          ┌─────────────────────────────────┘
          ▼
  ┌─────────────────────────────────────┐
  │ 3. Write payload to (header + 4)     │
  │    Direct memcpy into mmap region    │
  └──────────────┬──────────────────────┘
                 │
                 ▼
  ┌─────────────────────────────────────┐
  │ 4. Release store: write final header │
  │    @atomicStore(ptr, size, .release) │
  │    (clears WORKING bit, sets size)   │
  └──────────────┬──────────────────────┘
                 │
                 ▼
  ┌─────────────────────────────────────┐
  │ 5. If (seqnum % index_spacing == 0) │
  │    atomicStore index entry           │
  └──────────────┬──────────────────────┘
                 │
                 ▼
  ┌─────────────────────────────────────┐
  │ 6. Release-store write_position      │
  │    and optionally signal waiters      │
  └─────────────────────────────────────┘
```

### Reader Protocol

```
  ┌─────────────────────────────────────┐
  │ 1. Acquire load: read 4-byte header  │
  │    @atomicLoad(ptr, .acquire)        │
  └──────────────┬──────────────────────┘
                 │
                 ▼
  ┌─────────────────────────────────────┐
  │ 2. Decode header:                    │
  │    UNALLOCATED → wait/return         │
  │    WORKING     → busy, retry later   │
  │    METADATA    → skip                │
  │    EOF         → advance cycle       │
  │    DATA        → read payload        │
  └─────────────────────────────────────┘
```

### Tiered CAS Backoff

When CAS fails (another writer holds the position), the retry strategy uses a three-tier backoff to balance latency against CPU waste:

```
┌─────────────────────────────────────────────────────────────┐
│                   CAS Retry Backoff                          │
│                                                              │
│  Tier 1: Spin (iterations 0–64, ~0–2 μs)                    │
│    └─ spinLoopHint() — CPU pipeline hint (PAUSE on x86)     │
│    └─ Lowest latency, highest CPU usage                      │
│                                                              │
│  Tier 2: Yield (iterations 64–256, ~2–50 μs)                │
│    └─ std.Thread.yield() — give up timeslice to OS           │
│    └─ Moderate latency, lets other threads run               │
│                                                              │
│  Tier 3: Exponential backoff (iterations 256+)               │
│    └─ std.time.sleep(delay) — exponential, capped at 1 ms   │
│    └─ Highest latency, lowest CPU usage                      │
│    └─ Appropriate for extreme contention only                │
└─────────────────────────────────────────────────────────────┘
```

### Data Alignment Padding

After each message, padding bytes are added to maintain 4-byte alignment:

```
pad = (-payload_size) & 0x03;
next_header_offset = current_offset + 4 + payload_size + pad;
```

This ensures the next header's CAS target is always 4-byte aligned.

## Index Structure

The index is a **flat inline array** of `u64` offsets stored immediately after the 64-byte file header, before any data messages. There is no two-level indirection, no `I64_ARRAY` metadata messages, and no wire encoding.

```
Queue file layout with index:

  Offset 0x0000:  QueueFileHeader (64 bytes)
  Offset 0x0040:  Index Region starts
                   ┌──────────────────────────────────────────┐
                   │ [0]  u64 → byte offset of msg at seq 0   │
                   │ [1]  u64 → byte offset of msg at seq S   │
                   │ [2]  u64 → byte offset of msg at seq 2S  │
                   │ ...                                       │
                   │ [N-1] u64 → byte offset of msg at seq    │
                   │              (N-1) × S                    │
                   │                                           │
                   │ (0 = not yet written)                     │
                   └──────────────────────────────────────────┘
  Offset 0x0040 + N×8:  Data region starts
```

Where `S` = `index_spacing` and `N` = `index_count`.

**Example configuration:**
- `index_spacing` = 256, `index_count` = 4096
- Index region size = 4096 × 8 = 32,768 bytes (32 KiB)
- Data starts at offset 64 + 32,768 = 32,832

### Index Write Protocol

The appender atomically stores a `u64` into the index every `index_spacing` data messages:

```
if (seqnum % index_spacing == 0) {
    slot = seqnum / index_spacing;
    if (slot < index_count) {
        @atomicStore(&index[slot], msg_offset, .release);
    }
}
```

### Index Read Protocol (Seek)

Readers perform a binary search over the index region for `O(log n)` seek instead of `O(n)` linear scan:

```
1. target_slot = target_seqnum / index_spacing
2. Binary search index[0..index_count] for the largest slot ≤ target_slot
   where index[slot] != 0
3. Start linear scan from that byte offset
4. Skip forward (target_seqnum % index_spacing) messages
```

### Index Configuration by Roll Scheme

| Scheme | index_count | index_spacing | Index Region Size |
|---|---|---|---|
| DAILY | 8192 | 64 | 64 KiB |
| FAST_DAILY | 4096 | 256 | 32 KiB |
| FAST_HOURLY | 4096 | 256 | 32 KiB |
| TEST4_SECONDLY | 32 | 4 | 256 B |

## Roll Cycle Mechanism

### Roll Schemes

brz-queue ships with multiple built-in roll schemes. Each defines:

| Field | Description |
|---|---|
| `name` | Identifier (e.g., `"FAST_DAILY"`) |
| `format` | Date format string (e.g., `"yyyyMMdd'F'"`) |
| `roll_length_ms` | Duration of each cycle in milliseconds |
| `index_count` | Number of entries in the flat index region |
| `index_spacing` | Every Nth data message is indexed |

### Filename Generation

```
filename = dirname ++ "/" ++ formatDate(cycle × roll_length_ms) ++ ".brz"
```

The cycle number maps to a time: `timestamp_ms = cycle × roll_length_ms + roll_epoch`

### Roll Detection During Append

```
current_cycle = (clock_ms - roll_epoch) / roll_length_ms

if current_cycle > appender_cycle:
    1. Release-store HD_EOF at the current write position
    2. Swap to pre-created/pre-touched next cycle file (if available)
       — OR create new queue file for current_cycle
    3. Update highest_cycle and write_position in shared metadata (release)
    4. Bump modcount (atomic fetch_add)
    5. Retry append in new file
```

### EOF Patching

If an appender finds itself holding the write lock but the current file's cycle is behind `highest_cycle`, it writes an EOF marker to "patch" the old file. This handles the case where a previous writer crashed without writing EOF.

### Pre-Create Next Cycle File

To eliminate the latency spike during roll transitions (which would otherwise require 8+ syscalls for file creation, allocation, mmap, and page faults), brz-queue pre-creates and pre-touches the next cycle file:

```
┌─────────────────────────────────────────────────────────────┐
│                  Pre-Roll Protocol                           │
│                                                              │
│  1. When current time is within preroll_ms of roll boundary  │
│     (configurable, default: 1000 ms)                         │
│                                                              │
│  2. Create next cycle's .brz file:                           │
│     a. open() + fallocate() to pre-allocate disk blocks     │
│     b. Write QueueFileHeader + zero index region             │
│     c. mmap() + writable page pre-touch                     │
│                                                              │
│  3. Store pre-mapped file handle in prefetch handoff state   │
│                                                              │
│  4. When roll actually happens:                              │
│     a. Write EOF to current file (single atomic store)       │
│     b. Swap appender pointers to pre-mapped file             │
│     c. Zero syscalls on the hot path                         │
│                                                              │
│  Result: Roll transition goes from ~50 μs to ~50 ns         │
└─────────────────────────────────────────────────────────────┘
```

## Shared Metadata File

The shared metadata file (`metadata.brz`) is a fixed 512-byte `extern struct SharedMetadata` that is mmap'd by all processes. There is no wire protocol, no parsing — just mmap the file and cast the pointer to the struct type.

### Layout

```
┌─────────────────────────────────────────────────────────────┐
│ extern struct SharedMetadata (512 bytes)                      │
│                                                              │
│  Fixed fields (offset 0x00 – 0x1F):                          │
│    magic .............. u32 = 0x4D515A42 ("BZQM" bytes)      │
│    version ............ u16 = 1                               │
│    flags .............. u16 = feature bitfield                │
│    roll_length_secs ... u32 = cycle duration                  │
│    index_spacing ...... u32 = index every Nth msg             │
│    index_count ........ u32 = flat index entries              │
│    epoch_ms ........... u64 = roll epoch                      │
│                                                              │
│  Atomic fields (offset 0x20 – 0x47, 8-byte aligned):        │
│    highest_cycle ...... u64 (atomic acquire/release)          │
│    lowest_cycle ....... u64 (atomic acquire/release)          │
│    modcount ........... u64 (atomic fetch_add)                │
│    write_position ..... u64 (atomic release/acquire)          │
│    appender_lock ...... u64 (atomic appender lease)           │
│                                                              │
│  Reserved (offset 0x48 – 0x1FF):                             │
│    440 bytes for future use                                  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Modcount Protocol

```
Reader/Tailer polling:
  1. val = @atomicLoad(&metadata.modcount, .acquire)
  2. If val != cached_modcount:
     - Read highest_cycle, lowest_cycle (atomic acquire loads)
     - May trigger opening new queue files

Writer updating:
  1. @atomicStore(&metadata.highest_cycle, new_value, .release)
  2. @atomicStore(&metadata.lowest_cycle, new_value, .release)
  3. @atomicStore(&metadata.write_position, new_tip, .release)
  4. _ = @atomicRmw(&metadata.modcount, .Add, 1, .release)
```

## Data Structures

### `Queue`

```
const Queue = struct {
    dirname: []const u8,              // Queue directory path
    blocksize: u32,                   // mmap chunk size (power of 2, default 2 MiB)

    // Shared metadata (mmap'd)
    metadata_fd: std.posix.fd_t,      // File descriptor for metadata.brz
    metadata: *SharedMetadata,        // Pointer into mmap (cast from mmap base)

    // Observed cycle range (cached from shared metadata)
    highest_cycle: u64,
    lowest_cycle: u64,
    modcount: u64,

    // Roll configuration (copied from metadata on open)
    roll_length_ms: u32,
    roll_epoch_ms: u32,
    roll_format: [32]u8,
    index_count: u32,
    index_spacing: u32,

    // Index decomposition
    cycle_shift: u6,                  // Bits to shift for cycle
    seqnum_mask: u64,                 // Mask for seqnum

    // io_uring notification (optional)
    ring: ?IoUring,                   // io_uring instance
    waiting_tailers: WaiterRegistry,  // per-tailer wakeup eventfds

    // Background helpers (optional)
    prefetcher: ?PagePrefetcher,      // prepares future appender pages/windows
    cleaner: ?Cleaner,                // unmaps/drops/reclaims old resources

    // Allocator for non-hot-path allocations
    allocator: std.mem.Allocator,
};
```

### `Appender`

```
const Appender = struct {
    queue: *Queue,

    // Currently open queue file
    cycle: u64,                       // Cycle of currently open file
    fd: std.posix.fd_t,               // File descriptor
    seqnum: u64,                      // Next sequence number to write

    // Current mmap window (PROT_READ | PROT_WRITE)
    buf: [*]align(4096) u8,          // mmap base address
    mmap_offset: u64,                 // Offset from file start
    mmap_size: u64,                   // Size of mapping
    tip: u64,                         // Byte position of next header

    // Handoff from prefetcher
    ready_window: ?MappedWindow,      // Pre-mapped/pre-touched next window
};
```

### `Tailer`

```
const Tailer = struct {
    queue: *Queue,
    dispatch_after: u64,              // Resume: skip messages ≤ this index
    state: TailerState,               // Current state

    // Currently open queue file
    cycle: u64,                       // Cycle of currently open file
    fd: std.posix.fd_t,               // File descriptor

    // Current mmap window (PROT_READ only)
    buf: [*]align(4096) const u8,     // mmap base address
    mmap_offset: u64,                 // Offset from file start
    mmap_size: u64,                   // Size of mapping

    tip: u64,                         // Byte position of next header
    index: u64,                       // Full 64-bit index (cycle << shift | seqnum)

    // io_uring wakeup (optional)
    waiting_on_uring: bool,           // Whether an SQE is submitted
};
```

### Tailer States

| Value | Name | Meaning |
|---|---|---|
| 0 | `awaiting_entry` | At end of data, waiting for new entry |
| 1 | `busy` | Hit a WORKING header, writer in progress |
| 2 | `awaiting_queuefile` | Expected queue file doesn't exist yet |
| 3 | `error_stat` | `fstat()` failed |
| 4 | `error_mmap` | `mmap()` failed (probably fatal) |
| 5 | `ready` | Not yet polled |
| 6 | `collected` | A value was collected (collect mode) |

## Appender Lifecycle

```
Appender.append(payload)
         │
         ▼
┌─────────────────────────┐
│ Calculate write size      │
│ Check size fits in 30 bits│
│ Double blocksize if needed│
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ Check modcount            │◄─── Refresh cycle info from shared metadata
│ (atomic acquire load)     │     (only if modcount changed)
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ Check pre-roll            │  If within preroll_ms of next cycle boundary
│ Pre-create next file      │  and preroll file not already created
│ (fallocate + pre-touch)  │
└────────────┬────────────┘
             │
             ▼
  ┌──────────────────────┐
  │  WRITE LOOP           │◄──────────────────────────┐
  │                        │                           │
  │  Check tip position    │                           │
  └──────────┬─────────────┘                           │
             │                                         │
     ┌───────┼──────────┬────────────┐                 │
     ▼       ▼          ▼            ▼                 │
  AWAITING  AWAITING   NEED_REMAP  ROLL               │
  _ENTRY    _QUEUEFILE             DETECTED            │
     │       │          │            │                 │
     │       │          ▼            ▼                 │
     │       │   Swap prepared    Write EOF,           │
     │       │   window           swap to preroll──────┤
     │       │   (prefetched)     (or fallback create) │
     │       │                                         │
     │       ▼                                         │
     │   Create new .brz file:                         │
     │   1. open + fallocate                           │
     │   2. Write QueueFileHeader + zero index         │
     │   3. mmap + write pre-touch                     │
     │   4. Update highest_cycle (atomic release)      │
     │   5. Bump modcount (atomic fetch_add) ──────────┘
     │
     ▼
  ┌──────────────────────────────────┐
  │ CAS: @cmpxchgWeak                 │
  │   (&header, UNALLOC, WORKING)     │
  │   .success = .monotonic           │
  │   .failure = .monotonic            │
  └──────────┬───────────────────────┘
             │
      ┌──────┴──────┐
      ▼             ▼
   SUCCESS     FAILED → tiered backoff
      │          Tier 1: spinLoopHint (0-64)
      │          Tier 2: yield (64-256)
      │          Tier 3: exp sleep ≤1ms (256+)
      │
      ▼
  ┌──────────────────────────┐
  │ Write payload at tip + 4  │
  │ (direct memcpy into mmap) │
  └──────────┬───────────────┘
             │
             ▼
  ┌──────────────────────────┐
  │ Release store: final hdr  │
  │ @atomicStore(.release)    │
  │ header = payload_size     │
  │ (clears WORKING, sets sz) │
  └──────────┬───────────────┘
             │
             ▼
  ┌──────────────────────────┐
  │ Update index if           │
  │ seqnum % spacing == 0    │
  └──────────┬───────────────┘
             │
             ▼
  ┌──────────────────────────┐
  │ Store write_position;     │
  │ optionally signal waiters │
  └──────────┬───────────────┘
             │
             ▼
  ┌──────────────────────────┐
  │ Return 64-bit index       │
  └──────────────────────────┘
```

## Tailer Lifecycle

### Registration

```
tailer = Tailer.init(queue, start_index);
```

1. Decomposes `start_index` into cycle and seqnum
2. Clamps cycle to `[lowest_cycle, highest_cycle]`
3. Sets `dispatch_after = start_index - 1`
4. Sets `index` to the start of the cycle's file (seqnum 0)
5. If io_uring is available, prepares eventfd SQE for wakeup

### Polling (`Tailer.poll`)

This is the core tailer read path:

```
┌─────────────────────────────────────────┐
│ OUTER LOOP (while true)                  │
│                                          │
│ ┌───────────────────────────────────┐    │
│ │ Extract cycle from index           │    │
│ │ cycle = index >> cycle_shift       │    │
│ └──────────────┬────────────────────┘    │
│                │                         │
│      ┌─────────┴──────────┐              │
│      │ cycle != open?     │              │
│      │ or fd not valid?   │              │
│      └─────────┬──────────┘              │
│           YES  │                         │
│                ▼                         │
│  ┌─────────────────────────┐             │
│  │ Close old fd/mmap        │             │
│  │ Generate new filename    │             │
│  │ Open new file            │             │
│  │ If open fails:           │             │
│  │   cycle < highest?       │             │
│  │     → skip to next cycle │             │
│  │   else                   │             │
│  │     → return .awaiting_qf│             │
│  └──────────┬──────────────┘             │
│             │                            │
│             ▼                            │
│  ┌──────────────────────────────┐        │
│  │ Seek using flat index         │        │
│  │ (binary search for O(log n)) │        │
│  │ Then linear scan to exact pos │        │
│  └──────────┬───────────────────┘        │
│             │                            │
│             ▼                            │
│  ┌─────────────────────────┐             │
│  │ Calculate mmap window    │             │
│  │ mmapoff = tip & mask     │             │
│  │ madvise(MADV_SEQUENTIAL) │             │
│  │ mmap 2×blocksize chunk   │             │
│  └──────────┬──────────────┘             │
│             │                            │
│             ▼                            │
│  ┌─────────────────────────────┐         │
│  │ Acquire load: read header    │         │
│  │ @atomicLoad(ptr, .acquire)   │         │
│  └──────────┬──────────────────┘         │
│             │                            │
│     ┌───────┼─────────┬──────┐           │
│     ▼       ▼         ▼      ▼           │
│  AWAITING  BUSY    EOF    DATA           │
│  _ENTRY             │      │             │
│     │               ▼      ▼             │
│     │         Advance    Read payload    │
│     │         to next    Dispatch to     │
│     │         cycle      callback        │
│     │         ──────────────────────────►│
│     │                                    │
│     ▼                                    │
│  Wait for new data:                      │
│  ┌────────────────────────────────┐      │
│  │ If io_uring available:         │      │
│  │   Register per-tailer eventfd  │      │
│  │   Near-zero wakeup latency     │      │
│  │   Zero CPU during idle         │      │
│  │ Else:                          │      │
│  │   Tiered backoff polling        │      │
│  └────────────────────────────────┘      │
│                                          │
└──────────────────────────────────────────┘
```

### io_uring Wakeup Protocol

```
┌───────────────────────────────────────────────────────────────┐
│                    io_uring Notification                        │
│                                                                │
│  Setup:                                                        │
│    - Queue initializes io_uring instance                       │
│    - Each blocking tailer owns an eventfd                      │
│                                                                │
│  Writer (after publishing header/write_position):               │
│    - optionally write(eventfd, 1) for each registered waiter    │
│                                                                │
│  Reader (when no data available):                               │
│    1. Register as waiting                                      │
│    2. Submit io_uring SQE: IORING_OP_READ on tailer eventfd    │
│    3. io_uring_submit_and_wait(1) — blocks in kernel           │
│    4. Kernel wakes reader when eventfd becomes readable         │
│    5. Reader unregisters and resumes polling                    │
│                                                                │
│  Latency: low, but paid for by appender notification syscalls   │
│  CPU during idle: 0% (blocked in kernel)                        │
│                                                                │
│  Fallback: If io_uring is unavailable (old kernel, no support), │
│  readers use tiered backoff polling (same as CAS backoff).      │
└───────────────────────────────────────────────────────────────┘
```

### Collect Mode

`Tailer.collect()` provides a synchronous blocking interface:

1. Polls in a loop calling `Tailer.poll()`
2. If io_uring is available, blocks efficiently in kernel between polls
3. Otherwise uses tiered backoff
4. When a DATA message is found, returns the payload slice
5. The slice points directly into the mmap region (zero-copy)

## Module Decomposition

brz-queue has a simple module structure — no wire protocol serialization layer is needed.

### `queue.zig` — Queue Management

| Responsibility | Key Functions |
|---|---|
| Initialization | `Queue.init`, `Queue.open`, `Queue.deinit` |
| Configuration | `Queue.setRollScheme`, `Queue.setBlocksize` |
| Metadata | `Queue.openMetadata`, `Queue.refreshModcount` |
| File management | `Queue.createQueueFile`, `Queue.openQueueFile` |
| Appender lease | `Queue.acquireAppenderLease`, `Queue.releaseAppenderLease` |

### `appender.zig` — Single-Writer Append Engine

| Responsibility | Key Functions |
|---|---|
| Writing | `Appender.append`, `Appender.appendWithTimestamp` |
| Header state | `Appender.claimEntry`, `Appender.publishEntry` |
| Roll handling | `Appender.checkRoll`, `Appender.preCreateNextCycle` |
| Index update | `Appender.updateIndex` |
| Notification | `Appender.signalReaders` |

### `tailer.zig` — Multi-Reader Engine

| Responsibility | Key Functions |
|---|---|
| Reading | `Tailer.poll`, `Tailer.collect` |
| Seeking | `Tailer.seekToIndex`, `Tailer.binarySearchIndex` |
| Wakeup | `Tailer.waitForData`, `Tailer.submitUringRead` |
| State | `Tailer.state`, `Tailer.currentIndex` |

### `mmap.zig` — Memory Mapping Utilities

| Responsibility | Key Functions |
|---|---|
| Mapping | `MappedWindow.init`, `MappedWindow.remap` |
| Optimization | `MappedWindow.populate`, `MappedWindow.adviseSequential` |
| Huge pages | `MappedWindow.tryHugePages` |
| Pre-mapping | `MappedWindow.premapNext` |
| Pre-touching | `MappedWindow.populateWrite`, `MappedWindow.touchWritablePages` |

### `prefetcher.zig` — Appender Page Preparation

| Responsibility | Key Functions |
|---|---|
| Runway tracking | `PagePrefetcher.observeWritePosition`, `PagePrefetcher.targetWindow` |
| Future mapping | `PagePrefetcher.prepareWindow`, `PagePrefetcher.prepareCycle` |
| Handoff | `PagePrefetcher.tryTakeReadyWindow` |
| Fault prevention | `PagePrefetcher.touchWritablePages`, `PagePrefetcher.tryMlock` |

### `cleaner.zig` — Asynchronous Reclamation

| Responsibility | Key Functions |
|---|---|
| Mapping cleanup | `Cleaner.deferUnmap`, `Cleaner.reapUnmaps` |
| Page-cache pressure | `Cleaner.adviseDontNeed`, `Cleaner.posixFadviseDontNeed` |
| Retention | `Cleaner.deleteCyclesBefore`, `Cleaner.computeRetentionFloor` |
| Durability option | `Cleaner.flushIfConfigured` |

### `metadata.zig` — Shared Metadata Structures

| Responsibility | Key Types |
|---|---|
| Metadata struct | `SharedMetadata` (extern struct, 512 bytes) |
| File header | `QueueFileHeader` (extern struct, 64 bytes) |
| Header constants | `HD_UNALLOCATED`, `HD_WORKING`, `HD_EOF`, etc. |
| Roll schemes | `RollScheme`, builtin scheme table |

### `uring.zig` — io_uring Integration (Optional)

| Responsibility | Key Functions |
|---|---|
| Setup | `UringNotifier.init`, `UringNotifier.deinit` |
| Writer signal | `UringNotifier.signal` |
| Reader wait | `UringNotifier.waitForSignal` |
| Fallback | `UringNotifier.isAvailable` |

## Concurrency and Memory Ordering

### Acquire/Release Instead of SeqCst

brz-queue uses **acquire/release** semantics rather than sequential consistency (`SeqCst`) for all hot-path atomic operations:

```
┌─────────────────────────────────────────────────────────────┐
│              Memory Ordering on x86 (TSO)                    │
│                                                              │
│  Operation              Ordering        x86 Cost             │
│  ─────────              ────────        ────────             │
│  Writer: store header   .release        FREE (MOV)           │
│  Reader: load header    .acquire        FREE (MOV)           │
│  Writer: claim header   .monotonic      LOCK CMPXCHG        │
│  Writer: modcount++     .release        LOCK XADD            │
│  SeqCst store           .seq_cst        MOV + MFENCE (~33ns) │
│                                                              │
│  On x86 TSO:                                                 │
│  - All stores are release stores (hardware provides this)    │
│  - All loads are acquire loads (hardware provides this)      │
│  - acquire/release fences compile to plain MOV instructions  │
│  - SeqCst requires MFENCE which costs ~33ns per operation    │
│                                                              │
│  Savings: ~33 ns per message on the read path                │
│  (eliminates MFENCE that SeqCst would require)               │
└─────────────────────────────────────────────────────────────┘
```

### Guarantees Relied Upon

1. **x86 Total Store Order (TSO):** Stores are visible in program order. Loads are not reordered with respect to other loads. Acquire/release maps to plain MOV on x86.

2. **Acquire semantics (readers):** The `@atomicLoad(.acquire)` on the header ensures that all subsequent reads of the payload see the data written by the producer. On x86, this is a plain load (free).

3. **Release semantics (writers):** The `@atomicStore(.release)` of the final header ensures that all prior writes (payload data) are visible before the header becomes visible to readers. On x86, this is a plain store (free).

4. **CAS (`cmpxchg`):** Atomic compare-and-swap is used only to claim an entry header (`UNALLOCATED → WORKING`). Correctness does not rely on it publishing payload bytes; the final release-store of the DATA header does that.

5. **`@atomicRmw(.Add, ...)` (`lock xadd`):** Atomic fetch-and-add with implicit barrier. Used for modcount increment.

### Threading Model

```
┌─────────────────────────────────────────────────────────────┐
│                    Threading Model                            │
│                                                              │
│  Within a single process:                                    │
│    - Appender: single-threaded (NOT thread-safe)             │
│    - Prefetcher/Cleaner: helper threads, no current mapping  │
│      mutation from the appender hot path                     │
│    - Tailers: each tailer is independent, can run on its     │
│      own thread. Multiple tailer threads are safe.           │
│    - No per-process locking needed for tailers               │
│                                                              │
│  Across processes:                                           │
│    - Multiple processes can share the queue directory         │
│    - Exactly one process may hold the appender lease          │
│    - Each process mmaps the same files                       │
│    - Reader processes never interfere with writers            │
│    - Each process has independent mmap windows               │
│                                                              │
│  Summary:                                                    │
│    ┌──────────┬──────────────┬───────────────────┐           │
│    │          │ Same Process │ Different Process  │           │
│    ├──────────┼──────────────┼───────────────────┤           │
│    │ Writer   │ 1 thread only│ 1 lease globally  │           │
│    │ Reader   │ N threads OK │ N processes OK     │           │
│    │ W+R      │ Separate     │ Independent        │           │
│    │          │ threads      │ mmap windows       │           │
│    └──────────┴──────────────┴───────────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

### Critical Sections

There are no traditional hot-path locks:
- The appender lease is acquired outside the append loop
- The appender uses CAS on the 4-byte header as an entry state transition
- If CAS fails, the entry is busy or corrupt; recovery/backoff policy handles it outside the common path
- Readers never block writers (readers only do atomic loads)
- The acquire ordering on header read ensures payload visibility

## Error Handling

brz-queue uses Zig's native error handling via error unions:

```
pub const QueueError = error{
    InvalidMagic,
    UnsupportedVersion,
    MmapFailed,
    FallocateFailed,
    FileOpenFailed,
    MetadataCorrupted,
    MessageTooLarge,
    BlocksizeExceeded,
    RollFailed,
    IoUringSetupFailed,
};
```

Functions return error unions:
- `Queue.init() !Queue` — returns Queue or error
- `Appender.append() !u64` — returns 64-bit index or error
- `Tailer.poll() !?Message` — returns optional message or error

Errors on the hot path (CAS failure, WORKING header) are **not** represented as errors — they are handled inline via retry loops. Only truly exceptional conditions (mmap failure, corrupted files, disk full) surface as Zig errors.

## Diagrams

### Complete Message Write Sequence

```
  Process A (Writer)                   Shared Memory (mmap)              Process B (Reader)
  ──────────────────                   ────────────────────              ──────────────────

  1. len = payload.len                 ┌────────┐
                                       │  0x00  │ UNALLOCATED
  2. CAS(0x00 → 0x80000000) ────────►  │  0x80  │ WORKING                3. atomicLoad(.acquire)
                                       │  0000  │                            → sees WORKING
                                       │  00    │                            → returns .busy
  4. memcpy payload ─────────────────► ├────────┤
                                       │  data  │ payload bytes
  5. atomicStore(.release) ──────────► ├────────┤
     header = len                      │  len   │ DATA                   6. atomicLoad(.acquire)
                                       ├────────┤                            → sees DATA|len
                                       │  data  │                        7. Read payload
  8. store write_position              └────────┘                            (visible due to
     optionally signal waiters                                               acquire ordering)
                                                                         8. Dispatch callback
```

### Multi-Process Queue Interaction

```
  ┌────────────┐     ┌────────────┐     ┌────────────┐
  │ Appender   │     │  Tailer 1  │     │  Tailer 2  │
  │ (Process A)│     │ (Process B)│     │ (Process C)│
  └─────┬──────┘     └─────┬──────┘     └─────┬──────┘
        │                   │                   │
        │   mmap(SHARED)    │   mmap(SHARED)    │   mmap(SHARED)
        │   pre-touched     │   MADV_SEQUENTIAL │   MADV_SEQUENTIAL
        │                   │                   │
        ▼                   ▼                   ▼
  ┌─────────────────────────────────────────────────────┐
  │              Kernel Page Cache                       │
  │   (same physical pages for same file regions)        │
  │                                                      │
  │  ┌──────────────┐  ┌──────────────────────────┐      │
  │  │metadata.brz  │  │ 20211118F.brz            │      │
  │  │ (512 bytes)  │  │ ┌──────────────────────┐ │      │
  │  │ modcount: 5  │  │ │ QueueFileHeader 64B  │ │      │
  │  │ highest: 42  │  │ ├──────────────────────┤ │      │
  │  │ lowest: 40   │  │ │ Index Region 32 KiB  │ │      │
  │  │ app_lock:tok │  │ ├──────────────────────┤ │      │
  │  │ write_pos:.. │  │ │                      │ │      │
  │  └──────────────┘  │ │ [HDR][DATA]          │ │      │
  │                     │ │ [HDR][DATA]          │ │      │
  │  ┌──────────────┐   │ │ [UNALLOC...]        │ │      │
  │  │ tailer       │  │ └──────────────────────┘ │      │
  │  │ eventfds     │  └──────────────────────────┘      │
  │  └──────────────┘                                     │
  └─────────────────────────────────────────────────────┘
        │                   │                   │
        ▼                   ▼                   ▼
  ┌─────────────────────────────────────────────────────┐
  │                   Disk (Filesystem)                   │
  │   Pre-allocated via fallocate(2)                      │
  │   Kernel writes dirty pages asynchronously            │
  └─────────────────────────────────────────────────────┘
```

### Queue File Lifecycle with Pre-Roll

```
Time ─────────────────────────────────────────────────────────────────►

Cycle N                               Cycle N+1
├─────────────────────────────────────┤──────────────────────────────┤

  File: 20211118F.brz                   File: 20211119F.brz
  ┌────────────────────────┐            ┌────────────────────────┐
  │ QueueFileHeader (64B)  │            │ QueueFileHeader (64B)  │
  │ Index Region (32 KiB)  │            │ Index Region (32 KiB)  │
  │ DATA: msg 0            │            │ DATA: msg 0            │
  │ DATA: msg 1            │            │ DATA: msg 1            │
  │ DATA: msg 2            │            │ ...                    │
  │ ...                    │            │ UNALLOCATED...         │
  │ DATA: msg N            │            └────────────────────────┘
  │ EOF ◄── written on roll│                     ▲
  │ UNALLOCATED...         │                     │
  └────────────────────────┘              Pre-created within
                                          preroll_ms (1000ms)
  Index: 0x4A050000_00000000              of roll boundary.
         to 0x4A050000_0000000N           Already fallocate'd
                                          and pre-touched.
                                          Roll = pointer swap,
                                          zero syscalls.
```

### Tiered Backoff Timing

```
  CAS Failure
       │
       ▼
  ┌─────────────────────────────────────────────────────────────┐
  │                                                             │
  │  Iteration   0        64       256                    ∞     │
  │  ────────── ├─────────┼─────────┼─────────────────────►     │
  │              │ TIER 1  │ TIER 2  │      TIER 3              │
  │              │  SPIN   │  YIELD  │  EXPONENTIAL SLEEP       │
  │              │         │         │                           │
  │  Latency:    │  0-2 μs │ 2-50 μs│  50 μs → 1 ms (cap)     │
  │  CPU cost:   │  100%   │  ~50%  │  ~0%                     │
  │  Mechanism:  │ PAUSE   │ sched  │  nanosleep               │
  │              │ hint    │ yield  │  (doubles each iter)     │
  │              │         │        │                           │
  └─────────────────────────────────────────────────────────────┘
```

### Flat Index Lookup (Seek)

```
  Target: seqnum = 1500, index_spacing = 256

  Step 1: target_slot = 1500 / 256 = 5

  Step 2: Binary search index region
  ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐
  │  0  │  1  │  2  │  3  │  4  │  5  │  6  │  7  │  ...
  │32832│34100│35980│37200│39500│41000│  0  │  0  │
  └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘
                                  ▲
                                  └── slot 5: offset 41000
                                      → seqnum 1280

  Step 3: Linear scan from offset 41000
           Skip (1500 - 1280) = 220 messages
           → O(log n) + O(index_spacing) instead of O(n)
```

---

*This document describes the architecture of brz-queue, a clean-room memory-mapped IPC queue implemented in Zig 0.16. For protocol changes between versions, see `CHANGES.md`.*
