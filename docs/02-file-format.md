# ringloom-queue File Format Specification

This document provides a byte-level specification of the ringloom-queue on-disk format. It covers the v1 format and is intended to be sufficient for writing a compatible implementation from scratch.

ringloom-queue is a clean-room, high-performance, lock-free, memory-mapped IPC queue written in Zig. All headers are fixed-layout `extern struct` types — just `mmap` and cast. Zero parsing, zero allocations on the hot path.

---

## Table of Contents

1. [Overview](#overview)
2. [Terminology](#terminology)
3. [Queue Directory Layout](#queue-directory-layout)
4. [Shared Metadata File (`metadata.ringloom`)](#shared-metadata-file-metadataringloom)
5. [Queue Data File (`.ringloom`) Format](#queue-data-file-ringloom-format)
6. [Message Framing (4-Byte Header)](#message-framing-4-byte-header)
7. [Flat Inline Index](#flat-inline-index)
8. [64-Bit Index Layout](#64-bit-index-layout)
9. [Roll Schemes](#roll-schemes)
10. [Cycle Calculation](#cycle-calculation)
11. [File Pre-Allocation](#file-pre-allocation)
12. [Alignment and Endianness](#alignment-and-endianness)
13. [Concurrency](#concurrency)
14. [Invariants and Constraints](#invariants-and-constraints)

---

## Overview

A ringloom-queue is a persistent, memory-mapped message queue stored entirely within a single filesystem directory. The format is designed so that:

- Multiple threads can read and write concurrently using only `mmap` and atomic CPU instructions
- No broker or coordinator process is required
- The operating system kernel handles persistence (dirty page writeback)
- All structures are fixed-layout `extern struct` types — mmap and cast a pointer, zero parsing

The format consists of:
1. **One shared metadata file** (`metadata.ringloom`) — fixed 512-byte struct with roll config and atomic counters
2. **Zero or more queue data files** (`.ringloom`) — message storage, one per cycle period

---

## Terminology

| Term | Definition |
|------|-----------|
| **Queue** | A directory on disk containing `metadata.ringloom` and `.ringloom` data files |
| **Cycle** | A time period (e.g., one day, one hour) corresponding to one `.ringloom` file |
| **Seqnum** | The sequential message number within a single cycle/file |
| **Index** | A 64-bit value combining cycle (upper 32 bits) and seqnum (lower 32 bits) |
| **Appender** | A thread writing messages to the queue |
| **Tailer** | A thread reading messages from the queue |
| **Roll** | The act of closing one cycle's file and starting the next |
| **Header** | The 4-byte framing word preceding every message |

---

## Queue Directory Layout

```
queue_directory/
├── metadata.ringloom               # Shared metadata (fixed 512-byte struct, mmap'd)
├── 20240115.ringloom               # Queue data file for cycle N
├── 20240116.ringloom               # Queue data file for cycle N+1
└── ...
```

File extension is `.ringloom`. Metadata file is `metadata.ringloom`.

### Filename Convention

Queue data filenames are derived from the cycle number using the roll scheme's date format:

```
filename = strftime(cycle * roll_length_secs, roll_format) + ".ringloom"
```

Examples for different schemes:

| Scheme | Cycle 0 Filename | Cycle 1 Filename |
|--------|-----------------|-----------------|
| `DAILY` (`yyyyMMdd`) | `19700101.ringloom` | `19700102.ringloom` |
| `FAST_DAILY` (`yyyyMMdd'F'`) | `19700101F.ringloom` | `19700102F.ringloom` |
| `FAST_HOURLY` (`yyyyMMdd-HH'F'`) | `19700101-00F.ringloom` | `19700101-01F.ringloom` |
| `FIVE_MINUTELY` (`yyyyMMdd-HHmm'V'`) | `19700101-0000V.ringloom` | `19700101-0005V.ringloom` |
| `TEST_SECONDLY` (`yyyyMMdd-HHmmss'T'`) | `19700101-000000T.ringloom` | `19700101-000001T.ringloom` |

The time represented is UTC. Cycle 0 corresponds to the Unix epoch (1970-01-01T00:00:00Z) plus the configured epoch offset.

---

## Shared Metadata File (`metadata.ringloom`)

The shared metadata file is a fixed 512-byte `extern struct` — all fields at known offsets. Processes mmap the file and cast the pointer directly to `*SharedMetadata`. Zero parsing.

### Struct Definition

```zig
pub const SharedMetadata = extern struct {
    magic: u32 = 0x4D515A42,         // "BZQM" bytes in little-endian u32 form
    version: u16 = 1,
    flags: u16 = 0,
    roll_length_secs: u32,
    index_spacing: u32,
    index_count: u32,
    epoch_ms: u64,
    highest_cycle: u64 align(8),      // atomic — highest active cycle
    lowest_cycle: u64 align(8),       // atomic — lowest active cycle
    modcount: u64 align(8),           // atomic — modification counter
    write_position: u64 align(8),     // atomic — published byte offset in active cycle
    appender_lock: u64 align(8),      // atomic — 0 = unlocked, non-zero = owner token
    _reserved: [440]u8 = [_]u8{0} ** 440,
    // Total: 512 bytes (one disk sector)
};
```

### Byte-Level Offset Table

| Offset | Size | Type | Field | Description |
|--------|------|------|-------|-------------|
| 0 | 4 | `u32` | `magic` | Magic number `0x4D515A42` (`"BZQM"` bytes on little-endian systems) |
| 4 | 2 | `u16` | `version` | Format version (currently `1`) |
| 6 | 2 | `u16` | `flags` | Reserved flags (currently `0`) |
| 8 | 4 | `u32` | `roll_length_secs` | Roll period in seconds (e.g., 86400 for daily) |
| 12 | 4 | `u32` | `index_spacing` | Messages between index entries (e.g., 256) |
| 16 | 4 | `u32` | `index_count` | Number of index entries per file (e.g., 4096) |
| 20 | 4 | — | _(padding)_ | Implicit padding for `u64` alignment |
| 24 | 8 | `u64` | `epoch_ms` | Epoch offset in milliseconds (typically 0) |
| 32 | 8 | `u64` | `highest_cycle` | Highest active cycle number (atomic) |
| 40 | 8 | `u64` | `lowest_cycle` | Lowest active cycle number (atomic) |
| 48 | 8 | `u64` | `modcount` | Modification counter, atomically incremented (atomic) |
| 56 | 8 | `u64` | `write_position` | Published write offset in `highest_cycle` / active cycle (atomic) |
| 64 | 8 | `u64` | `appender_lock` | Appender lease; `0` = unlocked, non-zero = owner token (atomic) |
| 72 | 440 | `[440]u8` | `_reserved` | Reserved, zero-filled |
| **512** | — | — | — | **Total size** |

### Notes

- The file is exactly 512 bytes — one disk sector — ensuring atomic writeback on most hardware.
- All atomic fields (`highest_cycle`, `lowest_cycle`, `modcount`, `write_position`, `appender_lock`) are 8-byte aligned for correct atomic access through the shared mapping.
- The `magic` field must be `0x4D515A42` or the file is corrupt / not a ringloom-queue metadata file.
- The magic constants are chosen so a hex dump of the little-endian file starts with readable ASCII (`42 5A 51 4D` = `BZQM`, `42 5A 51 43` = `BZQC`).
- `write_position` is updated with release ordering after the final message header is published; tailers read it with acquire ordering to avoid probing beyond committed data.
- `appender_lock` is a lifecycle lease, not a per-append lock. An appender CASes it from `0` to an owner token when opened and restores `0` on close. No global lock is taken on the hot path.

---

## Queue Data File (`.ringloom`) Format

Each `.ringloom` queue data file stores the messages for one cycle. Its structure is:

1. **File header** (offset 0, 64 bytes) — fixed `extern struct QueueFileHeader`
2. **Index region** (offset 64, `index_count × 8` bytes) — flat array of `u64` offsets
3. **Data region** (offset `64 + index_count × 8`) — sequence of 4-byte-header-framed messages
4. **EOF marker** — 4-byte header with EOF bits (written when the cycle rolls)
5. **Unallocated space** — zeros (pre-allocated file space)

### Overall Structure

```
Offset 0x0000:
┌────────────────────────────────────────────────────────────┐
│ QueueFileHeader (64 bytes, fixed extern struct)             │
│   magic, version, flags, roll config, created_cycle         │
├────────────────────────────────────────────────────────────┤
│ Index Region (index_count × 8 bytes)                        │
│   Flat array of u64 byte offsets into the data region       │
│   Entry i → byte offset of message at seqnum i×index_spacing│
├────────────────────────────────────────────────────────────┤
│ DATA: Message seqnum=0                                      │
│   [4-byte header] [payload] [padding]                       │
├────────────────────────────────────────────────────────────┤
│ DATA: Message seqnum=1                                      │
│   [4-byte header] [payload] [padding]                       │
├────────────────────────────────────────────────────────────┤
│ ... more data messages ...                                  │
├────────────────────────────────────────────────────────────┤
│ EOF: End of file marker (when cycle is rolled)              │
│   Header: 0xC0000000                                        │
├────────────────────────────────────────────────────────────┤
│ UNALLOCATED: Zero bytes (pre-allocated file space)          │
│   0x00000000 0x00000000 ...                                 │
└────────────────────────────────────────────────────────────┘
```

### QueueFileHeader Struct Definition

```zig
pub const QueueFileHeader = extern struct {
    magic: u32 = 0x43515A42,         // "BZQC" bytes in little-endian u32 form
    version: u16 = 1,
    flags: u16 = 0,
    roll_length_secs: u32,
    index_spacing: u32,
    index_count: u32,
    epoch_ms: u64,
    created_cycle: u32,
    _reserved: [28]u8 = [_]u8{0} ** 28,
    // Total: 64 bytes
};
```

### QueueFileHeader Byte-Level Offset Table

| Offset | Size | Type | Field | Description |
|--------|------|------|-------|-------------|
| 0 | 4 | `u32` | `magic` | Magic number `0x43515A42` (`"BZQC"` bytes on little-endian systems) |
| 4 | 2 | `u16` | `version` | Format version (currently `1`) |
| 6 | 2 | `u16` | `flags` | Reserved flags (currently `0`) |
| 8 | 4 | `u32` | `roll_length_secs` | Roll period in seconds |
| 12 | 4 | `u32` | `index_spacing` | Messages between index entries |
| 16 | 4 | `u32` | `index_count` | Number of index entries in this file |
| 20 | 4 | — | _(padding)_ | Implicit padding for `u64` alignment |
| 24 | 8 | `u64` | `epoch_ms` | Epoch offset in milliseconds |
| 32 | 4 | `u32` | `created_cycle` | Cycle number this file was created for |
| 36 | 28 | `[28]u8` | `_reserved` | Reserved, zero-filled |
| **64** | — | — | — | **Total size** |

### Notes

- The `magic` field must be `0x43515A42` or the file is corrupt / not a ringloom-queue data file.
- Roll configuration fields (`roll_length_secs`, `index_spacing`, `index_count`, `epoch_ms`) are duplicated from the shared metadata for self-describing files and integrity checking.

---

## Message Framing (4-Byte Header)

Every message in the data region is preceded by a 4-byte little-endian header word. This header serves dual purpose: message framing and write arbitration via atomic CAS.

### Header Word Layout

```
Bit:  31  30  29                              0
     ┌───┬───┬──────────────────────────────────┐
     │ W │ M │          SIZE                     │
     └───┴───┴──────────────────────────────────┘

W = Working bit (bit 31)
M = Metadata bit (bit 30)
SIZE = 30-bit payload size (bits 0–29)
```

### Header Types

| W | M | Hex Value(s) | Meaning |
|---|---|-------------|---------|
| 0 | 0 | `0x00000000` | **UNALLOCATED** — no message written here yet |
| 0 | 0 | `0x00000001`–`0x3FFFFFFF` | **DATA** — user message; value = payload byte count |
| 0 | 1 | `0x40000001`–`0x7FFFFFFF` | **METADATA** — internal message; value & `0x3FFFFFFF` = payload byte count |
| 1 | 0 | `0x80000000` | **WORKING** — write lock held, write in progress (no PID encoded) |
| 1 | 1 | `0xC0000000` | **EOF** — end of file, cycle has rolled |

### Bitmask Constants

```
HD_UNALLOCATED = 0x00000000
HD_WORKING     = 0x80000000
HD_METADATA    = 0x40000000
HD_EOF         = 0xC0000000
HD_MASK_LENGTH = 0x3FFFFFFF    (extracts payload size from lower 30 bits)
HD_MASK_META   = 0xC0000000    (extracts type from upper 2 bits)
```

### Constraints

- Maximum payload size: `0x3FFFFFFF` = 1,073,741,823 bytes (≈ 1 GiB)
- Payload size MUST be positive for DATA and METADATA messages
- `HD_UNALLOCATED` is the full 32-bit zero value `0x00000000`
- `HD_EOF` is the specific value `0xC0000000` (SIZE field is zero)
- `HD_WORKING` is the specific value `0x80000000` (SIZE field is zero, no PID)

### Message Layout in File

```
Offset  Content
+0      [4 bytes] Header word (little-endian u32)
+4      [N bytes] Payload data
+4+N    [P bytes] Zero padding where P = (-N) & 0x03
+4+N+P  [4 bytes] Next header word (now 4-byte aligned)
```

### Alignment Padding

After each message's payload, zero-padding bytes are inserted to align the next header to a 4-byte boundary:

```
padding = (-payload_size) & 0x03
```

This formula computes the number of bytes needed to round up to the next multiple of 4.

**Examples:**

| Payload Size | Padding | Total Entry Size | Calculation |
|-------------|---------|-----------------|-------------|
| 1 | 3 | 4 + 1 + 3 = 8 | `(-1) & 0x03 = 3` |
| 2 | 2 | 4 + 2 + 2 = 8 | `(-2) & 0x03 = 2` |
| 3 | 1 | 4 + 3 + 1 = 8 | `(-3) & 0x03 = 1` |
| 4 | 0 | 4 + 4 + 0 = 8 | `(-4) & 0x03 = 0` |
| 5 | 3 | 4 + 5 + 3 = 12 | `(-5) & 0x03 = 3` |
| 8 | 0 | 4 + 8 + 0 = 12 | `(-8) & 0x03 = 0` |
| 100 | 0 | 4 + 100 + 0 = 104 | `(-100) & 0x03 = 0` |
| 101 | 3 | 4 + 101 + 3 = 108 | `(-101) & 0x03 = 3` |

This ensures the CAS target (4-byte header) is always naturally aligned, which is required for atomic `cmpxchg` to behave correctly.

---

## Flat Inline Index

The index region is a flat array of `index_count` `u64` values immediately after the 64-byte file header. There is no two-level index structure — just a single contiguous array.

### Layout

```
File offset 64:
┌──────────────────────────────────────────────────────┐
│ index[0]     (u64)  → byte offset of msg at seqnum 0                │
│ index[1]     (u64)  → byte offset of msg at seqnum index_spacing    │
│ index[2]     (u64)  → byte offset of msg at seqnum 2×index_spacing  │
│ ...                                                                  │
│ index[index_count-1] (u64)  → byte offset of last indexed msg       │
└──────────────────────────────────────────────────────┘
```

### Properties

- **Offset of entry `i`** = `64 + (i × 8)`
- **Value** = byte offset from file start of the data message at `seqnum = i × index_spacing`
- **Value `0`** = entry not yet written
- Entries are written **atomically** by the appender every `index_spacing` messages
- Readers **binary-search** the index for O(log n) seek to any seqnum

### FAST_DAILY Defaults

With the default `FAST_DAILY` roll scheme (`index_count = 4096`, `index_spacing = 256`):

| Property | Value |
|----------|-------|
| Index entries | 4,096 |
| Index region size | 4,096 × 8 = 32,768 bytes (32 KiB) |
| Data region starts at | 64 + 32,768 = **32,832** (offset `0x8040`) |
| Messages covered by one index page | 4,096 × 256 = **1,048,576** messages |

### Seek Algorithm

To seek to a target seqnum:

1. Compute `index_slot = target_seqnum / index_spacing`
2. If `index_slot < index_count` and `index[index_slot] != 0`:
   - Jump to the byte offset stored in `index[index_slot]`
   - The message at that offset has `seqnum = index_slot × index_spacing`
   - Scan forward `target_seqnum - (index_slot × index_spacing)` messages
3. If the index slot is empty (`0`), binary-search backwards for the nearest populated slot, then scan forward
4. If no populated slot exists, start at the data-region offset with seqnum `0`
5. Worst case in a healthy file: scan forward `index_spacing - 1` messages from the nearest index entry

### Index Sparsity and Recovery

Index entries are an acceleration structure, not the source of truth. The source of truth is the message stream: a released DATA header commits a message; an EOF header commits a cycle boundary.

The appender writes an index entry after publishing every `index_spacing`-th message. If a process crashes after publishing the DATA header but before writing the index entry, the message remains valid and discoverable by linear scan.

Required behavior:

1. `index[0]` should be written during normal creation of the first message in a cycle, because it points to the first data-region offset.
2. Seek must tolerate any index slot containing `0`.
3. On appender startup, before appending to an existing active cycle, recovery scans forward from the last populated index entry and repairs any missing index entries up to the last committed message.
4. If recovery is skipped for read-only opens, tailers still seek correctly by falling back to linear scan from the nearest earlier populated slot or from the data-region start.

```zig
fn seekOffset(index: IndexRegion, target_seqnum: u32, data_start: u64) !SeekPoint {
    const target_slot = target_seqnum / index.spacing;
    var slot = @min(target_slot, index.count - 1);

    while (true) {
        if (index.lookup(slot)) |offset| {
            return .{ .offset = offset, .seqnum = slot * index.spacing };
        }
        if (slot == 0) break;
        slot -= 1;
    }

    return .{ .offset = data_start, .seqnum = 0 };
}
```

---

## 64-Bit Index Layout

The 64-bit index combines the cycle number and sequence number into a single value for cross-file addressing:

```
63                              32 31                              0
┌──────────────────────────────────┬──────────────────────────────────┐
│              CYCLE               │             SEQNUM               │
│         (upper 32 bits)          │         (lower 32 bits)          │
└──────────────────────────────────┴──────────────────────────────────┘
```

### Decomposition

```
cycle_shift  = 32
seqnum_mask  = 0x00000000FFFFFFFF

cycle  = index >> cycle_shift           // upper 32 bits
seqnum = index & seqnum_mask            // lower 32 bits
index  = (cycle << cycle_shift) | seqnum
```

### Example

For a `FAST_DAILY` queue with `epoch_ms = 0`:

| Index (hex) | Cycle | Seqnum | Date | Filename |
|-------------|-------|--------|------|----------|
| `0x0000_4A05_0000_0000` | 18,949 | 0 | 2021-11-18 | `20211118F.ringloom` |
| `0x0000_4A05_0000_0003` | 18,949 | 3 | 2021-11-18 | `20211118F.ringloom` |
| `0x0000_4A06_0000_0000` | 18,950 | 0 | 2021-11-19 | `20211119F.ringloom` |

---

## Roll Schemes

### Complete Roll Scheme Table

| Name | Format | Roll Period | `index_count` | `index_spacing` |
|------|--------|-------------|---------------|-----------------|
| `FIVE_MINUTELY` | `yyyyMMdd-HHmm'V'` | 5 min | 2,048 | 256 |
| `TEN_MINUTELY` | `yyyyMMdd-HHmm'X'` | 10 min | 2,048 | 256 |
| `TWENTY_MINUTELY` | `yyyyMMdd-HHmm'XX'` | 20 min | 2,048 | 256 |
| `HALF_HOURLY` | `yyyyMMdd-HHmm'H'` | 30 min | 2,048 | 256 |
| `FAST_HOURLY` | `yyyyMMdd-HH'F'` | 1 hour | 4,096 | 256 |
| `TWO_HOURLY` | `yyyyMMdd-HH'II'` | 2 hours | 4,096 | 256 |
| `FOUR_HOURLY` | `yyyyMMdd-HH'IV'` | 4 hours | 4,096 | 256 |
| `SIX_HOURLY` | `yyyyMMdd-HH'VI'` | 6 hours | 4,096 | 256 |
| `FAST_DAILY` | `yyyyMMdd'F'` | 24 hours | 4,096 | 256 |
| `MINUTELY` | `yyyyMMdd-HHmm` | 1 min | 2,048 | 16 |
| `HOURLY` | `yyyyMMdd-HH` | 1 hour | 4,096 | 16 |
| `DAILY` | `yyyyMMdd` | 24 hours | 8,192 | 64 |
| `LARGE_HOURLY` | `yyyyMMdd-HH'L'` | 1 hour | 8,192 | 64 |
| `LARGE_DAILY` | `yyyyMMdd'L'` | 24 hours | 32,768 | 128 |
| `XLARGE_DAILY` | `yyyyMMdd'X'` | 24 hours | 32,768 | 256 |
| `HUGE_DAILY` | `yyyyMMdd'H'` | 24 hours | 32,768 | 1,024 |
| `SMALL_DAILY` | `yyyyMMdd'S'` | 24 hours | 8,192 | 8 |
| `LARGE_HOURLY_SPARSE` | `yyyyMMdd-HH'LS'` | 1 hour | 4,096 | 1,024 |
| `LARGE_HOURLY_XSPARSE` | `yyyyMMdd-HH'LX'` | 1 hour | 2,048 | 1,048,576 |
| `HUGE_DAILY_XSPARSE` | `yyyyMMdd'HX'` | 24 hours | 16,384 | 1,048,576 |
| `TEST_SECONDLY` | `yyyyMMdd-HHmmss'T'` | 1 sec | 32,768 | 4 |
| `TEST4_SECONDLY` | `yyyyMMdd-HHmmss'T4'` | 1 sec | 32 | 4 |
| `TEST_HOURLY` | `yyyyMMdd-HH'T'` | 1 hour | 16 | 4 |
| `TEST_DAILY` | `yyyyMMdd'T1'` | 24 hours | 8 | 1 |
| `TEST2_DAILY` | `yyyyMMdd'T2'` | 24 hours | 16 | 2 |
| `TEST4_DAILY` | `yyyyMMdd'T4'` | 24 hours | 32 | 4 |
| `TEST8_DAILY` | `yyyyMMdd'T8'` | 24 hours | 128 | 8 |

### Format String Conversion

The format strings use Java-style date patterns. The equivalent POSIX `strftime` conversions are:

| Java Pattern | strftime | Meaning |
|-------------|----------|---------|
| `yyyy` | `%Y` | 4-digit year |
| `MM` | `%m` | 2-digit month (01–12) |
| `dd` | `%d` | 2-digit day (01–31) |
| `HH` | `%H` | 2-digit hour (00–23) |
| `mm` | `%M` | 2-digit minute (00–59) |
| `ss` | `%S` | 2-digit second (00–59) |
| `'...'` | literal | Quoted literal text (apostrophes stripped) |
| `-` | `-` | Literal dash |

### Default Scheme

The default roll scheme is **`FAST_DAILY`** (`roll_length_secs = 86400`, `index_count = 4096`, `index_spacing = 256`).

---

## Cycle Calculation

```
cycle = (current_time_ms - epoch_ms) / roll_length_ms
```

Where:
- `current_time_ms` = milliseconds since Unix epoch (1970-01-01 00:00:00 UTC)
- `epoch_ms` = configured epoch offset in milliseconds (typically 0)
- `roll_length_ms` = `roll_length_secs × 1000`

Cycle calculation is UTC-only. Local time zones and daylight-saving transitions must not affect cycle numbers or filenames.

### Clock Handling

- Appends use wall-clock UTC milliseconds to choose the roll cycle.
- The appender must never roll backwards. If the wall clock moves backwards and computes a cycle lower than the currently open cycle, keep writing to the current cycle.
- Pre-roll scheduling should use a monotonic timestamp for relative delays once the next wall-clock boundary has been computed. This prevents NTP slews or administrator clock changes from repeatedly creating or cancelling pre-roll work.
- If the wall clock jumps forward by multiple cycles, the appender writes EOF to the old cycle and opens the computed current cycle; it does not create empty intermediate cycle files unless configured for audit completeness.

### Filename from Cycle

```
time_seconds = cycle × roll_length_secs
filename = dirname + "/" + strftime(time_seconds + epoch_secs, converted_format) + ".ringloom"
```

---

## File Pre-Allocation

Queue data files are pre-allocated at creation time using the platform's real
allocation primitive: `fallocate(2)` on Linux, and `fcntl(F_PREALLOCATE)`
followed by `ftruncate` on macOS. Unlike the `lseek` + `write` approach, this
reserves disk blocks in the filesystem where supported, eliminating block
allocation faults during writes.

| Property | Value |
|----------|-------|
| Default pre-allocation size | 83,754,496 bytes (≈ 79.9 MiB) |
| Extension trigger | Fewer than `2 × blocksize` bytes remaining |
| Extension size | Another 83,754,496 bytes |

The entire pre-allocated region is zero-filled, which is recognized as `HD_UNALLOCATED` (header value `0x00000000`).

Pre-allocation alone does not remove every page-fault source. It prevents filesystem block allocation from occurring on first write, but the process can still take minor faults when page-table entries are installed, and on some kernels the first write can still fault if a page was populated read-only. For the appender latency profile, each future writable window must be prepared before the appender reaches it:

1. Preallocate the file range with the platform implementation.
2. Map the future window in a prefetcher (helper thread in native Zig, caller-driven poll in the C ABI).
3. Prefer `madvise(MADV_POPULATE_WRITE)` where available; otherwise use `MAP_POPULATE` where available plus a manual write-touch of one byte per page in still-unallocated space.
4. Optionally `mlock` the current and next appender windows when `RLIMIT_MEMLOCK` allows it.
5. Hand the ready mapping to the appender for pointer swap.

The manual write-touch must be idempotent and constrained to future unallocated pages. It may store `0` to one byte in each page because the preallocated region is already zero-filled, but it must never touch at or behind the appender's claimed/published write position.

---

## Alignment and Endianness

### Byte Order

All multi-byte values in the entire format are **little-endian**.

### Alignment Requirements

| Item | Alignment | Reason |
|------|-----------|--------|
| Message 4-byte header | 4-byte aligned | Required for atomic CAS (`@cmpxchgStrong` / `lock cmpxchgl`) |
| `SharedMetadata` atomic fields | 8-byte aligned | Correct atomic load/store through shared mmap |
| Index entries (`u64` array) | 8-byte aligned | Naturally aligned — contiguous `u64` array starting at offset 64 |
| `mmap` offset | Page-aligned | OS requirement for `mmap()` |

---

## Concurrency

ringloom-queue supports **one active writer thread/process and multiple reader threads/processes** on the same machine.

### Write Path

1. Writer atomically CASes the 4-byte header from `HD_UNALLOCATED` (`0x00000000`) to `HD_WORKING` (`0x80000000`) using `@cmpxchgStrong` with monotonic ordering. This claims the slot; it does not publish payload bytes.
2. Writer copies payload bytes into the region after the header
3. Writer atomically stores the final header (`DATA | payload_size`) with `.release` ordering
4. If this message's seqnum is a multiple of `index_spacing`, atomically write the index entry
5. Writer release-stores the new `write_position` in `metadata.ringloom`

### Read Path

1. Reader performs an atomic `.acquire` load on the 4-byte header
2. If `HD_UNALLOCATED`: no message yet — spin or wait
3. If `HD_WORKING`: write in progress — spin or wait
4. If `DATA` or `METADATA`: read the payload bytes (acquire load on header provides barrier)
5. If `HD_EOF`: cycle has rolled — open next file

### Memory Ordering

- Slot-claim CAS on the 4-byte header uses monotonic ordering; the final header store is the release operation that publishes payload bytes
- On ARM/other architectures, acquire/release semantics ensure correct ordering
- No explicit `mfence` is needed beyond what the atomic operations provide

### Limitations

- Single active writer only; the `appender_lock` lease prevents accidental concurrent appenders
- Cross-machine access is **NOT** supported — shared memory primitives are machine-local
- Multiple reader threads are fully supported with no coordination needed

---

## Invariants and Constraints

### File System

1. The queue directory must exist before the queue is opened
2. All files within the queue directory are owned by the queue
3. File I/O must use `mmap` with `MAP_SHARED`; `read()`/`write()` syscalls may see stale data
4. `metadata.ringloom` must be exactly 512 bytes
5. Queue data files must start with a valid 64-byte `QueueFileHeader`

### Magic Numbers

1. `metadata.ringloom` must have magic `0x4D515A42` ("BZQM" bytes) at offset 0
2. Queue data files must have magic `0x43515A42` ("BZQC" bytes) at offset 0
3. Any other value indicates corruption or an incompatible file

### Message Ordering

1. All writes resolve into a total order preserved on replay
2. Writes within a single cycle file are totally ordered by file position
3. Cross-cycle ordering is determined by cycle number
4. The 64-bit index value is strictly monotonically increasing (within a cycle, seqnum increments by 1)

### Concurrency

1. Writers MUST use atomic CAS (`@cmpxchgStrong` / `lock cmpxchgl`) on the 4-byte header
2. Readers MUST use atomic acquire loads on the header before reading payload
3. Single writer thread, multiple reader threads supported
4. Cross-machine access is NOT supported (shared memory primitives are machine-local)

### Size Limits

| Limit | Value |
|-------|-------|
| Maximum message payload | 1,073,741,823 bytes (30-bit size field, ≈ 1 GiB) |
| Maximum seqnum | 4,294,967,295 (32-bit) |
| Maximum cycle | 4,294,967,295 (32-bit) |
| Maximum index entries per file | Limited by `index_count` (up to 32,768 in standard schemes) |

### Cycle Management

1. `highest_cycle` and `lowest_cycle` in the shared metadata must be kept consistent
2. `modcount` must be atomically incremented when the cycle range changes
3. EOF markers (`0xC0000000`) must be written when rolling to a new cycle
4. Missing EOF markers can be patched by appenders within a configurable number of cycles behind `highest_cycle`
5. Readers may skip past a missing EOF if the file's cycle is sufficiently behind `highest_cycle`

---

*This specification defines the ringloom-queue v1 on-disk format.
