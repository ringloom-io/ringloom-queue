# libchronicle Architecture Overview

## Table of Contents

1. [Introduction](#introduction)
2. [Project Summary](#project-summary)
3. [High-Level Architecture](#high-level-architecture)
4. [Core Concepts](#core-concepts)
5. [File Layout on Disk](#file-layout-on-disk)
6. [Memory-Mapped I/O Design](#memory-mapped-io-design)
7. [Message Framing and Header Protocol](#message-framing-and-header-protocol)
8. [Write Arbitration (Lock-Free CAS Protocol)](#write-arbitration-lock-free-cas-protocol)
9. [Index Structure](#index-structure)
10. [Roll Cycle Mechanism](#roll-cycle-mechanism)
11. [Directory Listing / Metadata File](#directory-listing--metadata-file)
12. [Wire Protocol (BinaryWire Serialization)](#wire-protocol-binarywire-serialization)
13. [Data Structures](#data-structures)
14. [Appender Lifecycle](#appender-lifecycle)
15. [Tailer Lifecycle](#tailer-lifecycle)
16. [Module Decomposition](#module-decomposition)
17. [Concurrency and Memory Ordering](#concurrency-and-memory-ordering)
18. [Error Handling](#error-handling)
19. [Diagrams](#diagrams)

---

## Introduction

This document provides a comprehensive architectural description of `libchronicle`, an open-source C implementation of the chronicle-queue shared-memory IPC protocol originally created by OpenHFT (Chronicle Software Ltd.) in Java. The goal is to serve as a definitive reference for understanding, maintaining, and reimplementing the library.

## Project Summary

| Property | Value |
|---|---|
| **Language** | C (gnu99) |
| **License** | Apache 2.0 |
| **Protocol Versions** | v5 |
| **IPC Mechanism** | Memory-mapped files (`mmap` + `MAP_SHARED`) |
| **Arbitration** | `lock cmpxchgl` (x86 CAS instruction) |
| **Memory Ordering** | `mfence` for read barriers |
| **Bindings** | Python (ctypes), kdb+ |
| **Test Framework** | cmocka, libarchive (for test data) |
| **Fuzzer** | AFL (American Fuzzy Lop) |

## High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        User Application                          │
│                                                                  │
│   ┌─────────────┐    ┌──────────────┐    ┌────────────────┐     │
│   │  Encoder     │    │   Decoder    │    │  Dispatch CB   │     │
│   │ (sizeof+write│    │ (parse+free) │    │ (index, msg)   │     │
│   └──────┬───────┘    └──────┬───────┘    └───────┬────────┘     │
│          │                   │                    │              │
├──────────┼───────────────────┼────────────────────┼──────────────┤
│          ▼                   ▼                    ▼              │
│  ┌────────────────────────────────────────────────────────┐      │
│  │                  libchronicle.h (Public API)           │      │
│  │                                                        │      │
│  │  chronicle_init()    chronicle_open()                  │      │
│  │  chronicle_append()  chronicle_tailer()                │      │
│  │  chronicle_peek()    chronicle_collect()               │      │
│  │  chronicle_cleanup()                                   │      │
│  └────────────────────┬───────────────────────────────────┘      │
│                       │                                          │
│  ┌────────────────────┼───────────────────────────────────┐      │
│  │            libchronicle.c (Implementation)             │      │
│  │                    │                                   │      │
│  │  ┌────────────┐  ┌┴───────────┐  ┌─────────────────┐  │      │
│  │  │  Appender  │  │   Tailer   │  │ Directory List.  │  │      │
│  │  │  (writer)  │  │  (reader)  │  │ (cycle tracker)  │  │      │
│  │  └─────┬──────┘  └─────┬──────┘  └────────┬────────┘  │      │
│  │        │               │                   │           │      │
│  │  ┌─────┴───────────────┴───────────────────┴────────┐  │      │
│  │  │         mmap / CAS / mfence primitives           │  │      │
│  │  └──────────────────────┬───────────────────────────┘  │      │
│  └─────────────────────────┼──────────────────────────────┘      │
│                            │                                     │
│  ┌─────────────────────────┼──────────────────────────────┐      │
│  │               wire.c / wire.h                          │      │
│  │          BinaryWire serialization format               │      │
│  └─────────────────────────┬──────────────────────────────┘      │
│                            │                                     │
│  ┌─────────────────────────┼──────────────────────────────┐      │
│  │              buffer.c / buffer.h                       │      │
│  │          Hex dump and debug formatting                 │      │
│  └─────────────────────────┬──────────────────────────────┘      │
│                            │                                     │
└────────────────────────────┼─────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Operating System / Kernel                      │
│                                                                  │
│   ┌──────────┐   ┌──────────┐   ┌────────────────────────┐      │
│   │  mmap()  │   │  open()  │   │  fstat() / lseek()     │      │
│   │MAP_SHARED│   │ O_RDWR   │   │  rename() / write()    │      │
│   └──────────┘   └──────────┘   └────────────────────────┘      │
│                                                                  │
│   ┌──────────────────────────────────────────────────────┐       │
│   │              File System (queue directory)            │       │
│   │                                                      │       │
│   │  metadata.cq4t   20211118F.cq4   20211119F.cq4      │       │
│   └──────────────────────────────────────────────────────┘       │
└──────────────────────────────────────────────────────────────────┘
```

## Core Concepts

### Queue
A chronicle-queue is a **directory** on disk containing:
- One metadata/directory-listing file (`.cq4t`)
- Zero or more queue data files (`.cq4`)

There is **no broker process**. The OS kernel provides persistence via `mmap` with `MAP_SHARED`, and x86 hardware provides atomic arbitration via `lock cmpxchgl`.

### Appender
A writer process. Appends messages to the queue and receives a 64-bit **index** identifying the write position. Multiple appenders on the same machine are supported.

### Tailer
A reader/subscriber process. Reads messages sequentially starting from index 0 or a provided resume index. Multiple tailers are supported and can be added/removed at will.

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

The `cycle_shift` is currently hardcoded to 32 bits in libchronicle (matching the DAILY scheme split). The upper bits are the **cycle** (maps to a time-based filename) and the lower bits are the **seqnum** (sequential message number within that file).

### Roll Cycle
Determines when a new queue file is started. The cycle value is derived from the current clock:

```
cycle = (current_time_ms - roll_epoch) / roll_length_ms
```

When the cycle advances, `seqnum` resets to zero and a new `.cq4` file is created.

## File Layout on Disk

```
queue_directory/
├── metadata.cq4t              # roll config + cycle counters (mmap'd)
│
├── 20211118F.cq4              # Queue data file for cycle N
├── 20211119F.cq4              # Queue data file for cycle N+1
└── ...
```

### Queue File (`.cq4`) Internal Layout

```
Offset 0x0000
┌─────────────────────────────────────────────────────────────┐
│ [4-byte header: METADATA | size] Metadata: "header" block   │
│    Contains: index2index, indexing config                     │
│    Serialized using BinaryWire format                        │
├─────────────────────────────────────────────────────────────┤
│ [4-byte header: METADATA | size] Index2Index page            │
│    Array of byte positions pointing to index pages           │
├─────────────────────────────────────────────────────────────┤
│ [4-byte header: METADATA | size] Index page(s)              │
│    Array of byte positions pointing to data messages         │
├─────────────────────────────────────────────────────────────┤
│ [4-byte header: DATA | size] Data message 0                 │
│    User payload bytes                                        │
├─────────────────────────────────────────────────────────────┤
│ [4-byte header: DATA | size] Data message 1                 │
│    User payload bytes                                        │
├─────────────────────────────────────────────────────────────┤
│                        ...                                   │
├─────────────────────────────────────────────────────────────┤
│ [4-byte header: EOF]  End of file marker                     │
├─────────────────────────────────────────────────────────────┤
│ [HD_UNALLOCATED = 0x00000000 ...]  Unallocated space         │
└─────────────────────────────────────────────────────────────┘
```

Data and metadata messages are **4-byte aligned** (padded with `(-size) & 0x03` zero bytes after each entry).

## Memory-Mapped I/O Design

### Why mmap?

The kernel guarantees that `MAP_SHARED` mappings of the same file region by multiple processes map to the **same physical pages**. This enables shared-memory IPC without explicit shared memory APIs.

### Chunked Mapping Strategy

Files are not mapped entirely. Instead, they are mapped in chunks of `blocksize` bytes (default: 1 MiB, always a power of two):

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
- If a message is larger than blocksize, blocksize is **doubled** (`queue_double_blocksize`)
- The mmap is only refreshed when the desired window changes

### File Extension (Appenders Only)

When an appender detects fewer than 2 blocks remain, it extends the file:
1. `lseek()` to the new end position
2. `write()` a single byte to materialize the space
3. Re-`fstat()` to pick up the new size

The default extension size is ~83.7 MB (`qf_disk_sz = 83754496L`).

## Message Framing and Header Protocol

Every message (data or metadata) is preceded by a 4-byte header:

```
┌────────────────────────────────────────────────┐
│              32-bit Header Word                 │
│                                                 │
│  Bit 31    Bit 30    Bits 29-0                  │
│  (working) (meta)    (size / pid)               │
│                                                 │
│  0         0         0x00000000  → UNALLOCATED  │
│  0         0         size        → DATA payload │
│  0         1         size        → METADATA     │
│  1         0         pid         → WORKING      │
│  1         1         0x00000000  → EOF          │
└────────────────────────────────────────────────┘

Constants:
  HD_UNALLOCATED = 0x00000000
  HD_WORKING     = 0x80000000
  HD_METADATA    = 0x40000000
  HD_EOF         = 0xC0000000
  HD_MASK_LENGTH = 0x3FFFFFFF
  HD_MASK_META   = 0xC0000000  (= HD_EOF)
```

Maximum payload size: `0x3FFFFFFF` = 1,073,741,823 bytes (~1 GiB).

## Write Arbitration (Lock-Free CAS Protocol)

Writers compete for the write position using a compare-and-swap (CAS) operation on the 4-byte header at the current tail position:

```
Writer Protocol:
                                                    
  ┌─────────────────────────────────────┐           
  │ 1. Read header at tail position      │           
  │    Expected: HD_UNALLOCATED (0x0)    │           
  └──────────────┬──────────────────────┘           
                 │                                   
                 ▼                                   
  ┌─────────────────────────────────────┐           
  │ 2. CAS: UNALLOCATED → WORKING       │           
  │    lock cmpxchgl(&header, 0, 0x80..)│           
  │                                      │           
  │    Returns old value:                │           
  │    - If 0x0: WE WON THE LOCK ──────►├──┐       
  │    - Otherwise: LOST, retry ────────►├──┤       
  └─────────────────────────────────────┘  │       
                                            │       
          ┌─────────────────────────────────┘       
          ▼                                         
  ┌─────────────────────────────────────┐           
  │ 3. mfence (write barrier)            │           
  └──────────────┬──────────────────────┘           
                 │                                   
                 ▼                                   
  ┌─────────────────────────────────────┐           
  │ 4. Write payload to (header + 4)     │           
  │    Using append_write callback       │           
  └──────────────┬──────────────────────┘           
                 │                                   
                 ▼                                   
  ┌─────────────────────────────────────┐           
  │ 5. mfence (write barrier)            │           
  └──────────────┬──────────────────────┘           
                 │                                   
                 ▼                                   
  ┌─────────────────────────────────────┐           
  │ 6. Write final header:               │           
  │    header = size & HD_MASK_LENGTH    │           
  │    (clears WORKING bit, sets size)   │           
  └─────────────────────────────────────┘           


Reader Protocol:
                                                    
  ┌─────────────────────────────────────┐           
  │ 1. Read 4-byte header                │           
  └──────────────┬──────────────────────┘           
                 │                                   
                 ▼                                   
  ┌─────────────────────────────────────┐           
  │ 2. mfence (read barrier)             │           
  │    Prevents prefetch of payload      │           
  │    before header is confirmed        │           
  └──────────────┬──────────────────────┘           
                 │                                   
                 ▼                                   
  ┌─────────────────────────────────────┐           
  │ 3. Decode header:                    │           
  │    UNALLOCATED → wait/return         │           
  │    WORKING     → busy, retry later   │           
  │    METADATA    → parse, skip         │           
  │    EOF         → advance cycle       │           
  │    DATA        → parse + dispatch    │           
  └─────────────────────────────────────┘           
```

The `lock; cmpxchgl` instruction is implemented via inline assembly:

```c
static inline uint32_t lock_cmpxchgl(unsigned char *mem, uint32_t newval, uint32_t oldval) {
    __typeof (*mem) ret;
    __asm __volatile ("lock; cmpxchgl %2, %1"
    : "=a" (ret), "=m" (*mem)
    : "r" (newval), "m" (*mem), "0" (oldval));
    return (uint32_t) ret;
}
```

This returns the **original** value in memory. If it equals `oldval`, the swap succeeded.

## Index Structure

The index is a double-level structure stored as metadata messages within the queue file:

```
Index2Index Page (root):
┌──────────────────────────────────────────────────────┐
│ I64_ARRAY: capacity = index_count                     │
│                                                       │
│ [0] → byte offset of Index Page 0                     │
│ [1] → byte offset of Index Page 1                     │
│ [2] → byte offset of Index Page 2                     │
│ ...                                                   │
│ [index_count-1] → byte offset of Index Page N         │
└──────────────────────────────────────────────────────┘
          │
          ▼
Index Page 0:
┌──────────────────────────────────────────────────────┐
│ I64_ARRAY: capacity = index_count                     │
│                                                       │
│ [0] → byte offset of data msg (seqnum 0)              │
│ [1] → byte offset of data msg (seqnum = index_spacing)│
│ [2] → byte offset of data msg (seqnum = 2*spacing)    │
│ ...                                                   │
└──────────────────────────────────────────────────────┘
```

Configuration per roll scheme:

| Scheme | index_count (entries per page) | index_spacing |
|---|---|---|
| DAILY | 8192 (8<<10) | 64 |
| FAST_HOURLY | 4096 (4<<10) | 256 |
| FAST_DAILY | 4096 (4<<10) | 256 |
| TEST4_SECONDLY | 32 | 4 |

> **Note:** libchronicle does **not** currently write index structures. This is listed as a missing feature. Reading of index structures is partially supported through the wire parser.

## Roll Cycle Mechanism

### Roll Schemes

libchronicle ships with 25+ built-in roll schemes. Each defines:

| Field | Description |
|---|---|
| `name` | Identifier (e.g., `"FAST_DAILY"`) |
| `formatstr` | Java-style date format (e.g., `"yyyyMMdd'F'"`) |
| `roll_length_secs` | Duration of each cycle in seconds |
| `entries` | `index_count` — entries per index page |
| `index` | `index_spacing` — every Nth message is indexed |

### Date Format Conversion

Java date format strings (e.g., `yyyyMMdd-HH'F'`) are converted to `strftime` patterns:

| Java | strftime |
|---|---|
| `yyyy` | `%Y` |
| `MM` | `%m` |
| `dd` | `%d` |
| `HH` | `%H` |
| `mm` | `%M` |
| `ss` | `%S` |
| `'...'` | literal (quotes stripped) |
| `-` | literal dash |

Example: `yyyyMMdd-HH'F'` → `%Y%m%d-%HF`

### Filename Generation

```
filename = dirname + "/" + strftime(cycle * roll_length_secs) + ".cq4"
```

The cycle number maps to a time: `time_t rawtime = cycle * (roll_length / 1000)`

### Roll Detection During Append

```
current_cycle = (clock_ms - roll_epoch) / roll_length_ms

if current_cycle > appender_cycle:
    1. CAS-lock the current position
    2. Write HD_EOF marker
    3. Create new queue file for current_cycle
    4. Update highest_cycle in directory listing
    5. Bump modcount (atomic increment)
    6. Retry append in new file
```

### EOF Patching

If an appender finds itself holding the write lock but the current file's cycle is behind `highest_cycle`, it writes an EOF marker to "patch" the old file. This handles the case where a previous writer crashed without writing EOF. The `patch_cycles` constant (default: 3) controls how far back this lookback extends.

## Directory Listing / Metadata File

### Purpose

The directory listing file (`metadata.cq4t`) is a small file that is mmap'd by all processes. It contains:

1. **Roll configuration**
2. **Shared counters** (memory-mapped pointers for real-time updates):
   - `listing.highestCycle` — uint64, highest active cycle
   - `listing.lowestCycle` — uint64, lowest active cycle
   - `listing.modCount` — uint64, atomically incremented on changes

### Structure (v5 metadata.cq4t)

```
┌─────────────────────────────────────────────────────┐
│ METADATA message: "header" (STStore)                 │
│   wireType: BINARY_LIGHT                             │
│   metadata: (SCQMeta)                                │
│     roll: (SCQSRoll)                                 │
│       length: 86400000  (ms)                         │
│       format: "yyyyMMdd'F'"                          │
│       epoch: 0                                       │
│     deltaCheckpointInterval: 64                      │
│     sourceId: 0                                      │
├─────────────────────────────────────────────────────┤
│ DATA message: listing.highestCycle = uint64           │
│ DATA message: listing.lowestCycle  = uint64           │
│ DATA message: listing.modCount     = uint64           │
│ DATA message: chronicle.write.lock = uint64           │
│ DATA message: chronicle.lastIndexReplicated = uint64  │
│ DATA message: chronicle.lastAcknowledgedIndexRep...   │
└─────────────────────────────────────────────────────┘
```

### Modcount Protocol

```
Reader/Tailer polling:
  1. memcpy(&modcount, dirlist_fields.modcount, 8)
  2. If modcount != cached_modcount:
     - Copy highest_cycle, lowest_cycle from shared memory
     - May trigger opening new queue files

Writer updating:
  1. memcpy(dirlist_fields.highest_cycle, &new_value, 8)
  2. memcpy(dirlist_fields.lowest_cycle, &new_value, 8)
  3. lock_xadd(dirlist_fields.modcount, 1)  // atomic increment
```

## Wire Protocol (BinaryWire Serialization)

The wire protocol is used for metadata messages, directory listing contents, and optionally for user data payloads. It is a self-describing binary format.

### Control Bytes

| Range | Meaning |
|---|---|
| `0x00–0x7F` | Inline unsigned integer value (0–127) |
| `0x82` | `BYTES_LENGTH32` — nested structure with 4-byte length |
| `0x8D` | `I64_ARRAY` — array of 64-bit integers |
| `0x8E` | `PADDING_32` — variable-length padding |
| `0x8F` | `PADDING` — single byte padding |
| `0x90` | `FLOAT32` — 4-byte IEEE float |
| `0xA5` | `INT16` — 2-byte little-endian integer |
| `0xA6` | `INT32` — 4-byte little-endian integer |
| `0xA7` | `INT64` — 8-byte little-endian integer |
| `0xB6` | `TYPE_PREFIX` — type name with stop-bit length |
| `0xB8` | Text value with stop-bit length |
| `0xB9` | `EVENT_NAME` — event/key name with stop-bit length |
| `0xC0–0xDF` | Small field name (length = byte - 0xC0) |
| `0xE0–0xFF` | Small text value (length = byte - 0xE0) |

### Stop-Bit Encoding

Variable-length integer encoding where each byte's MSB indicates continuation:

```
Byte:  [1xxxxxxx] [1xxxxxxx] [0xxxxxxx]
        ▲ more      ▲ more     ▲ last
        7 bits       7 bits     7 bits
```

### Nesting

The `0x82` control byte introduces a nested structure. A 4-byte little-endian length follows, defining how many bytes the nested content spans. Nesting is tracked with a stack of end-positions.

### Alignment for Shared Memory Fields

Fields that are memory-mapped for concurrent access (like `listing.highestCycle`) use `INT64` with padding to ensure 8-byte alignment. The `wirepad_uint64_aligned` function handles this by inserting `0x8F` or `0x8E` padding bytes before the `0xA7` prefix.

## Data Structures

### `queue_t` (struct queue)

```
struct queue {
    char*             dirname;          // Queue directory path
    uint              blocksize;        // mmap chunk size (power of 2, default 1MiB)
    uint8_t           version;          // 4 or 5
    uint8_t           create;           // Permission to create queue files

    // Directory listing (mmap'd)
    char*             dirlist_name;     // Path to .cq4t file
    int               dirlist_fd;       // File descriptor
    unsigned char*    dirlist;          // mmap base pointer
    dirlist_fields_t  dirlist_fields;   // Pointers into mmap for live fields

    // File discovery
    char*             queuefile_pattern;// Glob pattern "dirname/*.cq4"
    glob_t            queuefile_glob;   // Results of glob()

    // Observed cycle range (from directory listing)
    uint64_t          highest_cycle;
    uint64_t          lowest_cycle;
    uint64_t          modcount;

    // Roll configuration
    int               roll_length;      // Roll period in milliseconds
    int               roll_epoch;       // Epoch offset in milliseconds
    char*             roll_format;      // Java-style date format
    char*             roll_name;        // Scheme name (e.g., "FAST_DAILY")
    char*             roll_strftime;    // Converted strftime pattern
    int               index_count;      // Entries per index page
    int               index_spacing;    // Index every Nth message

    // Index decomposition
    int               cycle_shift;      // Bits to shift for cycle (always 32)
    uint64_t          seqnum_mask;      // Mask for seqnum (0x00000000FFFFFFFF)

    // User-provided serialization callbacks
    cparse_f          parser;           // Deserialize bytes → object
    cparsefree_f      parser_free;      // Free deserialized object
    csizeof_f         append_sizeof;    // Size of object in bytes
    cappend_f         append_write;     // Serialize object → bytes

    // Linked list of tailers
    tailer_t*         tailers;          // Doubly-linked list head

    // Appender (special tailer with PROT_READ|PROT_WRITE)
    tailer_t*         appender;

    // Global queue linked list
    struct queue*     next;
};
```

### `tailer_t` (struct tailer)

```
struct tailer {
    uint64_t          dispatch_after;   // Resume support: skip messages ≤ this index
    tailstate_t       state;            // Current state (enum)
    cdispatch_f       dispatcher;       // User callback for messages
    void*             dispatch_ctx;     // User context for callback
    collected_t*      collect;          // For synchronous collect operation

    int               mmap_protection;  // PROT_READ or PROT_READ|PROT_WRITE

    // Currently open queue file
    uint64_t          qf_cycle_open;    // Cycle of currently open file
    char*             qf_fn;            // Filename of currently open file
    struct stat       qf_statbuf;       // Cached stat of open file
    int               qf_fd;            // File descriptor

    uint64_t          qf_tip;           // Byte position of next header in file
    uint64_t          qf_index;         // Full 64-bit index (cycle << 32 | seqnum)

    // Currently mapped region
    unsigned char*    qf_buf;           // mmap base address
    uint64_t          qf_mmapoff;       // Offset from file start
    uint64_t          qf_mmapsz;        // Size of mapping

    struct queue*     queue;            // Parent queue pointer

    // Doubly-linked list
    struct tailer*    next;
    struct tailer*    prev;
};
```

### Tailer States (`tailstate_t`)

| Value | Name | Meaning |
|---|---|---|
| 0 | `TS_AWAITING_ENTRY` | At end of data, waiting for new entry |
| 1 | `TS_BUSY` | Hit a WORKING header, writer in progress |
| 2 | `TS_AWAITING_QUEUEFILE` | Expected queue file doesn't exist yet |
| 3 | `TS_E_STAT` | `fstat()` failed |
| 4 | `TS_E_MMAP` | `mmap()` failed (probably fatal) |
| 5 | `TS_PEEK` | Not yet polled |
| 6 | `TS_EXTEND_FAIL` | Queue file needs extending (appender only) |
| 7 | `TS_COLLECTED` | A value was collected (collect mode) |

## Appender Lifecycle

```
chronicle_append(queue, msg)
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
│ peek_queue_modcount()    │◄─── Refresh cycle info from shared memory
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ Create appender tailer   │  (first call only)
│ - PROT_READ|PROT_WRITE  │
│ - Start from highest     │
│   cycle - patch_cycles   │
│ - Reopen dirlist as RW   │
└────────────┬────────────┘
             │
             ▼
  ┌──────────────────────┐
  │  WRITE LOOP           │◄──────────────────────────┐
  │                        │                           │
  │  peek_queue_tailer()   │                           │
  └──────────┬─────────────┘                           │
             │                                         │
     ┌───────┼──────────┬────────────┐                 │
     ▼       ▼          ▼            ▼                 │
  AWAITING  AWAITING   EXTEND     OTHER               │
  _ENTRY    _QUEUEFILE _FAIL      (sleep+retry)───────┤
     │       │          │                              │
     │       │          ▼                              │
     │       │   lseek+write to                        │
     │       │   extend file ──────────────────────────┤
     │       │                                         │
     │       ▼                                         │
     │   Create new .cq4 file:                         │
     │   1. queuefile_init(tmp_name)                   │
     │   2. rename(tmp → final)                        │
     │   3. Update highest_cycle                       │
     │   4. poke_queue_modcount() ─────────────────────┘
     │
     ▼
  ┌──────────────────────────────┐
  │ CAS: lock_cmpxchgl            │
  │   (&header, UNALLOC, WORKING) │
  └──────────┬───────────────────┘
             │
      ┌──────┴──────┐
      ▼             ▼
   SUCCESS        FAILED
      │          (sleep+retry)
      ▼
  ┌──────────────────────────┐
  │ Check cycle / write EOF   │
  │ if roll needed            │
  └──────────┬───────────────┘
             │
             ▼
  ┌──────────────────────────┐
  │ mfence                    │
  │ Write payload at ptr+4    │
  │ mfence                    │
  │ Write size header at ptr  │
  └──────────┬───────────────┘
             │
             ▼
  ┌──────────────────────────┐
  │ Return appender->qf_index │
  └──────────────────────────┘
```

## Tailer Lifecycle

### Registration

```c
tailer = chronicle_tailer(queue, callback, context, start_index);
```

1. Decomposes `start_index` into cycle and seqnum
2. Clamps cycle to `[lowest_cycle, highest_cycle]`
3. Sets `dispatch_after = start_index - 1`
4. Sets `qf_index` to the start of the cycle's file (seqnum 0)
5. Links into the queue's doubly-linked tailer list

### Polling (`chronicle_peek_queue_tailer_r`)

This is the core tailer loop, structured as a generator/coroutine that suspends when blocked:

```
┌─────────────────────────────────────────┐
│ OUTER LOOP (while true)                  │
│                                          │
│ ┌───────────────────────────────────┐    │
│ │ Extract cycle from qf_index       │    │
│ │ cycle = qf_index >> cycle_shift   │    │
│ └──────────────┬────────────────────┘    │
│                │                         │
│      ┌─────────┴──────────┐              │
│      │ cycle != open?     │              │
│      │ or qf_fn == NULL?  │              │
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
│  │     → return AWAITING_QF │             │
│  └──────────┬──────────────┘             │
│             │                            │
│             ▼                            │
│  ┌─────────────────────────┐             │
│  │ Calculate mmap window    │             │
│  │ mmapoff = tip & mask     │             │
│  │ Refresh fstat if needed  │             │
│  │ mmap 2×blocksize chunk   │             │
│  └──────────┬──────────────┘             │
│             │                            │
│             ▼                            │
│  ┌─────────────────────────┐             │
│  │ parse_queue_block()      │             │
│  │  → iterate headers       │             │
│  │  → dispatch data msgs    │             │
│  └──────────┬──────────────┘             │
│             │                            │
│     ┌───────┼─────────┬──────┐           │
│     ▼       ▼         ▼      ▼           │
│  AWAITING  BUSY    EOF    EXTEND         │
│  _ENTRY             │    (double blk)    │
│     │               ▼                    │
│     │         Advance to                 │
│     │         next cycle ────────────────┤
│     │                                    │
│     ▼                                    │
│  (If cycle < highest - patch_cycles)     │
│  → Skip missing EOF, advance ────────────┤
│  (else)                                  │
│  → return AWAITING_ENTRY                 │
│                                          │
└──────────────────────────────────────────┘
```

### Collect Mode

`chronicle_collect()` provides a synchronous blocking interface:

1. Sets `tailer->collect` to point to a `collected_t` struct
2. Polls in a loop calling `chronicle_peek_tailer()`
3. When `parse_data_cb` finds a message, it fills the collect struct and returns `QB_COLLECTED`
4. The loop breaks when state becomes `TS_COLLECTED`
5. Returns the parsed message object

## Module Decomposition

### `libchronicle.h` / `libchronicle.c` — Core Queue Engine

| Responsibility | Key Functions |
|---|---|
| Initialization | `chronicle_init`, `chronicle_open`, `chronicle_cleanup` |
| Configuration | `chronicle_set_version`, `chronicle_set_roll_scheme`, `chronicle_set_create`, `chronicle_set_encoder`, `chronicle_set_decoder` |
| Writing | `chronicle_append`, `chronicle_append_ts` |
| Reading | `chronicle_tailer`, `chronicle_peek`, `chronicle_peek_queue`, `chronicle_collect`, `chronicle_return` |
| State Query | `chronicle_tailer_state`, `chronicle_tailer_index`, `chronicle_debug` |
| File Management | `queuefile_init`, `directory_listing_init`, `directory_listing_reopen` |
| Internal | `parse_queue_block`, `parse_data_cb`, `parse_dirlist`, `parse_queuefile_meta`, `lock_cmpxchgl`, `lock_xadd` |

### `wire.h` / `wire.c` — BinaryWire Serialization

| Responsibility | Key Functions |
|---|---|
| Parsing | `wire_parse` — stateful parser with callbacks |
| Writing | `wirepad_init`, `wirepad_field_*`, `wirepad_text`, `wirepad_event_name`, `wirepad_type_prefix` |
| QC framing | `wirepad_qc_start`, `wirepad_qc_finish` |
| Nesting | `wirepad_nest_enter`, `wirepad_nest_exit` |
| Integration | `wire_parse_textonly`, `wirepad_sizeof`, `wirepad_write` |

### `buffer.h` / `buffer.c` — Debug Utilities

| Function | Purpose |
|---|---|
| `printbuf` | Print buffer as C-string with octal escapes |
| `formatbuf` | Format buffer as hex+ASCII dump (16 bytes per line) |

## Data Alignment Padding

After each message, padding bytes are added:
```c
int pad4 = -sz & 0x03;
base = base + 4 + sz + pad4;
```
This ensures the next header's CAS target is 4-byte aligned.

## Concurrency and Memory Ordering

### Guarantees Relied Upon

1. **x86 Total Store Order (TSO):** Stores are visible in program order. However, loads can be reordered before stores.

2. **`mfence`:** Used as a full barrier to prevent:
   - Compiler reordering (`asm volatile` with `"memory"` clobber)
   - CPU speculative loads (prevents reading payload before header write is visible)

3. **`lock cmpxchgl`:** Atomic compare-and-swap with implicit full barrier. Used for write lock acquisition.

4. **`lock xaddl`:** Atomic fetch-and-add with implicit barrier. Used for modcount increment.

### Critical Sections

There are no traditional locks. The protocol is lock-free:
- Writers use CAS on the 4-byte header as a spinlock
- If CAS fails, the writer sleeps and retries
- Readers never block writers (readers only do `memcpy` of the header)
- The `mfence` between header read and payload read is essential for correctness

### Single-Threaded per Process

The library is **not thread-safe** within a single process. External locking is required if multiple threads share a queue handle. However, multiple **processes** can safely share the same queue directory.

## Error Handling

Errors are stored in a global `cerr_msg` string:

```c
int chronicle_err(const char* msg);        // Sets error, returns -1
void* chronicle_perr(const char* msg);     // Sets error, returns NULL
const char* chronicle_strerror();          // Retrieves last error message
```

Functions return:
- `0` for success, non-zero for failure (int-returning functions)
- `NULL` for failure (pointer-returning functions)

This is a simple, non-thread-safe error model.

## Diagrams

### Complete Message Write Sequence

```
  Process A (Writer)                   Shared Memory (mmap)              Process B (Reader)
  ──────────────────                   ────────────────────              ──────────────────
                                       
  1. sizeof(msg) → sz                  ┌────────┐
                                       │  0x00  │ UNALLOCATED
  2. CAS(0x00 → 0x80|pid) ──────────► │  0x80  │ WORKING                3. Read header
                                       │  xxxx  │                            → sees WORKING
  4. mfence                            └────────┘                            → returns BUSY
  5. Write payload ──────────────────► ┌────────┐
                                       │  data  │ payload bytes
  6. mfence                            └────────┘
  7. Write header(sz) ──────────────►  ┌────────┐
                                       │  sz    │ DATA                   8. Read header
                                       ├────────┤                            → sees DATA|sz
                                       │  data  │                        9. mfence
                                       └────────┘                       10. Read payload
                                                                        11. Dispatch callback
```

### Multi-Process Queue Interaction

```
  ┌────────────┐     ┌────────────┐     ┌────────────┐
  │ Appender 1 │     │ Appender 2 │     │  Tailer 1  │
  │ (Process A)│     │ (Process B)│     │ (Process C)│
  └─────┬──────┘     └─────┬──────┘     └─────┬──────┘
        │                   │                   │
        │   mmap(SHARED)    │   mmap(SHARED)    │   mmap(SHARED)
        │                   │                   │
        ▼                   ▼                   ▼
  ┌─────────────────────────────────────────────────────┐
  │              Kernel Page Cache                       │
  │   (same physical pages for same file regions)        │
  │                                                      │
  │  ┌──────────────┐  ┌──────────────┐                  │
  │  │metadata.cq4t │  │ 20211118.cq4 │                  │
  │  │ modcount: 5  │  │ [HDR][DATA]  │                  │
  │  │ highest: 42  │  │ [HDR][DATA]  │                  │
  │  │ lowest: 40   │  │ [UNALLOC...] │                  │
  │  └──────────────┘  └──────────────┘                  │
  └─────────────────────────────────────────────────────┘
        │                   │                   │
        ▼                   ▼                   ▼
  ┌─────────────────────────────────────────────────────┐
  │                   Disk (Filesystem)                   │
  │   Kernel writes dirty pages asynchronously            │
  └─────────────────────────────────────────────────────┘
```

### Queue File Lifecycle

```
Time ─────────────────────────────────────────────────────────►

Cycle N                          Cycle N+1
├────────────────────────────────┤────────────────────────────┤

  File: 20211118F.cq4              File: 20211119F.cq4
  ┌────────────────────────┐       ┌────────────────────────┐
  │ META: header           │       │ META: header           │
  │ DATA: msg 0            │       │ DATA: msg 0            │
  │ DATA: msg 1            │       │ DATA: msg 1            │
  │ DATA: msg 2            │       │ ...                    │
  │ ...                    │       │ UNALLOCATED...         │
  │ DATA: msg N            │       └────────────────────────┘
  │ EOF ◄──── written by   │
  │      appender on roll  │
  │ UNALLOCATED...         │
  └────────────────────────┘

  Index: 0x4A050000_00000000       Index: 0x4A060000_00000000
         to 0x4A050000_0000000N           ...
```

### Global Linked List Structure

```
  queue_head ──► queue_t ──► queue_t ──► NULL
                   │             │
                   │             └─ tailers ──► tailer_t ──► NULL
                   │
                   ├─ tailers ──► tailer_t ◄──► tailer_t ──► NULL
                   │              (doubly linked)
                   │
                   └─ appender ──► tailer_t (special: RW mmap)
```

All queues are linked in a singly-linked global list (`queue_head`). `chronicle_peek()` iterates all queues and all their tailers. Within a queue, tailers form a **doubly-linked list** (for O(1) removal). The appender is a separate tailer with write permissions.

---

*This document was generated from analysis of the libchronicle source code. For protocol changes between versions, see `CHANGES.md`.*