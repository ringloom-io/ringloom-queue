# Task 6: Tailer (Reader) Implementation

This document covers the Zig reimplementation of the Chronicle Queue tailer
(reader) — the `chronicle_peek_queue_tailer_r` state machine, the inner
`parse_queue_block` loop, tailer lifecycle management, and the synchronous
`collect` API. The tailer is the read path of Chronicle Queue and the
counterpart to the appender documented in `07-zig-task-appender.md`.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Tailer Struct Design](#2-tailer-struct-design)
3. [Creating a Tailer (`chronicle_tailer`)](#3-creating-a-tailer)
4. [Closing a Tailer (`chronicle_tailer_close`)](#4-closing-a-tailer)
5. [Polling Hierarchy](#5-polling-hierarchy)
6. [The Main State Machine (`chronicle_peek_queue_tailer_r`)](#6-the-main-state-machine)
7. [The Inner Parsing Loop (`parse_queue_block`)](#7-the-inner-parsing-loop)
8. [The Data Callback (`parse_data_cb`)](#8-the-data-callback)
9. [Synchronous Blocking Read (`chronicle_collect`)](#9-synchronous-blocking-read)
10. [Modcount Polling (`peek_queue_modcount`)](#10-modcount-polling)
11. [Filename Generation (`chronicle_get_cycle_fn`)](#11-filename-generation)
12. [Reader vs Appender Tailers](#12-reader-vs-appender-tailers)
13. [Complete Zig State Machine Code](#13-complete-zig-state-machine-code)
14. [Complete Zig `parse_queue_block` Code](#14-complete-zig-parse_queue_block-code)
15. [Testing Notes](#15-testing-notes)

---

## 1. Overview

Readers in Chronicle Queue are called **tailers**. A tailer tracks its
position in the queue as a 64-bit index (upper 32 bits = cycle number, lower
32 bits = sequence number within that cycle's file), and advances through the
queue by being repeatedly polled.

The reading architecture is a three-level hierarchy:

```
chronicle_peek()                    ← poll ALL queues
  └─ chronicle_peek_queue(queue)    ← poll all tailers on ONE queue
       └─ chronicle_peek_queue_tailer_r(queue, tailer)  ← advance ONE tailer
            └─ parse_queue_block(...)                    ← parse entries in mmap window
                 └─ parse_data_cb(...)                   ← dispatch one data entry
```

Each call to the state machine may:
- Open/close queue files as the tailer crosses cycle boundaries
- Re-mmap the file window as the read position advances
- Parse multiple metadata and data entries in a single pass
- Dispatch data entries to the user's callback
- Handle EOF markers, missing files, and busy (locked) entries

### C function reference

| C function | Lines | Purpose |
|---|---|---|
| `chronicle_tailer` | 1233-1267 | Create a tailer, clamp index, link into list |
| `chronicle_tailer_close` | 1304-1324 | Unmap, close fd, unlink from list, free |
| `chronicle_tailer_state` | 1296-1298 | Return current state enum |
| `chronicle_tailer_index` | 1300-1302 | Return current 64-bit index |
| `chronicle_peek` | 780-786 | Poll all queues (global list) |
| `chronicle_peek_queue` | 812-822 | Poll modcount then all tailers |
| `chronicle_peek_queue_tailer_r` | 824-965 | Main state machine |
| `chronicle_peek_tailer` | 967-969 | Thin wrapper |
| `parse_queue_block` | 605-651 | Inner parsing loop |
| `parse_data_cb` | 661-688 | Data entry dispatch callback |
| `chronicle_collect` | 1269-1288 | Synchronous blocking read |
| `chronicle_return` | 1290-1294 | Free collected message |
| `chronicle_get_cycle_fn` | 233-250 | Cycle number → filename |

---

## 2. Tailer Struct Design

The C `struct tailer` (lines 110-138 of `libchronicle.c`) tracks the read
position, the currently open queue file, the mmap window, and the dispatch
callback. It is linked into the parent queue's doubly-linked tailer list.

### C struct

```c
struct tailer {
    uint64_t          dispatch_after;   // resume index — skip entries ≤ this
    tailstate_t       state;
    cdispatch_f       dispatcher;       // user callback
    void*             dispatch_ctx;     // user context for callback
    collected_t*      collect;          // non-null during collect operation

    int               mmap_protection;  // PROT_READ for readers, PROT_READ|PROT_WRITE for appender

    // currently open queue file
    uint64_t          qf_cycle_open;    // cycle number of the open file
    char*             qf_fn;            // filename of the open file
    struct stat       qf_statbuf;       // last fstat result
    int               qf_fd;            // file descriptor

    uint64_t          qf_tip;           // byte offset of the next header in the file
    uint64_t          qf_index;         // 64-bit index: (cycle << 32) | seqnum

    // mmap window
    unsigned char*    qf_buf;           // mmap base address
    uint64_t          qf_mmapoff;       // file offset where mmap starts
    uint64_t          qf_mmapsz;        // size of the mmap window

    struct queue*     queue;            // parent queue
    struct tailer*    next;             // doubly-linked list
    struct tailer*    prev;
};
```

### Zig struct

```zig
const std = @import("std");
const posix = std.posix;

pub const TailState = enum(u8) {
    awaiting_entry = 0,
    busy = 1,
    awaiting_queuefile = 2,
    e_stat = 3,
    e_mmap = 4,
    peek = 5,
    extend_fail = 6,
    collected = 7,
};

pub const Collected = struct {
    msg: ?[]const u8 = null,
    sz: usize = 0,
    index: u64 = 0,
};

pub const DispatchFn = *const fn (ctx: ?*anyopaque, index: u64, msg: []const u8) void;

pub const Tailer = struct {
    const Self = @This();

    // ── Resume / dispatch ─────────────────────────────────────
    dispatch_after: u64 = 0,
    state: TailState = .peek,
    dispatcher: ?DispatchFn = null,
    dispatch_ctx: ?*anyopaque = null,
    collect: ?*Collected = null,

    // ── Protection mode ───────────────────────────────────────
    /// PROT_READ for reader tailers, PROT_READ|PROT_WRITE for the appender.
    mmap_protection: u32 = posix.PROT.READ,

    // ── Currently open queue file ─────────────────────────────
    qf_cycle_open: u64 = 0,
    qf_fn: ?[]const u8 = null,
    qf_fd: ?posix.fd_t = null,
    qf_stat_size: u64 = 0,

    // ── File position ─────────────────────────────────────────
    /// Byte offset in the file of the next header to read.
    qf_tip: u64 = 0,
    /// 64-bit index: upper 32 bits = cycle, lower 32 bits = seqnum.
    qf_index: u64 = 0,

    // ── Mmap window ───────────────────────────────────────────
    qf_buf: ?[*]align(std.mem.page_size) u8 = null,
    qf_mmapoff: u64 = 0,
    qf_mmapsz: u64 = 0,

    // ── Parent ────────────────────────────────────────────────
    queue: *Queue = undefined,

    // ── Intrusive doubly-linked list node ─────────────────────
    /// Used by std.DoublyLinkedList or manual prev/next.
    prev: ?*Tailer = null,
    next: ?*Tailer = null,

    // ── Allocator for self-owned memory ───────────────────────
    allocator: std.mem.Allocator = undefined,
};
```

Key design decisions:

- **`qf_fd` is `?posix.fd_t`** — nullable, replacing the C convention of
  checking `> 0`. Zig's optional type makes the "no file open" state
  unambiguous.
- **`qf_buf` is `?[*]align(std.mem.page_size) u8`** — a nullable
  page-aligned pointer for the mmap base. The alignment annotation ensures
  correct mmap semantics.
- **`mmap_protection`** is stored as a `u32` to match the `std.posix.mmap`
  API directly. Reader tailers get `PROT.READ`; the appender gets
  `PROT.READ | PROT.WRITE`.
- **Intrusive linked list** — we use manual `prev`/`next` pointers rather
  than `std.DoublyLinkedList` to keep the struct flat and avoid an extra
  level of indirection. The queue owns the list head.

---

## 3. Creating a Tailer

### C implementation (`chronicle_tailer`, lines 1233-1267)

```c
tailer_t* chronicle_tailer(queue_t *queue, cdispatch_f dispatcher,
                           void* dispatch_ctx, uint64_t index) {
    if (queue == NULL) return chronicle_perr("queue is not valid");

    int cycle = index >> queue->cycle_shift;
    int seqnum = index & queue->seqnum_mask;

    // Clamp to valid range
    if (cycle < queue->lowest_cycle)
        index = queue->lowest_cycle << queue->cycle_shift;
    if (cycle > queue->highest_cycle)
        index = queue->highest_cycle << queue->cycle_shift;

    tailer_t* tailer = malloc(sizeof(tailer_t));
    bzero(tailer, sizeof(tailer_t));

    tailer->dispatch_after = index - 1;
    tailer->qf_index = index & ~queue->seqnum_mask; // start of cycle
    tailer->dispatcher = dispatcher;
    tailer->dispatch_ctx = dispatch_ctx;
    tailer->state = 5; // TS_PEEK
    tailer->mmap_protection = PROT_READ;

    // Prepend to doubly-linked list
    tailer->next = queue->tailers;
    tailer->prev = NULL;
    if (queue->tailers) queue->tailers->prev = tailer;
    queue->tailers = tailer;

    tailer->queue = queue;
    return tailer;
}
```

Key details:

1. **Index clamping** — the requested index is clamped so the cycle falls
   within `[lowest_cycle, highest_cycle]`. This prevents the tailer from
   trying to open files that definitely don't exist.
2. **`dispatch_after = index - 1`** — entries with index ≤ `dispatch_after`
   are silently skipped. This enables resume: if you want to start reading
   from index N, entries 0 through N-1 are skipped.
3. **`qf_index = index & ~seqnum_mask`** — the tailer always starts replay
   from the **first entry in the cycle file** (seqnum = 0), even if the
   requested index is mid-file. This is because the file must be scanned
   sequentially from the beginning to find the correct byte offset for any
   given seqnum. The `dispatch_after` mechanism ensures only entries after
   the requested index are actually dispatched.
4. **`mmap_protection = PROT_READ`** — readers cannot write to the mmap.

### Zig implementation

```zig
pub fn createTailer(
    self: *Queue,
    dispatcher: ?DispatchFn,
    dispatch_ctx: ?*anyopaque,
    requested_index: u64,
) !*Tailer {
    var index = requested_index;

    // Decompose and clamp
    const cycle = index >> self.cycle_shift;
    if (cycle < self.lowest_cycle) {
        index = self.lowest_cycle << self.cycle_shift;
    } else if (cycle > self.highest_cycle) {
        index = self.highest_cycle << self.cycle_shift;
    }

    const tailer = try self.allocator.create(Tailer);
    errdefer self.allocator.destroy(tailer);

    tailer.* = Tailer{
        .dispatch_after = index -% 1, // wrapping subtract for index == 0
        .qf_index = index & ~self.seqnum_mask, // start of cycle file
        .dispatcher = dispatcher,
        .dispatch_ctx = dispatch_ctx,
        .state = .peek,
        .mmap_protection = posix.PROT.READ,
        .queue = self,
        .allocator = self.allocator,
    };

    // Link into queue's tailer list (prepend)
    tailer.next = self.tailer_head;
    tailer.prev = null;
    if (self.tailer_head) |head| {
        head.prev = tailer;
    }
    self.tailer_head = tailer;

    log.info("tailer added index={} (cycle={}, seqnum={}) cb={*}", .{
        index,
        index >> self.cycle_shift,
        index & self.seqnum_mask,
        dispatcher,
    });

    return tailer;
}
```

---

## 4. Closing a Tailer

### C implementation (`chronicle_tailer_close`, lines 1304-1324)

```c
void chronicle_tailer_close(tailer_t* tailer) {
    if (tailer->qf_fn)  free(tailer->qf_fn);
    if (tailer->qf_buf) munmap(tailer->qf_buf, tailer->qf_mmapsz);
    if (tailer->qf_fd)  close(tailer->qf_fd);

    // Unlink from doubly-linked list
    if (tailer->next) tailer->next->prev = tailer->prev;
    if (tailer->prev) tailer->prev->next = tailer->next;
    else if (tailer->queue->tailers == tailer)
        tailer->queue->tailers = tailer->next;

    free(tailer);
}
```

Three resources to release:
1. The cached filename string (`qf_fn`)
2. The mmap'd buffer (`qf_buf`, `qf_mmapsz`)
3. The file descriptor (`qf_fd`)

Plus unlinking from the parent queue's doubly-linked list — with special
handling for the head pointer if we're the first element.

### Zig implementation

```zig
pub fn close(self: *Tailer) void {
    // Release mmap
    if (self.qf_buf) |buf| {
        const slice = @as([*]align(std.mem.page_size) u8, @ptrCast(buf))[0..self.qf_mmapsz];
        posix.munmap(slice);
        self.qf_buf = null;
    }

    // Close file descriptor
    if (self.qf_fd) |fd| {
        posix.close(fd);
        self.qf_fd = null;
    }

    // Free cached filename
    if (self.qf_fn) |fn_str| {
        self.allocator.free(fn_str);
        self.qf_fn = null;
    }

    // Unlink from doubly-linked list
    if (self.next) |n| {
        n.prev = self.prev;
    }
    if (self.prev) |p| {
        p.next = self.next;
    } else if (self.queue.tailer_head == self) {
        self.queue.tailer_head = self.next;
    }

    // Free the tailer struct itself
    self.allocator.destroy(self);
}
```

**Zig advantage:** The optional types (`?fd_t`, `?[*]u8`, `?[]const u8`)
make the null checks natural with `if (x) |val|` syntax, and there's no
risk of double-free since we set each field to `null` after releasing.

---

## 5. Polling Hierarchy

### `chronicle_peek` (C line 780)

Iterates the global queue list and polls each queue:

```c
void chronicle_peek() {
    queue_t *queue = queue_head;
    while (queue != NULL) {
        chronicle_peek_queue(queue);
        queue = queue->next;
    }
}
```

In the Zig port, this is replaced by `QueueRegistry.peekAll()` (see
`06-zig-task-queue-lifecycle.md` §11) or by calling `queue.peekQueue()`
directly.

### `chronicle_peek_queue` (C line 812)

Polls the shared modcount, then iterates all tailers:

```c
void chronicle_peek_queue(queue_t *queue) {
    peek_queue_modcount(queue);
    tailer_t *tailer = queue->tailers;
    while (tailer != NULL) {
        chronicle_peek_queue_tailer(queue, tailer);
        tailer = tailer->next;
    }
}
```

### `chronicle_peek_tailer` / `chronicle_peek_queue_tailer` (C lines 967-973)

Thin wrappers that call `chronicle_peek_queue_tailer_r` and store the result
in `tailer->state`:

```c
int chronicle_peek_queue_tailer(queue_t *queue, tailer_t *tailer) {
    return tailer->state = chronicle_peek_queue_tailer_r(queue, tailer);
}

int chronicle_peek_tailer(tailer_t *tailer) {
    return chronicle_peek_queue_tailer(tailer->queue, tailer);
}
```

### Zig equivalents

```zig
pub fn peekQueue(self: *Queue) void {
    self.peekQueueModcount();

    var tailer = self.tailer_head;
    while (tailer) |t| {
        _ = self.peekQueueTailer(t);
        tailer = t.next;
    }
}

pub fn peekQueueTailer(self: *Queue, tailer: *Tailer) TailState {
    const state = self.peekQueueTailerR(tailer);
    tailer.state = state;
    return state;
}

pub fn peekTailer(tailer: *Tailer) TailState {
    return tailer.queue.peekQueueTailer(tailer);
}
```

---

## 6. The Main State Machine

`chronicle_peek_queue_tailer_r` (C lines 824-965) is the heart of the
reader. It runs in a `while(1)` loop, each iteration handling one level of
the nested `for each cycle { for each block { for each entry } }` iteration.

### State machine diagram

```
                    ┌──────────────────────────────────────┐
                    │           while (true)                │
                    │                                       │
                    │  1. Extract cycle from qf_index       │
                    │  2. If cycle changed → open new file  │
                    │  3. Calculate mmap window              │
                    │  4. Re-mmap if window changed          │
                    │  5. Call parse_queue_block              │
                    │  6. Interpret result                    │
                    │                                       │
                    └──────────┬────────────────────────────┘
                               │
            ┌──────────────────┼──────────────────────┐
            │                  │                      │
    QB_AWAITING_ENTRY    QB_REACHED_EOF         QB_NEED_EXTEND
            │                  │                      │
     ┌──────┴───────┐    advance to              double blocksize
     │              │    next cycle               (if no progress)
  cycle < high-3?  return                        continue
     │     TS_AWAITING                           │
   skip    _ENTRY              QB_BUSY ──→ return TS_BUSY
   forward                     QB_COLLECTED ──→ return TS_COLLECTED
   continue
```

### Step-by-step walkthrough

#### Step 1: Extract cycle from index

```c
uint64_t cycle = tailer->qf_index >> queue->cycle_shift;
```

The upper 32 bits of `qf_index` encode the cycle number, which determines
which `.cq4` file we should be reading.

#### Step 2: Open/close queue file if cycle changed

```c
if (cycle != tailer->qf_cycle_open || tailer->qf_fn == NULL) {
    // Free old resources
    if (tailer->qf_fn)  free(tailer->qf_fn);
    if (tailer->qf_buf) { munmap(tailer->qf_buf, tailer->qf_mmapsz); tailer->qf_buf = NULL; }
    if (tailer->qf_fd > 0) close(tailer->qf_fd);

    tailer->qf_fn = chronicle_get_cycle_fn(queue, cycle);
    tailer->qf_tip = 0;

    int fopen_flags = O_RDONLY;
    if (tailer->mmap_protection != PROT_READ) fopen_flags = O_RDWR;

    if ((tailer->qf_fd = open(tailer->qf_fn, fopen_flags)) < 0) {
        // File doesn't exist
        if (cycle < queue->highest_cycle) {
            // Skip forward — file missing but newer cycles exist
            tailer->qf_index = (cycle + 1) << queue->cycle_shift;
            continue;
        }
        return TS_AWAITING_QUEUEFILE;
    }
    tailer->qf_cycle_open = cycle;

    if (fstat(tailer->qf_fd, &tailer->qf_statbuf) < 0) return TS_E_STAT;
}
```

Key details:
- **Free old resources first** — munmap, close fd, free filename string.
- **`qf_tip = 0`** — reset file position to the start of the new file.
- **Open flags** — `O_RDONLY` for readers, `O_RDWR` for the appender.
- **Missing file handling:**
  - If `cycle < highest_cycle`, skip to the next cycle. This handles gaps
    where a cycle file was never created (the writer rolled over it).
  - Otherwise, return `TS_AWAITING_QUEUEFILE` so the appender knows to
    create the file (or the reader waits).

#### Step 3: Calculate mmap window

```c
uint64_t blocksize_mask = ~(queue->blocksize - 1);
uint64_t mmapoff = tailer->qf_tip & blocksize_mask;

// Renew stat if approaching end of mapped region
if (tailer->qf_statbuf.st_size - mmapoff < 2 * queue->blocksize) {
    if (fstat(tailer->qf_fd, &tailer->qf_statbuf) < 0)
        return TS_E_STAT;
    // Signal extend needed for write tailers
    if (tailer->qf_statbuf.st_size - mmapoff < 2 * queue->blocksize
        && tailer->mmap_protection != PROT_READ) {
        return TS_EXTEND_FAIL;
    }
}

int limit = tailer->qf_statbuf.st_size - mmapoff > 2 * queue->blocksize
    ? 2 * queue->blocksize
    : tailer->qf_statbuf.st_size - mmapoff;
```

The mmap window calculation:
1. **`mmapoff`** — align `qf_tip` down to the nearest `blocksize` boundary.
   This is done by masking with `~(blocksize - 1)`. For the default 1 MiB
   blocksize, this aligns to 1 MiB boundaries.
2. **Stat refresh** — if fewer than 2 blocks remain between the mmap offset
   and the file size, refresh the stat to check if the file has grown. For
   write tailers, return `TS_EXTEND_FAIL` to signal that the file needs
   extending (handled by the appender — see `07-zig-task-appender.md` §9).
3. **`limit`** — the mmap window size is `min(2 * blocksize, file_size - mmapoff)`.
   Mapping 2× blocksize ensures we always have at least one full block
   available for parsing.

#### Step 4: Re-mmap if window changed

```c
if (tailer->qf_buf == NULL || mmapoff != tailer->qf_mmapoff || limit != tailer->qf_mmapsz) {
    if (tailer->qf_buf) {
        munmap(tailer->qf_buf, tailer->qf_mmapsz);
        tailer->qf_buf = NULL;
    }
    tailer->qf_mmapsz = limit;
    tailer->qf_mmapoff = mmapoff;
    tailer->qf_buf = mmap(0, tailer->qf_mmapsz, tailer->mmap_protection,
                          MAP_SHARED, tailer->qf_fd, tailer->qf_mmapoff);
    if (tailer->qf_buf == MAP_FAILED) {
        tailer->qf_buf = NULL;
        return TS_E_MMAP;
    }
}
```

The mmap is only refreshed when the window parameters actually change. This
avoids unnecessary munmap/mmap syscalls when the tailer is reading within the
same window.

#### Step 5: Call `parse_queue_block`

```c
unsigned char* basep = (tailer->qf_tip - tailer->qf_mmapoff) + tailer->qf_buf;
unsigned char* basep_old = basep;
unsigned char* extent = tailer->qf_buf + tailer->qf_mmapsz;
uint64_t index = tailer->qf_index;

parseqb_state_t s = parse_queue_block(queue, &basep, &index, extent,
                                       &hcbs, parse_data_cb, tailer);
```

`basep` is the current read position within the mmap window, calculated as:
`(file_tip - mmap_offset) + mmap_base`. `extent` is the end of the mmap
window. Both `basep` and `index` are passed by pointer and updated by
`parse_queue_block` as entries are consumed.

#### Step 6: Interpret results

```c
// Double blocksize if stuck
if (s == QB_NEED_EXTEND && basep == basep_old) {
    queue_double_blocksize(queue);
}

// Commit progress
if (basep != basep_old) {
    tailer->qf_tip = basep - tailer->qf_buf + tailer->qf_mmapoff;
    tailer->qf_index = index;
}

if (s == QB_BUSY) return TS_BUSY;
if (s == QB_COLLECTED) return TS_COLLECTED;

if (s == QB_AWAITING_ENTRY) {
    if (cycle < queue->highest_cycle - patch_cycles) {
        // Skip missing EOF — advance to next cycle
        tailer->qf_index = (cycle + 1) << queue->cycle_shift;
        continue;
    }
    return TS_AWAITING_ENTRY;
}

if (s == QB_REACHED_EOF) {
    uint64_t eof_cycle = ((tailer->qf_index >> queue->cycle_shift) + 1)
                          << queue->cycle_shift;
    tailer->qf_index = eof_cycle;
    // continue — will open next cycle's file on next iteration
}
```

Result handling:

| `parse_queue_block` result | Action |
|---|---|
| `QB_NEED_EXTEND` with no progress | Double blocksize (an entry header spans the mmap boundary). Loop will re-mmap with the larger window. |
| `QB_NEED_EXTEND` with progress | Normal — committed the progress, will re-mmap on next iteration. |
| `QB_BUSY` | Return `TS_BUSY` — another writer is holding the lock on this header. Caller should back off. |
| `QB_COLLECTED` | Return `TS_COLLECTED` — a value was captured for the `collect` API. |
| `QB_AWAITING_ENTRY` and `cycle < highest_cycle - patch_cycles` | **Skip missing EOF.** The file has no more data and no EOF marker, but we know newer cycles exist. Skip to the next cycle. This tolerates Java writers that don't write EOF on roll. |
| `QB_AWAITING_ENTRY` otherwise | Return `TS_AWAITING_ENTRY` — we're at the tail of the queue, waiting for new data. |
| `QB_REACHED_EOF` | Advance `qf_index` to the start of the next cycle (seqnum=0). The `continue` restarts the loop, which will close this file and open the next. |

---

## 7. The Inner Parsing Loop

`parse_queue_block` (C lines 605-651) is the inner loop that reads entries
from a contiguous mmap'd region. It processes metadata entries silently
(passing them to the wire parser) and data entries through a user-supplied
callback.

### C implementation

```c
parseqb_state_t parse_queue_block(queue_t *queue,
        unsigned char** basep, uint64_t *indexp,
        unsigned char* extent,
        wirecallbacks_t* hcbs,
        datacallback_f parse_data,
        void* userdata)
{
    uint32_t header;
    int sz;
    unsigned char* base = *basep;
    uint64_t index = *indexp;
    parseqb_state_t pd = QB_AWAITING_ENTRY;

    while (pd == QB_AWAITING_ENTRY) {
        // Check we can read 4-byte header
        if (base + 4 >= extent) return QB_NEED_EXTEND;

        // Read header (relaxed — memcpy is fine on x86)
        memcpy(&header, base, sizeof(header));

        // Memory fence — no speculative reads of payload before header is known
        asm volatile ("mfence" ::: "memory");

        if (header == HD_UNALLOCATED) {
            return QB_AWAITING_ENTRY;
        } else if ((header & HD_MASK_META) == HD_WORKING) {
            return QB_BUSY;
        } else if ((header & HD_MASK_META) == HD_METADATA) {
            sz = (header & HD_MASK_LENGTH);
            if (base + 4 + sz >= extent) return QB_NEED_EXTEND;
            wire_parse(base + 4, sz, hcbs);
            // metadata does NOT increment the index
        } else if ((header & HD_MASK_META) == HD_EOF) {
            return QB_REACHED_EOF;
        } else {
            // data entry
            sz = (header & HD_MASK_LENGTH);
            if (parse_data) {
                if (base + 4 + sz >= extent) return QB_NEED_EXTEND;
                pd = parse_data(base + 4, sz, index, userdata);
            } else {
                return QB_NULL_ITEM;
            }
            index++;
            *indexp = index;
        }
        // v5 padding: align data entries to 4 bytes
        int pad4 = (queue->version < 5) ? 0 : -sz & 0x03;
        base = base + 4 + sz + pad4;
        *basep = base;
    }
    return pd;
}
```

### Header format recap

Each entry in a queue file starts with a 4-byte little-endian header:

| Header value | Meaning |
|---|---|
| `0x00000000` | **Unallocated** — no entry here yet |
| `0x80000000 \| pid` | **Working** — write in progress (lock held) |
| `0x40000000 \| size` | **Metadata** — wire protocol metadata, `size` bytes follow |
| `0xC0000000` | **EOF** — end of this cycle file |
| `size` (bits 31-30 = `00`) | **Data** — user data, `size` bytes follow |

The top two bits (`HD_MASK_META = 0xC0000000`) determine the type. The
lower 30 bits (`HD_MASK_LENGTH = 0x3FFFFFFF`) hold the payload size.

### V5 padding

In v5, after each entry (header + payload), the position is aligned up to a
4-byte boundary:

```c
int pad4 = (queue->version < 5) ? 0 : -sz & 0x03;
base = base + 4 + sz + pad4;
```

The expression `-sz & 0x03` computes the number of padding bytes needed.
For example:
- `sz = 5` → `-5 & 3 = 3` → 3 bytes padding (total 4+5+3 = 12, divisible by 4)
- `sz = 8` → `-8 & 3 = 0` → no padding needed
- `sz = 6` → `-6 & 3 = 2` → 2 bytes padding

This alignment is critical for interoperability with Java Chronicle Queue v5.

### Loop semantics

The inner loop continues processing entries as long as the data callback
returns `QB_AWAITING_ENTRY`. It breaks (returns) on:
- End of mmap window (`base + 4 >= extent`) → `QB_NEED_EXTEND`
- Unallocated header → `QB_AWAITING_ENTRY`
- Working header → `QB_BUSY`
- EOF header → `QB_REACHED_EOF`
- Data callback returns non-`QB_AWAITING_ENTRY` (e.g., `QB_COLLECTED`)

**Important:** metadata entries do NOT increment the index. Only data entries
do. This means the index counter tracks data entries only, matching the
Chronicle Queue protocol where the 64-bit index addresses data messages.

---

## 8. The Data Callback

### `parse_data_cb` (C lines 661-688)

This is the callback passed to `parse_queue_block` for each data entry:

```c
parseqb_state_t parse_data_cb(unsigned char* base, int lim,
                               uint64_t index, void* userdata) {
    tailer_t* tailer = (tailer_t*)userdata;

    if (index > tailer->dispatch_after) {
        COBJ msg = tailer->queue->parser(base, lim);
        if (msg == NULL) {
            return QB_AWAITING_ENTRY; // skip null parse results
        }

        // Collect mode — return the value inline instead of dispatching
        if (tailer->collect) {
            tailer->collect->msg = msg;
            tailer->collect->index = index;
            tailer->collect->sz = lim;
            return QB_COLLECTED;
        }

        // Normal mode — dispatch to user callback
        if (tailer->dispatcher) {
            tailer->dispatcher(tailer->dispatch_ctx, index, msg);
        }

        // Free the parsed message
        if (tailer->queue->parser_free) {
            tailer->queue->parser_free(msg);
        }
    }
    return QB_AWAITING_ENTRY;
}
```

Two modes of operation:

1. **Dispatch mode** (normal): the parser decodes the raw bytes into a
   message object, the dispatcher callback delivers it to the application,
   and the parser_free callback frees it. The callback is fire-and-forget —
   the message is invalid after the callback returns.

2. **Collect mode** (synchronous): when `tailer->collect` is non-null, the
   parsed message is stored in the `collected_t` struct and `QB_COLLECTED` is
   returned, which bubbles all the way up to the caller of `chronicle_collect`.
   The message is NOT freed — the caller must call `chronicle_return` to free
   it.

The `dispatch_after` check enables resume: entries with index ≤
`dispatch_after` are silently skipped (the callback is never called). The
file is still scanned sequentially, but no work is done for old entries.

### Zig implementation

```zig
const ParseQBState = enum {
    awaiting_entry,
    busy,
    reached_eof,
    need_extend,
    null_item,
    collected,
};

fn parseDataCallback(
    base: []const u8,
    index: u64,
    tailer: *Tailer,
) ParseQBState {
    // Skip entries at or before the resume point
    if (index <= tailer.dispatch_after) {
        return .awaiting_entry;
    }

    // Parse the raw bytes into a message
    const parser = tailer.queue.parser orelse return .awaiting_entry;
    const msg = parser(base) orelse {
        log.debug("parse returned null at index {}, skipping", .{index});
        return .awaiting_entry;
    };

    // Collect mode: capture the value for chronicle_collect
    if (tailer.collect) |collect| {
        collect.msg = msg;
        collect.index = index;
        collect.sz = base.len;
        return .collected;
    }

    // Dispatch mode: call user callback and free
    if (tailer.dispatcher) |dispatch| {
        dispatch(tailer.dispatch_ctx, index, msg);
    }
    if (tailer.queue.parser_free) |free_fn| {
        free_fn(msg);
    }

    return .awaiting_entry;
}
```

---

## 9. Synchronous Blocking Read

### `chronicle_collect` (C lines 1269-1288)

This provides a blocking synchronous API on top of the polling model:

```c
COBJ chronicle_collect(tailer_t *tailer, collected_t *collected) {
    if (tailer == NULL) return chronicle_perr("null tailer");
    if (collected == NULL) return chronicle_perr("null collected");
    tailer->collect = collected;

    uint64_t delaycount = 0;
    while (1) {
        int r = chronicle_peek_tailer(tailer);
        if (r == TS_COLLECTED) break;
        if (delaycount++ > 20) {
            usleep(delaycount);
            peek_queue_modcount(tailer->queue);
        }
    }
    tailer->collect = NULL;
    return collected->msg;
}
```

The function:
1. Sets `tailer->collect` to point to the caller's `collected_t` struct.
   This signals `parse_data_cb` to use collect mode.
2. Spins calling `peek_tailer` until `TS_COLLECTED` is returned.
3. After 20 empty iterations, starts sleeping with increasing delay and
   refreshes the modcount (which may reveal new cycle files).
4. Clears `tailer->collect` and returns the message pointer.

### `chronicle_return` (C lines 1290-1294)

```c
void chronicle_return(tailer_t *tailer, collected_t *collected) {
    if (tailer->queue->parser && tailer->queue->parser_free) {
        tailer->queue->parser_free(collected->msg);
    }
}
```

Frees the message captured by `collect`. This is necessary because collect
mode skips the automatic free in `parse_data_cb`.

### Zig implementation

```zig
pub fn collect(tailer: *Tailer, collected: *Collected) ![]const u8 {
    tailer.collect = collected;
    defer tailer.collect = null;

    var delay_count: u64 = 0;
    while (true) {
        const r = peekTailer(tailer);
        if (r == .collected) {
            return collected.msg orelse return error.CollectReturnedNull;
        }
        delay_count += 1;
        if (delay_count > 20) {
            // Progressive backoff with modcount refresh
            std.time.sleep(delay_count * std.time.ns_per_us);
            tailer.queue.peekQueueModcount();
        }
    }
}

pub fn returnCollected(tailer: *Tailer, collected: *Collected) void {
    if (collected.msg) |msg| {
        if (tailer.queue.parser_free) |free_fn| {
            free_fn(msg);
        }
        collected.msg = null;
    }
}
```

**Usage pattern:**

```zig
var collected: Collected = .{};
const msg = try Tailer.collect(tailer, &collected);
defer Tailer.returnCollected(tailer, &collected);

// Use msg...
processMessage(msg);
```

The `defer` ensures the message is always freed, even if processing panics
or returns an error.

---

## 10. Modcount Polling

### `peek_queue_modcount` (C lines 788-800)

This is the lightweight polling mechanism that detects changes made by other
processes:

```c
void peek_queue_modcount(queue_t* queue) {
    uint64_t modcount;
    memcpy(&modcount, queue->dirlist_fields.modcount, sizeof(modcount));

    if (queue->modcount != modcount) {
        memcpy(&queue->modcount, queue->dirlist_fields.modcount, sizeof(modcount));
        memcpy(&queue->lowest_cycle, queue->dirlist_fields.lowest_cycle, sizeof(modcount));
        memcpy(&queue->highest_cycle, queue->dirlist_fields.highest_cycle, sizeof(modcount));
    }
}
```

Fast path: read 8 bytes from the mmap'd `modcount` field. If unchanged,
return immediately (no syscalls). If changed, refresh all three shared fields.

The `memcpy` calls act as non-tearing reads — on x86, an 8-byte aligned read
is naturally atomic, and `memcpy` prevents the compiler from splitting it or
caching it. In Zig, we use `volatile` reads (see `07-zig-task-appender.md`
§5).

The modcount is the key to the Chronicle Queue IPC protocol:
- Writers increment modcount (with `lock; xaddl`) after updating cycle fields
- Readers poll modcount — if changed, they know to re-check cycle boundaries
  and re-glob queue files

---

## 11. Filename Generation

### `chronicle_get_cycle_fn` (C lines 233-250)

Converts a cycle number to a filename by:
1. Computing the epoch time: `cycle * (roll_length / 1000)` → seconds since epoch
2. Formatting with strftime using the pre-built format string
3. Joining with the directory name and `.cq4` suffix

```c
char* chronicle_get_cycle_fn(queue_t* queue, int cycle) {
    time_t rawtime = (time_t) cycle * (queue->roll_length / 1000);
    struct tm info;
    gmtime_r(&rawtime, &info);

    char* strftime_buf = strdup(queue->roll_format);
    strftime(strftime_buf, strlen(strftime_buf)+1, queue->roll_strftime, &info);

    char* fnbuf;
    asprintf(&fnbuf, "%s/%s.cq4", queue->dirname, strftime_buf);
    free(strftime_buf);
    return fnbuf;
}
```

### Zig implementation

Zig doesn't have `strftime` in the standard library, so we need either a C
interop call or a custom formatter. Since we've already converted the Java
date format to a known set of components (`%Y`, `%m`, `%d`, `%H`, `%M`,
`%S`, plus literals), we can build a pure-Zig formatter:

```zig
pub fn getCycleFn(self: *Queue, cycle: u64) ![]const u8 {
    // Convert cycle number to epoch seconds
    const roll_secs: u64 = @intCast(@divTrunc(self.roll_length_ms, 1000));
    const epoch_secs = cycle * roll_secs;

    // Convert to broken-down time (UTC)
    const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = epoch_secs };
    const day = epoch_seconds.getEpochDay();
    const year_day = day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch_seconds.getDaySeconds();

    const year = year_day.year;
    const month = @intFromEnum(month_day.month);
    const day_of_month = month_day.day_index + 1;
    const hour = day_seconds.getHoursIntoDay();
    const minute = day_seconds.getMinutesIntoHour();
    const second = day_seconds.getSecondsIntoMinute();

    // Format using the pre-converted strftime pattern
    const date_part = try formatDateFromPattern(
        self.allocator,
        self.roll_strftime orelse return error.NoRollStrftime,
        year,
        month,
        day_of_month,
        hour,
        minute,
        second,
    );
    defer self.allocator.free(date_part);

    // Build full path: <dirname>/<date_part>.cq4
    return try std.fmt.allocPrint(self.allocator, "{s}/{s}.cq4", .{
        self.dirname,
        date_part,
    });
}

/// Interpret a strftime-like pattern with only the tokens we support:
///   %Y → 4-digit year    %m → 2-digit month    %d → 2-digit day
///   %H → 2-digit hour    %M → 2-digit minute   %S → 2-digit second
/// All other characters are copied literally.
fn formatDateFromPattern(
    allocator: std.mem.Allocator,
    pattern: []const u8,
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
) ![]const u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();

    var i: usize = 0;
    while (i < pattern.len) {
        if (pattern[i] == '%' and i + 1 < pattern.len) {
            const spec = pattern[i + 1];
            switch (spec) {
                'Y' => try buf.writer().print("{d:0>4}", .{year}),
                'm' => try buf.writer().print("{d:0>2}", .{month}),
                'd' => try buf.writer().print("{d:0>2}", .{day}),
                'H' => try buf.writer().print("{d:0>2}", .{hour}),
                'M' => try buf.writer().print("{d:0>2}", .{minute}),
                'S' => try buf.writer().print("{d:0>2}", .{second}),
                else => {
                    try buf.append('%');
                    try buf.append(spec);
                },
            }
            i += 2;
        } else {
            try buf.append(pattern[i]);
            i += 1;
        }
    }

    return try buf.toOwnedSlice();
}
```

---

## 12. Reader vs Appender Tailers

Both reader tailers and the appender use the **same** `Tailer` struct and
the **same** `peekQueueTailerR` state machine. The differences are:

| Aspect | Reader tailer | Appender tailer |
|---|---|---|
| **`mmap_protection`** | `PROT_READ` | `PROT_READ \| PROT_WRITE` |
| **File open flags** | `O_RDONLY` | `O_RDWR` |
| **`dispatcher`** | User callback | `null` |
| **`dispatch_ctx`** | User context | `null` |
| **In queue's list** | `queue.tailer_head` linked list | `queue.appender` (single pointer) |
| **Created by** | `chronicle_tailer` / `Queue.createTailer` | `chronicle_append_ts` / `Queue.ensureAppender` (lazy) |
| **Start index** | User-specified (clamped) | `(highest_cycle - patch_cycles) << cycle_shift` |
| **`TS_EXTEND_FAIL`** | Never returned (read tailers) | Returned when < 2 blocks remaining |
| **Advances after write** | N/A | Does NOT advance — lets next peek handle it |

The `mmap_protection` field is the key discriminator. In the state machine:

```c
// Only write tailers get TS_EXTEND_FAIL
if (tailer->qf_statbuf.st_size - mmapoff < 2*queue->blocksize
    && tailer->mmap_protection != PROT_READ) {
    return TS_EXTEND_FAIL;
}
```

And during file open:

```c
int fopen_flags = O_RDONLY;
if (tailer->mmap_protection != PROT_READ) fopen_flags = O_RDWR;
```

In Zig, the same check:

```zig
const is_writer = self.mmap_protection != posix.PROT.READ;
const open_flags: std.fs.File.OpenFlags = if (is_writer)
    .{ .mode = .read_write }
else
    .{};
```

---

## 13. Complete Zig State Machine Code

Here is the full `peekQueueTailerR` function translated to Zig:

```zig
const HD_UNALLOCATED: u32 = 0x00000000;
const HD_WORKING: u32     = 0x80000000;
const HD_METADATA: u32    = 0x40000000;
const HD_EOF: u32         = 0xC0000000;
const HD_MASK_LENGTH: u32 = 0x3FFFFFFF;
const HD_MASK_META: u32   = 0xC0000000;

fn peekQueueTailerR(self: *Queue, tailer: *Tailer) TailState {
    while (true) {
        // ── Step 1: Extract cycle from index ─────────────────
        const cycle: u64 = tailer.qf_index >> self.cycle_shift;

        // ── Step 2: Open queue file if cycle changed ─────────
        if (cycle != tailer.qf_cycle_open or tailer.qf_fn == null) {
            // Free old resources
            if (tailer.qf_fn) |old_fn| {
                self.allocator.free(old_fn);
                tailer.qf_fn = null;
            }
            if (tailer.qf_buf) |buf| {
                const aligned: [*]align(std.mem.page_size) u8 = @alignCast(buf);
                posix.munmap(aligned[0..tailer.qf_mmapsz]);
                tailer.qf_buf = null;
            }
            if (tailer.qf_fd) |fd| {
                posix.close(fd);
                tailer.qf_fd = null;
            }

            // Generate filename for this cycle
            tailer.qf_fn = self.getCycleFn(cycle) catch return .e_stat;
            tailer.qf_tip = 0;

            log.info("opening cycle {} filename {s} (highest_cycle {})", .{
                cycle,
                tailer.qf_fn.?,
                self.highest_cycle,
            });

            // Open the file
            const is_writer = tailer.mmap_protection != posix.PROT.READ;
            const open_flags: std.fs.File.OpenFlags = if (is_writer)
                .{ .mode = .read_write }
            else
                .{};

            const file = std.fs.cwd().openFile(tailer.qf_fn.?, open_flags) catch {
                log.info("awaiting queuefile for {s}", .{tailer.qf_fn.?});

                // If our cycle < highest_cycle, skip this missing file
                if (cycle < self.highest_cycle) {
                    const skip_to = (cycle + 1) << self.cycle_shift;
                    log.info("skipping queuefile, bumping index from {} to {}", .{
                        tailer.qf_index,
                        skip_to,
                    });
                    tailer.qf_index = skip_to;
                    continue;
                }
                return .awaiting_queuefile;
            };

            tailer.qf_fd = file.handle;
            tailer.qf_cycle_open = cycle;

            // Refresh stat
            const stat = posix.fstat(file.handle) catch return .e_stat;
            tailer.qf_stat_size = @intCast(stat.size);
        }

        // ── Step 3: Calculate mmap window ────────────────────
        const blocksize_mask: u64 = ~(@as(u64, self.blocksize) - 1);
        const mmapoff: u64 = tailer.qf_tip & blocksize_mask;
        const two_blocks: u64 = 2 * @as(u64, self.blocksize);

        // Renew stat if near end of file
        if (tailer.qf_stat_size -| mmapoff < two_blocks) {
            log.debug("approaching file size limit, refreshing stat", .{});
            if (tailer.qf_fd) |fd| {
                const stat = posix.fstat(fd) catch return .e_stat;
                tailer.qf_stat_size = @intCast(stat.size);
            }
            // Signal extend needed for write tailers
            if (tailer.qf_stat_size -| mmapoff < two_blocks and
                tailer.mmap_protection != posix.PROT.READ)
            {
                return .extend_fail;
            }
        }

        const remaining = tailer.qf_stat_size -| mmapoff;
        const limit: u64 = if (remaining > two_blocks) two_blocks else remaining;
        if (limit == 0) {
            return .awaiting_entry;
        }

        // ── Step 4: Re-mmap if window changed ────────────────
        if (tailer.qf_buf == null or
            mmapoff != tailer.qf_mmapoff or
            limit != tailer.qf_mmapsz)
        {
            // Unmap old window
            if (tailer.qf_buf) |buf| {
                const aligned: [*]align(std.mem.page_size) u8 = @alignCast(buf);
                posix.munmap(aligned[0..tailer.qf_mmapsz]);
                tailer.qf_buf = null;
            }

            tailer.qf_mmapsz = limit;
            tailer.qf_mmapoff = mmapoff;

            const fd = tailer.qf_fd orelse return .e_mmap;
            const mapped = posix.mmap(
                null,
                limit,
                tailer.mmap_protection,
                .{ .TYPE = .SHARED },
                fd,
                @intCast(mmapoff),
            ) catch {
                log.err("mmap failed for {s} offset 0x{x} size 0x{x}", .{
                    tailer.qf_fn orelse "<null>",
                    mmapoff,
                    limit,
                });
                return .e_mmap;
            };
            tailer.qf_buf = mapped.ptr;

            log.debug("mmap offset 0x{x} size 0x{x}", .{ mmapoff, limit });
        }

        // ── Step 5: Call parse_queue_block ────────────────────
        const buf = tailer.qf_buf orelse return .e_mmap;
        const offset_in_map = tailer.qf_tip - tailer.qf_mmapoff;

        var basep: u64 = offset_in_map;
        const basep_old: u64 = basep;
        var index: u64 = tailer.qf_index;

        const s = parseQueueBlock(
            self,
            buf,
            &basep,
            &index,
            tailer.qf_mmapsz,
            tailer,
        );

        // ── Step 6: Interpret results ────────────────────────

        // Double blocksize if parser couldn't make progress
        if (s == .need_extend and basep == basep_old) {
            self.doubleBlocksize();
        }

        // Commit any progress
        if (basep != basep_old) {
            const new_tip = basep + tailer.qf_mmapoff;
            log.debug("parser advanced: tip {} -> {}, index {} -> {}", .{
                tailer.qf_tip,
                new_tip,
                tailer.qf_index,
                index,
            });
            tailer.qf_tip = new_tip;
            tailer.qf_index = index;
        }

        if (s == .busy) return .busy;
        if (s == .collected) return .collected;

        if (s == .awaiting_entry) {
            // If cycle is far enough behind highest, skip missing EOF
            if (cycle < self.highest_cycle -| Queue.patch_cycles) {
                const skip_to = (cycle + 1) << self.cycle_shift;
                log.info("missing EOF, skipping from {} to {}", .{
                    tailer.qf_index,
                    skip_to,
                });
                tailer.qf_index = skip_to;
                continue;
            }
            return .awaiting_entry;
        }

        if (s == .reached_eof) {
            // Advance to start of next cycle
            const current_cycle = tailer.qf_index >> self.cycle_shift;
            const eof_next = (current_cycle + 1) << self.cycle_shift;
            log.info("hit EOF, advancing index from {} to {}", .{
                tailer.qf_index,
                eof_next,
            });
            tailer.qf_index = eof_next;
            continue; // will open next cycle file
        }

        // For need_extend with progress, loop will re-mmap
        if (s == .need_extend) continue;

        // null_item — shouldn't happen with parse_data_cb installed
        return .awaiting_entry;
    }
}
```

---

## 14. Complete Zig `parse_queue_block` Code

```zig
fn parseQueueBlock(
    queue: *Queue,
    buf: [*]u8,
    basep: *u64,
    indexp: *u64,
    extent_sz: u64,
    tailer: *Tailer,
) ParseQBState {
    var base = basep.*;
    var index = indexp.*;
    var pd: ParseQBState = .awaiting_entry;

    while (pd == .awaiting_entry) {
        // ── Check we can read a 4-byte header ────────────────
        if (base + 4 > extent_sz) return .need_extend;

        // ── Read header (little-endian, 4 bytes) ─────────────
        const header_bytes = buf[base..][0..4];
        const header: u32 = std.mem.readInt(u32, header_bytes, .little);

        // ── Memory fence: do not speculatively read payload ──
        // On x86 this is mfence; on ARM it would be dmb.
        @fence(.seq_cst);

        // ── Dispatch based on header type ────────────────────
        if (header == HD_UNALLOCATED) {
            return .awaiting_entry;
        }

        const meta_bits = header & HD_MASK_META;
        const sz: u32 = header & HD_MASK_LENGTH;
        const sz_usize: u64 = @intCast(sz);

        if (meta_bits == HD_WORKING) {
            log.debug("hit working header at offset {}, pid {}", .{
                base,
                sz,
            });
            return .busy;
        }

        if (meta_bits == HD_METADATA) {
            // Metadata entry — pass to wire parser, don't increment index
            if (base + 4 + sz_usize > extent_sz) return .need_extend;

            const payload = buf[base + 4 ..][0..sz_usize];
            queue.wireParseMetadata(payload);

            // Note: metadata does NOT increment the data index
        } else if (meta_bits == HD_EOF) {
            return .reached_eof;
        } else {
            // Data entry — dispatch to user callback
            if (base + 4 + sz_usize > extent_sz) return .need_extend;

            const payload = buf[base + 4 ..][0..sz_usize];
            pd = parseDataCallback(payload, index, tailer);

            // Increment data index (even if callback skipped it)
            index += 1;
            indexp.* = index;
        }

        // ── Advance past this entry ──────────────────────────
        // V5 padding: align to 4-byte boundary after each entry
        const pad4: u64 = if (queue.version == .v5)
            ((~sz_usize) +% 1) & 0x03   // equivalent to -sz & 0x03
        else
            0;

        base = base + 4 + sz_usize + pad4;
        basep.* = base;
    }
    return pd;
}
```

### Notes on the Zig translation

1. **`@fence(.seq_cst)`** — generates `mfence` on x86-64 and the appropriate
   barrier on other architectures. This replaces the `asm volatile ("mfence")`
   in the C code and ensures no speculative reads of payload data occur before
   the header value is known.

2. **`std.mem.readInt(u32, ..., .little)`** — reads a little-endian 32-bit
   integer from an arbitrary byte slice. This replaces the C `memcpy(&header, base, 4)`
   and is explicit about endianness.

3. **V5 padding calculation** — `((~sz_usize) +% 1) & 0x03` is the Zig
   equivalent of `-sz & 0x03` in C. The `+%` operator performs wrapping
   addition, which is needed because `~sz_usize + 1` would overflow for
   `sz_usize = 0`. Alternatively, this can be written as:
   ```zig
   const pad4 = (4 - (sz_usize & 0x03)) & 0x03;
   ```
   Both produce the same result.

4. **`basep` and `indexp` are pointer parameters** — matching the C
   convention of passing `unsigned char** basep` and `uint64_t* indexp`.
   In Zig we pass `*u64` for both (with `basep` being an offset into the
   buffer rather than a raw pointer, which is safer and avoids pointer
   arithmetic on mmap'd memory).

5. **The inner while loop** continues as long as `pd == .awaiting_entry`.
   When `parseDataCallback` returns `.collected` or any other non-awaiting
   state, the loop exits and that state bubbles up. This is identical to the
   C logic.

---

## 15. Testing Notes

### Unit tests for the parsing loop

```zig
const testing = std.testing;

test "parse_queue_block handles unallocated header" {
    // A buffer of all zeros = HD_UNALLOCATED
    var buf = [_]u8{0} ** 64;
    var basep: u64 = 0;
    var index: u64 = 0;

    const result = parseQueueBlock(
        &test_queue,
        &buf,
        &basep,
        &index,
        64,
        &test_tailer,
    );

    try testing.expectEqual(ParseQBState.awaiting_entry, result);
    try testing.expectEqual(@as(u64, 0), basep); // no progress
    try testing.expectEqual(@as(u64, 0), index); // no data consumed
}

test "parse_queue_block handles EOF" {
    var buf: [8]u8 = undefined;
    // Write HD_EOF = 0xC0000000 in little-endian
    std.mem.writeInt(u32, buf[0..4], HD_EOF, .little);

    var basep: u64 = 0;
    var index: u64 = 0;

    const result = parseQueueBlock(
        &test_queue,
        &buf,
        &basep,
        &index,
        8,
        &test_tailer,
    );

    try testing.expectEqual(ParseQBState.reached_eof, result);
}

test "parse_queue_block handles data entry with v5 padding" {
    // Header: size = 5 (no meta bits)
    // Payload: 5 bytes
    // Pad: 3 bytes (to align to 4)
    // Total: 4 + 5 + 3 = 12 bytes
    var buf = [_]u8{0} ** 64;
    std.mem.writeInt(u32, buf[0..4], 5, .little);
    @memcpy(buf[4..9], "hello");
    // Next header at offset 12 — unallocated (zeros)

    var test_queue_v5 = makeTestQueue(.v5);
    var basep: u64 = 0;
    var index: u64 = 0;

    const result = parseQueueBlock(
        &test_queue_v5,
        &buf,
        &basep,
        &index,
        64,
        &test_tailer,
    );

    try testing.expectEqual(ParseQBState.awaiting_entry, result);
    try testing.expectEqual(@as(u64, 12), basep); // 4 + 5 + 3 padding
    try testing.expectEqual(@as(u64, 1), index);  // one data entry consumed
}

test "parse_queue_block handles metadata then data" {
    var buf = [_]u8{0} ** 128;
    // Metadata entry: HD_METADATA | 8
    std.mem.writeInt(u32, buf[0..4], HD_METADATA | 8, .little);
    // 8 bytes of metadata payload (ignored content for this test)
    // v5 pad: -8 & 3 = 0 (already aligned)
    // Total: 4 + 8 = 12 bytes

    // Data entry at offset 12: size = 3
    std.mem.writeInt(u32, buf[12..16], 3, .little);
    @memcpy(buf[16..19], "abc");
    // v5 pad: -3 & 3 = 1
    // Next header at offset 12 + 4 + 3 + 1 = 20 — unallocated

    var test_queue_v5 = makeTestQueue(.v5);
    var basep: u64 = 0;
    var index: u64 = 0;

    const result = parseQueueBlock(
        &test_queue_v5,
        &buf,
        &basep,
        &index,
        128,
        &test_tailer,
    );

    try testing.expectEqual(ParseQBState.awaiting_entry, result);
    try testing.expectEqual(@as(u64, 20), basep); // past both entries
    try testing.expectEqual(@as(u64, 1), index);  // only data entry counted
}
```

### Integration tests

1. **Read a Java-created queue:** Open a v5 queue created by Java Chronicle
   Queue, verify all messages are read in order with correct indices.

2. **Multi-cycle read:** Create a queue with multiple cycle files, verify
   the tailer correctly transitions between files, handling EOF markers and
   cycle rolls.

3. **Resume from index:** Create a tailer at index N, verify that entries
   0 through N-1 are scanned but not dispatched, and entry N is the first
   dispatched.

4. **Missing EOF tolerance:** Create a multi-cycle queue where one cycle
   file lacks an EOF marker. Verify that a tailer with
   `cycle < highest_cycle - patch_cycles` skips forward correctly.

5. **Collect API:** Use `collect` and `returnCollected` to synchronously
   read values, verify the correct message and index are returned.

6. **Writer + Reader together:** Start a writer appending messages. Start a
   reader tailer. Verify the reader receives all messages in order with
   correct indexing, including across cycle rolls.

7. **Blocksize doubling:** Create a queue with a small initial blocksize,
   write a message larger than the blocksize, verify the blocksize is doubled
   and the message is read correctly.

### Memory leak detection

Zig's `testing.allocator` will catch any leaked allocations from:
- Unclosed tailers (leaked `qf_fn`, mmap)
- Unfreed `Collected` messages
- Unclosed file descriptors (though Zig tracks only allocations, not fds)

For fd leak detection, check `/proc/self/fd` count before and after the test.

---

## Summary of C → Zig Mapping

| C function | Zig method | Key changes |
|---|---|---|
| `chronicle_tailer` | `Queue.createTailer` | Allocator-based, `errdefer`, enum state |
| `chronicle_tailer_close` | `Tailer.close` | Optional types, no null-ptr deref risk |
| `chronicle_tailer_state` | `tailer.state` (direct field access) | Enum type |
| `chronicle_tailer_index` | `tailer.qf_index` (direct field access) | Same |
| `chronicle_peek` | `QueueRegistry.peekAll` or `queue.peekQueue` | No global list |
| `chronicle_peek_queue` | `Queue.peekQueue` | Same logic |
| `chronicle_peek_queue_tailer_r` | `Queue.peekQueueTailerR` | `switch` on enum, Zig mmap API |
| `chronicle_peek_tailer` | `Tailer.peekTailer` (free function or method) | Thin wrapper |
| `parse_queue_block` | `parseQueueBlock` | `std.mem.readInt`, `@fence`, offset-based |
| `parse_data_cb` | `parseDataCallback` | Zig slices instead of ptr+len |
| `chronicle_collect` | `Tailer.collect` | Error union, `defer` for cleanup |
| `chronicle_return` | `Tailer.returnCollected` | Same logic |
| `chronicle_get_cycle_fn` | `Queue.getCycleFn` | Pure Zig date formatting |
| `peek_queue_modcount` | `Queue.peekQueueModcount` | `volatile` reads |
| `parse_wire_data` | `Queue.wireParseMetadata` | Delegates to wire parser |
| `mfence` asm | `@fence(.seq_cst)` | Portable across architectures |
| `memcpy(&header, ...)` | `std.mem.readInt(u32, ..., .little)` | Explicit endianness |
| `PROT_READ` / `PROT_WRITE` | `posix.PROT.READ` / `posix.PROT.WRITE` | Same semantics |
| `mmap` / `munmap` | `posix.mmap` / `posix.munmap` | Zig slices, error unions |