# Task 5: Appender (Writer) Implementation

This document covers the Zig reimplementation of the Chronicle Queue appender
(writer) — the `chronicle_append_ts` function and all supporting logic. The
appender is the most complex single function in libchronicle, combining lazy
initialisation, a state machine loop, atomic CAS operations, cycle rolling,
EOF patching, and file creation with atomic rename.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Appender Architecture](#2-appender-architecture)
3. [chronicle_append / chronicle_append_ts Entry Points](#3-entry-points)
4. [Size Validation](#4-size-validation)
5. [Modcount Refresh](#5-modcount-refresh)
6. [Lazy Appender Creation](#6-lazy-appender-creation)
7. [The Write Loop State Machine](#7-the-write-loop-state-machine)
8. [Handling TS_AWAITING_QUEUEFILE](#8-handling-ts_awaiting_queuefile)
9. [Handling TS_EXTEND_FAIL](#9-handling-ts_extend_fail)
10. [Handling Non-AWAITING_ENTRY States](#10-handling-non-awaiting_entry-states)
11. [The CAS Write Sequence](#11-the-cas-write-sequence)
12. [Cycle Rolling](#12-cycle-rolling)
13. [EOF Patching](#13-eof-patching)
14. [Clock and Cycle Helpers](#14-clock-and-cycle-helpers)
15. [Poke Queue Modcount](#15-poke-queue-modcount)
16. [Concurrency Model](#16-concurrency-model)
17. [Complete Zig Code Sketch](#17-complete-zig-code-sketch)
18. [Testing Notes](#18-testing-notes)

---

## 1. Overview

The appender is the write path of Chronicle Queue. A single call to
`chronicle_append_ts(queue, msg, ms)` may trigger any combination of:

- Creating the appender tailer (first call only)
- Re-opening the directory listing in read-write mode
- Creating a new `.cq4` queuefile (with atomic rename)
- Extending an existing queuefile on disk
- Writing an EOF marker to roll to the next cycle
- Patching EOF markers on stale cycle files
- Performing a CAS (compare-and-swap) to acquire the write lock
- Writing the payload and releasing the lock by writing the size header

All of this happens inside a `while(1)` loop that polls the appender tailer's
state machine (the same `peek_queue_tailer_r` used by readers) and reacts to
each state.

### C function reference

| C function | Lines | Purpose |
|---|---|---|
| `chronicle_append` | 1036-1039 | Wrapper: calls `chronicle_append_ts` with current clock |
| `chronicle_append_ts` | 1041-1231 | Full append logic |
| `chronicle_clock_ms` | 582-588 | Get current time in milliseconds |
| `chronicle_cycle_from_ms` | 590-592 | Convert milliseconds to cycle number |
| `poke_queue_modcount` | 802-810 | Push cycle/modcount to shared mmap |
| `lock_cmpxchgl` | 216-222 | Inline asm: `lock; cmpxchgl` |
| `lock_xadd` | 224-231 | Inline asm: `lock; xaddl` |
| `queuefile_init` | 1370-1398 | Create empty .cq4 file |
| `queue_double_blocksize` | 252-256 | Double blocksize when needed |

---

## 2. Appender Architecture

The appender is **not** a separate struct — it is a `tailer_t` with special
properties:

| Property | Reader tailer | Appender tailer |
|---|---|---|
| `mmap_protection` | `PROT_READ` | `PROT_READ \| PROT_WRITE` |
| `dispatcher` | User callback | `null` |
| `dispatch_ctx` | User context | `null` |
| Linked list | In `queue->tailers` | Stored as `queue->appender` |
| Index start | User-specified | `(highest_cycle - patch_cycles) << cycle_shift` |

The appender reuses the tailer's state machine (`peek_queue_tailer_r`) to
navigate through the queue files. Because it has `PROT_WRITE`, it can:
- Write to the mmap'd queuefile
- Signal `TS_EXTEND_FAIL` when the file needs growing
- Perform CAS operations on headers

---

## 3. Entry Points

### `chronicle_append` (C line 1036)

A thin wrapper that calls `chronicle_append_ts` with the current clock:

```c
uint64_t chronicle_append(queue_t *queue, COBJ msg) {
    long ms = chronicle_clock_ms(queue);
    return chronicle_append_ts(queue, msg, ms);
}
```

In Zig:

```zig
pub fn append(self: *Queue, msg: *const anyopaque) !u64 {
    return self.appendTs(msg, self.clockMs());
}
```

### `chronicle_append_ts` (C line 1041)

This is the full implementation, detailed in sections 4-13 below.

---

## 4. Size Validation

Before anything else, the appender validates the write size (C lines 1082-1086):

```c
size_t write_sz = queue->append_sizeof(msg);
if (write_sz < 0) return 0;
if (write_sz > HD_MASK_META) return chronicle_err("shm msg sz > 30bit");
while (write_sz > queue->blocksize)
    queue_double_blocksize(queue);
```

Three checks:
1. **Positive size** — the sizeof callback must return a valid size
2. **Fits in 30 bits** — the header format uses 30 bits for the length field
   (`HD_MASK_LENGTH = 0x3FFFFFFF`), so maximum message size is ~1 GiB
3. **Fits in blocksize** — if the message is larger than the current blocksize,
   double the blocksize repeatedly until it fits. This ensures the mmap window
   (2× blocksize) can contain the entire message.

In Zig:

```zig
const write_sz = self.append_sizeof.?(msg);
if (write_sz == 0) return error.ZeroSizeMessage;
if (write_sz > HD_MASK_LENGTH) return error.MessageTooLarge;
while (write_sz > self.blocksize) {
    self.doubleBlocksize();
}
```

### `doubleBlocksize`

```zig
fn doubleBlocksize(self: *Queue) void {
    const new = self.blocksize << 1;
    log.info("doubling blocksize from 0x{x} to 0x{x}", .{ self.blocksize, new });
    self.blocksize = new;
}
```

---

## 5. Modcount Refresh

Before writing, we poll the directory listing mmap for changes (C line 1089):

```c
peek_queue_modcount(queue);
```

This reads the shared `modcount` field from the mmap. If it has changed since
our last check, it refreshes `highest_cycle` and `lowest_cycle` too. This
allows our appender to follow another appender that may have created new
queuefiles.

### `peek_queue_modcount` (C lines 788-800)

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

In Zig:

```zig
fn peekQueueModcount(self: *Queue) void {
    const fields = self.dirlist_fields;
    const modcount = @as(*const volatile u64, @ptrCast(fields.modcount.?)).*;

    if (self.modcount != modcount) {
        log.info("{s} modcount changed from {} to {}", .{ self.dirname, self.modcount, modcount });
        self.modcount = modcount;
        self.lowest_cycle = @as(*const volatile u64, @ptrCast(fields.lowest_cycle.?)).*;
        self.highest_cycle = @as(*const volatile u64, @ptrCast(fields.highest_cycle.?)).*;
    }
}
```

**Note on `volatile`:** The C code uses `memcpy` from an mmap'd pointer,
which prevents the compiler from caching the value. In Zig, we use
`@as(*const volatile u64, ...).*` to achieve the same effect — ensuring
every read actually hits the mmap'd memory and is not elided by the
optimiser.

---

## 6. Lazy Appender Creation

The appender is created on the first call to `appendTs`, not during `open`.
This is because appending requires the directory listing to be mapped
read-write, and most readers never need that.

### C logic (lines 1092-1112)

```c
if (queue->appender == NULL) {
    tailer_t* tailer = malloc(sizeof(tailer_t));
    bzero(tailer, sizeof(tailer_t));
    tailer->qf_index = (queue->highest_cycle - patch_cycles) << queue->cycle_shift;
    tailer->dispatcher = NULL;
    tailer->state = 5;  // TS_PEEK
    tailer->mmap_protection = PROT_READ | PROT_WRITE;
    tailer->queue = queue;
    queue->appender = tailer;

    int x = directory_listing_reopen(queue, O_RDWR, PROT_READ | PROT_WRITE);
    if (x != 0) return -1;
}
```

Key points:
- **`qf_index`** starts at `(highest_cycle - patch_cycles) << cycle_shift`.
  The `patch_cycles = 3` lookback ensures the appender scans recent cycle
  files to patch any missing EOF markers before writing new data.
- **`mmap_protection`** is `PROT_READ | PROT_WRITE` — this is what allows
  CAS operations and direct writes to the mmap'd queuefile.
- **Directory listing re-open** — switches from read-only to read-write so
  the appender can update `highest_cycle`, `lowest_cycle`, and `modcount`
  in the shared mmap.

### Zig sketch

```zig
fn ensureAppender(self: *Queue) !*Tailer {
    if (self.appender) |app| return app;

    var tailer = try self.allocator.create(Tailer);
    errdefer self.allocator.destroy(tailer);
    tailer.* = Tailer{
        .queue = self,
        .mmap_protection = std.posix.PROT.READ | std.posix.PROT.WRITE,
        .state = .peek,
        .dispatcher = null,
        .dispatch_ctx = null,
    };

    // Start scanning from patch_cycles before highest to patch missing EOFs
    const start_cycle = if (self.highest_cycle > patch_cycles)
        self.highest_cycle - patch_cycles
    else
        0;
    tailer.qf_index = start_cycle << self.cycle_shift;

    self.appender = tailer;

    // Re-open directory listing in read-write mode
    try self.directoryListingReopen(.read_write);

    log.info("appender created, starting at cycle {}", .{start_cycle});
    return tailer;
}
```

---

## 7. The Write Loop State Machine

After the appender exists, `chronicle_append_ts` enters a `while(1)` loop
(C lines 1115-1228) that repeatedly polls the appender and reacts to its
state:

```
┌─────────────────────────────────────────────┐
│              while (true)                    │
│  ┌─────────────────────────────────────────┐ │
│  │  r = peek_queue_tailer(queue, appender) │ │
│  └─────────────┬───────────────────────────┘ │
│                │                             │
│    ┌───────────┼───────────┬──────────┐      │
│    ▼           ▼           ▼          ▼      │
│  AWAITING    EXTEND     AWAITING   (other)   │
│  QUEUEFILE   FAIL       ENTRY                │
│    │           │           │          │      │
│  create      extend      CAS       sleep     │
│  file        file        write     retry     │
│    │           │           │                 │
│  continue    continue    break               │
│                                              │
└──────────────────────────────────────────────┘
```

The C code actually calls `peek_queue_tailer` **twice** before switching on
the result (lines 1117-1118):

```c
int r = chronicle_peek_queue_tailer(queue, appender);
r = chronicle_peek_queue_tailer(queue, appender);
```

The comment says "2nd call defensive to ensure 1 whole blocksize is available
to put". This double-poll advances the appender far enough to guarantee the
write window is positioned correctly.

---

## 8. Handling TS_AWAITING_QUEUEFILE

When the appender's cycle points to a file that doesn't exist, we need to
create it. C lines 1124-1150:

```c
if (r == TS_AWAITING_QUEUEFILE) {
    char* fn_buf;
    asprintf(&fn_buf, "%s.%d.tmp", appender->qf_fn, pid_header);
    if (queuefile_init(fn_buf, queue) != 0) return -1;
    if (rename(fn_buf, appender->qf_fn) != 0) {
        sleep(1);
        continue;
    }
    free(fn_buf);
    uint64_t cyc = appender->qf_index >> queue->cycle_shift;
    if (cyc > queue->highest_cycle) {
        queue->highest_cycle = cyc;
        poke_queue_modcount(queue);
    }
    continue;
}
```

### Protocol

1. **Generate temp filename** — `<desired_name>.cq4.<pid>.tmp`
2. **Create the queuefile** — call `queuefile_init` which creates the file
   and extends it to `qf_disk_sz` (83,754,496 bytes)
3. **Atomic rename** — `rename(tmp, final)`. If this fails (another writer
   raced us), sleep and retry. The rename is atomic on POSIX, so exactly one
   writer wins.
4. **Bump modcount** — if our new file has a higher cycle than the recorded
   `highest_cycle`, update the shared mmap fields and atomically increment
   `modcount` so other processes see the change.
5. **Continue** — retry the loop; the tailer will now successfully open the file.

### Zig sketch

```zig
.awaiting_queuefile => {
    const qf_fn = appender.qf_fn orelse return error.NoFilename;

    // Build temporary filename: <path>.cq4.<pid>.tmp
    const pid = std.os.linux.getpid();
    const tmp_name = try std.fmt.allocPrint(
        self.allocator,
        "{s}.{d}.tmp",
        .{ qf_fn, pid },
    );
    defer self.allocator.free(tmp_name);

    // Create the empty queuefile at the temporary path
    try self.queuefileInit(tmp_name);

    // Atomic rename — if it fails, another writer won the race
    std.fs.cwd().rename(tmp_name, qf_fn) catch |err| {
        log.warn("create queuefile rename failed: {}", .{err});
        std.time.sleep(1 * std.time.ns_per_s);
        continue;
    };

    log.info("created queuefile: {s}", .{qf_fn});

    // If this is a new highest cycle, tell other processes
    const cyc = appender.qf_index >> self.cycle_shift;
    if (cyc > self.highest_cycle) {
        self.highest_cycle = cyc;
        self.pokeQueueModcount();
    }
    continue;
},
```

---

## 9. Handling TS_EXTEND_FAIL

The tailer state machine returns `TS_EXTEND_FAIL` when the appender's mmap
window is approaching the end of the file (less than 2× blocksize remaining)
and the tailer has write protection. The appender must extend the file on
disk. C lines 1152-1165:

```c
if (r == TS_EXTEND_FAIL) {
    uint64_t extend_to = appender->qf_statbuf.st_size + qf_disk_sz;
    if (lseek(appender->qf_fd, extend_to - 1, SEEK_SET) == -1) {
        sleep(1); continue;
    }
    if (write(appender->qf_fd, "", 1) != 1) {
        sleep(1); continue;
    }
    continue;
}
```

This extends the file by another `qf_disk_sz` (≈83 MB) chunk. The technique
of seeking past the end and writing one byte causes the filesystem to allocate
the space (or create a sparse file, depending on the filesystem).

### Zig sketch

```zig
.extend_fail => {
    const fd = appender.qf_fd orelse return error.NoFileDescriptor;
    const current_size = (try std.posix.fstat(fd)).size;
    const extend_to: u64 = @intCast(current_size + @as(i64, @intCast(Queue.qf_disk_sz)));

    // Extend by seeking and writing a single byte
    const file = std.fs.File{ .handle = fd };
    file.seekTo(extend_to - 1) catch {
        std.time.sleep(1 * std.time.ns_per_s);
        continue;
    };
    file.writer().writeByte(0) catch {
        std.time.sleep(1 * std.time.ns_per_s);
        continue;
    };

    log.info("extended queuefile to {} bytes", .{extend_to});
    continue;
},
```

---

## 10. Handling Non-AWAITING_ENTRY States

If the tailer returns anything other than `TS_AWAITING_ENTRY`,
`TS_AWAITING_QUEUEFILE`, or `TS_EXTEND_FAIL`, the appender cannot write.
This could be `TS_BUSY` (another writer holds the lock) or an error state.
The C code sleeps and retries (lines 1171-1175):

```c
if (r != TS_AWAITING_ENTRY) {
    printf("shmipc: Cannot write in state %d, sleeping\n", r);
    sleep(1);
    continue;
}
```

In Zig, we can be more nuanced — return transient errors for retryable states
and hard errors for fatal ones:

```zig
.busy => {
    // Another writer holds the lock — back off and retry
    std.time.sleep(1 * std.time.ns_per_s);
    continue;
},
.e_stat, .e_mmap => {
    return error.FatalTailerError;
},
else => {
    log.warn("cannot write in state {}, sleeping", .{r});
    std.time.sleep(1 * std.time.ns_per_s);
    continue;
},
```

---

## 11. The CAS Write Sequence

This is the core of the appender — the actual write operation. When the
tailer is in `TS_AWAITING_ENTRY`, we know `qf_tip` points to an unallocated
header in the mmap. The sequence is:

### Step-by-step (C lines 1177-1228)

#### 1. Bounds check

```c
if ((appender->qf_tip - appender->qf_mmapoff) + write_sz > appender->qf_mmapsz) {
    printf("aborting on bug: write would segfault buffer!\n");
    abort();
}
```

This is a safety check — if we somehow got here without enough space in the
mmap window, abort rather than SIGBUS.

#### 2. Calculate pointer

```c
unsigned char* ptr = (appender->qf_tip - appender->qf_mmapoff) + appender->qf_buf;
```

`ptr` is the address within the mmap that corresponds to the file position
`qf_tip`. The math: `qf_tip` is the absolute file offset; `qf_mmapoff` is
where the mmap window starts; `qf_buf` is the base address of the mmap.

#### 3. CAS: UNALLOCATED → WORKING

```c
uint32_t ret = lock_cmpxchgl(ptr, HD_UNALLOCATED, HD_WORKING);
```

This performs an atomic compare-and-swap on the 4-byte header:
- Expected old value: `HD_UNALLOCATED` (0x00000000)
- Desired new value: `HD_WORKING` (0x80000000) — sets the "working" bit,
  with the PID in the lower bits (the C code uses `HD_WORKING` without PID
  for the actual CAS, but `pid_header` is prepared for debugging)

The `lock;` prefix ensures this is atomic across all CPUs sharing the mmap.

**If CAS fails** (`ret != HD_UNALLOCATED`): another writer beat us. Sleep
and retry the entire loop.

#### 4. Memory fence

```c
asm volatile ("mfence" ::: "memory");
```

After acquiring the lock, issue a full memory fence to ensure no subsequent
reads are reordered before the CAS.

#### 5. Check for cycle roll

```c
if (ms > 0) {
    uint64_t cyc = chronicle_cycle_from_ms(queue, ms);
    if (cyc > appender->qf_index >> queue->cycle_shift) {
        appender->qf_index = cyc << queue->cycle_shift;
        uint32_t header = HD_EOF;
        memcpy(ptr, &header, sizeof(header));
        continue;
    }
}
```

If the current timestamp indicates we should be in a newer cycle than the
file we're writing to, write an `HD_EOF` marker instead of the data. This
signals to all readers that this cycle file is complete. The loop continues,
and the next iteration will create the new cycle's queuefile.

#### 6. Check for stale cycle (EOF patching)

```c
if (appender->qf_index < queue->highest_cycle << queue->cycle_shift) {
    uint32_t header = HD_EOF;
    memcpy(ptr, &header, sizeof(header));
    continue;
}
```

If we acquired the write lock but we're in a cycle file older than
`highest_cycle`, another writer has already moved on. Write EOF and continue.
This patches files that Java may have left without an EOF (Java relies on
readers timing out instead).

#### 7. Write payload

```c
queue->append_write(ptr+4, msg, write_sz);
```

Write the message body starting 4 bytes after the header.

#### 8. Memory fence

```c
asm volatile ("mfence" ::: "memory");
```

Ensure the payload is fully written to memory before we publish the header.
Without this fence, another reader could see the size header and read
partially-written payload data.

#### 9. Write header (release lock)

```c
uint32_t header = write_sz & HD_MASK_LENGTH;
memcpy(ptr, &header, sizeof(header));
```

Overwrite the header from `HD_WORKING` to the actual size. This atomically
"publishes" the entry — any reader seeing a non-working, non-zero header
will read the payload. The size in the header is the payload size (not
including the 4-byte header itself).

#### 10. Break

The write is complete. `chronicle_append_ts` returns `appender->qf_index`,
which is the 64-bit index of the written entry (cycle in upper 32 bits,
seqnum in lower 32 bits).

### Important note on post-write state

The appender does **NOT** advance `qf_tip` or `qf_index` after writing.
Instead, it lets the tailer's `peek_queue_tailer_r` function discover the
entry on the next poll and advance naturally. This avoids duplicating the
advancement logic and ensures the mmap window is correctly maintained.

---

## 12. Cycle Rolling

Cycle rolling is the process of finishing one queuefile and starting the next
when a time boundary is crossed (e.g., a new hour for `FAST_HOURLY`).

### How the appender detects a roll

In step 5 of the CAS sequence (§11), after acquiring the write lock:

```c
uint64_t cyc = chronicle_cycle_from_ms(queue, ms);
if (cyc > appender->qf_index >> queue->cycle_shift) {
    // Current timestamp says we should be in a later cycle
    appender->qf_index = cyc << queue->cycle_shift;
    // Write EOF to close this cycle file
    uint32_t header = HD_EOF;
    memcpy(ptr, &header, sizeof(header));
    continue;
}
```

The steps:
1. Convert the current timestamp to a cycle number: `cycle = (ms - epoch) / roll_length`
2. Compare with the cycle embedded in the appender's index
3. If the timestamp cycle is greater, set `qf_index` to the start of the new
   cycle (seqnum = 0) and write `HD_EOF` to the current position
4. The `continue` restarts the write loop. On the next iteration,
   `peek_queue_tailer_r` will see the EOF, advance to the next cycle, and
   return `TS_AWAITING_QUEUEFILE` (since the new file doesn't exist yet)
5. The `TS_AWAITING_QUEUEFILE` handler creates the new file
6. On the next iteration after that, the appender is positioned in the new
   file and the write proceeds

### Cycle number calculation

```c
long chronicle_clock_ms(queue_t* queue) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000;
}

uint64_t chronicle_cycle_from_ms(queue_t* queue, long ms) {
    return (ms - queue->roll_epoch) / queue->roll_length;
}
```

For `FAST_HOURLY` with `epoch=0` and `roll_length=3600000`:
- Cycle 0 = 1970-01-01 00:00 to 00:59
- The current cycle at any moment is simply `ms_since_epoch / 3600000`

---

## 13. EOF Patching

Java Chronicle Queue has a design flaw: when a writer is down during a cycle
roll, the previous cycle file may not get an EOF marker. Java readers work
around this by "timing out" — if a reader is waiting for data in cycle N and
knows cycle N+3 exists, it skips ahead.

The C library takes a more proactive approach: the appender starts scanning
from `highest_cycle - patch_cycles` (where `patch_cycles = 3`) and writes
EOF markers to any stale files it encounters.

This happens in two places:

### 1. During the CAS sequence (§11, step 6)

If the appender acquires the write lock in a cycle file that is older than
`highest_cycle`, it writes EOF instead of data. This catches the case where
the appender is still scanning old files during its catch-up phase.

### 2. In the tailer state machine (covered in doc 08)

The tailer's `QB_AWAITING_ENTRY` handler checks if
`cycle < highest_cycle - patch_cycles` and skips forward, effectively
tolerating missing EOFs for sufficiently old files.

### The `patch_cycles` constant

```c
uint32_t patch_cycles = 3;
```

This value of 3 is a compromise: scan the last 3 cycle files for missing
EOFs, but don't waste time scanning the entire history. It matches Java's
timeout heuristic.

---

## 14. Clock and Cycle Helpers

### `chronicle_clock_ms`

```c
long chronicle_clock_ms(queue_t* queue) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000;
}
```

In Zig:

```zig
pub fn clockMs(self: *const Queue) i64 {
    _ = self; // reserved for future custom clock support
    const ts = std.time.milliTimestamp();
    return ts;
}
```

`std.time.milliTimestamp()` returns milliseconds since the Unix epoch, which
is exactly what we need.

### `chronicle_cycle_from_ms`

```c
uint64_t chronicle_cycle_from_ms(queue_t* queue, long ms) {
    return (ms - queue->roll_epoch) / queue->roll_length;
}
```

In Zig:

```zig
pub fn cycleFromMs(self: *const Queue, ms: i64) u64 {
    const adjusted = ms - @as(i64, self.roll_epoch);
    return @intCast(@divTrunc(adjusted, @as(i64, self.roll_length_ms)));
}
```

---

## 15. Poke Queue Modcount

After creating a new queuefile or updating cycle boundaries, the appender
must inform other processes by writing to the shared mmap and atomically
incrementing the modcount.

### C implementation (lines 802-810)

```c
void poke_queue_modcount(queue_t* queue) {
    memcpy(queue->dirlist_fields.highest_cycle, &queue->highest_cycle, sizeof(modcount));
    memcpy(queue->dirlist_fields.lowest_cycle, &queue->lowest_cycle, sizeof(modcount));
    lock_xadd(queue->dirlist_fields.modcount, 1);
}
```

The `lock; xaddl` instruction atomically increments the modcount in the
shared mmap. Other processes polling `peek_queue_modcount` will see the new
value and refresh their cycle boundaries.

### Zig implementation

Zig's `@atomicRmw` compiles to the appropriate atomic instruction:

```zig
fn pokeQueueModcount(self: *Queue) void {
    const fields = self.dirlist_fields;

    // Write updated cycle values to the mmap
    const high_ptr: *volatile u64 = @ptrCast(fields.highest_cycle.?);
    const low_ptr: *volatile u64 = @ptrCast(fields.lowest_cycle.?);
    high_ptr.* = self.highest_cycle;
    low_ptr.* = self.lowest_cycle;

    // Atomically increment modcount
    const mod_ptr: *u64 = @ptrCast(@alignCast(fields.modcount.?));
    _ = @atomicRmw(u64, mod_ptr, .Add, 1, .seq_cst);

    log.info("bumped modcount", .{});
}
```

**Note on memory ordering:** We use `.seq_cst` (sequentially consistent) for
the atomic add to match the semantics of the x86 `lock; xaddl` instruction,
which provides a full memory barrier. The volatile writes to the cycle fields
ensure they are flushed before the modcount increment.

---

## 16. Concurrency Model

### CAS operation in Zig

The C code uses inline assembly for the CAS:

```c
static inline uint32_t lock_cmpxchgl(unsigned char *mem, uint32_t newval, uint32_t oldval) {
    __typeof (*mem) ret;
    __asm __volatile ("lock; cmpxchgl %2, %1"
    : "=a" (ret), "=m" (*mem)
    : "r" (newval), "m" (*mem), "0" (oldval));
    return (uint32_t) ret;
}
```

Note the C calling convention here: `lock_cmpxchgl(ptr, HD_UNALLOCATED, HD_WORKING)`
passes `newval=HD_UNALLOCATED=0x00000000` and `oldval=HD_WORKING=0x80000000`.
But looking at the actual call site (line 1192):

```c
uint32_t ret = lock_cmpxchgl(ptr, HD_UNALLOCATED, HD_WORKING);
```

And the check `if (ret == HD_UNALLOCATED)` — this means the function is called
with the parameters in an unusual order where the "expected" value is the
second parameter (`HD_UNALLOCATED`) and the "desired" value is the third
(`HD_WORKING`). Actually, re-reading the assembly: `cmpxchgl %2, %1` compares
`EAX` (loaded with `oldval` via `"0"(oldval)`) against `%1` (the memory),
and if equal, stores `%2` (`newval`) into memory. So `oldval=HD_WORKING` is
what we expect to find... wait.

Let's trace more carefully. The call is:
```c
lock_cmpxchgl(ptr, HD_UNALLOCATED, HD_WORKING)
```
So `newval = HD_UNALLOCATED = 0`, `oldval = HD_WORKING = 0x80000000`. The
inline asm loads `EAX = oldval = 0x80000000`, then `cmpxchgl` compares EAX
to `*mem`. If `*mem == 0x80000000`, store `newval = 0` into `*mem`.

But the check after is `if (ret == HD_UNALLOCATED)` meaning `if (ret == 0)`,
i.e., the **original** value in memory was 0 (unallocated). If `cmpxchgl`
found `*mem != EAX`, it loads `*mem` into `EAX` (the return value). So when
`*mem` is actually `0x00000000` (unallocated), the compare fails
(`0 != 0x80000000`), EAX gets `0`, and `ret == 0 == HD_UNALLOCATED`, which
the code treats as **success**.

**This means the CAS is actually NOT performing a swap when it "succeeds".**
The original memory remains `0x00000000`. The function is being used as a
**read-with-fence** — it atomically reads the header with the `lock` prefix
ensuring cache coherence. The actual "lock" of the header for writing is
done later by the `memcpy` of `HD_EOF` or the size header.

Re-examining the logic: the C code acquires the "write lock" conceptually
by checking the header is `HD_UNALLOCATED` via the CAS, then immediately
writes either EOF or the payload+header. The CAS serves as an atomic test —
if two writers race, only one sees `HD_UNALLOCATED` and proceeds. The other
sees `HD_WORKING` (or the size if the first writer already finished) and
retries.

Wait — actually, let me re-read. When `*mem == 0x00000000` and
`EAX == 0x80000000`: `cmpxchgl` compares, they're NOT equal, so it loads
`*mem` into EAX (ret = 0) and does NOT store. But the intent is clearly to
atomically set `HD_WORKING`...

Looking at the parameter order more carefully: the third parameter in the
inline asm maps to constraint `"0"(oldval)` which pre-loads EAX. The second
maps to `"r"(newval)` which is `%2`. So `cmpxchgl %2, %1` does:
`if (*mem == EAX) { *mem = %2; ZF=1; } else { EAX = *mem; ZF=0; }`.
With `EAX = HD_WORKING`, `%2 = HD_UNALLOCATED = 0`.

The function parameters are named confusingly. In the call:
`lock_cmpxchgl(ptr, HD_UNALLOCATED, HD_WORKING)` means
`newval = 0, oldval = 0x80000000`. The asm compares memory against
`0x80000000`. If memory is `0x80000000` (working), it writes `0`
(unallocated) — that's an unlock. If memory is `0` (unallocated), the
compare fails and ret = `0`.

**So the parameters are actually swapped at the call site.** The call should
logically be `lock_cmpxchgl(ptr, HD_WORKING, HD_UNALLOCATED)` to CAS from
unallocated to working. The swapped parameters mean the "CAS" as coded
actually tries to swap WORKING→UNALLOCATED, which fails when memory is
UNALLOCATED (the common case), returning the actual value `0x00000000`. The
code then checks `ret == HD_UNALLOCATED` and considers this success.

**The practical effect:** The CAS instruction itself doesn't modify memory on
"success". It functions as an atomic read. The write lock is then established
by the subsequent `memcpy` of the header. This works because:
1. Two concurrent callers both call the CAS
2. Both read `HD_UNALLOCATED` and proceed
3. But only one will successfully write the payload before the other re-reads
   and sees the modified header

This is actually a **race condition** in the C code — two writers could both
pass the CAS check and both write to the same slot. In practice, the race
window is tiny and the `mfence` after the CAS provides some serialisation,
but it's not truly safe for multiple concurrent writers.

**For the Zig reimplementation**, we should fix this and do a proper CAS:

```zig
fn cmpxchgHeader(ptr: *u32, expected: u32, desired: u32) ?u32 {
    return @cmpxchgStrong(u32, ptr, expected, desired, .seq_cst, .seq_cst);
}
```

`@cmpxchgStrong` returns `null` on success (the swap happened) or the actual
value on failure. This gives us a correct atomic lock:

```zig
// Attempt to acquire write lock: UNALLOCATED → WORKING
const header_ptr: *u32 = @ptrCast(@alignCast(ptr));
if (cmpxchgHeader(header_ptr, HD_UNALLOCATED, HD_WORKING)) |_| {
    // CAS failed — another writer got there first
    log.info("write lock failed, retrying", .{});
    std.time.sleep(1 * std.time.ns_per_s);
    continue;
}
// CAS succeeded — we own this slot
```

---

## 17. Complete Zig Code Sketch

Bringing it all together, here is the full `appendTs` function:

```zig
pub fn appendTs(self: *Queue, msg: *const anyopaque, ms: i64) !u64 {
    // ── 1. Size validation ───────────────────────────────────
    const write_sz: u32 = @intCast(self.append_sizeof.?(msg));
    if (write_sz == 0) return error.ZeroSizeMessage;
    if (write_sz > HD_MASK_LENGTH) return error.MessageTooLarge;
    while (write_sz > self.blocksize) {
        self.doubleBlocksize();
    }

    // ── 2. Refresh modcount ──────────────────────────────────
    self.peekQueueModcount();

    // ── 3. Lazy appender creation ────────────────────────────
    const appender = try self.ensureAppender();

    // ── 4. Write loop ────────────────────────────────────────
    while (true) {
        // Poll appender state (double poll for safety)
        _ = self.peekQueueTailer(appender);
        const r = self.peekQueueTailer(appender);

        switch (r) {
            // ── 4a. Need to create queuefile ─────────────────
            .awaiting_queuefile => {
                const qf_fn = appender.qf_fn orelse return error.NoFilename;
                const pid = std.os.linux.getpid();
                const tmp_name = try std.fmt.allocPrint(
                    self.allocator,
                    "{s}.{d}.tmp",
                    .{ qf_fn, pid },
                );
                defer self.allocator.free(tmp_name);

                try self.queuefileInit(tmp_name);

                std.fs.cwd().rename(tmp_name, qf_fn) catch {
                    std.time.sleep(1 * std.time.ns_per_s);
                    continue;
                };

                const cyc = appender.qf_index >> self.cycle_shift;
                if (cyc > self.highest_cycle) {
                    self.highest_cycle = cyc;
                    self.pokeQueueModcount();
                }
                continue;
            },

            // ── 4b. Need to extend queuefile ─────────────────
            .extend_fail => {
                const fd = appender.qf_fd orelse return error.NoFileDescriptor;
                const stat = try std.posix.fstat(fd);
                const extend_to = @as(u64, @intCast(stat.size)) + Queue.qf_disk_sz;

                const file = std.fs.File{ .handle = fd };
                file.seekTo(extend_to - 1) catch {
                    std.time.sleep(1 * std.time.ns_per_s);
                    continue;
                };
                file.writer().writeByte(0) catch {
                    std.time.sleep(1 * std.time.ns_per_s);
                    continue;
                };
                continue;
            },

            // ── 4c. Ready to write ───────────────────────────
            .awaiting_entry => {
                // Bounds check
                const offset_in_map = appender.qf_tip - appender.qf_mmapoff;
                if (offset_in_map + write_sz + 4 > appender.qf_mmapsz) {
                    log.err("write would exceed mmap buffer — aborting", .{});
                    return error.WriteWouldOverflow;
                }

                // Calculate pointer into mmap
                const base = appender.qf_buf orelse return error.NoMmapBuffer;
                const ptr: [*]u8 = base + offset_in_map;
                const header_ptr: *align(1) u32 = @ptrCast(ptr);

                // CAS: UNALLOCATED → WORKING
                if (@cmpxchgStrong(
                    u32,
                    header_ptr,
                    HD_UNALLOCATED,
                    HD_WORKING,
                    .seq_cst,
                    .seq_cst,
                )) |_| {
                    // CAS failed — another writer holds the slot
                    log.info("write lock failed, retrying", .{});
                    std.time.sleep(1 * std.time.ns_per_s);
                    continue;
                }

                // CAS succeeded — we own this slot.
                // Full memory fence (implicit in seq_cst above, but explicit
                // for clarity matching the C code's mfence)
                @fence(.seq_cst);

                // Check if we need to roll to a new cycle
                if (ms > 0) {
                    const cyc = self.cycleFromMs(ms);
                    const current_cycle = appender.qf_index >> self.cycle_shift;
                    if (cyc > current_cycle) {
                        log.info("cycle roll: current {} proposed {}", .{ current_cycle, cyc });
                        appender.qf_index = cyc << self.cycle_shift;

                        // Write EOF to trigger roll
                        const eof: u32 = HD_EOF;
                        @as(*align(1) u32, @ptrCast(ptr)).* = eof;
                        continue;
                    }
                }

                // Check if we're behind highest_cycle (patch EOF)
                if (appender.qf_index < self.highest_cycle << self.cycle_shift) {
                    log.info("patching EOF on stale cycle file", .{});
                    const eof: u32 = HD_EOF;
                    @as(*align(1) u32, @ptrCast(ptr)).* = eof;
                    continue;
                }

                // ── Write the payload ────────────────────────
                self.append_write.?(ptr + 4, msg, write_sz);

                // Fence: ensure payload is visible before header
                @fence(.seq_cst);

                // ── Publish: write size header (releases lock) ─
                const size_header: u32 = write_sz & HD_MASK_LENGTH;
                @as(*align(1) u32, @ptrCast(ptr)).* = size_header;

                log.debug("wrote {} bytes at index {}", .{ write_sz, appender.qf_index });
                break;
            },

            // ── 4d. Any other state — back off ───────────────
            else => {
                log.warn("cannot write in state {}, sleeping", .{@intFromEnum(r)});
                std.time.sleep(1 * std.time.ns_per_s);
                continue;
            },
        }
    }

    return appender.qf_index;
}
```

---

## 18. Testing Notes

### Unit tests

```zig
test "size validation rejects oversized messages" {
    var queue = try Queue.init(testing.allocator, "/tmp/test");
    defer queue.deinit();

    // Mock a sizeof that returns > 30 bits
    queue.append_sizeof = struct {
        fn f(_: *const anyopaque) usize {
            return 0x40000000; // exceeds HD_MASK_LENGTH
        }
    }.f;

    const result = queue.appendTs(undefined, 0);
    try testing.expectError(error.MessageTooLarge, result);
}

test "blocksize doubles to fit large messages" {
    var queue = try Queue.init(testing.allocator, "/tmp/test");
    defer queue.deinit();

    try testing.expectEqual(@as(u32, 1 << 20), queue.blocksize);

    // Simulate needing to double
    while (@as(u32, 2 << 20) > queue.blocksize) {
        queue.doubleBlocksize();
    }
    try testing.expectEqual(@as(u32, 2 << 20), queue.blocksize);
}
```

### Integration tests

1. **Single-writer append:** Create a queue, append 100 messages, verify
   they can be read back with a tailer in the correct order.
2. **Cycle roll:** Use `TEST_SECONDLY` roll scheme, append messages spanning
   multiple seconds, verify multiple `.cq4` files are created with proper
   EOF markers.
3. **File extension:** Append enough data to exceed the initial 83 MB file
   size, verify the file is extended and writes continue.
4. **Interop:** Create a queue with Zig, read it with Java Chronicle Queue
   (and vice versa).
5. **Multi-writer stress test:** Two writer processes appending simultaneously
   to validate CAS correctness (expect no data corruption or lost messages).

### Debugging aids

The C code uses `chronicle_debug_tailer` to dump appender state. The Zig
equivalent should implement `std.fmt.format` for the `Tailer` struct:

```zig
pub fn format(self: *const Tailer, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
    _ = fmt;
    _ = options;
    try writer.print("Tailer{{ state={}, cycle={}, seqnum={}, tip={}, fd={} }}", .{
        @intFromEnum(self.state),
        self.qf_index >> self.queue.cycle_shift,
        self.qf_index & self.queue.seqnum_mask,
        self.qf_tip,
        self.qf_fd,
    });
}
```

---

## Summary of C → Zig Mapping

| C function | Zig method | Key changes |
|---|---|---|
| `chronicle_append` | `Queue.append` | Thin wrapper, same logic |
| `chronicle_append_ts` | `Queue.appendTs` | Error unions, proper CAS, `switch` instead of `if` chain |
| `chronicle_clock_ms` | `Queue.clockMs` | `std.time.milliTimestamp()` |
| `chronicle_cycle_from_ms` | `Queue.cycleFromMs` | Typed arithmetic |
| `poke_queue_modcount` | `Queue.pokeQueueModcount` | `@atomicRmw` instead of inline asm |
| `peek_queue_modcount` | `Queue.peekQueueModcount` | `volatile` reads instead of `memcpy` |
| `lock_cmpxchgl` | `@cmpxchgStrong` | Correct CAS semantics (fixes C race) |
| `lock_xadd` | `@atomicRmw(.Add)` | Standard Zig atomic |
| `queuefile_init` | `Queue.queuefileInit` | `std.fs.File` API |
| `queue_double_blocksize` | `Queue.doubleBlocksize` | Same logic |