# brz-queue Documentation

## Overview

This documentation set provides a comprehensive reference for **brz-queue**, a high-performance, lock-free, memory-mapped IPC queue implemented in [Zig](https://ziglang.org/). brz-queue is a **clean-room implementation** — it is not based on or compatible with any existing queue protocol.

### Design Goals

- **Zero allocations on the hot path** — all message writes and reads operate directly on memory-mapped regions with no heap allocation.
- **Lock-free single-writer / multi-reader** — a single appender thread uses atomic CAS on a 4-byte message header; multiple tailer threads read independently with no coordination or per-process locking.
- **Fixed-layout on-disk format** — file headers and metadata are fixed `extern struct` layouts readable by casting an mmap pointer. No self-describing wire protocol.
- **io_uring integration** — tailer wakeup uses io_uring for near-zero latency notification with zero CPU waste during idle periods.
- **Minimal syscall overhead** — `MAP_POPULATE`, `madvise(MADV_SEQUENTIAL)`, huge page support, pre-mapped windows, `fallocate`-based preallocation, and pre-roll file creation eliminate jitter from the hot path.

### Threading Model

- **Appender**: Single writer thread. The appender is **not** thread-safe — only one thread may append at a time.
- **Tailers**: Multiple reader threads. Each tailer is independently thread-safe with its own mmap window and state. No inter-reader coordination is needed.

---

## Document Map

### Architecture & Reference

| # | Document | Description |
|---|----------|-------------|
| 01 | [Architecture Overview](01-architecture-overview.md) | Comprehensive architecture of brz-queue: fixed-layout on-disk format, CAS write protocol with tiered backoff, acquire/release memory ordering, flat inline index, roll cycle mechanism with pre-roll file creation, io_uring tailer wakeup, and concurrency model. |

### Implementation Tasks

These documents are ordered by dependency — each task builds on concepts from prior tasks, though most can be implemented and tested independently.

| # | Document | Task | Dependencies |
|---|----------|------|--------------|
| 02 | [File Format Specification](02-file-format.md) | **Reference:** Complete specification of the on-disk format — fixed `extern struct` metadata and queue file headers, 4-byte message headers, flat inline u64 index array, alignment rules, and roll file naming. No BinaryWire. | — |
| 03 | [Core Types and Constants](03-zig-task-core-types.md) | **Task 1:** Define all constants, enums, structs (`Queue`, `Tailer`, `Appender`, `RollScheme`, `Collected`, `IoUringContext`), error types, and cycle/index arithmetic. | None |
| 04 | [Serialization & Codec Interface](04-zig-task-codec-interface.md) | **Task 2:** Define the minimal user-payload codec interface — a comptime-generic serialization/deserialization contract for encoding user messages into the queue's data region. No wire protocol internals. | Task 1 (types) |
| 05 | [Memory-Mapped File I/O and Atomic Operations](05-zig-task-memory-mapping.md) | **Task 3:** Implement mmap/munmap wrappers with `MAP_POPULATE` (appender) and `madvise(MADV_SEQUENTIAL)` (tailer), configurable block sizes, `MAP_HUGETLB` support, pre-mapping of the next window, atomic CAS (`cmpxchgStrong`), atomic store/load with acquire/release ordering, and `fallocate`-based file preallocation. | Task 1 (types) |
| 06 | [Queue Lifecycle Management](06-zig-task-queue-lifecycle.md) | **Task 4:** Implement queue initialization, directory listing creation/parsing, queue file creation with `fallocate`, pre-roll file creation (detect pre-roll window, pre-allocate next cycle file before roll deadline), metadata reading via `extern struct` pointer cast, open/close lifecycle. | Tasks 1, 2, 3 |
| 07 | [Appender (Writer) Implementation](07-zig-task-appender.md) | **Task 5:** Implement the full append logic including single-writer design, the CAS write loop with tiered backoff (spin with pause hint 0–64 iters / ~0–2μs, yield 64–256 iters, exponential backoff capped at 1ms), flat inline index updates, cycle rolling via pointer swap (no syscalls on roll), and EOF patching. | Tasks 1–4 |
| 08 | [Tailer (Reader) Implementation](08-zig-task-tailer.md) | **Task 6:** Implement tailer creation/polling, the read state machine, io_uring-based wakeup (replacing spin-loop with progressive sleep), binary search over the flat inline index for O(log n) seeking, per-tailer mmap window management, and multi-reader safety. | Tasks 1–4 |
| 09 | [Public API](09-zig-task-public-api.md) | **Task 7:** Design the public Zig API using comptime generics, expose queue/appender/tailer creation, the codec interface, lifecycle management, and example programs. | Tasks 1–6 |
| 10 | [Testing and Validation Strategy](10-zig-task-testing.md) | **Task 8:** Implement unit tests for all modules, byte-level format tests against the file format spec, single-writer/multi-reader concurrency tests, latency benchmarks, io_uring integration tests, and fuzz testing for the codec interface and message header parsing. | Tasks 1–7 |

---

## Dependency Graph

```text
Task 1: Core Types ─────────┬──────────────────────────────────────┐
                             │                                      │
Task 2: Codec Interface ─────┤                                      │
                             │                                      │
Task 3: Memory/Atomics ──────┤                                      │
                             │                                      │
                             ▼                                      │
                   Task 4: Queue Lifecycle                          │
                             │                                      │
                    ┌────────┴────────┐                             │
                    ▼                 ▼                              │
          Task 5: Appender    Task 6: Tailer                        │
            (CAS backoff,      (io_uring,                           │
             inline index)      binary search)                      │
                    │                 │                              │
                    └────────┬────────┘                             │
                             ▼                                      │
                   Task 7: Public API ◄─────────────────────────────┘
                             │
                             ▼
                   Task 8: Testing
```

Tasks 5 (Appender) and 6 (Tailer) can be developed **in parallel** since they operate on disjoint write logic vs read logic, though both depend on the queue lifecycle (Task 4).

### Key Dependencies

- **io_uring** — Linux 5.1+ kernel required. Used by the tailer for efficient wakeup notification. Falls back to polling if unavailable.
- **Codec Interface** — Minimal and decoupled. User-defined serialization plugs in via comptime generics; the core queue is codec-agnostic.
- **Memory/Atomics** — The foundation for both appender and tailer. All atomic operations use **acquire/release** ordering (not SeqCst) — on x86-TSO, acquire fences are free.

---

## Suggested Implementation Order

For a fresh implementation, the recommended approach is:

1. **Start with Task 1** (Core Types) — this establishes the foundational data model and can be fully unit-tested in isolation. Define the fixed `extern struct` layouts for metadata and queue file headers here.

2. **Implement Task 2** (Codec Interface) — define the comptime-generic codec contract. This is a thin interface and can be tested with trivial codecs (e.g., raw bytes, length-prefixed strings).

3. **Implement Task 3** (Memory/Atomics) — mmap wrappers with `MAP_POPULATE` and `MADV_SEQUENTIAL` support, `fallocate`-based preallocation, huge page configuration, pre-mapping logic, and acquire/release atomics. Test with simple files before integrating with queue logic.

4. **Implement Task 4** (Queue Lifecycle) — this combines the first three tasks. Test by creating a queue directory, verifying the metadata file is readable via pointer cast to the `extern struct`, and that `fallocate` correctly preallocates queue files. Implement pre-roll file creation here.

5. **Implement Task 6 first** (Tailer/Reader) — reading is simpler than writing. Test against queue files you create manually or with a minimal test writer. Implement io_uring wakeup and binary search over the flat index.

6. **Implement Task 5** (Appender/Writer) — implement the single-writer CAS loop with tiered backoff, flat inline index updates, and cycle rolling. Verify by reading back with the tailer.

7. **Implement Task 7** (Public API) — design the ergonomic Zig interface with comptime generics.

8. **Complete Task 8** (Testing) — fill in the full test matrix, concurrency stress tests, latency benchmarks, and fuzzing.

---

## Key Protocol Facts (Quick Reference)

| Property | Value |
|----------|-------|
| Max message size | 1,073,741,823 bytes (~1 GiB, 30-bit length) |
| Message header size | 4 bytes (WORKING bit + 30-bit length; no PID encoding) |
| Default block size | 1 MiB (1,048,576 bytes) |
| Metadata format | Fixed `extern struct`, read via mmap pointer cast |
| Queue file headers | Fixed `extern struct`, read via mmap pointer cast |
| Index format | Flat inline array of u64 offsets in file header region |
| Index lookup | O(log n) binary search |
| Index layout | cycle_shift=32, seqnum in lower 32 bits |
| Write arbitration | x86 `lock cmpxchgl` (atomic CAS) with tiered backoff |
| CAS backoff tiers | Spin+pause (0–64 iters, ~0–2μs) → yield (64–256) → exp. backoff (cap 1ms) |
| Memory ordering | Acquire/Release (not SeqCst); acquire fences are free on x86-TSO |
| Tailer wakeup | io_uring-based notification (fallback: polling) |
| Appender mmap flags | `MAP_POPULATE` (prefault pages) |
| Tailer mmap hints | `madvise(MADV_SEQUENTIAL)` |
| Huge page support | `MAP_HUGETLB` (configurable) |
| File preallocation | `fallocate` (avoids filesystem block allocation faults) |
| Pre-roll | Next cycle file pre-created before roll deadline; roll swaps pointers only |
| Pre-mapping | Next mmap window mapped before current window is exhausted |
| Data alignment | 4-byte aligned (padding after each message) |
| Threading model | Single writer, multiple independent readers |
| Appender thread safety | NOT thread-safe (single writer only) |
| Tailer thread safety | Independently thread-safe (own mmap window and state) |
| Typical IPC latency | Sub-microsecond (architecture dependent) |

---

## Architectural Decisions

The following design choices distinguish brz-queue from traditional shared-memory queue implementations:

1. **Fixed-layout headers over self-describing wire** — Metadata and queue file headers are fixed `extern struct` layouts. Reading them is a pointer cast, not a parse. This eliminates the entire wire protocol module from the critical path.

2. **Simplified message header** — The 4-byte header retains the WORKING bit and 30-bit length field, but the WORKING state no longer encodes a PID. The PID was decorative in practice — recovery never used it.

3. **Acquire/Release over SeqCst** — All atomic operations use acquire/release ordering. On x86-TSO, acquire fences compile to zero instructions, saving ~33ns per message on the read path.

4. **Tiered CAS backoff** — The CAS retry loop uses spin-with-pause (0–64 iterations, ~0–2μs), then yield (64–256 iterations), then exponential backoff capped at 1ms. This replaces a catastrophic 1-second flat sleep.

5. **Flat inline index** — A flat array of u64 offsets lives at known positions in the file header region. No separate index metadata messages. The appender updates entries atomically; readers binary-search in O(log n) instead of scanning in O(n).

6. **Eliminated mmap jitter** — `MAP_POPULATE` on appender windows prefaults pages. `madvise(MADV_SEQUENTIAL)` on tailer windows enables kernel read-ahead. Larger block sizes and `MAP_HUGETLB` reduce TLB pressure. Pre-mapping the next window before it's needed eliminates mmap syscalls from the hot path.

7. **Pre-roll file creation** — When the appender detects it is within the pre-roll window, it pre-allocates the next cycle file. The actual roll just swaps pointers — zero syscalls at the roll boundary.

8. **`fallocate` over `lseek`+`write`** — Queue files are preallocated with `fallocate`, which actually allocates disk blocks and eliminates filesystem block allocation faults on first write.

9. **io_uring tailer wakeup** — Tailers use io_uring-based notification instead of spin-loops with progressive sleep. This provides near-zero wakeup latency with zero CPU waste during idle periods.

---

*brz-queue — zero-allocation, lock-free, memory-mapped IPC for Zig.*