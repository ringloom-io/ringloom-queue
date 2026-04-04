# libchronicle Documentation

## Overview

This documentation set provides a comprehensive reference for **libchronicle**, an open-source C implementation of the [OpenHFT Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) shared-memory IPC protocol. It is designed to serve two purposes:

1. **Architectural reference** — Understanding the design, data structures, file formats, and concurrency model of the chronicle-queue protocol and this implementation.
2. **Zig reimplementation guide** — A series of self-contained task documents with detailed instructions for reimplementing the library in the [Zig programming language](https://ziglang.org/).

---

## Document Map

### Architecture & Reference

| # | Document | Description |
|---|----------|-------------|
| 01 | [Architecture Overview](01-architecture-overview.md) | Comprehensive architecture of the queue implementation including high-level design, data structures, file formats, memory-mapped I/O strategy, CAS write protocol, roll cycle mechanism, wire serialization, concurrency model, and detailed ASCII diagrams. |

### Zig Reimplementation Tasks

These documents are ordered by dependency — each task builds on concepts from prior tasks, though most can be implemented and tested independently.

| # | Document | Task | Dependencies |
|---|----------|------|--------------|
| 03 | [Core Types and Constants](03-zig-task-core-types.md) | **Task 1:** Define all constants, enums, structs (`Queue`, `Tailer`, `RollScheme`, `Collected`), function pointer type equivalents, error types, and Java date format conversion logic. | None |
| 04 | [BinaryWire Serialization Protocol](04-zig-task-wire-protocol.md) | **Task 2:** Implement the BinaryWire parser (`wire_parse`) and writer (`wirepad_t`), including all control byte handling, stop-bit encoding, nesting, and integration helpers. | Task 1 (types) |
| 05 | [Memory-Mapped File I/O and Atomic Operations](05-zig-task-memory-mapping.md) | **Task 3:** Implement mmap/munmap wrappers, blocksize-based windowing, atomic CAS (`cmpxchgStrong`), atomic add, memory fences, and file extension logic. | Task 1 (types) |
| 06 | [Queue Lifecycle Management](06-zig-task-queue-lifecycle.md) | **Task 4:** Implement queue initialization, version detection, directory listing creation/parsing, queue file creation, configuration setters, open/close lifecycle. | Tasks 1, 2, 3 |
| 07 | [Appender (Writer) Implementation](07-zig-task-appender.md) | **Task 5:** Implement the full append logic including lazy appender creation, the write loop state machine, CAS locking, cycle rolling, EOF patching, and file extension. | Tasks 1–4 |
| 08 | [Tailer (Reader) Implementation](08-zig-task-tailer.md) | **Task 6:** Implement tailer creation/polling, the `peekQueueTailerR` state machine, `parseQueueBlock` inner loop, data dispatch, collect API, and modcount polling. | Tasks 1–4 |
| 09 | [Public API and Language Bindings](09-zig-task-public-api.md) | **Task 7:** Design the public Zig API using comptime generics, expose a C ABI compatibility layer for Python/kdb bindings, implement debug output and example programs. | Tasks 1–6 |
| 10 | [Testing and Validation Strategy](10-zig-task-testing.md) | **Task 8:** Port all C test cases, set up wire protocol byte-level tests, Java interop tests, fuzzing, and the full test matrix. | Tasks 1–7 |

---

## Dependency Graph

```text
Task 1: Core Types ─────────┬──────────────────────────────────────┐
                             │                                      │
Task 2: Wire Protocol ───────┤                                      │
                             │                                      │
Task 3: Memory/Atomics ──────┤                                      │
                             │                                      │
                             ▼                                      │
                   Task 4: Queue Lifecycle                          │
                             │                                      │
                    ┌────────┴────────┐                             │
                    ▼                 ▼                              │
          Task 5: Appender    Task 6: Tailer                        │
                    │                 │                              │
                    └────────┬────────┘                             │
                             ▼                                      │
                   Task 7: Public API ◄─────────────────────────────┘
                             │
                             ▼
                   Task 8: Testing
```

Tasks 5 (Appender) and 6 (Tailer) can be developed **in parallel** since they operate on disjoint write logic vs read logic, though both depend on the queue lifecycle (Task 4).

---

## Suggested Implementation Order

For a fresh Zig reimplementation, the recommended approach is:

1. **Start with Task 1** (Core Types) — this establishes the foundational data model and can be fully unit-tested in isolation.

2. **Implement Task 2** (Wire Protocol) — the BinaryWire format is needed for reading and writing metadata. This is self-contained and thoroughly testable with byte-level comparisons.

3. **Implement Task 3** (Memory/Atomics) — mmap wrappers and atomic operations. Test with simple files before integrating with queue logic.

4. **Implement Task 4** (Queue Lifecycle) — this combines the first three tasks. Test by creating a queue directory and verifying the metadata.cq4t file is byte-compatible with Java's output.

5. **Implement Task 6 first** (Tailer/Reader) — reading is simpler than writing. Test against the Java-written sample queue files (cqv5-sample-input.tar.bz2).

6. **Implement Task 5** (Appender/Writer) — once you can read, implement writing and verify by reading back what you wrote.

7. **Implement Task 7** (Public API) — design the ergonomic Zig interface and C ABI layer.

8. **Complete Task 8** (Testing) — fill in the full test matrix, fuzzing, and cross-compatibility validation.

---

## Source Code Reference

The C implementation consists of the following source files in `native/`:

| File | Lines | Purpose |
|------|-------|---------|
| `libchronicle.h` | ~110 | Public API header — all types, constants, and function declarations |
| `libchronicle.c` | ~1512 | Core implementation — queue, appender, tailer, mmap, CAS, roll logic |
| `wire.h` | ~95 | Wire protocol header — parser callbacks and writer declarations |
| `wire.c` | ~495 | Wire protocol implementation — BinaryWire parser and wirepad writer |
| `buffer.h` | ~20 | Debug utility header |
| `buffer.c` | ~100 | Debug hex dump formatting |
| `shmmain.c` | ~115 | Command-line tool (read/write/follow) |
| `shm_example_writer.c` | ~30 | Example: stdin → queue writer |
| `shm_example_reader.c` | ~45 | Example: queue → stdout reader |
| `fuzzmain.c` | ~195 | AFL fuzzing harness |

Test files in `native/test/`:

| File | Purpose |
|------|---------|
| `test_queue.c` | Queue lifecycle, v5 read, write, roll scheme tests |
| `test_wire.c` | Wire protocol serialization/deserialization tests |
| `test_buffer.c` | Buffer formatting tests |
| `testdata.h` | Test data extraction utilities (tar.bz2 unpacking) |
| `cqv5-sample-input.tar.bz2` | v5 queue written by Java Chronicle Queue |

---

## Key Protocol Facts (Quick Reference)

| Property | Value |
|----------|-------|
| Max message size | 1,073,741,823 bytes (~1 GiB, 30-bit length) |
| Header size | 4 bytes |
| Default block size | 1 MiB (1,048,576 bytes) |
| Default file size | ~83.7 MB (83,754,496 bytes) |
| Index layout | cycle_shift=32, seqnum in lower 32 bits |
| Typical IPC latency | ~1 μs (polling dependent) |
| Arbitration | x86 `lock cmpxchgl` (atomic CAS) |
| Read barrier | x86 `mfence` |
| Modcount update | x86 `lock xaddl` (atomic fetch-add) |
| v5 data alignment | 4-byte aligned (padding after each message) |
| Patch cycles | 3 (EOF lookback window) |

---

*Generated from libchronicle source code analysis. See also: [CHANGES.md](../CHANGES.md) for protocol version history.*