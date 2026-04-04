# Chronicle Queue File Format Specification

This document provides a byte-level specification of the chronicle-queue on-disk format as implemented by libchronicle. It covers the v5 format and is intended to be sufficient for writing a compatible implementation from scratch.

---

## Table of Contents

1. [Overview](#overview)
2. [Terminology](#terminology)
3. [Queue Directory Layout](#queue-directory-layout)
4. [Message Framing (4-Byte Header)](#message-framing-4-byte-header)
5. [Queue File (.cq4) Format](#queue-file-cq4-format)
6. [Directory Listing File (.cq4t) Format](#directory-listing-file-cq4t-format)
7. [BinaryWire Encoding Reference](#binarywire-encoding-reference)
8. [64-Bit Index Layout](#64-bit-index-layout)
9. [Roll Schemes](#roll-schemes)
10. [Index Structures (Index2Index and Index Pages)](#index-structures-index2index-and-index-pages)
11. [Byte-Level Walkthroughs](#byte-level-walkthroughs)
12. [Endianness and Alignment](#endianness-and-alignment)
13. [Invariants and Constraints](#invariants-and-constraints)

---

## Overview

A chronicle-queue is a persistent, memory-mapped message queue stored entirely within a single filesystem directory. The format is designed so that:

- Multiple processes can read and write concurrently using only `mmap` and atomic CPU instructions
- No broker or coordinator process is required
- Files are machine-independent and can be copied between systems
- The operating system kernel handles persistence (dirty page writeback)

The format consists of:
1. **One directory listing / metadata file** (`.cq4t`) — shared state for cycle tracking
2. **Zero or more queue data files** (`.cq4`) — message storage, one per cycle period

## Terminology

| Term | Definition |
|------|-----------|
| **Queue** | A directory on disk containing `.cq4t` and `.cq4` files |
| **Cycle** | A time period (e.g., one day, one hour) corresponding to one `.cq4` file |
| **Seqnum** | The sequential message number within a single cycle/file |
| **Index** | A 64-bit value combining cycle and seqnum |
| **Appender** | A process writing messages to the queue |
| **Tailer** | A process reading messages from the queue |
| **Roll** | The act of closing one cycle's file and starting the next |
| **BinaryWire** | The self-describing serialization format used for metadata |
| **Header** | The 4-byte framing word preceding every message |

## Queue Directory Layout

```
queue_directory/
│
├── metadata.cq4t              # directory listing (roll config + shared counters)
│
├── 20231115F.cq4              # Queue data file for one cycle (FAST_DAILY example)
├── 20231116F.cq4              # Next cycle
├── 20231117F.cq4              # ...
└── ...
```

### Filename Convention

Queue data filenames are derived from the cycle number using the roll scheme's date format:

```
filename = strftime(cycle * roll_length_secs, roll_format) + ".cq4"
```

Examples for different schemes:

| Scheme | Cycle 0 Filename | Cycle 1 Filename |
|--------|-----------------|-----------------|
| `DAILY` (`yyyyMMdd`) | `19700101.cq4` | `19700102.cq4` |
| `FAST_DAILY` (`yyyyMMdd'F'`) | `19700101F.cq4` | `19700102F.cq4` |
| `FAST_HOURLY` (`yyyyMMdd-HH'F'`) | `19700101-00F.cq4` | `19700101-01F.cq4` |
| `FIVE_MINUTELY` (`yyyyMMdd-HHmm'V'`) | `19700101-0000V.cq4` | `19700101-0005V.cq4` |
| `TEST_SECONDLY` (`yyyyMMdd-HHmmss'T'`) | `19700101-000000T.cq4` | `19700101-000001T.cq4` |

The time represented is UTC. Cycle 0 corresponds to the Unix epoch (1970-01-01T00:00:00Z) plus the `roll_epoch` offset.

---

## Message Framing (4-Byte Header)

Every logical entry in a `.cq4` or `.cq4t` file is preceded by a 4-byte little-endian header word. This header is the fundamental unit of the protocol and serves dual purpose: message framing and write arbitration.

### Header Word Layout

```
Bit:  31  30  29                              0
     ┌───┬───┬──────────────────────────────────┐
     │ W │ M │          SIZE / PID               │
     └───┴───┴──────────────────────────────────┘

W = Working bit (bit 31)
M = Metadata bit (bit 30)
SIZE/PID = 30-bit payload (bits 0-29)
```

### Header Types

| W | M | Constant | Hex Value | Meaning |
|---|---|----------|-----------|---------|
| 0 | 0 | `HD_UNALLOCATED` | `0x00000000` | Unallocated space (when SIZE=0) |
| 0 | 0 | _(data)_ | `0x00000001–0x3FFFFFFF` | Data message; SIZE = payload byte count |
| 0 | 1 | `HD_METADATA` | `0x40000000 \| size` | Metadata message; SIZE = payload byte count |
| 1 | 0 | `HD_WORKING` | `0x80000000 \| pid` | Write lock held; PID = writer's process ID (low 30 bits) |
| 1 | 1 | `HD_EOF` | `0xC0000000` | End of file marker |

### Bitmask Constants

```
HD_UNALLOCATED = 0x00000000
HD_WORKING     = 0x80000000
HD_METADATA    = 0x40000000
HD_EOF         = 0xC0000000
HD_MASK_LENGTH = 0x3FFFFFFF    (extracts size/pid from lower 30 bits)
HD_MASK_META   = 0xC0000000    (extracts type from upper 2 bits)
```

### Constraints

- Maximum payload size: `0x3FFFFFFF` = 1,073,741,823 bytes (≈ 1 GiB)
- Payload size MUST be positive for data and metadata messages
- `HD_UNALLOCATED` is defined as the full 32-bit zero value
- `HD_EOF` is the specific value `0xC0000000` (SIZE field is zero)

### Message Layout in File

**Message layout (4-byte aligned):**
```
Offset  Content
+0      [4 bytes] Header word (little-endian uint32)
+4      [N bytes] Payload data
+4+N    [P bytes] Zero padding where P = (-N) & 0x03
+4+N+P  [4 bytes] Next header word (now 4-byte aligned)
```

The padding formula ensures that the next header always starts at a 4-byte aligned offset, which is important for the correctness of atomic CAS operations on the header.

---

## Queue File (.cq4) Format

A `.cq4` file stores the actual message data for one cycle. Its structure is a linear sequence of framed messages (using the 4-byte header protocol described above).

### Overall Structure

```
Offset 0x0000:
┌────────────────────────────────────────────────────────────┐
│ METADATA: Queue file header                                 │
│   (BinaryWire-encoded roll config, index config)            │
│   Header: 0x4000XXXX where XXXX = size                      │
├────────────────────────────────────────────────────────────┤
│ METADATA: Index2Index page (optional)                       │
│   (I64_ARRAY of byte offsets to index pages)                │
│   Header: 0x4000XXXX                                        │
├────────────────────────────────────────────────────────────┤
│ METADATA: Index page 0 (optional)                           │
│   (I64_ARRAY of byte offsets to data messages)              │
│   Header: 0x4000XXXX                                        │
├────────────────────────────────────────────────────────────┤
│ DATA: Message seqnum=0                                      │
│   Header: 0x000000XX (XX = size)                            │
│   Payload: user bytes                                       │
├────────────────────────────────────────────────────────────┤
│ DATA: Message seqnum=1                                      │
│   ...                                                       │
├────────────────────────────────────────────────────────────┤
│ ... more data and index pages interleaved ...               │
├────────────────────────────────────────────────────────────┤
│ EOF: End of file marker (when cycle is rolled)              │
│   Header: 0xC0000000                                        │
├────────────────────────────────────────────────────────────┤
│ UNALLOCATED: Zero bytes (pre-allocated file space)          │
│   0x00000000 0x00000000 ...                                 │
└────────────────────────────────────────────────────────────┘
```

### Queue File Header

The queue file header is minimal or absent because roll configuration is stored in `metadata.cq4t`. New queue files created by libchronicle do not currently write any header metadata (this is a known TODO).

### Initial File Size

Queue files are pre-allocated at creation time. libchronicle creates files of `83,754,496` bytes (≈ 79.9 MiB) filled with zeros. When the file runs low on space (fewer than 2 × blocksize bytes remaining), it is extended by another 83,754,496 bytes.

---

## Directory Listing File (.cq4t) Format

### Purpose

The directory listing file serves as shared state between all appenders and tailers on the same machine. It is memory-mapped (`MAP_SHARED`) by every process and contains:

1. Queue configuration (roll scheme details)
2. Live counters that are updated atomically through the shared mapping

### Filename

The directory listing file is named `metadata.cq4t`.

### Structure

The file consists of **one metadata message** followed by **six data messages**, all using the standard 4-byte header framing.

#### Message 1: Header (METADATA)

```
Header: 0x4000XXXX (metadata, size = XXXX)
Payload (BinaryWire):
  event_name "header"
  type_prefix "STStore"
  nested {                                      # BYTES_LENGTH32
    field "wireType"
    type_prefix "WireType"
    text "BINARY_LIGHT"
    
    field "metadata"
    type_prefix "SCQMeta"
    nested {                                    # BYTES_LENGTH32
      field "roll"
      type_prefix "SCQSRoll"
      nested {                                  # BYTES_LENGTH32
        field "length" = varint (roll period ms, e.g. 86400000 for FAST_DAILY)
        field "format" = text (Java date format, e.g. "yyyyMMdd'F'")
        field "epoch"  = varint (epoch offset, typically 0)
      }
      field "deltaCheckpointInterval" = varint (e.g. 64)
      field "sourceId" = varint (e.g. 0)
    }
    padding to 8-byte boundary (0x8F bytes)
  }
```

#### Messages 2–7: Shared Fields (DATA)

Each is a data message (not metadata) containing an event name and an 8-byte aligned `INT64` value:

| # | Event Name | Value | Purpose |
|---|-----------|-------|---------|
| 2 | `listing.highestCycle` | uint64 | Highest active cycle number |
| 3 | `listing.lowestCycle` | uint64 | Lowest active cycle number |
| 4 | `listing.modCount` | uint64 | Modification counter (atomically incremented) |
| 5 | `chronicle.write.lock` | uint64 | Write lock (`0x8000000000000000` = unlocked) |
| 6 | `chronicle.lastIndexReplicated` | uint64 | Last replicated index (`-1` = none) |
| 7 | `chronicle.lastAcknowledgedIndexReplicated` | uint64 | Last ack'd replicated index (`-1` = none) |

#### Alignment Requirement

The `uint64` values in messages 2–7 MUST be 8-byte aligned within the file because they are accessed directly through the memory mapping by multiple processes. The `wirepad_uint64_aligned` function ensures this by inserting padding bytes (`0x8F` single-byte padding or `0x8E` multi-byte padding) before the `0xA7` (INT64) control byte.

The alignment rule: the `0xA7` byte must be at offset `(8n - 1)` from the start of the file/mapping, so that the 8 data bytes that follow it start at offset `8n`.

### Complete Byte Dump Example (v5 FAST_DAILY)

```
Offset   Hex                                       ASCII
──────── ──────────────────────────────────────── ────────────────
00000000 ac 00 00 40 b9 06 68 65  61 64 65 72 b6 07 53 54 ...@..he ader..ST
00000010 53 74 6f 72 65 82 96 00  00 00 c8 77 69 72 65 54 Store... ...wireT
00000020 79 70 65 b6 08 57 69 72  65 54 79 70 65 ec 42 49 ype..Wir eType.BI
00000030 4e 41 52 59 5f 4c 49 47  48 54 c8 6d 65 74 61 64 NARY_LIG HT.metad
00000040 61 74 61 b6 07 53 43 51  4d 65 74 61 82 5d 00 00 ata..SCQ Meta.]..
00000050 00 c4 72 6f 6c 6c b6 08  53 43 51 53 52 6f 6c 6c ..roll.. SCQSRoll
00000060 82 26 00 00 00 c6 6c 65  6e 67 74 68 a6 00 5c 26 .&....le ngth..\&
00000070 05 c6 66 6f 72 6d 61 74  eb 79 79 79 79 4d 4d 64 ..format .yyyyMMd
00000080 64 27 46 27 c5 65 70 6f  63 68 00 d7 64 65 6c 74 d'F'.epo ch..delt
00000090 61 43 68 65 63 6b 70 6f  69 6e 74 49 6e 74 65 72 aCheckpo intInter
000000a0 76 61 6c 40 c8 73 6f 75  72 63 65 49 64 00 8f 8f val@.sou rceId...
000000b0 24 00 00 00 b9 14 6c 69  73 74 69 6e 67 2e 68 69 $.....li sting.hi
000000c0 67 68 65 73 74 43 79 63  6c 65 8e 00 00 00 00 a7 ghestCyc le......
000000d0 fd 49 00 00 00 00 00 00  24 00 00 00 b9 13 6c 69 .I...... $.....li
000000e0 73 74 69 6e 67 2e 6c 6f  77 65 73 74 43 79 63 6c sting.lo westCycl
000000f0 65 8e 01 00 00 00 00 a7  fd 49 00 00 00 00 00 00 e....... .I......
00000100 1c 00 00 00 b9 10 6c 69  73 74 69 6e 67 2e 6d 6f ......li sting.mo
00000110 64 43 6f 75 6e 74 8f a7  01 00 00 00 00 00 00 00 dCount.. ........
00000120 24 00 00 00 b9 14 63 68  72 6f 6e 69 63 6c 65 2e $.....ch ronicle.
00000130 77 72 69 74 65 2e 6c 6f  63 6b 8e 00 00 00 00 a7 write.lo ck......
00000140 00 00 00 00 00 00 00 80  2c 00 00 00 b9 1d 63 68 ........ ,.....ch
00000150 72 6f 6e 69 63 6c 65 2e  6c 61 73 74 49 6e 64 65 ronicle. lastInde
00000160 78 52 65 70 6c 69 63 61  74 65 64 8f 8f 8f 8f a7 xReplica ted.....
00000170 ff ff ff ff ff ff ff ff  34 00 00 00 b9 29 63 68 ........ 4....)ch
00000180 72 6f 6e 69 63 6c 65 2e  6c 61 73 74 41 63 6b 6e ronicle. lastAckn
00000190 6f 77 6c 65 64 67 65 64  49 6e 64 65 78 52 65 70 owledged IndexRep
000001a0 6c 69 63 61 74 65 64 a7  ff ff ff ff ff ff ff ff licated. ........
```

### Annotated Breakdown

```
# Message 1: METADATA header (0xAC = 172 bytes, 0x40 = metadata flag)
00: AC 00 00 40  → header: METADATA | size=0x000000AC (172 bytes)
04: B9 06        → EVENT_NAME, length=6
06: 68 65 61 64 65 72  → "header"
0C: B6 07        → TYPE_PREFIX, length=7
0E: 53 54 53 74 6F 72 65  → "STStore"
15: 82 96 00 00 00  → BYTES_LENGTH32, nest length=0x96 (150 bytes)
  1A: C8         → field name, length=8 (0xC8-0xC0)
  1B: 77 69 72 65 54 79 70 65  → "wireType"
  23: B6 08      → TYPE_PREFIX, length=8
  25: 57 69 72 65 54 79 70 65  → "WireType"
  2D: EC         → text value, length=12 (0xEC-0xE0)
  2E: 42 49 4E 41 52 59 5F 4C 49 47 48 54  → "BINARY_LIGHT"
  3A: C8         → field name, length=8
  3B: 6D 65 74 61 64 61 74 61  → "metadata"
  43: B6 07      → TYPE_PREFIX, length=7
  45: 53 43 51 4D 65 74 61  → "SCQMeta"
  4C: 82 5D 00 00 00  → BYTES_LENGTH32, nest length=0x5D (93 bytes)
    51: C4       → field name, length=4
    52: 72 6F 6C 6C  → "roll"
    56: B6 08    → TYPE_PREFIX, length=8
    58: 53 43 51 53 52 6F 6C 6C  → "SCQSRoll"
    60: 82 26 00 00 00  → BYTES_LENGTH32, nest length=0x26 (38 bytes)
      65: C6     → field name, length=6
      66: 6C 65 6E 67 74 68  → "length"
      6C: A6     → INT32
      6D: 00 5C 26 05  → 86,400,000 (little-endian) = 24h in ms
      71: C6     → field name, length=6
      72: 66 6F 72 6D 61 74  → "format"
      78: EB     → text value, length=11 (0xEB-0xE0)
      79: 79 79 79 79 4D 4D 64 64 27 46 27  → "yyyyMMdd'F'"
      84: C5     → field name, length=5
      85: 65 70 6F 63 68  → "epoch"
      8A: 00     → inline varint value 0
    # end of SCQSRoll nest (0x65 + 0x26 = 0x8B)
    8B: D7       → field name, length=23 (0xD7-0xC0)
    8C: 64 65 6C 74 61 ... 76 61 6C  → "deltaCheckpointInterval"
    A3: 40       → inline varint value 64
    A4: C8       → field name, length=8
    A5: 73 6F 75 72 63 65 49 64  → "sourceId"
    AD: 00       → inline varint value 0
  # end of SCQMeta nest (0x51 + 0x5D = 0xAE)
  AE: 8F 8F     → two PADDING bytes (align to 8-byte boundary)
# end of STStore nest (0x1A + 0x96 = 0xB0)
# end of metadata message (0x04 + 0xAC = 0xB0)

# Message 2: DATA — listing.highestCycle
B0: 24 00 00 00  → header: DATA | size=0x24 (36 bytes)
B4: B9 14        → EVENT_NAME, length=20
B6: 6C 69 73 74 69 6E 67 2E 68 69 67 68 65 73 74
    43 79 63 6C 65  → "listing.highestCycle"
CA: 8E 00 00 00 00  → PADDING_32, 0 extra bytes (5-byte padding)
CF: A7           → INT64
D0: FD 49 00 00 00 00 00 00  → 18941 (little-endian) = cycle value

# Message 3: DATA — listing.lowestCycle
D8: 24 00 00 00  → header: DATA | size=0x24 (36 bytes)
DC: B9 13        → EVENT_NAME, length=19
DE: 6C 69 73 74 69 6E 67 2E 6C 6F 77 65 73 74
    43 79 63 6C 65  → "listing.lowestCycle"
F1: 8E 01 00 00 00 00  → PADDING_32, 1 extra byte (6-byte padding)
F7: A7           → INT64
F8: FD 49 00 00 00 00 00 00  → 18941 (little-endian)

# Message 4: DATA — listing.modCount
100: 1C 00 00 00  → header: DATA | size=0x1C (28 bytes)
104: B9 10        → EVENT_NAME, length=16
106: 6C 69 73 74 69 6E 67 2E 6D 6F 64 43 6F 75 6E 74
                   → "listing.modCount"
116: 8F           → PADDING (1 byte)
117: A7           → INT64
118: 01 00 00 00 00 00 00 00  → 1 (little-endian)

# Message 5: DATA — chronicle.write.lock
120: 24 00 00 00  → header: DATA | size=0x24 (36 bytes)
124: B9 14        → EVENT_NAME, length=20
126: 63 68 72 6F 6E 69 63 6C 65 2E 77 72 69 74 65
     2E 6C 6F 63 6B  → "chronicle.write.lock"
13A: 8E 00 00 00 00  → PADDING_32
13F: A7           → INT64
140: 00 00 00 00 00 00 00 80  → 0x8000000000000000 (lock value)

# Message 6: DATA — chronicle.lastIndexReplicated
148: 2C 00 00 00  → header: DATA | size=0x2C (44 bytes)
14C: B9 1D        → EVENT_NAME, length=29
14E: 63 68 72 6F 6E 69 63 6C 65 2E 6C 61 73 74 49 6E
     64 65 78 52 65 70 6C 69 63 61 74 65 64
                   → "chronicle.lastIndexReplicated"
16B: 8F 8F 8F 8F  → four PADDING bytes
16F: A7           → INT64
170: FF FF FF FF FF FF FF FF  → -1 (0xFFFFFFFFFFFFFFFF)

# Message 7: DATA — chronicle.lastAcknowledgedIndexReplicated
178: 34 00 00 00  → header: DATA | size=0x34 (52 bytes)
17C: B9 29        → EVENT_NAME, length=41
17E: 63 68 72 6F 6E 69 63 6C 65 2E 6C 61 73 74 41 63
     6B 6E 6F 77 6C 65 64 67 65 64 49 6E 64 65 78 52
     65 70 6C 69 63 61 74 65 64
                   → "chronicle.lastAcknowledgedIndexReplicated"
1A7: A7           → INT64 (no padding needed — already aligned)
1A8: FF FF FF FF FF FF FF FF  → -1

# Total file size: 0x1B0 (432 bytes)
```

---

## BinaryWire Encoding Reference

BinaryWire is a self-describing binary serialization format. Each value is preceded by a control byte that identifies its type and encoding.

### Control Byte Map

| Byte Range | Code Name | Description | Followed By |
|-----------|-----------|-------------|-------------|
| `0x00–0x7F` | _(inline value)_ | Unsigned integer 0–127 | Nothing |
| `0x82` | `BYTES_LENGTH32` | Nested structure | 4-byte LE length, then nested content |
| `0x8D` | `I64_ARRAY` | Array of int64 | 8-byte capacity, 8-byte used count, then N×8-byte values |
| `0x8E` | `PADDING_32` | Multi-byte padding | 4-byte LE count of additional bytes to skip |
| `0x8F` | `PADDING` | Single byte padding | Nothing |
| `0x90` | `FLOAT32` | IEEE 754 float | 4 bytes LE |
| `0xA5` | `INT16` | 16-bit integer | 2 bytes LE |
| `0xA6` | `INT32` | 32-bit integer | 4 bytes LE |
| `0xA7` | `INT64` | 64-bit integer | 8 bytes LE |
| `0xB6` | `TYPE_PREFIX` | Type annotation | Stop-bit encoded length, then UTF-8 name |
| `0xB8` | `TEXT_ANY` | Text (any length) | Stop-bit encoded length, then UTF-8 text |
| `0xB9` | `EVENT_NAME` | Event/key name | Stop-bit encoded length, then UTF-8 name |
| `0xC0–0xDF` | _(small field)_ | Field name | `(byte - 0xC0)` UTF-8 bytes of name |
| `0xE0–0xFF` | _(small text)_ | Text value | `(byte - 0xE0)` UTF-8 bytes of text |

### Stop-Bit Encoding

Used for lengths in `EVENT_NAME`, `TYPE_PREFIX`, and `TEXT_ANY`:

```
For values 0–127:   single byte with MSB = 0
  [0xxxxxxx]

For values 128+:    multi-byte, MSB = 1 means "more bytes follow"
  [1xxxxxxx] [1xxxxxxx] ... [0xxxxxxx]
  
Each byte contributes 7 bits. Bytes are read sequentially, accumulating:
  result = 0
  for each byte b:
    result = (result << 7) | (b & 0x7F)
    if (b & 0x80) == 0: break
```

### Nesting (`BYTES_LENGTH32`)

```
Byte:  0x82
+1:    length[0]    ┐
+2:    length[1]    │ 4-byte little-endian length
+3:    length[2]    │
+4:    length[3]    ┘
+5:    ... nested content for `length` bytes ...
```

The parser maintains a nesting stack. When the read position reaches `start + 4 + length`, the nesting level pops.

### I64_ARRAY (`0x8D`)

Used for index structures:

```
Byte:  0x8D
+1:    capacity[0..7]    8-byte LE uint64: total array capacity
+9:    used[0..7]        8-byte LE uint64: number of entries actually used
+17:   values[0..7]      first int64 value
+25:   values[0..7]      second int64 value
...
+17+8*capacity:          end of array
```

### Varint Encoding (for compact integer representation)

The wirepad writer selects the smallest encoding:

| Value Range | Encoding | Bytes |
|------------|----------|-------|
| 0–0x7F | Inline control byte | 1 |
| 0x80–0xFFFF | `0xA5` + 2 bytes LE | 3 |
| 0x10000–0xFFFFFFFF | `0xA6` + 4 bytes LE | 5 |
| 0x100000000–0xFFFFFFFFFFFFFFFF | `0xA7` + 8 bytes LE | 9 |

### Aligned INT64 (`wirepad_uint64_aligned`)

For fields that will be accessed concurrently via memory mapping, the 8 data bytes must be 8-byte aligned. The writer inserts padding before the `0xA7` prefix:

```
Goal: the 0xA7 byte is at offset (8n - 1), so data starts at 8n.

padding_needed = -((current_position + 1) - base) & 0x07

If padding_needed == 0:
  (no padding)
Else if padding_needed < 5:
  Insert padding_needed × 0x8F bytes
Else:
  Insert 0x8E + 4-byte LE (padding_needed - 5) + zero fill
```

---

## 64-Bit Index Layout

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

### Cycle Calculation

```
cycle = (current_time_ms - roll_epoch) / roll_length_ms
```

Where:
- `current_time_ms` = milliseconds since Unix epoch (1970-01-01 00:00:00 UTC)
- `roll_epoch` = configured epoch offset in milliseconds (typically 0)
- `roll_length_ms` = roll period in milliseconds (e.g., 86,400,000 for daily)

### Filename from Cycle

```
time_seconds = cycle * (roll_length_ms / 1000)
filename = dirname + "/" + strftime(time_seconds, converted_format) + ".cq4"
```

### Example

For a `FAST_DAILY` queue with `roll_epoch=0`:

| Index (hex) | Cycle | Seqnum | Date | Filename |
|-------------|-------|--------|------|----------|
| `0x4A0500000000` | 18949 | 0 | 2021-11-18 | `20211118F.cq4` |
| `0x4A0500000003` | 18949 | 3 | 2021-11-18 | `20211118F.cq4` |
| `0x4A0600000000` | 18950 | 0 | 2021-11-19 | `20211119F.cq4` |

---

## Roll Schemes

### Complete Roll Scheme Table

| Name | Java Format | Roll Period | index_count | index_spacing |
|------|-------------|-------------|-------------|---------------|
| `FIVE_MINUTELY` | `yyyyMMdd-HHmm'V'` | 5 min | 2048 | 256 |
| `TEN_MINUTELY` | `yyyyMMdd-HHmm'X'` | 10 min | 2048 | 256 |
| `TWENTY_MINUTELY` | `yyyyMMdd-HHmm'XX'` | 20 min | 2048 | 256 |
| `HALF_HOURLY` | `yyyyMMdd-HHmm'H'` | 30 min | 2048 | 256 |
| `FAST_HOURLY` | `yyyyMMdd-HH'F'` | 1 hour | 4096 | 256 |
| `TWO_HOURLY` | `yyyyMMdd-HH'II'` | 2 hours | 4096 | 256 |
| `FOUR_HOURLY` | `yyyyMMdd-HH'IV'` | 4 hours | 4096 | 256 |
| `SIX_HOURLY` | `yyyyMMdd-HH'VI'` | 6 hours | 4096 | 256 |
| `FAST_DAILY` | `yyyyMMdd'F'` | 24 hours | 4096 | 256 |
| `MINUTELY` | `yyyyMMdd-HHmm` | 1 min | 2048 | 16 |
| `HOURLY` | `yyyyMMdd-HH` | 1 hour | 4096 | 16 |
| `DAILY` | `yyyyMMdd` | 24 hours | 8192 | 64 |
| `LARGE_HOURLY` | `yyyyMMdd-HH'L'` | 1 hour | 8192 | 64 |
| `LARGE_DAILY` | `yyyyMMdd'L'` | 24 hours | 32768 | 128 |
| `XLARGE_DAILY` | `yyyyMMdd'X'` | 24 hours | 32768 | 256 |
| `HUGE_DAILY` | `yyyyMMdd'H'` | 24 hours | 32768 | 1024 |
| `SMALL_DAILY` | `yyyyMMdd'S'` | 24 hours | 8192 | 8 |
| `LARGE_HOURLY_SPARSE` | `yyyyMMdd-HH'LS'` | 1 hour | 4096 | 1024 |
| `LARGE_HOURLY_XSPARSE` | `yyyyMMdd-HH'LX'` | 1 hour | 2048 | 1,048,576 |
| `HUGE_DAILY_XSPARSE` | `yyyyMMdd'HX'` | 24 hours | 16384 | 1,048,576 |
| `TEST_SECONDLY` | `yyyyMMdd-HHmmss'T'` | 1 sec | 32768 | 4 |
| `TEST4_SECONDLY` | `yyyyMMdd-HHmmss'T4'` | 1 sec | 32 | 4 |
| `TEST_HOURLY` | `yyyyMMdd-HH'T'` | 1 hour | 16 | 4 |
| `TEST_DAILY` | `yyyyMMdd'T1'` | 24 hours | 8 | 1 |
| `TEST2_DAILY` | `yyyyMMdd'T2'` | 24 hours | 16 | 2 |
| `TEST4_DAILY` | `yyyyMMdd'T4'` | 24 hours | 32 | 4 |
| `TEST8_DAILY` | `yyyyMMdd'T8'` | 24 hours | 128 | 8 |

### Java Date Format → strftime Conversion

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

The default roll scheme is `FAST_DAILY`.

---

## Index Structures (Index2Index and Index Pages)

### Overview

The index is a two-level lookup structure that allows efficient seeking to a specific seqnum without scanning from the beginning of the file. It is stored as metadata messages within the queue file.

```
Index2Index (root)
    │
    ├── [0] → Index Page 0  →  [0] msg at seqnum 0
    │                           [1] msg at seqnum 64
    │                           [2] msg at seqnum 128
    │                           ...
    │
    ├── [1] → Index Page 1  →  [0] msg at seqnum 64*8192
    │                           [1] msg at seqnum 64*8192 + 64
    │                           ...
    │
    └── [index_count-1] → ...
```

### Capacity

For the `DAILY` scheme:
- Each index page has `index_count = 8192` entries
- Index spacing is `64` (every 64th message is indexed)
- One index page covers `8192 × 64 = 524,288` messages
- The index2index can point to `8192` index pages
- Total capacity: `8192 × 8192 × 64 = 4,294,967,296` messages (matches 32-bit seqnum space)

### Wire Format

Both index2index and index pages are encoded as metadata messages containing an `I64_ARRAY`:

```
METADATA header [size]
  event_name "index2index" (or "index")
  I64_ARRAY {
    capacity: index_count
    used: N
    values: [byte_offset_0, byte_offset_1, ..., byte_offset_N-1, 0, 0, ...]
  }
```

Values are byte offsets from the start of the queue file pointing to the target (either index pages or data messages).

> **Note:** libchronicle does NOT currently write index structures. It reads queue files by sequential scanning. This means joining a large existing queue is slower than it could be with index support.

---

## Alignment Padding Detail

After each message's payload, zero-padding bytes are inserted to align the next header to a 4-byte boundary:

```
padding_bytes = (-payload_size) & 0x03

Example: payload_size = 5
  padding = (-5) & 0x03 = 0xFFFFFFFB & 0x03 = 3
  Total entry: 4 (header) + 5 (payload) + 3 (padding) = 12 bytes (divisible by 4)

Example: payload_size = 8
  padding = (-8) & 0x03 = 0
  Total entry: 4 (header) + 8 (payload) + 0 (padding) = 12 bytes
```

This ensures the CAS target (4-byte header) is always naturally aligned, which is required for the `lock cmpxchgl` instruction to behave correctly.

---

## Endianness and Alignment

### Byte Order

All multi-byte integers in both the framing headers and BinaryWire content are **little-endian**.

### Alignment Requirements

| Item | Alignment | Reason |
|------|-----------|--------|
| 4-byte message header | 4-byte aligned (v5 only) | Required for atomic CAS |
| Shared uint64 fields in .cq4t | 8-byte aligned | Concurrent mmap access |
| mmap offset | Page-aligned (OS requirement) | `mmap()` requires page-aligned offset; libchronicle uses blocksize (≥ page size) |
| BinaryWire fields | Unaligned | Read via `memcpy`, not direct cast |

---

## Invariants and Constraints

### File System

1. The queue directory must exist before `chronicle_open` is called
2. All files within the queue directory are owned by the queue (no other files should be present)
3. Queue files are machine-independent and can be copied between systems
4. File I/O must use `mmap` with `MAP_SHARED`; `read()`/`write()` may see stale data

### Message Ordering

1. All writes resolve into a total order preserved on replay
2. Writes within a single cycle file are totally ordered by file position
3. Cross-cycle ordering is determined by cycle number
4. The index value is strictly monotonically increasing (within a cycle, seqnum increments by 1)

### Concurrency

1. Writers MUST use `lock cmpxchgl` (or equivalent atomic CAS) on the 4-byte header
2. Readers MUST use `mfence` (or equivalent memory barrier) between reading the header and reading the payload
3. Multiple appenders and tailers on the same machine are supported
4. Cross-machine access is NOT supported (shared memory primitives are local)
5. The library is single-threaded within a process; external locking is needed for multi-threaded use

### Size Limits

1. Maximum message payload: 1,073,741,823 bytes (30-bit size field)
2. Maximum seqnum: 4,294,967,295 (32-bit)
3. Maximum cycle: 4,294,967,295 (32-bit)
4. Minimum blocksize: 1,048,576 bytes (1 MiB, default)
5. blocksize must be a power of two

### Cycle Management

1. `highest_cycle` and `lowest_cycle` in the directory listing must be kept consistent
2. `modcount` must be atomically incremented when cycle range changes
3. EOF markers should be written when rolling to a new cycle
4. Missing EOF markers can be patched by appenders within `patch_cycles` (default: 3) of `highest_cycle`
5. Readers may skip past a missing EOF if the file's cycle is more than `patch_cycles` behind `highest_cycle`

---

*This specification was derived from analysis of the libchronicle C source code and verified against test data written by Java Chronicle Queue.*