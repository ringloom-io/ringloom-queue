# ringloom-queue Replication — Architecture & Integration Guide

**Audience:** Engineers and AI agents integrating replication into their projects.
**Target runtime:** Zig 0.16, ringloom-queue (this repository).
**Scope:** Architecture, integration points (native Zig, C ABI), transport implementation,
configuration, metrics, lifecycle, and operational guidance.

---

## 1. What replication provides

The replication layer adds **single-master, multi-follower** replication to ringloom-queue with
**byte-exact index alignment**. For every excerpt written on the master at `index`, each follower
stores the identical payload under the **same** `index`, in a directory that is itself a
bit-for-bit valid ringloom-queue. A follower can be read by ordinary tailers exactly like the
master, and is indistinguishable from it at the user-data level.

Key properties:

- **Index-exact.** The follower reproduces the master's `index` space (`cycle << 32 | seqnum`),
  payload-for-payload, including cycle rolls and empty intermediate cycles.
- **Format-preserving.** Followers are written through the normal ringloom append path
  (`writeAtIndex`), so they are valid queues — no separate replica format.
- **Pluggable transport.** The library ships **no** transport. You supply one that is
  **reliable, ordered, and in-session** (e.g. TCP, a message bus, shared memory, io_uring).
- **Lowest-latency hot path.** Zero per-message application acknowledgement, zero-copy where
  possible, and a comptime-monomorphized transport contract with **no runtime indirect call** on
  the native hot path.
- **Polling-first, no mandatory threads.** The source and sink are bounded, non-blocking
  state machines you drive with `step()`. They never spawn threads of their own; the enclosing
  application owns scheduling.

### 1.1 Non-goals

- No multi-master / no conflict resolution. Exactly one writer (the master appender) per queue.
- No application-level per-message ack and no synchronous handshakes during steady state.
- No built-in transport, discovery, authentication, or encryption — those belong to the
  transport the consumer provides.
- No reordering or de-duplication beyond what the transport guarantees; the transport **must**
  deliver whole frames, in order, within a session.

---

## 2. Architecture

```
        MASTER NODE                                   FOLLOWER NODE
 ┌──────────────────────────┐                  ┌──────────────────────────┐
 │  ringloom Queue (master) │                  │  ringloom Queue (replica)│
 │      ▲ append            │                  │      ▲ writeAtIndex      │
 │      │                   │                  │      │                   │
 │  ┌───┴──────────────┐    │   frames         │   ┌──┴───────────────┐   │
 │  │ ReplicationSource │===│=================►│   │ ReplicationSink   │   │
 │  │  (tails queue,    │   │   (your          │   │ (decodes, applies │   │
 │  │   ships excerpts) │◄══│=================│   │  via writeAtIndex)│   │
 │  └───────────────────┘   │   control        │   └───────────────────┘   │
 │      │ step()            │   (HELLO/ACK/…)  │      │ step()            │
 └──────┼───────────────────┘                  └──────┼───────────────────┘
        │                                              │
   your event loop                                your event loop
        │                                              │
   ┌────┴──────────────────┐                  ┌────────┴──────────────┐
   │ Transport.Outbound/In │◄════ network ════►│ Transport.Outbound/In │
   └───────────────────────┘                  └───────────────────────┘
```

The data direction is **source → sink** (excerpts, batches, cycle rolls, heartbeats). The control
direction is **sink → source** (HELLO at connect, periodic ACK, RESET on trouble). Both directions
flow over the transport channels you supply.

### 2.1 Components

| Component | Role | Zig module |
|---|---|---|
| **ReplicationSource** | Master side. Owns one `Tailer` per active session, reads raw excerpts, encodes frames, and `offer`s them to the outbound channel. Handles HELLO and chooses replay mode. | `src/ringloom/repl/source.zig` |
| **ReplicationSink** | Follower side. Sends HELLO, decodes incoming frames, and applies each excerpt to the local queue at the exact index via `writeAtIndex`. Detects gaps and drives recovery. | `src/ringloom/repl/sink.zig` |
| **FrameCodec** | Allocation-free encode/decode of the wire frames into caller-owned buffers. Decoders return borrowed views (sub-slices). | `src/ringloom/repl/codec.zig`, `protocol.zig` |
| **Transport SPI** | The comptime-generic contract your transport satisfies. Plus `CTransport`, the runtime-vtable adapter used only at the C ABI boundary. | `src/ringloom/repl/transport.zig` |
| **CycleSynchronizer** | Sink-side helper that seals the previous cycle, materializes any empty intermediate cycles, and opens the target cycle so the follower's roll boundaries match the master byte-for-byte. | `src/ringloom/repl/cycle_sync.zig` |
| **writeAtIndex** | Core appender capability that writes a payload at an *explicit* index (rather than auto-assigning). The sink uses it to reproduce the master's index space. | `src/ringloom/appender.zig`, `repl/write_at_index.zig` |
| **session helpers** | Session-id generation, reconnect backoff, and the queue-config identity used for compatibility checks. | `src/ringloom/repl/session.zig` |

### 2.2 The `writeAtIndex` core change

The normal appender path auto-assigns indices. Replication needs the follower to write at the
master's exact index, so the appender gained:

- `writeAtIndex(index, payload)` / `writePartsAtIndex(...)` — open the target cycle (rolling
  forward and sealing previous cycles as needed), enforce contiguity, and write the body using the
  **same** byte layout as a normal append. Out-of-order or duplicate writes return
  `error.DuplicateIndex`; a forward jump returns `error.IndexGap`.
- `sealTip` / `openCycleForWrite` / `sealCurrentCycleIfAt` — used by the `CycleSynchronizer` to
  reproduce cycle boundaries and empty intermediate cycles.

Because `writeAtIndex` and the normal append share one private body-writing helper, a replicated
record is **byte-identical** to the original, which is what makes follower files valid and
tailer-parity exact.

### 2.3 Cycle synchronization

When the master rolls from one cycle to another it emits a `CYCLE_ROLL` frame. The sink's
`CycleSynchronizer.onCycleRoll(from, to, next_expected)`:

1. Seals the `from` cycle (writes EOF) if the follower is positioned there.
2. **Materializes empty intermediate cycles** — if the master skipped cycles (e.g. rolled 0 → 4
   with no data in 1–3), the sink creates and seals those empty cycle files so a tailer rolls
   across them exactly as on the master.
3. Opens the `to` cycle, so the next excerpt lands at `Index.compose(to, 0)`.

> Note: a master that *itself* skips cycle files cannot have its own tailers (and therefore the
> source) advance across a missing cycle — the source only streams cycles the master actually
> created. Empty-cycle materialization is a **sink-side** concern that reproduces whatever cycle
> boundaries the master's files imply.

---

## 3. Concurrency & driver model

The source and sink are **pollable state machines**. You call `step(max_work_units)` repeatedly
from your own thread or event loop:

- `max_work_units` bounds how much the call does (frames polled, excerpts shipped/applied) so a
  single `step` never runs unbounded.
- The return value is a `StepResult`: `.idle` (nothing to do — back off), `.made_progress`, or
  `.more_work` (call again immediately).
- Neither component spawns threads. You decide the threading model: a dedicated thread per
  component, a shared reactor, or interleaved with other work.

A typical loop:

```zig
while (running) {
    const r = try source.step(64);   // or sink.step(64)
    if (r == .idle) backoffSleep();   // your idle strategy (spin, sleep, futex, …)
}
```

The same `step` call both **drains inbound control/data frames** (via `inbound.poll` +
`inbound.nextFrame`) and **produces outbound frames** (via `outbound.offer`), so one driver thread
per component is sufficient.

---

## 4. Wire protocol (summary)

All multi-byte integers are **little-endian**. The transport delivers **whole frames**; the
protocol carries no outer length prefix. Every frame begins with a fixed 16-byte header.

### 4.1 Common header (16 bytes)

| Offset | Size | Field | Notes |
|---|---|---|---|
| 0 | 2 | `magic` | `0x524C` |
| 2 | 1 | `version` | `1` |
| 3 | 1 | `frame_type` | see below |
| 4 | 4 | `flags` | bitfield (`END_OF_BATCH`, `CATCHUP`, …) |
| 8 | 8 | `session_id` | `0` for HELLO / HELLO_ACK / HELLO_NACK |

A decoder validates `magic` and `version` before trusting any later byte.

### 4.2 Frame types

| Type | Code | Direction | Purpose |
|---|---|---|---|
| `HELLO` | `0x01` | sink → source | Connect; carries the follower's `last_applied_index` and full roll-scheme config for compatibility validation. |
| `HELLO_ACK` | `0x02` | source → sink | Accept; assigns `session_id`, reports `source_first_available_index` / `source_last_index` and the chosen replay `mode`. |
| `HELLO_NACK` | `0x03` | source → sink | Reject (fatal to the sink): config mismatch, sink-ahead, version incompatible, index unavailable, queue-id mismatch. |
| `EXCERPT` | `0x10` | source → sink | One record: `index` + raw payload bytes (excluding ringloom's 4-byte header and padding). |
| `EXCERPT_BATCH` | `0x11` | source → sink | `first_index` + `count` contiguous records. Indices are implicit; a batch never spans a cycle boundary. |
| `CYCLE_ROLL` | `0x20` | source → sink | `from_cycle`, `to_cycle`, and `next_expected_index = compose(to_cycle, 0)`. |
| `HEARTBEAT` | `0x30` | source → sink | Liveness; carries `hwm_index` and wall-clock nanos. |
| `ACK` | `0x31` | sink → source | Periodic progress: `last_applied_index`. Throttled; not per-message. |
| `RESET` | `0x40` | sink → source | Sink hit a gap / corrupt frame / operator request; tear down and re-handshake. |
| `CLOSE` | `0xF0` | bidirectional | Orderly shutdown of a session. |

### 4.3 Replay modes

On HELLO the source inspects the follower's `last_applied_index` and chooses a mode (informational
to the sink):

- **`full_replay`** — follower is empty; stream from the source's first available index.
- **`catchup`** — follower is behind; stream from `last_applied_index + 1` (frames flagged
  `CATCHUP`). When the source drains the backlog it transitions the session to live.
- **`live`** — follower is already at the source tip; only new excerpts flow.

If the follower is *ahead* of the source, or its config is incompatible, or the requested index is
no longer retained, the source replies `HELLO_NACK` and the sink fails **without** touching the
local queue.

---

## 5. Transport SPI — implementing a transport

The library defines transport channels as a **comptime, duck-typed contract**. Any type that
provides the required methods satisfies it; there is no `interface` keyword and no per-call vtable
on the native path. A runtime-vtable adapter (`CTransport`) exists **only** for the C ABI boundary.

You must guarantee the transport is **reliable, ordered, and in-session**: every offered frame is
eventually delivered exactly once, in order, or the channel reports a disconnect. A dropped or
reordered frame must surface as a **disconnect**, never as a silent gap. (The sink detects gaps and
recovers, but it relies on the transport never silently losing a frame mid-stream.)

### 5.1 Outbound contract

```zig
/// Non-blocking. Returns >= 0 (accepted at this transport position) or a negative OfferResult.
pub fn offer(self: *Self, frame: []const u8) i64;
pub fn isConnected(self: *Self) bool;
pub fn isBackPressured(self: *Self) bool;
```

`offer` must **copy or fully consume** `frame` before returning — the buffer is reused by the
caller immediately after. Return one of the negative `OfferResult` codes when you cannot accept:

```zig
pub const OfferResult = enum(i64) {
    back_pressured = -1,        // try again later (the caller retries this exact frame)
    not_connected = -2,         // session is down → triggers teardown/reconnect
    admin_action = -3,
    closed = -4,
    max_position_exceeded = -5,
    // any value >= 0 means "accepted at this transport position"
};
```

`back_pressured` is the cooperative path: the source holds the un-offered frame and retries it on
the next `step` (it never drops or skips). A `not_connected`/`closed` result tears the session down
and the sink reconnects with backoff.

### 5.2 Inbound contract — pull model

```zig
/// Non-blocking. Make up to `fragment_limit` frames available; return how many are now buffered.
pub fn poll(self: *Self, fragment_limit: u32) u32;
/// Next buffered frame as a borrowed slice (valid until the next poll/nextFrame), or null when drained.
pub fn nextFrame(self: *Self) ?[]const u8;
pub fn isConnected(self: *Self) bool;
```

The inbound side is **pull-based**, not callback-based: `poll` reassembles up to `fragment_limit`
whole frames into your internal queue and returns the count; the library then calls `nextFrame`
repeatedly to drain them. This keeps the per-frame decode + `writeAtIndex` loop fully
monomorphized in library code with no re-entrancy.

**Buffers returned by `nextFrame` are borrowed.** The library copies what it needs (via
`writeAtIndex`) before the next `poll`. Keep each frame valid until the next `poll`/`nextFrame`
call and treat it as read-only.

### 5.3 Why comptime and not a vtable

`offer`/`poll`/`nextFrame` run on the hot path (potentially millions of calls/sec). A runtime
function-pointer vtable would add an indirect branch and an inlining barrier per call. Instead,
`ReplicationSource`/`ReplicationSink` are **generic over the channel types**, so the calls bind
statically and inline to the transport's own body. The channel *instances* are ordinary runtime
fields; only the *types* are comptime parameters. This is the same mechanism the queue core uses
for `Codec(T)`.

The library validates the contract once per instantiation with `transport.AssertOutbound(T)` /
`transport.AssertInbound(T)`, producing a clear compile error if a method is missing.

### 5.4 Minimal native transport skeleton

```zig
const repl = @import("ringloom_queue").repl;

const MyOutbound = struct {
    conn: *MyConnection,
    pub fn offer(self: *MyOutbound, frame: []const u8) i64 {
        return self.conn.trySend(frame) orelse
            @intFromEnum(repl.transport.OfferResult.back_pressured);
        // return a position (>=0) on success; negative OfferResult otherwise
    }
    pub fn isConnected(self: *MyOutbound) bool { return self.conn.up; }
    pub fn isBackPressured(self: *MyOutbound) bool { return self.conn.sendQueueFull(); }
};

const MyInbound = struct {
    conn: *MyConnection,
    pub fn poll(self: *MyInbound, fragment_limit: u32) u32 {
        return self.conn.reassembleUpTo(fragment_limit); // fill internal frame queue
    }
    pub fn nextFrame(self: *MyInbound) ?[]const u8 {
        return self.conn.popFrame();                     // borrowed; valid until next poll
    }
    pub fn isConnected(self: *MyInbound) bool { return self.conn.up; }
};
```

A complete, runnable reference transport (an in-memory ring pair with disconnect/backpressure
simulation) is in `src/ringloom/repl/loopback.zig`. Use it as a template and as a test harness.

---

## 6. Native Zig integration

The replication surface is exposed under `@import("ringloom_queue").repl` (see `src/root.zig` and
`src/ringloom/repl/mod.zig`). The two generic constructors are:

```zig
pub fn ReplicationSource(comptime Outbound: type, comptime Inbound: type) type;
pub fn ReplicationSink(comptime Outbound: type, comptime Inbound: type) type;

// Convenience when your transport groups both channel types:
pub fn Source(comptime Transport: type) type; // = ReplicationSource(Transport.Outbound, Transport.Inbound)
pub fn Sink(comptime Transport: type) type;   // = ReplicationSink(Transport.Outbound, Transport.Inbound)
```

### 6.1 Master (source) setup

```zig
const rq = @import("ringloom_queue");
const repl = rq.repl;

// 1. Open the master queue normally.
const master = try rq.Queue.init(allocator, "/data/master");
defer master.deinit();
master.setCreate(true);
try master.setRollSchemeName("FAST_DAILY");
try master.open();

// (your application appends to `master` via an Appender as usual)

// 2. Bind a source over your transport channels.
const Source = repl.ReplicationSource(MyOutbound, MyInbound);
var src = Source.init(allocator, master, &my_outbound, &my_inbound, .{
    .heartbeat_interval_ns = 100 * std.time.ns_per_ms,
});
defer src.deinit();

// 3. Drive it.
while (running) {
    if (try src.step(64) == .idle) idleBackoff();
}
```

### 6.2 Follower (sink) setup

```zig
// 1. Open the follower queue with the SAME roll scheme as the master.
const follower = try rq.Queue.init(allocator, "/data/follower");
defer follower.deinit();
follower.setCreate(true);
try follower.setRollSchemeName("FAST_DAILY");
try follower.open();

// 2. Bind a sink. init() opens an appender and derives last_applied_index from the local queue,
//    so a restarted follower resumes exactly where it left off.
const Sink = repl.ReplicationSink(MyOutbound, MyInbound);
var snk = try Sink.init(allocator, follower, &my_outbound, &my_inbound, .{
    .node_id = my_node_uuid,    // 16 bytes
    .queue_id = my_queue_uuid,  // 16 bytes
});
defer snk.deinit();

// 3. Drive it. The sink sends HELLO, applies excerpts, ACKs progress, and reconnects on failure.
while (running) {
    if (try snk.step(64) == .idle) idleBackoff();
}

// Read the replica with ordinary tailers — it is a valid ringloom queue.
```

The follower **must** be created with a roll scheme matching the master's geometry
(`roll_length_secs`, `index_count`, `index_spacing`, `block_size`, `format_version`, and roll
name). The source validates this from the HELLO config block and replies `HELLO_NACK`
(`config_mismatch` / `version_incompatible`) on mismatch.

### 6.3 Testing / time injection

Both components expose a `now_override_ns: ?u64` field. Set it to drive heartbeat / ack / timeout
logic deterministically in tests (the integration suite in `src/ringloom/repl/tests.zig` does this).
An `error_hook` field (a function pointer) lets you observe non-fatal anomalies such as the
backpressure watchdog firing.

---

## 7. C ABI integration

The same generic core is instantiated once over `CTransport.Out` / `CTransport.In` and projected
over the C ABI in `src/ringloom/c_api.zig`. The foreign caller supplies function pointers; the
library calls them and **never spawns threads**. The indirect call happens once per
`offer`/`poll` (which amortizes a whole batch), not once per frame — the per-frame decode +
`writeAtIndex` stays monomorphized.

### 7.1 Channels as C function pointers

```c
typedef int64_t (*ringloom_offer_fn)(void *ctx, const uint8_t *buf, size_t len);
typedef bool    (*ringloom_is_connected_fn)(void *ctx);
typedef bool    (*ringloom_is_backpressured_fn)(void *ctx);
typedef void    (*ringloom_close_fn)(void *ctx);
typedef uint32_t (*ringloom_poll_fn)(void *ctx, uint32_t fragment_limit);
typedef const uint8_t *(*ringloom_next_frame_fn)(void *ctx, size_t *out_len);

typedef struct {
    uint32_t size;                          // = sizeof(this struct)
    void *ctx;
    ringloom_offer_fn offer;
    ringloom_is_connected_fn is_connected;
    ringloom_is_backpressured_fn is_backpressured;
    ringloom_close_fn close;                // optional (may be NULL)
} ringloom_outbound_channel;

typedef struct {
    uint32_t size;
    void *ctx;
    ringloom_poll_fn poll;
    ringloom_next_frame_fn next_frame;      // borrowed; valid until next poll
    ringloom_is_connected_fn is_connected;
    ringloom_close_fn close;                // optional
} ringloom_inbound_channel;
```

The inbound side uses the **pull model** (`poll` then repeated `next_frame`) — there is no
`on_frame` upcall. Your `poll` reassembles bytes into your own queue; `next_frame` hands back one
borrowed frame at a time (return `NULL` / set `*out_len = 0` when drained).

### 7.2 Exported functions

```c
// Source
int  ringloom_repl_source_open(const ringloom_repl_source_options *opts, ringloom_repl_source_t **out);
int  ringloom_repl_source_step(ringloom_repl_source_t *s, uint32_t max_work_units, ringloom_step_result_t *out);
int  ringloom_repl_source_metrics(ringloom_repl_source_t *s, ringloom_repl_source_metrics_t *out);
void ringloom_repl_source_close(ringloom_repl_source_t *s);

// Sink
int  ringloom_repl_sink_open(const ringloom_repl_sink_options *opts, ringloom_repl_sink_t **out);
int  ringloom_repl_sink_step(ringloom_repl_sink_t *s, uint32_t max_work_units, ringloom_step_result_t *out);
int  ringloom_repl_sink_last_applied_index(ringloom_repl_sink_t *s, int64_t *out);
int  ringloom_repl_sink_metrics(ringloom_repl_sink_t *s, ringloom_repl_sink_metrics_t *out);
void ringloom_repl_sink_close(ringloom_repl_sink_t *s);
```

The options structs (`ringloom_repl_source_options`, `ringloom_repl_sink_options`) follow the
existing C ABI conventions: a leading `size` field for additive forward-compatibility, a `queue`
handle, 16-byte `node_id` / `queue_id`, the two channel structs, and millisecond-valued tuning
fields (heartbeat, ack intervals, timeouts, reconnect backoff). The bound queue **must outlive**
the source/sink; closing the source/sink does **not** close the queue.

### 7.3 Driver loop (no threads)

```c
for (;;) {
    ringloom_step_result_t r;
    ringloom_repl_sink_step(snk, 64, &r);
    if (r == RINGLOOM_STEP_IDLE) my_idle_backoff();
}
```

### 7.4 Error codes

Replication extends `ringloom_error_t` with stable codes, each with `ringloom_strerror` text:
`repl_config_mismatch` (-20), `repl_sink_ahead` (-21), `repl_index_unavailable` (-22),
`repl_gap_detected` (-23), `repl_corrupt_frame` (-24), `repl_transport_error` (-25),
`repl_version_incompatible` (-26), `repl_queue_id_mismatch` (-27).

A full in-process C ABI loopback smoke test (two queues + ring-buffer channels, source + sink in
one process, asserting follower parity) lives in the `c_api.zig` test block and is a good
copy-paste starting point.

---

## 8. Configuration reference

### 8.1 `SourceConfig`

| Field | Default | Meaning |
|---|---|---|
| `node_salt` | `1` | Seeds session-id generation (C ABI derives it from `node_id`). |
| `max_frame_bytes` | `2 MiB` | Per-session send-buffer size; also the largest frame the source emits. Must exceed the largest excerpt + framing. |
| `control_fragment_limit` | `16` | Max control frames polled per `step` on the source's inbound channel. |
| `heartbeat_interval_ns` | `100 ms` | Idle interval after which the source emits a `HEARTBEAT`. |
| `backpressure_watchdog_ns` | `30 s` | If a session stays back-pressured this long, the `error_hook` is invoked (non-fatal). |
| `backpressure_fatal_ns` | `0` (disabled) | If non-zero, a session back-pressured this long is torn down. |

### 8.2 `SinkConfig`

| Field | Default | Meaning |
|---|---|---|
| `node_id` | zeroed | 16-byte follower identity, sent in HELLO. |
| `queue_id` | zeroed | 16-byte queue identity, validated by the source. |
| `max_frame_bytes` | `2 MiB` | Scratch/receive buffer size; must accommodate the largest frame. |
| `ack_interval_ns` | `50 ms` | Minimum spacing between ACKs when progress was made. |
| `force_ack_interval_ns` | `1 s` | Send an ACK at least this often even when idle. |
| `heartbeat_timeout_ns` | `500 ms` | If no inbound frame arrives within this window while live/syncing, request a RESET. |
| `hello_timeout_ns` | `5 s` | If no HELLO_ACK arrives within this window, reconnect. |
| `backoff` | `min 50 ms, max 5 s` | Reconnect backoff policy (`BackoffPolicy{ min_ms, max_ms, factor }`). |

Choose `heartbeat_interval_ns` (source) comfortably below `heartbeat_timeout_ns` (sink) so normal
idle traffic does not trip a false reset (e.g. 100 ms vs 500 ms).

---

## 9. Metrics

Both components expose a `metrics` struct (and C ABI snapshot functions) for observability.

**`SourceMetrics`:** `hwm_index`, `frames_sent`, `bytes_sent`, `backpressure_nanos`,
`active_sessions`, `cycles_rolled`, `hello_nacks`, `decode_errors`, `unexpected_frames`.

**`SinkMetrics`:** `last_applied_index`, `frames_applied`, `bytes_applied`, `replay_reset_count`,
`current_session_id`, `lag_from_source_hwm`, `gaps_detected`, `decode_errors`.

Useful signals: rising `gaps_detected` / `replay_reset_count` indicates an unreliable transport;
sustained `backpressure_nanos` indicates the consumer/transport cannot keep up with the master's
write rate; `lag_from_source_hwm` is your replication lag in index units.

---

## 10. Session lifecycle & recovery

### 10.1 Handshake

1. The sink connects and sends **HELLO** with its `last_applied_index` and full roll config.
2. The source validates config and index availability:
   - Incompatible config / ahead-of-source / unavailable index → **HELLO_NACK** (sink fails,
     local queue untouched).
   - Otherwise it creates a session, picks a replay `mode`, and replies **HELLO_ACK** with
     `source_first_available_index`, `source_last_index`, and `session_id`.
3. The sink anchors its `expected_next` to the source's first available index (important when the
   master's earliest retained cycle is non-zero) and begins applying excerpts.

### 10.2 Steady state

The source ships `EXCERPT` / `EXCERPT_BATCH` frames (coalescing contiguous records within a cycle),
emits `CYCLE_ROLL` at cycle boundaries, and `HEARTBEAT` when idle. The sink applies each record via
`writeAtIndex`, advances `expected_next`, and periodically sends `ACK` with its
`last_applied_index` (throttled — never per message).

### 10.3 Failure & recovery

- **Backpressure** — `offer` returns `back_pressured`; the source holds the exact frame and retries
  next `step`. No data is skipped or reordered.
- **Disconnect** — `offer`/channel reports `not_connected`; the source tears the session down. The
  sink notices (timeout or disconnect), backs off, and re-HELLOs. Because `init`/reconnect
  re-derives `last_applied_index` from the local queue, replay resumes from exactly the last
  durably-applied index — even across a follower crash.
- **Gap or corrupt frame** — if an excerpt's index ≠ `expected_next`, or a frame fails to decode,
  the sink increments `gaps_detected`/`decode_errors` and sends **RESET**; both sides re-handshake
  and the source replays from the follower's true position. The sink never writes past a gap.
- **Sink ahead** — a follower whose `last_applied_index` exceeds the source's last index is
  rejected with `sink_ahead_of_source`; the sink fails rather than rewinding or overwriting.

The guiding invariant: **the follower never silently diverges.** Any anomaly resolves either by
re-handshaking to a known-good position or by failing loudly without corrupting the local queue.

---

## 11. Operational guidance & best practices

- **Match roll schemes exactly.** Create the follower with the same `setRollSchemeName(...)` (or
  equivalent geometry) as the master. A mismatch is rejected at HELLO.
- **One source, many sinks.** The source supports multiple concurrent sessions (one per follower).
  Each follower runs its own sink against its own local queue.
- **Size `max_frame_bytes`** to exceed your largest excerpt plus framing overhead on both sides.
- **Pick an idle strategy.** `step()` returning `.idle` is your cue to spin, sleep, or block on a
  transport readiness primitive — the library deliberately leaves the wait policy to you.
- **Let the transport own delivery semantics.** The library assumes reliable, ordered, in-session
  delivery and that any loss surfaces as a disconnect. Encryption, auth, discovery, and
  reconnection of the *underlying socket* belong to your transport.
- **The bound queue must outlive the source/sink.** Close the source/sink first, then the queue.
- **Followers are first-class queues.** Read them with ordinary tailers; chain them if you need a
  follower-of-a-follower (point a new source at a follower queue).

---

## 12. Testing

- **Unit/integration suite:** `src/ringloom/repl/tests.zig` exercises full replay, catch-up,
  reconnect, gap/RESET recovery, cycle rolls (including empty intermediate cycles), backpressure,
  and byte-for-byte tailer/cycle-file parity over the loopback transport.
- **Reference transport:** `src/ringloom/repl/loopback.zig` is a deterministic in-memory channel
  pair with disconnect / reconnect / drop simulation — use it as a template and as a harness for
  your own transport's conformance tests.
- **C ABI smoke test:** the `c_api.zig` test block wires two queues through ring-buffer channels and
  asserts follower parity end-to-end.

Run everything with:

```sh
zig build test     # module + replication + C ABI tests
zig build c-abi    # build the C ABI shared library
zig build bench    # opt-in benchmarks
```
