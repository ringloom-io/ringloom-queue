// SPDX-License-Identifier: Apache-2.0

//! Replication module: wire protocol, codec, transport SPI, source/sink state
//! machines, and cycle synchronization.
//!
//! `ReplicationSource(Outbound, Inbound)` and `ReplicationSink(Outbound, Inbound)`
//! are comptime-generic over the concrete transport channel types, so the
//! `offer`/`poll`/`nextFrame` hot path binds statically and inlines — no runtime
//! vtable except the `CTransport` adapter at the C ABI boundary.

const std = @import("std");

pub const protocol = @import("protocol.zig");
pub const codec = @import("codec.zig");
pub const transport = @import("transport.zig");
pub const session = @import("session.zig");
pub const cycle_sync = @import("cycle_sync.zig");
pub const source = @import("source.zig");
pub const sink = @import("sink.zig");
pub const write_at_index = @import("write_at_index.zig");

// Common protocol re-exports.
pub const FrameCodec = codec.FrameCodec;
pub const FrameType = protocol.FrameType;
pub const OfferResult = transport.OfferResult;
pub const CTransport = transport.CTransport;

// Generic source/sink constructors.
pub const ReplicationSource = source.ReplicationSource;
pub const ReplicationSink = sink.ReplicationSink;
pub const SourceConfig = source.SourceConfig;
pub const SinkConfig = sink.SinkConfig;
pub const SourceMetrics = source.SourceMetrics;
pub const SinkMetrics = sink.SinkMetrics;
pub const CycleSynchronizer = cycle_sync.CycleSynchronizer;

pub const deriveLastAppliedIndex = write_at_index.deriveLastAppliedIndex;

/// Ergonomic alias: derive both channel types from a transport exposing
/// `Outbound`/`Inbound` decls.
pub fn Source(comptime Transport: type) type {
    return ReplicationSource(Transport.Outbound, Transport.Inbound);
}

pub fn Sink(comptime Transport: type) type {
    return ReplicationSink(Transport.Outbound, Transport.Inbound);
}

test {
    // Discover unit tests in the production modules.
    std.testing.refAllDecls(@This());
    // Discover test-only fixtures and integration scenarios.
    _ = @import("loopback.zig");
    _ = @import("tests.zig");
}
