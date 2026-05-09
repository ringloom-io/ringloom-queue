// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const builtin = @import("builtin");

const config = @import("config.zig");

pub inline fn cmpxchg32(ptr: *u32, expected: u32, desired: u32) ?u32 {
    return @cmpxchgStrong(u32, ptr, expected, desired, .monotonic, .monotonic);
}

pub inline fn atomicLoad32(ptr: *const u32) u32 {
    return @atomicLoad(u32, ptr, .acquire);
}

pub inline fn atomicStore32(ptr: *u32, val: u32) void {
    @atomicStore(u32, ptr, val, .release);
}

pub inline fn atomicFetchAdd32(ptr: *u32, val: u32) u32 {
    return @atomicRmw(u32, ptr, .Add, val, .release);
}

pub inline fn atomicLoad64(ptr: *const u64) u64 {
    return @atomicLoad(u64, ptr, .acquire);
}

pub inline fn atomicStore64(ptr: *u64, val: u64) void {
    @atomicStore(u64, ptr, val, .release);
}

pub inline fn atomicFetchAdd64(ptr: *u64, val: u64) u64 {
    return @atomicRmw(u64, ptr, .Add, val, .release);
}

pub fn casBackoff(attempt: u32) void {
    if (attempt < 64) {
        std.atomic.spinLoopHint();
    } else if (attempt < 256) {
        std.Thread.yield() catch {};
    } else {
        const shift: u6 = @intCast(@min(attempt - 256, 20));
        const delay_ns = @min(@as(u64, 1000) << shift, config.max_cas_backoff_ns);
        sleepNs(delay_ns);
    }
}

fn sleepNs(ns: u64) void {
    if (builtin.os.tag == .linux) {
        const linux = std.os.linux;
        var req: linux.timespec = .{
            .sec = @intCast(ns / std.time.ns_per_s),
            .nsec = @intCast(ns % std.time.ns_per_s),
        };
        var rem: linux.timespec = undefined;
        while (true) {
            switch (std.posix.errno(linux.nanosleep(&req, &rem))) {
                .SUCCESS => return,
                .INTR => req = rem,
                else => return,
            }
        }
    } else {
        std.Thread.yield() catch {};
    }
}

test "atomic 32-bit helpers load store fetch add and CAS" {
    var value: u32 align(4) = 0;

    try std.testing.expectEqual(@as(u32, 0), atomicLoad32(&value));
    atomicStore32(&value, 7);
    try std.testing.expectEqual(@as(u32, 7), atomicLoad32(&value));
    try std.testing.expectEqual(@as(u32, 7), atomicFetchAdd32(&value, 5));
    try std.testing.expectEqual(@as(u32, 12), atomicLoad32(&value));

    try std.testing.expectEqual(@as(?u32, null), cmpxchg32(&value, 12, 99));
    try std.testing.expectEqual(@as(u32, 99), atomicLoad32(&value));
    try std.testing.expectEqual(@as(?u32, 99), cmpxchg32(&value, 12, 100));
}

test "atomic 64-bit helpers load store and fetch add" {
    var value: u64 align(8) = 0;

    atomicStore64(&value, 1234);
    try std.testing.expectEqual(@as(u64, 1234), atomicLoad64(&value));
    try std.testing.expectEqual(@as(u64, 1234), atomicFetchAdd64(&value, 66));
    try std.testing.expectEqual(@as(u64, 1300), atomicLoad64(&value));
}

test "casBackoff tiers are callable" {
    casBackoff(0);
    casBackoff(64);
    casBackoff(256);
}
