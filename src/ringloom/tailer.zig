const std = @import("std");

const config = @import("config.zig");
const DispatchAction = @import("codec.zig").DispatchAction;
const Index = @import("index.zig").Index;
const Queue = @import("queue.zig").Queue;
const ReadPrefetchState = @import("prefetcher.zig").ReadPrefetchState;

pub const TailerState = enum(u8) {
    awaiting_entry = 0,
    busy = 1,
    awaiting_queue_file = 2,
    err_stat = 3,
    err_mmap = 4,
    not_yet_polled = 5,
    extend_needed = 6,
    collected = 7,

    pub fn description(self: TailerState) []const u8 {
        return switch (self) {
            .awaiting_entry => "AWAITING_ENTRY",
            .busy => "BUSY",
            .awaiting_queue_file => "AWAITING_QUEUEFILE",
            .err_stat => "E_STAT",
            .err_mmap => "E_MMAP",
            .not_yet_polled => "NOT_YET_POLLED",
            .extend_needed => "EXTEND_NEEDED",
            .collected => "COLLECTED",
        };
    }
};

pub const ParseBlockState = enum(u8) {
    awaiting_entry = 0,
    busy = 1,
    reached_eof = 2,
    need_extend = 3,
    null_item = 4,
    collected = 7,
};

pub fn Collected(comptime T: type) type {
    return struct {
        msg: ?T = null,
        size: usize = 0,
        index: u64 = 0,
    };
}

pub const RawCollected = struct {
    msg: ?[]const u8 = null,
    size: usize = 0,
    index: u64 = 0,
};

pub const MmapProtection = enum {
    read_only,
    read_write,
};

pub const Tailer = struct {
    dispatch_after: u64 = 0,
    state: TailerState = .not_yet_polled,
    dispatcher: ?*const fn (ctx: *anyopaque, index: u64, msg: []const u8) DispatchAction = null,
    dispatch_ctx: ?*anyopaque = null,
    collect: ?*RawCollected = null,

    mmap_protection: MmapProtection = .read_only,

    qf_cycle_open: u64 = 0,
    qf_filename: ?[]const u8 = null,
    qf_fd: ?std.posix.fd_t = null,
    qf_file_size: u64 = 0,

    qf_tip: u64 = 0,
    qf_index: u64 = 0,

    qf_buf: ?[]align(config.page_alignment) u8 = null,
    qf_mmapoff: u64 = 0,
    qf_mmapsz: u64 = 0,

    read_prefetch: ReadPrefetchState = .{},
    queue: *Queue,

    pub fn init(queue: *Queue, start_index: u64) Tailer {
        return .{
            .queue = queue,
            .dispatch_after = if (start_index == 0) 0 else start_index - 1,
            .qf_cycle_open = Index.cycle(start_index),
            .qf_index = start_index,
        };
    }

    pub fn deinit(self: *Tailer) void {
        self.queue.unregisterTailerPrefetch(self);
        if (self.qf_filename) |filename| {
            self.queue.allocator.free(filename);
            self.qf_filename = null;
        }
        self.qf_buf = null;
        self.qf_fd = null;
        self.queue.allocator.destroy(self);
    }

    pub fn offsetToPtr(self: *const Tailer, file_offset: u64) ?[*]u8 {
        const buf = self.qf_buf orelse return null;
        if (file_offset < self.qf_mmapoff) return null;
        const delta = file_offset - self.qf_mmapoff;
        if (delta >= self.qf_mmapsz or delta >= buf.len) return null;
        return buf.ptr + delta;
    }
};

test "tailer states expose protocol descriptions" {
    try std.testing.expectEqualStrings("AWAITING_ENTRY", TailerState.awaiting_entry.description());
    try std.testing.expectEqualStrings("COLLECTED", TailerState.collected.description());
    try std.testing.expectEqual(@as(u8, 2), @intFromEnum(ParseBlockState.reached_eof));
}

test "collected defaults" {
    const C = Collected(u32);
    const collected: C = .{};
    try std.testing.expectEqual(@as(?u32, null), collected.msg);
    try std.testing.expectEqual(@as(usize, 0), collected.size);
    try std.testing.expectEqual(@as(u64, 0), collected.index);
}
