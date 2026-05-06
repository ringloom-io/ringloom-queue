const std = @import("std");

const config = @import("config.zig");
const Queue = @import("queue.zig").Queue;

pub const MappedWindow = struct {
    buf: []align(config.page_alignment) u8,
    mmap_offset: u64,
    mmap_size: u64,
};

pub const Appender = struct {
    queue: *Queue,
    cycle: u64 = 0,
    fd: ?std.posix.fd_t = null,
    seqnum: u64 = 0,
    buf: ?[]align(config.page_alignment) u8 = null,
    mmap_offset: u64 = 0,
    mmap_size: u64 = 0,
    tip: u64 = 0,
    ready_window: ?MappedWindow = null,

    pub fn init(queue: *Queue) Appender {
        return .{ .queue = queue };
    }

    pub fn deinit(self: *Appender) void {
        self.ready_window = null;
        self.buf = null;
        self.fd = null;
    }
};
