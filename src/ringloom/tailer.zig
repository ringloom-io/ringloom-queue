const std = @import("std");

const config = @import("config.zig");
const Codec = @import("codec.zig").Codec;
const DispatchAction = @import("codec.zig").DispatchAction;
const Header = @import("header.zig").Header;
const Index = @import("index.zig").Index;
const IndexRegion = @import("index.zig").IndexRegion;
const metadata_mod = @import("metadata.zig");
const QueueFileHeader = metadata_mod.QueueFileHeader;
const mmap_ops = @import("mmap_ops.zig");
const Queue = @import("queue.zig").Queue;
const ReadPrefetchState = @import("prefetcher.zig").ReadPrefetchState;
const StepResult = @import("platform.zig").StepResult;

/// Result state from the most recent tailer poll.
pub const TailerState = enum(u8) {
    awaiting_entry = 0,
    busy = 1,
    awaiting_queue_file = 2,
    err_stat = 3,
    err_mmap = 4,
    not_yet_polled = 5,
    extend_needed = 6,
    collected = 7,

    /// Returns a stable diagnostic label for this state.
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

/// Internal block scanner states used by low-level parsing paths.
pub const ParseBlockState = enum(u8) {
    awaiting_entry = 0,
    busy = 1,
    reached_eof = 2,
    need_extend = 3,
    null_item = 4,
    collected = 7,
};

/// Synchronous collection result for typed messages.
pub fn Collected(comptime T: type) type {
    return struct {
        msg: ?T = null,
        size: usize = 0,
        index: u64 = 0,
    };
}

/// Synchronous collection result for borrowed raw byte payloads.
pub const RawCollected = struct {
    msg: ?[]const u8 = null,
    size: usize = 0,
    index: u64 = 0,
};

/// Non-blocking raw poll result.
pub const RawEntry = struct {
    index: u64,
    payload: []const u8,
    raw_size: usize,
};

/// Non-blocking typed poll result.
pub fn Entry(comptime MessageType: type) type {
    return struct {
        index: u64,
        message: MessageType,
        raw_size: usize,
    };
}

/// Mapping protection used when opening a tailer cycle file.
pub const MmapProtection = enum {
    read_only,
    read_write,

    fn toProtection(self: MmapProtection) mmap_ops.Protection {
        return switch (self) {
            .read_only => .read_only,
            .read_write => .read_write,
        };
    }
};

/// Independent read cursor over queue cycle files.
pub const Tailer = struct {
    dispatch_after: u64 = 0,
    state: TailerState = .not_yet_polled,
    dispatcher: ?*const fn (ctx: *anyopaque, index: u64, msg: []const u8) DispatchAction = null,
    dispatch_ctx: ?*anyopaque = null,
    collect: ?*RawCollected = null,

    mmap_protection: MmapProtection = .read_only,

    qf_cycle_open: u64 = std.math.maxInt(u64),
    qf_filename: ?[]const u8 = null,
    qf_fd: ?std.posix.fd_t = null,
    qf_file_size: u64 = 0,

    qf_tip: u64 = 0,
    qf_index: u64 = 0,

    qf_buf: ?[]align(config.page_alignment) u8 = null,
    qf_mmapoff: u64 = 0,
    qf_mmapsz: u64 = 0,

    last_modcount: u64 = 0,
    read_prefetch: ReadPrefetchState = .{},
    queue: *Queue,

    /// Initializes a tailer positioned at the requested public index.
    pub fn init(queue: *Queue, start_index: u64) Tailer {
        return .{
            .queue = queue,
            .dispatch_after = if (start_index == 0) 0 else start_index - 1,
            .qf_index = start_index,
        };
    }

    /// Allocates, registers, and optionally seeks a new tailer.
    pub fn create(queue: *Queue, start_index: u64) !*Tailer {
        const tailer = try queue.allocator.create(Tailer);
        errdefer queue.allocator.destroy(tailer);
        tailer.* = Tailer.init(queue, start_index);
        errdefer tailer.closeCycleFile();

        try queue.registerTailer(tailer);
        errdefer queue.unregisterTailerPrefetch(tailer);

        if (start_index != 0) try tailer.seekTo(start_index);
        return tailer;
    }

    /// Unregisters this tailer and releases its mapping resources.
    pub fn deinit(self: *Tailer) void {
        self.queue.unregisterTailerPrefetch(self);
        self.closeCycleFile();
        self.queue.allocator.destroy(self);
    }

    /// Non-blocking raw read from the current tailer position.
    pub fn pollRaw(self: *Tailer) !?RawEntry {
        var iterations: usize = 0;
        while (iterations < 1024) : (iterations += 1) {
            const cycle = Index.cycle(self.qf_index);
            self.ensureCycleFile(cycle) catch |err| switch (err) {
                error.FileNotFound => {
                    self.state = .awaiting_queue_file;
                    return null;
                },
                error.MmapFailed => {
                    self.state = .err_mmap;
                    return err;
                },
                else => return err,
            };

            const header_ptr = self.headerPtr(self.qf_tip) orelse {
                self.state = .extend_needed;
                return null;
            };
            const raw = @atomicLoad(u32, header_ptr, .acquire);
            if (Header.isUnallocated(raw)) {
                self.state = .awaiting_entry;
                return null;
            }
            if (Header.isWorking(raw)) {
                self.state = .busy;
                return null;
            }
            if (Header.isEof(raw)) {
                self.advanceToNextCycle();
                continue;
            }

            const payload_size = Header.dataLength(raw);
            const entry_size = Header.entrySize(payload_size);
            if (self.qf_tip + entry_size > self.qf_file_size) return error.MetadataCorrupt;

            if (Header.isMetadata(raw)) {
                self.qf_tip += entry_size;
                continue;
            }
            if (!Header.isData(raw)) return error.MetadataCorrupt;

            const payload = self.constSliceAt(self.qf_tip + Header.HEADER_SIZE, payload_size) orelse return error.MmapFailed;
            const index = self.qf_index;
            self.qf_tip += entry_size;
            self.qf_index += 1;
            self.state = .collected;
            self.updateReadPrefetch();
            return .{
                .index = index,
                .payload = payload,
                .raw_size = payload_size,
            };
        }

        self.state = .busy;
        return null;
    }

    /// Non-blocking typed read using a codec to parse the payload.
    pub fn pollWithCodec(
        self: *Tailer,
        comptime MessageType: type,
        codec: Codec(MessageType),
    ) !?Entry(MessageType) {
        const raw = (try self.pollRaw()) orelse return null;
        return .{
            .index = raw.index,
            .message = try codec.parsePayload(raw.payload),
            .raw_size = raw.raw_size,
        };
    }

    /// Blocking typed read loop that yields while waiting for data.
    pub fn collectWithCodec(
        self: *Tailer,
        comptime MessageType: type,
        codec: Codec(MessageType),
    ) !Entry(MessageType) {
        while (true) {
            if (try self.pollWithCodec(MessageType, codec)) |entry| return entry;
            std.Thread.yield() catch {};
        }
    }

    /// Drives read-side prefetch preparation for this tailer.
    pub fn prefetchPoll(self: *Tailer, max_work_units: u32) !StepResult {
        const buf = self.qf_buf orelse return .idle;
        self.updateReadPrefetch();
        if (self.queue.prefetcher) |prefetcher| {
            return try prefetcher.prepareReadableRange(
                self.qf_fd,
                buf,
                self.qf_mmapoff,
                self.qf_file_size,
                &self.read_prefetch,
                max_work_units,
            );
        }
        return .idle;
    }

    /// Seeks to a public index using the inline index, then scans forward.
    pub fn seekTo(self: *Tailer, target_index: u64) !void {
        const target_cycle = Index.cycle(target_index);
        const target_seqnum = Index.seqnum(target_index);
        try self.ensureCycleFile(target_cycle);

        const buf = self.qf_buf orelse return error.MmapFailed;
        const hdr: *const QueueFileHeader = @ptrCast(@alignCast(buf.ptr));
        const region = IndexRegion.fromMmap(buf.ptr, hdr);
        const point = region.seekOffset(target_seqnum, dataStartOffset(self.queue));
        self.qf_tip = point.offset;
        self.qf_index = Index.compose(target_cycle, point.seqnum);

        while (Index.seqnum(self.qf_index) < target_seqnum) {
            const skipped = try self.skipOne();
            if (!skipped) break;
        }
        self.dispatch_after = if (target_index == 0) 0 else target_index - 1;
    }

    /// Converts an absolute file offset to a mapped pointer when in range.
    pub fn offsetToPtr(self: *const Tailer, file_offset: u64) ?[*]u8 {
        const buf = self.qf_buf orelse return null;
        if (file_offset < self.qf_mmapoff) return null;
        const delta = file_offset - self.qf_mmapoff;
        if (delta >= self.qf_mmapsz or delta >= buf.len) return null;
        return buf.ptr + @as(usize, @intCast(delta));
    }

    fn skipOne(self: *Tailer) !bool {
        const header_ptr = self.headerPtr(self.qf_tip) orelse return false;
        const raw = @atomicLoad(u32, header_ptr, .acquire);
        if (Header.isUnallocated(raw) or Header.isWorking(raw)) return false;
        if (Header.isEof(raw)) {
            self.advanceToNextCycle();
            return false;
        }
        const payload_size = Header.dataLength(raw);
        self.qf_tip += Header.entrySize(payload_size);
        if (Header.isData(raw)) self.qf_index += 1;
        return true;
    }

    fn ensureCycleFile(self: *Tailer, cycle: u64) !void {
        if (self.qf_fd != null and self.qf_cycle_open == cycle) return;
        self.closeCycleFile();
        try self.openCycleFile(cycle);
    }

    fn openCycleFile(self: *Tailer, cycle: u64) !void {
        const path = try self.queue.cyclePath(cycle);
        errdefer self.queue.allocator.free(path);

        const file = try std.Io.Dir.cwd().openFile(self.queue.io, path, .{
            .mode = if (self.mmap_protection == .read_only) .read_only else .read_write,
            .allow_directory = false,
        });
        errdefer file.close(self.queue.io);

        const stat = try file.stat(self.queue.io);
        if (stat.size < dataStartOffset(self.queue) + Header.HEADER_SIZE) return error.InvalidQueueFileHeader;
        if (stat.size > std.math.maxInt(usize)) return error.MmapFailed;

        const buf = try mmap_ops.mapFileWithFallback(file.handle, 0, @intCast(stat.size), self.mmap_protection.toProtection(), .{});
        errdefer mmap_ops.unmapFile(buf);
        try validateQueueFileHeader(buf, self.queue, cycle);
        mmap_ops.adviseSequential(buf) catch {};

        self.qf_filename = path;
        self.qf_fd = file.handle;
        self.qf_file_size = stat.size;
        @atomicStore(u64, &self.qf_cycle_open, cycle, .release);
        self.qf_buf = buf;
        self.qf_mmapoff = 0;
        self.qf_mmapsz = stat.size;

        if (Index.cycle(self.qf_index) == cycle and Index.seqnum(self.qf_index) != 0) {
            if (self.qf_tip == 0) self.qf_tip = dataStartOffset(self.queue);
        } else {
            self.qf_tip = dataStartOffset(self.queue);
            self.qf_index = Index.compose(@intCast(cycle), 0);
        }
        self.read_prefetch.reset(cycle, self.qf_tip);
    }

    fn closeCycleFile(self: *Tailer) void {
        if (self.qf_buf) |buf| {
            mmap_ops.unmapFile(buf);
            self.qf_buf = null;
        }
        if (self.qf_fd) |fd| {
            closeFd(self.queue.io, fd);
            self.qf_fd = null;
        }
        if (self.qf_filename) |filename| {
            self.queue.allocator.free(filename);
            self.qf_filename = null;
        }
        self.qf_file_size = 0;
        self.qf_mmapoff = 0;
        self.qf_mmapsz = 0;
        @atomicStore(u64, &self.qf_cycle_open, std.math.maxInt(u64), .release);
    }

    fn advanceToNextCycle(self: *Tailer) void {
        const next_cycle = @as(u64, Index.cycle(self.qf_index)) + 1;
        self.closeCycleFile();
        self.qf_tip = 0;
        self.qf_index = Index.compose(@intCast(next_cycle), 0);
        self.state = .awaiting_queue_file;
    }

    fn headerPtr(self: *Tailer, abs_offset: u64) ?*const u32 {
        const slice = self.constSliceAt(abs_offset, Header.HEADER_SIZE) orelse return null;
        return @ptrCast(@alignCast(slice.ptr));
    }

    fn constSliceAt(self: *const Tailer, abs_offset: u64, len: usize) ?[]const u8 {
        const buf = self.qf_buf orelse return null;
        if (abs_offset < self.qf_mmapoff) return null;
        const rel = abs_offset - self.qf_mmapoff;
        if (rel > self.qf_mmapsz or @as(u64, len) > self.qf_mmapsz - rel) return null;
        if (rel > std.math.maxInt(usize)) return null;
        const start: usize = @intCast(rel);
        if (start > buf.len or len > buf.len - start) return null;
        return buf[start..][0..len];
    }

    fn updateReadPrefetch(self: *Tailer) void {
        const meta = self.queue.metadata orelse return;
        self.last_modcount = @atomicLoad(u64, &meta.modcount, .acquire);
        if (!self.read_prefetch.active or self.read_prefetch.cycle != self.qf_cycle_open) {
            self.read_prefetch.reset(self.qf_cycle_open, self.qf_tip);
        }
        self.read_prefetch.next_offset = self.qf_tip;
        self.read_prefetch.published_limit = @atomicLoad(u64, &meta.write_position, .acquire);
    }
};

fn dataStartOffset(queue: *const Queue) u64 {
    return @sizeOf(QueueFileHeader) + @as(u64, queue.index_count) * @sizeOf(u64);
}

fn validateQueueFileHeader(buf: []align(config.page_alignment) u8, queue: *const Queue, cycle: u64) !void {
    const hdr: *const QueueFileHeader = @ptrCast(@alignCast(buf.ptr));
    if (hdr.magic != metadata_mod.queue_file_magic) return error.QueueFileMagicMismatch;
    if (hdr.version != metadata_mod.format_version) return error.QueueFileVersionMismatch;
    if (hdr.roll_length_secs != queue.roll_length_secs or
        hdr.index_spacing != queue.index_spacing or
        hdr.index_count != queue.index_count)
    {
        return error.InvalidQueueFileHeader;
    }
    if (hdr.created_cycle != @as(u32, @intCast(cycle))) return error.InvalidQueueFileHeader;
}

fn closeFd(io: std.Io, fd: std.posix.fd_t) void {
    const file: std.Io.File = .{
        .handle = fd,
        .flags = .{ .nonblocking = false },
    };
    file.close(io);
}

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

test "tailer polls raw payloads appended to a queue" {
    const Appender = @import("appender.zig").Appender;
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    const queue = try Queue.init(allocator, path);
    defer queue.deinit();
    queue.setCreate(true);
    try queue.setRollSchemeName("TEST4_SECONDLY");
    try queue.open();

    const app = try Appender.open(queue);
    try std.testing.expectEqual(Index.compose(0, 0), try app.appendWithTimestamp("one", 0));
    try std.testing.expectEqual(Index.compose(0, 1), try app.appendWithTimestamp("two", 0));

    const tailer = try Tailer.create(queue, 0);
    const first = (try tailer.pollRaw()).?;
    try std.testing.expectEqual(Index.compose(0, 0), first.index);
    try std.testing.expectEqualStrings("one", first.payload);

    const second = (try tailer.pollRaw()).?;
    try std.testing.expectEqual(Index.compose(0, 1), second.index);
    try std.testing.expectEqualStrings("two", second.payload);
    try std.testing.expect(try tailer.pollRaw() == null);
}

test "tailer seek uses inline index then scans to target" {
    const Appender = @import("appender.zig").Appender;
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    const queue = try Queue.init(allocator, path);
    defer queue.deinit();
    queue.setCreate(true);
    try queue.setRollSchemeName("TEST4_SECONDLY");
    try queue.open();

    const app = try Appender.open(queue);
    _ = try app.appendWithTimestamp("zero", 0);
    _ = try app.appendWithTimestamp("one", 0);
    _ = try app.appendWithTimestamp("two", 0);
    _ = try app.appendWithTimestamp("three", 0);
    _ = try app.appendWithTimestamp("four", 0);

    const tailer = try Tailer.create(queue, Index.compose(0, 3));
    const entry = (try tailer.pollRaw()).?;
    try std.testing.expectEqual(Index.compose(0, 3), entry.index);
    try std.testing.expectEqualStrings("three", entry.payload);
}

fn tmpQueuePath(allocator: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
}
