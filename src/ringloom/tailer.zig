// SPDX-License-Identifier: Apache-2.0

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

const CycleMapping = struct {
    filename: []const u8,
    fd: std.posix.fd_t,
    file_size: u64,
    buf: []align(config.page_alignment) u8,
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

    ready_cycle: u64 = std.math.maxInt(u64),
    ready_filename: ?[]const u8 = null,
    ready_fd: ?std.posix.fd_t = null,
    ready_file_size: u64 = 0,
    ready_buf: ?[]align(config.page_alignment) u8 = null,

    last_modcount: u64 = 0,
    read_prefetch: ReadPrefetchState = .{},
    prefetch_cursor_cycle: u64 = std.math.maxInt(u64),
    prefetch_cursor_offset: u64 = 0,
    prefetch_mapping_lock: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
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
                    // The cycle file does not exist. If the queue was empty when
                    // this tailer was created (anchored at cycle 0) and data has
                    // since arrived at a higher, date-derived cycle, re-seek
                    // forward to the queue's real lowest cycle instead of
                    // blocking forever on a cycle file that will never appear.
                    // The live lowest_cycle is read atomically from the shared
                    // metadata mmap so this also works across processes (e.g. a
                    // subscriber tailing a replica filled by the broker).
                    if (self.queue.metadata) |meta| {
                        const live_lowest = @atomicLoad(u64, &meta.lowest_cycle, .acquire);
                        if (live_lowest > cycle) {
                            self.seekTo(Index.compose(@intCast(live_lowest), 0)) catch {};
                            continue;
                        }
                    }
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
            self.publishReadPrefetchCursor();
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
        if (max_work_units == 0) return .idle;

        var result: StepResult = .idle;
        read_prefetch: {
            self.lockPrefetchMapping();
            defer self.unlockPrefetchMapping();

            const buf = self.qf_buf orelse break :read_prefetch;
            const meta = self.queue.metadata orelse break :read_prefetch;
            const open_cycle = @atomicLoad(u64, &self.qf_cycle_open, .acquire);
            const cursor_cycle = @atomicLoad(u64, &self.prefetch_cursor_cycle, .acquire);
            if (open_cycle == std.math.maxInt(u64) or cursor_cycle != open_cycle) break :read_prefetch;

            const cursor_offset = @atomicLoad(u64, &self.prefetch_cursor_offset, .acquire);
            self.refreshReadPrefetchLocked(meta, open_cycle, cursor_offset);

            if (self.queue.prefetcher) |prefetcher| {
                result = try prefetcher.prepareReadableRange(
                    self.qf_fd,
                    buf,
                    self.qf_mmapoff,
                    self.qf_file_size,
                    &self.read_prefetch,
                    max_work_units,
                );
            }
        }
        return combineStepResults(result, try self.prepareNextCycle());
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
        self.publishReadPrefetchCursor();
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
        const mapping = try self.openCycleMapping(cycle);

        self.lockPrefetchMapping();
        self.qf_filename = mapping.filename;
        self.qf_fd = mapping.fd;
        self.qf_file_size = mapping.file_size;
        @atomicStore(u64, &self.qf_cycle_open, cycle, .release);
        self.qf_buf = mapping.buf;
        self.qf_mmapoff = 0;
        self.qf_mmapsz = mapping.file_size;

        if (Index.cycle(self.qf_index) == cycle and Index.seqnum(self.qf_index) != 0) {
            if (self.qf_tip == 0) self.qf_tip = dataStartOffset(self.queue);
        } else {
            self.qf_tip = dataStartOffset(self.queue);
            self.qf_index = Index.compose(@intCast(cycle), 0);
        }
        self.read_prefetch = .{};
        self.unlockPrefetchMapping();
        self.publishReadPrefetchCursor();
    }

    fn closeCycleFile(self: *Tailer) void {
        self.lockPrefetchMapping();
        defer self.unlockPrefetchMapping();
        self.closeActiveCycleLocked(false);
        self.closeReadyCycleLocked(false);
    }

    fn closeActiveCycleLocked(self: *Tailer, defer_cleanup: bool) void {
        @atomicStore(u64, &self.prefetch_cursor_cycle, std.math.maxInt(u64), .release);
        self.releaseResource(self.qf_buf, self.qf_fd, defer_cleanup);
        self.qf_buf = null;
        self.qf_fd = null;
        if (self.qf_filename) |filename| {
            self.queue.allocator.free(filename);
            self.qf_filename = null;
        }
        self.qf_file_size = 0;
        self.qf_mmapoff = 0;
        self.qf_mmapsz = 0;
        self.read_prefetch = .{};
        @atomicStore(u64, &self.qf_cycle_open, std.math.maxInt(u64), .release);
    }

    fn advanceToNextCycle(self: *Tailer) void {
        const next_cycle = @as(u64, Index.cycle(self.qf_index)) + 1;
        if (!self.tryUseReadyCycle(next_cycle)) {
            self.lockPrefetchMapping();
            self.closeActiveCycleLocked(true);
            self.unlockPrefetchMapping();
            self.qf_tip = 0;
            self.qf_index = Index.compose(@intCast(next_cycle), 0);
        }
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

    fn publishReadPrefetchCursor(self: *Tailer) void {
        const open_cycle = @atomicLoad(u64, &self.qf_cycle_open, .acquire);
        @atomicStore(u64, &self.prefetch_cursor_offset, self.qf_tip, .release);
        @atomicStore(u64, &self.prefetch_cursor_cycle, open_cycle, .release);
    }

    fn refreshReadPrefetchLocked(
        self: *Tailer,
        meta: *metadata_mod.SharedMetadata,
        open_cycle: u64,
        cursor_offset: u64,
    ) void {
        self.last_modcount = @atomicLoad(u64, &meta.modcount, .acquire);
        if (!self.read_prefetch.active or self.read_prefetch.cycle != open_cycle) {
            self.read_prefetch.reset(open_cycle, cursor_offset);
        }
        self.read_prefetch.cursor_offset = cursor_offset;
        if (self.read_prefetch.next_offset < cursor_offset) self.read_prefetch.next_offset = cursor_offset;
        const highest_cycle = @atomicLoad(u64, &meta.highest_cycle, .acquire);
        self.read_prefetch.published_limit = if (open_cycle < highest_cycle)
            self.qf_file_size
        else
            @atomicLoad(u64, &meta.write_position, .acquire);
    }

    fn prepareNextCycle(self: *Tailer) !StepResult {
        const meta = self.queue.metadata orelse return .idle;
        const open_cycle = @atomicLoad(u64, &self.qf_cycle_open, .acquire);
        if (open_cycle == std.math.maxInt(u64)) return .idle;
        const highest_cycle = @atomicLoad(u64, &meta.highest_cycle, .acquire);
        if (open_cycle >= highest_cycle) return .idle;
        const next_cycle = open_cycle + 1;

        self.lockPrefetchMapping();
        defer self.unlockPrefetchMapping();
        if (@atomicLoad(u64, &self.qf_cycle_open, .acquire) != open_cycle) return .idle;
        if (self.ready_cycle == next_cycle) return .idle;
        self.closeReadyCycleLocked(false);

        const mapping = try self.openCycleMapping(next_cycle);
        self.ready_cycle = next_cycle;
        self.ready_filename = mapping.filename;
        self.ready_fd = mapping.fd;
        self.ready_file_size = mapping.file_size;
        self.ready_buf = mapping.buf;
        return .made_progress;
    }

    fn tryUseReadyCycle(self: *Tailer, cycle: u64) bool {
        self.lockPrefetchMapping();
        defer self.unlockPrefetchMapping();
        if (self.ready_cycle != cycle) return false;

        self.closeActiveCycleLocked(true);
        self.qf_filename = self.ready_filename;
        self.qf_fd = self.ready_fd;
        self.qf_file_size = self.ready_file_size;
        @atomicStore(u64, &self.qf_cycle_open, cycle, .release);
        self.qf_buf = self.ready_buf;
        self.qf_mmapoff = 0;
        self.qf_mmapsz = self.ready_file_size;

        self.ready_cycle = std.math.maxInt(u64);
        self.ready_filename = null;
        self.ready_fd = null;
        self.ready_file_size = 0;
        self.ready_buf = null;

        self.qf_tip = dataStartOffset(self.queue);
        self.qf_index = Index.compose(@intCast(cycle), 0);
        self.read_prefetch = .{};
        self.publishReadPrefetchCursor();
        return true;
    }

    fn closeReadyCycleLocked(self: *Tailer, defer_cleanup: bool) void {
        self.releaseResource(self.ready_buf, self.ready_fd, defer_cleanup);
        if (self.ready_filename) |filename| {
            self.queue.allocator.free(filename);
        }
        self.ready_cycle = std.math.maxInt(u64);
        self.ready_filename = null;
        self.ready_fd = null;
        self.ready_file_size = 0;
        self.ready_buf = null;
    }

    fn releaseResource(
        self: *Tailer,
        buf: ?[]align(config.page_alignment) u8,
        fd: ?std.posix.fd_t,
        defer_cleanup: bool,
    ) void {
        if (buf == null and fd == null) return;
        if (defer_cleanup) {
            if (self.queue.cleaner) |cleaner| {
                if (cleaner.deferResource(buf, fd)) return;
            }
        }
        if (buf) |old_buf| mmap_ops.unmapFile(old_buf);
        if (fd) |old_fd| closeFd(self.queue.io, old_fd);
    }

    fn openCycleMapping(self: *Tailer, cycle: u64) !CycleMapping {
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

        return .{
            .filename = path,
            .fd = file.handle,
            .file_size = stat.size,
            .buf = buf,
        };
    }

    fn lockPrefetchMapping(self: *Tailer) void {
        while (self.prefetch_mapping_lock.cmpxchgWeak(false, true, .acquire, .monotonic)) |_| {
            std.atomic.spinLoopHint();
        }
    }

    fn unlockPrefetchMapping(self: *Tailer) void {
        self.prefetch_mapping_lock.store(false, .release);
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

fn combineStepResults(a: StepResult, b: StepResult) StepResult {
    if (a == .more_work or b == .more_work) return .more_work;
    if (a == .made_progress or b == .made_progress) return .made_progress;
    return .idle;
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
