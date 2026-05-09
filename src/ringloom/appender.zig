const std = @import("std");

const atomic_ops = @import("atomic_ops.zig");
const config = @import("config.zig");
const Header = @import("header.zig").Header;
const Index = @import("index.zig").Index;
const IndexRegion = @import("index.zig").IndexRegion;
const metadata_mod = @import("metadata.zig");
const QueueFileHeader = metadata_mod.QueueFileHeader;
const mmap_ops = @import("mmap_ops.zig");
const Queue = @import("queue.zig").Queue;

/// Prepared mmap window that can be swapped into the appender.
pub const MappedWindow = struct {
    buf: []align(config.page_alignment) u8,
    mmap_offset: u64,
    mmap_size: u64,
};

/// Counters that describe appender activity and fallback paths.
pub const AppenderDiagnostics = struct {
    appends: u64 = 0,
    rolls: u64 = 0,
    cas_retries: u64 = 0,
    synchronous_cycle_opens: u64 = 0,
    preroll_misses: u64 = 0,
};

/// Result of a raw append, including the borrowed mmap payload slice.
pub const AppendResult = struct {
    index: u64,
    payload: []const u8,
};

/// Single-writer append handle for a queue.
pub const Appender = struct {
    queue: *Queue,
    cycle: u64 = 0,
    fd: ?std.posix.fd_t = null,
    seqnum: u64 = 0,
    buf: ?[]align(config.page_alignment) u8 = null,
    mmap_offset: u64 = 0,
    mmap_size: u64 = 0,
    file_size: u64 = 0,
    tip: u64 = 0,
    ready_window: ?MappedWindow = null,
    owner_token: u64 = 0,
    diagnostics: AppenderDiagnostics = .{},

    /// Initializes appender state before the lifecycle lease is acquired.
    pub fn init(queue: *Queue) Appender {
        return .{
            .queue = queue,
            .tip = dataStartOffset(queue),
        };
    }

    /// Opens or reuses the appender for this queue handle.
    ///
    /// Cross-process exclusivity is enforced by the mapped appender lease; the
    /// Zig handle is reused so `Queue.append` can stay allocation-free after
    /// the first append.
    pub fn open(queue: *Queue) !*Appender {
        if (queue.appender) |existing| return existing;

        const appender = try queue.allocator.create(Appender);
        errdefer queue.allocator.destroy(appender);

        appender.* = Appender.init(queue);
        appender.owner_token = makeOwnerToken(appender);
        try queue.acquireAppenderLease(appender.owner_token);
        errdefer queue.releaseAppenderLease(appender.owner_token) catch {};

        queue.appender = appender;
        return appender;
    }

    /// Releases the lifecycle lease and destroys this appender handle.
    pub fn close(self: *Appender) void {
        const queue = self.queue;
        self.deinit();
        if (queue.appender == self) queue.appender = null;
        queue.allocator.destroy(self);
    }

    /// Releases mappings, file descriptors, and the lifecycle lease.
    pub fn deinit(self: *Appender) void {
        if (self.ready_window) |window| {
            mmap_ops.unmapFile(window.buf);
            self.ready_window = null;
        }
        const old_buf = self.buf;
        const old_fd = self.fd;
        self.buf = null;
        self.fd = null;
        self.deferOldResources(old_buf, old_fd);
        if (self.owner_token != 0) {
            self.queue.releaseAppenderLease(self.owner_token) catch {};
            self.owner_token = 0;
        }
    }

    /// Appends a raw payload using the current wall-clock cycle.
    pub fn append(self: *Appender, payload: []const u8) !u64 {
        return self.appendWithTimestamp(payload, try self.nowMs());
    }

    /// Appends a raw payload using an explicit UTC millisecond timestamp.
    pub fn appendWithTimestamp(self: *Appender, payload: []const u8, now_ms: u64) !u64 {
        return (try self.appendRaw(payload, now_ms)).index;
    }

    /// Appends bytes directly to mmap and returns the assigned index and payload view.
    pub fn appendRaw(self: *Appender, payload: []const u8, now_ms: u64) !AppendResult {
        if (payload.len == 0) return error.EmptyPayload;
        if (payload.len > Header.SIZE_MASK) return error.MessageTooLarge;

        const payload_len_u30: u30 = @intCast(payload.len);
        const entry_size = Header.entrySize(payload.len);
        if (entry_size > std.math.maxInt(u32)) return error.MessageTooLarge;

        const current_cycle = try self.cycleFromTimestamp(now_ms);
        try self.ensureCycle(current_cycle);

        const required_end = self.tip +| entry_size;
        if (required_end > Queue.qf_disk_sz) return error.MessageTooLarge;
        try self.ensureWindow(required_end);

        const entry_index = try self.currentIndex();
        const header_ptr = try self.headerPtr(self.tip);
        try self.claimSlot(header_ptr);

        const payload_buf = try self.sliceAt(self.tip + Header.HEADER_SIZE, payload.len);
        @memcpy(payload_buf, payload);

        const pad = Header.paddingFor(payload.len);
        if (pad != 0) {
            const padding = try self.sliceAt(self.tip + Header.HEADER_SIZE + payload.len, pad);
            @memset(padding, 0);
        }

        @atomicStore(u32, header_ptr, Header.dataHeader(payload_len_u30), .release);
        self.maybeUpdateIndex();

        self.tip = required_end;
        self.seqnum += 1;
        self.diagnostics.appends += 1;
        self.publishTip();

        self.queue.maybePreroll(now_ms) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        return .{
            .index = entry_index,
            .payload = payload_buf,
        };
    }

    /// Serializes and appends a typed message through the provided codec.
    pub fn appendWithCodec(
        self: *Appender,
        comptime MessageType: type,
        codec: @import("codec.zig").Codec(MessageType),
        msg: MessageType,
        now_ms: u64,
    ) !u64 {
        const payload_size = try codec.payloadSize(msg);
        if (payload_size == 0) return error.EmptyPayload;
        const entry_size = Header.entrySize(payload_size);
        if (entry_size > std.math.maxInt(u32)) return error.MessageTooLarge;

        const current_cycle = try self.cycleFromTimestamp(now_ms);
        try self.ensureCycle(current_cycle);

        const required_end = self.tip +| entry_size;
        if (required_end > Queue.qf_disk_sz) return error.MessageTooLarge;
        try self.ensureWindow(required_end);

        const entry_index = try self.currentIndex();
        const header_ptr = try self.headerPtr(self.tip);
        try self.claimSlot(header_ptr);

        const payload_buf = try self.sliceAt(self.tip + Header.HEADER_SIZE, payload_size);
        _ = try codec.writePayload(payload_buf, msg);

        const pad = Header.paddingFor(payload_size);
        if (pad != 0) {
            const padding = try self.sliceAt(self.tip + Header.HEADER_SIZE + payload_size, pad);
            @memset(padding, 0);
        }

        @atomicStore(u32, header_ptr, Header.dataHeader(@intCast(payload_size)), .release);
        self.maybeUpdateIndex();

        self.tip = required_end;
        self.seqnum += 1;
        self.diagnostics.appends += 1;
        self.publishTip();

        self.queue.maybePreroll(now_ms) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        return entry_index;
    }

    fn nowMs(self: *const Appender) !u64 {
        const ms = std.Io.Clock.real.now(self.queue.io).toMilliseconds();
        if (ms < 0) return error.InvalidRollConfig;
        return @intCast(ms);
    }

    fn cycleFromTimestamp(self: *const Appender, now_ms: u64) !u64 {
        if (now_ms > @as(u64, @intCast(std.math.maxInt(i64)))) return error.InvalidRollConfig;
        const cycle = self.queue.cycleFromMs(@intCast(now_ms));
        if (cycle > std.math.maxInt(u32)) return error.InvalidRollConfig;
        return cycle;
    }

    fn ensureCycle(self: *Appender, target_cycle: u64) !void {
        if (self.fd == null) {
            try self.openCycle(target_cycle);
            return;
        }
        if (target_cycle == self.cycle) return;
        if (target_cycle < self.cycle) return error.InvalidRollConfig;
        try self.rollCycle(target_cycle);
    }

    fn rollCycle(self: *Appender, target_cycle: u64) !void {
        if (self.buf != null and self.tip + Header.HEADER_SIZE <= self.file_size) {
            const eof_ptr = try self.headerPtr(self.tip);
            @atomicStore(u32, eof_ptr, Header.EOF, .release);
        }

        if (self.buf) |buf| {
            mmap_ops.unmapFile(buf);
            self.buf = null;
        }
        if (self.fd) |fd| {
            closeFd(self.queue.io, fd);
            self.fd = null;
        }

        self.queue.lockPreroll();
        defer self.queue.unlockPreroll();

        if (self.queue.preroll_cycle != null and self.queue.preroll_cycle.? == target_cycle) {
            self.fd = self.queue.preroll_fd;
            self.buf = self.queue.preroll_mmap;
            self.mmap_offset = 0;
            self.mmap_size = if (self.buf) |buf| buf.len else 0;
            self.file_size = Queue.qf_disk_sz;
            self.queue.preroll_fd = null;
            self.queue.preroll_mmap = null;
            self.queue.preroll_cycle = null;
        } else {
            self.diagnostics.preroll_misses += 1;
            try self.openCycleFresh(target_cycle);
        }

        @atomicStore(u64, &self.cycle, target_cycle, .release);
        self.tip = dataStartOffset(self.queue);
        self.seqnum = 0;
        self.diagnostics.rolls += 1;
        self.publishCycle(target_cycle);
    }

    fn openCycle(self: *Appender, cycle: u64) !void {
        try self.openCycleFresh(cycle);
        @atomicStore(u64, &self.cycle, cycle, .release);
        self.publishCycle(cycle);
        self.publishTip();
    }

    fn openCycleFresh(self: *Appender, cycle: u64) !void {
        const path = try self.queue.cyclePath(cycle);
        defer self.queue.allocator.free(path);

        var created = false;
        var fd: std.posix.fd_t = undefined;
        var stat_size: u64 = Queue.qf_disk_sz;

        const cwd = std.Io.Dir.cwd();
        const file = cwd.openFile(self.queue.io, path, .{
            .mode = .read_write,
            .allow_directory = false,
        }) catch |err| switch (err) {
            error.FileNotFound => blk: {
                fd = try self.queue.queuefileInit(path, cycle);
                created = true;
                break :blk null;
            },
            else => return err,
        };
        if (file) |opened| {
            fd = opened.handle;
            stat_size = (try opened.stat(self.queue.io)).size;
        }
        errdefer closeFd(self.queue.io, fd);

        if (stat_size < dataStartOffset(self.queue) + Header.HEADER_SIZE) return error.InvalidQueueFileHeader;
        if (stat_size > std.math.maxInt(usize)) return error.MmapFailed;

        const buf = try mmap_ops.mapFileWithFallback(fd, 0, @intCast(stat_size), .read_write, .{
            .populate = self.queue.platform.supports_map_populate,
            .huge_tlb = self.queue.use_huge_pages and self.queue.platform.supports_huge_pages,
        });
        errdefer mmap_ops.unmapFile(buf);

        try validateQueueFileHeader(buf, self.queue, cycle);

        self.fd = fd;
        self.buf = buf;
        self.mmap_offset = 0;
        self.mmap_size = stat_size;
        self.file_size = stat_size;
        self.diagnostics.synchronous_cycle_opens += 1;

        if (created) {
            self.tip = dataStartOffset(self.queue);
            self.seqnum = 0;
        } else {
            self.recoverTipAndSeqnum();
        }
    }

    fn recoverTipAndSeqnum(self: *Appender) void {
        var offset = dataStartOffset(self.queue);
        var seq: u64 = 0;

        while (offset + Header.HEADER_SIZE <= self.file_size) {
            const header_ptr = self.headerPtr(offset) catch break;
            const raw = @atomicLoad(u32, header_ptr, .acquire);
            if (Header.isUnallocated(raw) or Header.isWorking(raw) or Header.isEof(raw)) break;

            const len = Header.dataLength(raw);
            offset += Header.entrySize(len);
            if (Header.isData(raw)) seq += 1;
        }

        self.tip = offset;
        self.seqnum = seq;
    }

    fn ensureWindow(self: *Appender, required_end: u64) !void {
        if (self.buf == null) return error.MmapFailed;
        if (required_end > self.file_size) return error.MessageTooLarge;
        if (required_end > self.mmap_offset and required_end <= self.mmap_offset + self.mmap_size) return;
        return error.MmapFailed;
    }

    fn currentIndex(self: *const Appender) !u64 {
        if (self.cycle > std.math.maxInt(u32) or self.seqnum > std.math.maxInt(u32)) {
            return error.InvalidRollConfig;
        }
        return Index.compose(@intCast(self.cycle), @intCast(self.seqnum));
    }

    fn claimSlot(self: *Appender, ptr: *u32) !void {
        var attempt: u32 = 0;
        while (true) {
            const prev = @cmpxchgStrong(
                u32,
                ptr,
                Header.UNALLOCATED,
                Header.WORKING,
                .monotonic,
                .monotonic,
            );
            if (prev == null) return;
            if (prev.? != Header.WORKING) return error.WriteConflict;
            self.diagnostics.cas_retries += 1;
            atomic_ops.casBackoff(attempt);
            attempt +|= 1;
            if (attempt > 4096) return error.WriteConflict;
        }
    }

    fn maybeUpdateIndex(self: *Appender) void {
        const buf = self.buf orelse return;
        if (self.seqnum > std.math.maxInt(u32)) return;
        const region = IndexRegion.fromMmap(buf.ptr, @ptrCast(@alignCast(buf.ptr)));
        if (region.slotFor(@intCast(self.seqnum))) |slot| {
            region.store(slot, self.tip);
        }
    }

    fn publishTip(self: *Appender) void {
        const meta = self.queue.metadata orelse return;
        @atomicStore(u64, &meta.write_position, self.tip, .release);
        _ = @atomicRmw(u64, &meta.modcount, .Add, 1, .acq_rel);
        self.queue.modcount = @atomicLoad(u64, &meta.modcount, .acquire);
    }

    fn publishCycle(self: *Appender, cycle: u64) void {
        const meta = self.queue.metadata orelse return;
        const highest = @atomicLoad(u64, &meta.highest_cycle, .acquire);
        if (cycle > highest) @atomicStore(u64, &meta.highest_cycle, cycle, .release);

        const lowest = @atomicLoad(u64, &meta.lowest_cycle, .acquire);
        const modcount = @atomicLoad(u64, &meta.modcount, .acquire);
        if (lowest == 0 and cycle != 0 and modcount == 0) {
            @atomicStore(u64, &meta.lowest_cycle, cycle, .release);
        }

        self.queue.highest_cycle = @atomicLoad(u64, &meta.highest_cycle, .acquire);
        self.queue.lowest_cycle = @atomicLoad(u64, &meta.lowest_cycle, .acquire);
    }

    fn headerPtr(self: *Appender, abs_offset: u64) !*u32 {
        const slice = try self.sliceAt(abs_offset, Header.HEADER_SIZE);
        return @ptrCast(@alignCast(slice.ptr));
    }

    fn sliceAt(self: *Appender, abs_offset: u64, len: usize) ![]u8 {
        const buf = self.buf orelse return error.MmapFailed;
        if (abs_offset < self.mmap_offset) return error.MmapFailed;
        const rel = abs_offset - self.mmap_offset;
        if (rel > self.mmap_size or @as(u64, len) > self.mmap_size - rel) return error.MmapFailed;
        if (rel > std.math.maxInt(usize)) return error.MmapFailed;
        const start: usize = @intCast(rel);
        if (start > buf.len or len > buf.len - start) return error.MmapFailed;
        return buf[start..][0..len];
    }

    fn deferOldResources(
        self: *Appender,
        buf: ?[]align(config.page_alignment) u8,
        fd: ?std.posix.fd_t,
    ) void {
        if (buf == null and fd == null) return;
        if (self.queue.cleaner) |cleaner| {
            if (cleaner.deferResource(buf, fd)) return;
        }
        if (buf) |old_buf| mmap_ops.unmapFile(old_buf);
        if (fd) |old_fd| closeFd(self.queue.io, old_fd);
    }
};

/// Returns the first data byte after the file header and inline index.
pub fn dataStartOffset(queue: *const Queue) u64 {
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

fn makeOwnerToken(ptr: *const Appender) u64 {
    const token = @intFromPtr(ptr);
    return if (token == 0) 1 else token;
}

fn closeFd(io: std.Io, fd: std.posix.fd_t) void {
    const file: std.Io.File = .{
        .handle = fd,
        .flags = .{ .nonblocking = false },
    };
    file.close(io);
}

test "appender writes raw payloads and inline index entries" {
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
    const idx0 = try app.appendWithTimestamp("abcd", 0);
    const idx1 = try app.appendWithTimestamp("ef", 0);

    try std.testing.expectEqual(Index.compose(0, 0), idx0);
    try std.testing.expectEqual(Index.compose(0, 1), idx1);
    try std.testing.expectEqual(@as(u64, dataStartOffset(queue) + Header.entrySize(4) + Header.entrySize(2)), app.tip);

    const region = IndexRegion.fromMmap(app.buf.?.ptr, @ptrCast(@alignCast(app.buf.?.ptr)));
    try std.testing.expectEqual(dataStartOffset(queue), region.lookup(0).?);
}

test "appender rejects empty payload because header zero is unallocated" {
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
    try std.testing.expectError(error.EmptyPayload, app.appendWithTimestamp("", 0));
}

test "appender rolls cycles and resets per-cycle seqnum" {
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
    try std.testing.expectEqual(Index.compose(0, 0), try app.appendWithTimestamp("a", 0));
    try std.testing.expectEqual(Index.compose(1, 0), try app.appendWithTimestamp("b", 1000));
    try std.testing.expectEqual(@as(u64, 1), app.cycle);
    try std.testing.expectEqual(@as(u64, 1), app.seqnum);
}

fn tmpQueuePath(allocator: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
}
