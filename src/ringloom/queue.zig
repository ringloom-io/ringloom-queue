const std = @import("std");

const config = @import("config.zig");
const Index = @import("index.zig").Index;
const QueueFileHeader = @import("metadata.zig").QueueFileHeader;
const RollScheme = @import("roll.zig").RollScheme;
const roll = @import("roll.zig");
const javaFormatToStrftime = @import("roll.zig").javaFormatToStrftime;
const rollCycleFromMs = @import("roll.zig").cycleFromMs;
const metadata_mod = @import("metadata.zig");
const SharedMetadata = metadata_mod.SharedMetadata;
const Appender = @import("appender.zig").Appender;
const Prefetcher = @import("prefetcher.zig").Prefetcher;
const Cleaner = @import("cleaner.zig").Cleaner;
const Platform = @import("platform.zig").Platform;
const StepResult = @import("platform.zig").StepResult;
const Tailer = @import("tailer.zig").Tailer;
const mmap_ops = @import("mmap_ops.zig");

/// Supported on-disk format versions.
pub const Version = enum(u8) {
    unknown = 0,
    v1 = 1,
};

/// Detects the queue format version by reading the metadata magic.
pub fn detectVersion(dirname: []const u8) !Version {
    const io = defaultIo();
    const cwd = std.Io.Dir.cwd();
    var dir = openDirPath(io, cwd, dirname, .{}) catch |err| switch (err) {
        error.FileNotFound => return .unknown,
        else => return err,
    };
    defer dir.close(io);

    var magic_buf: [4]u8 = undefined;
    const read = dir.readFile(io, config.metadata_filename, &magic_buf) catch |err| switch (err) {
        error.FileNotFound => return .unknown,
        else => return err,
    };
    if (read.len < magic_buf.len) return error.MetadataCorrupt;

    const magic = std.mem.readInt(u32, &magic_buf, .little);
    if (magic != metadata_mod.metadata_magic) return error.InvalidMagic;
    return .v1;
}

/// Core untyped queue lifecycle and shared metadata owner.
pub const Queue = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    dirname: []const u8,

    blocksize: u32 = config.default_blocksize,
    create: bool = false,
    version: Version = .unknown,

    metadata_fd: ?std.posix.fd_t = null,
    metadata_mmap: ?[]align(config.page_alignment) u8 = null,
    metadata: ?*SharedMetadata = null,

    queuefile_paths: std.ArrayList([]const u8) = .empty,
    highest_cycle: u64 = 0,
    lowest_cycle: u64 = 0,
    modcount: u64 = 0,

    roll_length_ms: u64 = 0,
    roll_length_secs: u32 = 0,
    roll_epoch: i64 = 0,
    roll_format: ?[]const u8 = null,
    roll_name: ?[]const u8 = null,
    roll_strftime: ?[]const u8 = null,

    index_count: u32 = 0,
    index_spacing: u32 = 0,
    cycle_shift: u6 = Index.cycle_shift,
    seqnum_mask: u64 = Index.seqnum_mask,

    preroll_fd: ?std.posix.fd_t = null,
    preroll_mmap: ?[]align(config.page_alignment) u8 = null,
    preroll_cycle: ?u64 = null,
    preroll_ms: u64 = config.default_preroll_ms,

    platform: Platform,
    use_huge_pages: bool = false,
    enable_prefetcher: bool = true,
    prefetch_runway_bytes: u64 = 8 * 1024 * 1024,
    read_prefetch_runway_bytes: u64 = 4 * 1024 * 1024,
    enable_cleaner: bool = true,
    spawn_helper_threads: bool = true,
    retention_cycles: ?u32 = null,
    prefetcher: ?*Prefetcher = null,
    cleaner: ?*Cleaner = null,

    tailers: std.ArrayList(*Tailer) = .empty,
    appender: ?*Appender = null,

    last_error: ?[]const u8 = null,

    pub const qf_disk_sz: u64 = config.default_qf_disk_size;
    pub const metadata_file_sz: u64 = @sizeOf(SharedMetadata);

    /// Allocates queue state for a directory path.
    pub fn init(allocator: std.mem.Allocator, dirname: []const u8) !*Queue {
        const queue = try allocator.create(Queue);
        queue.* = .{
            .allocator = allocator,
            .io = defaultIo(),
            .dirname = try allocator.dupe(u8, dirname),
            .platform = Platform.detect(),
        };
        return queue;
    }

    /// Sets the roll scheme used when creating or validating metadata.
    pub fn setRollScheme(self: *Queue, scheme: RollScheme) !void {
        if (self.roll_name) |n| self.allocator.free(n);
        if (self.roll_format) |f| self.allocator.free(f);
        if (self.roll_strftime) |s| self.allocator.free(s);

        self.roll_name = try self.allocator.dupe(u8, scheme.name);
        self.roll_format = try self.allocator.dupe(u8, scheme.format_str);
        self.roll_length_ms = scheme.rollLengthMs();
        self.roll_length_secs = scheme.roll_length_secs;
        self.index_count = scheme.index_count;
        self.index_spacing = scheme.index_spacing;

        var sf_buf: [128]u8 = undefined;
        var cl_buf: [128]u8 = undefined;
        const result = try javaFormatToStrftime(scheme.format_str, &sf_buf, &cl_buf);
        self.roll_strftime = try self.allocator.dupe(u8, result.strftime);
    }

    /// Sets the roll scheme by its stable name.
    pub fn setRollSchemeName(self: *Queue, name: []const u8) !void {
        const scheme = roll.findSchemeByName(name) orelse return error.UnknownRollScheme;
        try self.setRollScheme(scheme);
    }

    /// Allows or disallows creating a missing queue directory and metadata file.
    pub fn setCreate(self: *Queue, permitted: bool) void {
        self.create = permitted;
    }

    /// Sets the block size used for mapping and extension decisions.
    pub fn setBlocksize(self: *Queue, blocksize: u32) void {
        self.blocksize = blocksize;
    }

    /// Sets how close to a cycle boundary pre-roll creation should start.
    pub fn setPrerollMs(self: *Queue, ms: u64) void {
        self.preroll_ms = ms;
    }

    /// Enables best-effort Linux huge-page mappings for queue files.
    pub fn setUseHugePages(self: *Queue, enabled: bool) void {
        self.use_huge_pages = enabled;
    }

    /// Configures write-side prefetching.
    pub fn setPrefetcher(self: *Queue, enabled: bool, runway_bytes: u64) void {
        self.enable_prefetcher = enabled;
        self.prefetch_runway_bytes = runway_bytes;
    }

    /// Configures read-side prefetch runway length.
    pub fn setReadPrefetcher(self: *Queue, runway_bytes: u64) void {
        self.read_prefetch_runway_bytes = runway_bytes;
    }

    /// Records whether native helper threads are allowed for maintenance work.
    pub fn setHelperThreads(self: *Queue, enabled: bool) void {
        self.spawn_helper_threads = enabled;
    }

    /// Configures cleaner availability and retention policy.
    pub fn setCleaner(self: *Queue, enabled: bool, retention_cycles: ?u32) void {
        self.enable_cleaner = enabled;
        self.retention_cycles = retention_cycles;
    }

    /// Doubles the configured block size.
    pub fn doubleBlocksize(self: *Queue) void {
        self.blocksize <<= 1;
    }

    /// Converts a UTC millisecond timestamp to a queue cycle number.
    pub fn cycleFromMs(self: *const Queue, ms: i64) u64 {
        return rollCycleFromMs(ms, self.roll_epoch, self.roll_length_ms);
    }

    /// Returns the detected or created metadata version.
    pub fn getVersion(self: *const Queue) Version {
        return self.version;
    }

    /// Registers a tailer so queue teardown can release it.
    pub fn registerTailer(self: *Queue, tailer: *Tailer) !void {
        try self.tailers.append(self.allocator, tailer);
    }

    /// Removes a tailer registration when the caller closes it.
    pub fn unregisterTailerPrefetch(self: *Queue, tailer: *Tailer) void {
        for (self.tailers.items, 0..) |registered, i| {
            if (registered == tailer) {
                _ = self.tailers.swapRemove(i);
                return;
            }
        }
    }

    /// Opens or reuses this handle's single appender.
    pub fn openAppender(self: *Queue) !*Appender {
        return Appender.open(self);
    }

    /// Opens an independent tailer at a public index.
    pub fn openTailer(self: *Queue, start_index: u64) !*Tailer {
        return Tailer.create(self, start_index);
    }

    /// Opens existing metadata or creates a new queue when configured.
    pub fn open(self: *Queue) !void {
        if (self.metadata != null) return error.OpenFailed;

        var dir = openQueueDir(self, false) catch |err| switch (err) {
            error.FileNotFound => {
                if (!self.create) return error.QueueNotFound;
                try std.Io.Dir.cwd().createDirPath(self.io, self.dirname);
                return try self.createNew();
            },
            else => return err,
        };
        defer dir.close(self.io);

        self.version = try detectVersion(self.dirname);
        switch (self.version) {
            .unknown => {
                if (!self.create) return error.QueueNotFound;
                return try self.createNew();
            },
            .v1 => {},
        }

        const file = try dir.openFile(self.io, config.metadata_filename, .{
            .mode = .read_write,
            .allow_directory = false,
        });
        self.metadata_fd = file.handle;
        errdefer self.closeMetadata();

        const mapped = try mmap_ops.mapSharedMetadata(file.handle, .read_write);
        self.metadata_mmap = mapped.buf;
        self.metadata = mapped.metadata;

        try self.loadAndValidateMetadata();
        try self.refreshQueueFiles();
        try self.initMaintenance();

        self.preroll_fd = null;
        self.preroll_mmap = null;
        self.preroll_cycle = null;
    }

    fn createNew(self: *Queue) !void {
        try self.ensureCreateConfig();
        try self.metadataInit();
        errdefer self.closeMetadata();

        self.version = .v1;
        self.highest_cycle = 0;
        self.lowest_cycle = 0;
        self.modcount = 0;

        try self.refreshQueueFiles();
        try self.initMaintenance();
    }

    fn metadataInit(self: *Queue) !void {
        var dir = try openQueueDir(self, false);
        defer dir.close(self.io);

        const file = try dir.createFile(self.io, config.metadata_filename, .{
            .read = true,
            .truncate = false,
            .exclusive = true,
        });
        errdefer file.close(self.io);

        try self.preallocateBestEffort(file.handle, metadata_file_sz);
        try file.setLength(self.io, metadata_file_sz);

        const mapped = try mmap_ops.mapSharedMetadata(file.handle, .read_write);
        errdefer mapped.unmap();

        const epoch_ms = try self.metadataEpoch();
        mapped.metadata.* = SharedMetadata.init(
            self.roll_length_secs,
            self.index_spacing,
            self.index_count,
            epoch_ms,
        );
        @atomicStore(u64, &mapped.metadata.highest_cycle, 0, .release);
        @atomicStore(u64, &mapped.metadata.lowest_cycle, 0, .release);
        @atomicStore(u64, &mapped.metadata.modcount, 0, .release);
        @atomicStore(u64, &mapped.metadata.write_position, 0, .release);
        @atomicStore(u64, &mapped.metadata.appender_lock, 0, .release);
        @memset(&mapped.metadata._reserved, 0);
        try std.posix.msync(mapped.buf, std.posix.MSF.SYNC);

        self.metadata_fd = file.handle;
        self.metadata_mmap = mapped.buf;
        self.metadata = mapped.metadata;
    }

    fn loadAndValidateMetadata(self: *Queue) !void {
        const meta = self.metadata orelse return error.MetadataFieldsMissing;
        if (meta.magic != metadata_mod.metadata_magic) return error.InvalidMagic;
        if (meta.version != metadata_mod.format_version) return error.InvalidVersion;
        if (meta.roll_length_secs == 0 or meta.index_count == 0 or meta.index_spacing == 0) {
            return error.InvalidRollConfig;
        }

        const metadata_roll_ms = @as(u64, meta.roll_length_secs) * 1000;
        if (self.roll_length_ms != 0) {
            if (self.roll_length_ms != metadata_roll_ms or
                self.index_count != meta.index_count or
                self.index_spacing != meta.index_spacing)
            {
                return error.RollConfigMismatch;
            }
        } else {
            const scheme = findUniqueScheme(meta.roll_length_secs, meta.index_count, meta.index_spacing) orelse {
                return error.RollConfigMismatch;
            };
            try self.setRollScheme(scheme);
        }

        if (meta.epoch_ms > @as(u64, @intCast(std.math.maxInt(i64)))) return error.InvalidRollConfig;
        self.roll_epoch = @intCast(meta.epoch_ms);
        self.roll_length_secs = meta.roll_length_secs;
        self.roll_length_ms = metadata_roll_ms;
        self.index_count = meta.index_count;
        self.index_spacing = meta.index_spacing;
        self.highest_cycle = @atomicLoad(u64, &meta.highest_cycle, .acquire);
        self.lowest_cycle = @atomicLoad(u64, &meta.lowest_cycle, .acquire);
        self.modcount = @atomicLoad(u64, &meta.modcount, .acquire);

        try self.validateConfig();
    }

    /// Refreshes the sorted list of cycle files in the queue directory.
    pub fn refreshQueueFiles(self: *Queue) !void {
        for (self.queuefile_paths.items) |path| {
            self.allocator.free(path);
        }
        self.queuefile_paths.clearRetainingCapacity();

        var dir = try openQueueDir(self, true);
        defer dir.close(self.io);

        var iter = dir.iterate();
        while (try iter.next(self.io)) |entry| {
            if (entry.kind != .file) continue;
            if (std.mem.eql(u8, entry.name, config.metadata_filename)) continue;
            if (!std.mem.endsWith(u8, entry.name, config.queue_file_extension)) continue;

            const full_path = try std.Io.Dir.path.join(self.allocator, &.{ self.dirname, entry.name });
            try self.queuefile_paths.append(self.allocator, full_path);
        }

        std.mem.sort([]const u8, self.queuefile_paths.items, {}, struct {
            fn lessThan(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.lessThan(u8, a, b);
            }
        }.lessThan);
    }

    /// Creates and initializes a queue data file for `cycle`.
    pub fn queuefileInit(self: *Queue, path: []const u8, cycle: u64) !std.posix.fd_t {
        if (cycle > std.math.maxInt(u32)) return error.InvalidRollConfig;
        try self.validateConfig();

        const file = try std.Io.Dir.cwd().createFile(self.io, path, .{
            .read = true,
            .truncate = false,
            .exclusive = true,
        });
        errdefer file.close(self.io);

        try self.preallocateBestEffort(file.handle, qf_disk_sz);
        try file.setLength(self.io, qf_disk_sz);

        const header_plus_index_sz = @sizeOf(QueueFileHeader) + @as(u64, self.index_count) * @sizeOf(u64);
        const map_sz_u64 = std.mem.alignForward(u64, header_plus_index_sz, std.heap.pageSize());
        if (map_sz_u64 > std.math.maxInt(usize)) return error.MmapFailed;

        const buf = try mmap_ops.mapFile(file.handle, 0, @intCast(map_sz_u64), .read_write, .{});
        errdefer mmap_ops.unmapFile(buf);

        const hdr: *QueueFileHeader = @ptrCast(@alignCast(buf.ptr));
        hdr.* = QueueFileHeader.init(
            self.roll_length_secs,
            self.index_spacing,
            self.index_count,
            try self.metadataEpoch(),
            @intCast(cycle),
        );

        const index_byte_len = @as(usize, self.index_count) * @sizeOf(u64);
        const index_start = @sizeOf(QueueFileHeader);
        @memset(buf[index_start..][0..index_byte_len], 0);
        try std.posix.msync(buf, std.posix.MSF.SYNC);
        mmap_ops.unmapFile(buf);

        return file.handle;
    }

    /// Allocates the path for a queue data file cycle.
    pub fn cyclePath(self: *Queue, cycle: u64) ![]u8 {
        if (self.roll_strftime == null) return error.RollFormatMissing;
        if (self.roll_length_secs == 0) return error.InvalidRollConfig;

        const cycle_i64: i64 = if (cycle <= @as(u64, @intCast(std.math.maxInt(i64))))
            @intCast(cycle)
        else
            return error.InvalidRollConfig;
        const roll_secs_i64: i64 = @intCast(self.roll_length_secs);
        if (cycle_i64 > @divTrunc(std.math.maxInt(i64), roll_secs_i64)) {
            return error.InvalidRollConfig;
        }
        const cycle_seconds = cycle_i64 * roll_secs_i64;
        const epoch_seconds = @divFloor(self.roll_epoch, 1000);
        if (epoch_seconds > std.math.maxInt(i64) - cycle_seconds) {
            return error.InvalidRollConfig;
        }
        const timestamp_seconds = epoch_seconds + cycle_seconds;

        var date_buf: [64]u8 = undefined;
        const date = try roll.formatTimestamp(&date_buf, timestamp_seconds, self.roll_strftime.?);
        return std.fmt.allocPrint(self.allocator, "{s}/{s}{s}", .{
            self.dirname,
            date,
            config.queue_file_extension,
        });
    }

    /// Pre-creates and maps the next cycle file when near a roll boundary.
    pub fn maybePreroll(self: *Queue, current_time_ms: u64) !void {
        if (self.preroll_ms == 0 or self.preroll_cycle != null) return;
        try self.validateConfig();

        const epoch_ms = try self.metadataEpoch();
        const current_cycle: u64 = if (current_time_ms <= epoch_ms)
            0
        else
            (current_time_ms - epoch_ms) / self.roll_length_ms;
        if (current_cycle == std.math.maxInt(u64)) return error.InvalidRollConfig;
        const next_cycle = current_cycle + 1;
        if (next_cycle > std.math.maxInt(u64) / self.roll_length_ms) return error.InvalidRollConfig;
        const cycle_offset_ms = next_cycle * self.roll_length_ms;
        if (epoch_ms > std.math.maxInt(u64) - cycle_offset_ms) return error.InvalidRollConfig;
        const cycle_end_ms = epoch_ms + cycle_offset_ms;
        if (current_time_ms >= cycle_end_ms) return;
        if (cycle_end_ms - current_time_ms >= self.preroll_ms) return;

        const path = try self.cyclePath(next_cycle);
        defer self.allocator.free(path);

        const fd = try self.queuefileInit(path, next_cycle);
        errdefer closeFd(self.io, fd);

        const target_map_sz = @min(@as(u64, self.blocksize) * 2, qf_disk_sz);
        const map_sz_u64 = std.mem.alignForward(u64, target_map_sz, std.heap.pageSize());
        if (map_sz_u64 > std.math.maxInt(usize)) return error.MmapFailed;

        const buf = try mmap_ops.mapFileWithFallback(fd, 0, @intCast(map_sz_u64), .read_write, .{
            .populate = self.platform.supports_map_populate,
            .huge_tlb = self.use_huge_pages and self.platform.supports_huge_pages,
        });
        errdefer mmap_ops.unmapFile(buf);

        touchWritablePagesPastHeader(buf, self.platform.page_size);

        self.preroll_fd = fd;
        self.preroll_mmap = buf;
        self.preroll_cycle = next_cycle;
    }

    /// Acquires the on-disk single-appender lifecycle lease.
    pub fn acquireAppenderLease(self: *Queue, owner_token: u64) !void {
        if (owner_token == 0) return error.AppenderAlreadyOpen;
        const meta = self.metadata orelse return error.MetadataFieldsMissing;
        const prev = @cmpxchgStrong(
            u64,
            &meta.appender_lock,
            0,
            owner_token,
            .acquire,
            .monotonic,
        );
        if (prev != null) return error.AppenderAlreadyOpen;
    }

    /// Releases the on-disk appender lifecycle lease.
    pub fn releaseAppenderLease(self: *Queue, owner_token: u64) !void {
        if (owner_token == 0) return error.AppenderLeaseLost;
        const meta = self.metadata orelse return error.MetadataFieldsMissing;
        const prev = @cmpxchgStrong(
            u64,
            &meta.appender_lock,
            owner_token,
            0,
            .release,
            .monotonic,
        );
        if (prev != null) return error.AppenderLeaseLost;
    }

    /// Drives bounded prefetcher and cleaner work.
    pub fn maintenancePoll(self: *Queue, max_work_units: usize) !StepResult {
        var result: StepResult = .idle;
        if (self.prefetcher) |prefetcher| {
            result = combineStepResults(result, prefetcher.poll(max_work_units));
        }
        if (self.cleaner) |cleaner| {
            result = combineStepResults(result, cleaner.poll(max_work_units));
        }
        return result;
    }

    /// Releases all queue-owned memory, mappings, descriptors, and helper state.
    pub fn deinit(self: *Queue) void {
        while (self.tailers.pop()) |tailer| {
            tailer.deinit();
        }
        self.tailers.deinit(self.allocator);

        if (self.appender) |appender| {
            appender.deinit();
            self.allocator.destroy(appender);
            self.appender = null;
        }
        if (self.prefetcher) |prefetcher| {
            prefetcher.deinit();
            self.allocator.destroy(prefetcher);
            self.prefetcher = null;
        }
        if (self.cleaner) |cleaner| {
            cleaner.deinit();
            self.allocator.destroy(cleaner);
            self.cleaner = null;
        }

        if (self.preroll_mmap) |buf| {
            mmap_ops.unmapFile(buf);
            self.preroll_mmap = null;
        }
        if (self.preroll_fd) |fd| {
            closeFd(self.io, fd);
            self.preroll_fd = null;
        }
        self.preroll_cycle = null;
        if (self.metadata_mmap) |buf| {
            mmap_ops.unmapFile(buf);
            self.metadata_mmap = null;
            self.metadata = null;
        }
        if (self.metadata_fd) |fd| {
            closeFd(self.io, fd);
            self.metadata_fd = null;
        }

        for (self.queuefile_paths.items) |path| {
            self.allocator.free(path);
        }
        self.queuefile_paths.deinit(self.allocator);

        if (self.roll_format) |f| self.allocator.free(f);
        if (self.roll_name) |n| self.allocator.free(n);
        if (self.roll_strftime) |s| self.allocator.free(s);
        if (self.last_error) |e| self.allocator.free(e);
        self.allocator.free(self.dirname);
        self.allocator.destroy(self);
    }

    fn ensureCreateConfig(self: *Queue) !void {
        if (self.roll_length_ms == 0) try self.setRollScheme(roll.default_scheme);
        try self.validateConfig();
    }

    fn validateConfig(self: *const Queue) !void {
        if (self.roll_length_ms == 0 or self.roll_length_secs == 0) return error.InvalidRollConfig;
        if (self.index_count == 0 or self.index_spacing == 0) return error.InvalidRollConfig;
        if (self.blocksize == 0) return error.InvalidRollConfig;
        if (self.use_huge_pages and self.blocksize % (2 * 1024 * 1024) != 0) {
            return error.InvalidRollConfig;
        }
    }

    fn metadataEpoch(self: *const Queue) !u64 {
        if (self.roll_epoch < 0) return error.InvalidRollConfig;
        return @intCast(self.roll_epoch);
    }

    fn preallocateBestEffort(self: *Queue, fd: std.posix.fd_t, len: u64) !void {
        self.platform.preallocate(fd, 0, len) catch |err| switch (err) {
            error.PlatformCapabilityUnavailable => {},
            else => return err,
        };
    }

    fn initMaintenance(self: *Queue) !void {
        self.platform = Platform.detect();
        if (self.enable_prefetcher and self.prefetcher == null) {
            const prefetcher = try self.allocator.create(Prefetcher);
            prefetcher.* = Prefetcher.init(self.allocator);
            self.prefetcher = prefetcher;
        }
        if (self.enable_cleaner and self.cleaner == null) {
            const cleaner = try self.allocator.create(Cleaner);
            cleaner.* = Cleaner.init(self.allocator);
            cleaner.retention_floor_cycle = if (self.retention_cycles) |retention|
                self.lowest_cycle + retention
            else
                0;
            self.cleaner = cleaner;
        }
    }

    fn openQueueDir(self: *Queue, iterate: bool) !std.Io.Dir {
        return openDirPath(self.io, std.Io.Dir.cwd(), self.dirname, .{ .iterate = iterate });
    }

    fn closeMetadata(self: *Queue) void {
        if (self.metadata_mmap) |buf| {
            mmap_ops.unmapFile(buf);
            self.metadata_mmap = null;
        }
        self.metadata = null;
        if (self.metadata_fd) |fd| {
            closeFd(self.io, fd);
            self.metadata_fd = null;
        }
    }
};

fn defaultIo() std.Io {
    return std.Io.Threaded.global_single_threaded.io();
}

fn openDirPath(io: std.Io, cwd: std.Io.Dir, path: []const u8, options: std.Io.Dir.OpenOptions) !std.Io.Dir {
    if (std.Io.Dir.path.isAbsolute(path)) {
        return std.Io.Dir.openDirAbsolute(io, path, options);
    }
    return cwd.openDir(io, path, options);
}

fn closeFd(io: std.Io, fd: std.posix.fd_t) void {
    const file: std.Io.File = .{
        .handle = fd,
        .flags = .{ .nonblocking = false },
    };
    file.close(io);
}

fn findUniqueScheme(roll_length_secs: u32, index_count: u32, index_spacing: u32) ?RollScheme {
    var found: ?RollScheme = null;
    for (roll.roll_schemes) |scheme| {
        if (scheme.roll_length_secs == roll_length_secs and
            scheme.index_count == index_count and
            scheme.index_spacing == index_spacing)
        {
            if (found != null) return null;
            found = scheme;
        }
    }
    return found;
}

fn combineStepResults(a: StepResult, b: StepResult) StepResult {
    if (a == .more_work or b == .more_work) return .more_work;
    if (a == .made_progress or b == .made_progress) return .made_progress;
    return .idle;
}

fn touchWritablePagesPastHeader(buf: []align(config.page_alignment) u8, page_size: usize) void {
    var off = page_size;
    while (off < buf.len) : (off += page_size) {
        const ptr: *volatile u8 = @ptrCast(&buf[off]);
        ptr.* = 0;
    }
}

test "queue init set roll scheme and cycle arithmetic" {
    const allocator = std.testing.allocator;
    const scheme = @import("roll.zig").findSchemeByName("FAST_DAILY").?;

    const queue = try Queue.init(allocator, "queue-dir");
    defer queue.deinit();

    try queue.setRollScheme(scheme);
    try std.testing.expectEqualStrings("queue-dir", queue.dirname);
    try std.testing.expectEqualStrings("FAST_DAILY", queue.roll_name.?);
    try std.testing.expectEqualStrings("%Y%m%dF", queue.roll_strftime.?);
    try std.testing.expectEqual(@as(u32, 4096), queue.index_count);
    try std.testing.expectEqual(@as(u64, 1), queue.cycleFromMs(86_400_000));

    queue.doubleBlocksize();
    try std.testing.expectEqual(@as(u32, config.default_blocksize * 2), queue.blocksize);
}

test "queue owns registered tailers" {
    const allocator = std.testing.allocator;
    const queue = try Queue.init(allocator, "queue-dir");
    defer queue.deinit();

    const tailer = try allocator.create(Tailer);
    tailer.* = Tailer.init(queue, Index.compose(2, 3));
    try queue.registerTailer(tailer);
    try std.testing.expectEqual(@as(usize, 1), queue.tailers.items.len);
}

test "queue open creates fixed-size metadata and maps fields" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    const queue = try Queue.init(allocator, path);
    defer queue.deinit();
    queue.setCreate(true);

    try queue.open();

    const stat = try tmp.dir.statFile(std.testing.io, config.metadata_filename, .{});
    try std.testing.expectEqual(@as(u64, Queue.metadata_file_sz), stat.size);
    try std.testing.expectEqual(Version.v1, queue.getVersion());
    try std.testing.expectEqual(metadata_mod.metadata_magic, queue.metadata.?.magic);
    try std.testing.expectEqual(metadata_mod.format_version, queue.metadata.?.version);
    try std.testing.expectEqual(@as(u32, 86_400), queue.metadata.?.roll_length_secs);
    try std.testing.expectEqual(@as(u32, 4096), queue.metadata.?.index_count);
    try std.testing.expectEqual(@as(u64, 0), @atomicLoad(u64, &queue.metadata.?.appender_lock, .acquire));
}

test "queue detects and reopens existing v1 metadata" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    {
        const queue = try Queue.init(allocator, path);
        defer queue.deinit();
        queue.setCreate(true);
        try queue.open();
        try std.testing.expectEqual(Version.v1, try detectVersion(path));
    }

    const reopened = try Queue.init(allocator, path);
    defer reopened.deinit();
    try reopened.open();
    try std.testing.expectEqual(Version.v1, reopened.getVersion());
    try std.testing.expectEqual(@as(u64, 86_400_000), reopened.roll_length_ms);
    try std.testing.expectEqualStrings("FAST_DAILY", reopened.roll_name.?);
}

test "version detection rejects corrupt metadata magic" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    var magic_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &magic_buf, 0xdead_beef, .little);
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = config.metadata_filename,
        .data = &magic_buf,
    });

    try std.testing.expectError(error.InvalidMagic, detectVersion(path));

    const queue = try Queue.init(allocator, path);
    defer queue.deinit();
    queue.setCreate(true);
    try std.testing.expectError(error.InvalidMagic, queue.open());
}

test "queuefileInit writes header and zero index" {
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

    const qf_path = try queue.cyclePath(0);
    defer allocator.free(qf_path);
    const fd = try queue.queuefileInit(qf_path, 0);
    defer closeFd(queue.io, fd);

    const stat = try std.Io.Dir.cwd().statFile(queue.io, qf_path, .{});
    try std.testing.expectEqual(Queue.qf_disk_sz, stat.size);

    const map_sz = std.mem.alignForward(usize, @sizeOf(QueueFileHeader) + @as(usize, queue.index_count) * @sizeOf(u64), std.heap.pageSize());
    const buf = try mmap_ops.mapFile(fd, 0, map_sz, .read_write, .{});
    defer mmap_ops.unmapFile(buf);

    const header: *const QueueFileHeader = @ptrCast(@alignCast(buf.ptr));
    try std.testing.expectEqual(metadata_mod.queue_file_magic, header.magic);
    try std.testing.expectEqual(metadata_mod.format_version, header.version);
    try std.testing.expectEqual(@as(u32, 0), header.created_cycle);
    try std.testing.expectEqual(queue.index_count, header.index_count);

    const index_entries: [*]align(8) const u64 = @ptrCast(@alignCast(buf.ptr + @sizeOf(QueueFileHeader)));
    try std.testing.expectEqual(@as(u64, 0), index_entries[0]);
    try std.testing.expectEqual(@as(u64, 0), index_entries[queue.index_count - 1]);
}

test "refreshQueueFiles excludes metadata and sorts data files" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "19700103F.ringloom", .data = "" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "19700101F.ringloom", .data = "" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "19700102F.ringloom", .data = "" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = config.metadata_filename, .data = "" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "ignore.txt", .data = "" });

    const queue = try Queue.init(allocator, path);
    defer queue.deinit();
    try queue.refreshQueueFiles();

    try std.testing.expectEqual(@as(usize, 3), queue.queuefile_paths.items.len);
    try std.testing.expect(std.mem.endsWith(u8, queue.queuefile_paths.items[0], "19700101F.ringloom"));
    try std.testing.expect(std.mem.endsWith(u8, queue.queuefile_paths.items[1], "19700102F.ringloom"));
    try std.testing.expect(std.mem.endsWith(u8, queue.queuefile_paths.items[2], "19700103F.ringloom"));
}

test "maybePreroll creates next cycle without corrupting header" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    const queue = try Queue.init(allocator, path);
    defer queue.deinit();
    queue.setCreate(true);
    try queue.setRollSchemeName("TEST4_SECONDLY");
    queue.setPrerollMs(1000);
    try queue.open();

    try queue.maybePreroll(500);

    try std.testing.expectEqual(@as(?u64, 1), queue.preroll_cycle);
    try std.testing.expect(queue.preroll_fd != null);
    try std.testing.expect(queue.preroll_mmap != null);

    const header: *const QueueFileHeader = @ptrCast(@alignCast(queue.preroll_mmap.?.ptr));
    try std.testing.expectEqual(metadata_mod.queue_file_magic, header.magic);
    try std.testing.expectEqual(@as(u32, 1), header.created_cycle);
}

test "cyclePath rejects timestamp overflow" {
    const allocator = std.testing.allocator;
    const queue = try Queue.init(allocator, "queue-dir");
    defer queue.deinit();
    try queue.setRollSchemeName("FAST_DAILY");

    const too_large_cycle = @as(u64, @intCast(@divTrunc(std.math.maxInt(i64), @as(i64, 86_400)))) + 1;
    try std.testing.expectError(error.InvalidRollConfig, queue.cyclePath(too_large_cycle));
}

test "appender lease acquire and release use metadata CAS" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const path = try tmpQueuePath(allocator, &tmp);
    defer allocator.free(path);

    const queue = try Queue.init(allocator, path);
    defer queue.deinit();
    queue.setCreate(true);
    try queue.open();

    try queue.acquireAppenderLease(1234);
    try std.testing.expectError(error.AppenderAlreadyOpen, queue.acquireAppenderLease(5678));
    try std.testing.expectError(error.AppenderLeaseLost, queue.releaseAppenderLease(5678));
    try queue.releaseAppenderLease(1234);
    try queue.acquireAppenderLease(5678);
    try queue.releaseAppenderLease(5678);
}

fn tmpQueuePath(allocator: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
}
