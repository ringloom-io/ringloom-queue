const std = @import("std");

const config = @import("config.zig");

pub const RollScheme = struct {
    name: []const u8,
    format_str: []const u8,
    roll_length_secs: u32,
    index_count: u32,
    index_spacing: u32,

    pub fn rollLengthMs(self: RollScheme) u64 {
        return @as(u64, self.roll_length_secs) * 1000;
    }
};

pub const roll_schemes = [_]RollScheme{
    .{ .name = "FIVE_MINUTELY", .format_str = "yyyyMMdd-HHmm'V'", .roll_length_secs = 5 * 60, .index_count = 2048, .index_spacing = 256 },
    .{ .name = "TEN_MINUTELY", .format_str = "yyyyMMdd-HHmm'X'", .roll_length_secs = 10 * 60, .index_count = 2048, .index_spacing = 256 },
    .{ .name = "TWENTY_MINUTELY", .format_str = "yyyyMMdd-HHmm'XX'", .roll_length_secs = 20 * 60, .index_count = 2048, .index_spacing = 256 },
    .{ .name = "HALF_HOURLY", .format_str = "yyyyMMdd-HHmm'H'", .roll_length_secs = 30 * 60, .index_count = 2048, .index_spacing = 256 },
    .{ .name = "FAST_HOURLY", .format_str = "yyyyMMdd-HH'F'", .roll_length_secs = 3600, .index_count = 4096, .index_spacing = 256 },
    .{ .name = "TWO_HOURLY", .format_str = "yyyyMMdd-HH'II'", .roll_length_secs = 2 * 3600, .index_count = 4096, .index_spacing = 256 },
    .{ .name = "FOUR_HOURLY", .format_str = "yyyyMMdd-HH'IV'", .roll_length_secs = 4 * 3600, .index_count = 4096, .index_spacing = 256 },
    .{ .name = "SIX_HOURLY", .format_str = "yyyyMMdd-HH'VI'", .roll_length_secs = 6 * 3600, .index_count = 4096, .index_spacing = 256 },
    .{ .name = "FAST_DAILY", .format_str = "yyyyMMdd'F'", .roll_length_secs = 86400, .index_count = 4096, .index_spacing = 256 },
    .{ .name = "MINUTELY", .format_str = "yyyyMMdd-HHmm", .roll_length_secs = 60, .index_count = 2048, .index_spacing = 16 },
    .{ .name = "HOURLY", .format_str = "yyyyMMdd-HH", .roll_length_secs = 3600, .index_count = 4096, .index_spacing = 16 },
    .{ .name = "DAILY", .format_str = "yyyyMMdd", .roll_length_secs = 86400, .index_count = 8192, .index_spacing = 64 },
    .{ .name = "LARGE_HOURLY", .format_str = "yyyyMMdd-HH'L'", .roll_length_secs = 3600, .index_count = 8192, .index_spacing = 64 },
    .{ .name = "LARGE_DAILY", .format_str = "yyyyMMdd'L'", .roll_length_secs = 86400, .index_count = 32768, .index_spacing = 128 },
    .{ .name = "XLARGE_DAILY", .format_str = "yyyyMMdd'X'", .roll_length_secs = 86400, .index_count = 32768, .index_spacing = 256 },
    .{ .name = "HUGE_DAILY", .format_str = "yyyyMMdd'H'", .roll_length_secs = 86400, .index_count = 32768, .index_spacing = 1024 },
    .{ .name = "SMALL_DAILY", .format_str = "yyyyMMdd'S'", .roll_length_secs = 86400, .index_count = 8192, .index_spacing = 8 },
    .{ .name = "LARGE_HOURLY_SPARSE", .format_str = "yyyyMMdd-HH'LS'", .roll_length_secs = 3600, .index_count = 4096, .index_spacing = 1024 },
    .{ .name = "LARGE_HOURLY_XSPARSE", .format_str = "yyyyMMdd-HH'LX'", .roll_length_secs = 3600, .index_count = 2048, .index_spacing = 1048576 },
    .{ .name = "HUGE_DAILY_XSPARSE", .format_str = "yyyyMMdd'HX'", .roll_length_secs = 86400, .index_count = 16384, .index_spacing = 1048576 },
    .{ .name = "TEST_SECONDLY", .format_str = "yyyyMMdd-HHmmss'T'", .roll_length_secs = 1, .index_count = 32768, .index_spacing = 4 },
    .{ .name = "TEST4_SECONDLY", .format_str = "yyyyMMdd-HHmmss'T4'", .roll_length_secs = 1, .index_count = 32, .index_spacing = 4 },
    .{ .name = "TEST_HOURLY", .format_str = "yyyyMMdd-HH'T'", .roll_length_secs = 3600, .index_count = 16, .index_spacing = 4 },
    .{ .name = "TEST_DAILY", .format_str = "yyyyMMdd'T1'", .roll_length_secs = 86400, .index_count = 8, .index_spacing = 1 },
    .{ .name = "TEST2_DAILY", .format_str = "yyyyMMdd'T2'", .roll_length_secs = 86400, .index_count = 16, .index_spacing = 2 },
    .{ .name = "TEST4_DAILY", .format_str = "yyyyMMdd'T4'", .roll_length_secs = 86400, .index_count = 32, .index_spacing = 4 },
    .{ .name = "TEST8_DAILY", .format_str = "yyyyMMdd'T8'", .roll_length_secs = 86400, .index_count = 128, .index_spacing = 8 },
};

pub const default_scheme = roll_schemes[8];

pub fn findSchemeByName(name: []const u8) ?RollScheme {
    for (roll_schemes) |scheme| {
        if (std.mem.eql(u8, scheme.name, name)) return scheme;
    }
    return null;
}

pub fn findSchemeByFormat(format_str: []const u8) ?RollScheme {
    for (roll_schemes) |scheme| {
        if (std.mem.eql(u8, scheme.format_str, format_str)) return scheme;
    }
    return null;
}

pub const ConversionError = error{
    UnrecognizedToken,
    UnterminatedQuote,
    BufferOverflow,
};

pub fn javaFormatToStrftime(
    java_fmt: []const u8,
    strftime_buf: []u8,
    clean_buf: []u8,
) ConversionError!struct { strftime: []const u8, clean: []const u8 } {
    var si: usize = 0;
    var ci: usize = 0;
    var fi: usize = 0;
    var in_quote = false;

    while (fi < java_fmt.len) {
        if (java_fmt[fi] == '\'') {
            in_quote = !in_quote;
            fi += 1;
        } else if (in_quote) {
            try appendByte(strftime_buf, &si, java_fmt[fi]);
            try appendByte(clean_buf, &ci, java_fmt[fi]);
            fi += 1;
        } else if (java_fmt[fi] == '-') {
            try appendByte(strftime_buf, &si, '-');
            try appendByte(clean_buf, &ci, '-');
            fi += 1;
        } else if (startsWith(java_fmt[fi..], "yyyy")) {
            try appendSlice(strftime_buf, &si, "%Y");
            try appendSlice(clean_buf, &ci, "yyyy");
            fi += 4;
        } else if (startsWith(java_fmt[fi..], "MM")) {
            try appendSlice(strftime_buf, &si, "%m");
            try appendSlice(clean_buf, &ci, "MM");
            fi += 2;
        } else if (startsWith(java_fmt[fi..], "dd")) {
            try appendSlice(strftime_buf, &si, "%d");
            try appendSlice(clean_buf, &ci, "dd");
            fi += 2;
        } else if (startsWith(java_fmt[fi..], "HH")) {
            try appendSlice(strftime_buf, &si, "%H");
            try appendSlice(clean_buf, &ci, "HH");
            fi += 2;
        } else if (startsWith(java_fmt[fi..], "mm")) {
            try appendSlice(strftime_buf, &si, "%M");
            try appendSlice(clean_buf, &ci, "mm");
            fi += 2;
        } else if (startsWith(java_fmt[fi..], "ss")) {
            try appendSlice(strftime_buf, &si, "%S");
            try appendSlice(clean_buf, &ci, "ss");
            fi += 2;
        } else {
            return error.UnrecognizedToken;
        }
    }

    if (in_quote) return error.UnterminatedQuote;
    return .{ .strftime = strftime_buf[0..si], .clean = clean_buf[0..ci] };
}

pub fn cycleFromMs(ms: i64, epoch_ms: i64, roll_length_ms: u64) u64 {
    std.debug.assert(roll_length_ms != 0);
    if (ms <= epoch_ms) return 0;
    return @intCast(@divFloor(ms - epoch_ms, @as(i64, @intCast(roll_length_ms))));
}

pub fn getCycleFilename(
    allocator: std.mem.Allocator,
    dirname: []const u8,
    cycle: u64,
    roll_length_ms: u64,
    strftime_pattern: []const u8,
) ![]u8 {
    if (roll_length_ms == 0) return error.RollLengthMissing;
    const seconds_per_cycle = roll_length_ms / 1000;
    if (seconds_per_cycle == 0) return error.RollLengthMissing;

    const seconds: i64 = @intCast(cycle * seconds_per_cycle);
    var date_buf: [64]u8 = undefined;
    const date = try formatTimestamp(&date_buf, seconds, strftime_pattern);
    return std.fmt.allocPrint(allocator, "{s}/{s}{s}", .{ dirname, date, config.queue_file_extension });
}

pub fn getCycleFn(
    allocator: std.mem.Allocator,
    dirname: []const u8,
    cycle: u64,
    roll_length_ms: u64,
    strftime_pattern: []const u8,
) ![]u8 {
    return getCycleFilename(allocator, dirname, cycle, roll_length_ms, strftime_pattern);
}

pub fn formatTimestamp(buf: []u8, seconds: i64, strftime_pattern: []const u8) ConversionError![]const u8 {
    const dt = dateTimeFromUnixSeconds(seconds);
    var out: usize = 0;
    var i: usize = 0;
    while (i < strftime_pattern.len) {
        if (strftime_pattern[i] != '%') {
            try appendByte(buf, &out, strftime_pattern[i]);
            i += 1;
            continue;
        }
        if (i + 1 >= strftime_pattern.len) return error.UnrecognizedToken;
        switch (strftime_pattern[i + 1]) {
            'Y' => try appendPadded(buf, &out, @intCast(dt.year), 4),
            'm' => try appendPadded(buf, &out, dt.month, 2),
            'd' => try appendPadded(buf, &out, dt.day, 2),
            'H' => try appendPadded(buf, &out, dt.hour, 2),
            'M' => try appendPadded(buf, &out, dt.minute, 2),
            'S' => try appendPadded(buf, &out, dt.second, 2),
            else => return error.UnrecognizedToken,
        }
        i += 2;
    }
    return buf[0..out];
}

fn startsWith(haystack: []const u8, needle: []const u8) bool {
    return haystack.len >= needle.len and std.mem.eql(u8, haystack[0..needle.len], needle);
}

fn appendByte(buf: []u8, index: *usize, byte: u8) ConversionError!void {
    if (index.* >= buf.len) return error.BufferOverflow;
    buf[index.*] = byte;
    index.* += 1;
}

fn appendSlice(buf: []u8, index: *usize, bytes: []const u8) ConversionError!void {
    if (index.* + bytes.len > buf.len) return error.BufferOverflow;
    @memcpy(buf[index.*..][0..bytes.len], bytes);
    index.* += bytes.len;
}

fn appendPadded(buf: []u8, index: *usize, value: u32, width: u8) ConversionError!void {
    if (index.* + width > buf.len) return error.BufferOverflow;
    var divisor: u32 = 1;
    var n: u8 = 1;
    while (n < width) : (n += 1) divisor *= 10;

    var current = divisor;
    while (current > 0) : (current /= 10) {
        buf[index.*] = @intCast('0' + (value / current) % 10);
        index.* += 1;
    }
}

const DateTime = struct {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
};

fn dateTimeFromUnixSeconds(seconds: i64) DateTime {
    const days = @divFloor(seconds, 86_400);
    const day_seconds: u32 = @intCast(@mod(seconds, 86_400));
    const ymd = civilFromDays(days);
    return .{
        .year = ymd.year,
        .month = ymd.month,
        .day = ymd.day,
        .hour = day_seconds / 3600,
        .minute = (day_seconds % 3600) / 60,
        .second = day_seconds % 60,
    };
}

fn civilFromDays(days_since_epoch: i64) struct { year: i32, month: u32, day: u32 } {
    const z = days_since_epoch + 719_468;
    const era = @divFloor(z, 146_097);
    const doe = z - era * 146_097;
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36_524) - @divFloor(doe, 146_096), 365);
    var year = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const day = doy - @divFloor(153 * mp + 2, 5) + 1;
    const month = mp + if (mp < 10) @as(i64, 3) else -9;
    if (month <= 2) year += 1;
    return .{ .year = @intCast(year), .month = @intCast(month), .day = @intCast(day) };
}

test "roll scheme table and lookups" {
    try std.testing.expectEqual(@as(usize, 27), roll_schemes.len);
    const fast_daily = findSchemeByName("FAST_DAILY").?;
    try std.testing.expectEqualStrings("yyyyMMdd'F'", fast_daily.format_str);
    try std.testing.expectEqual(@as(u64, 86_400_000), fast_daily.rollLengthMs());
    try std.testing.expectEqualStrings("DAILY", findSchemeByFormat("yyyyMMdd").?.name);
}

test "java format conversion" {
    var sf_buf: [64]u8 = undefined;
    var cl_buf: [64]u8 = undefined;

    {
        const r = try javaFormatToStrftime("yyyyMMdd", &sf_buf, &cl_buf);
        try std.testing.expectEqualStrings("%Y%m%d", r.strftime);
        try std.testing.expectEqualStrings("yyyyMMdd", r.clean);
    }
    {
        const r = try javaFormatToStrftime("yyyyMMdd-HH'F'", &sf_buf, &cl_buf);
        try std.testing.expectEqualStrings("%Y%m%d-%HF", r.strftime);
        try std.testing.expectEqualStrings("yyyyMMdd-HHF", r.clean);
    }
    {
        const r = try javaFormatToStrftime("yyyyMMdd-HHmmss'T'", &sf_buf, &cl_buf);
        try std.testing.expectEqualStrings("%Y%m%d-%H%M%ST", r.strftime);
    }
}

test "cycle arithmetic and filenames are UTC" {
    try std.testing.expectEqual(@as(u64, 18_950), cycleFromMs(1_637_280_000_000, 0, 86_400_000));

    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings("19700101", try formatTimestamp(&buf, 0, "%Y%m%d"));
    try std.testing.expectEqualStrings("19700101-000001T", try formatTimestamp(&buf, 1, "%Y%m%d-%H%M%ST"));

    const allocator = std.testing.allocator;
    const filename = try getCycleFilename(allocator, "queue", 1, 86_400_000, "%Y%m%d");
    defer allocator.free(filename);
    try std.testing.expectEqualStrings("queue/19700102.ringloom", filename);
}
