const std = @import("std");
const ringloom = @import("ringloom_queue");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;
    const cwd = std.Io.Dir.cwd();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/ringloom-bench-{d}", .{
        std.Io.Clock.awake.now(io).toNanoseconds(),
    });
    defer allocator.free(path);
    cwd.deleteTree(io, path) catch {};
    defer cwd.deleteTree(io, path) catch {};

    var queue = try ringloom.Queue([]const u8).open(.{
        .dir = path,
        .create = true,
        .roll_scheme = ringloom.roll.findSchemeByName("TEST4_SECONDLY").?,
        .allocator = allocator,
        .spawn_helper_threads = false,
        .enable_prefetcher = true,
    }, ringloom.RawCodec);
    defer queue.deinit();

    const iterations = 4096;
    const payload = "benchmark-payload-64-byte-smoke-message-for-ringloom-queue";
    const append_start = std.Io.Clock.awake.now(io);
    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        _ = try queue.appendWithTimestamp(payload, 0);
    }
    const append_elapsed = append_start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds();

    var tailer = try queue.tailer(0);
    defer tailer.deinit();
    const poll_start = std.Io.Clock.awake.now(io);
    var read_count: usize = 0;
    while (try tailer.poll()) |_| read_count += 1;
    const poll_elapsed = poll_start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds();

    if (read_count != iterations) return error.BenchmarkReadMismatch;

    var stdout_buffer: [1024]u8 = undefined;
    var stdout_file_writer: std.Io.File.Writer = .init(.stdout(), io, &stdout_buffer);
    const stdout = &stdout_file_writer.interface;
    try stdout.print(
        "\nringloom benchmark smoke: appended {d} msgs in {d}ns; polled {d} msgs in {d}ns\n",
        .{ iterations, append_elapsed, read_count, poll_elapsed },
    );
    try stdout.flush();
}
