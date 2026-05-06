const std = @import("std");
const ringloom_queue = @import("ringloom_queue");

pub fn main(init: std.process.Init) !void {
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_file_writer: std.Io.File.Writer = .init(.stdout(), init.io, &stdout_buffer);
    const stdout = &stdout_file_writer.interface;

    try stdout.print("ringloom-queue format v{d}\n", .{ringloom_queue.metadata.format_version});
    try stdout.flush();
}

test "library exposes core protocol constants" {
    try std.testing.expectEqual(@as(u32, 0x80000000), ringloom_queue.Header.WORKING);
}
