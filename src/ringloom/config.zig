const std = @import("std");

pub const page_alignment = std.heap.page_size_min;

pub const metadata_filename = "metadata.ringloom";
pub const queue_file_extension = ".ringloom";

pub const patch_cycles: u32 = 3;
pub const default_qf_disk_size: u64 = 83_754_496;
pub const default_blocksize: u32 = 2 * 1024 * 1024;
pub const max_data_size: usize = 0x3fffffff;
pub const default_preroll_ms: u64 = 1000;
pub const max_cas_backoff_ns: u64 = 1_000_000;

comptime {
    std.debug.assert(std.math.isPowerOfTwo(default_blocksize));
}

test "default block size is huge-page aligned" {
    try std.testing.expect(std.math.isPowerOfTwo(default_blocksize));
    try std.testing.expectEqual(@as(usize, 0), default_blocksize % page_alignment);
}
