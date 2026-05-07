const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("ringloom_queue", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const mod_tests = b.addTest(.{
        .root_module = mod,
    });
    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);

    const bench_mod = b.createModule(.{
        .root_source_file = b.path("src/ringloom/bench_tests.zig"),
        .target = target,
        .optimize = .ReleaseFast,
        .imports = &.{
            .{ .name = "ringloom_queue", .module = mod },
        },
    });
    const bench_exe = b.addExecutable(.{
        .name = "ringloom_queue_bench",
        .root_module = bench_mod,
    });
    const run_bench_tests = b.addRunArtifact(bench_exe);
    if (b.args) |args| run_bench_tests.addArgs(args);
    const bench_step = b.step("bench", "Run opt-in benchmarks");
    bench_step.dependOn(&run_bench_tests.step);
}
