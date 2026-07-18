const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const dependency = b.dependency("paxos", .{
        .target = target,
        .optimize = optimize,
    });

    const consumer = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{
            .name = "paxos",
            .module = dependency.module("paxos"),
        }},
    });
    const tests = b.addTest(.{ .root_module = consumer });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Verify dependency consumption");
    test_step.dependOn(&run_tests.step);
}
