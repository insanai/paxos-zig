const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const paxos = b.addModule("paxos", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    addExample(b, target, optimize, paxos);
    addTests(b, paxos);
    addApiDocs(b, paxos);
    addBenchmarks(b, target);
    addFormatting(b);
    addBook(b);
}

fn addApiDocs(b: *std.Build, paxos: *std.Build.Module) void {
    const docs_object = b.addObject(.{
        .name = "paxos-api-docs",
        .root_module = paxos,
    });
    const install_docs = b.addInstallDirectory(.{
        .source_dir = docs_object.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "docs/api",
    });
    const docs_step = b.step("docs", "Generate the Zig API documentation");
    docs_step.dependOn(&install_docs.step);
}

fn addExample(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    paxos: *std.Build.Module,
) void {
    const example = b.addExecutable(.{
        .name = "paxos-example",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/counter.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "paxos", .module = paxos }},
        }),
    });
    b.installArtifact(example);

    const run_example = b.addRunArtifact(example);
    run_example.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_example.addArgs(args);
    const run_step = b.step("run", "Run the in-memory counter example");
    run_step.dependOn(&run_example.step);
}

fn addTests(b: *std.Build, paxos: *std.Build.Module) void {
    const tests = b.addTest(.{ .root_module = paxos });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run the protocol test suite");
    test_step.dependOn(&run_tests.step);
}

fn addBenchmarks(b: *std.Build, target: std.Build.ResolvedTarget) void {
    const benchmark_paxos = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const benchmark = b.addExecutable(.{
        .name = "paxos-benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("benchmarks/benchmark.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{.{ .name = "paxos", .module = benchmark_paxos }},
        }),
    });
    const run_benchmark = b.addRunArtifact(benchmark);
    const zig_step = b.step("benchmark-zig", "Run the Zig in-memory benchmark");
    zig_step.dependOn(&run_benchmark.step);

    const rust_benchmark = b.addSystemCommand(&.{
        "cargo", "run", "--release", "--locked", "--manifest-path",
    });
    rust_benchmark.addFileArg(b.path("benchmarks/omnipaxos-rust/Cargo.toml"));
    rust_benchmark.step.dependOn(&run_benchmark.step);

    const libpaxos = b.addSystemCommand(&.{ "sh", "benchmarks/libpaxos-c/run.sh" });
    const libpaxos_step = b.step(
        "benchmark-libpaxos",
        "Run the pinned C LibPaxos3 in-memory benchmark",
    );
    libpaxos_step.dependOn(&libpaxos.step);

    const aggregate_libpaxos = b.addSystemCommand(&.{
        "sh", "benchmarks/libpaxos-c/run.sh",
    });
    aggregate_libpaxos.step.dependOn(&rust_benchmark.step);
    const benchmark_step = b.step(
        "benchmark",
        "Compare Zig with Rust OmniPaxos and C LibPaxos3",
    );
    benchmark_step.dependOn(&aggregate_libpaxos.step);
}

fn addFormatting(b: *std.Build) void {
    const fmt = b.addFmt(.{
        .paths = &.{ "build.zig", "src", "examples", "benchmarks" },
        .check = true,
    });
    const style = b.addSystemCommand(&.{ "sh", "tools/check-style.sh" });
    const fmt_step = b.step("fmt", "Check zig fmt and project style");
    fmt_step.dependOn(&fmt.step);
    fmt_step.dependOn(&style.step);
}

fn addBook(b: *std.Build) void {
    const book = b.addSystemCommand(&.{ "typst", "compile" });
    book.addFileArg(b.path("docs/book.typ"));
    book.addArg("docs/part-time-parliament.pdf");
    const book_step = b.step("book", "Build the Part Time Parliament book");
    book_step.dependOn(&book.step);
}
