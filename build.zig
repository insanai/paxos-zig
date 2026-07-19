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
    const test_step = addTests(b, paxos);
    addSimulation(b, target, optimize, paxos, test_step);
    addApiDocs(b, paxos);
    addBenchmarks(b, target);
    addFormatting(b);
    addBook(b);
    addZaxonlite(b);
}

fn addZaxonlite(b: *std.Build) void {
    // Zaxonlite is its own package (it pins the SQLite amalgamation and
    // depends on this package by path), so its steps run in its directory.
    const tests = b.addSystemCommand(&.{ "zig", "build", "test" });
    tests.setCwd(b.path("zaxonlite"));
    const test_step = b.step(
        "test-zaxonlite",
        "Run the zaxonlite (embedded replicated SQLite) test suite",
    );
    test_step.dependOn(&tests.step);

    const install = b.addSystemCommand(&.{ "zig", "build" });
    install.setCwd(b.path("zaxonlite"));
    const zaxon_step = b.step(
        "zaxon",
        "Build the zaxon CLI into zaxonlite/zig-out/bin",
    );
    zaxon_step.dependOn(&install.step);
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

fn addTests(b: *std.Build, paxos: *std.Build.Module) *std.Build.Step {
    const tests = b.addTest(.{ .root_module = paxos });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run the protocol test suite");
    test_step.dependOn(&run_tests.step);
    return test_step;
}

fn addSimulation(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    paxos: *std.Build.Module,
    test_step: *std.Build.Step,
) void {
    const sim_options = b.addOptions();
    sim_options.addOption(
        u64,
        "seeds",
        b.option(u64, "sim-seeds", "Simulation seeds per configuration") orelse 64,
    );
    sim_options.addOption(
        u32,
        "steps",
        b.option(u32, "sim-steps", "Simulation steps per seed") orelse 512,
    );

    const sim_module = b.createModule(.{
        .root_source_file = b.path("sim/simulation.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "paxos", .module = paxos },
            .{ .name = "sim_options", .module = sim_options.createModule() },
        },
    });
    const sim_tests = b.addTest(.{ .root_module = sim_module });
    const run_sim_tests = b.addRunArtifact(sim_tests);
    test_step.dependOn(&run_sim_tests.step);

    const sim_exe = b.addExecutable(.{
        .name = "paxos-sim",
        .root_module = b.createModule(.{
            .root_source_file = b.path("sim/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "paxos", .module = paxos },
                .{ .name = "sim_options", .module = sim_options.createModule() },
            },
        }),
    });
    const run_sim = b.addRunArtifact(sim_exe);
    if (b.args) |args| run_sim.addArgs(args);
    const sim_step = b.step("sim", "Run the deterministic protocol simulator");
    sim_step.dependOn(&run_sim.step);
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

    const durable = b.addExecutable(.{
        .name = "paxos-benchmark-durable",
        .root_module = b.createModule(.{
            .root_source_file = b.path("benchmarks/durable.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{.{ .name = "paxos", .module = benchmark_paxos }},
        }),
    });
    const run_durable = b.addRunArtifact(durable);
    const durable_step = b.step(
        "benchmark-durable",
        "Run the fsync write-ahead journal benchmark",
    );
    durable_step.dependOn(&run_durable.step);

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
    benchmark_step.dependOn(&run_durable.step);
}

fn addFormatting(b: *std.Build) void {
    const fmt = b.addFmt(.{
        .paths = &.{ "build.zig", "src", "examples", "benchmarks", "sim" },
        .check = true,
    });
    const style = b.addSystemCommand(&.{ "sh", "tools/check-style.sh" });
    const fmt_step = b.step("fmt", "Check zig fmt and project style");
    fmt_step.dependOn(&fmt.step);
    fmt_step.dependOn(&style.step);
}

fn addBook(b: *std.Build) void {
    // --root exposes benchmarks/results/ to the book's generated tables.
    const book = b.addSystemCommand(&.{ "typst", "compile", "--root", "." });
    book.addFileArg(b.path("docs/book.typ"));
    book.addArg("docs/part-time-parliament.pdf");
    const book_step = b.step("book", "Build the Part Time Parliament book");
    book_step.dependOn(&book.step);

    const zaxonlite_book = b.addSystemCommand(&.{
        "typst", "compile", "--root", ".",
    });
    zaxonlite_book.addFileArg(b.path("docs/zaxonlite/book.typ"));
    zaxonlite_book.addArg("docs/zaxonlite/zaxonlite.pdf");
    const zaxonlite_book_step = b.step(
        "book-zaxonlite",
        "Build the Zaxonlite book (docs/zaxonlite/zaxonlite.pdf)",
    );
    zaxonlite_book_step.dependOn(&zaxonlite_book.step);
}
