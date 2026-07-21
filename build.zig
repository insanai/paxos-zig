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
    addZds(b);
    addZaxonlite(b);
}

fn addZds(b: *std.Build) void {
    const make_dir = b.addSystemCommand(&.{ "mkdir", "-p", "docs/build" });

    const filter = b.option(
        []const u8,
        "zds",
        "Build only the ZDS record matching this number (e.g. 2 or 0002) or slug",
    );

    const zds_step = b.step("zds", "Build the Zaxon Discussion (ZDS) record PDFs");
    const stems = zdsRecordStems(b, filter);
    if (stems.len == 0) {
        const message = if (filter) |value|
            b.fmt("no ZDS record in docs/zds/records matches -Dzds={s}", .{value})
        else
            "no numbered ZDS records found in docs/zds/records";
        zds_step.dependOn(&b.addFail(message).step);
    }
    for (stems) |stem| {
        const compile = b.addSystemCommand(&.{
            "typst",
            "compile",
            "--root",
            "docs",
            b.fmt("docs/zds/records/{s}.typ", .{stem}),
            b.fmt("docs/build/zds-{s}.pdf", .{stem}),
        });
        compile.step.dependOn(&make_dir.step);
        zds_step.dependOn(&compile.step);
    }

    const index_step = b.step("zds-index", "Build the ZDS index PDF");
    const compile_index = b.addSystemCommand(&.{
        "typst",              "compile",
        "--root",             "docs",
        "docs/zds/index.typ", "docs/build/zds-index.pdf",
    });
    compile_index.step.dependOn(&make_dir.step);
    index_step.dependOn(&compile_index.step);

    const site_step = b.step("zds-site", "Build the experimental ZDS HTML bundle");
    const make_site_dir = b.addSystemCommand(&.{ "mkdir", "-p", "docs/build/zds-site" });
    const compile_site = b.addSystemCommand(&.{
        "typst",               "compile",
        "--features",          "html,bundle",
        "--root",              "docs",
        "--format",            "bundle",
        "docs/zds/bundle.typ", "docs/build/zds-site",
    });
    compile_site.step.dependOn(&make_site_dir.step);
    site_step.dependOn(&compile_site.step);

    addZdsTool(b);
}

/// Numbered record stems (`NNNN-slug`) discovered in docs/zds/records, or
/// every record matching `filter` when `-Dzds=` is given. Placeholder drafts
/// (`XXXXX-slug`) are built only when the filter selects them.
fn zdsRecordStems(b: *std.Build, filter: ?[]const u8) [][]const u8 {
    const io = b.graph.io;
    var stems = std.ArrayList([]const u8).empty;
    var dir = b.build_root.handle.openDir(io, "docs/zds/records", .{ .iterate = true }) catch
        return stems.items;
    defer dir.close(io);
    var it = dir.iterate();
    while (it.next(io) catch null) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".typ")) continue;
        const stem = entry.name[0 .. entry.name.len - ".typ".len];
        if (stem.len < "0000-a".len) continue;
        const numbered = for (stem[0..4]) |byte| {
            if (!std.ascii.isDigit(byte)) break false;
        } else stem[4] == '-';
        const selected = if (filter) |value|
            zdsRecordMatches(stem, numbered, value)
        else
            numbered;
        if (selected) stems.append(b.allocator, b.dupe(stem)) catch @panic("OOM");
    }
    std.mem.sort([]const u8, stems.items, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.order(u8, lhs, rhs) == .lt;
        }
    }.lessThan);
    return stems.items;
}

fn zdsRecordMatches(stem: []const u8, numbered: bool, filter: []const u8) bool {
    if (std.mem.eql(u8, stem, filter)) return true;
    const slug = if (numbered)
        stem["0000-".len..]
    else if (std.mem.startsWith(u8, stem, "XXXXX-"))
        stem["XXXXX-".len..]
    else
        stem;
    if (std.mem.eql(u8, slug, filter)) return true;
    if (numbered) {
        // Accept unpadded numbers such as -Dzds=2 for 0002.
        const wanted = std.fmt.parseInt(u16, filter, 10) catch return false;
        const actual = std.fmt.parseInt(u16, stem[0..4], 10) catch return false;
        return wanted == actual;
    }
    return false;
}

fn addZdsTool(b: *std.Build) void {
    const tool = b.addExecutable(.{
        .name = "zds-tool",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/zds.zig"),
            .target = b.graph.host,
            .optimize = .Debug,
        }),
    });

    const list_run = b.addRunArtifact(tool);
    list_run.has_side_effects = true;
    list_run.addArgs(&.{ "--root", b.pathFromRoot("."), "list" });
    const list_step = b.step("zds-list", "List ZDS registry entries and placeholder drafts");
    list_step.dependOn(&list_run.step);

    const new_run = b.addRunArtifact(tool);
    new_run.has_side_effects = true;
    new_run.addArgs(&.{ "--root", b.pathFromRoot("."), "new" });
    if (b.args) |args| new_run.addArgs(args);
    const new_step = b.step(
        "zds-new",
        "Create a placeholder ZDS draft: zig build zds-new -- <slug>",
    );
    new_step.dependOn(&new_run.step);

    const promote_run = b.addRunArtifact(tool);
    promote_run.has_side_effects = true;
    promote_run.addArgs(&.{ "--root", b.pathFromRoot("."), "promote" });
    if (b.args) |args| promote_run.addArgs(args);
    const promote_step = b.step(
        "zds-promote",
        "Assign the next number to a draft and register it: zig build zds-promote -- <slug>",
    );
    promote_step.dependOn(&promote_run.step);
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

    // The autodoc output is a WASM application; browsers refuse to load it
    // from file://, so serve the installed directory over local HTTP.
    const serve_docs = b.addSystemCommand(&.{
        "python3", "-m", "http.server", "8000", "-d",
    });
    serve_docs.addArg(b.getInstallPath(.prefix, "docs/api"));
    serve_docs.step.dependOn(&install_docs.step);
    const serve_step = b.step(
        "docs-serve",
        "Serve the API documentation at http://localhost:8000",
    );
    serve_step.dependOn(&serve_docs.step);
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

    // Deterministic multi-node reconfiguration harness for the replicated
    // log: stop-sign sealing, checkpoint, and epoch handover oracles.
    const reconfiguration_module = b.createModule(.{
        .root_source_file = b.path("sim/reconfiguration.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "paxos", .module = paxos }},
    });
    const reconfiguration_tests = b.addTest(.{ .root_module = reconfiguration_module });
    const run_reconfiguration_tests = b.addRunArtifact(reconfiguration_tests);
    test_step.dependOn(&run_reconfiguration_tests.step);

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
        .paths = &.{ "build.zig", "src", "examples", "benchmarks", "sim", "tools" },
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
