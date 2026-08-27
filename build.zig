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
    addMisuseTests(b, target, test_step);
    addCompileErrorTests(b, target, test_step);
    addIntegrationTests(b, test_step);
    addSimulation(b, target, optimize, paxos, test_step);
    addApiDocs(b, paxos);
    addBenchmarks(b, target);
    addBenchGate(b, test_step);
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

/// Effect-order misuse fixtures run as child processes in every optimize
/// mode. A violation must abort with its stable diagnostic on stderr even
/// in ReleaseFast and ReleaseSmall; the correct sequence must exit cleanly.
fn addMisuseTests(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    test_step: *std.Build.Step,
) void {
    const step = b.step(
        "test-misuse",
        "Verify effect-order misuse stops the process in every optimize mode",
    );
    const cases = [_]struct {
        src: []const u8,
        name: []const u8,
        diagnostic: ?[]const u8,
    }{
        .{
            .src = "tests/misuse/messages_before_confirm.zig",
            .name = "messages-before-confirm",
            .diagnostic = "paxos: messagesSlice before confirmWritesDurable",
        },
        .{
            .src = "tests/misuse/reset_unconfirmed.zig",
            .name = "reset-unconfirmed",
            .diagnostic = "paxos: reset discarded unconfirmed writes",
        },
        .{
            .src = "tests/misuse/correct_order.zig",
            .name = "correct-order",
            .diagnostic = null,
        },
    };
    const modes = [_]std.builtin.OptimizeMode{
        .Debug, .ReleaseSafe, .ReleaseFast, .ReleaseSmall,
    };
    for (modes) |mode| {
        const paxos = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = mode,
        });
        for (cases) |case| {
            const exe = b.addExecutable(.{
                .name = b.fmt("paxos-misuse-{s}-{t}", .{ case.name, mode }),
                .root_module = b.createModule(.{
                    .root_source_file = b.path(case.src),
                    .target = target,
                    .optimize = mode,
                    .imports = &.{.{ .name = "paxos", .module = paxos }},
                }),
            });
            const run = b.addRunArtifact(exe);
            if (case.diagnostic) |diagnostic| {
                run.addCheck(.{ .expect_term = .{ .signal = .ABRT } });
                run.expectStdErrMatch(diagnostic);
            } else {
                run.expectExitCode(0);
            }
            step.dependOn(&run.step);
        }
    }
    test_step.dependOn(step);
}

/// Fixtures whose compilation must fail with the paired stable message.
const compile_fail_cases = [_]struct { src: []const u8, expected: []const u8 }{
    .{
        .src = "tests/compile_fail/protocol_zero_max_members.zig",
        .expected = "paxos Protocol option max_members must be greater than zero",
    },
    .{
        .src = "tests/compile_fail/protocol_zero_window_slots.zig",
        .expected = "paxos Protocol option window_slots must be greater than zero",
    },
    .{
        .src = "tests/compile_fail/protocol_window_not_power_of_two.zig",
        .expected = "paxos Protocol option window_slots must be a power of two",
    },
    .{
        .src = "tests/compile_fail/protocol_max_members_too_large.zig",
        .expected = "paxos Protocol option max_members must be at most 65535",
    },
    .{
        .src = "tests/compile_fail/protocol_zero_election_timeout.zig",
        .expected = "paxos Protocol option election_timeout_ticks " ++
            "must be greater than zero",
    },
    .{
        .src = "tests/compile_fail/protocol_zero_heartbeat_interval.zig",
        .expected = "paxos Protocol option heartbeat_interval_ticks " ++
            "must be greater than zero",
    },
    .{
        .src = "tests/compile_fail/protocol_zero_resend_interval.zig",
        .expected = "paxos Protocol option resend_interval_ticks " ++
            "must be greater than zero",
    },
    .{
        .src = "tests/compile_fail/protocol_pointer_value.zig",
        .expected = "Value type '*u64' must not contain pointers, " ++
            "slices, or references.",
    },
    .{
        .src = "tests/compile_fail/replicated_log_zero_max_batch.zig",
        .expected = "paxos ReplicatedLog option max_batch must be greater than zero",
    },
    .{
        .src = "tests/compile_fail/replicated_log_batch_exceeds_window.zig",
        .expected = "paxos ReplicatedLog option max_batch must not exceed window_slots",
    },
    .{
        .src = "tests/compile_fail/replicated_log_metadata_too_large.zig",
        .expected = "paxos ReplicatedLog option max_metadata_bytes " ++
            "must be at most 65535",
    },
    .{
        .src = "tests/compile_fail/learner_zero_max_entries.zig",
        .expected = "paxos Learner option max_entries must be greater than zero",
    },
};

/// Invalid compile-time options must fail compilation with their stable
/// identifying message. Each fixture succeeds only when the compiler
/// rejects it with the expected error.
fn addCompileErrorTests(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    test_step: *std.Build.Step,
) void {
    const step = b.step(
        "test-compile-errors",
        "Verify invalid comptime options fail with stable messages",
    );
    for (compile_fail_cases, 0..) |case, index| {
        const name = b.fmt("paxos-compile-fail-{d}", .{index});
        addCompileFailCase(b, step, target, name, case.src, "paxos", "src/root.zig", case.expected);
    }

    // The BitSet factory is internal, so its fixture imports the file directly.
    addCompileFailCase(
        b,
        step,
        target,
        "paxos-compile-fail-bit-set",
        "tests/compile_fail/bit_set_zero_bits.zig",
        "bitset",
        "src/bit_set.zig",
        "paxos BitSet bit_count must be greater than zero",
    );

    // The derived-capacity overflow is only reachable where usize is 32 bits,
    // so this fixture compiles for a 32-bit freestanding target.
    const overflow_target = b.resolveTargetQuery(.{
        .cpu_arch = .wasm32,
        .os_tag = .freestanding,
    });
    addCompileFailCase(
        b,
        step,
        overflow_target,
        "paxos-compile-fail-capacity-overflow",
        "tests/compile_fail/protocol_capacity_overflow.zig",
        "paxos",
        "src/root.zig",
        "paxos Protocol options max_members and window_slots " ++
            "overflow the derived message capacity",
    );

    test_step.dependOn(step);
}

fn addCompileFailCase(
    b: *std.Build,
    step: *std.Build.Step,
    target: std.Build.ResolvedTarget,
    name: []const u8,
    src: []const u8,
    import_name: []const u8,
    import_root: []const u8,
    expected: []const u8,
) void {
    const object = b.addObject(.{
        .name = name,
        .root_module = b.createModule(.{
            .root_source_file = b.path(src),
            .target = target,
            .optimize = .Debug,
            .imports = &.{.{
                .name = import_name,
                .module = b.createModule(.{
                    .root_source_file = b.path(import_root),
                    .target = target,
                    .optimize = .Debug,
                }),
            }},
        }),
    });
    object.expect_errors = .{ .contains = expected };
    step.dependOn(&object.step);
}

/// The path-dependency consumer proves the published package surface still
/// resolves and compiles for a downstream `zig fetch` user.
fn addIntegrationTests(b: *std.Build, test_step: *std.Build.Step) void {
    const consumer = b.addSystemCommand(&.{ "zig", "build", "test" });
    consumer.setCwd(b.path("integration/consumer"));
    const step = b.step(
        "test-integration",
        "Run the path-dependency consumer smoke test",
    );
    step.dependOn(&consumer.step);
    test_step.dependOn(step);
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

/// The statistical regression gate compares two recorded results files
/// (ZDS 0011): it fails when a candidate run's Hodges-Lehmann shift
/// confidence bound regresses past the workload's threshold.
fn addBenchGate(b: *std.Build, test_step: *std.Build.Step) void {
    const gate_module = b.createModule(.{
        .root_source_file = b.path("tools/bench-gate.zig"),
        .target = b.graph.host,
        .optimize = .ReleaseSafe,
    });
    const gate = b.addExecutable(.{
        .name = "bench-gate",
        .root_module = gate_module,
    });
    const run_gate = b.addRunArtifact(gate);
    run_gate.has_side_effects = true;
    if (b.args) |args| run_gate.addArgs(args);
    const gate_step = b.step(
        "bench-gate",
        "Gate a candidate results file against a baseline: " ++
            "zig build bench-gate -- <baseline.json> <candidate.json>",
    );
    gate_step.dependOn(&run_gate.step);

    const gate_tests = b.addTest(.{ .root_module = gate_module });
    test_step.dependOn(&b.addRunArtifact(gate_tests).step);
}

fn addFormatting(b: *std.Build) void {
    const fmt = b.addFmt(.{
        .paths = &.{ "build.zig", "src", "examples", "benchmarks", "sim", "tests", "tools" },
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
