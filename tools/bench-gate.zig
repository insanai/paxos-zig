//! Statistical benchmark regression gate over the recorded results protocol
//! (ZDS 0011 performance gates).
//!
//! Compares a baseline and a candidate results file (the JSON emitted into
//! benchmarks/results/) and exits nonzero only when a matched paxos-zig
//! run whose records BOTH carry a raw `samples_ns_per_value` array of at
//! least 32 observations, recorded in matching environments, regresses.
//! Per matched pair the Hodges-Lehmann shift between the
//! two sample vectors is estimated with a percentile-bootstrap 95%
//! confidence interval under a fixed PRNG seed, so repeated invocations
//! reproduce the same interval bit for bit. (The verification plan asks
//! for a bootstrap interval; the plain percentile bootstrap satisfies it
//! and BCa correction is deliberately not required.) An enforced pair
//! fails when the interval's upper bound exceeds +3% of the baseline
//! median for in-memory workloads or +5% for durable workloads.
//!
//! Pairs are matched on workload and mode plus every fixture-defining
//! numeric field both records carry (values, nodes, payload_bytes,
//! values_per_iteration, measurement_iterations, max_slots, window_slots,
//! wraps). A pair differing in any present fixture field describes two
//! different experiments; it is skipped with a printed reason and never
//! compared.
//!
//! Runs recorded before the raw field existed expose only summary
//! statistics: the window or batch p50/p90/p99/max quantiles, or
//! min/median/max of `ns_total` normalized per value for durable runs. A
//! four-point quantile grid is not a sample with a defensible frequentist
//! interpretation, so such pairs are REPORT-ONLY: they are compared and
//! printed as "report-only: summary quantiles, not a sample" but never
//! affect the exit code. Quantile summaries are order statistics, so the
//! two-sample pairwise-difference form of Hodges-Lehmann would let one
//! heavy `max` dominate; matched summaries are therefore paired
//! positionally and the shift is the one-sample Hodges-Lehmann estimator
//! (median of Walsh averages) over the per-quantile differences,
//! bootstrapped by resampling those differences. Raw sample arrays use
//! the two-sample form (median of all pairwise differences) with
//! independent resampling of each side. A raw pair where either side has
//! fewer than 32 observations is likewise report-only ("insufficient
//! samples"): the bootstrap interval of a tiny sample is too fragile to
//! gate on.
//!
//! When both files record environment metadata (meta host, cpu, os, zig)
//! any difference downgrades every raw pair to report-only with a printed
//! reason — cross-environment shifts are not attributable to the code —
//! and is never a hard failure.
//!
//! Within one run the window/batch observations share a single process
//! lifetime and are autocorrelated, so the ordinary bootstrap used here
//! understates the true variance of the shift. The interval is reported
//! exactly as computed and never widened silently; the gold standard for
//! confirming a suspected shift is repeated same-fixture runs (a fresh
//! process per recording) compared across result files.
//!
//! The periodicity check reads the candidate's per-batch series (optional
//! `batch_ns_series` array, emitted by the u64-3n-moving workload in
//! benchmarks/benchmark.zig). Runs recorded before the field existed lack
//! it; the check then reports itself skipped and never fails the gate.
//! With the series, autocorrelation is reported at the window-wrap and
//! applied-anchor cadences against a predeclared report-only threshold.

const std = @import("std");
const Io = std.Io;

const usage_text =
    \\usage: bench-gate <baseline.json> <candidate.json>
    \\                  [--wrap-lag N] [--anchor-lag N]
    \\
    \\Both files must follow the recorded results protocol of
    \\benchmarks/results/: one object {"meta":{...},"runs":[...]}. Only
    \\runs with "impl":"paxos-zig" are gated; each needs "workload" and
    \\"mode" plus one sample source, tried in this order:
    \\
    \\  samples_ns_per_value      optional raw array of per-value ns samples
    \\  window_ns_per_value_p50/p90/p99/max   in-memory matrix runs
    \\  batch_ns_per_value_p50/p90/p99/max    moving-window runs
    \\  ns_total_min/median/max divided by "values"   durable runs
    \\
    \\Runs lacking every source are ignored. Runs pair on workload and
    \\mode plus every fixture-defining numeric field both records carry
    \\(values, nodes, payload_bytes, values_per_iteration,
    \\measurement_iterations, max_slots, window_slots, wraps); a pair
    \\differing in any present fixture field is skipped with a printed
    \\reason instead of being compared.
    \\
    \\The gate is the bootstrap 95% upper confidence bound of the
    \\Hodges-Lehmann shift: it must stay at or below +3% of the baseline
    \\median, or +5% for workloads whose name starts with "durable". Only
    \\pairs where BOTH runs carry at least 32 raw samples_ns_per_value
    \\observations, recorded in matching environments, are enforced (exit
    \\1 on regression). Every other matched pair is report-only and never
    \\affects the exit code: summary-quantile pairs, raw pairs below 32
    \\observations ("insufficient samples (N < 32)"), and every pair when
    \\the meta host/cpu/os/zig fields differ between the files. Durable
    \\rows record only ns_total min/median/max today (no per-operation
    \\timing array exists in benchmarks/durable.zig), so durable pairs
    \\stay report-only until their producer records raw samples. Quantile
    \\summaries are compared as paired per-quantile differences; raw
    \\arrays as all pairwise differences.
    \\
    \\Within-run observations share one process lifetime and are
    \\autocorrelated, so the bootstrap interval understates variance; it
    \\is never widened silently. Confirm a suspected shift with repeated
    \\same-fixture runs recorded into separate result files.
    \\
    \\The autocorrelation check reads the optional per-batch
    \\"batch_ns_series" array from candidate runs (emitted by the
    \\u64-3n-moving workload); --wrap-lag and --anchor-lag override the
    \\lags derived from "wraps" and the 10000-slot anchor cadence.
    \\Without the series the check is reported as skipped and never fails
    \\the gate.
    \\
;

const maximum_file_bytes = 16 * 1024 * 1024;

/// Fixed so repeated gate runs reproduce identical bootstrap intervals.
const bootstrap_seed: u64 = 0x2026_0011;
const bootstrap_iterations = 2000;

const in_memory_limit: f64 = 0.03;
const durable_limit: f64 = 0.05;

/// Raw pairs where either side records fewer observations than this are
/// too fragile to gate on and downgrade to report-only.
const min_raw_samples = 32;

/// Predeclared report-only autocorrelation threshold for periodicity.
const autocorrelation_flag = 0.2;
/// Applied-state anchor cadence in slots (ZDS 0011 shipped default).
const anchor_cadence_slots: f64 = 10_000;

const exit_ok: u8 = 0;
const exit_fail: u8 = 1;
const exit_usage: u8 = 2;

pub fn main(init: std.process.Init) !u8 {
    const gpa = init.gpa;
    const io = init.io;

    var stdout_buffer: [16 * 1024]u8 = undefined;
    var stdout_writer = Io.File.stdout().writerStreaming(io, &stdout_buffer);
    const out = &stdout_writer.interface;
    var stderr_buffer: [4096]u8 = undefined;
    var stderr_writer = Io.File.stderr().writerStreaming(io, &stderr_buffer);
    const err_out = &stderr_writer.interface;

    const code = run(gpa, io, init.minimal.args, out, err_out) catch |err| blk: {
        err_out.print("error: {t}\n", .{err}) catch {};
        break :blk exit_fail;
    };
    out.flush() catch {};
    err_out.flush() catch {};
    return code;
}

const Lags = struct {
    wrap: ?usize = null,
    anchor: ?usize = null,
};

fn run(
    gpa: std.mem.Allocator,
    io: Io,
    args: std.process.Args,
    out: *Io.Writer,
    err_out: *Io.Writer,
) !u8 {
    var iterator = std.process.Args.Iterator.init(args);
    defer iterator.deinit();
    _ = iterator.next();

    var paths: [2]?[]const u8 = .{ null, null };
    var lags = Lags{};
    while (iterator.next()) |arg| {
        if (std.mem.eql(u8, arg, "--wrap-lag")) {
            lags.wrap = try lagValue(&iterator, err_out) orelse return exit_usage;
        } else if (std.mem.eql(u8, arg, "--anchor-lag")) {
            lags.anchor = try lagValue(&iterator, err_out) orelse return exit_usage;
        } else if (paths[0] == null) {
            paths[0] = arg;
        } else if (paths[1] == null) {
            paths[1] = arg;
        } else {
            return usageError(err_out, "too many arguments");
        }
    }
    const baseline_path = paths[0] orelse {
        try out.writeAll(usage_text);
        return exit_usage;
    };
    const candidate_path = paths[1] orelse
        return usageError(err_out, "both a baseline and a candidate file are required");

    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const baseline = try loadRuns(alloc, io, baseline_path, err_out);
    const candidate = try loadRuns(alloc, io, candidate_path, err_out);
    try out.print(
        "bench-gate: baseline {s} ({d} comparable runs), candidate {s} ({d} comparable runs)\n",
        .{ baseline_path, baseline.runs.len, candidate_path, candidate.runs.len },
    );
    const env_mismatch = try warnEnvironment(baseline.meta, candidate.meta, out);
    return gateAll(alloc, baseline.runs, candidate.runs, lags, env_mismatch, out);
}

fn lagValue(iterator: *std.process.Args.Iterator, err_out: *Io.Writer) !?usize {
    const text = iterator.next() orelse {
        _ = try usageError(err_out, "--wrap-lag and --anchor-lag need a value");
        return null;
    };
    return std.fmt.parseInt(usize, text, 10) catch {
        _ = try usageError(err_out, "lag values must be nonnegative integers");
        return null;
    };
}

fn usageError(err_out: *Io.Writer, message: []const u8) !u8 {
    try err_out.print("error: {s}\n\n", .{message});
    try err_out.writeAll(usage_text);
    return exit_usage;
}

// ------------------------------------------------------------- loading

const SampleSource = enum { raw, window_quantiles, batch_quantiles, total_span };

/// Environment fields compared across the two files; any of them may be
/// absent from older recordings.
const meta_field_names = [_][]const u8{ "host", "cpu", "os", "zig" };

const Meta = struct {
    fields: [meta_field_names.len]?[]const u8 = @splat(null),
};

const Loaded = struct {
    runs: []Run,
    meta: Meta,
};

/// Differing recording environments make a shift unattributable to the
/// code; the mismatch is loudly reported and every raw pair downgrades
/// to report-only, but the gate never hard-fails on it.
fn warnEnvironment(base: Meta, cand: Meta, out: *Io.Writer) !bool {
    var mismatched = false;
    for (meta_field_names, base.fields, cand.fields) |name, base_field, cand_field| {
        const baseline_text = base_field orelse continue;
        const candidate_text = cand_field orelse continue;
        if (std.mem.eql(u8, baseline_text, candidate_text)) continue;
        mismatched = true;
        try out.print(
            "WARNING: environment mismatch on {s}: baseline \"{s}\" vs candidate \"{s}\"\n",
            .{ name, baseline_text, candidate_text },
        );
    }
    if (mismatched) {
        try out.writeAll("WARNING: shifts across differing environments may reflect " ++
            "the machine, not the code; raw pairs downgrade to report-only\n");
    }
    return mismatched;
}

/// Fixture-defining numeric fields: two runs describe the same experiment
/// only when every one of these carried by both records is equal. The
/// result families differ (matrix runs carry values_per_iteration and
/// max_slots, the moving run window_slots and wraps, durable runs
/// neither), so only fields present on both sides are compared.
const fixture_field_names = [_][]const u8{
    "values",                 "nodes",
    "payload_bytes",          "values_per_iteration",
    "measurement_iterations", "max_slots",
    "window_slots",           "wraps",
};

const Run = struct {
    workload: []const u8,
    mode: []const u8,
    durable: bool,
    source: SampleSource,
    /// Nanoseconds per value; quantile sources keep summary order.
    samples: []f64,
    series: ?[]f64,
    values: f64,
    wraps: f64,
    /// Fixture fields in `fixture_field_names` order; null when absent.
    fixture: [fixture_field_names.len]?f64,
};

fn loadRuns(
    alloc: std.mem.Allocator,
    io: Io,
    path: []const u8,
    err_out: *Io.Writer,
) !Loaded {
    const bytes = Io.Dir.cwd().readFileAlloc(
        io,
        path,
        alloc,
        .limited(maximum_file_bytes),
    ) catch |err| {
        try err_out.print("error: cannot read {s}\n", .{path});
        return err;
    };
    const parsed = std.json.parseFromSliceLeaky(std.json.Value, alloc, bytes, .{}) catch |err| {
        try err_out.print("error: {s} is not valid JSON\n", .{path});
        return err;
    };
    if (parsed != .object) {
        try err_out.print("error: {s} is not a results object\n", .{path});
        return error.InvalidResultsFile;
    }
    const runs_value = parsed.object.get("runs") orelse {
        try err_out.print("error: {s} has no \"runs\" array\n", .{path});
        return error.InvalidResultsFile;
    };
    if (runs_value != .array) {
        try err_out.print("error: {s} has a non-array \"runs\" field\n", .{path});
        return error.InvalidResultsFile;
    }

    var runs = std.ArrayList(Run).empty;
    for (runs_value.array.items) |item| {
        const record = try extractRun(alloc, item) orelse continue;
        try runs.append(alloc, record);
    }
    return .{ .runs = runs.items, .meta = extractMeta(parsed.object) };
}

fn extractMeta(object: std.json.ObjectMap) Meta {
    var meta = Meta{};
    const meta_value = object.get("meta") orelse return meta;
    if (meta_value != .object) return meta;
    for (meta_field_names, &meta.fields) |name, *slot| {
        slot.* = stringField(meta_value.object, name);
    }
    return meta;
}

fn extractRun(alloc: std.mem.Allocator, value: std.json.Value) !?Run {
    if (value != .object) return null;
    const object = value.object;
    const impl = stringField(object, "impl") orelse return null;
    if (!std.mem.eql(u8, impl, "paxos-zig")) return null;
    const workload = stringField(object, "workload") orelse return null;
    const mode = stringField(object, "mode") orelse return null;
    const vector = try sampleVector(alloc, object) orelse return null;
    var fixture: [fixture_field_names.len]?f64 = undefined;
    for (fixture_field_names, &fixture) |name, *slot| {
        slot.* = numberField(object, name);
    }
    return .{
        .workload = workload,
        .mode = mode,
        .durable = std.mem.startsWith(u8, workload, "durable"),
        .source = vector.source,
        .samples = vector.samples,
        .series = try floatArray(alloc, object, "batch_ns_series"),
        .values = numberField(object, "values") orelse 0,
        .wraps = numberField(object, "wraps") orelse 0,
        .fixture = fixture,
    };
}

const SampleVector = struct {
    source: SampleSource,
    samples: []f64,
};

fn sampleVector(alloc: std.mem.Allocator, object: std.json.ObjectMap) !?SampleVector {
    if (try floatArray(alloc, object, "samples_ns_per_value")) |raw| {
        if (raw.len == 0) return null;
        return .{ .source = .raw, .samples = raw };
    }
    if (try quantileSet(alloc, object, "window_ns_per_value_")) |set| {
        return .{ .source = .window_quantiles, .samples = set };
    }
    if (try quantileSet(alloc, object, "batch_ns_per_value_")) |set| {
        return .{ .source = .batch_quantiles, .samples = set };
    }
    return try totalSpanVector(alloc, object);
}

fn quantileSet(
    alloc: std.mem.Allocator,
    object: std.json.ObjectMap,
    comptime prefix: []const u8,
) !?[]f64 {
    const names = [_][]const u8{
        prefix ++ "p50", prefix ++ "p90", prefix ++ "p99", prefix ++ "max",
    };
    const out = try alloc.alloc(f64, names.len);
    for (names, out) |name, *slot| {
        slot.* = numberField(object, name) orelse return null;
    }
    return out;
}

/// Durable runs record no per-value quantiles, only the min/median/max of
/// the whole-run totals; normalized per value these three order statistics
/// form the smallest usable summary vector.
fn totalSpanVector(alloc: std.mem.Allocator, object: std.json.ObjectMap) !?SampleVector {
    const values = numberField(object, "values") orelse return null;
    if (values <= 0) return null;
    const names = [_][]const u8{ "ns_total_min", "ns_total_median", "ns_total_max" };
    const out = try alloc.alloc(f64, names.len);
    for (names, out) |name, *slot| {
        const total = numberField(object, name) orelse return null;
        slot.* = total / values;
    }
    return .{ .source = .total_span, .samples = out };
}

fn stringField(object: std.json.ObjectMap, name: []const u8) ?[]const u8 {
    const value = object.get(name) orelse return null;
    return switch (value) {
        .string => |text| text,
        else => null,
    };
}

fn numberField(object: std.json.ObjectMap, name: []const u8) ?f64 {
    const value = object.get(name) orelse return null;
    return switch (value) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        else => null,
    };
}

fn floatArray(
    alloc: std.mem.Allocator,
    object: std.json.ObjectMap,
    name: []const u8,
) !?[]f64 {
    const value = object.get(name) orelse return null;
    if (value != .array) return null;
    const items = value.array.items;
    const out = try alloc.alloc(f64, items.len);
    for (items, out) |item, *slot| {
        slot.* = switch (item) {
            .integer => |number| @floatFromInt(number),
            .float => |number| number,
            else => return null,
        };
    }
    return out;
}

// -------------------------------------------------------------- gating

fn gateAll(
    alloc: std.mem.Allocator,
    baseline: []const Run,
    candidate: []const Run,
    lags: Lags,
    env_mismatch: bool,
    out: *Io.Writer,
) !u8 {
    var gated: usize = 0;
    var report_only: usize = 0;
    var failures: usize = 0;
    var series_seen = false;
    for (candidate) |*cand| {
        const base = findRun(baseline, cand.workload, cand.mode) orelse {
            try out.print("skip {s}/{s}: not in baseline\n", .{ cand.workload, cand.mode });
            continue;
        };
        if (fixtureMismatch(base, cand)) |diff| {
            try out.print(
                "skip {s}/{s}: fixture mismatch on {s} (baseline {d} vs candidate {d})\n",
                .{ cand.workload, cand.mode, diff.name, diff.base, diff.cand },
            );
            continue;
        }
        if (base.source != cand.source) {
            try out.print(
                "skip {s}/{s}: incompatible sample sources ({t} vs {t})\n",
                .{ cand.workload, cand.mode, base.source, cand.source },
            );
            continue;
        }
        const limit = if (cand.durable) durable_limit else in_memory_limit;
        const gate = try evaluate(alloc, base.samples, cand.samples, cand.source != .raw, limit);
        const policy = pairPolicy(base, cand, env_mismatch);
        if (policy.reason == .raw_enforced) {
            gated += 1;
            if (!gate.pass) failures += 1;
        } else {
            report_only += 1;
        }
        try printGate(out, cand.*, gate, limit, policy);
        if (cand.series) |series| {
            series_seen = true;
            try reportSeries(out, cand.*, series, lags);
        }
    }
    for (baseline) |base| {
        if (findRun(candidate, base.workload, base.mode) == null) {
            try out.print("skip {s}/{s}: not in candidate\n", .{ base.workload, base.mode });
        }
    }
    return printSummary(out, gated, report_only, failures, series_seen);
}

fn printSummary(
    out: *Io.Writer,
    gated: usize,
    report_only: usize,
    failures: usize,
    series_seen: bool,
) !u8 {
    if (!series_seen) {
        try out.writeAll("autocorrelation: skipped, no candidate run carries a " ++
            "batch_ns_series array (percentile-only data)\n");
    }
    try out.print(
        "bench-gate: {d} raw-sample runs gated, {d} regressions, {d} report-only\n",
        .{ gated, failures, report_only },
    );
    if (gated == 0 and report_only == 0) {
        try out.writeAll("bench-gate: no comparable runs; failing\n");
        return exit_fail;
    }
    if (gated == 0) {
        try out.writeAll("bench-gate: nothing enforced; every matched pair is " ++
            "report-only (exit unaffected)\n");
    }
    return if (failures > 0) exit_fail else exit_ok;
}

const PolicyReason = enum { raw_enforced, summary, few_samples, env_mismatch };

const PairPolicy = struct {
    reason: PolicyReason,
    /// Smaller of the two raw sample counts, for the few-samples tag.
    sample_count: usize = 0,
};

/// A pair is enforced only when both sides are raw sample vectors of at
/// least `min_raw_samples` observations recorded in matching
/// environments; every other pair reports without touching the exit code.
fn pairPolicy(base: *const Run, cand: *const Run, env_mismatch: bool) PairPolicy {
    if (base.source != .raw or cand.source != .raw) return .{ .reason = .summary };
    const count = @min(base.samples.len, cand.samples.len);
    if (count < min_raw_samples) return .{ .reason = .few_samples, .sample_count = count };
    if (env_mismatch) return .{ .reason = .env_mismatch };
    return .{ .reason = .raw_enforced };
}

const FixtureDiff = struct {
    name: []const u8,
    base: f64,
    cand: f64,
};

/// First fixture-defining field carried by both records with differing
/// values; null when the two runs describe the same experiment.
fn fixtureMismatch(base: *const Run, cand: *const Run) ?FixtureDiff {
    for (fixture_field_names, base.fixture, cand.fixture) |name, base_slot, cand_slot| {
        const base_value = base_slot orelse continue;
        const cand_value = cand_slot orelse continue;
        if (base_value == cand_value) continue;
        return .{ .name = name, .base = base_value, .cand = cand_value };
    }
    return null;
}

fn findRun(runs: []const Run, workload: []const u8, mode: []const u8) ?*const Run {
    for (runs) |*record| {
        if (std.mem.eql(u8, record.workload, workload) and
            std.mem.eql(u8, record.mode, mode))
        {
            return record;
        }
    }
    return null;
}

fn printGate(
    out: *Io.Writer,
    run_record: Run,
    gate: Gate,
    limit: f64,
    policy: PairPolicy,
) !void {
    const enforced = policy.reason == .raw_enforced;
    const status = if (!enforced)
        "report-only"
    else if (gate.pass)
        "pass"
    else
        "FAIL";
    var detail_buffer: [64]u8 = undefined;
    const detail: []const u8 = switch (policy.reason) {
        .raw_enforced => "raw samples",
        .summary => "report-only: summary quantiles, not a sample",
        .few_samples => try std.fmt.bufPrint(
            &detail_buffer,
            "report-only: insufficient samples ({d} < {d})",
            .{ policy.sample_count, min_raw_samples },
        ),
        .env_mismatch => "report-only: environment mismatch",
    };
    const over_note = if (!enforced and !gate.pass)
        " (bound over limit; not enforced)"
    else
        "";
    try out.print(
        "{s} {s}/{s}: shift {d:.2} ns/value, 95% ci [{d:.2}, {d:.2}], " ++
            "baseline median {d:.2}, limit +{d:.0}% ({d:.2}) [{s}]{s}\n",
        .{
            status,
            run_record.workload,
            run_record.mode,
            gate.shift,
            gate.lower,
            gate.upper,
            gate.base_median,
            limit * 100,
            gate.limit_ns,
            detail,
            over_note,
        },
    );
}

// ---------------------------------------------------------- statistics

const Gate = struct {
    shift: f64,
    lower: f64,
    upper: f64,
    base_median: f64,
    limit_ns: f64,
    pass: bool,
    paired: bool,
};

/// Summary vectors are matched order statistics, so they gate as paired
/// per-quantile differences; raw vectors gate as the two-sample estimator
/// over all pairwise differences. The gate is the bootstrap upper bound,
/// never the point estimate alone.
fn evaluate(
    alloc: std.mem.Allocator,
    base: []const f64,
    cand: []const f64,
    paired: bool,
    relative_limit: f64,
) !Gate {
    if (base.len == 0 or cand.len == 0) return error.EmptySampleVector;
    if (paired and base.len != cand.len) return error.SummaryShapeMismatch;

    const sorted_base = try alloc.dupe(f64, base);
    std.mem.sort(f64, sorted_base, {}, std.sort.asc(f64));
    const base_median = medianOfSorted(sorted_base);
    if (base_median <= 0) return error.NonPositiveBaselineMedian;

    var shift: f64 = undefined;
    var interval: [2]f64 = undefined;
    if (paired) {
        const diffs = try alloc.alloc(f64, cand.len);
        for (diffs, cand, base) |*diff, c, b| diff.* = c - b;
        const scratch = try alloc.alloc(f64, walshCount(diffs.len));
        shift = walshMedianInto(scratch, diffs);
        interval = try bootstrapPaired(alloc, diffs);
    } else {
        const scratch = try alloc.alloc(f64, base.len * cand.len);
        shift = pairwiseMedianInto(scratch, base, cand);
        interval = try bootstrapUnpaired(alloc, base, cand);
    }
    const limit_ns = relative_limit * base_median;
    return .{
        .shift = shift,
        .lower = interval[0],
        .upper = interval[1],
        .base_median = base_median,
        .limit_ns = limit_ns,
        .pass = interval[1] <= limit_ns,
        .paired = paired,
    };
}

fn walshCount(count: usize) usize {
    return count * (count + 1) / 2;
}

/// One-sample Hodges-Lehmann estimator: median of the Walsh averages.
fn walshMedianInto(scratch: []f64, diffs: []const f64) f64 {
    var count: usize = 0;
    for (diffs, 0..) |left, left_index| {
        for (diffs[left_index..]) |right| {
            scratch[count] = (left + right) / 2;
            count += 1;
        }
    }
    std.mem.sort(f64, scratch[0..count], {}, std.sort.asc(f64));
    return medianOfSorted(scratch[0..count]);
}

/// Two-sample Hodges-Lehmann estimator: median of candidate-minus-baseline
/// over every pair.
fn pairwiseMedianInto(scratch: []f64, base: []const f64, cand: []const f64) f64 {
    var count: usize = 0;
    for (cand) |candidate_sample| {
        for (base) |baseline_sample| {
            scratch[count] = candidate_sample - baseline_sample;
            count += 1;
        }
    }
    std.mem.sort(f64, scratch[0..count], {}, std.sort.asc(f64));
    return medianOfSorted(scratch[0..count]);
}

fn medianOfSorted(sorted: []const f64) f64 {
    const half = sorted.len / 2;
    if (sorted.len % 2 == 1) return sorted[half];
    return (sorted[half - 1] + sorted[half]) / 2;
}

fn bootstrapPaired(alloc: std.mem.Allocator, diffs: []const f64) ![2]f64 {
    var prng = std.Random.DefaultPrng.init(bootstrap_seed);
    const random = prng.random();
    const resample = try alloc.alloc(f64, diffs.len);
    const scratch = try alloc.alloc(f64, walshCount(diffs.len));
    const shifts = try alloc.alloc(f64, bootstrap_iterations);
    for (shifts) |*shift| {
        for (resample) |*value| {
            value.* = diffs[random.uintLessThan(usize, diffs.len)];
        }
        shift.* = walshMedianInto(scratch, resample);
    }
    return confidenceBounds(shifts);
}

fn bootstrapUnpaired(
    alloc: std.mem.Allocator,
    base: []const f64,
    cand: []const f64,
) ![2]f64 {
    var prng = std.Random.DefaultPrng.init(bootstrap_seed);
    const random = prng.random();
    const base_resample = try alloc.alloc(f64, base.len);
    const cand_resample = try alloc.alloc(f64, cand.len);
    const scratch = try alloc.alloc(f64, base.len * cand.len);
    const shifts = try alloc.alloc(f64, bootstrap_iterations);
    for (shifts) |*shift| {
        for (base_resample) |*value| {
            value.* = base[random.uintLessThan(usize, base.len)];
        }
        for (cand_resample) |*value| {
            value.* = cand[random.uintLessThan(usize, cand.len)];
        }
        shift.* = pairwiseMedianInto(scratch, base_resample, cand_resample);
    }
    return confidenceBounds(shifts);
}

fn confidenceBounds(shifts: []f64) [2]f64 {
    std.mem.sort(f64, shifts, {}, std.sort.asc(f64));
    const lower = shifts[shifts.len * 25 / 1000];
    const upper = shifts[shifts.len * 975 / 1000 - 1];
    return .{ lower, upper };
}

// ------------------------------------------------------ autocorrelation

fn reportSeries(out: *Io.Writer, run_record: Run, series: []const f64, lags: Lags) !void {
    const wrap = lags.wrap orelse defaultWrapLag(run_record, series.len);
    const anchor = lags.anchor orelse defaultAnchorLag(run_record, series.len);
    try reportLag(out, run_record, series, "window-wrap", wrap);
    try reportLag(out, run_record, series, "anchor-cadence", anchor);
}

/// Batches per window wrap, derived from the recorded wrap count.
fn defaultWrapLag(run_record: Run, series_len: usize) usize {
    if (run_record.wraps <= 0) return 0;
    const wraps: usize = @intFromFloat(run_record.wraps);
    if (wraps == 0 or series_len % wraps != 0) return 0;
    return series_len / wraps;
}

/// Batches per applied-state anchor at the shipped 10000-slot cadence.
fn defaultAnchorLag(run_record: Run, series_len: usize) usize {
    if (run_record.values <= 0 or series_len == 0) return 0;
    const values_per_batch = run_record.values / @as(f64, @floatFromInt(series_len));
    if (values_per_batch <= 0) return 0;
    return @intFromFloat(@round(anchor_cadence_slots / values_per_batch));
}

fn reportLag(
    out: *Io.Writer,
    run_record: Run,
    series: []const f64,
    name: []const u8,
    lag: usize,
) !void {
    if (lag == 0 or lag >= series.len / 2) {
        try out.print(
            "  autocorrelation {s}/{s} {s}: lag unavailable for {d} batches\n",
            .{ run_record.workload, run_record.mode, name, series.len },
        );
        return;
    }
    const r = autocorrelation(series, lag);
    const verdict = if (@abs(r) > autocorrelation_flag)
        " PERIODICITY SUSPECTED (report only)"
    else
        "";
    try out.print(
        "  autocorrelation {s}/{s} {s} lag {d}: r={d:.3}{s}\n",
        .{ run_record.workload, run_record.mode, name, lag, r, verdict },
    );
}

fn autocorrelation(series: []const f64, lag: usize) f64 {
    var mean: f64 = 0;
    for (series) |value| mean += value;
    mean /= @floatFromInt(series.len);
    var denominator: f64 = 0;
    for (series) |value| denominator += (value - mean) * (value - mean);
    if (denominator == 0) return 0;
    var numerator: f64 = 0;
    for (series[0 .. series.len - lag], series[lag..]) |early, late| {
        numerator += (early - mean) * (late - mean);
    }
    return numerator / denominator;
}

// --------------------------------------------------------------- tests

fn testRun(source: SampleSource, samples: []f64) Run {
    return .{
        .workload = "u64-3n-moving",
        .mode = "batch64",
        .durable = false,
        .source = source,
        .samples = samples,
        .series = null,
        .values = 0,
        .wraps = 0,
        .fixture = @splat(null),
    };
}

/// Runs the whole gate policy over in-memory Run records, discarding the
/// report text and returning only the exit code.
fn testGateAll(base_runs: []const Run, cand_runs: []const Run, env_mismatch: bool) !u8 {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var buffer: [8192]u8 = undefined;
    var writer = Io.Writer.fixed(&buffer);
    return gateAll(arena.allocator(), base_runs, cand_runs, .{}, env_mismatch, &writer);
}

/// Fills a baseline vector and a candidate vector regressed by the given
/// factor over the same deterministic spread.
fn fillRegression(base: []f64, cand: []f64, factor: f64) void {
    for (base, cand, 0..) |*baseline_sample, *candidate_sample, index| {
        baseline_sample.* = 100 + @as(f64, @floatFromInt((index * 7) % 13));
        candidate_sample.* = baseline_sample.* * factor;
    }
}

test "identical raw distributions pass the gate" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var base: [64]f64 = undefined;
    for (&base, 0..) |*sample, index| {
        sample.* = 100 + @as(f64, @floatFromInt((index * 7) % 13));
    }
    const gate = try evaluate(arena.allocator(), &base, &base, false, in_memory_limit);
    try std.testing.expectApproxEqAbs(0, gate.shift, 1e-9);
    try std.testing.expect(gate.upper <= gate.limit_ns);
    try std.testing.expect(gate.pass);
}

test "a ten percent raw shift fails the gate" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var base: [64]f64 = undefined;
    var cand: [64]f64 = undefined;
    for (&base, &cand, 0..) |*baseline_sample, *candidate_sample, index| {
        baseline_sample.* = 100 + @as(f64, @floatFromInt((index * 7) % 13));
        candidate_sample.* = baseline_sample.* * 1.10;
    }
    const gate = try evaluate(arena.allocator(), &base, &cand, false, in_memory_limit);
    try std.testing.expect(gate.shift > 0.09 * gate.base_median);
    try std.testing.expect(!gate.pass);
}

test "paired quantile summaries are compared identically and on shift" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    // A realistic summary with a heavy max, as recorded for u64-3n sync.
    const base = [_]f64{ 130, 140, 160, 4040 };
    const same = try evaluate(alloc, &base, &base, true, in_memory_limit);
    try std.testing.expectApproxEqAbs(0, same.shift, 1e-9);
    try std.testing.expect(same.pass);

    var shifted: [4]f64 = undefined;
    for (&shifted, base) |*candidate_sample, baseline_sample| {
        candidate_sample.* = baseline_sample * 1.10;
    }
    const gate = try evaluate(alloc, &base, &shifted, true, in_memory_limit);
    try std.testing.expect(gate.shift > gate.limit_ns);
    try std.testing.expect(!gate.pass);
}

test "identical raw samples pass the whole gate" {
    var base: [64]f64 = undefined;
    for (&base, 0..) |*sample, index| {
        sample.* = 100 + @as(f64, @floatFromInt((index * 7) % 13));
    }
    var same = base;
    var base_runs = [_]Run{testRun(.raw, &base)};
    var cand_runs = [_]Run{testRun(.raw, &same)};
    try std.testing.expectEqual(exit_ok, try testGateAll(&base_runs, &cand_runs, false));
}

test "a raw-sample regression fails the whole gate" {
    var base: [64]f64 = undefined;
    var cand: [64]f64 = undefined;
    fillRegression(&base, &cand, 1.10);
    var base_runs = [_]Run{testRun(.raw, &base)};
    var cand_runs = [_]Run{testRun(.raw, &cand)};
    try std.testing.expectEqual(exit_fail, try testGateAll(&base_runs, &cand_runs, false));
}

test "a quantile-only regression is report-only and never fails the gate" {
    var base = [_]f64{ 130, 140, 160, 4040 };
    var shifted: [4]f64 = undefined;
    for (&shifted, base) |*candidate_sample, baseline_sample| {
        candidate_sample.* = baseline_sample * 1.10;
    }
    const base_run = testRun(.batch_quantiles, &base);
    const cand_run = testRun(.batch_quantiles, &shifted);
    try std.testing.expectEqual(
        PolicyReason.summary,
        pairPolicy(&base_run, &cand_run, false).reason,
    );
    var base_runs = [_]Run{base_run};
    var cand_runs = [_]Run{cand_run};
    try std.testing.expectEqual(exit_ok, try testGateAll(&base_runs, &cand_runs, false));
}

test "a raw pair below 32 samples is report-only and never fails the gate" {
    var base: [16]f64 = undefined;
    var cand: [16]f64 = undefined;
    fillRegression(&base, &cand, 1.10);
    const base_run = testRun(.raw, &base);
    const cand_run = testRun(.raw, &cand);
    const policy = pairPolicy(&base_run, &cand_run, false);
    try std.testing.expectEqual(PolicyReason.few_samples, policy.reason);
    try std.testing.expectEqual(@as(usize, 16), policy.sample_count);
    var base_runs = [_]Run{base_run};
    var cand_runs = [_]Run{cand_run};
    try std.testing.expectEqual(exit_ok, try testGateAll(&base_runs, &cand_runs, false));
}

test "an environment mismatch downgrades a raw regression to report-only" {
    var base: [64]f64 = undefined;
    var cand: [64]f64 = undefined;
    fillRegression(&base, &cand, 1.10);
    var base_runs = [_]Run{testRun(.raw, &base)};
    var cand_runs = [_]Run{testRun(.raw, &cand)};
    try std.testing.expectEqual(exit_fail, try testGateAll(&base_runs, &cand_runs, false));
    try std.testing.expectEqual(exit_ok, try testGateAll(&base_runs, &cand_runs, true));
}

test "a fixture mismatch skips the pair instead of comparing it" {
    var base: [64]f64 = undefined;
    var cand: [64]f64 = undefined;
    fillRegression(&base, &cand, 1.10);
    // The regressed pair differs on the first fixture field ("values"),
    // so the gate must skip it rather than fail on it; a clean matching
    // pair keeps the files comparable.
    var mismatched_base = testRun(.raw, &base);
    mismatched_base.fixture[0] = 4096;
    var mismatched_cand = testRun(.raw, &cand);
    mismatched_cand.fixture[0] = 8192;
    try std.testing.expect(fixtureMismatch(&mismatched_base, &mismatched_cand) != null);

    var same = base;
    var clean_base = testRun(.raw, &base);
    clean_base.workload = "u64-3n";
    var clean_cand = testRun(.raw, &same);
    clean_cand.workload = "u64-3n";
    // A field carried by only one side never blocks the pair.
    try std.testing.expect(fixtureMismatch(&mismatched_base, &clean_cand) == null);

    var base_runs = [_]Run{ mismatched_base, clean_base };
    var cand_runs = [_]Run{ mismatched_cand, clean_cand };
    try std.testing.expectEqual(exit_ok, try testGateAll(&base_runs, &cand_runs, false));
}

test "autocorrelation flags an injected periodic series" {
    var series: [256]f64 = undefined;
    for (&series, 0..) |*value, index| {
        value.* = if (index % 16 == 0) 150 else 100;
    }
    try std.testing.expect(autocorrelation(&series, 16) > autocorrelation_flag);
    try std.testing.expect(@abs(autocorrelation(&series, 7)) < autocorrelation_flag);
}
