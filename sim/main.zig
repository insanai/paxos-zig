//! Command-line runner for the deterministic Paxos simulator.
//!
//! Replay a failing seed exactly:
//!   zig build sim -- --seed=42 --steps=512 --nodes=3 --verbose
//! Sweep many seeds:
//!   zig build sim -- --seeds=10000 --steps=4096

const std = @import("std");
const simulation = @import("simulation.zig");

const Cli = struct {
    seed: ?u64 = null,
    seeds: u64 = 64,
    steps: u32 = 512,
    nodes: u16 = 3,
    verbose: bool = false,
};

pub fn main(init: std.process.Init) !void {
    const cli = try parseCli(init);
    if (cli.nodes != 3 and cli.nodes != 5) return error.UnsupportedNodeCount;

    const first = cli.seed orelse 1;
    const last = if (cli.seed) |seed| seed else cli.seeds;
    var seed = first;
    while (seed <= last) : (seed += 1) {
        runSeed(cli, seed) catch {
            const minimal = bisect(cli, seed);
            std.debug.print(
                "seed {d} fails; minimal failing steps: {d}\n" ++
                    "replay: zig build sim -- --seed={d} --steps={d} --nodes={d} --verbose\n",
                .{ seed, minimal, seed, minimal, cli.nodes },
            );
            std.process.exit(1);
        };
    }
    std.debug.print(
        "simulation ok: seeds {d}..{d}, {d} steps, {d} nodes\n",
        .{ first, last, cli.steps, cli.nodes },
    );
}

fn runSeed(cli: Cli, seed: u64) !void {
    return runSteps(cli, seed, cli.steps, cli.verbose);
}

fn runSteps(cli: Cli, seed: u64, steps: u32, verbose: bool) !void {
    const allocator = std.heap.page_allocator;
    if (cli.nodes == 5) {
        const sim = try allocator.create(simulation.Sim5Flexible);
        defer allocator.destroy(sim);
        return sim.run(.{
            .seed = seed,
            .steps = steps,
            .node_count = 5,
            .verbose = verbose,
        });
    }
    const sim = try allocator.create(simulation.Sim3);
    defer allocator.destroy(sim);
    return sim.run(.{
        .seed = seed,
        .steps = steps,
        .node_count = 3,
        .verbose = verbose,
    });
}

/// Halves the step budget while the same seed still fails, reporting the
/// smallest reproduction found. Deterministic seeds make prefixes exact.
fn bisect(cli: Cli, seed: u64) u32 {
    var minimal = cli.steps;
    var candidate = cli.steps / 2;
    while (candidate >= 8) : (candidate /= 2) {
        runSteps(cli, seed, candidate, false) catch {
            minimal = candidate;
            continue;
        };
        break;
    }
    return minimal;
}

fn parseCli(init: std.process.Init) !Cli {
    var cli = Cli{};
    var iterator = std.process.Args.Iterator.init(init.minimal.args);
    _ = iterator.skip();
    while (iterator.next()) |argument| {
        if (std.mem.eql(u8, argument, "--verbose")) {
            cli.verbose = true;
        } else if (std.mem.startsWith(u8, argument, "--seed=")) {
            cli.seed = try std.fmt.parseInt(u64, argument["--seed=".len..], 10);
        } else if (std.mem.startsWith(u8, argument, "--seeds=")) {
            cli.seeds = try std.fmt.parseInt(u64, argument["--seeds=".len..], 10);
        } else if (std.mem.startsWith(u8, argument, "--steps=")) {
            cli.steps = try std.fmt.parseInt(u32, argument["--steps=".len..], 10);
        } else if (std.mem.startsWith(u8, argument, "--nodes=")) {
            cli.nodes = try std.fmt.parseInt(u16, argument["--nodes=".len..], 10);
        } else {
            std.debug.print("unknown argument: {s}\n", .{argument});
            return error.InvalidArgument;
        }
    }
    return cli;
}
