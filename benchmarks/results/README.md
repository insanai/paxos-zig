# Benchmark results

Machine-readable results produced by `sh benchmarks/run-all.sh`. Each file
is named `<date>-<host>.json`; `latest.json` is a copy of the most recent
run and is the only file the book and README may cite numbers from.

## What these numbers are

Workload fixtures: in-memory, single-process runs of pinned upstream code
(OmniPaxos 0.2.2, LibPaxos3 `d255f8b`) and this library, across commit
modes (sync, pipelined, batched), payload sizes, cluster sizes, and log
slack, plus a durable-path run that fsyncs a write-ahead journal before
any message moves. They are regression signals for this repository.

They are **not** a language comparison and **not** service throughput or
latency:

- The three implementations run different protocol paths. LibPaxos3
  advances a phase-one preexecution window (~12 measured messages per
  value). OmniPaxos coalesces log entries into batched messages when the
  harness pipelines, so its message count per value drops far below six —
  a design difference, not an inefficiency in either direction.
- The sync mode measures per-append CPU overhead with everything hot in
  cache; the pipelined modes measure how each design amortizes. Compare
  like with like, and read the durable-path numbers to see what the safety
  contract costs on a real filesystem.

## Recording protocol

Run on AC power with the machine otherwise idle, close heavyweight
applications, and prefer a machine without aggressive thermal throttling.
Record at least the default seven samples per mode (three for the durable
path) and commit the produced file unedited. The `meta` block captures the
date, host, CPU, OS, git revision, and toolchain versions; results without
that context are not comparable and should not be committed.
