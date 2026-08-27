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

In-memory samples aggregate repeated, independently initialized
stable-leader iterations before selecting the median of seven totals. Setup and
post-run validation are outside the timed interval. The separate per-window
percentiles are averages within a drained proposal window; they are not request
latency percentiles.

## Recording protocol

Run on AC power with the machine otherwise idle, close heavyweight
applications, and prefer a machine without aggressive thermal throttling.
Record at least the default seven samples per mode (three for the durable
path) and commit the produced file unedited. The `meta` block captures the
date, host, CPU, OS, git revision, whether the working tree was modified, and
toolchain versions. A dirty result is useful during development but is not an
archival comparison point: after committing benchmark changes, rerun once more
so the recorded revision contains the exact measured source. Results without
that context are not comparable and should not be committed.

## Regression gating

After recording a candidate file, run
`zig build bench-gate -- <baseline.json> <candidate.json>` against the
last committed archival file before publishing numbers or merging a
performance-relevant change. The gate estimates the Hodges-Lehmann shift
per workload/mode with a deterministic percentile-bootstrap 95%
confidence interval. Only pairs where both records carry the raw
`samples_ns_per_value` array gate (exit nonzero on regression); files
recorded before that field existed expose only summary quantiles and are
compared report-only, never affecting the exit code. The tool also warns,
without failing, when the two files were recorded on different hosts or
toolchains — such shifts are not attributable to the code.
