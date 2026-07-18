#!/bin/sh
# Runs the full benchmark matrix (Zig, durable-path, OmniPaxos Rust,
# LibPaxos3 C), captures the environment, and writes one machine-readable
# results file under benchmarks/results/. Commit the file so the book and
# README can only cite measured, attributable numbers.
set -eu

cd "$(dirname "$0")/.."

stamp=$(date +%Y%m%d)
host=$(hostname -s)
out_dir="benchmarks/results"
out_file="$out_dir/$stamp-$host.json"
mkdir -p "$out_dir"

capture() {
    # Benchmarks print human blocks and JSON lines on stderr/stdout; keep
    # only the JSON lines.
    grep -h '^{' "$1" || true
}

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

echo "running: zig build benchmark-zig" >&2
zig build benchmark-zig >"$tmp_dir/zig.out" 2>&1 || {
    cat "$tmp_dir/zig.out" >&2
    exit 1
}
echo "running: zig build benchmark-durable" >&2
zig build benchmark-durable >"$tmp_dir/durable.out" 2>&1 || {
    cat "$tmp_dir/durable.out" >&2
    exit 1
}
echo "running: cargo run --release --locked (OmniPaxos)" >&2
cargo run --release --locked \
    --manifest-path benchmarks/omnipaxos-rust/Cargo.toml \
    >"$tmp_dir/rust.out" 2>&1 || {
    cat "$tmp_dir/rust.out" >&2
    exit 1
}
echo "running: benchmarks/libpaxos-c/run.sh" >&2
sh benchmarks/libpaxos-c/run.sh >"$tmp_dir/c.out" 2>&1 || {
    cat "$tmp_dir/c.out" >&2
    exit 1
}

cpu="unknown"
if command -v sysctl >/dev/null 2>&1; then
    cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)
fi
if [ "$cpu" = "unknown" ] && [ -r /proc/cpuinfo ]; then
    cpu=$(awk -F': ' '/model name/ { print $2; exit }' /proc/cpuinfo)
fi

{
    printf '{"meta":{'
    printf '"date":"%s",' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '"host":"%s",' "$host"
    printf '"cpu":"%s",' "$cpu"
    printf '"os":"%s",' "$(uname -sr)"
    printf '"git":"%s",' "$(git rev-parse --short HEAD 2>/dev/null || echo none)"
    printf '"zig":"%s",' "$(zig version)"
    printf '"rustc":"%s"' "$(rustc --version | cut -d' ' -f2)"
    printf '},"runs":[\n'
    capture "$tmp_dir/zig.out" | sed 's/$/,/'
    capture "$tmp_dir/durable.out" | sed 's/$/,/'
    capture "$tmp_dir/rust.out" | sed 's/$/,/'
    capture "$tmp_dir/c.out" | sed '$!s/$/,/'
    printf ']}\n'
} >"$out_file"

# Validate and pretty-check the JSON before publishing it.
python3 -m json.tool "$out_file" >/dev/null
cp "$out_file" "$out_dir/latest.json"
echo "results written to $out_file (and latest.json)" >&2
