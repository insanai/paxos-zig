#!/bin/sh
# The full local verification battery, gated on real exit codes plus
# success markers read back from each suite's own output. Pipelines are
# never placed between a command and its gate: an `... | tail && echo OK`
# chain reports the pipe tail's status, not the command's, and that
# shape once let a broken build print green. Run from the repository
# root; pass a directory as $1 to keep the logs.
set -eu

log_dir=${1:-"$(mktemp -d "${TMPDIR:-/tmp}/zaxon-verify.XXXXXX")"}
mkdir -p "$log_dir"
echo "verify: logs in $log_dir"

run() {
    name=$1
    marker=$2
    dir=$3
    shift 3
    (cd "$dir" && "$@") > "$log_dir/$name.log" 2>&1 || {
        echo "verify: $name FAILED (exit $?)" >&2
        tail -20 "$log_dir/$name.log" >&2
        exit 1
    }
    if [ -n "$marker" ] && ! grep -q "$marker" "$log_dir/$name.log"; then
        echo "verify: $name exited 0 without its marker: $marker" >&2
        tail -20 "$log_dir/$name.log" >&2
        exit 1
    fi
    echo "verify: $name ok"
}

run fmt-style "" . zig build fmt
run root-test "" . zig build test
run zds "" . zig build zds
run zx-fmt "" zaxonlite zig fmt --check build.zig src
run zx-check "" zaxonlite zig build check
run zx-test "crash matrix: all 15 cases passed" zaxonlite zig build test
run zx-crash "all 15 cases passed" zaxonlite zig build test-crash
run zx-cabi "capi smoke: all checks passed" zaxonlite zig build test-cabi
run zx-cli "all checks passed" zaxonlite zig build test-cli
run zx-cluster "all 1 run(s) passed" zaxonlite zig build test-cluster
run zx-replace "scenario complete" zaxonlite zig build test-replace-cluster
run zx-transfer "scenario complete" zaxonlite zig build test-transfer-cluster
run zx-roles "role cluster:" zaxonlite zig build test-roles
run zx-fault "slow-sync passed" zaxonlite zig build test-fault-network
run zx-bench "positive control: anchors visible" zaxonlite zig build benchmark

echo "verify: every gate passed"
