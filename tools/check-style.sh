#!/bin/sh
set -eu

# Scan every source tree that exists in this checkout: the monorepo
# carries zaxonlite/src, the split paxos-zig repository does not.
roots=""
for root in build.zig src examples benchmarks integration/consumer sim zaxonlite/src; do
    if [ -e "$root" ]; then
        roots="$roots $root"
    fi
done

find $roots \
    -type d \( -name .zig-cache -o -name zig-out -o -name target \) -prune -o \
    -type f -name '*.zig' -print | sort | while IFS= read -r source; do
    awk -f tools/check-style.awk "$source"
done
