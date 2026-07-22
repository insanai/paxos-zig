#!/bin/sh
set -eu

find build.zig src examples benchmarks integration/consumer sim zaxonlite/src \
    -type d \( -name .zig-cache -o -name zig-out -o -name target \) -prune -o \
    -type f -name '*.zig' -print | sort | while IFS= read -r source; do
    awk -f tools/check-style.awk "$source"
done

