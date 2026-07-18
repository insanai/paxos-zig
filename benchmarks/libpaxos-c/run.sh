#!/bin/sh
set -eu

revision=d255f8b67a32d5e0ef43ac1a393b72cee23d8e0e
repository=https://bitbucket.org/sciascid/libpaxos.git
project_root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cache_root="$project_root/.zig-cache/benchmarks"
source_root=${LIBPAXOS_SOURCE:-"$cache_root/libpaxos-$revision"}
binary="$cache_root/libpaxos-benchmark"

mkdir -p "$cache_root"
if [ ! -d "$source_root/.git" ]; then
    git init -q "$source_root"
    git -C "$source_root" remote add origin "$repository"
    git -C "$source_root" fetch -q --depth 1 origin "$revision"
    git -C "$source_root" checkout -q --detach FETCH_HEAD
fi

actual_revision=$(git -C "$source_root" rev-parse HEAD)
if [ "$actual_revision" != "$revision" ]; then
    echo "expected LibPaxos revision $revision, found $actual_revision" >&2
    exit 1
fi

zig cc -O3 -DNDEBUG \
    -I "$source_root/paxos/include" \
    "$project_root/benchmarks/libpaxos-c/benchmark.c" \
    "$source_root/paxos/paxos.c" \
    "$source_root/paxos/acceptor.c" \
    "$source_root/paxos/learner.c" \
    "$source_root/paxos/proposer.c" \
    "$source_root/paxos/carray.c" \
    "$source_root/paxos/quorum.c" \
    "$source_root/paxos/storage.c" \
    "$source_root/paxos/storage_utils.c" \
    "$source_root/paxos/storage_mem.c" \
    -o "$binary"

exec "$binary"
