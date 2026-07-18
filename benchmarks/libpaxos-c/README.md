# LibPaxos3 C benchmark

This benchmark compiles the unmodified LibPaxos3 core at revision
`d255f8b67a32d5e0ef43ac1a393b72cee23d8e0e` with its in-memory storage.
The revision is fetched into `.zig-cache` and verified before compilation.

The workload uses three acceptors, 4,096 `uint64_t` values, seven samples, and
the median elapsed time. It maintains LibPaxos3's normal 128-slot phase-one
preexecution window. Advancing that window adds six phase-one messages to the
six phase-two messages for each value. Therefore its message count is not
semantically identical to the stable-leader Multi-Paxos workloads.

Run it with:

```sh
zig build benchmark-libpaxos
```

To use an existing checkout without network access:

```sh
LIBPAXOS_SOURCE=/path/to/libpaxos zig build benchmark-libpaxos
```
