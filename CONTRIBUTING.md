# Contributing

Run `zig build fmt`, `zig build test`, `zig build run`, and
`zig build benchmark` before submitting a change. The benchmark requires Rust,
Cargo, and Git because it uses locked OmniPaxos 0.2.2 and LibPaxos3 revisions.
Protocol changes should add a test that explains the failure schedule being
exercised.

The project follows TigerStyle's priorities: safety, performance, and developer
experience, in that order. Keep control flow explicit, bound memory and work,
state invariants positively, handle every error, explain why in comments, and
keep Zig source lines at or below 100 columns.

Never weaken the write-before-send rule. A message acknowledging a promise,
acceptance, or commit may be emitted only alongside the durable record that must
precede it.
