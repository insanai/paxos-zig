# Summary

<!-- What does this change do, and why? One or two short paragraphs. -->

## Checklist

- [ ] `zig build fmt` passes (zig fmt plus the style checker).
- [ ] `zig build test` passes (unit tests plus the seeded fault simulation).
- [ ] The write-before-send rule is untouched: no message that acknowledges a
      promise, acceptance, or commit is emitted before its durable record.
- [ ] Protocol changes add a test that explains the failure schedule being
      exercised, and note any simulation seed that motivated the change.
- [ ] Protocol changes keep the handler code, the simulator oracles,
      `specs/Paxos.tla`, and the book's conformance appendix
      (in [insanai/zxdocs](https://github.com/insanai/zxdocs)) in sync.
- [ ] Memory and work stay bounded; every error is handled; Zig lines are at
      or below 100 columns (TigerStyle).

## Verification

<!-- Paste the commands you ran and their results. If you replayed a
     simulation seed, include it: zig build sim -- --seed=N --steps=M -->
