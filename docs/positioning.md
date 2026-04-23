# Positioning

Scope and boundaries: what Nexus is, what it is not, and its relationship
to prior art. If another document in this repository contradicts this one,
treat this one as the reference.

## What Nexus is

- A **single-node, single-writer, append-only key–value storage engine**
  written in ~7k lines of Rust with no unsafe code outside the standard
  library's own.
- A **reproducible measurement harness** that runs identical workloads against
  two independent engines (a passive baseline and an adapter-enabled
  "adapted") and reports paired, seed-deterministic physical I/O metrics.
- An **adaptive physical-layout system**: reads are observed into a bounded
  co-access graph; a deterministic planner proposes co-located groups; a
  budgeted background scheduler rewrites small batches of records so that
  recurring read batches collapse into one contiguous disk range read.
- **Explicit about its scope.** The harness reports medians, IQR,
  min/max, and the count of negative-gain runs for every scenario.
  Scenarios that do not benefit are reported as such.

## Out of scope

- **Not a database.** There is no query language, no transactions beyond
  per-record atomic write, no indexes other than the primary key, no MVCC,
  no secondary indexing, no snapshots.
- **Not a replacement for RocksDB, LMDB, or any production LSM / B-tree.**
  Those engines solve problems Nexus ignores: crash-recovery durability
  guarantees, write-amplification bounds under arbitrary workloads, on-disk
  space reclamation under long-running deletes, concurrent readers and
  writers, variable-sized tail-latency envelopes under contention.
- **Not a distributed system.** No replication, no cluster, no consensus,
  no networking. Single node, single process, single writer.
- **Not a general-purpose performance win.** On uniform random reads over
  a large key space the adapter has nothing to learn, and the scheduler's
  rewrite cost is pure overhead (small, but not negative). This is recorded
  in [`results.md`](results.md).
- **Not an index-only locality demonstration.** The primary measured outcome is
  `range_read_ops` — actual `seek + read_exact` calls per batch. It falls
  because the scheduler physically moves records, not because an index has
  been rewritten to describe a different layout.
- **Not a research paper.** No new algorithm is proposed. The co-access
  graph is elementary; the planner is greedy; the scheduler is budgeted by
  two scalars. The project focuses on integrating these parts and
  evaluating them under a documented measurement methodology.

## Benchmark and design properties

Properties the harness and engine together enforce:

1. **Matched baseline / adapted runs.** Every run builds two independent
   engines on two independent data directories and verifies via a
   workload digest that both replay the same seeded sequence.
2. **Physical metric as the primary measurement.** The harness counts
   `seek + read_exact` calls after merging (`range_read_ops`) instead of
   a derived "locality score". If the count does not fall, there is no
   measured improvement.
3. **Multi-run with reported spread.** N ≥ 11 runs with a deterministic
   seed schedule; every scenario reports median, IQR, min/max,
   positive/negative/zeroish run counts, and adjacent-run sign-flip
   counts.
4. **Subprocess isolation as a supported mode.** The bench binary
   recursively spawns itself as a child per run (`--mode subprocess`) to
   reset user-space allocator and FD state between runs.
5. **Regression accounting in the summary.** `negative_gain_runs` and
   `sign_flip_count` are always printed; every scenario appears in the
   output regardless of its sign.
6. **Determinism enforced by tests.** `tests/phase11_multirun_benchmark.rs`
   runs the multi-run harness twice with the same `base_seed` and asserts
   that seed schedule, range-read physical metrics, and planner counters
   are bit-for-bit identical.
7. **End-to-end testable read chain.** `tests/phase10_range_reads.rs` and
   the `RelocationTrace` entries in the benchmark report record, per
   relocation group, the predicted `range_reads_before`,
   `range_reads_after`, `file_opens_before`, `file_opens_after`.

## Relationship to prior art

None of the individual ideas are new:

- Co-access graphs: standard in cache-aware data placement literature.
- Record-level coalescing reads: any database with a page cache performs
  a coarser form of this.
- Budgeted background rewriters: every LSM engine has one.

This repository combines those parts in one small codebase,
with a test suite that ties the co-access → layout →
single-read chain together and a multi-run harness that reports the
signed effect with its spread. The methodology is written down in
[`evaluation.md`](evaluation.md) so a reviewer does not have to
reverse-engineer it from benchmark output.

## Review path

Read, in this order:

1. [`../README.md`](../README.md) — short summary and quick start.
2. This file (`positioning.md`) — scope and limits.
3. [`evaluation.md`](evaluation.md) — how numbers are produced.
4. [`results.md`](results.md) — what those numbers currently say.
5. `src/engine/api.rs::get_many_inner` and `src/scheduler/background.rs`
   — where the range-merged read path and the relocation loop are
   implemented.
