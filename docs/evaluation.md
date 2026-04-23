# Evaluation methodology

This document describes **how Nexus is measured** — the constructs, the
choices, and the biases we are aware of. It is deliberately longer than
the benchmark numbers in [`results.md`](results.md); the methodology is
documented first so the numbers can be read in its context.

## What is measured

A **run** is one end-to-end execution of:

```
1. Load a deterministic dataset into a baseline engine.
2. Replay a workload sequence and record per-batch metrics (baseline phase).
3. On a second, independent engine, load the same dataset.
4. Let the co-access graph + planner + scheduler run `stabilization_cycles` times.
5. Replay the same workload sequence and record per-batch metrics (adapted phase).
```

Within one run, baseline and adapted see **identical** data, seeds, and
per-batch key sequences — verified by `workload_sequence_digest`, which is part
of every `BenchmarkReport`.

Per batch, the harness records:

- `latency` (wall clock, `Instant::now()`),
- `file_opens`, `physical_read_ops`, `physical_bytes_read`,
- `segment_groups`, `offsets_span_total`,
- Phase 10 range-merged counters: `range_read_ops`, `range_bytes_read`,
  `records_in_ranges`, `range_merges`, `gap_bytes_merged`.

These are projected into per-phase aggregates (p50, p95, means, sums) by
`AggregatedMetrics::from_samples`.

## Primary vs. secondary metrics

**Primary physical metrics (load-independent).** These are the primary
metrics used to evaluate the effect of adaptation:

- `range_read_ops` — actual `seek + read_exact` calls per batch. Falling =
  win.
- `records_per_range_read_avg` — user records returned per disk range. Rising
  = win. This is `range_merges` absorbing into a single open range.
- `file_opens` — distinct segment files opened per batch. Correlates with
  range_ops when a segment yields one range.

**Secondary metrics (useful context).**

- Raw `p50` / `p95` read latency in nanoseconds. They *do* move with the
  physical metrics, but they are the **noisiest** dimension in a user-space
  benchmark on commodity hardware; see "residual bias" below.
- `median p95_gain` — signed, relative; negative means adapted got slower.
- `mean_colocated_pair_ratio_logical` — same-segment pair ratio from the
  index alone. **Logical, not physical.** Not suitable as a primary
  measurement on its own.

**Cost metrics.**

- `rewrite_amplification = rewrite_bytes_total / user_write_bytes_total`
  across the stabilisation cycles. Lower is better; high amplification means
  the scheduler is spending a lot of bytes to reshape the layout.
- `planner_actions_total` — groups relocated over the stabilisation window.

## Why multi-run matters

A single run's p95 is noisy for three independent reasons that are not
removed by improvements to the code under test:

1. **OS scheduler noise.** Context switches and interrupt servicing add
   per-syscall jitter that can reach tens of µs on a contended desktop.
2. **Page-cache warming.** A second invocation of the same workload sees
   cached segment blocks; a first one does not.
3. **Allocator and layout noise.** `jemalloc`, `mimalloc` and the Windows
   heap all have workload-sensitive warmups; fresh allocations land on fresh
   pages.

The multi-run harness addresses this by running each scenario N ≥ 11
times and reporting the **median**, **IQR**, **min**, **max**, and the
**signed-gain distribution** (positive / negative / zeroish runs, plus
adjacent-run sign flips). Medians are robust to outliers; IQR quantifies
spread; sign-flip count is a coarse noise indicator.

### What constitutes "statistically robust" here

- **Median over mean.** Mean is sensitive to the worst tail; median is not.
- **IQR over stddev.** Latency distributions are *not* Gaussian; IQR is
  distribution-free.
- **Signed-gain counts.** Reporting +/−/0 directly exposes "N=11 with 3
  positives" — which is genuinely weak evidence — without hiding it behind a
  single median number.
- **No confidence intervals.** The harness does not print a CI because
  the sample of runs is not i.i.d. at the OS level, so a parametric CI
  would be misleading. IQR plus the raw runs in `raw_runs.json` let a
  reader compute whatever interval they trust.

## Why subprocess mode is preferred

`--mode subprocess` spawns a fresh child process per run. Compared to the
single-process mode, this:

- **Resets user-space state.** Allocator arenas, index `HashMap` bucket
  layouts, and thread-local caches all start cold. A single-process sequence
  of 11 runs will warm these up; that warm-up is *not* a property of the
  algorithm being measured.
- **Resets file descriptor state.** Closed FDs are returned to the OS, and
  per-process read-ahead windows reset.
- **Partially resets the OS page cache.** The kernel can still serve hot
  pages from the global cache, but many userspace-side readahead heuristics
  are per-process. On Linux this is modest; on Windows SuperFetch it is
  larger but still meaningful.
- **Prevents measurement contamination.** If the harness itself has a bug
  where state leaks between runs, subprocess mode hides it. Single-process
  mode would compound it.

Subprocess mode is slower on Windows (`CreateProcess` ≈ 200–400 ms per
child). For a small `--key-space` the spawn cost dominates; for
`--key-space ≥ 4000` it is a few seconds out of tens.

The preferred preset for evaluation is
[`scripts/bench-official-subprocess.sh`](../scripts/bench-official-subprocess.sh).

## How seeds are derived

```
seed_i = base_seed .wrapping_add( (run_id as u64) .wrapping_mul(0x9E37_79B9_7F4A_7C15) )
```

`0x9E37…` is the 64-bit fractional part of the golden ratio — a standard
choice for fixed-stride hash mixing (e.g. `HashMap` in Rust's stdlib,
Fibonacci hashing). The practical properties we care about:

- **Deterministic.** Same `base_seed + run_id` → same seed, bit-for-bit. Any
  two runs of `bench --runs N --base-seed X` produce the same seed schedule.
- **Well-distributed for small base seeds.** `base_seed = 0` would give `i·φ`
  rather than `i` — no clumping.
- **Injective in practice.** For `run_id ≤ 2³²` the multiplied offset does
  not collide; we do not need cryptographic injectivity.

This is **not** a CSPRNG. Cryptographic unpredictability is not a goal
for a benchmark harness; the goals are reproducibility and the absence
of low-order correlation, both of which the golden-ratio mix provides.

## How to interpret median and IQR

Given N runs of adapted p95 latency `p95_a[0..N]`:

- **`median_p95_adapted_ns`** — "the typical p95 the adapter produces." If
  you were to run one more time, this is your best point estimate.
- **`p95_adapted_iqr`** — spread between the 25th and 75th percentile of
  those same values. A practical estimate of measurement spread under
  the current preset. If `IQR / median_baseline_p95 > |median_p95_gain|`,
  the gain is smaller than the observed spread and should not be
  reported as a win.
- **`p95_adapted_min`, `p95_adapted_max`** — envelope across the N runs.
  Useful for communicating "worst case observed under this preset".

We also report `median_p95_gain`, `min_p95_gain`, `max_p95_gain`. The
**min gain** serves as a practical proxy for a worst-case bound under
the preset. If `min_p95_gain > 0` across N ≥ 11 runs, the measured
improvement is consistently positive under this preset. If
`min_p95_gain ≪ 0`, the result should be treated with caution.

## Regression accounting

Every `MultiRunSummary` contains:

- `positive_gain_runs` — count of runs with `p95_gain ≥ 0.01` (1% speedup).
- `negative_gain_runs` — count with `p95_gain ≤ -0.01` (1% regression).
- `zeroish_gain_runs` — count with `|p95_gain| < 0.01` (noise band).
- `sign_flip_count` — number of adjacent-run flips in the `p95_gain`
  sequence after zeroish runs are removed from the sign test. High
  values indicate oscillation, which usually reflects measurement noise
  rather than algorithmic behaviour.

The zeroish threshold is exposed as `ZEROISH_GAIN_THRESHOLD = 0.01`; if you
disagree with it, the raw per-run gains are in `raw_runs.json`.

## Known residual bias

The items below are inherent to user-space benchmarking and are not
eliminated by harness-level changes.

1. **Shared OS page cache.** Subprocess mode resets user-space state,
   not the kernel's page cache. A hot segment read by run 0 may still
   be in RAM when run 1 starts. A stricter approach would require
   cross-run `drop_caches` / `echo 3 > /proc/sys/vm/drop_caches` on
   Linux or a reboot on Windows; the harness does not perform either.
2. **First-run cold effects.** The first run of any multi-run sequence pays
   cold-cache and JIT-like allocator warmup costs. With N = 11 this is 1/11
   of the signal; with N = 3 it is 1/3 and may move the median.
3. **Wall-clock precision.** `Instant::now()` on Windows is ~100 ns
   resolution; latencies below ~1 µs have >10% quantisation error. Small
   key-space presets (`--key-space 500`) hit this floor. Use
   `--key-space ≥ 4000` for latency claims.
4. **Scheduler placement.** On a loaded laptop the OS scheduler can park the
   bench thread on an E-core mid-run. We do not pin affinity.
5. **Thermal throttling.** Sustained CPU load for several minutes can lower
   frequency. This affects baseline and adapted *equally*, so signed gains
   survive, but absolute p95 values drift.
6. **Filesystem-level readahead.** Linux `readahead` and Windows
   prefetch can speculatively read bytes adjacent to our `ReadRange`,
   which can make range merging appear cheaper than the in-engine work
   alone would be. Our counter is the `read_exact` call, not the bytes
   the kernel actually fetched; the metric remains useful as a count of
   range reads issued by the engine even when the kernel serves some
   of them from prefetched pages.
7. **SkewedZipfian phase skew.** The zipfian generator is seeded but its
   per-batch key sets depend sensitively on the seed; two adjacent seeds can
   produce materially different hot-key distributions. The multi-run median
   averages over these; a single run's p95 does not.

## Criteria for interpreting a positive result

For a scenario to be reported as a win under the `official-medium`
preset (`--runs 11`, `--key-space 8000`, `--batches 96`, `--cycles 24`),
the following conditions are expected:

- `median_p95_gain > 0.05` (≥ 5% median p95 improvement),
- `positive_gain_runs ≥ 8` (of 11),
- `median range_ops_adapted < median range_ops_baseline`,
- `median_rec_per_range_adapted > median_rec_per_range_baseline`,
- `p95_adapted_iqr / median_p95_baseline < median_p95_gain` (gain
  exceeds the observed measurement spread).

Scenarios that do not meet these criteria are reported as noisy or
unfavourable rather than excluded — see [`results.md`](results.md).
