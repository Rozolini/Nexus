# Nexus

**Adaptive locality-first storage engine (Rust).** A single-node
append-only KV store that continuously reshapes its on-disk layout so
that records observed to be *read together* end up *stored together* —
turning logical co-access into fewer, larger disk reads.

> **Status.** Single-node research storage engine, not a production
> database. Results are reproducible from the repository via
> [Quick start](#quick-start).

## What is Nexus

- **Append-only on-disk format.** Sealed segment files with per-record
  CRC32, an in-segment offset footer, and a JSON manifest written via
  atomic rename.
- **In-memory primary index.** `key → (SegmentId, offset, version, size)`
  with deterministic tie-break and tombstone semantics.
- **A feedback loop that reshapes the layout.** Reads feed a bounded
  co-access graph; a planner proposes relocation groups; a budgeted
  background scheduler rewrites small batches so that co-accessed keys
  land contiguous on disk.

~6–8k lines of Rust, narrow modules, no network, no query language, no
MVCC. Storage substrate plus a reproducible measurement harness — nothing
more.

## Core thesis

> *Physical locality is earned, not declared. A storage engine that
> continuously co-locates records observed to be read together can
> translate that locality into measurably fewer disk I/O operations.*

The chain from observation to saved I/O is short:

1. `get_many` emits one bounded `CoReadEvent` per batch → `CoAccessGraph`.
2. `build_layout_plan` groups strongly connected keys; the scheduler
   rewrites them contiguous in a fresh segment (guarded against
   already-colocated groups).
3. `Engine::get_many` groups the request by `SegmentId`, sorts by offset,
   and **merges adjacent records into one `seek + read_exact` per range**
   under `ReadMergePolicy { max_read_gap_bytes, max_range_bytes }`.

The end-to-end measurement of this chain is `range_read_ops` — the
count of actual `read_exact` calls.

## Design and evaluation choices

- **Physical outcome as the primary metric.** `range_read_ops` and
  `file_opens` are counted directly; we do not derive a "locality score".
- **Separate baseline and adapted engines.** Every run creates two
  disjoint engines on two disjoint data dirs, loads the same dataset, and
  replays the same workload sequence. The only difference is whether the
  adaptation pipeline runs on the adapted engine.
- **Multi-run with subprocess isolation.** A single run's p95 is noisy.
  Each scenario is repeated `N ≥ 11` times with a deterministic seed
  schedule, and `--mode subprocess` runs every measurement in a fresh
  child process to reset user-space allocators and FD tables between runs.
- **Regression accounting.** Every scenario reports
  `positive_gain_runs / negative_gain_runs / zeroish_gain_runs` and
  adjacent-run sign flips alongside the median gain.

## Architecture at a glance

```
 put/get/get_many ──► Engine ──► PrimaryIndex (key → seg,off,ver,size)
                          │
 reads ─► ReadTracker ─► CoAccessGraph ─► Planner ─► Scheduler ─► relocate
                                                                     │
                         range-merged read  ◄── sealed segments ◄────┘
```

- `engine/` — public API, read/write paths.
- `storage/` — sealed-segment format, record codec, manifest.
- `tracker/` + `graph/` — bounded co-access observation, decay.
- `planner/` — deterministic plan from a graph snapshot.
- `scheduler/` + `compaction/` — budgeted relocation, atomic install.
- `benchmark/` — multi-run harness, metrics, JSON/CSV export.

## Quick start

```bash
# Lint gate.
cargo clippy --all-targets -- -D warnings

# Full test suite (integration + unit). ~30s on a warm release cache.
cargo test --release

# Reproducible multi-run benchmark with subprocess isolation.
cargo run --release --bin bench -- --runs 11 --mode subprocess --out-dir ./bench-out
```

The main evaluation artifact is **`./bench-out/summary.json`** — one
entry per scenario with aggregated medians, IQR, and regression counts.
`raw_runs.json` holds the per-run scalars for ad-hoc analysis.

## Official evaluation commands

Three presets, each writes into `./bench-out/<preset-name>/`:

```bash
# Smoke (~20s). Quick regression check.
bash scripts/bench-official-small.sh        # or: pwsh scripts/bench-official-small.ps1

# Reference preset (~2–5 min). Preset used for the results in docs/results.md.
bash scripts/bench-official-medium.sh       # or: pwsh scripts/bench-official-medium.ps1

# Reference preset with subprocess isolation (~10–15 min on Windows).
bash scripts/bench-official-subprocess.sh   # or: pwsh scripts/bench-official-subprocess.ps1
```

Custom knobs (decimal or `0x…` seed):

```bash
cargo run --release --bin bench -- \
    --runs 21 --base-seed 0xDEADBEEF \
    --key-space 8000 --batches 96 --cycles 24 \
    --out-dir ./bench-out --csv
```

## How to interpret results

Four numbers describe the measured effect, in order:

- **`range_ops b→a`** — median `seek + read_exact` calls per batch,
  baseline → adapted. This is the value the engine is optimised to
  reduce.
- **`rec/range b→a`** — `records_in_ranges / range_read_ops`. Rising
  rec/range means each read returns more user records — the direct
  consequence of a relocation that landed co-accessed keys contiguously.
- **`file_opens b→a`** — distinct segment files per batch; tracks
  `range_ops` when one segment yields one range.
- **`median p95_gain [min, max]`** — signed p95 gain per run plus its
  min/max envelope. Negative means adapted got slower. Always read
  alongside `+/-/0` (run counts) and `flips` (adjacent-run sign flips).

Machine-readable form: `summary.json`; schema inline in
`src/benchmark/multirun.rs`. Benchmark methodology, primary metrics,
and residual bias are documented in
[`docs/evaluation.md`](docs/evaluation.md).

## Current scope and limitations

- **Nexus is not a production database replacement.** See
  [`docs/positioning.md`](docs/positioning.md).
- **Single-node, single-writer.** No replication, no network layer, no
  crash-consistency guarantees beyond per-record CRC and manifest atomic
  rename.
- **In-memory primary index.** Must fit in RAM.
- **No on-disk GC beyond relocation.** Dead versions are reclaimed only
  when a key's latest version migrates; long-running adversarial
  workloads can grow footprint over time.
- **Synchronous read path.** No async / io_uring / readahead —
  intentional, so the range-merge saving is not confounded by other
  I/O-hiding tricks.
- **Workload matters.** Clustered / group-correlated / read-heavy mixes
  show a clear positive gain; uniform random and adversarial alternation
  hover near zero and are reported as such. See
  [`docs/results.md`](docs/results.md).

## Read next

**Quick review path (≈15 min)**

1. Skim this README.
2. `cargo test --release` — exercises all invariants.
3. `bash scripts/bench-official-small.sh` (or `.ps1`).
4. Open `bench-out/official-small/summary.json` — the primary evaluation artifact.

**Deeper docs**

- [`docs/positioning.md`](docs/positioning.md) — project scope: what
  Nexus is and is not.
- [`docs/evaluation.md`](docs/evaluation.md) — methodology, multi-run +
  subprocess rationale, seed derivation, residual bias.
- [`docs/results.md`](docs/results.md) — current results: what is stably
  positive, what is noisy.

**Useful files to inspect first**

- `src/benchmark/multirun.rs` — run aggregation and export.
- `src/engine/api.rs::get_many_inner` — the range-merged read path.
- `src/scheduler/background.rs` — already-colocated guard and budgets.

---

License: see `LICENSE`.
