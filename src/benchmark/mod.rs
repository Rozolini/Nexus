//! # Benchmark harness
//!
//! Everything needed to run a deterministic, reproducible comparison of
//! Nexus-with-adaptation ("adapted") vs. Nexus-without-adaptation
//! ("baseline") under a fixed workload. The harness is organised around
//! three layers:
//!
//! * [`harness`]  — single-scenario driver: opens two engines, runs the
//!   same workload against both, captures per-query samples.
//! * [`metrics`]  — the per-query `QuerySample` type and its aggregation.
//! * [`report`]   — user-visible report shapes (phase report, relocation
//!   trace, stabilisation summary) used by `bin/bench` and tests.
//! * [`multirun`] — statistical layer: runs a scenario `N` times
//!   with a deterministic seed schedule, aggregates median/IQR, and can
//!   optionally fan out to subprocesses for page-cache isolation.
//! * [`stats`]    — small, dependency-free statistics helpers (median,
//!   percentile, IQR) used by [`multirun`].
//!
//! The harness is **pure I/O + math**: no global state, no time-based
//! decisions. All randomness is seeded; all ordering is deterministic.

pub mod harness;
pub mod metrics;
pub mod multirun;
pub mod report;
pub mod stats;

pub use harness::{run_benchmark, BenchmarkHarnessConfig};
pub use metrics::{colocated_pair_ratio, AggregatedMetrics, QuerySample};
pub use multirun::{
    aggregate_runs, derive_seed, run_multi_single_process, run_once,
    run_single_scenario_and_emit_json, write_multirun_artifacts, IsolationMode, MultiRunConfig,
    MultiRunReport, MultiRunSummary, MultirunArtifacts, RunResult, ZEROISH_GAIN_THRESHOLD,
};
pub use report::{
    AggregatedReportNumbers, BenchmarkReport, PhaseReport, RelocationTrace, StabilizationReport,
};
