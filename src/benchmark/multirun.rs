//! Multi-run statistical benchmark aggregator.
//!
//! A *run* is one full `baseline → stabilization → adapted` pass on separate engine
//! instances (see `harness::run_benchmark`). This module repeats every scenario `N` times
//! with a **deterministic** seed schedule derived from `base_seed` and aggregates
//! per-run scalars into medians, IQR, min/max and regression counters. All outputs
//! are reproducible across identical inputs (no clock, no thread-level non-determinism).

use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::benchmark::harness::{run_benchmark, BenchmarkHarnessConfig};
use crate::benchmark::report::BenchmarkReport;
use crate::benchmark::stats;
use crate::error::{NexusError, Result};

/// How runs are isolated. Subprocess mode must be implemented by the **caller**
/// (e.g. the `bench` binary spawns child processes that call
/// [`run_single_scenario_and_emit_json`]); the lib only records the mode for reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationMode {
    SingleProcess,
    Subprocess,
}

impl IsolationMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            IsolationMode::SingleProcess => "single-process",
            IsolationMode::Subprocess => "subprocess",
        }
    }
}

/// Deterministic, high-quality seed derivation from `(base_seed, run_id)`.
/// Uses the golden-ratio constant 0x9E37… so that small `base_seed` values still
/// produce well-distributed per-run seeds.
pub fn derive_seed(base_seed: u64, run_id: u32) -> u64 {
    const PHI: u64 = 0x9E37_79B9_7F4A_7C15;
    base_seed.wrapping_add((run_id as u64).wrapping_mul(PHI))
}

/// Scalar projection of one `BenchmarkReport` into the numbers the multi-run
/// aggregator cares about. Serializable so subprocess mode can exchange this type
/// over stdout as JSON.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunResult {
    pub run_id: u32,
    pub seed: u64,
    pub scenario: String,

    // latency — per-phase p50/p95
    pub p50_baseline_ns: u64,
    pub p50_adapted_ns: u64,
    pub p95_baseline_ns: u64,
    pub p95_adapted_ns: u64,

    // physical read-path
    pub range_ops_baseline: f64,
    pub range_ops_adapted: f64,
    pub range_bytes_baseline: f64,
    pub range_bytes_adapted: f64,
    pub file_opens_baseline: f64,
    pub file_opens_adapted: f64,
    pub rec_per_range_baseline: f64,
    pub rec_per_range_adapted: f64,

    // signed gains (copied from the per-run stabilization report)
    pub p95_gain: f64,
    pub range_ops_gain: f64,
    pub range_bytes_gain: f64,
    pub file_opens_gain: f64,
    pub physical_ops_gain: f64,

    // cost/effort
    pub rewrite_amplification: f64,
    pub planner_actions: u64,
    pub rewrite_bytes_total: u64,

    pub elapsed_ms: u128,
}

impl RunResult {
    /// Project a full `BenchmarkReport` into a `RunResult` keyed by `(run_id, seed)`.
    pub fn from_report(
        run_id: u32,
        seed: u64,
        scenario: &str,
        rep: &BenchmarkReport,
        elapsed_ms: u128,
    ) -> Self {
        let b = &rep.baseline.aggregated;
        let a = &rep.after_stabilization.aggregated;
        let s = &rep.stabilization;
        Self {
            run_id,
            seed,
            scenario: scenario.to_string(),
            p50_baseline_ns: b.p50_read_latency_ns,
            p50_adapted_ns: a.p50_read_latency_ns,
            p95_baseline_ns: b.p95_read_latency_ns,
            p95_adapted_ns: a.p95_read_latency_ns,
            range_ops_baseline: b.mean_range_read_ops,
            range_ops_adapted: a.mean_range_read_ops,
            range_bytes_baseline: b.mean_range_bytes_read,
            range_bytes_adapted: a.mean_range_bytes_read,
            file_opens_baseline: b.mean_file_opens,
            file_opens_adapted: a.mean_file_opens,
            rec_per_range_baseline: b.records_per_range_read_avg,
            rec_per_range_adapted: a.records_per_range_read_avg,
            p95_gain: s.locality_gain_score,
            range_ops_gain: s.range_ops_gain,
            range_bytes_gain: s.range_bytes_gain,
            file_opens_gain: s.file_opens_gain,
            physical_ops_gain: s.physical_ops_gain,
            rewrite_amplification: s.rewrite_amplification,
            planner_actions: s.planner_actions_total,
            rewrite_bytes_total: s.rewrite_bytes_total,
            elapsed_ms,
        }
    }
}

/// Aggregated statistics over `N` runs of the same scenario. All medians are
/// computed independently per field (no row-level joining). `negative_gain_runs`
/// + `positive_gain_runs` + `zeroish_gain_runs` == total runs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MultiRunSummary {
    pub scenario: String,
    pub runs: usize,
    pub base_seed: u64,
    pub mode: IsolationMode,

    // median latency
    pub median_p50_baseline_ns: f64,
    pub median_p50_adapted_ns: f64,
    pub median_p95_baseline_ns: f64,
    pub median_p95_adapted_ns: f64,

    // p95 spread
    pub p95_adapted_iqr: f64,
    pub p95_adapted_min: f64,
    pub p95_adapted_max: f64,

    // median physical metrics
    pub median_range_ops_baseline: f64,
    pub median_range_ops_adapted: f64,
    pub median_range_bytes_baseline: f64,
    pub median_range_bytes_adapted: f64,
    pub median_file_opens_baseline: f64,
    pub median_file_opens_adapted: f64,
    pub median_rec_per_range_baseline: f64,
    pub median_rec_per_range_adapted: f64,

    // signed p95 gain distribution
    pub median_p95_gain: f64,
    pub min_p95_gain: f64,
    pub max_p95_gain: f64,

    // signed physical gains (medians)
    pub median_range_ops_gain: f64,
    pub median_range_bytes_gain: f64,
    pub median_file_opens_gain: f64,

    // regression accounting — threshold for "zeroish" is fixed at 0.01 (|gain| < 0.01).
    pub zeroish_gain_threshold: f64,
    pub negative_gain_runs: usize,
    pub positive_gain_runs: usize,
    pub zeroish_gain_runs: usize,
    /// Number of adjacent-run sign flips in the `p95_gain` sequence.
    pub sign_flip_count: usize,

    // cost
    pub median_rewrite_amplification: f64,
    pub median_planner_actions: f64,
}

/// One complete multi-run report — the summary plus raw per-run scalars.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MultiRunReport {
    pub summary: MultiRunSummary,
    pub raw_runs: Vec<RunResult>,
}

/// Multi-run configuration. The `harness_template.seed` is **ignored** — the
/// aggregator re-seeds every run via [`derive_seed`].
#[derive(Debug, Clone)]
pub struct MultiRunConfig {
    pub scenario_name: String,
    pub runs: usize,
    pub base_seed: u64,
    pub mode: IsolationMode,
    pub data_dir_root: PathBuf,
    pub harness_template: BenchmarkHarnessConfig,
}

/// Fixed regression threshold for "zeroish" gains (|gain| < threshold).
pub const ZEROISH_GAIN_THRESHOLD: f64 = 0.01;

/// Aggregate a pre-collected vector of `RunResult`s into a `MultiRunSummary`.
/// This is the pure function tested by unit tests; `run_multi_single_process`
/// glues it to the engine.
pub fn aggregate_runs(runs: &[RunResult], cfg: &MultiRunConfig) -> MultiRunSummary {
    let field = |f: fn(&RunResult) -> f64| -> Vec<f64> { runs.iter().map(f).collect() };

    let p50_b = field(|r| r.p50_baseline_ns as f64);
    let p50_a = field(|r| r.p50_adapted_ns as f64);
    let p95_b = field(|r| r.p95_baseline_ns as f64);
    let p95_a = field(|r| r.p95_adapted_ns as f64);

    let rop_b = field(|r| r.range_ops_baseline);
    let rop_a = field(|r| r.range_ops_adapted);
    let rby_b = field(|r| r.range_bytes_baseline);
    let rby_a = field(|r| r.range_bytes_adapted);
    let fo_b = field(|r| r.file_opens_baseline);
    let fo_a = field(|r| r.file_opens_adapted);
    let rpr_b = field(|r| r.rec_per_range_baseline);
    let rpr_a = field(|r| r.rec_per_range_adapted);

    let gains = field(|r| r.p95_gain);
    let range_ops_gains = field(|r| r.range_ops_gain);
    let range_bytes_gains = field(|r| r.range_bytes_gain);
    let file_opens_gains = field(|r| r.file_opens_gain);
    let amps = field(|r| r.rewrite_amplification);
    let plan_actions = field(|r| r.planner_actions as f64);

    let thr = ZEROISH_GAIN_THRESHOLD;
    let mut neg = 0usize;
    let mut pos = 0usize;
    let mut zer = 0usize;
    for g in &gains {
        if g.abs() < thr {
            zer += 1;
        } else if *g < 0.0 {
            neg += 1;
        } else {
            pos += 1;
        }
    }
    let mut flips = 0usize;
    for w in gains.windows(2) {
        let a = w[0];
        let b = w[1];
        if a.abs() >= thr && b.abs() >= thr && a.signum() != b.signum() {
            flips += 1;
        }
    }

    MultiRunSummary {
        scenario: cfg.scenario_name.clone(),
        runs: runs.len(),
        base_seed: cfg.base_seed,
        mode: cfg.mode,
        median_p50_baseline_ns: stats::median(&p50_b),
        median_p50_adapted_ns: stats::median(&p50_a),
        median_p95_baseline_ns: stats::median(&p95_b),
        median_p95_adapted_ns: stats::median(&p95_a),
        p95_adapted_iqr: stats::iqr(&p95_a),
        p95_adapted_min: stats::min_f(&p95_a),
        p95_adapted_max: stats::max_f(&p95_a),
        median_range_ops_baseline: stats::median(&rop_b),
        median_range_ops_adapted: stats::median(&rop_a),
        median_range_bytes_baseline: stats::median(&rby_b),
        median_range_bytes_adapted: stats::median(&rby_a),
        median_file_opens_baseline: stats::median(&fo_b),
        median_file_opens_adapted: stats::median(&fo_a),
        median_rec_per_range_baseline: stats::median(&rpr_b),
        median_rec_per_range_adapted: stats::median(&rpr_a),
        median_p95_gain: stats::median(&gains),
        min_p95_gain: stats::min_f(&gains),
        max_p95_gain: stats::max_f(&gains),
        median_range_ops_gain: stats::median(&range_ops_gains),
        median_range_bytes_gain: stats::median(&range_bytes_gains),
        median_file_opens_gain: stats::median(&file_opens_gains),
        zeroish_gain_threshold: thr,
        negative_gain_runs: neg,
        positive_gain_runs: pos,
        zeroish_gain_runs: zer,
        sign_flip_count: flips,
        median_rewrite_amplification: stats::median(&amps),
        median_planner_actions: stats::median(&plan_actions),
    }
}

/// Execute a single scenario run *in-process*. Used by both `run_multi_single_process`
/// and the subprocess child mode (see `run_single_scenario_and_emit_json`).
pub fn run_once(
    data_dir_root: &Path,
    scenario_name: &str,
    run_id: u32,
    seed: u64,
    mut harness_template: BenchmarkHarnessConfig,
) -> Result<RunResult> {
    harness_template.seed = seed;
    let dir = data_dir_root.join(format!("{}_run{:02}", scenario_name, run_id));
    if dir.exists() {
        let _ = std::fs::remove_dir_all(&dir);
    }
    std::fs::create_dir_all(&dir).map_err(|e| NexusError::io(&dir, e))?;
    let t0 = Instant::now();
    let report = run_benchmark(&dir, &harness_template)?;
    let elapsed_ms = t0.elapsed().as_millis();
    // Best-effort cleanup — leaving stale dirs is harmless for correctness but wastes disk.
    let _ = std::fs::remove_dir_all(&dir);
    Ok(RunResult::from_report(
        run_id,
        seed,
        scenario_name,
        &report,
        elapsed_ms,
    ))
}

/// Run `cfg.runs` iterations of one scenario in the current process, deterministically
/// re-seeding each run. Every run uses a **fresh data dir** under `cfg.data_dir_root`.
pub fn run_multi_single_process(cfg: &MultiRunConfig) -> Result<MultiRunReport> {
    let mut raw: Vec<RunResult> = Vec::with_capacity(cfg.runs);
    for i in 0..cfg.runs {
        let seed = derive_seed(cfg.base_seed, i as u32);
        let r = run_once(
            &cfg.data_dir_root,
            &cfg.scenario_name,
            i as u32,
            seed,
            cfg.harness_template.clone(),
        )?;
        raw.push(r);
    }
    let summary = aggregate_runs(&raw, cfg);
    Ok(MultiRunReport {
        summary,
        raw_runs: raw,
    })
}

/// Subprocess child entrypoint: execute **one** scenario run and print a single
/// `RunResult` JSON line to stdout. The parent process reads this line and aggregates.
/// `run_id` is used only for diagnostic labelling.
pub fn run_single_scenario_and_emit_json(
    data_dir_root: &Path,
    scenario_name: &str,
    run_id: u32,
    seed: u64,
    harness_template: BenchmarkHarnessConfig,
) -> Result<String> {
    let r = run_once(data_dir_root, scenario_name, run_id, seed, harness_template)?;
    serde_json::to_string(&r).map_err(|e| NexusError::Internal(format!("json serialize: {e}")))
}

/// Write `raw_runs.json`, `summary.json` and (optionally) `runs.csv` into `out_dir`.
/// Files are overwritten. Returns the three paths on success.
pub fn write_multirun_artifacts(
    out_dir: &Path,
    reports: &[MultiRunReport],
    emit_csv: bool,
) -> Result<MultirunArtifacts> {
    std::fs::create_dir_all(out_dir).map_err(|e| NexusError::io(out_dir, e))?;
    let raw_path = out_dir.join("raw_runs.json");
    let sum_path = out_dir.join("summary.json");
    let csv_path = out_dir.join("runs.csv");

    let raw_payload: Vec<&RunResult> = reports.iter().flat_map(|r| r.raw_runs.iter()).collect();
    let summaries: Vec<&MultiRunSummary> = reports.iter().map(|r| &r.summary).collect();

    let raw_json = serde_json::to_string_pretty(&raw_payload)
        .map_err(|e| NexusError::Internal(format!("json raw: {e}")))?;
    let sum_json = serde_json::to_string_pretty(&summaries)
        .map_err(|e| NexusError::Internal(format!("json sum: {e}")))?;
    std::fs::write(&raw_path, raw_json).map_err(|e| NexusError::io(&raw_path, e))?;
    std::fs::write(&sum_path, sum_json).map_err(|e| NexusError::io(&sum_path, e))?;

    if emit_csv {
        let mut buf = String::new();
        buf.push_str(
            "scenario,run_id,seed,p50_baseline_ns,p50_adapted_ns,p95_baseline_ns,p95_adapted_ns,\
range_ops_baseline,range_ops_adapted,range_bytes_baseline,range_bytes_adapted,\
file_opens_baseline,file_opens_adapted,rec_per_range_baseline,rec_per_range_adapted,\
p95_gain,range_ops_gain,range_bytes_gain,file_opens_gain,rewrite_amplification,\
planner_actions,elapsed_ms\n",
        );
        for r in &raw_payload {
            buf.push_str(&format!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
                csv_escape(&r.scenario),
                r.run_id,
                r.seed,
                r.p50_baseline_ns,
                r.p50_adapted_ns,
                r.p95_baseline_ns,
                r.p95_adapted_ns,
                r.range_ops_baseline,
                r.range_ops_adapted,
                r.range_bytes_baseline,
                r.range_bytes_adapted,
                r.file_opens_baseline,
                r.file_opens_adapted,
                r.rec_per_range_baseline,
                r.rec_per_range_adapted,
                r.p95_gain,
                r.range_ops_gain,
                r.range_bytes_gain,
                r.file_opens_gain,
                r.rewrite_amplification,
                r.planner_actions,
                r.elapsed_ms,
            ));
        }
        std::fs::write(&csv_path, buf).map_err(|e| NexusError::io(&csv_path, e))?;
    }

    Ok(MultirunArtifacts {
        raw_runs_json: raw_path,
        summary_json: sum_path,
        runs_csv: if emit_csv { Some(csv_path) } else { None },
    })
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        let esc = s.replace('"', "\"\"");
        format!("\"{esc}\"")
    } else {
        s.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct MultirunArtifacts {
    pub raw_runs_json: PathBuf,
    pub summary_json: PathBuf,
    pub runs_csv: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mkrun(id: u32, seed: u64, p95_b: u64, p95_a: u64, gain: f64) -> RunResult {
        RunResult {
            run_id: id,
            seed,
            scenario: "T".into(),
            p50_baseline_ns: 100,
            p50_adapted_ns: 80,
            p95_baseline_ns: p95_b,
            p95_adapted_ns: p95_a,
            range_ops_baseline: 5.0,
            range_ops_adapted: 3.0,
            range_bytes_baseline: 2000.0,
            range_bytes_adapted: 1500.0,
            file_opens_baseline: 4.0,
            file_opens_adapted: 2.0,
            rec_per_range_baseline: 1.5,
            rec_per_range_adapted: 3.0,
            p95_gain: gain,
            range_ops_gain: 0.4,
            range_bytes_gain: 0.25,
            file_opens_gain: 0.5,
            physical_ops_gain: 0.4,
            rewrite_amplification: 0.1,
            planner_actions: 10,
            rewrite_bytes_total: 1024,
            elapsed_ms: 500,
        }
    }

    #[test]
    fn derive_seed_is_injective_for_distinct_runs() {
        let a = derive_seed(42, 0);
        let b = derive_seed(42, 1);
        let c = derive_seed(42, 2);
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert_ne!(a, c);
        // Deterministic.
        assert_eq!(derive_seed(42, 1), b);
    }

    #[test]
    fn aggregate_counts_signed_gains() {
        let cfg = MultiRunConfig {
            scenario_name: "T".into(),
            runs: 5,
            base_seed: 1,
            mode: IsolationMode::SingleProcess,
            data_dir_root: PathBuf::from("."),
            harness_template: BenchmarkHarnessConfig::default(),
        };
        let runs = vec![
            mkrun(0, 1, 100, 50, 0.5),
            mkrun(1, 2, 100, 60, 0.4),
            mkrun(2, 3, 100, 100, 0.0),
            mkrun(3, 4, 100, 150, -0.5),
            mkrun(4, 5, 100, 120, -0.2),
        ];
        let s = aggregate_runs(&runs, &cfg);
        assert_eq!(s.positive_gain_runs, 2);
        assert_eq!(s.negative_gain_runs, 2);
        assert_eq!(s.zeroish_gain_runs, 1);
        assert!((s.median_p95_gain - 0.0).abs() < 1e-12);
        // sign flips: 0.5 → 0.4 (same), 0.4 → 0 skipped, 0 → -0.5 skipped, -0.5 → -0.2 (same) => 0
        assert_eq!(s.sign_flip_count, 0);
    }

    #[test]
    fn sign_flip_count_detects_alternation() {
        let cfg = MultiRunConfig {
            scenario_name: "T".into(),
            runs: 4,
            base_seed: 1,
            mode: IsolationMode::SingleProcess,
            data_dir_root: PathBuf::from("."),
            harness_template: BenchmarkHarnessConfig::default(),
        };
        let runs = vec![
            mkrun(0, 1, 100, 50, 0.5),
            mkrun(1, 2, 100, 200, -0.5),
            mkrun(2, 3, 100, 50, 0.5),
            mkrun(3, 4, 100, 200, -0.5),
        ];
        let s = aggregate_runs(&runs, &cfg);
        assert_eq!(s.sign_flip_count, 3);
    }
}
