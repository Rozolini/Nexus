//! Multi-run statistical benchmark: determinism, JSON/CSV export, regression accounting.

use nexus::{
    aggregate_runs, derive_seed, run_multi_single_process, write_multirun_artifacts,
    BenchmarkHarnessConfig, IsolationMode, MultiRunConfig, PlannerConfig, RunResult,
    SchedulerConfig, WorkloadPattern, WorkloadSpec, ZEROISH_GAIN_THRESHOLD,
};
use tempfile::tempdir;

fn tiny_template(pattern: WorkloadPattern) -> BenchmarkHarnessConfig {
    BenchmarkHarnessConfig {
        seed: 0,
        spec: WorkloadSpec {
            pattern,
            key_space: 256,
            num_query_batches: 16,
            query_batch_size: 8,
            cluster_count: 8,
            zipf_s: 1.1,
            read_fraction: 0.9,
            mixed_steps: 0,
            write_payload_len: 16,
        },
        load_key_count: 256,
        stabilization_cycles: 8,
        planner: PlannerConfig {
            rewrite_affinity_threshold: 0.05,
            min_expected_gain: 0.0,
            hysteresis_per_key: 0.0,
            max_keys_per_group: 8,
            ..Default::default()
        },
        scheduler: SchedulerConfig {
            max_groups_relocated_per_cycle: 4,
            max_bytes_rewritten_per_cycle: 256 * 1024,
            minimum_improvement_delta: 0.0,
            ..Default::default()
        },
        max_graph_edges: 500_000,
        initial_segment_rotation: 32,
        max_relocation_traces: 4,
    }
}

fn mkrun(id: u32, seed: u64, p95_a: u64, gain: f64) -> RunResult {
    RunResult {
        run_id: id,
        seed,
        scenario: "T".into(),
        p50_baseline_ns: 100,
        p50_adapted_ns: 80,
        p95_baseline_ns: 1000,
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

fn cfg_for(scenario: &str, runs: usize, seed: u64) -> MultiRunConfig {
    MultiRunConfig {
        scenario_name: scenario.into(),
        runs,
        base_seed: seed,
        mode: IsolationMode::SingleProcess,
        data_dir_root: std::env::temp_dir(),
        harness_template: tiny_template(WorkloadPattern::Clustered),
    }
}

// 1 -------------------------------------------------------------------------

#[test]
fn same_seed_schedule_produces_same_summary() {
    // What is deterministic across `run_multi_single_process` invocations with identical
    // inputs: the seed schedule, the workload-driven physical metrics (range_ops,
    // range_bytes, file_opens, rec_per_range), and the planner/rewrite cost counters.
    // Wall-clock-derived latency and the p95_gain score derived from it are NOT — they
    // depend on `Instant::now()` and are checked for finiteness + ordering elsewhere.
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let mut ca = cfg_for("Clustered", 4, 0xA1B2_C3D4);
    let mut cb = cfg_for("Clustered", 4, 0xA1B2_C3D4);
    ca.data_dir_root = dir_a.path().to_path_buf();
    cb.data_dir_root = dir_b.path().to_path_buf();

    let ra = run_multi_single_process(&ca).unwrap();
    let rb = run_multi_single_process(&cb).unwrap();

    assert_eq!(ra.raw_runs.len(), rb.raw_runs.len());
    for (x, y) in ra.raw_runs.iter().zip(rb.raw_runs.iter()) {
        assert_eq!(x.seed, y.seed);
        assert_eq!(x.run_id, y.run_id);
        // Physical read-path metrics must match bit-for-bit (same dataset + policy).
        assert_eq!(x.range_ops_baseline, y.range_ops_baseline);
        assert_eq!(x.range_ops_adapted, y.range_ops_adapted);
        assert_eq!(x.range_bytes_baseline, y.range_bytes_baseline);
        assert_eq!(x.range_bytes_adapted, y.range_bytes_adapted);
        assert_eq!(x.file_opens_baseline, y.file_opens_baseline);
        assert_eq!(x.file_opens_adapted, y.file_opens_adapted);
        assert_eq!(x.rec_per_range_baseline, y.rec_per_range_baseline);
        assert_eq!(x.rec_per_range_adapted, y.rec_per_range_adapted);
        assert_eq!(x.planner_actions, y.planner_actions);
        assert_eq!(x.rewrite_bytes_total, y.rewrite_bytes_total);
        assert_eq!(x.rewrite_amplification, y.rewrite_amplification);
    }
    // Scenario identity + config.
    assert_eq!(ra.summary.scenario, rb.summary.scenario);
    assert_eq!(ra.summary.runs, rb.summary.runs);
    assert_eq!(ra.summary.base_seed, rb.summary.base_seed);
    assert_eq!(ra.summary.mode, rb.summary.mode);
    // Physical metric medians match bit-for-bit.
    assert_eq!(
        ra.summary.median_range_ops_baseline,
        rb.summary.median_range_ops_baseline
    );
    assert_eq!(
        ra.summary.median_range_ops_adapted,
        rb.summary.median_range_ops_adapted
    );
    assert_eq!(
        ra.summary.median_file_opens_adapted,
        rb.summary.median_file_opens_adapted
    );
    assert_eq!(
        ra.summary.median_rec_per_range_adapted,
        rb.summary.median_rec_per_range_adapted
    );
    assert_eq!(
        ra.summary.median_rewrite_amplification,
        rb.summary.median_rewrite_amplification
    );
    assert_eq!(
        ra.summary.median_planner_actions,
        rb.summary.median_planner_actions
    );
}

// 2 -------------------------------------------------------------------------

#[test]
fn median_and_iqr_are_deterministic() {
    let cfg = cfg_for("T", 5, 1);
    // Deliberately shuffled p95_adapted values + fixed gains.
    let runs = vec![
        mkrun(0, 1, 900, 0.1),
        mkrun(1, 2, 500, 0.5),
        mkrun(2, 3, 700, 0.3),
        mkrun(3, 4, 300, 0.7),
        mkrun(4, 5, 1100, -0.1),
    ];
    let s1 = aggregate_runs(&runs, &cfg);
    // Re-shuffle input; summary must stay identical.
    let mut shuffled = runs.clone();
    shuffled.swap(0, 4);
    shuffled.swap(1, 3);
    let s2 = aggregate_runs(&shuffled, &cfg);
    // Sign-flip and min/max don't depend on order for medians, but sign_flip
    // does. Verify the order-insensitive ones separately.
    assert_eq!(s1.median_p95_adapted_ns, s2.median_p95_adapted_ns);
    assert_eq!(s1.p95_adapted_iqr, s2.p95_adapted_iqr);
    assert_eq!(s1.p95_adapted_min, s2.p95_adapted_min);
    assert_eq!(s1.p95_adapted_max, s2.p95_adapted_max);
    assert_eq!(s1.median_p95_gain, s2.median_p95_gain);
    assert_eq!(s1.positive_gain_runs, s2.positive_gain_runs);
    assert_eq!(s1.negative_gain_runs, s2.negative_gain_runs);
    // Concrete medians (5 values): p95 = 700, gain = 0.3.
    assert!((s1.median_p95_adapted_ns - 700.0).abs() < 1e-9);
    assert!((s1.median_p95_gain - 0.3).abs() < 1e-9);
    // IQR for [300, 500, 700, 900, 1100] = P75 - P25 = 900 - 500 = 400.
    assert!((s1.p95_adapted_iqr - 400.0).abs() < 1e-9);
}

fn make_artifacts(
    runs_count: u32,
    csv: bool,
) -> (
    tempfile::TempDir,
    nexus::MultirunArtifacts,
    nexus::MultiRunReport,
) {
    let cfg = cfg_for("Clustered", runs_count as usize, 0xDEAD_BEEF);
    let runs: Vec<RunResult> = (0..runs_count)
        .map(|i| mkrun(i, derive_seed(cfg.base_seed, i), 1000 - i as u64 * 100, 0.1))
        .collect();
    let summary = aggregate_runs(&runs, &cfg);
    let report = nexus::MultiRunReport {
        summary,
        raw_runs: runs,
    };
    let out = tempdir().unwrap();
    let arts = write_multirun_artifacts(out.path(), std::slice::from_ref(&report), csv).unwrap();
    (out, arts, report)
}

// 3 -------------------------------------------------------------------------

#[test]
fn summary_json_is_emitted() {
    let (_dir, arts, report) = make_artifacts(3, false);
    assert!(arts.summary_json.exists());
    assert!(arts.runs_csv.is_none());
    let sum_text = std::fs::read_to_string(&arts.summary_json).unwrap();
    let summaries: Vec<nexus::MultiRunSummary> = serde_json::from_str(&sum_text).unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0], report.summary);
    // Machine-readable: key fields present in the on-disk JSON as plain text.
    assert!(sum_text.contains("\"scenario\""));
    assert!(sum_text.contains("\"runs\""));
    assert!(sum_text.contains("\"negative_gain_runs\""));
    assert!(sum_text.contains("\"p95_adapted_iqr\""));
}

// 4 -------------------------------------------------------------------------

#[test]
fn raw_runs_json_contains_all_runs() {
    let (_dir, arts, report) = make_artifacts(5, true);
    assert!(arts.raw_runs_json.exists());
    let raw_text = std::fs::read_to_string(&arts.raw_runs_json).unwrap();
    let raw: Vec<RunResult> = serde_json::from_str(&raw_text).unwrap();
    assert_eq!(raw.len(), report.raw_runs.len());
    for (i, r) in raw.iter().enumerate() {
        assert_eq!(r.run_id, i as u32);
        assert_eq!(r.seed, report.raw_runs[i].seed);
    }
    // CSV sanity check: header + N data rows.
    let csv_path = arts.runs_csv.as_ref().unwrap();
    let csv_text = std::fs::read_to_string(csv_path).unwrap();
    let lines: Vec<&str> = csv_text.lines().collect();
    assert_eq!(lines.len(), raw.len() + 1);
    assert!(lines[0].starts_with("scenario,run_id,seed"));
}

// 5 -------------------------------------------------------------------------

#[test]
fn negative_gain_runs_count_is_correct() {
    let cfg = cfg_for("T", 6, 1);
    let runs = vec![
        mkrun(0, 1, 100, 0.2),   // +
        mkrun(1, 2, 100, -0.3),  // -
        mkrun(2, 3, 100, 0.0),   // 0
        mkrun(3, 4, 100, 0.005), // 0 (below threshold)
        mkrun(4, 5, 100, -0.4),  // -
        mkrun(5, 6, 100, 0.6),   // +
    ];
    let s = aggregate_runs(&runs, &cfg);
    assert_eq!(s.positive_gain_runs, 2);
    assert_eq!(s.negative_gain_runs, 2);
    assert_eq!(s.zeroish_gain_runs, 2);
    assert_eq!(
        s.positive_gain_runs + s.negative_gain_runs + s.zeroish_gain_runs,
        6
    );
    assert!((s.zeroish_gain_threshold - ZEROISH_GAIN_THRESHOLD).abs() < 1e-12);
}

// 6 -------------------------------------------------------------------------

#[test]
fn benchmark_report_includes_runs_and_mode() {
    let dir = tempdir().unwrap();
    let mut cfg = cfg_for("Clustered", 3, 0x1234_5678);
    cfg.data_dir_root = dir.path().to_path_buf();
    cfg.mode = IsolationMode::SingleProcess;
    let rep = run_multi_single_process(&cfg).unwrap();
    assert_eq!(rep.summary.runs, 3);
    assert_eq!(rep.summary.base_seed, 0x1234_5678);
    assert_eq!(rep.summary.mode, IsolationMode::SingleProcess);
    assert_eq!(rep.raw_runs.len(), 3);
    // Seeds must match the derive schedule exactly.
    for (i, r) in rep.raw_runs.iter().enumerate() {
        assert_eq!(r.seed, derive_seed(cfg.base_seed, i as u32));
        assert_eq!(r.run_id, i as u32);
    }
}

// 7 -------------------------------------------------------------------------

#[test]
fn multi_run_clustered_shows_nonzero_locality_signal() {
    let dir = tempdir().unwrap();
    let mut cfg = cfg_for("Clustered", 3, 0xC10C_C0DE);
    cfg.data_dir_root = dir.path().to_path_buf();
    // Clustered workload + clusters misaligned with initial rotation.
    cfg.harness_template.initial_segment_rotation = 20;
    cfg.harness_template.spec.pattern = WorkloadPattern::Clustered;

    let rep = run_multi_single_process(&cfg).unwrap();
    // Either at least one run produced a positive gain, OR the median physical
    // metric signals the adapter actually reorganised something.
    let has_positive = rep.summary.positive_gain_runs > 0;
    let range_ops_moved =
        (rep.summary.median_range_ops_adapted - rep.summary.median_range_ops_baseline).abs() > 1e-9;
    let rec_per_range_moved = (rep.summary.median_rec_per_range_adapted
        - rep.summary.median_rec_per_range_baseline)
        .abs()
        > 1e-9;
    assert!(
        has_positive || range_ops_moved || rec_per_range_moved,
        "clustered multi-run must surface SOME locality signal: summary={:?}",
        rep.summary
    );
    // Min/Max sanity: all finite, min ≤ max.
    assert!(rep.summary.min_p95_gain.is_finite());
    assert!(rep.summary.max_p95_gain.is_finite());
    assert!(rep.summary.min_p95_gain <= rep.summary.max_p95_gain);
}

// 8 -------------------------------------------------------------------------

#[test]
fn multi_run_random_does_not_panic_or_explode() {
    let dir = tempdir().unwrap();
    let mut cfg = cfg_for("Random", 3, 0x00BA_D00D);
    cfg.data_dir_root = dir.path().to_path_buf();
    cfg.harness_template.spec.pattern = WorkloadPattern::Random;
    let rep = run_multi_single_process(&cfg).unwrap();
    // 3 runs, all must have recorded seeds from the schedule.
    assert_eq!(rep.raw_runs.len(), 3);
    for (i, r) in rep.raw_runs.iter().enumerate() {
        assert_eq!(r.seed, derive_seed(cfg.base_seed, i as u32));
    }
    // The important invariant on a random (hostile) workload: we MUST NOT blow
    // up — latencies stay finite and bounded.
    assert!(rep.summary.median_p95_adapted_ns.is_finite());
    assert!(rep.summary.median_p95_adapted_ns < 1e12);
    assert!(rep.summary.p95_adapted_max >= rep.summary.p95_adapted_min);
    // Regression accounting adds up.
    assert_eq!(
        rep.summary.positive_gain_runs
            + rep.summary.negative_gain_runs
            + rep.summary.zeroish_gain_runs,
        rep.raw_runs.len()
    );
}
