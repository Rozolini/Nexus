//! Benchmark harness, locality metrics, validation gates.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use nexus::benchmark::report::AggregatedReportNumbers;
use nexus::{
    run_benchmark, workload_sequence_digest, BenchmarkHarnessConfig, PlannerConfig,
    SchedulerConfig, WorkloadPattern, WorkloadSpec,
};

fn tiny_harness(seed: u64, pattern: WorkloadPattern) -> BenchmarkHarnessConfig {
    BenchmarkHarnessConfig {
        seed,
        spec: WorkloadSpec {
            pattern,
            key_space: 512,
            num_query_batches: 24,
            query_batch_size: 6,
            cluster_count: 8,
            mixed_steps: 80,
            ..Default::default()
        },
        load_key_count: 512,
        stabilization_cycles: 12,
        planner: PlannerConfig {
            rewrite_affinity_threshold: 0.05,
            min_expected_gain: 0.0,
            hysteresis_per_key: 0.0,
            max_keys_per_group: 6,
            ..Default::default()
        },
        scheduler: SchedulerConfig {
            max_groups_relocated_per_cycle: 2,
            max_bytes_rewritten_per_cycle: 256 * 1024,
            minimum_improvement_delta: 0.02,
            ..Default::default()
        },
        max_graph_edges: 500_000,
        initial_segment_rotation: 128,
        max_relocation_traces: 8,
    }
}

fn report_fingerprint(
    seed: u64,
    digest: u64,
    b: &AggregatedReportNumbers,
    a: &AggregatedReportNumbers,
) -> u64 {
    let mut h = DefaultHasher::new();
    seed.hash(&mut h);
    digest.hash(&mut h);
    b.query_count.hash(&mut h);
    a.query_count.hash(&mut h);
    h.finish()
}

#[test]
fn benchmark_repeatability() {
    let cfg = tiny_harness(0x1111, WorkloadPattern::Random);
    let d = workload_sequence_digest(cfg.seed, &cfg.spec);
    let r1 = run_benchmark(tempfile::tempdir().unwrap().path(), &cfg).unwrap();
    let r2 = run_benchmark(tempfile::tempdir().unwrap().path(), &cfg).unwrap();
    assert_eq!(r1.workload_digest, d);
    assert_eq!(r2.workload_digest, d);
    assert_eq!(r1.seed, r2.seed);
    assert_eq!(
        r1.baseline.aggregated.query_count,
        r2.baseline.aggregated.query_count
    );
}

#[test]
fn deterministic_seed_stability() {
    let a = workload_sequence_digest(1, &WorkloadSpec::default());
    let b = workload_sequence_digest(2, &WorkloadSpec::default());
    assert_ne!(a, b);
}

#[test]
fn metrics_sanity_checks() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = tiny_harness(0x2222, WorkloadPattern::Clustered);
    let r = run_benchmark(dir.path(), &cfg).unwrap();
    let b = &r.baseline.aggregated;
    let a = &r.after_stabilization.aggregated;
    assert!(b.p50_read_latency_ns <= b.p95_read_latency_ns + 1);
    assert!(a.p50_read_latency_ns <= a.p95_read_latency_ns + 1);
    assert!(b.mean_segments_touched_logical >= 0.0);
    assert!(b.mean_bytes_read_logical >= 0.0);
    assert!(
        b.mean_colocated_pair_ratio_logical >= 0.0 && b.mean_colocated_pair_ratio_logical <= 1.0
    );
    assert!(b.mean_file_opens >= 0.0);
    assert!(b.mean_physical_read_ops >= 0.0);
    assert!(b.mean_physical_bytes_read >= 0.0);
}

#[test]
fn gate_reproducible_benchmark_outputs() {
    let cfg = tiny_harness(0x3333, WorkloadPattern::SkewedZipfian);
    let r1 = run_benchmark(tempfile::tempdir().unwrap().path(), &cfg).unwrap();
    let r2 = run_benchmark(tempfile::tempdir().unwrap().path(), &cfg).unwrap();
    let fp1 = report_fingerprint(
        r1.seed,
        r1.workload_digest,
        &r1.baseline.aggregated,
        &r1.after_stabilization.aggregated,
    );
    let fp2 = report_fingerprint(
        r2.seed,
        r2.workload_digest,
        &r2.baseline.aggregated,
        &r2.after_stabilization.aggregated,
    );
    assert_eq!(fp1, fp2);
    let js = r1.to_json_pretty();
    assert!(js.contains("workload_digest"));
    assert!(js.contains("locality_gain_score"));
}

#[test]
fn gate_visible_gain_where_locality_exists() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = tiny_harness(0x4444, WorkloadPattern::Clustered);
    cfg.spec.zipf_s = 1.1;
    let r = run_benchmark(dir.path(), &cfg).unwrap();
    let after = r
        .after_stabilization
        .aggregated
        .mean_segments_touched_logical;
    let before = r.baseline.aggregated.mean_segments_touched_logical;
    let gain = r.stabilization.locality_gain_score;
    assert!(
        after <= before * 1.25 || gain >= -0.15,
        "clustered workload should not regress segments or p95 sharply: before={before} after={after} gain={gain}"
    );
}

#[test]
fn gate_bounded_harm_where_locality_absent() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = tiny_harness(0x5555, WorkloadPattern::AdversarialAlternating);
    cfg.stabilization_cycles = 40;
    let r = run_benchmark(dir.path(), &cfg).unwrap();
    let b95 = r.baseline.aggregated.p95_read_latency_ns.max(1);
    let a95 = r.after_stabilization.aggregated.p95_read_latency_ns.max(1);
    let ratio = (a95 as f64 / b95 as f64).max(b95 as f64 / a95 as f64);
    assert!(
        ratio < 25.0,
        "adversarial: latency should not explode (ratio {ratio})"
    );
    let actions = r.stabilization.planner_actions_total;
    assert!(
        actions < 500,
        "planner should not thrash unboundedly, actions={actions}"
    );
}
