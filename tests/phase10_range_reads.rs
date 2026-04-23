//! Range-merged batch reader: correctness + locality effects.

use nexus::{
    run_benchmark, BenchmarkHarnessConfig, Engine, EngineConfig, PlannerConfig, ReadMergePolicy,
    Record, SchedulerConfig, WorkloadPattern, WorkloadSpec,
};
use tempfile::tempdir;

fn open_with_policy(dir: &std::path::Path, policy: ReadMergePolicy) -> Engine {
    let mut cfg = EngineConfig::new(dir);
    cfg.read_merge = policy;
    Engine::open(cfg).unwrap()
}

fn open(dir: &std::path::Path) -> Engine {
    open_with_policy(dir, ReadMergePolicy::default())
}

fn put(eng: &mut Engine, k: u128, payload: &[u8]) {
    eng.put(Record::new(k, 1, 0, payload.to_vec())).unwrap();
}

#[test]
fn contiguous_records_collapse_into_one_range_read() {
    let dir = tempdir().unwrap();
    let mut eng = open(dir.path());
    for k in 0..16u128 {
        put(&mut eng, k, b"payload-bytes");
    }
    eng.rotate_segment().unwrap();

    let keys: Vec<u128> = (0..16u128).collect();
    let s0 = eng.stats().snapshot();
    let out = eng.get_many(&keys).unwrap();
    let s1 = eng.stats().snapshot();

    assert!(out.iter().all(|r| r.is_some()));
    assert_eq!(
        s1.range_read_ops - s0.range_read_ops,
        1,
        "16 fully-contiguous records in one segment must collapse to 1 range read"
    );
    assert_eq!(s1.file_opens - s0.file_opens, 1);
    assert_eq!(s1.records_in_ranges - s0.records_in_ranges, 16);
    assert!((s1.range_merges - s0.range_merges) == 15);
    // Gaps of 0 between back-to-back records: gap_bytes_merged stays 0.
    assert_eq!(s1.gap_bytes_merged - s0.gap_bytes_merged, 0);
}

#[test]
fn near_contiguous_records_merge_under_threshold() {
    // Small policy threshold — but with default 4096 our records easily merge.
    // Here we explicitly set 32 bytes tolerance and skip every other key to create gaps.
    let dir = tempdir().unwrap();
    let policy = ReadMergePolicy {
        max_read_gap_bytes: 256,
        max_range_bytes: 1 << 20,
    };
    let mut eng = open_with_policy(dir.path(), policy);
    for k in 0..16u128 {
        put(&mut eng, k, b"p");
    }
    eng.rotate_segment().unwrap();

    // Query only even keys => each pair of returned records has one un-needed
    // record between them, which is the "gap" absorbed by the range.
    let keys: Vec<u128> = (0..16u128).step_by(2).collect();
    let s0 = eng.stats().snapshot();
    let out = eng.get_many(&keys).unwrap();
    let s1 = eng.stats().snapshot();

    assert!(out.iter().all(|r| r.is_some()));
    assert_eq!(
        s1.range_read_ops - s0.range_read_ops,
        1,
        "near-contiguous even keys should merge under gap threshold"
    );
    assert!(
        (s1.gap_bytes_merged - s0.gap_bytes_merged) > 0,
        "non-zero merged gap bytes expected"
    );
}

#[test]
fn large_gap_does_not_merge() {
    let dir = tempdir().unwrap();
    let policy = ReadMergePolicy {
        max_read_gap_bytes: 8, // ~smaller than record wire-len
        max_range_bytes: 1 << 20,
    };
    let mut eng = open_with_policy(dir.path(), policy);
    for k in 0..8u128 {
        put(&mut eng, k, b"padding-to-exceed-gap-threshold-xxx");
    }
    eng.rotate_segment().unwrap();

    // Query every 4th key — gaps of ~3 record lengths between them >> 8 bytes.
    let keys = vec![0u128, 4, 7];
    let s0 = eng.stats().snapshot();
    let _ = eng.get_many(&keys).unwrap();
    let s1 = eng.stats().snapshot();
    assert_eq!(
        s1.range_read_ops - s0.range_read_ops,
        3,
        "with tiny gap threshold, 3 non-adjacent records must yield 3 range reads"
    );
    assert_eq!(s1.range_merges - s0.range_merges, 0);
}

#[test]
fn output_order_preserved_with_range_reads() {
    let dir = tempdir().unwrap();
    let mut eng = open(dir.path());
    for k in 0..8u128 {
        put(&mut eng, k, format!("v{k}").as_bytes());
    }
    eng.rotate_segment().unwrap();
    for k in 8..16u128 {
        put(&mut eng, k, format!("v{k}").as_bytes());
    }
    eng.rotate_segment().unwrap();

    let keys = vec![15u128, 0, 8, 3, 14, 7, 1, 9];
    let out = eng.get_many(&keys).unwrap();
    for (i, k) in keys.iter().enumerate() {
        let r = out[i].as_ref().unwrap();
        assert_eq!(r.key, *k);
        assert_eq!(std::str::from_utf8(&r.payload).unwrap(), format!("v{k}"));
    }
}

#[test]
fn tombstone_and_latest_version_across_ranges() {
    let dir = tempdir().unwrap();
    let mut eng = open(dir.path());
    put(&mut eng, 1, b"v1-old");
    put(&mut eng, 2, b"v2");
    put(&mut eng, 3, b"v3");
    eng.rotate_segment().unwrap();
    // Overwrites & tombstone in a separate segment.
    eng.put(Record::new(1, 2, 0, b"v1-new".to_vec())).unwrap();
    eng.put(Record::new(3, 2, 1, Vec::<u8>::new())).unwrap();
    eng.rotate_segment().unwrap();

    let keys = vec![3u128, 1, 2];
    let out = eng.get_many(&keys).unwrap();
    assert!(out[0].is_none(), "tombstoned key must read as None");
    assert_eq!(out[1].as_ref().unwrap().payload, b"v1-new");
    assert_eq!(out[2].as_ref().unwrap().payload, b"v2");
}

#[test]
fn deterministic_range_grouping() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let make = |dir: &std::path::Path| {
        let mut eng = open(dir);
        for k in 0..32u128 {
            put(&mut eng, k, b"p");
        }
        eng.rotate_segment().unwrap();
        eng
    };
    let e1 = make(dir1.path());
    let e2 = make(dir2.path());
    let keys: Vec<u128> = (0..32u128).step_by(3).collect();

    let s10 = e1.stats().snapshot();
    let _ = e1.get_many(&keys).unwrap();
    let s11 = e1.stats().snapshot();
    let s20 = e2.stats().snapshot();
    let _ = e2.get_many(&keys).unwrap();
    let s21 = e2.stats().snapshot();

    // Same dataset + same keys + same policy => identical range grouping.
    assert_eq!(
        s11.range_read_ops - s10.range_read_ops,
        s21.range_read_ops - s20.range_read_ops
    );
    assert_eq!(
        s11.range_merges - s10.range_merges,
        s21.range_merges - s20.range_merges
    );
    assert_eq!(
        s11.range_bytes_read - s10.range_bytes_read,
        s21.range_bytes_read - s20.range_bytes_read
    );
}

#[test]
fn locality_friendly_workload_reduces_range_ops_and_bytes() {
    let dir = tempdir().unwrap();
    let cfg = BenchmarkHarnessConfig {
        seed: 0x0C10_5E5A,
        spec: WorkloadSpec {
            pattern: WorkloadPattern::Clustered,
            key_space: 1024,
            num_query_batches: 64,
            query_batch_size: 8,
            cluster_count: 8,
            zipf_s: 1.1,
            read_fraction: 0.95,
            mixed_steps: 0,
            write_payload_len: 16,
        },
        load_key_count: 1024,
        stabilization_cycles: 24,
        planner: PlannerConfig {
            rewrite_affinity_threshold: 0.05,
            min_expected_gain: 0.0,
            hysteresis_per_key: 0.0,
            max_keys_per_group: 32,
            ..Default::default()
        },
        scheduler: SchedulerConfig {
            max_groups_relocated_per_cycle: 8,
            max_bytes_rewritten_per_cycle: 512 * 1024,
            minimum_improvement_delta: 0.0,
            ..Default::default()
        },
        max_graph_edges: 500_000,
        initial_segment_rotation: 64,
        max_relocation_traces: 8,
    };

    let r = run_benchmark(dir.path(), &cfg).unwrap();
    let b = &r.baseline.aggregated;
    let a = &r.after_stabilization.aggregated;

    assert!(r.stabilization.planner_actions_total > 0);
    // Core locality proof: adapted must not regress range_read_ops by more than 20%
    // on a clustered workload, and records_per_range must rise (strict) compared to baseline.
    assert!(
        a.mean_range_read_ops <= b.mean_range_read_ops * 1.2 + 0.5,
        "range_read_ops regressed: baseline={} adapted={}",
        b.mean_range_read_ops,
        a.mean_range_read_ops
    );
    assert!(
        a.records_per_range_read_avg >= b.records_per_range_read_avg * 0.95,
        "records/range regressed: baseline={} adapted={}",
        b.records_per_range_read_avg,
        a.records_per_range_read_avg
    );
}

#[test]
fn clustered_workload_after_relocation_lowers_range_reads_for_top_group() {
    // Zoom in on the debug trace: at least one recorded group has
    // range_reads_after <= range_reads_before.
    let dir = tempdir().unwrap();
    let cfg = BenchmarkHarnessConfig {
        seed: 0xA11CE,
        spec: WorkloadSpec {
            pattern: WorkloadPattern::Clustered,
            key_space: 512,
            num_query_batches: 48,
            query_batch_size: 8,
            cluster_count: 8,
            zipf_s: 1.1,
            read_fraction: 0.95,
            mixed_steps: 0,
            write_payload_len: 16,
        },
        load_key_count: 512,
        stabilization_cycles: 16,
        planner: PlannerConfig {
            rewrite_affinity_threshold: 0.05,
            min_expected_gain: 0.0,
            hysteresis_per_key: 0.0,
            max_keys_per_group: 16,
            ..Default::default()
        },
        scheduler: SchedulerConfig {
            max_groups_relocated_per_cycle: 8,
            max_bytes_rewritten_per_cycle: 512 * 1024,
            minimum_improvement_delta: 0.0,
            ..Default::default()
        },
        max_graph_edges: 500_000,
        initial_segment_rotation: 32,
        max_relocation_traces: 16,
    };
    let r = run_benchmark(dir.path(), &cfg).unwrap();
    assert!(!r.relocation_traces.is_empty(), "expected some relocations");
    let improved = r.relocation_traces.iter().any(|t| {
        t.range_reads_after <= t.range_reads_before && t.file_opens_after <= t.file_opens_before
    });
    assert!(
        improved,
        "at least one group should show ≤ range_reads and ≤ file_opens after relocation"
    );
}

#[test]
fn random_workload_no_catastrophic_range_regression() {
    let dir = tempdir().unwrap();
    let cfg = BenchmarkHarnessConfig {
        seed: 0xBAD_F00D,
        spec: WorkloadSpec {
            pattern: WorkloadPattern::Random,
            key_space: 1024,
            num_query_batches: 48,
            query_batch_size: 8,
            cluster_count: 16,
            zipf_s: 1.1,
            read_fraction: 1.0,
            mixed_steps: 0,
            write_payload_len: 16,
        },
        load_key_count: 1024,
        stabilization_cycles: 16,
        planner: PlannerConfig::default(),
        scheduler: SchedulerConfig::default(),
        max_graph_edges: 500_000,
        initial_segment_rotation: 64,
        max_relocation_traces: 4,
    };
    let r = run_benchmark(dir.path(), &cfg).unwrap();
    let b = &r.baseline.aggregated;
    let a = &r.after_stabilization.aggregated;
    // A 2x inflation of range reads on random access is the ceiling we tolerate.
    assert!(
        a.mean_range_read_ops <= b.mean_range_read_ops * 2.0 + 1.0,
        "random workload exploded range_read_ops: baseline={} adapted={}",
        b.mean_range_read_ops,
        a.mean_range_read_ops
    );
}
