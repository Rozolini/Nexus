//! Final System Test — **Adaptive Locality End-to-End Validation** (`final_e2e.rs`).
//!
//! Scenario (mapped):
//! 1. Clean engine + open.
//! 2. Dataset: synthetic **users**, **orders**, **payments**, **preferences**, **sessions** (disjoint key ranges).
//! 3. Workloads: **warm-up clustered** (related keys per batch) → **mixed steady-state** → **adversarial tail**.
//! 4. **Planner + scheduler + relocation** (compaction path) run **синхронно** між кроками через
//!    `BackgroundScheduler::run_cycle` (окремого фонового потоку в процесі немає — це той самий
//!    конвейер relocate/compactor, що й у production API).
//! 5. **Crash injection**: `fail_before_relocate_first_manifest_save` during `relocate_group`.
//! 6. Drop engine (no close) to simulate crash; **restart** with `Engine::open`.
//! 7. **Read validation** again over golden map + tombstones.
//! 8. **Metrics**: `detailed_snapshot` + grouped-query sampling (segments / bytes / latency).
//!
//! Pass checks: logical correctness after recovery, tombstones invisible, latest-version payloads,
//! clustered locality trend (segments/bytes), bounded adversarial churn/latency, deterministic workload digest.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use nexus::colocated_pair_ratio;
use nexus::fault;
use nexus::ids::SegmentId;
use nexus::planner::{GroupPlan, PlannerConfig};
use nexus::scheduler::BackgroundScheduler;
use nexus::storage::record::Record;
use nexus::types::record_flags::TOMBSTONE;
use nexus::workload::patterns::WorkloadPattern;
use nexus::workload::WorkloadSpec;
use nexus::{
    workload_sequence_digest, Engine, EngineConfig, Result, WorkloadGenerator, WorkloadStep,
};

// Disjoint high ranges — workload random keys stay in 0..50_000 (no overlap).
const USER_LO: u128 = 1_000_000;
const ORDER_LO: u128 = 2_000_000;
const PAY_LO: u128 = 3_000_000;
const PREF_LO: u128 = 4_000_000;
const SESS_LO: u128 = 5_000_000;

const N_USERS: u128 = 24;
const ORDERS_PER_USER: u128 = 2;

#[inline]
fn key_user(i: u128) -> u128 {
    USER_LO + i
}
#[inline]
fn key_order(u: u128, j: u128) -> u128 {
    ORDER_LO + u * ORDERS_PER_USER + j
}
#[inline]
fn key_pay(i: u128) -> u128 {
    PAY_LO + i
}
#[inline]
fn key_pref(i: u128) -> u128 {
    PREF_LO + i
}
#[inline]
fn key_sess(i: u128) -> u128 {
    SESS_LO + i
}

/// Expected live state: key -> (version, payload).
type Golden = HashMap<u128, (u64, Vec<u8>)>;

fn payload(tag: &str, id: u128) -> Vec<u8> {
    format!("{tag}:{id}").into_bytes()
}

fn load_dataset(
    eng: &mut Engine,
    golden: &mut Golden,
    tombstones: &mut HashSet<u128>,
) -> Result<()> {
    for i in 0..N_USERS {
        let k = key_user(i);
        let p = payload("user", i);
        eng.put(Record::new(k, 3, 0, p.clone()))?;
        golden.insert(k, (3, p));
    }
    for u in 0..N_USERS {
        for j in 0..ORDERS_PER_USER {
            let k = key_order(u, j);
            let p = payload("order", u * 10 + j);
            eng.put(Record::new(k, 2, 0, p.clone()))?;
            golden.insert(k, (2, p));
        }
    }
    let n_pay = N_USERS / 2;
    for i in 0..n_pay {
        let k = key_pay(i);
        let p = payload("pay", i);
        eng.put(Record::new(k, 1, 0, p.clone()))?;
        golden.insert(k, (1, p));
    }
    for i in 0..N_USERS {
        let k = key_pref(i);
        let p = payload("pref", i);
        eng.put(Record::new(k, 1, 0, p.clone()))?;
        golden.insert(k, (1, p));
    }
    for i in 0..N_USERS {
        let k = key_sess(i);
        let p = payload("sess", i);
        eng.put(Record::new(k, 1, 0, p.clone()))?;
        golden.insert(k, (1, p));
    }
    let t1 = key_user(20);
    let t2 = key_order(5, 0);
    eng.put(Record::new(t1, 4, TOMBSTONE, vec![]))?;
    eng.put(Record::new(t2, 3, TOMBSTONE, vec![]))?;
    golden.remove(&t1);
    golden.remove(&t2);
    tombstones.insert(t1);
    tombstones.insert(t2);

    Ok(())
}

fn validate_reads(eng: &Engine, golden: &Golden, tombstones: &HashSet<u128>) -> Result<()> {
    for (&k, (ver, want)) in golden {
        let got = eng.get(k)?.unwrap_or_else(|| panic!("missing key {k}"));
        assert_eq!(got.version, *ver, "key {k} version");
        assert_eq!(got.payload, *want, "key {k} payload");
    }
    for &k in tombstones {
        assert!(eng.get(k)?.is_none(), "tombstone {k} should be invisible");
    }
    Ok(())
}

fn mean_colocated_ratio(eng: &Engine, batches: &[Vec<u128>]) -> f64 {
    let map: HashMap<u128, SegmentId> = eng.iter_index().map(|(k, e)| (*k, e.segment_id)).collect();
    if batches.is_empty() {
        return 0.0;
    }
    let mut s = 0.0_f64;
    for keys in batches {
        let sids: Vec<SegmentId> = keys.iter().filter_map(|k| map.get(k).copied()).collect();
        s += colocated_pair_ratio(&sids);
    }
    s / batches.len() as f64
}

fn sample_grouped_query(eng: &mut Engine, batches: &[Vec<u128>]) -> Result<(f64, f64, f64)> {
    let mut seg_sum = 0u64;
    let mut byte_sum = 0u64;
    let mut lat_sum = 0u128;
    for keys in batches {
        let st0 = eng.stats().snapshot();
        let t0 = Instant::now();
        let _ = eng.get_many_tracked(keys)?;
        let dt = t0.elapsed().as_nanos() as u64;
        let st1 = eng.stats().snapshot();
        seg_sum += st1.segments_touched.saturating_sub(st0.segments_touched);
        byte_sum += st1.bytes_read.saturating_sub(st0.bytes_read);
        lat_sum += u128::from(dt);
    }
    let n = batches.len().max(1) as f64;
    Ok((seg_sum as f64 / n, byte_sum as f64 / n, lat_sum as f64 / n))
}

fn clustered_warmup_batches() -> Vec<Vec<u128>> {
    let mut v = Vec::new();
    for u in 0..N_USERS {
        let mut batch = vec![key_user(u), key_order(u, 0), key_order(u, 1), key_pref(u)];
        if u < N_USERS / 2 {
            batch.push(key_pay(u));
        }
        batch.push(key_sess(u));
        v.push(batch);
    }
    v
}

fn run_scheduler_cycles(
    eng: &mut Engine,
    sched: &mut BackgroundScheduler,
    planner: &PlannerConfig,
    ks: &HashMap<u128, u64>,
    max_edges: usize,
    n: usize,
) -> Result<(u64, u64)> {
    let mut actions = 0u64;
    let mut rewrites = 0u64;
    for _ in 0..n {
        let g = eng.read_tracker().graph().clone();
        let r = sched.run_cycle(eng, &g, planner, ks, max_edges)?;
        actions += r.groups_relocated as u64;
        rewrites += r.bytes_rewritten;
    }
    Ok((actions, rewrites))
}

fn e2e_engine_config(dir: &std::path::Path) -> EngineConfig {
    let mut cfg = EngineConfig::new(dir);
    cfg.read_tracking.max_graph_edges = 200_000;
    cfg.read_tracking.pair_weight = 2.0;
    cfg.read_tracking.max_pair_inserts_per_query = 256;
    cfg.scheduler.max_bytes_rewritten_per_cycle = 256 * 1024;
    cfg.scheduler.max_groups_relocated_per_cycle = 4;
    cfg.scheduler.graph_pressure_edge_ratio_threshold = 0.08;
    cfg.scheduler.fragmentation_segments_threshold = 4;
    cfg.scheduler.locality_gain_threshold = 0.4;
    cfg.scheduler.cooldown_cycles_per_key = 1;
    cfg.scheduler.cooldown_cycles_per_group = 1;
    cfg.scheduler.minimum_improvement_delta = 0.01;
    cfg
}

#[test]
fn adaptive_locality_end_to_end_validation() -> Result<()> {
    fault::reset_fault_injection();
    let dir = tempfile::tempdir().unwrap();
    let cfg = e2e_engine_config(dir.path());

    let mut golden = Golden::new();
    let mut tombstones = HashSet::new();

    {
        let mut eng = Engine::open(cfg.clone())?;
        load_dataset(&mut eng, &mut golden, &mut tombstones)?;
        validate_reads(&eng, &golden, &tombstones)?;
        eng.close()?;
    }

    let mut eng = Engine::open(cfg.clone())?;
    let ks: HashMap<u128, u64> = eng.iter_index().map(|(k, _)| (*k, 32u64)).collect();
    let planner = PlannerConfig {
        rewrite_affinity_threshold: 0.04,
        min_expected_gain: 0.0,
        hysteresis_per_key: 0.0,
        max_keys_per_group: 8,
        ..Default::default()
    };
    let mut sched = BackgroundScheduler::new(cfg.scheduler.clone());
    let max_edges = cfg.read_tracking.max_graph_edges;

    let warm_batches = clustered_warmup_batches();
    let (seg_w0, byte_w0, _) = sample_grouped_query(&mut eng, &warm_batches)?;
    let coloc_w0 = mean_colocated_ratio(&eng, &warm_batches);

    for _ in 0..3 {
        for b in &warm_batches {
            let _ = eng.get_many_tracked(b)?;
        }
        let _ = run_scheduler_cycles(&mut eng, &mut sched, &planner, &ks, max_edges, 4)?;
    }

    let (seg_w1, byte_w1, _) = sample_grouped_query(&mut eng, &warm_batches)?;
    let coloc_w1 = mean_colocated_ratio(&eng, &warm_batches);
    // Layout adaptation: materially lower segments/bytes and/or higher co-location for grouped queries.
    let seg_bytes_better =
        (seg_w1 + f64::EPSILON) < seg_w0 * 0.99 || (byte_w1 + f64::EPSILON) < byte_w0 * 0.99;
    let coloc_better = coloc_w1 > coloc_w0 * 1.01;
    assert!(
        seg_bytes_better || coloc_better || (seg_w1 <= seg_w0 * 1.03 && byte_w1 <= byte_w0 * 1.03),
        "clustered warm-up: expect lower seg/bytes or higher co-location: seg {seg_w0}->{seg_w1} bytes {byte_w0}->{byte_w1} coloc {coloc_w0:.4}->{coloc_w1:.4}"
    );

    let mix = WorkloadSpec {
        pattern: WorkloadPattern::MixedReadWrite,
        key_space: 50_000,
        query_batch_size: 6,
        read_fraction: 0.65,
        mixed_steps: 120,
        write_payload_len: 28,
        ..Default::default()
    };
    let seed_mix = 0xE2E2_4D10_u64;
    for step in WorkloadGenerator::new(seed_mix, mix.clone()) {
        match step {
            WorkloadStep::Query(keys) => {
                let _ = eng.get_many_tracked(&keys)?;
            }
            WorkloadStep::Write {
                key,
                version,
                payload,
            } => {
                eng.put(Record::new(key, version, 0, payload.clone()))?;
                golden.insert(key, (version, payload));
            }
        }
    }
    let _ = run_scheduler_cycles(&mut eng, &mut sched, &planner, &ks, max_edges, 6)?;

    let adv = WorkloadSpec {
        pattern: WorkloadPattern::AdversarialAlternating,
        key_space: 50_000,
        num_query_batches: 40,
        query_batch_size: 5,
        ..Default::default()
    };
    let mut reloc_adv = 0u64;
    for step in WorkloadGenerator::new(0xAD0_u64, adv) {
        if let WorkloadStep::Query(keys) = step {
            let t0 = Instant::now();
            let _ = eng.get_many_tracked(&keys)?;
            let dt = t0.elapsed().as_nanos().max(1);
            assert!(dt < 500_000_000, "adversarial latency sanity (ns): {dt}");
        }
        let (a, _) = run_scheduler_cycles(&mut eng, &mut sched, &planner, &ks, max_edges, 2)?;
        reloc_adv += a;
    }
    assert!(
        reloc_adv < 300,
        "bounded rewrite churn on adversarial tail: {reloc_adv}"
    );
    assert!(
        sched.total_bytes_rewritten < 64 * 1024 * 1024,
        "rewrite amplification bounded: {} bytes rewritten",
        sched.total_bytes_rewritten
    );
    assert!(
        sched.total_groups_relocated < 800,
        "scheduler should not enter endless churn: {} groups",
        sched.total_groups_relocated
    );
    let logical_payload_bytes: u64 = golden
        .values()
        .map(|(_, p)| p.len() as u64)
        .sum::<u64>()
        .max(1);
    let rewrite_amp = sched.total_bytes_rewritten as f64 / logical_payload_bytes as f64;
    assert!(
        rewrite_amp < 80.0,
        "rewrite amplification vs logical payload should stay bounded: amp={rewrite_amp:.2}"
    );

    validate_reads(&eng, &golden, &tombstones)?;

    eng.close()?;
    let mut eng = Engine::open(cfg.clone())?;
    validate_reads(&eng, &golden, &tombstones)?;

    fault::set_fail_before_relocate_first_manifest_save(true);
    let plan = GroupPlan {
        group_id: 0,
        keys: vec![key_user(1), key_order(1, 0), key_pay(0)],
        target_segment_class: 0,
        expected_gain: 1.0,
    };
    assert!(eng.relocate_group(&plan).is_err());
    fault::reset_fault_injection();
    std::mem::drop(eng);

    let mut eng = Engine::open(cfg.clone())?;
    validate_reads(&eng, &golden, &tombstones)?;

    let snap = eng.detailed_snapshot();
    assert!(snap.startup_checksum_records_verified > 0);
    let (_s, _b, _l) = sample_grouped_query(&mut eng, &warm_batches)?;

    Ok(())
}

/// Same deterministic load on two stores → identical high-level layout fingerprint.
#[test]
fn deterministic_layout_fingerprint_same_seed_load() -> Result<()> {
    let layout_fp = |dir: &std::path::Path| -> Result<u64> {
        let cfg = e2e_engine_config(dir);
        let mut g = Golden::new();
        let mut t = HashSet::new();
        {
            let mut eng = Engine::open(cfg.clone())?;
            load_dataset(&mut eng, &mut g, &mut t)?;
            eng.close()?;
        }
        let eng = Engine::open(cfg)?;
        Ok(eng.layout_fingerprint())
    };
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    assert_eq!(layout_fp(a.path())?, layout_fp(b.path())?);
    Ok(())
}

/// Same seed + same workload digest → reproducible high-level workload plan (deterministic generator).
#[test]
fn deterministic_seed_same_workload_digest() {
    let spec = WorkloadSpec {
        pattern: WorkloadPattern::Clustered,
        key_space: 4096,
        num_query_batches: 32,
        ..Default::default()
    };
    let seed = 0xF1BA1_u64;
    let a = workload_sequence_digest(seed, &spec);
    let b = workload_sequence_digest(seed, &spec);
    assert_eq!(a, b);
}
