//! End-to-end benchmark harness.
//!
//! Methodology:
//! - **Two separate engine instances** (`baseline` and `adapted`) in **two separate data dirs**.
//! - Same dataset (deterministic load), same seed, same workload sequence.
//! - Baseline: planner/scheduler/relocation **never run**.
//! - Adapted: scheduler runs between baseline-queries and post-queries.
//! - Load phase rotates segments every `initial_segment_rotation` writes to avoid a
//!   pathological "everything in one segment" baseline where colocation is trivially 1.0.

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use crate::config::EngineConfig;
use crate::engine::Engine;
use crate::error::Result;
use crate::ids::SegmentId;
use crate::planner::{build_layout_plan, PlannerConfig};
use crate::scheduler::BackgroundScheduler;
use crate::storage::record::Record;
use crate::types::Key;
use crate::workload::generator::{
    workload_sequence_digest, WorkloadGenerator, WorkloadSpec, WorkloadStep,
};

use super::metrics::{colocated_pair_ratio, AggregatedMetrics, QuerySample};
use super::report::{
    sid_to_u64, AggregatedReportNumbers, BenchmarkReport, PhaseReport, RelocationTrace,
    StabilizationReport,
};

/// Tunables for a benchmark run.
#[derive(Debug, Clone)]
pub struct BenchmarkHarnessConfig {
    pub seed: u64,
    pub spec: WorkloadSpec,
    /// Initial sequential writes `0..load_key_count` so all workload keys exist.
    pub load_key_count: u128,
    pub stabilization_cycles: usize,
    pub planner: PlannerConfig,
    pub scheduler: crate::config::SchedulerConfig,
    pub max_graph_edges: usize,
    /// Rotate write segment every N initial load writes (default 512). Set to 0 to disable.
    pub initial_segment_rotation: u128,
    /// Cap on recorded relocation traces in the adapted run (default 16).
    pub max_relocation_traces: usize,
}

impl Default for BenchmarkHarnessConfig {
    fn default() -> Self {
        let planner = PlannerConfig {
            rewrite_affinity_threshold: 0.05,
            min_expected_gain: 0.0,
            hysteresis_per_key: 0.0,
            max_keys_per_group: 8,
            ..Default::default()
        };
        let scheduler = crate::config::SchedulerConfig {
            max_groups_relocated_per_cycle: 4,
            max_bytes_rewritten_per_cycle: 256 * 1024,
            minimum_improvement_delta: 0.02,
            ..Default::default()
        };
        Self {
            seed: 0xC0FFEE,
            spec: WorkloadSpec::default(),
            load_key_count: 10_000,
            stabilization_cycles: 24,
            planner,
            scheduler,
            max_graph_edges: 500_000,
            initial_segment_rotation: 512,
            max_relocation_traces: 16,
        }
    }
}

fn segment_ids_for_keys(engine: &Engine, keys: &[Key]) -> Vec<SegmentId> {
    let map: HashMap<Key, SegmentId> = engine
        .iter_index()
        .map(|(k, e)| (*k, e.segment_id))
        .collect();
    keys.iter().filter_map(|k| map.get(k).copied()).collect()
}

fn run_query_batch(engine: &mut Engine, keys: &[Key]) -> Result<QuerySample> {
    let st0 = engine.stats().snapshot();
    let t0 = Instant::now();
    let _ = engine.get_many_tracked(keys)?;
    let latency = t0.elapsed();
    let st1 = engine.stats().snapshot();
    let seg = st1.segments_touched.saturating_sub(st0.segments_touched);
    let bytes = st1.bytes_read.saturating_sub(st0.bytes_read);
    let file_opens = st1.file_opens.saturating_sub(st0.file_opens);
    let phys_ops = st1.physical_read_ops.saturating_sub(st0.physical_read_ops);
    let phys_bytes = st1
        .physical_bytes_read
        .saturating_sub(st0.physical_bytes_read);
    let seg_groups = st1
        .segment_groups_in_batches
        .saturating_sub(st0.segment_groups_in_batches);
    let span_total = st1.offsets_span_sum.saturating_sub(st0.offsets_span_sum);
    let range_ops = st1.range_read_ops.saturating_sub(st0.range_read_ops);
    let range_bytes = st1.range_bytes_read.saturating_sub(st0.range_bytes_read);
    let rec_in_ranges = st1.records_in_ranges.saturating_sub(st0.records_in_ranges);
    let range_merges = st1.range_merges.saturating_sub(st0.range_merges);
    let gap_bytes = st1.gap_bytes_merged.saturating_sub(st0.gap_bytes_merged);
    let sids = segment_ids_for_keys(engine, keys);
    let coloc = colocated_pair_ratio(&sids);
    Ok(QuerySample {
        latency,
        segments_touched: seg,
        bytes_read: bytes,
        colocated_pair_ratio: coloc,
        file_opens,
        physical_read_ops: phys_ops,
        physical_bytes_read: phys_bytes,
        segment_groups: seg_groups,
        offsets_span_total: span_total,
        range_read_ops: range_ops,
        range_bytes_read: range_bytes,
        records_in_ranges: rec_in_ranges,
        range_merges,
        gap_bytes_merged: gap_bytes,
    })
}

fn materialize_steps(seed: u64, spec: &WorkloadSpec) -> Vec<WorkloadStep> {
    WorkloadGenerator::new(seed, spec.clone()).collect()
}

fn run_measurement_steps(engine: &mut Engine, steps: &[WorkloadStep]) -> Result<Vec<QuerySample>> {
    let mut out = Vec::new();
    for step in steps {
        match step {
            WorkloadStep::Query(keys) => {
                let s = run_query_batch(engine, keys)?;
                out.push(s);
            }
            WorkloadStep::Write {
                key,
                version,
                payload,
            } => {
                engine.put(Record::new(*key, *version, 0, payload.clone()))?;
            }
        }
    }
    Ok(out)
}

fn load_dataset(engine: &mut Engine, cfg: &BenchmarkHarnessConfig) -> Result<u64> {
    let mut user_bytes: u64 = 0;
    let rotate = cfg.initial_segment_rotation;
    for k in 0..cfg.load_key_count {
        let payload = vec![(k & 0xFF) as u8; 24];
        user_bytes += payload.len() as u64;
        engine.put(Record::new(k, 1, 0, payload))?;
        if rotate > 0 && (k + 1) % rotate == 0 && k + 1 < cfg.load_key_count {
            engine.rotate_segment()?;
        }
    }
    Ok(user_bytes)
}

fn open_engine(data_dir: &Path, ecfg_base: &EngineConfig) -> Result<Engine> {
    let mut ecfg = ecfg_base.clone();
    ecfg.data_dir = data_dir.to_path_buf();
    Engine::open(ecfg)
}

fn segment_ids_for_trace(engine: &Engine, keys: &[Key]) -> Vec<SegmentId> {
    let map: HashMap<Key, SegmentId> = engine
        .iter_index()
        .map(|(k, e)| (*k, e.segment_id))
        .collect();
    keys.iter().filter_map(|k| map.get(k).copied()).collect()
}

fn offsets_for_keys(engine: &Engine, keys: &[Key]) -> Vec<u64> {
    let map: HashMap<Key, u64> = engine.iter_index().map(|(k, e)| (*k, e.offset)).collect();
    keys.iter().filter_map(|k| map.get(k).copied()).collect()
}

fn span(offsets: &[u64]) -> u64 {
    if offsets.is_empty() {
        return 0;
    }
    let mn = *offsets.iter().min().unwrap();
    let mx = *offsets.iter().max().unwrap();
    mx.saturating_sub(mn)
}

/// Analytic prediction of what a read of `keys` would do **right now**, based on index +
/// merge policy. No I/O performed — the same algorithm as `Engine::get_many_inner`.
///
/// Returns `(range_reads, range_bytes, file_opens)`.
fn predict_range_reads(
    engine: &Engine,
    keys: &[Key],
    policy: &crate::config::ReadMergePolicy,
) -> (u32, u64, u32) {
    use std::collections::BTreeMap;
    let idx: HashMap<Key, (SegmentId, u64, u32)> = engine
        .iter_index()
        .map(|(k, e)| (*k, (e.segment_id, e.offset, e.size)))
        .collect();
    let mut by_seg: BTreeMap<SegmentId, Vec<(u64, u32)>> = BTreeMap::new();
    for k in keys {
        if let Some(&(sid, off, sz)) = idx.get(k) {
            by_seg.entry(sid).or_default().push((off, sz));
        }
    }
    let mut ranges: u32 = 0;
    let mut bytes: u64 = 0;
    let file_opens = by_seg.len() as u32;
    for (_, mut rows) in by_seg {
        rows.sort_by_key(|&(off, _)| off);
        let mut cur: Option<(u64, u64)> = None; // (start, end)
        for (off, sz) in rows {
            let rec_end = off.saturating_add(sz as u64);
            match cur {
                Some((s, e)) => {
                    let gap = off.saturating_sub(e);
                    let new_end = rec_end.max(e);
                    let new_len = new_end.saturating_sub(s);
                    if gap <= policy.max_read_gap_bytes && new_len <= policy.max_range_bytes {
                        cur = Some((s, new_end));
                    } else {
                        ranges += 1;
                        bytes += e - s;
                        cur = Some((off, rec_end));
                    }
                }
                None => cur = Some((off, rec_end)),
            }
        }
        if let Some((s, e)) = cur {
            ranges += 1;
            bytes += e - s;
        }
    }
    (ranges, bytes, file_opens)
}

/// Run harness using two engine instances: baseline (planner off) and adapted (planner on).
/// `data_dir` is used as a parent for two subfolders `baseline/` and `adapted/`.
pub fn run_benchmark(data_dir: &Path, cfg: &BenchmarkHarnessConfig) -> Result<BenchmarkReport> {
    std::fs::create_dir_all(data_dir).ok();
    let base_dir = data_dir.join("baseline");
    let adap_dir = data_dir.join("adapted");

    let mut ecfg = EngineConfig::new(&base_dir);
    ecfg.scheduler = cfg.scheduler.clone();

    let mut eng_base = open_engine(&base_dir, &ecfg)?;
    let mut eng_adap = open_engine(&adap_dir, &ecfg)?;

    let load_bytes_base = load_dataset(&mut eng_base, cfg)?;
    let _load_bytes_adap = load_dataset(&mut eng_adap, cfg)?;

    let steps = materialize_steps(cfg.seed, &cfg.spec);
    let digest = workload_sequence_digest(cfg.seed, &cfg.spec);

    // --- Baseline phase: run full workload, NO scheduler cycles ---
    let baseline_samples = run_measurement_steps(&mut eng_base, &steps)?;
    let baseline_agg = AggregatedMetrics::from_samples(&baseline_samples);

    // --- Adapted phase: prime graph, stabilize, then measure. ---
    // Prime: run the workload once so the read-tracker graph reflects co-access.
    let _ = run_measurement_steps(&mut eng_adap, &steps)?;

    let mut graph_owned = eng_adap.read_tracker().graph().clone();
    let ks: HashMap<Key, u64> = eng_adap.iter_index().map(|(k, _)| (*k, 32u64)).collect();

    let mut sched = BackgroundScheduler::new(cfg.scheduler.clone());
    let mut planner_actions: u64 = 0;
    let mut rewrite_bytes: u64 = 0;
    let mut traces: Vec<RelocationTrace> = Vec::new();

    let merge_policy = ecfg.read_merge;

    for _ in 0..cfg.stabilization_cycles {
        // Pre-snapshot plan so we can correlate actual relocations with group intents.
        let plan_preview = build_layout_plan(&graph_owned, &ks, &cfg.planner);

        let segs_before: HashMap<u32, Vec<SegmentId>> = plan_preview
            .groups
            .iter()
            .map(|g| (g.group_id, segment_ids_for_trace(&eng_adap, &g.keys)))
            .collect();
        let offs_before: HashMap<u32, Vec<u64>> = plan_preview
            .groups
            .iter()
            .map(|g| (g.group_id, offsets_for_keys(&eng_adap, &g.keys)))
            .collect();
        let pred_before: HashMap<u32, (u32, u64, u32)> = plan_preview
            .groups
            .iter()
            .map(|g| {
                (
                    g.group_id,
                    predict_range_reads(&eng_adap, &g.keys, &merge_policy),
                )
            })
            .collect();

        let r = sched.run_cycle(
            &mut eng_adap,
            &graph_owned,
            &cfg.planner,
            &ks,
            cfg.max_graph_edges,
        )?;
        planner_actions += r.groups_relocated as u64;
        rewrite_bytes += r.bytes_rewritten;

        // Capture up to `max_relocation_traces` traces total, taking groups
        // that still satisfy current layout (`segment_id_after` has single value).
        if traces.len() < cfg.max_relocation_traces {
            for g in &plan_preview.groups {
                if traces.len() >= cfg.max_relocation_traces {
                    break;
                }
                let after = segment_ids_for_trace(&eng_adap, &g.keys);
                if after.is_empty() {
                    continue;
                }
                let offs_after = offsets_for_keys(&eng_adap, &g.keys);
                let before = segs_before.get(&g.group_id).cloned().unwrap_or_default();
                let before_ratio = colocated_pair_ratio(&before);
                let after_ratio = colocated_pair_ratio(&after);
                if before_ratio >= after_ratio && !before.is_empty() {
                    continue; // skip groups that didn't improve colocation this cycle
                }
                let dest = *after.first().unwrap();
                let pb = pred_before.get(&g.group_id).copied().unwrap_or((0, 0, 0));
                let pa = predict_range_reads(&eng_adap, &g.keys, &merge_policy);
                traces.push(RelocationTrace {
                    group_id: g.group_id,
                    keys_count: g.keys.len(),
                    keys: g.keys.clone(),
                    segment_ids_before: before.iter().map(|s| sid_to_u64(*s)).collect(),
                    segment_id_after: sid_to_u64(dest),
                    expected_gain: g.expected_gain,
                    observed_same_segment_ratio_before: before_ratio,
                    observed_same_segment_ratio_after: after_ratio,
                    observed_offsets_span_before: offs_before
                        .get(&g.group_id)
                        .map(|v| span(v))
                        .unwrap_or(0),
                    observed_offsets_span_after: span(&offs_after),
                    bytes_written: 0,
                    range_reads_before: pb.0,
                    range_reads_after: pa.0,
                    range_bytes_before: pb.1,
                    range_bytes_after: pa.1,
                    file_opens_before: pb.2,
                    file_opens_after: pa.2,
                });
            }
        }

        graph_owned = eng_adap.read_tracker().graph().clone();
    }

    let extra_writes: u64 = steps
        .iter()
        .filter_map(|s| match s {
            WorkloadStep::Write { payload, .. } => Some(payload.len() as u64),
            _ => None,
        })
        .sum();
    let user_write_bytes = load_bytes_base.saturating_add(extra_writes);

    let adapted_samples = run_measurement_steps(&mut eng_adap, &steps)?;
    let after_agg = AggregatedMetrics::from_samples(&adapted_samples);

    let amp = if user_write_bytes > 0 {
        rewrite_bytes as f64 / user_write_bytes as f64
    } else {
        0.0
    };
    let rate = if cfg.stabilization_cycles > 0 {
        planner_actions as f64 / cfg.stabilization_cycles as f64
    } else {
        0.0
    };

    // Signed gains (negative = regression). No saturating_sub here.
    let signed_gain = |b: f64, a: f64| if b > 0.0 { (b - a) / b } else { 0.0 };
    let p95_gain = signed_gain(
        baseline_agg.p95_read_latency_ns as f64,
        after_agg.p95_read_latency_ns as f64,
    );
    let ops_gain = signed_gain(
        baseline_agg.mean_physical_read_ops,
        after_agg.mean_physical_read_ops,
    );
    let fo_gain = signed_gain(baseline_agg.mean_file_opens, after_agg.mean_file_opens);
    let range_ops_gain = signed_gain(
        baseline_agg.mean_range_read_ops,
        after_agg.mean_range_read_ops,
    );
    let range_bytes_gain = signed_gain(
        baseline_agg.mean_range_bytes_read,
        after_agg.mean_range_bytes_read,
    );

    Ok(BenchmarkReport {
        seed: cfg.seed,
        workload_digest: digest,
        baseline: PhaseReport {
            aggregated: AggregatedReportNumbers::from(&baseline_agg),
        },
        after_stabilization: PhaseReport {
            aggregated: AggregatedReportNumbers::from(&after_agg),
        },
        stabilization: StabilizationReport {
            cycles_run: cfg.stabilization_cycles,
            planner_actions_total: planner_actions,
            rewrite_bytes_total: rewrite_bytes,
            user_write_bytes_total: user_write_bytes,
            rewrite_amplification: amp,
            planner_action_rate: rate,
            locality_gain_score: p95_gain,
            physical_ops_gain: ops_gain,
            file_opens_gain: fo_gain,
            range_ops_gain,
            range_bytes_gain,
        },
        relocation_traces: traces,
    })
}
