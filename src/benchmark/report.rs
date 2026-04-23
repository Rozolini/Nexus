//! Text / JSON benchmark reports.

use serde::Serialize;

use super::metrics::AggregatedMetrics;
use crate::ids::SegmentId;

/// Serializable full benchmark output.
///
/// The benchmark runs **two separate engine instances** with the same dataset and
/// workload sequence — `baseline` (planner/scheduler off) vs `adapted` (planner on).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct BenchmarkReport {
    pub seed: u64,
    pub workload_digest: u64,
    pub baseline: PhaseReport,
    /// Same as `after_stabilization`, kept for backward compatibility with older consumers.
    pub after_stabilization: PhaseReport,
    pub stabilization: StabilizationReport,
    /// Up to N relocation traces captured during the adapted run (debug aid).
    #[serde(default)]
    pub relocation_traces: Vec<RelocationTrace>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PhaseReport {
    pub aggregated: AggregatedReportNumbers,
}

/// Public JSON-facing copy of [`AggregatedMetrics`]; fields suffixed `_logical` are
/// **not physical I/O indicators** — they are payload-size driven and insensitive to layout.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AggregatedReportNumbers {
    pub query_count: usize,
    pub p50_read_latency_ns: u64,
    pub p95_read_latency_ns: u64,
    /// Logical: distinct segments that appeared in the batch's index lookups.
    pub mean_segments_touched_logical: f64,
    /// Logical: bytes of records returned (header+payload+crc). Does not reflect physical I/O.
    pub mean_bytes_read_logical: f64,
    /// Logical: same-segment pairs / total pairs per batch.
    pub mean_colocated_pair_ratio_logical: f64,

    // --- Physical-path metrics ----------------------------------------------
    /// Physical: `File::open` calls per batch.
    pub mean_file_opens: f64,
    /// Physical: `seek+read_exact` rounds per batch.
    pub mean_physical_read_ops: f64,
    /// Physical: bytes fetched from disk per batch (sum over records read).
    pub mean_physical_bytes_read: f64,
    /// Physical: segment groups (distinct files) touched per batch.
    pub mean_segment_groups_per_batch: f64,
    /// Physical: mean (max_offset − min_offset) per segment group per batch.
    pub mean_offsets_span_per_segment_batch: f64,

    // --- Range-merged metrics -----------------------------------------------
    /// Range reads (after merging) per batch — the primary physical-path metric.
    pub mean_range_read_ops: f64,
    /// Bytes read via range reads per batch (includes absorbed inter-record gaps).
    pub mean_range_bytes_read: f64,
    /// Records parsed per range read.
    pub records_per_range_read_avg: f64,
    /// Fraction of record fetches that were merged into an existing range.
    pub range_merge_ratio: f64,
    /// Mean gap (bytes) absorbed by merges in a batch.
    pub avg_gap_bytes_merged: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct StabilizationReport {
    pub cycles_run: usize,
    pub planner_actions_total: u64,
    pub rewrite_bytes_total: u64,
    pub user_write_bytes_total: u64,
    /// rewrite_bytes_total / user_write_bytes_total. Numerator is **actual** bytes.
    pub rewrite_amplification: f64,
    pub planner_action_rate: f64,
    /// Signed p95 gain: `(p95_baseline − p95_adapted) / p95_baseline`.
    /// Positive = adapted faster; negative = adapted slower.
    pub locality_gain_score: f64,
    /// Signed physical-reads gain:
    /// `(physical_read_ops_baseline − physical_read_ops_adapted) / physical_read_ops_baseline`.
    pub physical_ops_gain: f64,
    /// Signed file-open gain.
    pub file_opens_gain: f64,
    /// Signed range-read-ops gain (primary physical-I/O metric).
    pub range_ops_gain: f64,
    /// Signed range-bytes gain. Negative means adapted reads more bytes (e.g. wider ranges).
    pub range_bytes_gain: f64,
}

/// Debug record of a single relocation performed during the adapted run.
/// Range-read and file-open estimates computed analytically
/// from the engine's current index + merge policy (no extra I/O).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RelocationTrace {
    pub group_id: u32,
    pub keys_count: usize,
    pub keys: Vec<u128>,
    pub segment_ids_before: Vec<u64>,
    pub segment_id_after: u64,
    pub expected_gain: f64,
    pub observed_same_segment_ratio_before: f64,
    pub observed_same_segment_ratio_after: f64,
    pub observed_offsets_span_before: u64,
    pub observed_offsets_span_after: u64,
    pub bytes_written: u64,
    /// Range reads that would be issued for the group's keys (file-opens sum per segment).
    pub range_reads_before: u32,
    pub range_reads_after: u32,
    pub file_opens_before: u32,
    pub file_opens_after: u32,
    pub range_bytes_before: u64,
    pub range_bytes_after: u64,
}

impl From<&AggregatedMetrics> for AggregatedReportNumbers {
    fn from(m: &AggregatedMetrics) -> Self {
        Self {
            query_count: m.query_count,
            p50_read_latency_ns: m.p50_read_latency_ns,
            p95_read_latency_ns: m.p95_read_latency_ns,
            mean_segments_touched_logical: m.mean_segments_touched,
            mean_bytes_read_logical: m.mean_bytes_read,
            mean_colocated_pair_ratio_logical: m.mean_colocated_pair_ratio,
            mean_file_opens: m.mean_file_opens,
            mean_physical_read_ops: m.mean_physical_read_ops,
            mean_physical_bytes_read: m.mean_physical_bytes_read,
            mean_segment_groups_per_batch: m.mean_segment_groups,
            mean_offsets_span_per_segment_batch: m.mean_offsets_span_total,
            mean_range_read_ops: m.mean_range_read_ops,
            mean_range_bytes_read: m.mean_range_bytes_read,
            records_per_range_read_avg: m.mean_records_per_range_read,
            range_merge_ratio: m.mean_range_merge_ratio,
            avg_gap_bytes_merged: m.mean_gap_bytes_merged,
        }
    }
}

impl BenchmarkReport {
    pub fn to_json_pretty(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".into())
    }

    pub fn summary_lines(&self) -> String {
        let b = &self.baseline.aggregated;
        let a = &self.after_stabilization.aggregated;
        format!(
            "seed={}\ndigest=0x{:016x}\n\
baseline : p50={}ns p95={}ns range_ops={:.2} range_B={:.1} rec/range={:.2} file_opens={:.2}\n\
adapted  : p50={}ns p95={}ns range_ops={:.2} range_B={:.1} rec/range={:.2} file_opens={:.2}\n\
stabil   : cycles={} planner_actions={} rewrite_B={} user_B={} amp={:.4} \
p95_gain={:+.4} range_ops_gain={:+.4} range_bytes_gain={:+.4} file_opens_gain={:+.4}\n",
            self.seed,
            self.workload_digest,
            b.p50_read_latency_ns,
            b.p95_read_latency_ns,
            b.mean_range_read_ops,
            b.mean_range_bytes_read,
            b.records_per_range_read_avg,
            b.mean_file_opens,
            a.p50_read_latency_ns,
            a.p95_read_latency_ns,
            a.mean_range_read_ops,
            a.mean_range_bytes_read,
            a.records_per_range_read_avg,
            a.mean_file_opens,
            self.stabilization.cycles_run,
            self.stabilization.planner_actions_total,
            self.stabilization.rewrite_bytes_total,
            self.stabilization.user_write_bytes_total,
            self.stabilization.rewrite_amplification,
            self.stabilization.locality_gain_score,
            self.stabilization.range_ops_gain,
            self.stabilization.range_bytes_gain,
            self.stabilization.file_opens_gain,
        )
    }
}

/// Helper: build `segment_id_after` / `u64` lists for serialization.
pub(crate) fn sid_to_u64(s: SegmentId) -> u64 {
    s.0
}
