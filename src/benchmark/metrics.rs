//! Latency and locality metrics aggregation.

use std::time::Duration;

/// One measured query (batch read).
#[derive(Debug, Clone)]
pub struct QuerySample {
    pub latency: Duration,
    /// Distinct segments touched in this batch (logical index view).
    pub segments_touched: u64,
    /// Logical bytes of records returned (header+payload+crc). **Not physical I/O.**
    pub bytes_read: u64,
    /// Pairs of distinct keys in the batch that resolve to the same segment / `n` choose 2.
    pub colocated_pair_ratio: f64,

    // --- Physical read-path observations ------------------------------------
    /// Distinct `File::open` calls caused by this batch. Equals segment groups in batch.
    pub file_opens: u64,
    /// Number of `seek+read_exact` rounds performed by this batch.
    pub physical_read_ops: u64,
    /// Bytes physically fetched from disk by this batch.
    pub physical_bytes_read: u64,
    /// Segment groups in this batch (always == `file_opens` for segment-aware reader).
    pub segment_groups: u64,
    /// Max(offset) - min(offset) summed across segment groups in this batch.
    pub offsets_span_total: u64,

    // --- Range-merged read metrics ------------------------------------------
    /// Number of `seek + read_exact` ranges this batch issued after merging.
    pub range_read_ops: u64,
    /// Bytes read across range reads (includes merged inter-record gaps).
    pub range_bytes_read: u64,
    /// Records parsed from range reads (equals distinct keys found in index).
    pub records_in_ranges: u64,
    /// Count of records merged into an already-open range (not starting a new one).
    pub range_merges: u64,
    /// Gap bytes absorbed into ranges by merges.
    pub gap_bytes_merged: u64,
}

fn percentile(sorted_ns: &[u64], p: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let idx = ((sorted_ns.len() as f64 - 1.0) * p).round() as usize;
    sorted_ns[idx.min(sorted_ns.len() - 1)]
}

/// Aggregated statistics over many [`QuerySample`]s.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregatedMetrics {
    pub query_count: usize,
    pub p50_read_latency_ns: u64,
    pub p95_read_latency_ns: u64,
    /// Logical — bytes of records returned. Insensitive to layout by construction.
    pub mean_segments_touched: f64,
    pub mean_bytes_read: f64,
    pub mean_colocated_pair_ratio: f64,

    // Physical-path means.
    pub mean_file_opens: f64,
    pub mean_physical_read_ops: f64,
    pub mean_physical_bytes_read: f64,
    pub mean_segment_groups: f64,
    pub mean_offsets_span_total: f64,

    // Range-merged read means + ratios.
    pub mean_range_read_ops: f64,
    pub mean_range_bytes_read: f64,
    /// mean(records_in_ranges / range_read_ops) per batch (0 if batch did no range reads).
    pub mean_records_per_range_read: f64,
    /// mean(range_merges / records_in_ranges) per batch; 0..=1 where 1 ≡ single-range batches.
    pub mean_range_merge_ratio: f64,
    /// mean(gap_bytes_merged / range_merges) per batch; 0 if no merges.
    pub mean_gap_bytes_merged: f64,
}

impl AggregatedMetrics {
    pub fn from_samples(samples: &[QuerySample]) -> Self {
        if samples.is_empty() {
            return Self {
                query_count: 0,
                p50_read_latency_ns: 0,
                p95_read_latency_ns: 0,
                mean_segments_touched: 0.0,
                mean_bytes_read: 0.0,
                mean_colocated_pair_ratio: 0.0,
                mean_file_opens: 0.0,
                mean_physical_read_ops: 0.0,
                mean_physical_bytes_read: 0.0,
                mean_segment_groups: 0.0,
                mean_offsets_span_total: 0.0,
                mean_range_read_ops: 0.0,
                mean_range_bytes_read: 0.0,
                mean_records_per_range_read: 0.0,
                mean_range_merge_ratio: 0.0,
                mean_gap_bytes_merged: 0.0,
            };
        }
        let mut lat: Vec<u64> = samples
            .iter()
            .map(|s| s.latency.as_nanos() as u64)
            .collect();
        lat.sort_unstable();
        let p50 = percentile(&lat, 0.50);
        let p95 = percentile(&lat, 0.95);
        let n = samples.len() as f64;
        let mean = |f: fn(&QuerySample) -> f64| samples.iter().map(f).sum::<f64>() / n;

        Self {
            query_count: samples.len(),
            p50_read_latency_ns: p50,
            p95_read_latency_ns: p95,
            mean_segments_touched: mean(|s| s.segments_touched as f64),
            mean_bytes_read: mean(|s| s.bytes_read as f64),
            mean_colocated_pair_ratio: mean(|s| s.colocated_pair_ratio),
            mean_file_opens: mean(|s| s.file_opens as f64),
            mean_physical_read_ops: mean(|s| s.physical_read_ops as f64),
            mean_physical_bytes_read: mean(|s| s.physical_bytes_read as f64),
            mean_segment_groups: mean(|s| s.segment_groups as f64),
            mean_offsets_span_total: mean(|s| s.offsets_span_total as f64),
            mean_range_read_ops: mean(|s| s.range_read_ops as f64),
            mean_range_bytes_read: mean(|s| s.range_bytes_read as f64),
            mean_records_per_range_read: mean(|s| {
                if s.range_read_ops == 0 {
                    0.0
                } else {
                    s.records_in_ranges as f64 / s.range_read_ops as f64
                }
            }),
            mean_range_merge_ratio: mean(|s| {
                if s.records_in_ranges == 0 {
                    0.0
                } else {
                    s.range_merges as f64 / s.records_in_ranges as f64
                }
            }),
            mean_gap_bytes_merged: mean(|s| {
                if s.range_merges == 0 {
                    0.0
                } else {
                    s.gap_bytes_merged as f64 / s.range_merges as f64
                }
            }),
        }
    }
}

/// Pairs `(i,j)` with `i<j` where both keys hit the same segment.
pub fn colocated_pair_ratio(segment_ids: &[crate::ids::SegmentId]) -> f64 {
    let n = segment_ids.len();
    if n < 2 {
        return 0.0;
    }
    let pairs = (n * (n - 1)) / 2;
    let mut same = 0usize;
    for i in 0..n {
        for j in (i + 1)..n {
            if segment_ids[i] == segment_ids[j] {
                same += 1;
            }
        }
    }
    same as f64 / pairs as f64
}
