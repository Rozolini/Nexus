//! # Configuration
//!
//! All runtime knobs for the engine and its subsystems. Every tunable lives
//! here so that production embeddings can be reviewed in one place.
//!
//! Types in this module follow a few conventions:
//!
//! * `Default` always yields a **safe, conservative** configuration — no
//!   speculative optimisations, no aggressive rewrite budgets.
//! * Every bound that can affect correctness (e.g. `max_graph_edges`)
//!   is a **hard cap**, not a soft hint. Exceeding it causes pruning, not
//!   unbounded growth.
//! * Configurations are `Clone` + `PartialEq` so tests can assert that the
//!   engine was opened with the expected policy.

use std::path::PathBuf;

/// Bounded policy for read tracking / co-access.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadTrackingConfig {
    pub enabled: bool,
    /// Max unique keys considered from one query (after dedupe + sort).
    pub max_keys_per_session: usize,
    /// Max undirected pair updates per single `on_query_keys` call.
    pub max_pair_inserts_per_query: usize,
    /// Hard cap on distinct edges in memory (prunes weakest when exceeded).
    pub max_graph_edges: usize,
    pub pair_weight: f64,
}

impl Default for ReadTrackingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_keys_per_session: 256,
            max_pair_inserts_per_query: 4096,
            max_graph_edges: 500_000,
            pair_weight: 1.0,
        }
    }
}

/// Background scheduler and rewrite budgets.
#[derive(Debug, Clone, PartialEq)]
pub struct SchedulerConfig {
    pub max_bytes_rewritten_per_cycle: u64,
    pub max_groups_relocated_per_cycle: usize,
    pub max_background_cpu_share: f64,
    pub graph_pressure_edge_ratio_threshold: f64,
    pub fragmentation_segments_threshold: usize,
    pub locality_gain_threshold: f64,
    pub cooldown_cycles_per_key: u64,
    pub cooldown_cycles_per_group: u64,
    pub minimum_improvement_delta: f64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_bytes_rewritten_per_cycle: 4 * 1024 * 1024,
            max_groups_relocated_per_cycle: 8,
            max_background_cpu_share: 0.25,
            graph_pressure_edge_ratio_threshold: 0.35,
            fragmentation_segments_threshold: 12,
            locality_gain_threshold: 2.0,
            cooldown_cycles_per_key: 4,
            cooldown_cycles_per_group: 3,
            minimum_improvement_delta: 0.05,
        }
    }
}

/// Policy for merging multiple in-segment record reads into contiguous range reads.
///
/// The batch read path groups keys by `segment_id`, sorts by `offset`, and forms `ReadRange`s
/// by merging consecutive records whenever:
///   1. `gap = next.offset - prev.end <= max_read_gap_bytes`, AND
///   2. resulting `range_len = new_end - range_start <= max_range_bytes`.
///
/// One range ⇒ one `seek + read_exact`. Records are then parsed from the in-memory buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadMergePolicy {
    pub max_read_gap_bytes: u64,
    pub max_range_bytes: u64,
}

impl Default for ReadMergePolicy {
    fn default() -> Self {
        Self {
            max_read_gap_bytes: 4 * 1024,
            max_range_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub read_tracking: ReadTrackingConfig,
    pub scheduler: SchedulerConfig,
    pub read_merge: ReadMergePolicy,
}

impl EngineConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            read_tracking: ReadTrackingConfig::default(),
            scheduler: SchedulerConfig::default(),
            read_merge: ReadMergePolicy::default(),
        }
    }
}
