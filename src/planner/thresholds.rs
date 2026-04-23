//! Planner thresholds.

#[derive(Debug, Clone, PartialEq)]
pub struct PlannerConfig {
    pub max_keys_per_group: usize,
    pub max_bytes_per_group: u64,
    pub rewrite_affinity_threshold: f64,
    pub min_expected_gain: f64,
    pub hysteresis_per_key: f64,
    pub num_segment_classes: u32,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            max_keys_per_group: 64,
            max_bytes_per_group: 256 * 1024,
            rewrite_affinity_threshold: 0.15,
            min_expected_gain: 0.5,
            hysteresis_per_key: 0.02,
            num_segment_classes: 4,
        }
    }
}
