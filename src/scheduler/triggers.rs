//! Scheduler triggers and priority.

use crate::config::SchedulerConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TriggerKind {
    Fragmentation = 0,
    GraphPressure = 1,
    LocalityGain = 2,
}

pub struct TriggerInputs {
    pub edge_count: usize,
    pub max_graph_edges: usize,
    pub segment_count: usize,
    pub index_key_count: usize,
    pub plan_total_gain: f64,
}

fn edge_pressure_ratio(inputs: &TriggerInputs) -> f64 {
    if inputs.max_graph_edges == 0 {
        return 0.0;
    }
    inputs.edge_count as f64 / inputs.max_graph_edges as f64
}

/// Which triggers fire (independent checks).
pub fn triggers_fired(cfg: &SchedulerConfig, inputs: &TriggerInputs) -> Vec<TriggerKind> {
    let mut v = Vec::new();
    let ratio = edge_pressure_ratio(inputs);
    if ratio >= cfg.graph_pressure_edge_ratio_threshold {
        v.push(TriggerKind::GraphPressure);
    }
    if inputs.segment_count >= cfg.fragmentation_segments_threshold {
        v.push(TriggerKind::Fragmentation);
    }
    if inputs.plan_total_gain >= cfg.locality_gain_threshold {
        v.push(TriggerKind::LocalityGain);
    }
    v
}

/// Highest-priority firing trigger (lower enum value wins).
pub fn highest_priority_trigger(fired: &[TriggerKind]) -> Option<TriggerKind> {
    fired.iter().min().copied()
}

pub fn should_schedule_work(cfg: &SchedulerConfig, inputs: &TriggerInputs) -> Option<TriggerKind> {
    let fired = triggers_fired(cfg, inputs);
    highest_priority_trigger(&fired)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_priority_ordering() {
        let cfg = SchedulerConfig::default();
        let inputs = TriggerInputs {
            edge_count: 400_000,
            max_graph_edges: 500_000,
            segment_count: 50,
            index_key_count: 100,
            plan_total_gain: 10.0,
        };
        let fired = triggers_fired(&cfg, &inputs);
        assert!(fired.contains(&TriggerKind::GraphPressure));
        assert!(fired.contains(&TriggerKind::Fragmentation));
        assert!(fired.contains(&TriggerKind::LocalityGain));
        assert_eq!(
            highest_priority_trigger(&fired),
            Some(TriggerKind::Fragmentation)
        );
    }
}
