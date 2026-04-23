//! Bounded co-access recording for read workloads.

use crate::config::ReadTrackingConfig;
use crate::graph::coaccess::CoAccessGraph;
use crate::tracker::events::{deterministic_pair_downsample, CoReadEvent, CoReadQuery};
use crate::types::Key;

/// Applies bounded pair generation and updates the co-access graph.
#[derive(Debug)]
pub struct ReadTracker {
    config: ReadTrackingConfig,
    graph: CoAccessGraph,
    /// Total pair-weight updates applied (for tests / metrics).
    pub pair_updates_applied: u64,
}

impl ReadTracker {
    pub fn new(config: ReadTrackingConfig) -> Self {
        let max_edges = config.max_graph_edges;
        Self {
            config,
            graph: CoAccessGraph::new(max_edges),
            pair_updates_applied: 0,
        }
    }

    pub fn graph(&self) -> &CoAccessGraph {
        &self.graph
    }

    pub fn graph_mut(&mut self) -> &mut CoAccessGraph {
        &mut self.graph
    }

    /// Plan: co-read **event** emission → graph updates.
    pub fn emit_co_read_event(&mut self, event: CoReadEvent) {
        match event {
            CoReadEvent::Query(q) => self.apply_co_read_query(&q),
        }
    }

    fn apply_co_read_query(&mut self, q: &CoReadQuery) {
        let pairs = deterministic_pair_downsample(&q.keys, self.config.max_pair_inserts_per_query);
        for (a, b) in pairs {
            self.graph.add_weight(a, b, self.config.pair_weight);
            self.pair_updates_applied += 1;
        }
    }

    /// Raw keys from one query boundary → aggregate → emit [`CoReadEvent::Query`].
    pub fn on_query_keys(&mut self, keys: &[Key]) {
        let Some(q) = CoReadQuery::aggregate_bounded(keys, &self.config) else {
            return;
        };
        self.emit_co_read_event(CoReadEvent::Query(q));
    }

    pub fn apply_decay(&mut self, factor: f64) {
        self.graph.scale_all(factor);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ReadTrackingConfig;

    #[test]
    fn bounded_fanout_policy_truncates_keys() {
        let cfg = ReadTrackingConfig {
            max_keys_per_session: 4,
            max_pair_inserts_per_query: 100,
            ..Default::default()
        };
        let mut t = ReadTracker::new(cfg);
        let keys: Vec<Key> = (0u128..20).collect();
        t.on_query_keys(&keys);
        assert!(t.graph().edge_count() <= 6);
    }

    #[test]
    fn bounded_pair_budget() {
        let cfg = ReadTrackingConfig {
            max_keys_per_session: 100,
            max_pair_inserts_per_query: 3,
            ..Default::default()
        };
        let mut t = ReadTracker::new(cfg);
        t.on_query_keys(&[1u128, 2, 3, 4, 5]);
        assert_eq!(t.graph().edge_count(), 3);
    }

    #[test]
    fn emit_query_event_matches_on_query_keys() {
        let cfg = ReadTrackingConfig {
            max_keys_per_session: 10,
            ..Default::default()
        };
        let mut t = ReadTracker::new(cfg);
        let q = CoReadQuery::aggregate_bounded(&[5u128, 2, 2, 9], &t.config).unwrap();
        t.emit_co_read_event(CoReadEvent::Query(q.clone()));
        let w = t.graph().weight(2, 5);
        let mut t2 = ReadTracker::new(ReadTrackingConfig::default());
        t2.on_query_keys(&[5u128, 2, 2, 9]);
        assert!((w - t2.graph().weight(2, 5)).abs() < 1e-9);
    }
}
