//! Deterministic export of the co-access graph.

use serde::{Deserialize, Serialize};

use crate::graph::coaccess::CoAccessGraph;
use crate::types::Key;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphSnapshot {
    pub edges: Vec<SnapshotEdge>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SnapshotEdge {
    pub a: Key,
    pub b: Key,
    pub weight: f64,
}

impl CoAccessGraph {
    /// Sorted by `(a, b)` for stable bytes / JSON across reruns.
    pub fn export_snapshot(&self) -> GraphSnapshot {
        let mut edges: Vec<SnapshotEdge> = self
            .iter_edges()
            .map(|((a, b), w)| SnapshotEdge { a, b, weight: w })
            .collect();
        edges.sort_by_key(|x| (x.a, x.b));
        GraphSnapshot { edges }
    }
}

#[cfg(test)]
mod tests {
    use crate::graph::coaccess::CoAccessGraph;

    #[test]
    fn snapshot_stable_order() {
        let mut g = CoAccessGraph::new(100);
        g.add_weight(2, 1, 1.0);
        g.add_weight(10, 5, 2.0);
        let s1 = g.export_snapshot();
        let s2 = g.export_snapshot();
        assert_eq!(s1, s2);
        assert_eq!(s1.edges[0].a, 1);
        assert_eq!(s1.edges[0].b, 2);
    }
}
