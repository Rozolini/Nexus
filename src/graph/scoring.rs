//! Simple graph statistics.

use crate::graph::coaccess::CoAccessGraph;

pub fn total_edge_weight(g: &CoAccessGraph) -> f64 {
    g.iter_edges().map(|(_, w)| w).sum()
}
