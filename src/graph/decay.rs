//! Multiplicative decay on graph weights.

use crate::graph::coaccess::CoAccessGraph;

/// Applies `graph.scale_all(factor)` (typically 0 < factor ≤ 1).
pub fn apply_decay(graph: &mut CoAccessGraph, factor: f64) {
    debug_assert!(factor >= 0.0);
    graph.scale_all(factor);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::coaccess::CoAccessGraph;

    #[test]
    fn decay_correctness() {
        let mut g = CoAccessGraph::new(1000);
        g.add_weight(1, 2, 10.0);
        apply_decay(&mut g, 0.5);
        assert!((g.weight(1, 2) - 5.0).abs() < 1e-9);
    }
}
