//! Affinity: affinity(A,B) = coaccess_weight / normalization.

use crate::graph::coaccess::CoAccessGraph;
use crate::types::Key;

pub fn normalization_factor(graph: &CoAccessGraph) -> f64 {
    let m = graph.max_weight();
    if m <= 1e-15 {
        1.0
    } else {
        m
    }
}

pub fn affinity(graph: &CoAccessGraph, a: Key, b: Key, norm: f64) -> f64 {
    graph.weight(a, b) / norm
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::coaccess::CoAccessGraph;

    #[test]
    fn affinity_normalization() {
        let mut g = CoAccessGraph::new(1000);
        g.add_weight(1, 2, 10.0);
        g.add_weight(3, 4, 5.0);
        let n = normalization_factor(&g);
        assert!((n - 10.0).abs() < 1e-9);
        assert!((affinity(&g, 1, 2, n) - 1.0).abs() < 1e-9);
        assert!((affinity(&g, 3, 4, n) - 0.5).abs() < 1e-9);
    }
}
