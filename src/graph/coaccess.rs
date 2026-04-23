//! Undirected co-access graph: `edge(A,B) += weight` with symmetric storage.

use std::collections::HashMap;

use crate::types::Key;

/// Canonical undirected edge key: `(min(a,b), max(a,b))`.
#[inline]
pub fn normalize_pair(a: Key, b: Key) -> (Key, Key) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CoAccessGraph {
    edges: HashMap<(Key, Key), f64>,
    max_edges: usize,
}

impl CoAccessGraph {
    pub fn new(max_edges: usize) -> Self {
        Self {
            edges: HashMap::new(),
            max_edges,
        }
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Maximum weight over all stored edges (0 if empty).
    pub fn max_weight(&self) -> f64 {
        self.edges.values().cloned().fold(0.0_f64, f64::max)
    }

    pub fn weight(&self, a: Key, b: Key) -> f64 {
        let e = normalize_pair(a, b);
        self.edges.get(&e).copied().unwrap_or(0.0)
    }

    /// Adds `weight` to the undirected edge, enforcing a hard cap on distinct edges.
    pub fn add_weight(&mut self, a: Key, b: Key, weight: f64) {
        if a == b || weight == 0.0 {
            return;
        }
        let e = normalize_pair(a, b);
        if self.edges.contains_key(&e) {
            *self.edges.get_mut(&e).unwrap() += weight;
            return;
        }
        if self.edges.len() >= self.max_edges {
            self.prune_weakest_fraction(0.1);
            if self.edges.len() >= self.max_edges {
                return;
            }
        }
        self.edges.insert(e, weight);
    }

    /// Deterministic merge: same as repeated `add_weight` for each pair.
    pub fn merge(&mut self, other: &CoAccessGraph) {
        for (&k, &w) in &other.edges {
            *self.edges.entry(k).or_insert(0.0) += w;
        }
    }

    fn prune_weakest_fraction(&mut self, fraction: f64) {
        if self.edges.is_empty() {
            return;
        }
        let mut w: Vec<((Key, Key), f64)> = self.edges.iter().map(|(&k, &v)| (k, v)).collect();
        w.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let cut = ((w.len() as f64) * fraction).ceil() as usize;
        for ((ka, kb), _) in w.into_iter().take(cut) {
            self.edges.remove(&(ka, kb));
        }
    }

    pub fn iter_edges(&self) -> impl Iterator<Item = ((Key, Key), f64)> + '_ {
        self.edges.iter().map(|(&k, &v)| (k, v))
    }

    /// All edge weights multiplied by `factor` (typically in (0,1]).
    pub fn scale_all(&mut self, factor: f64) {
        for v in self.edges.values_mut() {
            *v *= factor;
        }
        self.edges.retain(|_, w| *w > 1e-12);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symmetric_edge_normalization() {
        let mut g = CoAccessGraph::new(10_000);
        g.add_weight(3, 7, 1.0);
        g.add_weight(7, 3, 2.0);
        assert_eq!(g.weight(3, 7), 3.0);
        assert_eq!(g.edge_count(), 1);
    }

    #[test]
    fn edge_accumulation() {
        let mut g = CoAccessGraph::new(10_000);
        g.add_weight(1, 2, 0.5);
        g.add_weight(1, 2, 0.5);
        assert!((g.weight(1, 2) - 1.0).abs() < 1e-9);
    }
}
