//! Planner grouping.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use crate::graph::coaccess::CoAccessGraph;
use crate::planner::affinity::{affinity, normalization_factor};
use crate::planner::plan::{GroupPlan, LayoutPlan};
use crate::planner::thresholds::PlannerConfig;
use crate::types::Key;

fn filtered_adjacency(
    graph: &CoAccessGraph,
    norm: f64,
    cfg: &PlannerConfig,
) -> BTreeMap<Key, Vec<Key>> {
    let mut adj: BTreeMap<Key, Vec<Key>> = BTreeMap::new();
    for ((a, b), w) in graph.iter_edges() {
        let aff = w / norm;
        if aff < cfg.rewrite_affinity_threshold {
            continue;
        }
        adj.entry(a).or_default().push(b);
        adj.entry(b).or_default().push(a);
    }
    for v in adj.values_mut() {
        v.sort_unstable();
        v.dedup();
    }
    adj
}

fn connected_components(adj: &BTreeMap<Key, Vec<Key>>) -> Vec<Vec<Key>> {
    let mut seen: BTreeSet<Key> = BTreeSet::new();
    let mut comps = Vec::new();
    for &start in adj.keys() {
        if seen.contains(&start) {
            continue;
        }
        let mut comp = Vec::new();
        let mut q = VecDeque::new();
        q.push_back(start);
        seen.insert(start);
        while let Some(u) = q.pop_front() {
            comp.push(u);
            if let Some(nbrs) = adj.get(&u) {
                for &v in nbrs {
                    if seen.insert(v) {
                        q.push_back(v);
                    }
                }
            }
        }
        comps.push(comp);
    }
    comps.sort();
    comps
}

fn pack_component(
    comp: &[Key],
    graph: &CoAccessGraph,
    norm: f64,
    key_bytes: &HashMap<Key, u64>,
    cfg: &PlannerConfig,
    group_id: &mut u32,
) -> Vec<GroupPlan> {
    let mut out = Vec::new();
    let mut start = 0usize;
    while start < comp.len() {
        let mut end = start + 1;
        let mut bytes = *key_bytes.get(&comp[start]).unwrap_or(&0);
        while end < comp.len() {
            let next_b = *key_bytes.get(&comp[end]).unwrap_or(&0);
            if end - start >= cfg.max_keys_per_group {
                break;
            }
            if bytes + next_b > cfg.max_bytes_per_group {
                break;
            }
            bytes += next_b;
            end += 1;
        }
        let chunk = &comp[start..end];
        start = end;
        if chunk.len() >= 2 {
            let gain = group_internal_gain(graph, norm, chunk, cfg);
            if gain >= cfg.min_expected_gain {
                let gid = *group_id;
                *group_id += 1;
                let class = gid % cfg.num_segment_classes.max(1);
                out.push(GroupPlan {
                    group_id: gid,
                    keys: chunk.to_vec(),
                    target_segment_class: class,
                    expected_gain: gain,
                });
            }
        }
    }
    out
}

fn group_internal_gain(graph: &CoAccessGraph, norm: f64, keys: &[Key], cfg: &PlannerConfig) -> f64 {
    let mut sum = 0.0;
    for ii in 0..keys.len() {
        for jj in (ii + 1)..keys.len() {
            let a = keys[ii];
            let b = keys[jj];
            if graph.weight(a, b) > 0.0 {
                sum += affinity(graph, a, b, norm);
            }
        }
    }
    sum - cfg.hysteresis_per_key * (keys.len() as f64)
}

pub fn build_layout_plan(
    graph: &CoAccessGraph,
    key_bytes: &HashMap<Key, u64>,
    cfg: &PlannerConfig,
) -> LayoutPlan {
    let norm = normalization_factor(graph);
    if graph.edge_count() == 0 || norm <= 0.0 {
        return LayoutPlan { groups: vec![] };
    }

    let adj = filtered_adjacency(graph, norm, cfg);
    if adj.is_empty() {
        return LayoutPlan { groups: vec![] };
    }

    let components = connected_components(&adj);
    let mut groups = Vec::new();
    let mut gid = 0u32;
    for comp in components {
        if comp.len() < 2 {
            continue;
        }
        groups.extend(pack_component(&comp, graph, norm, key_bytes, cfg, &mut gid));
    }

    groups.sort_by_key(|a| a.group_id);
    LayoutPlan { groups }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::coaccess::CoAccessGraph;

    #[test]
    fn grouping_under_size_caps() {
        let mut g = CoAccessGraph::new(100);
        for i in 0..10u128 {
            g.add_weight(i, i + 1, 100.0);
        }
        let mut kb = HashMap::new();
        for i in 0..11u128 {
            kb.insert(i, 1);
        }
        let cfg = PlannerConfig {
            max_keys_per_group: 3,
            rewrite_affinity_threshold: 0.01,
            min_expected_gain: 0.0,
            hysteresis_per_key: 0.0,
            ..Default::default()
        };
        let plan = build_layout_plan(&g, &kb, &cfg);
        for gp in &plan.groups {
            assert!(gp.keys.len() <= 3);
        }
    }

    #[test]
    fn hysteresis_threshold_behavior() {
        let mut g = CoAccessGraph::new(100);
        g.add_weight(1, 2, 1.0);
        g.add_weight(2, 3, 1.0);
        let kb = [(1u128, 1u64), (2, 1), (3, 1)].into_iter().collect();
        let cfg = PlannerConfig {
            rewrite_affinity_threshold: 0.01,
            min_expected_gain: 1e9,
            hysteresis_per_key: 100.0,
            ..Default::default()
        };
        let plan = build_layout_plan(&g, &kb, &cfg);
        assert!(plan.groups.is_empty());
    }

    #[test]
    fn overlap_rejection_by_construction() {
        let mut g = CoAccessGraph::new(100);
        g.add_weight(1, 2, 10.0);
        g.add_weight(2, 3, 10.0);
        let kb = [(1u128, 1u64), (2, 1), (3, 1)].into_iter().collect();
        let cfg = PlannerConfig {
            rewrite_affinity_threshold: 0.01,
            min_expected_gain: 0.0,
            hysteresis_per_key: 0.0,
            ..Default::default()
        };
        let plan = build_layout_plan(&g, &kb, &cfg);
        assert!(!plan.has_overlapping_keys());
    }

    #[test]
    fn overlap_rejection_nonempty_multigroup() {
        let mut g = CoAccessGraph::new(500);
        g.add_weight(1, 2, 100.0);
        g.add_weight(10, 11, 100.0);
        g.add_weight(20, 21, 100.0);
        let kb = [1u128, 2, 10, 11, 20, 21]
            .into_iter()
            .map(|k| (k, 1u64))
            .collect();
        let cfg = PlannerConfig {
            rewrite_affinity_threshold: 0.01,
            min_expected_gain: 0.0,
            hysteresis_per_key: 0.0,
            max_keys_per_group: 2,
            ..Default::default()
        };
        let plan = build_layout_plan(&g, &kb, &cfg);
        assert!(
            plan.groups.len() >= 2,
            "expected several disjoint pair groups"
        );
        assert!(!plan.has_overlapping_keys());
        let n: usize = plan.groups.iter().map(|gp| gp.keys.len()).sum();
        assert_eq!(n, plan.total_keys_in_plan());
    }
}
