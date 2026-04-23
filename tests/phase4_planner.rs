use std::collections::HashMap;

use nexus::{
    build_layout_plan, CoAccessGraph, GroupPlan, LayoutPlan, LayoutPlanner, PlannerConfig,
};

fn kb_uniform(keys: impl Iterator<Item = u128>, b: u64) -> HashMap<u128, u64> {
    keys.map(|k| (k, b)).collect()
}

#[test]
fn clustered_dataset_coherent_groups() {
    let mut g = CoAccessGraph::new(10_000);
    for i in 10..20u128 {
        for j in (i + 1)..20 {
            g.add_weight(i, j, 50.0);
        }
    }
    let kb = kb_uniform(10..20, 1);
    let cfg = PlannerConfig {
        max_keys_per_group: 8,
        rewrite_affinity_threshold: 0.01,
        min_expected_gain: 0.0,
        hysteresis_per_key: 0.0,
        ..Default::default()
    };
    let plan = build_layout_plan(&g, &kb, &cfg);
    assert!(
        !plan.groups.is_empty(),
        "clique should yield at least one group"
    );
    assert!(!plan.has_overlapping_keys());
    for gp in &plan.groups {
        assert!(gp.keys.len() >= 2);
        assert!(gp.keys.len() <= cfg.max_keys_per_group);
    }
}

#[test]
fn weak_signal_minimal_plan() {
    let mut g = CoAccessGraph::new(10_000);
    g.add_weight(900, 901, 100.0);
    for i in 0..30u128 {
        g.add_weight(i, i + 1, 1.0);
    }
    let mut kb = kb_uniform(0..31, 1);
    kb.insert(900, 1);
    kb.insert(901, 1);
    let cfg = PlannerConfig {
        rewrite_affinity_threshold: 0.05,
        ..Default::default()
    };
    let plan = build_layout_plan(&g, &kb, &cfg);
    assert!(
        plan.groups.len() <= 2,
        "weak chain vs one strong pair should not explode into many groups"
    );
    assert!(!plan.has_overlapping_keys());
}

#[test]
fn random_dataset_noop_when_rewrite_unsatisfied() {
    let mut g = CoAccessGraph::new(50_000);
    for i in 0..80u128 {
        g.add_weight(i, i + 1, 1.0);
        g.add_weight(i, i + 2, 1.0);
    }
    let kb = kb_uniform(0..82, 1);
    let cfg = PlannerConfig {
        rewrite_affinity_threshold: 1.01,
        ..Default::default()
    };
    let plan = build_layout_plan(&g, &kb, &cfg);
    assert!(plan.is_empty());
    assert_eq!(plan.total_expected_gain(), 0.0);
}

#[test]
fn unstable_graph_resists_thrash() {
    let kb: HashMap<u128, u64> = [(1, 1), (2, 1), (3, 1), (4, 1)].into_iter().collect();
    let cfg = PlannerConfig {
        rewrite_affinity_threshold: 0.01,
        min_expected_gain: 0.0,
        hysteresis_per_key: 0.0,
        ..Default::default()
    };

    let mut g1 = CoAccessGraph::new(500);
    g1.add_weight(1, 2, 50.0);
    g1.add_weight(2, 3, 50.0);
    g1.add_weight(3, 4, 50.0);
    g1.add_weight(1, 4, 10.0);

    let mut g2 = CoAccessGraph::new(500);
    g2.add_weight(1, 2, 50.0);
    g2.add_weight(2, 3, 49.999);
    g2.add_weight(3, 4, 50.0);
    g2.add_weight(1, 4, 10.001);

    let p1 = build_layout_plan(&g1, &kb, &cfg);
    let p2 = build_layout_plan(&g2, &kb, &cfg);
    assert_eq!(plan_fingerprint(&p1), plan_fingerprint(&p2));
    assert_eq!(p1.total_expected_gain(), p2.total_expected_gain());
}

#[test]
fn identical_graph_identical_plan() {
    let mut g1 = CoAccessGraph::new(1000);
    g1.add_weight(1, 2, 10.0);
    g1.add_weight(2, 3, 10.0);
    g1.add_weight(1, 3, 10.0);
    let mut g2 = CoAccessGraph::new(1000);
    g2.add_weight(1, 2, 10.0);
    g2.add_weight(2, 3, 10.0);
    g2.add_weight(1, 3, 10.0);
    let kb: HashMap<u128, u64> = [(1, 1), (2, 1), (3, 1)].into_iter().collect();
    let cfg = PlannerConfig {
        rewrite_affinity_threshold: 0.01,
        min_expected_gain: 0.0,
        hysteresis_per_key: 0.0,
        ..Default::default()
    };
    let p1 = build_layout_plan(&g1, &kb, &cfg);
    let p2 = build_layout_plan(&g2, &kb, &cfg);
    assert_eq!(plan_fingerprint(&p1), plan_fingerprint(&p2));
}

fn plan_fingerprint(p: &LayoutPlan) -> Vec<(u32, Vec<u128>, u32, i64)> {
    let mut v: Vec<_> = p
        .groups
        .iter()
        .map(|g: &GroupPlan| {
            (
                g.group_id,
                g.keys.clone(),
                g.target_segment_class,
                (g.expected_gain * 1_000_000.0).round() as i64,
            )
        })
        .collect();
    v.sort_by_key(|a| a.0);
    v
}

#[test]
fn gate_no_overlapping_intents() {
    let mut g = CoAccessGraph::new(5000);
    for block in 0..3u128 {
        let base = block * 10;
        for i in base..base + 5 {
            for j in (i + 1)..base + 5 {
                g.add_weight(i, j, 20.0);
            }
        }
    }
    let kb = kb_uniform(0..30, 2);
    let cfg = PlannerConfig {
        max_keys_per_group: 6,
        rewrite_affinity_threshold: 0.01,
        min_expected_gain: 0.0,
        hysteresis_per_key: 0.0,
        ..Default::default()
    };
    let planner = LayoutPlanner::new(cfg);
    let plan = planner.plan(&g, &kb);
    assert!(!plan.has_overlapping_keys());
}

#[test]
fn gate_plan_stable_across_runs() {
    let mut g = CoAccessGraph::new(2000);
    for i in 0..12u128 {
        g.add_weight(i, i + 1, 5.0);
    }
    let kb = kb_uniform(0..13, 1);
    let cfg = PlannerConfig::default();
    let p1 = build_layout_plan(&g, &kb, &cfg);
    let p2 = build_layout_plan(&g, &kb, &cfg);
    assert_eq!(plan_fingerprint(&p1), plan_fingerprint(&p2));
    assert_eq!(p1.total_expected_gain(), p2.total_expected_gain());
}

#[test]
fn gate_plan_quality_measurable_and_stable() {
    let mut g = CoAccessGraph::new(3000);
    for i in 0..8u128 {
        for j in (i + 1)..8 {
            g.add_weight(i, j, 30.0);
        }
    }
    let kb = kb_uniform(0..8, 1);
    let cfg = PlannerConfig {
        max_keys_per_group: 8,
        rewrite_affinity_threshold: 0.01,
        min_expected_gain: 0.0,
        hysteresis_per_key: 0.0,
        ..Default::default()
    };
    let a = build_layout_plan(&g, &kb, &cfg);
    let b = build_layout_plan(&g, &kb, &cfg);
    let gain = a.total_expected_gain();
    assert!(gain.is_finite() && gain >= 0.0);
    assert_eq!(gain, b.total_expected_gain());
    assert_eq!(a.total_keys_in_plan(), b.total_keys_in_plan());
    assert!(!a.has_overlapping_keys());
}
