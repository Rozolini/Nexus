//! Scheduler integration and gate.

use std::collections::HashMap;

use nexus::graph::CoAccessGraph;
use nexus::planner::PlannerConfig;
use nexus::storage::record::Record;
use nexus::{BackgroundScheduler, Engine, EngineConfig, SchedulerConfig};

fn sched_cfg_tight() -> SchedulerConfig {
    SchedulerConfig {
        max_bytes_rewritten_per_cycle: 256,
        max_groups_relocated_per_cycle: 1,
        max_background_cpu_share: 1.0,
        graph_pressure_edge_ratio_threshold: 0.0,
        fragmentation_segments_threshold: 0,
        locality_gain_threshold: 0.0,
        cooldown_cycles_per_key: 2,
        cooldown_cycles_per_group: 2,
        minimum_improvement_delta: 0.0,
    }
}

#[test]
fn locality_workload_improves_gradually_not_explosively() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = EngineConfig::new(dir.path());
    cfg.scheduler = sched_cfg_tight();
    let mut eng = Engine::open(cfg.clone()).unwrap();
    for k in 0u128..8 {
        eng.put(Record::new(k, 1, 0, vec![k as u8])).unwrap();
    }
    let mut graph = CoAccessGraph::new(10_000);
    for i in 0..8u128 {
        for j in (i + 1)..8 {
            graph.add_weight(i, j, 30.0);
        }
    }
    let planner = PlannerConfig {
        rewrite_affinity_threshold: 0.01,
        min_expected_gain: 0.0,
        hysteresis_per_key: 0.0,
        max_keys_per_group: 4,
        ..Default::default()
    };

    let ks: HashMap<u128, u64> = (0u128..8).map(|k| (k, 16u64)).collect();
    let mut sched = BackgroundScheduler::new(cfg.scheduler.clone());
    let max_per_cycle = cfg.scheduler.max_groups_relocated_per_cycle;
    let mut total_groups = 0usize;
    for _ in 0..20 {
        let r = sched
            .run_cycle(&mut eng, &graph, &planner, &ks, 500_000)
            .unwrap();
        total_groups += r.groups_relocated;
        assert!(r.groups_relocated <= max_per_cycle);
    }
    assert!(total_groups <= 20 * max_per_cycle);
}

#[test]
fn adversarial_alternating_workload_does_not_thrash_endlessly() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = EngineConfig::new(dir.path());
    cfg.scheduler = SchedulerConfig {
        max_bytes_rewritten_per_cycle: 512,
        max_groups_relocated_per_cycle: 1,
        max_background_cpu_share: 0.5,
        graph_pressure_edge_ratio_threshold: 0.0,
        fragmentation_segments_threshold: 0,
        locality_gain_threshold: 0.0,
        cooldown_cycles_per_key: 5,
        cooldown_cycles_per_group: 5,
        minimum_improvement_delta: 0.1,
    };
    let mut eng = Engine::open(cfg.clone()).unwrap();
    eng.put(Record::new(1u128, 1, 0, b"a")).unwrap();
    eng.put(Record::new(2u128, 1, 0, b"b")).unwrap();
    let mut graph = CoAccessGraph::new(1000);
    let planner = PlannerConfig {
        rewrite_affinity_threshold: 0.01,
        min_expected_gain: 0.0,
        hysteresis_per_key: 0.0,
        ..Default::default()
    };

    let ks: HashMap<u128, u64> = [(1u128, 8), (2, 8)].into_iter().collect();
    let mut sched = BackgroundScheduler::new(cfg.scheduler.clone());
    let mut reloc_total = 0u64;
    for c in 0..200u32 {
        if c % 2 == 0 {
            graph.add_weight(1, 2, 100.0);
        } else {
            graph = CoAccessGraph::new(1000);
            graph.add_weight(1, 2, 100.0);
        }
        let r = sched
            .run_cycle(&mut eng, &graph, &planner, &ks, 500_000)
            .unwrap();
        reloc_total += r.groups_relocated as u64;
    }
    assert!(
        reloc_total < 80,
        "relocations should stay bounded, got {reloc_total}"
    );
}

#[test]
fn mixed_read_write_load_remains_serviceable() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = EngineConfig::new(dir.path());
    cfg.scheduler = sched_cfg_tight();
    let mut eng = Engine::open(cfg.clone()).unwrap();
    let mut graph = CoAccessGraph::new(5000);
    let mut sched = BackgroundScheduler::new(cfg.scheduler.clone());
    for round in 0u128..30 {
        eng.put(Record::new(round, 1, 0, b"x")).unwrap();
        if round >= 2 {
            graph.add_weight(round - 1, round, 5.0);
        }
        let _ = eng.get(round).unwrap();
        let planner = PlannerConfig {
            rewrite_affinity_threshold: 0.05,
            min_expected_gain: 0.0,
            hysteresis_per_key: 0.0,
            ..Default::default()
        };
        let ks: HashMap<u128, u64> = eng.iter_index().map(|(k, _)| (*k, 4u64)).collect();
        let _ = sched
            .run_cycle(&mut eng, &graph, &planner, &ks, 500_000)
            .unwrap();
    }
}

#[test]
fn gate_rewrite_amplification_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = EngineConfig::new(dir.path());
    cfg.scheduler = sched_cfg_tight();
    let mut eng = Engine::open(cfg.clone()).unwrap();
    for k in [10u128, 11] {
        eng.put(Record::new(k, 1, 0, b"z")).unwrap();
    }
    let mut graph = CoAccessGraph::new(1000);
    graph.add_weight(10, 11, 40.0);
    let planner = PlannerConfig {
        rewrite_affinity_threshold: 0.01,
        min_expected_gain: 0.0,
        hysteresis_per_key: 0.0,
        ..Default::default()
    };
    let ks: HashMap<u128, u64> = [(10u128, 100), (11, 100)].into_iter().collect();
    let mut sched = BackgroundScheduler::new(cfg.scheduler.clone());
    for _ in 0..5 {
        let _ = sched
            .run_cycle(&mut eng, &graph, &planner, &ks, 500_000)
            .unwrap();
    }
    let logical = 200u64;
    let amp = if logical > 0 {
        sched.total_bytes_rewritten as f64 / logical as f64
    } else {
        0.0
    };
    assert!(
        amp < 50.0,
        "rewrite amplification ratio unexpectedly high: {amp}"
    );
}

#[test]
fn gate_no_pathological_endless_churn() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = EngineConfig::new(dir.path());
    cfg.scheduler = SchedulerConfig {
        max_bytes_rewritten_per_cycle: 128,
        max_groups_relocated_per_cycle: 1,
        max_background_cpu_share: 0.2,
        graph_pressure_edge_ratio_threshold: 0.0,
        fragmentation_segments_threshold: 0,
        locality_gain_threshold: 0.0,
        cooldown_cycles_per_key: 3,
        cooldown_cycles_per_group: 3,
        minimum_improvement_delta: 0.02,
    };
    let mut eng = Engine::open(cfg.clone()).unwrap();
    eng.put(Record::new(1u128, 1, 0, b"p")).unwrap();
    eng.put(Record::new(2u128, 1, 0, b"q")).unwrap();
    let mut graph = CoAccessGraph::new(2000);
    graph.add_weight(1, 2, 1.0);
    let planner = PlannerConfig {
        rewrite_affinity_threshold: 0.001,
        min_expected_gain: 0.0,
        hysteresis_per_key: 0.0,
        ..Default::default()
    };

    let ks: HashMap<u128, u64> = [(1u128, 8), (2, 8)].into_iter().collect();
    let mut sched = BackgroundScheduler::new(cfg.scheduler.clone());
    let mut per_cycle = 0usize;
    for _ in 0..500 {
        let r = sched
            .run_cycle(&mut eng, &graph, &planner, &ks, 500_000)
            .unwrap();
        per_cycle += r.groups_relocated;
    }
    assert!(
        per_cycle < 100,
        "expected churn to stay sublinear, got {per_cycle} group relocations"
    );
}
