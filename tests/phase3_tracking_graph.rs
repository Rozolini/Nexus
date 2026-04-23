//! Read tracking, sessions, co-access graph.

use nexus::config::{EngineConfig, ReadTrackingConfig};
use nexus::graph::scoring::total_edge_weight;
use nexus::storage::record::Record;
use nexus::{Engine, ReadSession};
use tempfile::tempdir;

fn make_cfg(dir: &std::path::Path) -> EngineConfig {
    let mut c = EngineConfig::new(dir);
    c.read_tracking = ReadTrackingConfig {
        enabled: true,
        max_keys_per_session: 64,
        max_pair_inserts_per_query: 512,
        max_graph_edges: 10_000,
        pair_weight: 1.0,
    };
    c
}

#[test]
fn synthetic_cluster_workload_forms_dense_subgraph() {
    let dir = tempdir().unwrap();
    let cfg = make_cfg(dir.path());
    let cluster: Vec<u128> = (0..12).collect();
    let mut eng = Engine::open(cfg).unwrap();
    for &k in &cluster {
        eng.put(Record::new(k, 1, 0, vec![k as u8])).unwrap();
    }
    for _ in 0..30 {
        let _ = eng.get_many_tracked(&cluster).unwrap();
    }
    let w01 = eng.read_tracker().graph().weight(0, 1);
    assert!(w01 >= 20.0, "cluster pair should accumulate: {}", w01);
}

#[test]
fn random_queries_do_not_create_heavy_cross_edges() {
    let dir = tempdir().unwrap();
    let mut cfg = make_cfg(dir.path());
    cfg.read_tracking.max_pair_inserts_per_query = 64;
    let mut eng = Engine::open(cfg).unwrap();
    for k in 0..40u128 {
        eng.put(Record::new(k + 1000, 1, 0, vec![1])).unwrap();
    }
    for q in 0..80u128 {
        let keys = [1000 + q * 3, 1001 + q * 3, 1002 + q * 3];
        let _ = eng.get_many_tracked(&keys).unwrap();
    }
    let g = eng.read_tracker().graph();
    let w_far = g.weight(1000, 1039);
    assert!(
        w_far <= 2.0,
        "random triples rarely repeat same far pair: {}",
        w_far
    );
}

#[test]
fn graph_snapshot_stable_across_reruns() {
    let dir = tempdir().unwrap();
    let cfg = make_cfg(dir.path());
    let mut eng = Engine::open(cfg).unwrap();
    eng.put(Record::new(1, 1, 0, b"a".to_vec())).unwrap();
    eng.put(Record::new(2, 1, 0, b"b".to_vec())).unwrap();
    let _ = eng.get_many_tracked(&[1, 2]).unwrap();
    let s1 = eng.graph_snapshot();
    let s2 = eng.graph_snapshot();
    assert_eq!(s1, s2);
}

#[test]
fn gate_clustered_vs_random_separation() {
    let dir = tempdir().unwrap();
    let cfg = make_cfg(dir.path());
    let mut eng = Engine::open(cfg).unwrap();
    for k in 0..8u128 {
        eng.put(Record::new(k, 1, 0, vec![k as u8])).unwrap();
    }
    let cluster: Vec<u128> = (0..8).collect();
    for _ in 0..25 {
        let _ = eng.get_many_tracked(&cluster).unwrap();
    }
    for _ in 0..25 {
        let random_batch: Vec<u128> = (0..8).map(|i| 100 + i as u128).collect();
        for &k in &random_batch {
            eng.put(Record::new(k, 1, 0, vec![])).unwrap();
        }
        let _ = eng.get_many_tracked(&random_batch).unwrap();
    }
    let w_in = eng.read_tracker().graph().weight(0, 1);
    let w_mix = eng.read_tracker().graph().weight(0, 100);
    assert!(
        w_in > w_mix * 5.0,
        "cluster affinity {} vs mixed {}",
        w_in,
        w_mix
    );
}

#[test]
fn bounded_graph_growth() {
    let dir = tempdir().unwrap();
    let mut cfg = make_cfg(dir.path());
    cfg.read_tracking.max_graph_edges = 50;
    cfg.read_tracking.max_pair_inserts_per_query = 20;
    let mut eng = Engine::open(cfg).unwrap();
    for k in 0..200u128 {
        eng.put(Record::new(k, 1, 0, vec![])).unwrap();
    }
    for round in 0..100u128 {
        let keys: Vec<u128> = (0..30).map(|i| (round + i) % 200).collect();
        let _ = eng.get_many_tracked(&keys).unwrap();
    }
    assert!(
        eng.read_tracker().graph().edge_count() <= 80,
        "graph stays bounded"
    );
}

#[test]
fn read_session_query_boundary() {
    let dir = tempdir().unwrap();
    let cfg = make_cfg(dir.path());
    let mut eng = Engine::open(cfg).unwrap();
    eng.put(Record::new(5, 1, 0, b"x".to_vec())).unwrap();
    eng.put(Record::new(9, 1, 0, b"y".to_vec())).unwrap();
    let mut s = ReadSession::new(16);
    s.record_key(5);
    s.record_key(9);
    eng.finish_read_session(s);
    assert!((eng.read_tracker().graph().weight(5, 9) - 1.0).abs() < 1e-6);
}

#[test]
fn repeated_identical_query_increases_weight_predictably() {
    let dir = tempdir().unwrap();
    let cfg = make_cfg(dir.path());
    let mut eng = Engine::open(cfg).unwrap();
    for k in [10u128, 11] {
        eng.put(Record::new(k, 1, 0, vec![])).unwrap();
    }
    let keys = [10u128, 11];
    for _ in 0..5 {
        let _ = eng.get_many_tracked(&keys).unwrap();
    }
    assert!((eng.read_tracker().graph().weight(10, 11) - 5.0).abs() < 1e-6);
}

#[test]
fn decay_reduces_total_weight() {
    let dir = tempdir().unwrap();
    let cfg = make_cfg(dir.path());
    let mut eng = Engine::open(cfg).unwrap();
    eng.put(Record::new(1, 1, 0, vec![])).unwrap();
    eng.put(Record::new(2, 1, 0, vec![])).unwrap();
    let _ = eng.get_many_tracked(&[1, 2]).unwrap();
    let t0 = total_edge_weight(eng.read_tracker().graph());
    eng.decay_read_graph(0.5);
    let t1 = total_edge_weight(eng.read_tracker().graph());
    assert!(t1 < t0 && t1 > 0.0);
}
