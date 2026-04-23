//! Primary index, batch reads, tombstones.

use std::collections::HashMap;

use nexus::config::EngineConfig;
use nexus::storage::record::Record;
use nexus::types::record_flags;
use nexus::Engine;
use tempfile::tempdir;

#[test]
fn multiple_rewrites_same_key() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone()).unwrap();
        eng.put(Record::new(99, 1, 0, b"a".to_vec())).unwrap();
        eng.put(Record::new(99, 2, 0, b"b".to_vec())).unwrap();
        eng.put(Record::new(99, 3, 0, b"c".to_vec())).unwrap();
        assert_eq!(eng.get(99).unwrap().unwrap().payload, b"c");
        eng.close().unwrap();
    }
    let eng = Engine::open(cfg).unwrap();
    assert_eq!(eng.get(99).unwrap().unwrap().payload, b"c");
}

#[test]
fn delete_then_reopen() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone()).unwrap();
        eng.put(Record::new(5, 1, 0, b"live".to_vec())).unwrap();
        eng.delete(5, 2).unwrap();
        assert!(eng.get(5).unwrap().is_none());
        eng.close().unwrap();
    }
    {
        let eng = Engine::open(cfg.clone()).unwrap();
        assert!(eng.get(5).unwrap().is_none());
    }
    {
        let mut eng = Engine::open(cfg.clone()).unwrap();
        eng.put(Record::new(5, 3, 0, b"again".to_vec())).unwrap();
        eng.close().unwrap();
    }
    let eng = Engine::open(cfg).unwrap();
    assert_eq!(eng.get(5).unwrap().unwrap().payload, b"again");
}

/// Batch read result order follows the **request** key order (not segment order).
#[test]
fn batch_read_result_ordering() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone()).unwrap();
        for k in 0..1000u128 {
            eng.put(Record::new(k, 1, 0, format!("v{}", k).into_bytes()))
                .unwrap();
        }
        eng.close().unwrap();
    }
    let eng = Engine::open(cfg.clone()).unwrap();
    let keys: Vec<u128> = (0..1000)
        .map(|i| {
            let x = (i * 7919 + 13) % 1000;
            x as u128
        })
        .collect();
    let got = eng.get_many(&keys).unwrap();
    assert_eq!(got.len(), keys.len());
    for (i, &k) in keys.iter().enumerate() {
        let p = got[i].as_ref().expect("value");
        assert_eq!(p.payload, format!("v{}", k).as_bytes());
    }
}

#[test]
fn read_1k_random_keys_after_mixed_updates() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    let mut eng = Engine::open(cfg.clone()).unwrap();
    let mut expected: HashMap<u128, Vec<u8>> = HashMap::new();
    for pass in 0u64..5 {
        for k in 0u128..1000 {
            let v = pass * 1000 + (k as u64) + 1;
            let payload = format!("{}:{}", k, v).into_bytes();
            eng.put(Record::new(k, v, 0, payload.clone())).unwrap();
            expected.insert(k, payload);
        }
    }
    assert_eq!(expected.len(), 1000);
    for (k, want) in &expected {
        assert_eq!(&eng.get(*k).unwrap().unwrap().payload, want);
    }
}

#[test]
fn batch_read_across_multiple_segments() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone()).unwrap();
        eng.put(Record::new(1, 1, 0, b"seg-a".to_vec())).unwrap();
        eng.close().unwrap();
    }
    {
        let mut eng = Engine::open(cfg.clone()).unwrap();
        eng.put(Record::new(2, 1, 0, b"seg-b".to_vec())).unwrap();
        eng.close().unwrap();
    }
    let eng = Engine::open(cfg).unwrap();
    let keys = [2u128, 1];
    let got = eng.get_many(&keys).unwrap();
    assert_eq!(got[0].as_ref().unwrap().payload, b"seg-b");
    assert_eq!(got[1].as_ref().unwrap().payload, b"seg-a");
}

#[test]
fn tombstone_hides_old_from_get_many() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone()).unwrap();
        eng.put(Record::new(7, 1, 0, b"x".to_vec())).unwrap();
        eng.put(Record::new(7, 2, record_flags::TOMBSTONE, vec![]))
            .unwrap();
        let r = eng.get_many(&[7]).unwrap();
        assert!(r[0].is_none());
        eng.close().unwrap();
    }
}

#[test]
fn mixed_write_read_stats_monotonic() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    let mut eng = Engine::open(cfg.clone()).unwrap();
    let s0 = eng.stats().snapshot();
    eng.put(Record::new(1, 1, 0, b"a".to_vec())).unwrap();
    let after_put = eng.stats().snapshot();
    assert!(after_put.writes > s0.writes);
    let before_read = eng.stats().snapshot();
    eng.get(1).unwrap();
    let s1 = eng.stats().snapshot();
    assert_eq!(s1.writes, before_read.writes);
    assert!(s1.reads > before_read.reads);
    assert!(s1.bytes_read > before_read.bytes_read);
    assert!(s1.segments_touched > before_read.segments_touched);
}

/// Gate: mixed writes, close/reopen, all keys match deterministic latest versions.
#[test]
fn gate_mixed_workload_reopen_latest_deterministic() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    let mut expected: HashMap<u128, Vec<u8>> = HashMap::new();
    {
        let mut eng = Engine::open(cfg.clone()).unwrap();
        for i in 0..400 {
            let k = (i % 25) as u128;
            let pl = format!("i{}", i).into_bytes();
            eng.put(Record::new(k, i as u64 + 1, 0, pl.clone()))
                .unwrap();
            expected.insert(k, pl);
        }
        eng.close().unwrap();
    }
    let eng = Engine::open(cfg).unwrap();
    for (k, want) in &expected {
        assert_eq!(&eng.get(*k).unwrap().unwrap().payload, want);
    }
}
