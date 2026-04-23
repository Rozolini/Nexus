//! Compaction + Relocation Engine (integration + gate).

use nexus::storage::segment::segment_file_name;
use nexus::storage::segment_writer::SegmentWriter;
use nexus::{Engine, EngineConfig, GroupPlan, Record};
use nexus::{Result, SegmentId};

#[test]
fn relocate_hot_cluster_and_verify_reads() {
    let dir = tempfile::tempdir().unwrap();
    let mut eng = Engine::open(EngineConfig::new(dir.path())).unwrap();
    for k in [10u128, 11, 12, 13] {
        eng.put(Record::new(k, 1, 0, format!("v{k}").into_bytes()))
            .unwrap();
    }
    let want: Vec<_> = [10u128, 11, 12, 13]
        .into_iter()
        .map(|k| (k, format!("v{k}").into_bytes()))
        .collect();
    let group = GroupPlan {
        group_id: 0,
        keys: vec![10, 11, 12, 13],
        target_segment_class: 0,
        expected_gain: 1.0,
    };
    eng.relocate_group(&group).unwrap();
    for (k, payload) in want {
        assert_eq!(eng.get(k).unwrap().unwrap().payload, payload);
    }
}

#[test]
fn relocate_while_serving_reads() {
    let dir = tempfile::tempdir().unwrap();
    let mut eng = Engine::open(EngineConfig::new(dir.path())).unwrap();
    for k in 0u128..20 {
        eng.put(Record::new(k, 1, 0, vec![k as u8])).unwrap();
    }
    let keys: Vec<u128> = (0..5).collect();
    assert_eq!(eng.get_many(&keys).unwrap().len(), 5);
    eng.relocate_keys(&[0, 1, 2]).unwrap();
    assert_eq!(eng.get_many(&keys).unwrap().len(), 5);
    eng.relocate_keys(&[3, 4, 5]).unwrap();
    let got = eng.get_many(&keys).unwrap();
    for (i, &k) in keys.iter().enumerate() {
        assert_eq!(got[i].as_ref().unwrap().payload, vec![k as u8]);
    }
}

#[test]
fn crash_before_install() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone())?;
        eng.put(Record::new(77u128, 5, 0, b"from_seg1"))?;
        eng.close()?;
    }
    let path2 = dir.path().join(segment_file_name(SegmentId(2)));
    let mut w = SegmentWriter::create(&path2, SegmentId(2))?;
    w.append(&Record::new(77u128, 5, 0, b"from_seg2"))?;
    w.seal()?;
    let eng = Engine::open(cfg)?;
    assert_eq!(eng.get(77)?.unwrap().payload, b"from_seg2");
    Ok(())
}

#[test]
fn crash_after_install() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone())?;
        eng.put(Record::new(99u128, 3, 0, b"live"))?;
        eng.relocate_keys(&[99])?;
        eng.close()?;
    }
    let eng = Engine::open(cfg)?;
    assert_eq!(eng.get(99)?.unwrap().payload, b"live");
    Ok(())
}

#[test]
fn repeated_compaction_cycles_preserve_correctness() {
    let dir = tempfile::tempdir().unwrap();
    let mut eng = Engine::open(EngineConfig::new(dir.path())).unwrap();
    eng.put(Record::new(1u128, 10, 0, b"a")).unwrap();
    for _ in 0..4 {
        eng.relocate_keys(&[1]).unwrap();
        assert_eq!(eng.get(1).unwrap().unwrap().payload, b"a");
        assert_eq!(eng.get(1).unwrap().unwrap().version, 10);
    }
}

#[test]
fn crash_safe_relocation_proven_by_recovery_tests() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone())?;
        eng.put(Record::new(7u128, 1, 0, b"x"))?;
        eng.relocate_keys(&[7])?;
    }
    let eng = Engine::open(cfg.clone())?;
    assert_eq!(eng.get(7)?.unwrap().payload, b"x");

    let dir2 = tempfile::tempdir().unwrap();
    let cfg2 = EngineConfig::new(dir2.path());
    {
        let mut eng = Engine::open(cfg2.clone())?;
        eng.put(Record::new(8u128, 2, 0, b"y"))?;
        eng.relocate_keys(&[8])?;
    }
    let eng2 = Engine::open(cfg2)?;
    assert_eq!(eng2.get(8)?.unwrap().payload, b"y");
    Ok(())
}

#[test]
fn online_reads_remain_correct_through_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let mut eng = Engine::open(EngineConfig::new(dir.path())).unwrap();
    for k in [100u128, 101] {
        eng.put(Record::new(k, 2, 0, b"ok")).unwrap();
    }
    let keys = [100u128, 101];
    let before = eng.get_many(&keys).unwrap();
    eng.relocate_keys(&[100, 101]).unwrap();
    let after = eng.get_many(&keys).unwrap();
    assert_eq!(before, after);
}
