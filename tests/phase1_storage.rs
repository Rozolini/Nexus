//! Storage integration tests.

use std::fs;
use std::io::Write;

use nexus::config::EngineConfig;
use nexus::engine::Engine;
use nexus::error::NexusError;
use nexus::ids::SegmentId;
use nexus::storage::manifest::{Manifest, MANIFEST_FILE};
use nexus::storage::record::Record;
use nexus::storage::segment::recover_segment;
use nexus::storage::segment_writer::SegmentWriter;
use tempfile::tempdir;

#[test]
fn write_10k_reopen_read_all() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone()).unwrap();
        for i in 0..10_000u128 {
            let rec = Record::new(i, 1, 0, format!("v{}", i).into_bytes());
            eng.put(rec).unwrap();
        }
        eng.close().unwrap();
    }
    let eng = Engine::open(cfg).unwrap();
    for i in 0..10_000u128 {
        let g = eng.get(i).unwrap().expect("missing key");
        assert_eq!(g.payload, format!("v{}", i).as_bytes());
    }
}

#[test]
fn append_after_reopen() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone()).unwrap();
        eng.put(Record::new(1, 1, 0, b"a".to_vec())).unwrap();
        eng.close().unwrap();
    }
    {
        let mut eng = Engine::open(cfg.clone()).unwrap();
        eng.put(Record::new(2, 1, 0, b"b".to_vec())).unwrap();
        eng.close().unwrap();
    }
    let eng = Engine::open(cfg).unwrap();
    assert_eq!(eng.get(1).unwrap().unwrap().payload, b"a");
    assert_eq!(eng.get(2).unwrap().unwrap().payload, b"b");
}

#[test]
fn corrupted_tail_truncates_or_errors() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("00000000000000000001.seg");
    let mut w = SegmentWriter::create(&path, SegmentId(1)).unwrap();
    w.append(&Record::new(7, 1, 0, b"ok".to_vec())).unwrap();
    w.seal().unwrap();
    let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
    f.write_all(b"garbage").unwrap();
    drop(f);
    let r = recover_segment(&path);
    assert!(r.is_ok());
    let footer = r.unwrap();
    assert_eq!(footer.record_offsets.len(), 1);
}

#[test]
fn partial_segment_write_recovery_deterministic() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("00000000000000000001.seg");
    let mut w = SegmentWriter::create(&path, SegmentId(1)).unwrap();
    w.append(&Record::new(1, 1, 0, b"x".to_vec())).unwrap();
    w.seal().unwrap();
    let len = fs::metadata(&path).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(len - 3)
        .unwrap();
    let f1 = recover_segment(&path).unwrap();
    let f2 = recover_segment(&path).unwrap();
    assert_eq!(f1.record_offsets, f2.record_offsets);
}

#[test]
fn manifest_atomic_roundtrip() {
    let dir = tempdir().unwrap();
    let m = Manifest {
        version: 1,
        generation: 0,
        segments: vec!["00000000000000000001.seg".into()],
    };
    Manifest::save_atomic(dir.path(), &m).unwrap();
    let loaded = Manifest::load(&dir.path().join(MANIFEST_FILE)).unwrap();
    assert_eq!(loaded.segments, m.segments);
}

/// Corruption must surface as `Err`, not as a wrong payload (`Ok`).
/// Uses the same `Engine` after bit-flip so recovery does not truncate first.
#[test]
fn checksum_failure_is_not_silent_data_loss() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone()).unwrap();
        eng.put(Record::new(42, 1, 0, b"payload".to_vec())).unwrap();
        eng.close().unwrap();
    }
    let eng = Engine::open(cfg).unwrap();
    assert!(eng.get(42).unwrap().is_some());

    let seg = eng.segment_paths().last().unwrap().clone();
    let mut bytes = fs::read(&seg).unwrap();
    // Segment header 64 + record header 32 + first payload byte.
    let corrupt_at = 64 + 32;
    assert!(corrupt_at < bytes.len());
    bytes[corrupt_at] ^= 0x5a;
    fs::write(&seg, &bytes).unwrap();

    let err = eng.get(42).unwrap_err();
    match err {
        NexusError::ChecksumMismatch { .. } => {}
        e => panic!("expected checksum error, got {:?}", e),
    }
}
