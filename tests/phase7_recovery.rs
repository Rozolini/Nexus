//! Recovery, consistency, hardening.

use nexus::fault;
use nexus::ids::SegmentId;
use nexus::storage::manifest::{Manifest, MANIFEST_FILE};
use nexus::storage::record::Record;
use nexus::storage::segment::segment_file_name;
use nexus::storage::segment_writer::SegmentWriter;
use nexus::{Engine, EngineConfig, Result};
use std::fs;

fn workload_a(cfg: &EngineConfig) -> Result<u64> {
    let mut eng = Engine::open(cfg.clone())?;
    for k in [1u128, 2, 3] {
        eng.put(Record::new(k, 1, 0, vec![k as u8]))?;
    }
    eng.close()?;
    let eng = Engine::open(cfg.clone())?;
    Ok(eng.layout_fingerprint())
}

/// Matrix: manifest save failure, record-append failure, compaction first-manifest failure.
#[test]
fn failure_injection_matrix() -> Result<()> {
    // 1) New segment: manifest persist fails before any record is appended.
    fault::reset_fault_injection();
    {
        let dir = tempfile::tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path());
        {
            let mut eng = Engine::open(cfg.clone())?;
            eng.put(Record::new(9u128, 1, 0, b"z"))?;
            eng.close()?;
        }
        fault::set_fail_before_manifest_save(true);
        {
            let mut eng = Engine::open(cfg.clone())?;
            let r = eng.put(Record::new(10u128, 1, 0, b"w"));
            assert!(r.is_err(), "expected manifest save injection");
        }
        fault::reset_fault_injection();
        let eng = Engine::open(cfg)?;
        assert_eq!(eng.get(9)?.unwrap().payload, b"z");
    }

    // 2) Mid-write: segment + manifest exist for new file, append fails.
    fault::reset_fault_injection();
    {
        let dir = tempfile::tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path());
        {
            let mut eng = Engine::open(cfg.clone())?;
            eng.put(Record::new(1u128, 1, 0, b"stable"))?;
            eng.close()?;
        }
        fault::set_fail_before_record_append(true);
        {
            let mut eng = Engine::open(cfg.clone())?;
            let r = eng.put(Record::new(2u128, 1, 0, b"lost"));
            assert!(r.is_err(), "expected append injection");
        }
        fault::reset_fault_injection();
        let eng = Engine::open(cfg)?;
        assert_eq!(eng.get(1)?.unwrap().payload, b"stable");
        assert!(eng.get(2)?.is_none());
    }

    // 3) Mid-compaction: sealed destination segment exists, first post-relocate manifest save fails.
    fault::reset_fault_injection();
    {
        let dir = tempfile::tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path());
        {
            let mut eng = Engine::open(cfg.clone())?;
            for k in [10u128, 11, 12] {
                eng.put(Record::new(k, 1, 0, vec![k as u8]))?;
            }
            eng.close()?;
        }
        fault::set_fail_before_relocate_first_manifest_save(true);
        {
            let mut eng = Engine::open(cfg.clone())?;
            let r = eng.relocate_keys(&[10, 11, 12]);
            assert!(r.is_err(), "expected relocate manifest injection");
        }
        fault::reset_fault_injection();
        let eng = Engine::open(cfg)?;
        for k in [10u128, 11, 12] {
            assert_eq!(eng.get(k)?.unwrap().payload, vec![k as u8]);
        }
    }

    Ok(())
}

#[test]
fn repeated_crash_recover_cycles() -> Result<()> {
    fault::reset_fault_injection();
    let dir = tempfile::tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    for _ in 0..15 {
        let mut eng = Engine::open(cfg.clone())?;
        eng.put(Record::new(77u128, 1, 0, b"k"))?;
        eng.close()?;
    }
    let eng = Engine::open(cfg)?;
    assert_eq!(eng.get(77)?.unwrap().payload, b"k");
    Ok(())
}

#[test]
fn startup_rebuild_from_segments() -> Result<()> {
    fault::reset_fault_injection();
    let dir = tempfile::tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone())?;
        eng.put(Record::new(5u128, 2, 0, b"q"))?;
        eng.close()?;
    }
    fs::remove_file(dir.path().join(MANIFEST_FILE)).unwrap();
    let eng = Engine::open(cfg)?;
    assert_eq!(eng.get(5)?.unwrap().payload, b"q");
    Ok(())
}

#[test]
fn replay_identical_workload_twice_identical_final_layout() -> Result<()> {
    fault::reset_fault_injection();
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let c1 = EngineConfig::new(d1.path());
    let c2 = EngineConfig::new(d2.path());
    let fp1 = workload_a(&c1)?;
    let fp2 = workload_a(&c2)?;
    assert_eq!(fp1, fp2);
    Ok(())
}

#[test]
fn gate_recovery_correctness_under_injected_faults() -> Result<()> {
    fault::reset_fault_injection();
    let dir = tempfile::tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone())?;
        eng.put(Record::new(1u128, 1, 0, b"a"))?;
        eng.close()?;
    }
    {
        let mut eng = Engine::open(cfg.clone())?;
        eng.put(Record::new(2u128, 1, 0, b"b"))?;
        eng.close()?;
    }
    fs::write(dir.path().join(MANIFEST_FILE), b"{").unwrap();
    let m = Manifest::load_robust(dir.path())?;
    assert!(!m.segments.is_empty());
    let eng = Engine::open(cfg)?;
    assert!(eng.get(1)?.is_some() || eng.get(2)?.is_some());
    Ok(())
}

#[test]
fn gate_deterministic_replay_passes() -> Result<()> {
    fault::reset_fault_injection();
    let fp = workload_a(&EngineConfig::new(tempfile::tempdir().unwrap().path()))?;
    let fp2 = workload_a(&EngineConfig::new(tempfile::tempdir().unwrap().path()))?;
    assert_eq!(fp, fp2);
    Ok(())
}

/// Failure case: crash after segment file exists + manifest lists it, before record bytes land.
#[test]
fn process_crash_mid_write_recovery() -> Result<()> {
    fault::reset_fault_injection();
    let dir = tempfile::tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone())?;
        eng.put(Record::new(100u128, 1, 0, b"keep"))?;
        eng.close()?;
    }
    fault::set_fail_before_record_append(true);
    {
        let mut eng = Engine::open(cfg.clone())?;
        assert!(eng.put(Record::new(101u128, 1, 0, b"drop")).is_err());
    }
    fault::reset_fault_injection();
    let eng = Engine::open(cfg)?;
    assert_eq!(eng.get(100)?.unwrap().payload, b"keep");
    assert!(eng.get(101)?.is_none());
    Ok(())
}

/// Failure case: sealed compacted segment on disk, manifest not yet updated (install not completed).
#[test]
fn process_crash_mid_compaction_recovery() -> Result<()> {
    fault::reset_fault_injection();
    let dir = tempfile::tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone())?;
        eng.put(Record::new(20u128, 1, 0, b"x"))?;
        eng.close()?;
    }
    fault::set_fail_before_relocate_first_manifest_save(true);
    {
        let mut eng = Engine::open(cfg.clone())?;
        assert!(eng.relocate_keys(&[20]).is_err());
    }
    fault::reset_fault_injection();
    let eng = Engine::open(cfg)?;
    assert_eq!(eng.get(20)?.unwrap().payload, b"x");
    Ok(())
}

/// Truncated primary manifest: covered in unit tests; integration sanity with backup.
#[test]
fn truncated_manifest_recovery_integration() -> Result<()> {
    fault::reset_fault_injection();
    let dir = tempfile::tempdir().unwrap();
    let good = Manifest {
        version: 1,
        generation: 4,
        segments: vec![],
    };
    let json = serde_json::to_string_pretty(&good).unwrap();
    fs::write(dir.path().join("MANIFEST.json.bak"), &json).unwrap();
    let trunc = &json.as_bytes()[..json.len().saturating_sub(3)];
    fs::write(dir.path().join(MANIFEST_FILE), trunc).unwrap();
    let m = Manifest::load_robust(dir.path())?;
    assert_eq!(m.generation, 4);
    Ok(())
}

/// Orphan segment file not listed in manifest is discovered and replayed (latest wins).
#[test]
fn orphaned_new_segment_not_yet_installed() -> Result<()> {
    fault::reset_fault_injection();
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

/// Index is rebuilt from segment replay on every open; fingerprint matches after manifest loss.
#[test]
fn stale_index_rebuild_on_open() -> Result<()> {
    fault::reset_fault_injection();
    let dir = tempfile::tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    let fp_after_write = {
        let mut eng = Engine::open(cfg.clone())?;
        eng.put(Record::new(42u128, 1, 0, b"v"))?;
        let fp = eng.layout_fingerprint();
        eng.close()?;
        fp
    };
    let eng = Engine::open(cfg.clone())?;
    assert_eq!(eng.layout_fingerprint(), fp_after_write);
    fs::remove_file(dir.path().join(MANIFEST_FILE)).unwrap();
    let eng2 = Engine::open(cfg)?;
    assert_eq!(eng2.layout_fingerprint(), fp_after_write);
    assert_eq!(eng2.get(42)?.unwrap().payload, b"v");
    Ok(())
}

/// Codec checksums are enforced while scanning segments at startup.
#[test]
fn checksum_verification_on_startup() -> Result<()> {
    fault::reset_fault_injection();
    let dir = tempfile::tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    let seg_path = {
        let mut eng = Engine::open(cfg.clone())?;
        eng.put(Record::new(7u128, 1, 0, b"ok"))?;
        eng.close()?;
        let name = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .find(|e| e.path().extension().and_then(|x| x.to_str()) == Some("seg"))
            .unwrap()
            .path();
        name
    };
    // Destroy sealed segment: startup replay must fail checksum / parse validation.
    fs::write(&seg_path, vec![0u8; 32]).unwrap();
    assert!(Engine::open(cfg).is_err());
    Ok(())
}

#[test]
fn startup_empty_orphan_cleanup_and_detailed_snapshot() -> Result<()> {
    fault::reset_fault_injection();
    let dir = tempfile::tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    {
        let mut eng = Engine::open(cfg.clone())?;
        eng.put(Record::new(1u128, 1, 0, b"a"))?;
        eng.close()?;
    }
    fs::write(dir.path().join("00000000000000000099.seg"), b"").unwrap();
    let eng = Engine::open(cfg)?;
    assert!(!eng
        .last_startup_recovery()
        .empty_orphan_segments_removed
        .is_empty());
    let s = eng.detailed_snapshot();
    assert!(s.startup_checksum_records_verified >= 1);
    assert!(s.startup_empty_orphan_segments_removed >= 1);
    Ok(())
}

#[test]
fn robust_recovery_flow_debug_dump() -> Result<()> {
    fault::reset_fault_injection();
    let dir = tempfile::tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    let mut eng = Engine::open(cfg.clone())?;
    eng.put(Record::new(1u128, 1, 0, b"x"))?;
    let dump = eng.debug_dump();
    assert!(dump.contains("manifest_generation"));
    assert!(dump.contains("startup_checksum_records"));
    eng.close()?;
    Ok(())
}
