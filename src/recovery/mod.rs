//! # Recovery
//!
//! Startup-time sanity checks and cleanup. Runs once per `Engine::open`
//! before the engine accepts any new writes.
//!
//! ## Orphan segments
//!
//! A segment file is **orphan** if it exists on disk but is not listed in
//! the current manifest. Orphans are the normal outcome of a crash during
//! relocation/compaction: the new segments were partially written but the
//! manifest swap did not complete. We must neither replay them (they would
//! produce a stale history) nor silently leak them (they are dead disk
//! space).
//!
//! ### Policy
//!
//! * Orphan files are **detected and reported** in
//!   [`StartupRecoveryReport`] so operators can audit recovery behaviour.
//! * Empty orphans (0 bytes, tmp leftovers) are **removed** automatically —
//!   they cannot contain data.
//! * Non-empty orphans are **kept on disk** for forensic inspection. Future
//!   versions may move them to a `quarantine/` subdirectory.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::storage::manifest::Manifest;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StartupRecoveryReport {
    pub manifest_generation: u64,
    pub segments_replayed: usize,
    /// Orphan `.seg` files on disk not listed in the manifest (before cleanup).
    pub orphan_files: Vec<PathBuf>,
    /// Empty orphan segment files removed during startup (safe cleanup).
    pub empty_orphan_segments_removed: Vec<PathBuf>,
    pub checksum_records_verified: u64,
}

/// Segment files present on disk but not listed in the manifest (e.g. crash before install).
pub fn list_orphan_segment_files(dir: &Path, manifest: &Manifest) -> Result<Vec<PathBuf>> {
    let listed: HashSet<String> = manifest.segments.iter().cloned().collect();
    let mut out = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().and_then(|x| x.to_str()) != Some("seg") {
                continue;
            }
            let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !listed.contains(name) {
                out.push(p);
            }
        }
    }
    out.sort_by_key(|p| p.display().to_string());
    Ok(out)
}

/// Safe cleanup: only removes orphan files that are empty (0 bytes). Returns removed paths.
pub fn cleanup_empty_orphan_segments(dir: &Path, manifest: &Manifest) -> Result<Vec<PathBuf>> {
    let orphans = list_orphan_segment_files(dir, manifest)?;
    let mut removed = Vec::new();
    for p in orphans {
        let len = fs::metadata(&p)
            .map_err(|e| crate::error::NexusError::io(&p, e))?
            .len();
        if len == 0 {
            fs::remove_file(&p).map_err(|e| crate::error::NexusError::io(&p, e))?;
            removed.push(p);
        }
    }
    Ok(removed)
}

// Touches the real filesystem; skipped under Miri.
#[cfg(all(test, not(miri)))]
mod tests {
    use std::fs::{self, File};

    use tempfile::tempdir;

    use super::*;
    use crate::storage::manifest::Manifest;

    #[test]
    fn orphan_cleanup_safety() {
        let dir = tempdir().unwrap();
        let m = Manifest {
            version: 1,
            generation: 0,
            segments: vec![],
        };
        File::create(dir.path().join("00000000000000000001.seg")).unwrap();
        fs::write(dir.path().join("00000000000000000002.seg"), b"payload").unwrap();
        let removed = cleanup_empty_orphan_segments(dir.path(), &m).unwrap();
        assert_eq!(removed.len(), 1);
        assert!(!dir.path().join("00000000000000000001.seg").exists());
        assert!(dir.path().join("00000000000000000002.seg").exists());
    }
}
