//! Atomic manifest with generation and backup.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{NexusError, Result};

pub const MANIFEST_FILE: &str = "MANIFEST.json";
pub const MANIFEST_BACKUP_FILE: &str = "MANIFEST.json.bak";
const MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub version: u32,
    #[serde(default)]
    pub generation: u64,
    pub segments: Vec<String>,
}

impl Manifest {
    pub fn empty() -> Self {
        Self {
            version: MANIFEST_VERSION,
            generation: 0,
            segments: Vec::new(),
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let mut f = File::open(path).map_err(|e| NexusError::io(path, e))?;
        let mut s = String::new();
        f.read_to_string(&mut s)
            .map_err(|e| NexusError::io(path, e))?;
        let m: Manifest =
            serde_json::from_str(&s).map_err(|e| NexusError::ManifestDecode(e.to_string()))?;
        if m.version != MANIFEST_VERSION {
            return Err(NexusError::UnsupportedManifestVersion(m.version));
        }
        Ok(m)
    }

    /// Load `MANIFEST.json` and `MANIFEST.json.bak` when present; use [`Manifest::pick_newer`]
    /// when both decode. If only one decodes, use it. Truncated/corrupt primary still allows backup.
    pub fn load_robust(dir: &Path) -> Result<Manifest> {
        let primary_path = dir.join(MANIFEST_FILE);
        let bak_path = dir.join(MANIFEST_BACKUP_FILE);

        let primary = if primary_path.exists() {
            Self::load(&primary_path).ok()
        } else {
            None
        };
        let backup = if bak_path.exists() {
            Self::load(&bak_path).ok()
        } else {
            None
        };

        match (primary, backup) {
            (Some(a), Some(b)) => Ok(Self::pick_newer(&a, &b)),
            (Some(m), None) | (None, Some(m)) => Ok(m),
            (None, None) => Ok(Self::empty()),
        }
    }

    /// Select the manifest with the higher `generation` (tie-break: more segments).
    pub fn pick_newer(a: &Manifest, b: &Manifest) -> Manifest {
        if a.generation > b.generation {
            return a.clone();
        }
        if b.generation > a.generation {
            return b.clone();
        }
        if a.segments.len() >= b.segments.len() {
            a.clone()
        } else {
            b.clone()
        }
    }

    /// Write atomically; backs up existing primary to `.bak` before replace.
    pub fn save_atomic(dir: &Path, manifest: &Manifest) -> Result<PathBuf> {
        if crate::fault::take_fail_before_manifest_save() {
            return Err(NexusError::InjectedFault("before manifest save".into()));
        }
        let final_path = dir.join(MANIFEST_FILE);
        let tmp_path = dir.join("MANIFEST.json.tmp");
        let bak_path = dir.join(MANIFEST_BACKUP_FILE);
        if final_path.exists() {
            let _ = fs::copy(&final_path, &bak_path);
        }
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)
                .map_err(|e| NexusError::io(&tmp_path, e))?;
            let json = serde_json::to_string_pretty(manifest)
                .map_err(|e| NexusError::ManifestDecode(e.to_string()))?;
            f.write_all(json.as_bytes())
                .map_err(|e| NexusError::io(&tmp_path, e))?;
            f.sync_all().map_err(|e| NexusError::io(&tmp_path, e))?;
        }
        replace_atomic(&tmp_path, &final_path)?;
        Ok(final_path)
    }
}

fn replace_atomic(src: &Path, dst: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        if dst.exists() {
            std::fs::remove_file(dst).map_err(|e| NexusError::io(dst, e))?;
        }
        std::fs::rename(src, dst).map_err(|e| NexusError::io(dst, e))?;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        std::fs::rename(src, dst).map_err(|e| NexusError::io(dst, e))?;
        Ok(())
    }
}

// Miri cannot simulate host-FS syscalls; skip inline tests that write real
// files. These paths are covered by integration tests in `tests/` under
// the normal stable-Rust workflow.
#[cfg(all(test, not(miri)))]
mod tests {
    use std::fs;

    use super::*;
    use tempfile::tempdir;

    #[test]
    fn atomic_swap_replaces_previous() {
        let dir = tempdir().unwrap();
        let m1 = Manifest {
            version: 1,
            generation: 1,
            segments: vec!["a.seg".into()],
        };
        Manifest::save_atomic(dir.path(), &m1).unwrap();
        let m2 = Manifest {
            version: 1,
            generation: 2,
            segments: vec!["a.seg".into(), "b.seg".into()],
        };
        Manifest::save_atomic(dir.path(), &m2).unwrap();
        let loaded = Manifest::load(&dir.path().join(MANIFEST_FILE)).unwrap();
        assert_eq!(loaded.segments, m2.segments);
        assert_eq!(loaded.generation, 2);
    }

    #[test]
    fn generation_selection() {
        let a = Manifest {
            version: 1,
            generation: 2,
            segments: vec!["x.seg".into()],
        };
        let b = Manifest {
            version: 1,
            generation: 5,
            segments: vec!["y.seg".into()],
        };
        let p = Manifest::pick_newer(&a, &b);
        assert_eq!(p.generation, 5);
    }

    #[test]
    fn corrupted_manifest_fallback() {
        let dir = tempdir().unwrap();
        let good = Manifest {
            version: 1,
            generation: 7,
            segments: vec!["ok.seg".into()],
        };
        let json = serde_json::to_string_pretty(&good).unwrap();
        fs::write(dir.path().join(MANIFEST_BACKUP_FILE), &json).unwrap();
        fs::write(dir.path().join(MANIFEST_FILE), b"{ not json").unwrap();
        let m = Manifest::load_robust(dir.path()).unwrap();
        assert_eq!(m.generation, 7);
        assert_eq!(m.segments, vec!["ok.seg"]);
    }

    #[test]
    fn truncated_manifest_fallback() {
        let dir = tempdir().unwrap();
        let good = Manifest {
            version: 1,
            generation: 3,
            segments: vec!["x.seg".into()],
        };
        let json = serde_json::to_string_pretty(&good).unwrap();
        fs::write(dir.path().join(MANIFEST_BACKUP_FILE), &json).unwrap();
        let trunc = &json.as_bytes()[..json.len().saturating_sub(4)];
        fs::write(dir.path().join(MANIFEST_FILE), trunc).unwrap();
        let m = Manifest::load_robust(dir.path()).unwrap();
        assert_eq!(m, good);
    }

    #[test]
    fn load_robust_picks_newer_when_both_primary_and_backup_valid() {
        let dir = tempdir().unwrap();
        let older = Manifest {
            version: 1,
            generation: 2,
            segments: vec!["a.seg".into()],
        };
        let newer = Manifest {
            version: 1,
            generation: 9,
            segments: vec!["a.seg".into(), "b.seg".into()],
        };
        fs::write(
            dir.path().join(MANIFEST_FILE),
            serde_json::to_string_pretty(&older).unwrap(),
        )
        .unwrap();
        fs::write(
            dir.path().join(MANIFEST_BACKUP_FILE),
            serde_json::to_string_pretty(&newer).unwrap(),
        )
        .unwrap();
        let m = Manifest::load_robust(dir.path()).unwrap();
        assert_eq!(m.generation, 9);
        assert_eq!(m.segments, vec!["a.seg", "b.seg"]);
    }
}
