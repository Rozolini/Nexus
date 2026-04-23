//! Manifest install for relocation.

use std::path::Path;

use crate::error::Result;
use crate::storage::manifest::{Manifest, MANIFEST_FILE};

pub fn manifest_path(dir: &Path) -> std::path::PathBuf {
    dir.join(MANIFEST_FILE)
}

pub fn install_segments_atomic(dir: &Path, segment_names: &[String]) -> Result<()> {
    let m = Manifest {
        version: 1,
        generation: 0,
        segments: segment_names.to_vec(),
    };
    Manifest::save_atomic(dir, &m)?;
    Ok(())
}

// Uses `tempfile::tempdir` to drive real on-disk manifest rotation;
// Miri cannot interpret host-FS syscalls so we gate the module with
// `not(miri)`. Coverage is preserved by `tests/` integration tests.
#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use crate::storage::manifest::Manifest;
    use tempfile::tempdir;

    #[test]
    fn install_atomicity_model() {
        let dir = tempdir().unwrap();
        install_segments_atomic(dir.path(), &["a.seg".into()]).unwrap();
        install_segments_atomic(dir.path(), &["a.seg".into(), "b.seg".into()]).unwrap();
        let m = Manifest::load(&manifest_path(dir.path())).unwrap();
        assert_eq!(m.segments, vec!["a.seg", "b.seg"]);
    }
}
