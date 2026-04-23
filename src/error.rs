//! # Error model
//!
//! Every fallible operation in Nexus returns [`Result<T>`] — a type alias
//! for `std::result::Result<T, NexusError>`. `NexusError` is a single
//! enum that covers the full failure surface; we intentionally avoid
//! layered error types (no `EngineError` → `StorageError` → `IoError`
//! indirection) because the cost of that indirection far exceeds the
//! benefit for a single-crate library.
//!
//! Each variant carries the minimum context needed to diagnose the
//! problem: a file path for I/O errors, a segment offset for record
//! corruption, etc. There is no `Other(String)` escape hatch — add a
//! dedicated variant when you need one.

use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum NexusError {
    #[error("IO error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("corrupt record at offset {offset}: {reason}")]
    CorruptRecord { offset: u64, reason: String },

    #[error("corrupt segment footer in {path}: {reason}")]
    CorruptFooter { path: PathBuf, reason: String },

    #[error("invalid segment header in {path}")]
    InvalidSegmentHeader { path: PathBuf },

    #[error("manifest decode error: {0}")]
    ManifestDecode(String),

    #[error("injected fault: {0}")]
    InjectedFault(String),

    #[error("manifest version {0} is not supported")]
    UnsupportedManifestVersion(u32),

    #[error("engine data directory missing or not a directory: {0}")]
    InvalidDataDir(PathBuf),

    #[error("checksum mismatch at offset {offset}: expected {expected:#x}, got {got:#x}")]
    ChecksumMismatch {
        offset: u64,
        expected: u32,
        got: u32,
    },

    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, NexusError>;

impl NexusError {
    pub fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        NexusError::Io {
            path: path.into(),
            source,
        }
    }
}
