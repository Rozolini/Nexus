//! # On-disk storage
//!
//! Nexus stores data in append-only **segment files** (`0000…N.seg`), each
//! wrapping an opaque byte stream of encoded records. A **manifest** file
//! tracks which segments are live, in what order they should be replayed,
//! and at what generation. All filesystem mutations go through this
//! module — nothing higher up in the stack calls `std::fs` directly for
//! segment or manifest paths.
//!
//! ## Submodules
//!
//! * [`block`]            — fixed-size block helpers for buffered I/O.
//! * [`record`]           — the runtime `Record` struct (key/value/tombstone).
//! * [`segment`]          — segment-file naming and helper utilities.
//! * [`segment_index`]    — the per-segment offset index used by replay.
//! * [`segment_reader`]   — buffered, checksum-verifying reader.
//! * [`segment_writer`]   — append-only writer with fsync-on-close semantics.
//! * [`manifest`]         — atomic manifest rotation (`tmp` → `rename`).
//! * [`gc`]               — garbage-collection helper that removes segment files
//!   no longer referenced by the manifest.
//!
//! ## Crash safety
//!
//! All mutations follow a "write-then-link" pattern: data lands in a
//! temporary file, is `fsync`-ed, and then `rename`-ed into place. Manifest
//! rotation is atomic on POSIX and on Windows (via `MoveFileExA`
//! `MOVEFILE_REPLACE_EXISTING`).

pub mod block;
pub mod gc;
pub mod manifest;
pub mod record;
pub mod segment;
pub mod segment_index;
pub mod segment_reader;
pub mod segment_writer;
