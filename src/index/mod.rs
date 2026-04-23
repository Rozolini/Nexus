//! Index structures.

pub mod lookup;
pub mod primary;
pub mod remap;

pub use primary::{newer_wins, IndexEntry, PrimaryIndex};
pub use remap::{apply_remap, RemapEntry};
