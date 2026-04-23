//! # Compaction and relocation
//!
//! Executes a [`crate::planner::LayoutPlan`] by physically rewriting
//! records into new segment files. Two execution modes share almost all
//! code:
//!
//! * **Relocation** — targeted rewrite of a specific group of keys into a
//!   single destination segment. Used by the background scheduler.
//! * **Compaction** — space-reclamation rewrite of a segment that has
//!   accumulated too many dead records.
//!
//! ## Atomic installation
//!
//! New segments are staged under a temporary name and then published via
//! [`install::install_segments_atomic`], which updates the manifest and
//! file list in one step. On crash, [`crate::recovery`] observes any
//! half-written segments as orphans and cleans them up before the engine
//! accepts new writes.
//!
//! ## Safety rails
//!
//! * [`safety::validate_record_for_rewrite`] verifies every source record
//!   has a matching index entry before it is copied into a new segment.
//!   This is the last line of defence against silently rewriting stale
//!   records.
//! * [`relocation::select_live_records_for_keys`] gracefully skips keys
//!   that were evicted from the index between planning and execution
//!   (a legitimate race), rather than aborting the cycle.

pub mod compactor;
pub mod install;
pub mod relocation;
pub mod rewrite_policy;
pub mod safety;

pub use install::install_segments_atomic;
pub use relocation::{collect_source_segments, select_live_records_for_keys, RelocationMetadata};
pub use safety::validate_record_for_rewrite;
