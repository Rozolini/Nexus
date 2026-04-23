//! # Co-access graph
//!
//! Weighted undirected graph of keys that were read together. An edge
//! `(a, b)` with weight `w` means "keys `a` and `b` were observed in the
//! same read session, with a Jaccard-normalised contribution totalling
//! `w` after time-decay."
//!
//! The graph is **not** a cache — it's a statistical summary. Weights
//! decay each tick (see [`apply_decay`]) so stale co-accesses fade out
//! naturally and the planner always operates on a "recent enough" view.
//!
//! Keys in the graph are stored in a canonical order (`min(a, b)`, `max(a, b)`)
//! by [`normalize_pair`] so that symmetrical edges are never duplicated.
//!
//! ## Submodules
//!
//! * [`coaccess`] — the `CoAccessGraph` struct and its bump/merge API.
//! * [`decay`]    — time-decay implementation.
//! * [`scoring`]  — per-edge score helpers (used by the planner).
//! * [`snapshot`] — cheap copy-out for deterministic planning.

pub mod coaccess;
pub mod decay;
pub mod scoring;
pub mod snapshot;

pub use coaccess::{normalize_pair, CoAccessGraph};
pub use decay::apply_decay;
pub use snapshot::{GraphSnapshot, SnapshotEdge};
