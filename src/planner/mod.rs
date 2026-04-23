//! # Layout planner
//!
//! Given a decayed co-access graph from [`crate::graph`] and the current
//! segment assignment (from [`crate::index::primary::PrimaryIndex`]), the
//! planner produces a [`LayoutPlan`] — a ranked list of [`GroupPlan`]s
//! describing which keys should be co-located.
//!
//! Design goals:
//!
//! * **Deterministic.** Identical graph + identical index ⇒ identical plan.
//!   This is essential for reproducible benchmarks and test assertions.
//! * **Incremental.** The planner only proposes groups whose expected
//!   locality gain exceeds [`PlannerConfig::min_gain_threshold`]. Anything
//!   below is dropped before reaching the scheduler.
//! * **Bounded.** Groups are capped by [`PlannerConfig::max_keys_per_group`]
//!   so a single pathological hot key cannot pull the entire key space into
//!   one segment.
//!
//! The planner is **pure** — no I/O, no engine mutation. It takes snapshots
//! and returns a plan. Executing the plan is the job of the scheduler +
//! compaction layer.

pub mod affinity;
pub mod entry;
pub mod grouping;
pub mod plan;
pub mod thresholds;

pub use affinity::{affinity, normalization_factor};
pub use entry::LayoutPlanner;
pub use grouping::build_layout_plan;
pub use plan::{GroupPlan, LayoutPlan};
pub use thresholds::PlannerConfig;
