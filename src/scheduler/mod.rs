//! # Scheduler
//!
//! Orchestrates the adaptation loop:
//!
//! ```text
//!   co-access events ──► CoAccessGraph ──► LayoutPlan ──► relocations
//! ```
//!
//! The scheduler is **not** a thread. It is a stateful object with a
//! single entry point, [`BackgroundScheduler::run_cycle`], that the caller
//! invokes at whatever cadence they like. This keeps the scheduler
//! test-friendly (no timing non-determinism) and lets embedders integrate
//! with their own task runtime.
//!
//! ## Submodules
//!
//! * [`background`] — the scheduler loop itself, cooldown bookkeeping, and
//!   the "already colocated" guard.
//! * [`budget`]     — per-cycle rewrite-budget calculator (bytes to rewrite
//!   this cycle, bytes rewritten so far, hard caps).
//! * [`triggers`]   — decision function that says *whether* a cycle should
//!   run, and *why* (minimum evidence, backoff, cooldown elapsed, etc.).

pub mod background;
pub mod budget;
pub mod triggers;

pub use background::{BackgroundScheduler, SchedulerReport};
pub use budget::{budget_allows_reloc, effective_budget, EffectiveBudget};
pub use triggers::{
    highest_priority_trigger, should_schedule_work, triggers_fired, TriggerInputs, TriggerKind,
};
