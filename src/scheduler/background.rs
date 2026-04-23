//! # Background scheduler
//!
//! The scheduler is the adaptive feedback loop that turns observed access
//! patterns into on-disk locality. Each `run_cycle` call:
//!
//! 1. Consults [`crate::scheduler::triggers`] to decide whether *any* work
//!    should run this cycle (budget, cooldown, minimum-evidence checks).
//! 2. Asks the [`crate::planner`] for a [`LayoutPlan`] — a list of
//!    [`GroupPlan`]s, each proposing which keys should be co-located.
//! 3. For every group that passes cooldown + gain threshold + "not already
//!    colocated" guard, calls [`Engine::relocate_group`] to physically
//!    rewrite the records into a single destination segment.
//! 4. Updates per-key and per-group cooldown maps so the same group isn't
//!    re-scheduled on the next cycle.
//!
//! Why the "already colocated" guard exists: without it, SkewedZipfian
//! workloads suffer because hot keys that are already co-located get
//! *re-*located every cycle (the planner's gain score happily stays
//! positive even when the physical layout is fine). The guard skips those
//! groups and was shown empirically to eliminate the regression.
//!
//! The scheduler is a *library* type — the caller owns the cadence. In
//! tests we drive it synchronously (`scheduler.run_cycle(&mut engine, …)`).
//! A production embedding would call `run_cycle` on a background thread.
//!
//! [`LayoutPlan`]: crate::planner::LayoutPlan
//! [`Engine::relocate_group`]: crate::engine::Engine::relocate_group

use std::collections::HashMap;

use crate::config::SchedulerConfig;
use crate::engine::Engine;
use crate::error::Result;
use crate::graph::CoAccessGraph;
use crate::planner::{build_layout_plan, GroupPlan, PlannerConfig};
use crate::scheduler::budget::{budget_allows_reloc, effective_budget};
use crate::scheduler::triggers::{should_schedule_work, TriggerInputs, TriggerKind};
use crate::types::Key;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SchedulerReport {
    pub cycle: u64,
    pub ran: bool,
    pub primary_trigger: Option<TriggerKind>,
    pub bytes_rewritten: u64,
    pub groups_relocated: usize,
    pub skipped_cooldown: usize,
    pub skipped_low_gain: usize,
    pub budget_cutoff: bool,
    pub logical_bytes_estimated: u64,
}

pub struct BackgroundScheduler {
    pub config: SchedulerConfig,
    cycle: u64,
    key_next_reloc_cycle: HashMap<Key, u64>,
    group_next_reloc_cycle: HashMap<u32, u64>,
    pub total_bytes_rewritten: u64,
    pub total_groups_relocated: u64,
}

impl BackgroundScheduler {
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            config,
            cycle: 0,
            key_next_reloc_cycle: HashMap::new(),
            group_next_reloc_cycle: HashMap::new(),
            total_bytes_rewritten: 0,
            total_groups_relocated: 0,
        }
    }

    /// One bounded scheduler iteration: evaluate triggers, apply budgets, optional relocations.
    pub fn run_cycle(
        &mut self,
        engine: &mut Engine,
        graph: &CoAccessGraph,
        planner_cfg: &PlannerConfig,
        key_sizes: &HashMap<Key, u64>,
        max_graph_edges: usize,
    ) -> Result<SchedulerReport> {
        self.cycle += 1;
        let sizes = build_key_sizes(engine, key_sizes);
        let plan = build_layout_plan(graph, &sizes, planner_cfg);
        let total_gain = plan.total_expected_gain();

        let inputs = TriggerInputs {
            edge_count: graph.edge_count(),
            max_graph_edges,
            segment_count: engine.segment_count(),
            index_key_count: engine.index_len(),
            plan_total_gain: total_gain,
        };

        let primary = should_schedule_work(&self.config, &inputs);
        let Some(trigger) = primary else {
            return Ok(SchedulerReport {
                cycle: self.cycle,
                ran: false,
                primary_trigger: None,
                ..Default::default()
            });
        };

        let eff = effective_budget(&self.config);
        if eff.max_groups == 0 || eff.max_bytes == 0 {
            return Ok(SchedulerReport {
                cycle: self.cycle,
                ran: true,
                primary_trigger: Some(trigger),
                ..Default::default()
            });
        }

        let mut groups: Vec<GroupPlan> = plan.groups;
        groups.sort_by(|a, b| {
            b.expected_gain
                .partial_cmp(&a.expected_gain)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut bytes_so_far = 0u64;
        let mut groups_so_far = 0usize;
        let mut bytes_total = 0u64;
        let mut logical_total = 0u64;
        let mut skipped_cd = 0usize;
        let mut skipped_gain = 0usize;
        let mut budget_hit = false;

        for g in groups {
            if g.expected_gain < self.config.minimum_improvement_delta {
                skipped_gain += 1;
                continue;
            }
            if !self.group_cooldown_allows(&g) {
                skipped_cd += 1;
                continue;
            }
            // Already-colocated guard (SkewedZipfian fix): if **all** the group's keys already live
            // in the same segment, relocating fragments layout further by spawning a fresh
            // destination segment and breaking the natural locality that already exists.
            // The 1.0 threshold keeps it strictly safe — any real planner-intended merge
            // of keys spread across ≥2 segments still proceeds.
            if engine.group_already_colocated(&g.keys, 1.0) {
                skipped_gain += 1;
                continue;
            }
            let est = group_estimated_bytes(&g, &sizes);
            if !budget_allows_reloc(&eff, bytes_so_far, groups_so_far, est) {
                budget_hit = true;
                break;
            }
            let meta = engine.relocate_group(&g)?;
            // Use actual bytes written when the engine reports them;
            // fall back to planner estimate for budgets if unavailable (never 0 on healthy path).
            let actual = if meta.bytes_written > 0 {
                meta.bytes_written
            } else {
                est
            };
            bytes_so_far += actual;
            groups_so_far += 1;
            bytes_total += actual;
            logical_total += est;
            self.mark_cooldowns(&g);
            self.total_bytes_rewritten += actual;
            self.total_groups_relocated += 1;
        }

        Ok(SchedulerReport {
            cycle: self.cycle,
            ran: true,
            primary_trigger: Some(trigger),
            bytes_rewritten: bytes_total,
            groups_relocated: groups_so_far,
            skipped_cooldown: skipped_cd,
            skipped_low_gain: skipped_gain,
            budget_cutoff: budget_hit,
            logical_bytes_estimated: logical_total,
        })
    }

    fn group_cooldown_allows(&self, g: &GroupPlan) -> bool {
        let gn = self
            .group_next_reloc_cycle
            .get(&g.group_id)
            .copied()
            .unwrap_or(0);
        if self.cycle < gn {
            return false;
        }
        for &k in &g.keys {
            let kn = self.key_next_reloc_cycle.get(&k).copied().unwrap_or(0);
            if self.cycle < kn {
                return false;
            }
        }
        true
    }

    fn mark_cooldowns(&mut self, g: &GroupPlan) {
        let nk = self.cycle + self.config.cooldown_cycles_per_key;
        let ng = self.cycle + self.config.cooldown_cycles_per_group;
        for &k in &g.keys {
            self.key_next_reloc_cycle.insert(k, nk);
        }
        self.group_next_reloc_cycle.insert(g.group_id, ng);
    }
}

fn build_key_sizes(engine: &Engine, provided: &HashMap<Key, u64>) -> HashMap<Key, u64> {
    if !provided.is_empty() {
        return provided.clone();
    }
    engine.iter_index().map(|(k, _)| (*k, 8u64)).collect()
}

fn group_estimated_bytes(g: &GroupPlan, sizes: &HashMap<Key, u64>) -> u64 {
    g.keys
        .iter()
        .map(|k| sizes.get(k).copied().unwrap_or(8u64))
        .sum()
}

// The scheduler tests spin up an entire Engine against a temp dir. Miri
// cannot simulate FS syscalls, so the module is gated off under Miri.
#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use crate::config::EngineConfig;
    use crate::graph::CoAccessGraph;
    use crate::planner::PlannerConfig;
    use crate::storage::record::Record;
    use tempfile::tempdir;

    #[test]
    fn cooldown_enforcement() {
        let dir = tempdir().unwrap();
        let mut cfg = EngineConfig::new(dir.path());
        cfg.scheduler.cooldown_cycles_per_key = 100;
        cfg.scheduler.cooldown_cycles_per_group = 100;
        cfg.scheduler.max_bytes_rewritten_per_cycle = 10 * 1024 * 1024;
        cfg.scheduler.max_groups_relocated_per_cycle = 10;
        cfg.scheduler.max_background_cpu_share = 1.0;
        cfg.scheduler.graph_pressure_edge_ratio_threshold = 0.0;
        cfg.scheduler.fragmentation_segments_threshold = 0;
        cfg.scheduler.locality_gain_threshold = 0.0;
        cfg.scheduler.minimum_improvement_delta = 0.0;

        let mut eng = Engine::open(cfg.clone()).unwrap();
        eng.put(Record::new(1u128, 1, 0, b"a")).unwrap();
        // Put keys in DIFFERENT segments so the already-colocated guard
        // does not short-circuit relocation.
        eng.rotate_segment().unwrap();
        eng.put(Record::new(2u128, 1, 0, b"b")).unwrap();
        let mut graph = CoAccessGraph::new(1000);
        graph.add_weight(1, 2, 50.0);
        let planner = PlannerConfig {
            rewrite_affinity_threshold: 0.01,
            min_expected_gain: 0.0,
            hysteresis_per_key: 0.0,
            ..Default::default()
        };

        let mut sched = BackgroundScheduler::new(cfg.scheduler.clone());
        let ks: HashMap<u128, u64> = [(1u128, 10), (2, 10)].into_iter().collect();
        let r1 = sched
            .run_cycle(&mut eng, &graph, &planner, &ks, 500_000)
            .unwrap();
        assert!(r1.groups_relocated > 0, "expected first cycle to relocate");
        let r2 = sched
            .run_cycle(&mut eng, &graph, &planner, &ks, 500_000)
            .unwrap();
        assert!(r2.skipped_cooldown > 0);
    }
}
