//! Per-cycle rewrite budgets.

use crate::config::SchedulerConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveBudget {
    pub max_bytes: u64,
    pub max_groups: usize,
}

pub fn effective_budget(cfg: &SchedulerConfig) -> EffectiveBudget {
    let share = cfg.max_background_cpu_share.clamp(0.0, 1.0);
    let max_bytes = (cfg.max_bytes_rewritten_per_cycle as f64 * share) as u64;
    let max_groups = (cfg.max_groups_relocated_per_cycle as f64 * share).floor() as usize;
    EffectiveBudget {
        max_bytes,
        max_groups,
    }
}

/// Returns false when another relocation would exceed byte budget.
pub fn budget_allows_reloc(
    budget: &EffectiveBudget,
    bytes_so_far: u64,
    groups_so_far: usize,
    next_bytes: u64,
) -> bool {
    if budget.max_groups == 0 {
        return false;
    }
    if groups_so_far >= budget.max_groups {
        return false;
    }
    bytes_so_far.saturating_add(next_bytes) <= budget.max_bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SchedulerConfig;

    fn cfg(bytes: u64, groups: usize, share: f64) -> SchedulerConfig {
        SchedulerConfig {
            max_bytes_rewritten_per_cycle: bytes,
            max_groups_relocated_per_cycle: groups,
            max_background_cpu_share: share,
            ..SchedulerConfig::default()
        }
    }

    #[test]
    fn budget_cutoff_logic() {
        let b = effective_budget(&cfg(1000, 5, 0.5));
        assert_eq!(b.max_bytes, 500);
        assert_eq!(b.max_groups, 2);
        assert!(budget_allows_reloc(&b, 0, 0, 400));
        assert!(!budget_allows_reloc(&b, 400, 0, 200));
        assert!(!budget_allows_reloc(&b, 0, 2, 1));
    }
}
