//! Planner entry.

use std::collections::HashMap;

use crate::graph::CoAccessGraph;
use crate::planner::grouping::build_layout_plan;
use crate::planner::plan::LayoutPlan;
use crate::planner::thresholds::PlannerConfig;
use crate::types::Key;

#[derive(Debug, Clone, Default)]
pub struct LayoutPlanner {
    pub config: PlannerConfig,
}

impl LayoutPlanner {
    pub fn new(config: PlannerConfig) -> Self {
        Self { config }
    }

    pub fn plan(&self, graph: &CoAccessGraph, key_bytes: &HashMap<Key, u64>) -> LayoutPlan {
        build_layout_plan(graph, key_bytes, &self.config)
    }
}
