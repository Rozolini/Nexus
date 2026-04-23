//! Layout plan types.

use serde::{Deserialize, Serialize};

use crate::types::Key;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupPlan {
    pub group_id: u32,
    pub keys: Vec<Key>,
    pub target_segment_class: u32,
    pub expected_gain: f64,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct LayoutPlan {
    pub groups: Vec<GroupPlan>,
}

impl LayoutPlan {
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    pub fn total_expected_gain(&self) -> f64 {
        self.groups.iter().map(|g| g.expected_gain).sum()
    }

    pub fn total_keys_in_plan(&self) -> usize {
        self.groups.iter().map(|g| g.keys.len()).sum()
    }

    pub fn keys_assigned(&self) -> impl Iterator<Item = Key> + '_ {
        self.groups.iter().flat_map(|g| g.keys.iter().copied())
    }

    pub fn has_overlapping_keys(&self) -> bool {
        let mut seen = std::collections::BTreeSet::new();
        for k in self.keys_assigned() {
            if !seen.insert(k) {
                return true;
            }
        }
        false
    }
}
