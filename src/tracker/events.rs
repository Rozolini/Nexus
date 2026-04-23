//! Co-read event emission at query boundaries.
//!
//! Plan: bounded policy + deterministic aggregation (dedupe, sort, truncate keys;
//! downsample pairs to a lexicographic prefix of the pair sequence).

use std::collections::BTreeSet;

use crate::config::ReadTrackingConfig;
use crate::types::Key;

/// One logical query: keys read together, after deterministic aggregation.
#[derive(Debug, Clone, PartialEq)]
pub struct CoReadQuery {
    pub keys: Vec<Key>,
}

/// Emitted co-read event (plan: co-read event emission).
#[derive(Debug, Clone, PartialEq)]
pub enum CoReadEvent {
    Query(CoReadQuery),
}

impl CoReadQuery {
    /// Dedupe → sort → truncate to `max_keys_per_session` (deterministic aggregation).
    /// Returns `None` if tracking disabled or fewer than two keys remain.
    pub fn aggregate_bounded(raw: &[Key], cfg: &ReadTrackingConfig) -> Option<Self> {
        if !cfg.enabled {
            return None;
        }
        let mut uniq: Vec<Key> = raw
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        uniq.sort_unstable();
        if uniq.len() > cfg.max_keys_per_session {
            uniq.truncate(cfg.max_keys_per_session);
        }
        if uniq.len() < 2 {
            return None;
        }
        Some(CoReadQuery { keys: uniq })
    }
}

/// Deterministic downsample: take the first `max_pairs` undirected pairs in lex order `(i, j), i < j`
/// over the key slice. **Input must be sorted ascending** (as produced by [`CoReadQuery::aggregate_bounded`]).
pub fn deterministic_pair_downsample(keys: &[Key], max_pairs: usize) -> Vec<(Key, Key)> {
    let mut out = Vec::new();
    if keys.len() < 2 {
        return out;
    }
    'outer: for i in 0..keys.len() {
        for j in (i + 1)..keys.len() {
            if out.len() >= max_pairs {
                break 'outer;
            }
            out.push((keys[i], keys[j]));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ReadTrackingConfig;

    #[test]
    fn deterministic_pair_ordering() {
        let k = &[1u128, 2, 3];
        let p = deterministic_pair_downsample(k, 10);
        assert_eq!(p[0], (1, 2));
        assert_eq!(p[1], (1, 3));
        assert_eq!(p[2], (2, 3));
    }

    #[test]
    fn aggregate_bounded_respects_max_keys() {
        let cfg = ReadTrackingConfig {
            max_keys_per_session: 3,
            ..Default::default()
        };
        let raw: Vec<Key> = (0u128..10).collect();
        let q = CoReadQuery::aggregate_bounded(&raw, &cfg).unwrap();
        assert_eq!(q.keys.len(), 3);
        assert_eq!(q.keys, vec![0, 1, 2]);
    }
}
