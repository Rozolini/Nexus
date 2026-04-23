//! Read session: accumulates keys for a single query boundary (bounded).

use crate::types::Key;

/// Explicit read session — keys observed before [`ReadSession::finish`].
#[derive(Debug, Clone)]
pub struct ReadSession {
    keys: Vec<Key>,
    max_keys: usize,
}

impl ReadSession {
    pub fn new(max_keys: usize) -> Self {
        Self {
            keys: Vec::new(),
            max_keys,
        }
    }

    pub fn record_key(&mut self, key: Key) {
        if self.keys.len() < self.max_keys {
            self.keys.push(key);
        }
    }

    pub fn record_keys(&mut self, keys: &[Key]) {
        for &k in keys {
            self.record_key(k);
        }
    }

    pub fn keys(&self) -> &[Key] {
        &self.keys
    }

    /// Consumes session and returns deduplicated sorted keys (deterministic).
    pub fn finish(self) -> Vec<Key> {
        let cap = self.max_keys;
        let mut v: Vec<Key> = self
            .keys
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        v.sort_unstable();
        v.truncate(cap);
        v
    }
}
