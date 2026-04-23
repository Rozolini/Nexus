//! # Primary index
//!
//! Hash map from [`Key`] to [`IndexEntry`] pointing at the **latest**
//! version of that key on disk. Fully in-memory; rebuilt on startup by
//! replaying segments in manifest order.
//!
//! ## Tie-break rule
//!
//! Replay does not observe records in write order: a segment rewrite
//! (relocation) produces *new* records with the *same* `(key, version)` as
//! the originals. We therefore resolve ties using the deterministic
//! lexicographic order `(segment_id, offset)` — see [`newer_wins`]. This
//! makes recovery idempotent regardless of the manifest's segment ordering.
//!
//! ## Entry layout
//!
//! [`IndexEntry`] stores the full wire length (`size`) in addition to the
//! physical coordinate. This is what allows the range-merging reader to
//! decide mergeability without peeking the record header first.
//! Adding `size` did not change the on-disk format — the index is rebuilt
//! from disk every startup.

use std::collections::HashMap;

use crate::codec::record_wire_len;
use crate::ids::SegmentId;
use crate::storage::record::Record;
use crate::types::Key;

/// Resolved location of the latest stored record for a key (may be a tombstone).
///
/// `size` is the full wire length (`header + payload + crc`) — populated by [`PrimaryIndex::apply`]
/// and used by the range-merging batch reader to form contiguous `ReadRange`s
/// without first peeking the record header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexEntry {
    pub segment_id: SegmentId,
    pub offset: u64,
    pub version: u64,
    pub size: u32,
}

/// Whether `new` supersedes `old` as the latest version (deterministic tie-break).
#[inline]
pub fn newer_wins(
    old: Option<&IndexEntry>,
    new_version: u64,
    new_seg: SegmentId,
    new_off: u64,
) -> bool {
    match old {
        None => true,
        Some(e) => {
            if new_version > e.version {
                true
            } else if new_version < e.version {
                false
            } else {
                (new_seg.0, new_off) > (e.segment_id.0, e.offset)
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PrimaryIndex {
    inner: HashMap<Key, IndexEntry>,
}

impl PrimaryIndex {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn apply(&mut self, segment_id: SegmentId, offset: u64, rec: &Record) {
        let key = rec.key;
        let cand = IndexEntry {
            segment_id,
            offset,
            version: rec.version,
            size: record_wire_len(rec.payload_len) as u32,
        };
        let replace = newer_wins(self.inner.get(&key), rec.version, segment_id, offset);
        if replace {
            self.inner.insert(key, cand);
        }
    }

    pub fn get(&self, key: &Key) -> Option<&IndexEntry> {
        self.inner.get(key)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Key, &IndexEntry)> {
        self.inner.iter()
    }

    pub fn references_segment(&self, sid: SegmentId) -> bool {
        self.inner.values().any(|e| e.segment_id == sid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::record_flags;

    #[test]
    fn index_overwrite_higher_version() {
        let mut idx = PrimaryIndex::new();
        let k = 1u128;
        let r1 = Record::new(k, 1, 0, b"a");
        let r2 = Record::new(k, 2, 0, b"b");
        idx.apply(SegmentId(1), 64, &r1);
        idx.apply(SegmentId(1), 120, &r2);
        let e = idx.get(&k).unwrap();
        assert_eq!(e.version, 2);
        assert_eq!(e.offset, 120);
    }

    #[test]
    fn lower_version_does_not_overwrite() {
        let mut idx = PrimaryIndex::new();
        let k = 1u128;
        idx.apply(SegmentId(1), 200, &Record::new(k, 5, 0, b"x"));
        idx.apply(SegmentId(2), 64, &Record::new(k, 3, 0, b"y"));
        assert_eq!(idx.get(&k).unwrap().version, 5);
    }

    /// Tombstone visibility: index tracks latest record; when it is a tombstone, entry still exists (version 2).
    #[test]
    fn tombstone_visibility() {
        let mut idx = PrimaryIndex::new();
        let k = 7u128;
        idx.apply(SegmentId(1), 64, &Record::new(k, 1, 0, b"v"));
        idx.apply(
            SegmentId(1),
            100,
            &Record::new(k, 2, record_flags::TOMBSTONE, b""),
        );
        let e = idx.get(&k).unwrap();
        assert_eq!(e.version, 2);
        assert_eq!(e.offset, 100);
    }

    #[test]
    fn tombstone_hidden_by_newer_put() {
        let mut idx = PrimaryIndex::new();
        let k = 3u128;
        idx.apply(SegmentId(1), 64, &Record::new(k, 1, 0, b"a"));
        idx.apply(
            SegmentId(1),
            90,
            &Record::new(k, 2, record_flags::TOMBSTONE, b""),
        );
        idx.apply(SegmentId(2), 64, &Record::new(k, 3, 0, b"back"));
        assert_eq!(idx.get(&k).unwrap().version, 3);
    }

    #[test]
    fn batch_logical_order_independent_of_apply_order_same_segment() {
        let mut idx = PrimaryIndex::new();
        // Apply in "wrong" version order first — should not matter if we only call apply
        // in scan order; this test documents that apply must be called in segment order.
        // For same-key conflict, newer_wins handles it.
        let k = 9u128;
        idx.apply(SegmentId(1), 64, &Record::new(k, 10, 0, b"x"));
        idx.apply(SegmentId(1), 100, &Record::new(k, 5, 0, b"y"));
        assert_eq!(idx.get(&k).unwrap().version, 10);
    }

    #[test]
    fn tie_break_larger_offset_wins() {
        let mut idx = PrimaryIndex::new();
        let k = 1u128;
        let v = 5u64;
        idx.apply(SegmentId(1), 64, &Record::new(k, v, 0, b"a"));
        idx.apply(SegmentId(1), 200, &Record::new(k, v, 0, b"b"));
        assert_eq!(idx.get(&k).unwrap().offset, 200);
    }

    #[test]
    fn stale_segment_eligibility() {
        let mut idx = PrimaryIndex::new();
        idx.apply(SegmentId(7), 10, &Record::new(1u128, 1, 0, b"x"));
        assert!(idx.references_segment(SegmentId(7)));
        assert!(!idx.references_segment(SegmentId(8)));
    }
}
