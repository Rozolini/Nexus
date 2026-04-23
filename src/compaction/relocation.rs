//! Relocation pipeline types.

use std::collections::BTreeSet;

use crate::error::Result;
use crate::ids::SegmentId;
use crate::index::primary::PrimaryIndex;
use crate::storage::record::Record;
use crate::types::Key;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelocationMetadata {
    pub destination_segment_id: SegmentId,
    pub keys: Vec<Key>,
    pub source_segment_ids: Vec<SegmentId>,
    /// Actual bytes written into the destination segment (record header + payload + crc, per key).
    /// Populated by the engine's relocation path; compaction helpers default to 0.
    pub bytes_written: u64,
}

pub fn select_live_records_for_keys<R>(
    index: &PrimaryIndex,
    keys: &[Key],
    mut read_loc: R,
) -> Result<Vec<(Key, Record)>>
where
    R: FnMut(SegmentId, u64) -> Result<Record>,
{
    let uniq: BTreeSet<Key> = keys.iter().copied().collect();
    let mut out = Vec::new();
    for k in uniq {
        // Keys that are in the planner's group but not in the index (e.g. read-only
        // "phantom" keys that entered the co-access graph but were never written, or
        // were subsequently tombstoned+retired) are silently skipped: they're not
        // relocatable records. This is deliberate and keeps the planner+scheduler
        // robust to stale views of the index.
        let Some(e) = index.get(&k) else {
            continue;
        };
        let rec = read_loc(e.segment_id, e.offset)?;
        if crate::compaction::rewrite_policy::is_latest_record_relocatable(&rec) {
            out.push((k, rec));
        }
    }
    out.sort_by_key(|(k, _)| *k);
    Ok(out)
}

pub fn collect_source_segments(
    keys_records: &[(Key, Record)],
    index: &PrimaryIndex,
) -> Vec<SegmentId> {
    let mut s: BTreeSet<SegmentId> = BTreeSet::new();
    for (k, _) in keys_records {
        if let Some(e) = index.get(k) {
            s.insert(e.segment_id);
        }
    }
    s.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::primary::PrimaryIndex;

    #[test]
    fn live_record_selection() {
        let mut idx = PrimaryIndex::new();
        let k = 42u128;
        idx.apply(SegmentId(3), 100, &Record::new(k, 1, 0, b"z"));
        let rows = select_live_records_for_keys(&idx, &[k], |sid, off| {
            assert_eq!(sid, SegmentId(3));
            assert_eq!(off, 100);
            Ok(Record::new(k, 1, 0, b"z"))
        })
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, k);
    }
}
