//! Index remap after relocation.

use crate::ids::SegmentId;
use crate::index::primary::PrimaryIndex;
use crate::storage::record::Record;
use crate::types::Key;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemapEntry {
    pub key: Key,
    pub segment_id: SegmentId,
    pub offset: u64,
    pub record: Record,
}

pub fn apply_remap(index: &mut PrimaryIndex, entries: &[RemapEntry]) {
    for e in entries {
        index.apply(e.segment_id, e.offset, &e.record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::SegmentId;
    use crate::storage::record::Record;

    #[test]
    fn remap_correctness() {
        let mut idx = PrimaryIndex::new();
        idx.apply(SegmentId(1), 64, &Record::new(5u128, 2, 0, b"old"));
        let r = Record::new(5u128, 2, 0, b"new");
        apply_remap(
            &mut idx,
            &[RemapEntry {
                key: 5,
                segment_id: SegmentId(9),
                offset: 128,
                record: r.clone(),
            }],
        );
        let e = idx.get(&5u128).unwrap();
        assert_eq!(e.segment_id, SegmentId(9));
        assert_eq!(e.offset, 128);
    }
}
