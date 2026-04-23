//! Rewrite safety: validate payload before copying.

use crate::error::{NexusError, Result};
use crate::storage::record::Record;

pub fn validate_record_for_rewrite(rec: &Record) -> Result<()> {
    if rec.payload.len() != rec.payload_len as usize {
        return Err(NexusError::CorruptRecord {
            offset: 0,
            reason: "payload_len mismatch".into(),
        });
    }
    if rec.checksum != 0 && rec.computed_checksum() != rec.checksum {
        return Err(NexusError::ChecksumMismatch {
            offset: 0,
            expected: rec.computed_checksum(),
            got: rec.checksum,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::Record;

    #[test]
    fn validate_rejects_len_mismatch() {
        let mut r = Record::new(1u128, 1, 0, b"ab");
        r.payload_len = 99;
        assert!(validate_record_for_rewrite(&r).is_err());
    }
}
