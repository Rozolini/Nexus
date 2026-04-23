//! Live record filtering for relocation.

use crate::storage::record::Record;

#[inline]
pub fn is_latest_record_relocatable(_rec: &Record) -> bool {
    true
}
