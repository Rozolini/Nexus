//! CRC32 used for record bodies and structural footers.

use crc32fast::Hasher;

#[inline]
pub fn crc32_update(crc: u32, chunk: &[u8]) -> u32 {
    let mut h = Hasher::new_with_initial(crc);
    h.update(chunk);
    h.finalize()
}

#[inline]
pub fn crc32_bytes(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}
