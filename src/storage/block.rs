//! Block layout: fixed-size logical blocks over the byte stream (I/O granularity).
//!
//! Records are **packed** sequentially; a record may span two logical blocks. Corruption in any
//! byte covered by a record is detected when the **record** CRC is validated (see `codec`).

use crate::checksum::crc32_bytes;

/// Default read/granularity block size (bytes). Records may span block boundaries.
pub const DEFAULT_BLOCK_SIZE: u32 = 64 * 1024;

/// Returns `(block_index, offset_within_block)` for an absolute file offset.
#[inline]
pub fn locate_in_blocks(absolute_offset: u64, block_size: u32) -> (u64, u32) {
    let bs = block_size as u64;
    let block_index = absolute_offset / bs;
    let within = (absolute_offset % bs) as u32;
    (block_index, within)
}

/// Number of full blocks spanned by `[start, start + len)`.
pub fn blocks_touched(start: u64, len: u64, block_size: u32) -> u64 {
    if len == 0 {
        return 0;
    }
    let bs = block_size as u64;
    let end = start + len - 1;
    end / bs - start / bs + 1
}

/// CRC32 of each logical block slice that overlaps `[absolute_start, absolute_start + len)` in file space.
/// `file_data` is the on-disk bytes starting at `base_offset`.
pub fn crc32_per_touched_block(
    file_data: &[u8],
    base_offset: u64,
    absolute_start: u64,
    len: u64,
    block_size: u32,
) -> Vec<(u64, u32)> {
    if len == 0 {
        return Vec::new();
    }
    let bs = block_size as u64;
    let abs_end = absolute_start + len;
    let first_block = absolute_start / bs;
    let last_block = (abs_end - 1) / bs;
    let seg_end = base_offset + file_data.len() as u64;
    let mut out = Vec::new();
    for bi in first_block..=last_block {
        let block_start = bi * bs;
        let block_end = block_start + bs;
        let clip_start = block_start.max(base_offset);
        let clip_end = block_end.min(seg_end);
        if clip_start >= clip_end {
            continue;
        }
        let i0 = (clip_start - base_offset) as usize;
        let i1 = (clip_end - base_offset) as usize;
        out.push((bi, crc32_bytes(&file_data[i0..i1])));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_block_record_touches_multiple_blocks() {
        let bs = 256u32;
        let start = 200u64;
        let len = 400u64;
        assert!(blocks_touched(start, len, bs) >= 2);
    }

    #[test]
    fn flip_byte_changes_block_crc() {
        let bs = 64u32;
        let mut data = vec![0u8; bs as usize * 2];
        let abs = bs as u64 + 10;
        data[abs as usize] = 0x77;
        let before = crc32_per_touched_block(&data, 0, abs, 1, bs);
        data[abs as usize] ^= 0xff;
        let after = crc32_per_touched_block(&data, 0, abs, 1, bs);
        assert_ne!(before[0].1, after[0].1);
    }
}
