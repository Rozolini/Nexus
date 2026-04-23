//! Segment footer index: absolute offsets of record starts.

use crate::checksum::crc32_bytes;

pub const FOOTER_MAGIC: &[u8; 8] = b"NEXFOOTB";

/// Serialized footer: `MAGIC || num_records || offsets... || crc32` (crc covers prefix excluding itself).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFooter {
    pub record_offsets: Vec<u64>,
}

impl SegmentFooter {
    pub fn encoded_len(&self) -> usize {
        8 + 8 + self.record_offsets.len() * 8 + 4
    }

    pub fn encode(&self) -> Vec<u8> {
        let n = self.record_offsets.len() as u64;
        let mut out = Vec::with_capacity(self.encoded_len());
        out.extend_from_slice(FOOTER_MAGIC);
        out.extend_from_slice(&n.to_le_bytes());
        for o in &self.record_offsets {
            out.extend_from_slice(&o.to_le_bytes());
        }
        let crc = crc32_bytes(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    /// Parse footer at `data` where `data` is exactly one footer blob (no trailing bytes).
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 8 + 8 + 4 {
            return None;
        }
        if &data[0..8] != FOOTER_MAGIC {
            return None;
        }
        let n = u64::from_le_bytes(data[8..16].try_into().ok()?) as usize;
        let need = 8 + 8 + n * 8 + 4;
        if data.len() != need {
            return None;
        }
        let crc_stored = u32::from_le_bytes(data[data.len() - 4..].try_into().ok()?);
        let crc_expected = crc32_bytes(&data[..data.len() - 4]);
        if crc_stored != crc_expected {
            return None;
        }
        let mut record_offsets = Vec::with_capacity(n);
        for i in 0..n {
            let start = 16 + i * 8;
            record_offsets.push(u64::from_le_bytes(data[start..start + 8].try_into().ok()?));
        }
        Some(Self { record_offsets })
    }

    /// Locate footer in a file of `file_len` by scanning candidate record counts from the tail.
    pub fn parse_from_file_tail(file_len: u64, tail: &[u8]) -> Option<(Self, u64)> {
        let mut n = 0usize;
        loop {
            let footer_len = 20usize.saturating_add(n.saturating_mul(8));
            if footer_len > tail.len() {
                break;
            }
            if (footer_len as u64) > file_len {
                break;
            }
            let start_in_tail = tail.len() - footer_len;
            let slice = &tail[start_in_tail..];
            if let Some(f) = Self::decode(slice) {
                if f.record_offsets.len() == n {
                    let footer_start = file_len - footer_len as u64;
                    return Some((f, footer_start));
                }
            }
            n += 1;
            if n > 1_000_000 {
                break;
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::SegmentFooter;

    #[test]
    fn footer_roundtrip() {
        let f = SegmentFooter {
            record_offsets: vec![64, 120, 400],
        };
        let bytes = f.encode();
        let parsed = SegmentFooter::decode(&bytes).unwrap();
        assert_eq!(parsed, f);
    }

    #[test]
    fn parse_from_tail() {
        let f = SegmentFooter {
            record_offsets: vec![64, 128],
        };
        let blob = f.encode();
        let file_len = 10_000u64 + blob.len() as u64;
        let tail = vec![0u8; blob.len()];
        let mut tail = tail;
        tail.copy_from_slice(&blob);
        let (p, start) = SegmentFooter::parse_from_file_tail(file_len, &tail).unwrap();
        assert_eq!(p, f);
        assert_eq!(start, 10_000);
    }
}
