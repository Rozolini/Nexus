//! # Wire format (v1)
//!
//! Each record on disk is laid out as:
//!
//! ```text
//!   offset  | bytes | field
//!   --------+-------+-----------------------------
//!      0    |  16   | key        (u128 little-endian)
//!     16    |   8   | version    (u64 little-endian, monotonic per key)
//!     24    |   4   | flags      (u32 little-endian, bit 0 = tombstone)
//!     28    |   4   | payload_len (u32 little-endian, excludes header/crc)
//!     32    |   N   | payload    (opaque bytes)
//!   32+N    |   4   | crc32c     (over header + payload)
//! ```
//!
//! Rationale:
//! * Fixed-size header (32 B) ⇒ zero parsing branches in the hot path.
//! * Trailing CRC ⇒ single contiguous `read_exact` verifies integrity.
//! * `version` lives at a fixed offset so "latest wins" comparisons on
//!   replay do not need to decode the payload.
//!
//! Changing this format is a **hard compatibility break**: bump `v1` to
//! `v2` and write a migration tool; never mutate the layout in place.

use crate::error::{NexusError, Result};
use crate::types::Key;

/// Fixed header: key + version + flags + payload_len.
pub const RECORD_HEADER_LEN: usize = 32;

/// Full on-disk size for a record with `payload_len` bytes of payload (including stored checksum).
#[inline]
pub fn record_wire_len(payload_len: u32) -> usize {
    RECORD_HEADER_LEN + payload_len as usize + 4
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    pub key: Key,
    pub version: u64,
    pub flags: u32,
    pub payload_len: u32,
}

impl RecordHeader {
    pub fn encode_into(&self, dst: &mut [u8; RECORD_HEADER_LEN]) {
        dst[0..16].copy_from_slice(&self.key.to_le_bytes());
        dst[16..24].copy_from_slice(&self.version.to_le_bytes());
        dst[24..28].copy_from_slice(&self.flags.to_le_bytes());
        dst[28..32].copy_from_slice(&self.payload_len.to_le_bytes());
    }

    pub fn decode(src: &[u8; RECORD_HEADER_LEN]) -> Result<Self> {
        let key = u128::from_le_bytes(src[0..16].try_into().unwrap());
        let version = u64::from_le_bytes(src[16..24].try_into().unwrap());
        let flags = u32::from_le_bytes(src[24..28].try_into().unwrap());
        let payload_len = u32::from_le_bytes(src[28..32].try_into().unwrap());
        Ok(Self {
            key,
            version,
            flags,
            payload_len,
        })
    }
}

/// Computes checksum for a record: CRC32 over header || payload (checksum field excluded).
pub fn record_checksum(header: &RecordHeader, payload: &[u8]) -> u32 {
    let mut buf = [0u8; RECORD_HEADER_LEN];
    header.encode_into(&mut buf);
    let mut h = crc32fast::Hasher::new();
    h.update(&buf);
    h.update(payload);
    h.finalize()
}

pub fn encode_record(key: Key, version: u64, flags: u32, payload: &[u8]) -> Vec<u8> {
    let payload_len = payload.len() as u32;
    let header = RecordHeader {
        key,
        version,
        flags,
        payload_len,
    };
    let crc = record_checksum(&header, payload);
    let mut out = Vec::with_capacity(record_wire_len(payload_len));
    let mut hdr = [0u8; RECORD_HEADER_LEN];
    header.encode_into(&mut hdr);
    out.extend_from_slice(&hdr);
    out.extend_from_slice(payload);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

pub fn decode_record_bytes(data: &[u8], base_offset: u64) -> Result<(RecordHeader, &[u8], u32)> {
    if data.len() < RECORD_HEADER_LEN + 4 {
        return Err(NexusError::CorruptRecord {
            offset: base_offset,
            reason: "truncated record (header/checksum)".into(),
        });
    }
    let mut hdr_arr = [0u8; RECORD_HEADER_LEN];
    hdr_arr.copy_from_slice(&data[0..RECORD_HEADER_LEN]);
    let header = RecordHeader::decode(&hdr_arr)?;
    let plen = header.payload_len as usize;
    let need = RECORD_HEADER_LEN + plen + 4;
    if data.len() < need {
        return Err(NexusError::CorruptRecord {
            offset: base_offset,
            reason: format!("truncated record: need {} bytes, have {}", need, data.len()),
        });
    }
    let payload = &data[RECORD_HEADER_LEN..RECORD_HEADER_LEN + plen];
    let stored = u32::from_le_bytes(
        data[RECORD_HEADER_LEN + plen..RECORD_HEADER_LEN + plen + 4]
            .try_into()
            .unwrap(),
    );
    let expected = record_checksum(&header, payload);
    if stored != expected {
        return Err(NexusError::ChecksumMismatch {
            offset: base_offset,
            expected,
            got: stored,
        });
    }
    let checksum = stored;
    Ok((header, payload, checksum))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::record_flags;

    #[test]
    fn encode_decode_roundtrip() {
        let v = encode_record(42, 9, record_flags::TOMBSTONE, b"hello");
        let (h, p, cs) = decode_record_bytes(&v, 0).unwrap();
        assert_eq!(cs, record_checksum(&h, p));
        assert_eq!(h.key, 42);
        assert_eq!(h.version, 9);
        assert_eq!(h.flags, record_flags::TOMBSTONE);
        assert_eq!(p, b"hello");
    }

    #[test]
    fn checksum_mismatch_detected() {
        let mut v = encode_record(1, 1, 0, b"x");
        let last = v.len() - 1;
        v[last] ^= 0xff;
        let err = decode_record_bytes(&v, 0).unwrap_err();
        match err {
            NexusError::ChecksumMismatch { .. } => {}
            e => panic!("expected checksum mismatch, got {:?}", e),
        }
    }
}
