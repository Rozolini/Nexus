//! Logical record (data model v1).
//!
//! Wire shape: `key`, `version`, `flags`, `payload_len`, `payload`, `checksum`.

use crate::codec::{self, RecordHeader};
use crate::error::Result;
use crate::types::Key;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Key,
    pub version: u64,
    pub flags: u32,
    pub payload_len: u32,
    pub payload: Vec<u8>,
    /// CRC32 stored on wire after the payload (`0` until encoded/decoded from disk).
    pub checksum: u32,
}

impl Record {
    pub fn new(key: Key, version: u64, flags: u32, payload: impl Into<Vec<u8>>) -> Self {
        let payload = payload.into();
        let payload_len = payload.len() as u32;
        Self {
            key,
            version,
            flags,
            payload_len,
            payload,
            checksum: 0,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        (self.flags & crate::types::record_flags::TOMBSTONE) != 0
    }

    /// CRC32 over header ‖ payload (matches on-disk checksum when record is valid).
    pub fn computed_checksum(&self) -> u32 {
        debug_assert_eq!(
            self.payload_len as usize,
            self.payload.len(),
            "payload_len must match payload length"
        );
        codec::record_checksum(&self.header(), &self.payload)
    }

    pub fn encode(&self) -> Vec<u8> {
        debug_assert_eq!(
            self.payload_len as usize,
            self.payload.len(),
            "payload_len must match payload length"
        );
        codec::encode_record(self.key, self.version, self.flags, &self.payload)
    }

    pub fn decode(data: &[u8], base_offset: u64) -> Result<(Self, usize)> {
        let (hdr, payload, checksum) = codec::decode_record_bytes(data, base_offset)?;
        let total = codec::record_wire_len(hdr.payload_len);
        Ok((
            Self {
                key: hdr.key,
                version: hdr.version,
                flags: hdr.flags,
                payload_len: hdr.payload_len,
                payload: payload.to_vec(),
                checksum,
            },
            total,
        ))
    }

    pub fn header(&self) -> RecordHeader {
        debug_assert_eq!(
            self.payload_len as usize,
            self.payload.len(),
            "payload_len must match payload length"
        );
        RecordHeader {
            key: self.key,
            version: self.version,
            flags: self.flags,
            payload_len: self.payload_len,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_sets_payload_len_from_payload() {
        let r = Record::new(1, 1, 0, vec![1u8, 2, 3]);
        assert_eq!(r.payload_len, 3);
        assert_eq!(r.payload.len(), 3);
    }

    #[test]
    fn decode_sets_payload_len() {
        let r0 = Record::new(9, 2, 0, b"abc".as_slice());
        let wire = r0.encode();
        let (r, _) = Record::decode(&wire, 0).unwrap();
        assert_eq!(r.payload_len, 3);
        assert_eq!(r.payload, b"abc");
    }
}
