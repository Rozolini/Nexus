//! Read path for sealed segments.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::{NexusError, Result};
use crate::storage::record::Record;
use crate::storage::segment::recover_segment;
use crate::storage::segment_index::SegmentFooter;

/// Read a validated record at `offset` without scanning the segment (hot path for `get`).
pub fn read_record_at_path(path: &Path, offset: u64) -> Result<Record> {
    let mut f = File::open(path).map_err(|e| NexusError::io(path, e))?;
    f.seek(SeekFrom::Start(offset))
        .map_err(|e| NexusError::io(path, e))?;
    let mut hdr = [0u8; crate::codec::RECORD_HEADER_LEN];
    f.read_exact(&mut hdr)
        .map_err(|e| NexusError::io(path, e))?;
    let header = crate::codec::RecordHeader::decode(&hdr)?;
    let need = crate::codec::record_wire_len(header.payload_len);
    let mut buf = vec![0u8; need];
    buf[..crate::codec::RECORD_HEADER_LEN].copy_from_slice(&hdr);
    f.read_exact(&mut buf[crate::codec::RECORD_HEADER_LEN..])
        .map_err(|e| NexusError::io(path, e))?;
    let (rec, _) = Record::decode(&buf, offset)?;
    Ok(rec)
}

pub struct SegmentReader {
    pub path: std::path::PathBuf,
    pub footer: SegmentFooter,
}

impl SegmentReader {
    pub fn open(path: &Path) -> Result<Self> {
        let footer = recover_segment(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            footer,
        })
    }

    pub fn iter_offsets(&self) -> impl Iterator<Item = u64> + '_ {
        self.footer.record_offsets.iter().copied()
    }

    pub fn read_at(&self, offset: u64) -> Result<Record> {
        read_record_at_path(&self.path, offset)
    }
}
