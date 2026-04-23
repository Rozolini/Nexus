//! Append-only segment writer: header, records, sealed footer.

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{NexusError, Result};
use crate::ids::SegmentId;
use crate::storage::record::Record;
use crate::storage::segment::{SegmentHeader, SEGMENT_HEADER_LEN};
use crate::storage::segment_index::SegmentFooter;

pub struct SegmentWriter {
    segment_id: SegmentId,
    file: File,
    path: PathBuf,
    offsets: Vec<u64>,
    /// Next byte offset where a new record will start (equals current file len before seal).
    data_end: u64,
}

impl SegmentWriter {
    pub fn create(path: &Path, segment_id: SegmentId) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(path)
            .map_err(|e| NexusError::io(path, e))?;
        let hdr = SegmentHeader::new(segment_id);
        let bytes = hdr.encode();
        file.write_all(&bytes)
            .map_err(|e| NexusError::io(path, e))?;
        file.sync_all().map_err(|e| NexusError::io(path, e))?;
        Ok(Self {
            segment_id,
            file,
            path: path.to_path_buf(),
            offsets: Vec::new(),
            data_end: SEGMENT_HEADER_LEN,
        })
    }

    pub fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    /// Append a record; returns its starting offset in the file.
    pub fn append(&mut self, record: &Record) -> Result<u64> {
        let off = self.data_end;
        let wire = record.encode();
        self.file
            .seek(SeekFrom::Start(off))
            .map_err(|e| NexusError::io(&self.path, e))?;
        self.file
            .write_all(&wire)
            .map_err(|e| NexusError::io(&self.path, e))?;
        self.offsets.push(off);
        self.data_end = off + wire.len() as u64;
        Ok(off)
    }

    /// Write footer and fsync. After this the segment is immutable.
    pub fn seal(mut self) -> Result<SegmentFooter> {
        let footer = SegmentFooter {
            record_offsets: self.offsets.clone(),
        };
        let blob = footer.encode();
        self.file
            .seek(SeekFrom::Start(self.data_end))
            .map_err(|e| NexusError::io(&self.path, e))?;
        self.file
            .write_all(&blob)
            .map_err(|e| NexusError::io(&self.path, e))?;
        self.file
            .sync_all()
            .map_err(|e| NexusError::io(&self.path, e))?;
        Ok(footer)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn data_end(&self) -> u64 {
        self.data_end
    }
}
