//! Segment file layout: fixed header, record stream, footer index.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::codec::record_wire_len;
use crate::error::{NexusError, Result};
use crate::ids::SegmentId;
use crate::storage::record::Record;
use crate::storage::segment_index::SegmentFooter;

pub const SEGMENT_MAGIC: &[u8; 8] = b"NEXSEG01";
pub const SEGMENT_HEADER_LEN: u64 = 64;

#[derive(Debug, Clone)]
pub struct SegmentHeader {
    pub format_version: u32,
    pub segment_id: u64,
}

impl SegmentHeader {
    pub fn new(segment_id: SegmentId) -> Self {
        Self {
            format_version: 1,
            segment_id: segment_id.0,
        }
    }

    pub fn encode(&self) -> [u8; SEGMENT_HEADER_LEN as usize] {
        let mut buf = [0u8; SEGMENT_HEADER_LEN as usize];
        buf[0..8].copy_from_slice(SEGMENT_MAGIC);
        buf[8..12].copy_from_slice(&self.format_version.to_le_bytes());
        buf[12..20].copy_from_slice(&self.segment_id.to_le_bytes());
        buf
    }

    pub fn decode(data: &[u8; SEGMENT_HEADER_LEN as usize]) -> Option<Self> {
        if &data[0..8] != SEGMENT_MAGIC {
            return None;
        }
        let format_version = u32::from_le_bytes(data[8..12].try_into().ok()?);
        let segment_id = u64::from_le_bytes(data[12..20].try_into().ok()?);
        Some(Self {
            format_version,
            segment_id,
        })
    }
}

pub fn segment_file_name(id: SegmentId) -> String {
    format!("{:020}.seg", id.0)
}

/// Sequential scan from `data_start` validating records; returns offsets and end of last complete record.
pub fn scan_records(data: &[u8], data_start: u64) -> Result<(Vec<u64>, u64)> {
    let mut offsets = Vec::new();
    let mut pos = 0usize;
    let base = data_start;
    while pos < data.len() {
        let off = base + pos as u64;
        let remaining = &data[pos..];
        if remaining.len() < crate::codec::RECORD_HEADER_LEN + 4 {
            break;
        }
        let mut hdr = [0u8; crate::codec::RECORD_HEADER_LEN];
        hdr.copy_from_slice(&remaining[0..crate::codec::RECORD_HEADER_LEN]);
        let header = crate::codec::RecordHeader::decode(&hdr)?;
        let total = record_wire_len(header.payload_len);
        if remaining.len() < total {
            break;
        }
        match Record::decode(remaining, off) {
            Ok((_, len)) => {
                debug_assert_eq!(len, total);
                offsets.push(off);
                pos += len;
            }
            Err(_) => break,
        }
    }
    let end = base + pos as u64;
    Ok((offsets, end))
}

/// Read last up to `max` bytes for footer discovery.
pub fn read_file_tail(path: &Path, max: u64) -> Result<(u64, Vec<u8>)> {
    let mut f = File::open(path).map_err(|e| NexusError::io(path, e))?;
    let len = f.metadata().map_err(|e| NexusError::io(path, e))?.len();
    let read_len = max.min(len);
    let mut buf = vec![0u8; read_len as usize];
    f.seek(SeekFrom::Start(len - read_len))
        .map_err(|e| NexusError::io(path, e))?;
    f.read_exact(&mut buf)
        .map_err(|e| NexusError::io(path, e))?;
    Ok((len, buf))
}

/// Recover segment: prefer footer; on failure scan and truncate deterministically.
pub fn recover_segment(path: &Path) -> Result<SegmentFooter> {
    let (len, tail) = read_file_tail(path, 256 * 1024)?;
    if len < SEGMENT_HEADER_LEN {
        return Err(NexusError::CorruptFooter {
            path: path.to_path_buf(),
            reason: "file too small".into(),
        });
    }

    if let Some((footer, footer_start)) = SegmentFooter::parse_from_file_tail(len, &tail) {
        if footer_start < SEGMENT_HEADER_LEN {
            return Err(NexusError::CorruptFooter {
                path: path.to_path_buf(),
                reason: "invalid footer position".into(),
            });
        }
        let mut expected = SEGMENT_HEADER_LEN;
        let mut valid = true;
        let mut f = File::open(path).map_err(|e| NexusError::io(path, e))?;
        for &off in &footer.record_offsets {
            if off != expected {
                valid = false;
                break;
            }
            f.seek(SeekFrom::Start(off))
                .map_err(|e| NexusError::io(path, e))?;
            let mut hdr = [0u8; crate::codec::RECORD_HEADER_LEN];
            f.read_exact(&mut hdr)
                .map_err(|e| NexusError::io(path, e))?;
            let header = match crate::codec::RecordHeader::decode(&hdr) {
                Ok(h) => h,
                Err(_) => {
                    valid = false;
                    break;
                }
            };
            let need = record_wire_len(header.payload_len) as u64;
            if off + need > footer_start {
                valid = false;
                break;
            }
            let mut rec = vec![0u8; need as usize];
            f.seek(SeekFrom::Start(off))
                .map_err(|e| NexusError::io(path, e))?;
            f.read_exact(&mut rec)
                .map_err(|e| NexusError::io(path, e))?;
            if Record::decode(&rec, off).is_err() {
                valid = false;
                break;
            }
            expected += need;
        }
        if valid && expected == footer_start {
            return Ok(footer);
        }
    }

    // Scan from header
    let mut f = File::open(path).map_err(|e| NexusError::io(path, e))?;
    let data_len = len as usize;
    let mut body = vec![0u8; data_len];
    f.seek(SeekFrom::Start(0))
        .map_err(|e| NexusError::io(path, e))?;
    f.read_exact(&mut body)
        .map_err(|e| NexusError::io(path, e))?;

    let hdr_arr: [u8; SEGMENT_HEADER_LEN as usize] =
        body[0..SEGMENT_HEADER_LEN as usize].try_into().unwrap();
    if SegmentHeader::decode(&hdr_arr).is_none() {
        return Err(NexusError::InvalidSegmentHeader {
            path: path.to_path_buf(),
        });
    }

    let data = &body[SEGMENT_HEADER_LEN as usize..];
    let (offsets, end) = scan_records(data, SEGMENT_HEADER_LEN)?;
    let mut ff = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| NexusError::io(path, e))?;
    ff.set_len(end).map_err(|e| NexusError::io(path, e))?;
    let footer = SegmentFooter {
        record_offsets: offsets.clone(),
    };
    let blob = footer.encode();
    ff.seek(SeekFrom::End(0))
        .map_err(|e| NexusError::io(path, e))?;
    ff.write_all(&blob).map_err(|e| NexusError::io(path, e))?;
    ff.sync_all().map_err(|e| NexusError::io(path, e))?;
    Ok(footer)
}
