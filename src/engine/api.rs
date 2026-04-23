//! # Engine
//!
//! Public surface of Nexus. Everything the user interacts with goes through
//! the [`Engine`] struct: `put`, `get`, `get_many`, segment rotation, and
//! statistics.
//!
//! ## Data-flow at a glance
//!
//! ```text
//!     put(k, v) ──► SegmentWriter ──► active segment file
//!                       │
//!                       └──► PrimaryIndex[k] = (seg, offset, size)
//!
//!     get(k)    ──► PrimaryIndex[k] ──► seek+read_exact in segment file
//!     get_many  ──► group by seg ──► sort by offset ──► merge ranges
//!                                ──► one seek+read_exact per range
//! ```
//!
//! ## Invariants
//!
//! * Every `put` of key `k` yields a **new** index entry with a strictly
//!   greater `seq`. Older entries for the same key remain on disk as dead
//!   records until compaction reclaims them.
//! * Tombstones (`Record::tombstone`) share the same path — writing a
//!   tombstone atomically removes a key from the live set on replay.
//! * `get_many` is **range-merged**: adjacent or near-contiguous
//!   records within the same segment are served by a single read syscall.
//!   See `ReadMergePolicy` for the exact thresholds.
//! * Counters are updated per-record for `get` and per-range for
//!   `get_many`; the two views are intentionally different so the benchmark
//!   can see how many syscalls were *avoided* by merging.
//!
//! ## Concurrency
//!
//! The engine is single-writer. `&self` read methods are safe to call
//! concurrently only if no `&mut self` method is running. Internal counters
//! are atomic and therefore safe across observers.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::codec::record_wire_len;
use crate::compaction::relocation::{
    collect_source_segments, select_live_records_for_keys, RelocationMetadata,
};
use crate::compaction::safety::validate_record_for_rewrite;
use crate::config::EngineConfig;
use crate::error::{NexusError, Result};
use crate::fault;
use crate::graph::GraphSnapshot;
use crate::ids::SegmentId;
use crate::index::primary::{IndexEntry, PrimaryIndex};
use crate::planner::GroupPlan;
use crate::recovery::{
    cleanup_empty_orphan_segments, list_orphan_segment_files, StartupRecoveryReport,
};
use crate::stats::{EngineDetailedSnapshot, EngineStats};
use crate::storage::manifest::Manifest;
use crate::storage::record::Record;
use crate::storage::segment::segment_file_name;
use crate::storage::segment_reader::{read_record_at_path, SegmentReader};
use crate::storage::segment_writer::SegmentWriter;
use crate::tracker::{CoReadEvent, ReadTracker};
use crate::types::Key;

pub struct Engine {
    cfg: EngineConfig,
    index: PrimaryIndex,
    stats: EngineStats,
    read_tracker: ReadTracker,
    segment_paths: Vec<PathBuf>,
    next_segment_id: u64,
    writer: Option<SegmentWriter>,
    manifest_generation: AtomicU64,
    last_startup_recovery: StartupRecoveryReport,
}

impl Engine {
    pub fn open(cfg: EngineConfig) -> Result<Self> {
        let dir = &cfg.data_dir;
        if !dir.exists() {
            fs::create_dir_all(dir).map_err(|e| NexusError::io(dir, e))?;
        }
        if !dir.is_dir() {
            return Err(NexusError::InvalidDataDir(dir.clone()));
        }

        let manifest = Manifest::load_robust(dir)?;

        let orphan_files = list_orphan_segment_files(dir, &manifest)?;
        let empty_orphan_segments_removed = cleanup_empty_orphan_segments(dir, &manifest)?;

        let segment_paths = list_segment_paths(dir, &manifest)?;

        let mut next_segment_id: u64 = 1;
        for p in &segment_paths {
            if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                if let Some(id) = parse_segment_id_u64(name) {
                    next_segment_id = next_segment_id.max(id + 1);
                }
            }
        }

        let mut index = PrimaryIndex::new();
        let mut checksum_verified: u64 = 0;
        for p in &segment_paths {
            let sid = segment_id_from_path(p)
                .ok_or_else(|| NexusError::InvalidSegmentHeader { path: p.clone() })?;
            let reader = SegmentReader::open(p)?;
            for off in reader.iter_offsets() {
                let rec = reader.read_at(off)?;
                checksum_verified += 1;
                index.apply(sid, off, &rec);
            }
        }

        let read_tracker = ReadTracker::new(cfg.read_tracking.clone());
        let manifest_generation = AtomicU64::new(manifest.generation);
        let last_startup_recovery = StartupRecoveryReport {
            manifest_generation: manifest.generation,
            segments_replayed: segment_paths.len(),
            orphan_files,
            empty_orphan_segments_removed,
            checksum_records_verified: checksum_verified,
        };

        Ok(Self {
            cfg,
            index,
            stats: EngineStats::default(),
            read_tracker,
            segment_paths,
            next_segment_id,
            writer: None,
            manifest_generation,
            last_startup_recovery,
        })
    }

    #[inline]
    fn segment_path(&self, id: SegmentId) -> PathBuf {
        self.cfg.data_dir.join(segment_file_name(id))
    }

    fn read_at_entry(&self, e: &IndexEntry) -> Result<Record> {
        let path = self.segment_path(e.segment_id);
        read_record_at_path(&path, e.offset)
    }

    pub fn put(&mut self, record: Record) -> Result<()> {
        self.stats.writes.fetch_add(1, Ordering::Relaxed);

        let dir = &self.cfg.data_dir;
        if self.writer.is_none() {
            let id = SegmentId(self.next_segment_id);
            self.next_segment_id += 1;
            let name = segment_file_name(id);
            let path = dir.join(&name);
            let w = SegmentWriter::create(&path, id)?;
            self.writer = Some(w);
            self.segment_paths.push(path);
            self.persist_manifest()?;
        }
        let w = self.writer.as_mut().unwrap();
        if fault::take_fail_before_record_append() {
            return Err(NexusError::InjectedFault("before record append".into()));
        }
        let seg_id = w.segment_id();
        let off = w.append(&record)?;
        self.index.apply(seg_id, off, &record);
        Ok(())
    }

    /// Logical delete: append tombstone (latest version wins vs older values).
    pub fn delete(&mut self, key: Key, version: u64) -> Result<()> {
        self.put(Record::new(
            key,
            version,
            crate::types::record_flags::TOMBSTONE,
            Vec::new(),
        ))
    }

    /// Returns `Ok(None)` if missing or latest is tombstone. IO/checksum errors propagate.
    pub fn get(&self, key: Key) -> Result<Option<Record>> {
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        let Some(entry) = self.index.get(&key) else {
            return Ok(None);
        };
        self.stats.segments_touched.fetch_add(1, Ordering::Relaxed);
        self.stats
            .segment_groups_in_batches
            .fetch_add(1, Ordering::Relaxed);
        self.stats.file_opens.fetch_add(1, Ordering::Relaxed);
        self.stats.physical_read_ops.fetch_add(1, Ordering::Relaxed);
        self.stats.range_read_ops.fetch_add(1, Ordering::Relaxed);
        self.stats.records_in_ranges.fetch_add(1, Ordering::Relaxed);
        let rec = self.read_at_entry(entry)?;
        let wire = record_wire_len(rec.payload_len) as u64;
        self.stats.bytes_read.fetch_add(wire, Ordering::Relaxed);
        self.stats
            .physical_bytes_read
            .fetch_add(wire, Ordering::Relaxed);
        self.stats
            .range_bytes_read
            .fetch_add(wire, Ordering::Relaxed);
        if rec.is_tombstone() {
            return Ok(None);
        }
        Ok(Some(rec))
    }

    /// Seal the current write segment, so subsequent `put`s start a new segment.
    /// Used by benchmark harness to avoid pathological "single segment" baselines.
    pub fn rotate_segment(&mut self) -> Result<()> {
        if let Some(w) = self.writer.take() {
            w.seal()?;
            self.persist_manifest()?;
        }
        Ok(())
    }

    /// Multi-key read: results are in **the same order as `keys`**, independent of segment order.
    pub fn get_many(&self, keys: &[Key]) -> Result<Vec<Option<Record>>> {
        self.get_many_inner(keys)
    }

    /// Same as [`get_many`](Self::get_many), then records co-access for this query (bounded).
    pub fn get_many_tracked(&mut self, keys: &[Key]) -> Result<Vec<Option<Record>>> {
        let out = self.get_many_inner(keys)?;
        self.read_tracker.on_query_keys(keys);
        Ok(out)
    }

    fn get_many_inner(&self, keys: &[Key]) -> Result<Vec<Option<Record>>> {
        let mut out: Vec<Option<Record>> = (0..keys.len()).map(|_| None).collect();
        if keys.is_empty() {
            return Ok(out);
        }

        // (position_in_input, offset, size) for every key found in index, grouped by segment.
        // BTreeMap keyed by SegmentId guarantees deterministic open order across runs.
        let mut by_segment: BTreeMap<SegmentId, Vec<(usize, u64, u32)>> = BTreeMap::new();
        for (pos, &key) in keys.iter().enumerate() {
            self.stats.reads.fetch_add(1, Ordering::Relaxed);
            let Some(entry) = self.index.get(&key).copied() else {
                continue;
            };
            by_segment
                .entry(entry.segment_id)
                .or_default()
                .push((pos, entry.offset, entry.size));
        }

        let distinct_segments = by_segment.len() as u64;
        self.stats
            .segments_touched
            .fetch_add(distinct_segments, Ordering::Relaxed);
        self.stats
            .segment_groups_in_batches
            .fetch_add(distinct_segments, Ordering::Relaxed);

        let policy = self.cfg.read_merge;

        for (sid, mut rows) in by_segment {
            rows.sort_by_key(|&(_, off, _)| off);
            if !rows.is_empty() {
                let span = rows
                    .last()
                    .unwrap()
                    .1
                    .saturating_sub(rows.first().unwrap().1);
                self.stats
                    .offsets_span_sum
                    .fetch_add(span, Ordering::Relaxed);
                self.stats
                    .offsets_span_groups
                    .fetch_add(1, Ordering::Relaxed);
            }

            // --- Range merging: one pass over ascending-offset rows. ---
            // Invariant: `ranges.last().end` is the current committed upper bound (exclusive).
            struct Range {
                start: u64,
                end: u64,
                members: Vec<(usize, u64, u32)>,
            }
            let mut ranges: Vec<Range> = Vec::new();
            for (pos, off, sz) in rows {
                let rec_end = off.saturating_add(sz as u64);
                let mut extended = false;
                if let Some(r) = ranges.last_mut() {
                    let gap = off.saturating_sub(r.end);
                    let candidate_end = rec_end.max(r.end);
                    let candidate_len = candidate_end.saturating_sub(r.start);
                    if gap <= policy.max_read_gap_bytes && candidate_len <= policy.max_range_bytes {
                        self.stats.range_merges.fetch_add(1, Ordering::Relaxed);
                        self.stats
                            .gap_bytes_merged
                            .fetch_add(gap, Ordering::Relaxed);
                        r.end = candidate_end;
                        r.members.push((pos, off, sz));
                        extended = true;
                    }
                }
                if !extended {
                    ranges.push(Range {
                        start: off,
                        end: rec_end,
                        members: vec![(pos, off, sz)],
                    });
                }
            }

            let path = self.segment_path(sid);
            let mut f = File::open(&path).map_err(|e| NexusError::io(&path, e))?;
            self.stats.file_opens.fetch_add(1, Ordering::Relaxed);

            for r in ranges {
                let len = (r.end - r.start) as usize;
                if len == 0 {
                    continue;
                }
                let mut buf = vec![0u8; len];
                f.seek(SeekFrom::Start(r.start))
                    .map_err(|e| NexusError::io(&path, e))?;
                f.read_exact(&mut buf)
                    .map_err(|e| NexusError::io(&path, e))?;

                self.stats.range_read_ops.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .range_bytes_read
                    .fetch_add(len as u64, Ordering::Relaxed);
                self.stats
                    .records_in_ranges
                    .fetch_add(r.members.len() as u64, Ordering::Relaxed);
                self.stats.physical_read_ops.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .physical_bytes_read
                    .fetch_add(len as u64, Ordering::Relaxed);

                for (pos, off, sz) in r.members {
                    let rel = (off - r.start) as usize;
                    let end = rel + sz as usize;
                    if end > buf.len() {
                        return Err(NexusError::CorruptRecord {
                            offset: off,
                            reason: format!(
                                "index size={} exceeds range buffer {} (seg {:?})",
                                sz,
                                buf.len() - rel,
                                sid
                            ),
                        });
                    }
                    let slice = &buf[rel..end];
                    let (rec, _) = Record::decode(slice, off)?;
                    let wire = record_wire_len(rec.payload_len) as u64;
                    self.stats.bytes_read.fetch_add(wire, Ordering::Relaxed);
                    if rec.is_tombstone() {
                        out[pos] = None;
                    } else {
                        out[pos] = Some(rec);
                    }
                }
            }
        }

        Ok(out)
    }

    pub fn read_tracker(&self) -> &ReadTracker {
        &self.read_tracker
    }

    pub fn read_tracker_mut(&mut self) -> &mut ReadTracker {
        &mut self.read_tracker
    }

    /// Deterministic sorted edge list (stable across identical graph state).
    pub fn graph_snapshot(&self) -> GraphSnapshot {
        self.read_tracker.graph().export_snapshot()
    }

    /// Optional decay of co-access weights.
    pub fn decay_read_graph(&mut self, factor: f64) {
        self.read_tracker.apply_decay(factor);
    }

    /// Record co-access for keys read in one logical query (bounded policy applies).
    pub fn record_query_coaccess(&mut self, keys: &[Key]) {
        self.read_tracker.on_query_keys(keys);
    }

    /// Emit a single co-read event (plan: explicit event emission API).
    pub fn emit_co_read_event(&mut self, event: CoReadEvent) {
        self.read_tracker.emit_co_read_event(event);
    }

    /// Consume a [`crate::tracker::ReadSession`] at query boundary.
    pub fn finish_read_session(&mut self, session: crate::tracker::ReadSession) {
        let keys = session.finish();
        self.read_tracker.on_query_keys(&keys);
    }

    pub fn stats(&self) -> &EngineStats {
        &self.stats
    }

    /// Latest index entries (may point at tombstones).
    pub fn iter_index(&self) -> impl Iterator<Item = (&Key, &IndexEntry)> {
        self.index.iter()
    }

    pub fn flush_manifest(&self) -> Result<()> {
        self.persist_manifest()
    }

    pub fn close(mut self) -> Result<()> {
        if let Some(w) = self.writer.take() {
            w.seal()?;
        }
        self.persist_manifest()?;
        Ok(())
    }

    fn persist_manifest(&self) -> Result<()> {
        let dir = &self.cfg.data_dir;
        let segments: Vec<String> = self
            .segment_paths
            .iter()
            .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
            .collect();
        let gen = self.manifest_generation.fetch_add(1, Ordering::Relaxed) + 1;
        let manifest = Manifest {
            version: 1,
            generation: gen,
            segments,
        };
        Manifest::save_atomic(dir, &manifest)?;
        Ok(())
    }

    pub fn data_dir(&self) -> &Path {
        &self.cfg.data_dir
    }

    pub fn segment_count(&self) -> usize {
        self.segment_paths.len()
    }

    /// Returns `true` if at least `min_ratio` of `keys` that exist in the index share
    /// the **same** `segment_id` (scheduler guard against breaking already-colocated
    /// groups into many tiny destination segments — see `SkewedZipfian` regression).
    pub fn group_already_colocated(&self, keys: &[Key], min_ratio: f64) -> bool {
        if keys.is_empty() {
            return false;
        }
        let mut count_per_seg: std::collections::HashMap<SegmentId, usize> =
            std::collections::HashMap::new();
        let mut total = 0usize;
        for k in keys {
            if let Some(e) = self.index.get(k) {
                *count_per_seg.entry(e.segment_id).or_insert(0) += 1;
                total += 1;
            }
        }
        if total == 0 {
            return false;
        }
        let best = count_per_seg.values().copied().max().unwrap_or(0);
        (best as f64 / total as f64) >= min_ratio
    }

    pub fn index_len(&self) -> usize {
        self.index.len()
    }

    pub fn scheduler_cfg(&self) -> &crate::config::SchedulerConfig {
        &self.cfg.scheduler
    }

    pub fn manifest_generation(&self) -> u64 {
        self.manifest_generation.load(Ordering::Relaxed)
    }

    pub fn last_startup_recovery(&self) -> &StartupRecoveryReport {
        &self.last_startup_recovery
    }

    pub fn detailed_snapshot(&self) -> EngineDetailedSnapshot {
        let r = self.last_startup_recovery();
        EngineDetailedSnapshot {
            stats: self.stats.snapshot(),
            manifest_generation: self.manifest_generation(),
            segment_count: self.segment_count(),
            index_entries: self.index_len(),
            startup_checksum_records_verified: r.checksum_records_verified,
            startup_orphan_segments_detected: r.orphan_files.len(),
            startup_empty_orphan_segments_removed: r.empty_orphan_segments_removed.len(),
        }
    }

    pub fn debug_dump(&self) -> String {
        format!(
            "manifest_generation={}\nsegments={}\nindex_entries={}\nstartup_orphans={}\nstartup_empty_orphans_removed={}\nstartup_checksum_records={}\n",
            self.manifest_generation(),
            self.segment_count(),
            self.index_len(),
            self.last_startup_recovery.orphan_files.len(),
            self.last_startup_recovery.empty_orphan_segments_removed.len(),
            self.last_startup_recovery.checksum_records_verified,
        )
    }

    /// Deterministic fingerprint of primary index layout (for replay tests).
    pub fn layout_fingerprint(&self) -> u64 {
        let mut rows: Vec<(Key, u64, u64, u64)> = self
            .iter_index()
            .map(|(k, e)| (*k, e.version, e.segment_id.0, e.offset))
            .collect();
        rows.sort();
        let s = format!("{rows:?}");
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    pub fn segment_paths(&self) -> &[PathBuf] {
        &self.segment_paths
    }

    pub fn relocate_keys(&mut self, keys: &[Key]) -> Result<RelocationMetadata> {
        if keys.is_empty() {
            return Err(NexusError::CorruptRecord {
                offset: 0,
                reason: "empty relocation key list".into(),
            });
        }
        let dir = self.cfg.data_dir.clone();
        let live = select_live_records_for_keys(&self.index, keys, |sid, off| {
            let p = dir.join(segment_file_name(sid));
            let r = read_record_at_path(&p, off)?;
            validate_record_for_rewrite(&r)?;
            Ok(r)
        })?;
        let source_segment_ids = collect_source_segments(&live, &self.index);

        if let Some(w) = self.writer.take() {
            w.seal()?;
        }

        let new_id = SegmentId(self.next_segment_id);
        self.next_segment_id += 1;
        let path = self.cfg.data_dir.join(segment_file_name(new_id));
        let mut sw = SegmentWriter::create(&path, new_id)?;
        let mut offsets: Vec<u64> = Vec::with_capacity(live.len());
        let mut bytes_written: u64 = 0;
        for (_, rec) in &live {
            offsets.push(sw.append(rec)?);
            bytes_written += record_wire_len(rec.payload_len) as u64;
        }
        sw.seal()?;
        self.segment_paths.push(path);
        self.sort_paths_for_manifest();
        if fault::take_fail_before_relocate_first_manifest_save() {
            return Err(NexusError::InjectedFault(
                "before relocate first manifest save".into(),
            ));
        }
        self.persist_manifest()?;

        for (i, (_, rec)) in live.iter().enumerate() {
            self.index.apply(new_id, offsets[i], rec);
        }

        self.retire_stale_segments()?;
        self.sort_paths_for_manifest();
        self.persist_manifest()?;

        let keys_out: Vec<Key> = live.iter().map(|(k, _)| *k).collect();
        Ok(RelocationMetadata {
            destination_segment_id: new_id,
            keys: keys_out,
            source_segment_ids,
            bytes_written,
        })
    }

    pub fn relocate_group(&mut self, group: &GroupPlan) -> Result<RelocationMetadata> {
        self.relocate_keys(&group.keys)
    }

    fn sort_paths_for_manifest(&mut self) {
        self.segment_paths.sort_by(|a, b| {
            let an = a.file_name().map(|n| n.to_string_lossy().into_owned());
            let bn = b.file_name().map(|n| n.to_string_lossy().into_owned());
            an.cmp(&bn)
        });
    }

    fn retire_stale_segments(&mut self) -> Result<()> {
        let mut referenced: HashSet<SegmentId> = HashSet::new();
        for (_, e) in self.index.iter() {
            referenced.insert(e.segment_id);
        }
        if let Some(w) = &self.writer {
            referenced.insert(w.segment_id());
        }
        let mut keep: Vec<PathBuf> = Vec::new();
        let mut remove: Vec<PathBuf> = Vec::new();
        for p in &self.segment_paths {
            let sid = segment_id_from_path(p)
                .ok_or_else(|| NexusError::InvalidSegmentHeader { path: p.clone() })?;
            if referenced.contains(&sid) {
                keep.push(p.clone());
            } else {
                remove.push(p.clone());
            }
        }
        self.segment_paths = keep;
        for p in remove {
            let _ = fs::remove_file(&p);
        }
        Ok(())
    }
}

fn segment_id_from_path(p: &Path) -> Option<SegmentId> {
    let name = p.file_name()?.to_str()?;
    parse_segment_id_u64(name).map(SegmentId)
}

fn parse_segment_id_u64(file_name: &str) -> Option<u64> {
    let stem = file_name.strip_suffix(".seg")?;
    stem.parse().ok()
}

fn list_segment_paths(dir: &Path, manifest: &Manifest) -> Result<Vec<PathBuf>> {
    let mut seen = BTreeSet::new();
    for s in &manifest.segments {
        let p = dir.join(s);
        if p.exists() {
            seen.insert(p);
        }
    }
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().and_then(|x| x.to_str()) == Some("seg") {
                seen.insert(p);
            }
        }
    }
    let mut v: Vec<PathBuf> = seen.into_iter().collect();
    v.sort_by_key(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()));
    Ok(v)
}

// Inline unit tests hit the real filesystem via `tempfile`. Miri cannot
// model host-FS syscalls, so we gate this module with `not(miri)`.
// The integration test suite in `tests/` is not compiled under Miri by
// the CI (`cargo miri test --lib` only), so end-to-end coverage is safe.
#[cfg(all(test, not(miri)))]
mod tests {
    use std::collections::HashMap;

    use tempfile::tempdir;

    use super::*;
    use crate::config::EngineConfig;
    use crate::storage::record::Record;

    /// Unit: `get_many` results align with **request** key order (not storage order).
    #[test]
    fn batch_read_result_ordering() {
        let dir = tempdir().unwrap();
        let mut eng = Engine::open(EngineConfig::new(dir.path())).unwrap();
        eng.put(Record::new(10, 1, 0, b"a")).unwrap();
        eng.put(Record::new(20, 1, 0, b"b")).unwrap();
        let keys = [20u128, 10];
        let got = eng.get_many(&keys).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].as_ref().unwrap().payload, b"b");
        assert_eq!(got[1].as_ref().unwrap().payload, b"a");
    }

    /// Gate: interleaved writes and reads keep a single deterministic latest per key.
    #[test]
    fn gate_deterministic_latest_mixed_interleaved() {
        let dir = tempdir().unwrap();
        let mut eng = Engine::open(EngineConfig::new(dir.path())).unwrap();
        let mut want: HashMap<u128, Vec<u8>> = HashMap::new();
        for round in 0u64..200 {
            let k = (round % 15) as u128;
            let v = round + 1;
            let payload = format!("p{}", v).into_bytes();
            eng.put(Record::new(k, v, 0, payload.clone())).unwrap();
            want.insert(k, payload);
            if round % 7 == 0 {
                assert_eq!(eng.get(k).unwrap().unwrap().payload, want[&k]);
            }
        }
        for k in 0u128..15 {
            assert_eq!(eng.get(k).unwrap().unwrap().payload, want[&k]);
        }
    }

    #[test]
    fn get_many_counts_distinct_segments_touched() {
        let dir = tempdir().unwrap();
        let cfg = EngineConfig::new(dir.path());
        {
            let mut eng = Engine::open(cfg.clone()).unwrap();
            eng.put(Record::new(1, 1, 0, b"a")).unwrap();
            eng.close().unwrap();
        }
        {
            let mut eng = Engine::open(cfg.clone()).unwrap();
            eng.put(Record::new(2, 1, 0, b"b")).unwrap();
            eng.close().unwrap();
        }
        let eng = Engine::open(cfg).unwrap();
        let st0 = eng.stats().snapshot();
        let _ = eng.get_many(&[1u128, 2]).unwrap();
        let st1 = eng.stats().snapshot();
        assert_eq!(st1.reads - st0.reads, 2);
        assert_eq!(st1.segments_touched - st0.segments_touched, 2);
    }
}
