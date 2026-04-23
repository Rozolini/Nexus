//! Engine I/O counters.
//!
//! Counters are monotonic `u64` atomics incremented with `Ordering::Relaxed`.
//! Relaxed is correct here because:
//! 1. Every counter is **monotonic** (only `fetch_add`, never `store`).
//! 2. `snapshot()` is advisory: it records a valid past state — not
//!    necessarily a linearisable instant across all counters, which we never
//!    promise.
//! 3. No counter synchronises any other data; they are observers, not a
//!    happens-before edge.
//!
//! # Loom
//!
//! This module intentionally uses the stdlib atomics even under `--cfg loom`
//! so that the rest of the engine (which uses `std::sync::atomic::Ordering`
//! with the same counters) continues to compile. The concurrency contract
//! implemented here is mirrored by a standalone model in
//! `tests/phase12_loom_counters.rs` that uses `loom::sync::atomic` directly.

use std::sync::atomic::{AtomicU64, Ordering};

/// Engine-wide monotonic counters, populated by the read/write paths.
///
/// All fields are `AtomicU64` so the struct is safely shareable by reference
/// across threads; the engine itself is currently single-writer, so most
/// contention is reader-vs-observer (the stats snapshotter).
///
/// Adding a new counter:
/// 1. Add a field here with a doc comment explaining the *unit* (count vs.
///    bytes) and the *event* that increments it.
/// 2. Mirror it in [`StatsSnapshot`] and [`EngineStats::snapshot`].
/// 3. Mirror it in [`crate::benchmark::metrics::QuerySample`] if the
///    benchmark harness should observe it.
#[derive(Debug, Default)]
pub struct EngineStats {
    /// `put` calls that successfully appended a record.
    pub writes: AtomicU64,
    /// Logical `get` / `get_many` invocations (one per key requested).
    pub reads: AtomicU64,
    /// Logical bytes of read records (header + payload + CRC). **Does not
    /// reflect physical I/O** — see `physical_bytes_read` for that.
    pub bytes_read: AtomicU64,
    /// Sum over reads of distinct segments touched. `get` adds 1; `get_many`
    /// adds the number of unique segment ids in the batch.
    pub segments_touched: AtomicU64,

    // --- Physical read-path metrics ------------------------------------------
    /// Number of `File::open` calls performed by the read path. For the
    /// segment-aware batch reader this equals distinct segments per batch
    /// (not per-key), which is the measurable "did we re-open the same file"
    /// signal.
    pub file_opens: AtomicU64,
    /// Number of physical read syscalls (one per `seek + read_exact` round).
    /// After range-merging this equals `range_read_ops` for the
    /// batch path and is kept as a backwards-compatible alias.
    pub physical_read_ops: AtomicU64,
    /// Sum of bytes physically fetched from disk by the read path. Includes
    /// merged inter-record gaps absorbed by range reads.
    pub physical_bytes_read: AtomicU64,
    /// Sum over batches of distinct segment groups in that batch. Divide by
    /// the query count for mean groups-per-batch.
    pub segment_groups_in_batches: AtomicU64,
    /// Sum over `(batch, segment)` pairs of `max_offset - min_offset` within
    /// that group.
    pub offsets_span_sum: AtomicU64,
    /// Denominator for `offsets_span_sum`: number of `(batch, segment)` pairs.
    pub offsets_span_groups: AtomicU64,

    // --- Range-merged read-path metrics --------------------------------------
    /// Number of disk ranges actually read (one `seek + read_exact` per
    /// range). The primary physical-I/O metric.
    pub range_read_ops: AtomicU64,
    /// Sum of bytes read over ranges, **inclusive of merged gaps** between
    /// records inside one range.
    pub range_bytes_read: AtomicU64,
    /// Sum of user records covered by range reads. Use with `range_read_ops`
    /// to compute mean records-per-range.
    pub records_in_ranges: AtomicU64,
    /// Number of times a record was merged into an already-open range
    /// (i.e. did **not** start a new range).
    pub range_merges: AtomicU64,
    /// Sum of gap bytes absorbed into ranges by merges. Divide by
    /// `range_merges` for the mean absorbed gap.
    pub gap_bytes_merged: AtomicU64,
}

/// Point-in-time copy of [`EngineStats`] — cheap to pass around and diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub writes: u64,
    pub reads: u64,
    pub bytes_read: u64,
    pub segments_touched: u64,
    pub file_opens: u64,
    pub physical_read_ops: u64,
    pub physical_bytes_read: u64,
    pub segment_groups_in_batches: u64,
    pub offsets_span_sum: u64,
    pub offsets_span_groups: u64,
    pub range_read_ops: u64,
    pub range_bytes_read: u64,
    pub records_in_ranges: u64,
    pub range_merges: u64,
    pub gap_bytes_merged: u64,
}

/// Richer snapshot used by debug/observation endpoints. Carries runtime
/// structural state (manifest generation, segment count, index size) in
/// addition to the raw counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineDetailedSnapshot {
    pub stats: StatsSnapshot,
    pub manifest_generation: u64,
    pub segment_count: usize,
    pub index_entries: usize,
    /// Records verified via codec checksum during last startup replay.
    pub startup_checksum_records_verified: u64,
    pub startup_orphan_segments_detected: usize,
    pub startup_empty_orphan_segments_removed: usize,
}

impl EngineStats {
    /// Load every counter once under `Ordering::Relaxed`.
    ///
    /// The result is **not** a linearisable snapshot: counter `i` may reflect
    /// a later instant than counter `i-1`. That is acceptable for reporting
    /// and advisory uses; any invariant that requires a consistent cross-
    /// counter instant must be computed differently (e.g. by stopping the
    /// writer or taking a lock).
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            writes: self.writes.load(Ordering::Relaxed),
            reads: self.reads.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            segments_touched: self.segments_touched.load(Ordering::Relaxed),
            file_opens: self.file_opens.load(Ordering::Relaxed),
            physical_read_ops: self.physical_read_ops.load(Ordering::Relaxed),
            physical_bytes_read: self.physical_bytes_read.load(Ordering::Relaxed),
            segment_groups_in_batches: self.segment_groups_in_batches.load(Ordering::Relaxed),
            offsets_span_sum: self.offsets_span_sum.load(Ordering::Relaxed),
            offsets_span_groups: self.offsets_span_groups.load(Ordering::Relaxed),
            range_read_ops: self.range_read_ops.load(Ordering::Relaxed),
            range_bytes_read: self.range_bytes_read.load(Ordering::Relaxed),
            records_in_ranges: self.records_in_ranges.load(Ordering::Relaxed),
            range_merges: self.range_merges.load(Ordering::Relaxed),
            gap_bytes_merged: self.gap_bytes_merged.load(Ordering::Relaxed),
        }
    }
}
