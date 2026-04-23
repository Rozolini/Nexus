//! # Nexus — adaptive locality-first storage engine
//!
//! Nexus is a single-node key-value engine that co-locates keys on disk
//! according to the access patterns it has observed. The goal is to turn
//! *logical* read locality (keys touched together at the API layer) into
//! *physical* I/O efficiency (fewer disk seeks, bigger sequential reads).
//!
//! ## Module map
//!
//! | Module | Responsibility |
//! |--------|----------------|
//! | [`engine`]      | Public API (`Engine::open`, `put`, `get`, `get_many`) and the hot read/write paths. |
//! | [`storage`]     | On-disk layout: segment files, manifest, block readers/writers. |
//! | [`codec`]       | Binary wire format for records (header + payload + CRC). |
//! | [`index`]       | In-memory primary index `Key → (SegmentId, offset, size)`. |
//! | [`tracker`]     | Read-session co-access tracker (what's read together). |
//! | [`graph`]       | Persistent co-access graph (decayed weights). |
//! | [`planner`]     | Computes a layout plan from the graph (which keys should be grouped). |
//! | [`compaction`]  | Executes layout plans by rewriting records into new segments. |
//! | [`scheduler`]   | Orchestrates the tracker → graph → planner → compaction pipeline. |
//! | [`recovery`]    | Startup replay and orphan-segment cleanup. |
//! | [`benchmark`]   | Deterministic, multi-run benchmark harness and report types. |
//! | [`workload`]    | Workload generators (Random / Clustered / SkewedZipfian / …). |
//! | [`stats`]       | Engine counters (writes, reads, physical I/O ops, etc.). |
//!
//! ## Thread-safety
//!
//! The `Engine` is currently single-writer; reads may be issued from the
//! same thread or from a reader that holds an `&Engine`. Counters in
//! [`stats`] are `AtomicU64` with `Ordering::Relaxed` — see the module docs
//! for the ordering rationale. A Loom model of this pattern lives in
//! `tests/phase12_loom_counters.rs`.
//!
//! ## Error model
//!
//! All fallible APIs return [`Result`] (aliased to [`NexusError`]).
//! `NexusError` carries enough context to pinpoint the failing path, record
//! offset, or manifest file. No panics are used for control flow.

pub mod checksum;
pub mod codec;
pub mod config;
pub mod engine;
pub mod error;
pub mod fault;
pub mod ids;
pub mod storage;
pub mod types;

pub mod benchmark;
pub mod compaction;
pub mod graph;
pub mod index;
pub mod planner;
pub mod recovery;
pub mod scheduler;
pub mod stats;
pub mod tracker;
pub mod util;
pub mod workload;

pub use benchmark::{
    aggregate_runs, colocated_pair_ratio, derive_seed, run_benchmark, run_multi_single_process,
    run_once, run_single_scenario_and_emit_json, write_multirun_artifacts, AggregatedMetrics,
    AggregatedReportNumbers, BenchmarkHarnessConfig, BenchmarkReport, IsolationMode,
    MultiRunConfig, MultiRunReport, MultiRunSummary, MultirunArtifacts, PhaseReport, QuerySample,
    RelocationTrace, RunResult, StabilizationReport, ZEROISH_GAIN_THRESHOLD,
};
pub use compaction::{
    collect_source_segments, install_segments_atomic, select_live_records_for_keys,
    validate_record_for_rewrite, RelocationMetadata,
};
pub use config::{EngineConfig, ReadMergePolicy, ReadTrackingConfig, SchedulerConfig};
pub use engine::Engine;
pub use error::{NexusError, Result};
pub use fault as fault_injection;
pub use graph::{apply_decay, CoAccessGraph, GraphSnapshot, SnapshotEdge};
pub use ids::SegmentId;
pub use index::{apply_remap, RemapEntry};
pub use index::{newer_wins, IndexEntry, PrimaryIndex};
pub use planner::{
    affinity, build_layout_plan, normalization_factor, GroupPlan, LayoutPlan, LayoutPlanner,
    PlannerConfig,
};
pub use recovery::{
    cleanup_empty_orphan_segments, list_orphan_segment_files, StartupRecoveryReport,
};
pub use scheduler::{
    budget_allows_reloc, effective_budget, BackgroundScheduler, EffectiveBudget, SchedulerReport,
    TriggerInputs, TriggerKind,
};
pub use stats::{EngineDetailedSnapshot, EngineStats, StatsSnapshot};
pub use storage::manifest::Manifest;
pub use storage::record::Record;
pub use tracker::{
    deterministic_pair_downsample, CoReadEvent, CoReadQuery, ReadSession, ReadTracker,
};
pub use types::Key;
pub use workload::{
    workload_sequence_digest, WorkloadGenerator, WorkloadPattern, WorkloadSpec, WorkloadStep,
};
