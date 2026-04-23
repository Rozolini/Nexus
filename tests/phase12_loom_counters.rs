//! Loom model of the engine-stats concurrency pattern.
//!
//! The production counters in `src/stats/counters.rs` use `std::sync::atomic`
//! because that type is threaded through many modules (`engine::api` uses
//! `Ordering::Relaxed` on the same atomics). To keep the Loom surface tight
//! without refactoring every module to a custom atomics shim, we model the
//! *same* concurrency pattern here with `loom::sync::atomic::AtomicU64` and
//! let Loom exhaustively enumerate schedules.
//!
//! What we model:
//! * Multiple writer threads, each performing `fetch_add(x, Relaxed)` on a
//!   shared set of monotonic counters (mimicking `EngineStats` updates on
//!   the hot read path).
//! * A reader thread (or post-join observation) performing `load(Relaxed)`
//!   on every counter to build a snapshot.
//!
//! Invariants checked:
//! 1. After all writers have joined, every counter equals the exact sum of
//!    contributions — i.e. no lost updates.
//! 2. Any in-flight snapshot observes each counter as a value in `[0, N]`
//!    where `N` is the total-to-be-added, i.e. counters never go backwards
//!    or exceed the final value.
//!
//! Run with:
//! ```ignore
//! RUSTFLAGS="--cfg loom" cargo test --release --test phase12_loom_counters
//! ```
//!
//! The test is compiled into an empty binary when `--cfg loom` is absent,
//! so it's safe to include in the default workspace.

#![cfg(loom)]

use loom::sync::atomic::{AtomicU64, Ordering};
use loom::sync::Arc;
use loom::thread;

/// Minimal mirror of `EngineStats`. We only carry two counters because Loom's
/// state space grows super-linearly with each additional atomic and more
/// fields do not add coverage for the invariant we care about.
#[derive(Default)]
struct Mirror {
    writes: AtomicU64,
    reads: AtomicU64,
}

impl Mirror {
    fn snapshot(&self) -> (u64, u64) {
        // Mirrors `EngineStats::snapshot`: sequential relaxed loads, producing
        // a non-linearisable but always-valid-past-state view.
        (
            self.writes.load(Ordering::Relaxed),
            self.reads.load(Ordering::Relaxed),
        )
    }
}

#[test]
fn no_lost_updates_under_two_writers() {
    loom::model(|| {
        let m = Arc::new(Mirror::default());

        let m1 = m.clone();
        let t1 = thread::spawn(move || {
            m1.writes.fetch_add(1, Ordering::Relaxed);
            m1.reads.fetch_add(2, Ordering::Relaxed);
        });

        let m2 = m.clone();
        let t2 = thread::spawn(move || {
            m2.writes.fetch_add(3, Ordering::Relaxed);
            m2.reads.fetch_add(4, Ordering::Relaxed);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let (w, r) = m.snapshot();
        assert_eq!(w, 1 + 3, "lost update on writes");
        assert_eq!(r, 2 + 4, "lost update on reads");
    });
}

#[test]
fn concurrent_snapshot_never_observes_impossible_values() {
    loom::model(|| {
        let m = Arc::new(Mirror::default());

        // Writer adds to both counters in order: writes, then reads. Any
        // snapshot thread must observe writes <= 1 at the moment and reads
        // in {0, 1} — never exceeding the final target.
        let mw = m.clone();
        let writer = thread::spawn(move || {
            mw.writes.fetch_add(1, Ordering::Relaxed);
            mw.reads.fetch_add(1, Ordering::Relaxed);
        });

        let mr = m.clone();
        let reader = thread::spawn(move || {
            let (w, r) = mr.snapshot();
            // Monotonic bounds. Note that `(w=0, r=1)` is perfectly legal
            // under Relaxed even though the writer did `writes` first; the
            // point is that `(w, r)` are each in `[0, 1]`. No cross-counter
            // ordering is claimed here.
            assert!(w <= 1);
            assert!(r <= 1);
        });

        writer.join().unwrap();
        reader.join().unwrap();

        let (w, r) = m.snapshot();
        assert_eq!(w, 1);
        assert_eq!(r, 1);
    });
}
