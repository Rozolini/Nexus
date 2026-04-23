//! # Workload generator
//!
//! Deterministic synthetic workloads used by the benchmark harness and
//! integration tests. Each workload is parametrised by:
//!
//! * A [`WorkloadPattern`] (Random / Clustered / SkewedZipfian / Mixed /
//!   AdversarialAlternating / trace-replay).
//! * A [`WorkloadSpec`] — key space, batch size, op count, read/write mix.
//! * A `seed` — drives every RNG decision the generator makes.
//!
//! The same `(pattern, spec, seed)` always produces the same
//! `Vec<WorkloadStep>`, verified by
//! [`generator::workload_sequence_digest`] (a stable SHA-like fingerprint).
//! This is what lets the multi-run benchmark claim end-to-end determinism.

pub mod generator;
pub mod patterns;
pub mod traces;

pub use generator::{workload_sequence_digest, WorkloadGenerator, WorkloadSpec, WorkloadStep};
pub use patterns::WorkloadPattern;
