//! Named access patterns for benchmark workloads.

/// High-level workload shape (how keys / ops are drawn).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadPattern {
    /// Uniform random keys and query batches.
    Random,
    /// Keys grouped into clusters; queries stay inside one cluster with high probability.
    Clustered,
    /// Rank-based Zipf key selection (skewed hot set).
    SkewedZipfian,
    /// Alternates between disjoint key regions to stress planner / avoid easy locality.
    AdversarialAlternating,
    /// Interleaved reads and writes by `read_fraction` in [`super::generator::WorkloadSpec`].
    MixedReadWrite,
}
