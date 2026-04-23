//! Deterministic workload generation.

use crate::types::Key;
use crate::util::deterministic::SplitMix64;

use super::patterns::WorkloadPattern;

/// Configuration for [`WorkloadGenerator`].
#[derive(Debug, Clone)]
pub struct WorkloadSpec {
    pub pattern: WorkloadPattern,
    /// Keys live in `[0, key_space)`.
    pub key_space: u128,
    pub query_batch_size: usize,
    /// Number of batched read operations (each returns `query_batch_size` keys), unless mixed mode.
    pub num_query_batches: usize,
    /// For [`WorkloadPattern::Clustered`]: number of clusters partitioning the key space.
    pub cluster_count: u128,
    /// Zipf exponent `s` for [`WorkloadPattern::SkewedZipfian`] (typical 1.0–1.5).
    pub zipf_s: f64,
    /// For [`WorkloadPattern::MixedReadWrite`]: fraction of steps that are reads in `[0,1]`.
    pub read_fraction: f64,
    /// For mixed mode: number of total steps (read batches or single writes).
    pub mixed_steps: usize,
    /// Payload size for synthetic writes.
    pub write_payload_len: usize,
}

impl Default for WorkloadSpec {
    fn default() -> Self {
        Self {
            pattern: WorkloadPattern::Random,
            key_space: 10_000,
            query_batch_size: 8,
            num_query_batches: 64,
            cluster_count: 16,
            zipf_s: 1.2,
            read_fraction: 0.7,
            mixed_steps: 200,
            write_payload_len: 32,
        }
    }
}

/// Single step in a mixed read/write workload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkloadStep {
    Query(Vec<Key>),
    Write {
        key: Key,
        version: u64,
        payload: Vec<u8>,
    },
}

/// Precomputed Zipf probabilities for ranks `1..=n`.
#[derive(Debug, Clone)]
struct ZipfTable {
    cdf: Vec<f64>,
}

impl ZipfTable {
    fn new(n: usize, s: f64) -> Self {
        let mut denom = 0.0_f64;
        for k in 1..=n {
            denom += 1.0 / (k as f64).powf(s);
        }
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0_f64;
        for k in 1..=n {
            acc += (1.0 / (k as f64).powf(s)) / denom;
            cdf.push(acc);
        }
        Self { cdf }
    }

    fn sample(&self, u: f64) -> usize {
        let u = u.clamp(0.0, 1.0);
        self.cdf
            .iter()
            .position(|&c| u <= c)
            .unwrap_or(self.cdf.len().saturating_sub(1))
    }
}

/// Deterministic stream of workload steps for a given seed.
pub struct WorkloadGenerator {
    rng: SplitMix64,
    spec: WorkloadSpec,
    zipf_keys: Option<ZipfTable>,
    step_idx: usize,
    adversarial_flip: bool,
}

impl WorkloadGenerator {
    pub fn new(seed: u64, spec: WorkloadSpec) -> Self {
        let n = spec.key_space.min(usize::MAX as u128) as usize;
        let zipf_keys = if matches!(spec.pattern, WorkloadPattern::SkewedZipfian) && n > 0 {
            Some(ZipfTable::new(n.max(1), spec.zipf_s))
        } else {
            None
        };
        Self {
            rng: SplitMix64::new(seed),
            spec,
            zipf_keys,
            step_idx: 0,
            adversarial_flip: false,
        }
    }

    fn draw_key(&mut self) -> Key {
        let ks = self.spec.key_space.max(1);
        self.rng.gen_below_u128(ks)
    }

    fn draw_zipf_key(&mut self) -> Key {
        let tab = self.zipf_keys.as_ref().expect("zipf table");
        let u = self.rng.next_f64();
        let rank = tab.sample(u); // 0..n-1 maps to rank order; convert to key index
        let n = self.spec.key_space.max(1);
        (rank as u128).min(n.saturating_sub(1))
    }

    fn draw_cluster_batch(&mut self) -> Vec<Key> {
        let cc = self.spec.cluster_count.max(1);
        let ks = self.spec.key_space.max(1);
        let cluster = self.rng.gen_below_u128(cc);
        let span = ks.div_ceil(cc);
        let base = cluster.saturating_mul(span).min(ks.saturating_sub(1));
        let mut keys = Vec::with_capacity(self.spec.query_batch_size);
        for _ in 0..self.spec.query_batch_size {
            let off = self.rng.gen_below_u128(span.max(1));
            keys.push((base + off).min(ks.saturating_sub(1)));
        }
        keys.sort_unstable();
        keys.dedup();
        while keys.len() < self.spec.query_batch_size.min(ks as usize)
            && keys.len() < self.spec.query_batch_size
        {
            keys.push((base + self.rng.gen_below_u128(span.max(1))).min(ks.saturating_sub(1)));
            keys.sort_unstable();
            keys.dedup();
        }
        keys
    }

    fn draw_random_batch(&mut self) -> Vec<Key> {
        let mut keys = Vec::with_capacity(self.spec.query_batch_size);
        for _ in 0..self.spec.query_batch_size {
            keys.push(self.draw_key());
        }
        keys.sort_unstable();
        keys.dedup();
        while keys.len() < self.spec.query_batch_size {
            keys.push(self.draw_key());
            keys.sort_unstable();
            keys.dedup();
        }
        keys
    }

    fn draw_zipf_batch(&mut self) -> Vec<Key> {
        let mut keys = Vec::with_capacity(self.spec.query_batch_size);
        for _ in 0..self.spec.query_batch_size {
            keys.push(self.draw_zipf_key());
        }
        keys.sort_unstable();
        keys.dedup();
        while keys.len() < self.spec.query_batch_size {
            keys.push(self.draw_zipf_key());
            keys.sort_unstable();
            keys.dedup();
        }
        keys
    }

    fn draw_adversarial_batch(&mut self) -> Vec<Key> {
        let ks = self.spec.key_space.max(1);
        let half = (ks / 2).max(1);
        self.adversarial_flip = !self.adversarial_flip;
        let (lo, hi) = if self.adversarial_flip {
            (0u128, half.saturating_sub(1))
        } else {
            (half, ks.saturating_sub(1))
        };
        let mut keys = Vec::with_capacity(self.spec.query_batch_size);
        for _ in 0..self.spec.query_batch_size {
            let x = lo
                + self
                    .rng
                    .gen_below_u128(hi.saturating_sub(lo).saturating_add(1).max(1));
            keys.push(x.min(ks.saturating_sub(1)));
        }
        keys.sort_unstable();
        keys.dedup();
        while keys.len() < self.spec.query_batch_size {
            let x = lo
                + self
                    .rng
                    .gen_below_u128(hi.saturating_sub(lo).saturating_add(1).max(1));
            keys.push(x.min(ks.saturating_sub(1)));
            keys.sort_unstable();
            keys.dedup();
        }
        keys
    }

    fn next_query_batch(&mut self) -> Option<Vec<Key>> {
        match self.spec.pattern {
            WorkloadPattern::Random => Some(self.draw_random_batch()),
            WorkloadPattern::Clustered => Some(self.draw_cluster_batch()),
            WorkloadPattern::SkewedZipfian => Some(self.draw_zipf_batch()),
            WorkloadPattern::AdversarialAlternating => Some(self.draw_adversarial_batch()),
            WorkloadPattern::MixedReadWrite => None,
        }
    }

    fn next_mixed_step(&mut self) -> Option<WorkloadStep> {
        if self.step_idx >= self.spec.mixed_steps {
            return None;
        }
        self.step_idx += 1;
        let is_read = self.rng.next_f64() < self.spec.read_fraction;
        if is_read {
            let keys = match self.spec.pattern {
                WorkloadPattern::Random => self.draw_random_batch(),
                WorkloadPattern::Clustered => self.draw_cluster_batch(),
                WorkloadPattern::SkewedZipfian => self.draw_zipf_batch(),
                WorkloadPattern::AdversarialAlternating => self.draw_adversarial_batch(),
                // Steady-state mixed reads: half random, half clustered (final e2e spec).
                WorkloadPattern::MixedReadWrite => {
                    if self.rng.next_f64() < 0.5 {
                        self.draw_random_batch()
                    } else {
                        self.draw_cluster_batch()
                    }
                }
            };
            Some(WorkloadStep::Query(keys))
        } else {
            let key = self.draw_key();
            let mut p = vec![0u8; self.spec.write_payload_len];
            for b in p.iter_mut() {
                *b = self.rng.next_u64() as u8;
            }
            Some(WorkloadStep::Write {
                key,
                version: 1,
                payload: p,
            })
        }
    }
}

impl Iterator for WorkloadGenerator {
    type Item = WorkloadStep;

    fn next(&mut self) -> Option<Self::Item> {
        if matches!(self.spec.pattern, WorkloadPattern::MixedReadWrite) {
            return self.next_mixed_step();
        }
        if self.step_idx >= self.spec.num_query_batches {
            return None;
        }
        self.step_idx += 1;
        let keys = self.next_query_batch()?;
        Some(WorkloadStep::Query(keys))
    }
}

/// Stable digest of the workload **sequence** (ops only, no I/O).
pub fn workload_sequence_digest(seed: u64, spec: &WorkloadSpec) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    seed.hash(&mut h);
    for step in WorkloadGenerator::new(seed, spec.clone()) {
        match step {
            WorkloadStep::Query(keys) => {
                0u8.hash(&mut h);
                keys.hash(&mut h);
            }
            WorkloadStep::Write {
                key,
                version,
                ref payload,
            } => {
                1u8.hash(&mut h);
                key.hash(&mut h);
                version.hash(&mut h);
                payload.hash(&mut h);
            }
        }
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_stable_for_same_seed() {
        let spec = WorkloadSpec::default();
        let a = workload_sequence_digest(42, &spec);
        let b = workload_sequence_digest(42, &spec);
        assert_eq!(a, b);
    }

    #[test]
    fn digest_changes_with_seed() {
        let spec = WorkloadSpec::default();
        assert_ne!(
            workload_sequence_digest(1, &spec),
            workload_sequence_digest(2, &spec)
        );
    }
}
