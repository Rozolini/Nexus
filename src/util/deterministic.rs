//! Deterministic PRNG for reproducible benchmarks.

/// SplitMix64 — fast, seedable, no external crates.
#[derive(Debug, Clone)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    /// Uniform [0, 1)
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() as f64) / (u64::MAX as f64 + 1.0)
    }

    /// Uniform `0..max_exclusive` (biased mod for benchmark-sized spaces; OK for harness).
    pub fn gen_below_u128(&mut self, max_exclusive: u128) -> u128 {
        if max_exclusive <= 1 {
            return 0;
        }
        let hi = self.next_u64() as u128;
        let lo = self.next_u64() as u128;
        let v = (hi << 64) | lo;
        v % max_exclusive
    }
}
