//! Deterministic statistical helpers used by the multi-run benchmark aggregator.
//!
//! These routines deliberately avoid heavy dependencies (no `statrs`, no `ndarray`):
//! all computations are pure `f64` arithmetic over small vectors, with NaN-tolerant
//! filtering. The sort order and the `P-linear` interpolation method are fixed so
//! identical inputs always yield identical outputs across runs and platforms.

/// Return a new vector with `NaN` entries removed and sorted ascending.
pub fn sorted_clean(xs: &[f64]) -> Vec<f64> {
    let mut v: Vec<f64> = xs.iter().copied().filter(|x| !x.is_nan()).collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v
}

/// Median (P50). Returns `0.0` on empty input.
pub fn median(xs: &[f64]) -> f64 {
    percentile(xs, 50.0)
}

/// Percentile using the `linear` method (a.k.a. type-7 in R / `numpy` default).
/// Returns `0.0` on empty input.
pub fn percentile(xs: &[f64], p: f64) -> f64 {
    let v = sorted_clean(xs);
    let n = v.len();
    if n == 0 {
        return 0.0;
    }
    if n == 1 {
        return v[0];
    }
    let p = p.clamp(0.0, 100.0);
    let idx = (p / 100.0) * (n as f64 - 1.0);
    let lo = idx.floor() as usize;
    let hi = idx.ceil() as usize;
    if lo == hi {
        v[lo]
    } else {
        let frac = idx - lo as f64;
        v[lo] * (1.0 - frac) + v[hi] * frac
    }
}

/// Interquartile range (P75 − P25).
pub fn iqr(xs: &[f64]) -> f64 {
    percentile(xs, 75.0) - percentile(xs, 25.0)
}

/// Minimum. Returns `0.0` on empty input.
pub fn min_f(xs: &[f64]) -> f64 {
    xs.iter()
        .copied()
        .filter(|x| !x.is_nan())
        .fold(f64::INFINITY, f64::min)
        .pipe_or_zero()
}

/// Maximum. Returns `0.0` on empty input.
pub fn max_f(xs: &[f64]) -> f64 {
    xs.iter()
        .copied()
        .filter(|x| !x.is_nan())
        .fold(f64::NEG_INFINITY, f64::max)
        .pipe_or_zero()
}

trait PipeOrZero {
    fn pipe_or_zero(self) -> f64;
}
impl PipeOrZero for f64 {
    fn pipe_or_zero(self) -> f64 {
        if self.is_finite() {
            self
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_odd_even() {
        assert!((median(&[1.0, 2.0, 3.0]) - 2.0).abs() < 1e-12);
        assert!((median(&[1.0, 2.0, 3.0, 4.0]) - 2.5).abs() < 1e-12);
    }

    #[test]
    fn percentile_endpoints() {
        let xs = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((percentile(&xs, 0.0) - 1.0).abs() < 1e-12);
        assert!((percentile(&xs, 100.0) - 5.0).abs() < 1e-12);
        assert!((percentile(&xs, 50.0) - 3.0).abs() < 1e-12);
    }

    #[test]
    fn iqr_matches_reference() {
        // For [1..9], numpy P25=3, P75=7 → IQR=4.
        let xs: Vec<f64> = (1..=9).map(|x| x as f64).collect();
        assert!((iqr(&xs) - 4.0).abs() < 1e-12);
    }

    #[test]
    fn empty_inputs_are_zero() {
        assert_eq!(median(&[]), 0.0);
        assert_eq!(percentile(&[], 95.0), 0.0);
        assert_eq!(iqr(&[]), 0.0);
        assert_eq!(min_f(&[]), 0.0);
        assert_eq!(max_f(&[]), 0.0);
    }

    #[test]
    fn nan_is_filtered() {
        let xs = [1.0, f64::NAN, 3.0];
        assert!((median(&xs) - 2.0).abs() < 1e-12);
    }

    #[test]
    fn determinism_shuffled_equivalent() {
        let xs = [5.0, 1.0, 4.0, 2.0, 3.0];
        let ys = [3.0, 2.0, 1.0, 5.0, 4.0];
        assert_eq!(median(&xs), median(&ys));
        assert_eq!(percentile(&xs, 90.0), percentile(&ys, 90.0));
        assert_eq!(iqr(&xs), iqr(&ys));
    }
}
