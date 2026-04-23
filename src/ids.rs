//! Stable identifiers.

use serde::{Deserialize, Serialize};

/// Monotonic segment identifier assigned by the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentId(pub u64);
