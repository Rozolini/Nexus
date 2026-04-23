//! Core value types.

pub type Key = u128;

/// Bit 0: tombstone (logical delete). Other bits reserved.
pub mod record_flags {
    pub const TOMBSTONE: u32 = 1;
}
