//! # Read tracker
//!
//! Records which keys are read together *within a session* and feeds the
//! co-access graph. A "session" is an application-defined unit of logical
//! work (typically a batch read, a page render, a user request). The
//! tracker does not define what a session is — the caller opens one via
//! [`ReadTracker::open_session`] and closes it when done.
//!
//! The tracker is deliberately cheap: session membership is a small
//! `Vec<Key>`; co-access pairs are derived lazily via
//! [`events::deterministic_pair_downsample`], which caps pair production
//! at O(N log N) per session while keeping the selection reproducible.

pub mod events;
pub mod read_tracker;
pub mod session;

pub use events::{deterministic_pair_downsample, CoReadEvent, CoReadQuery};
pub use read_tracker::ReadTracker;
pub use session::ReadSession;
