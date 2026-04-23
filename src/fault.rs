//! Failure injection hooks. No-ops unless toggled; per-thread state for parallel tests.

use std::cell::Cell;

thread_local! {
    static FAIL_BEFORE_MANIFEST_SAVE: Cell<bool> = const { Cell::new(false) };
    static FAIL_BEFORE_RECORD_APPEND: Cell<bool> = const { Cell::new(false) };
    static FAIL_BEFORE_RELOCATE_FIRST_MANIFEST: Cell<bool> = const { Cell::new(false) };
}

/// Clears all injection flags on this thread (e.g. after a panicked test).
pub fn reset_fault_injection() {
    FAIL_BEFORE_MANIFEST_SAVE.with(|c| c.set(false));
    FAIL_BEFORE_RECORD_APPEND.with(|c| c.set(false));
    FAIL_BEFORE_RELOCATE_FIRST_MANIFEST.with(|c| c.set(false));
}

/// Backwards-compatible alias.
#[inline]
pub fn reset_fail_before_manifest_save() {
    reset_fault_injection();
}

pub fn set_fail_before_manifest_save(v: bool) {
    FAIL_BEFORE_MANIFEST_SAVE.with(|c| c.set(v));
}

pub fn set_fail_before_record_append(v: bool) {
    FAIL_BEFORE_RECORD_APPEND.with(|c| c.set(v));
}

pub fn set_fail_before_relocate_first_manifest_save(v: bool) {
    FAIL_BEFORE_RELOCATE_FIRST_MANIFEST.with(|c| c.set(v));
}

pub fn take_fail_before_manifest_save() -> bool {
    FAIL_BEFORE_MANIFEST_SAVE.with(|c| {
        let prev = c.get();
        c.set(false);
        prev
    })
}

pub fn take_fail_before_record_append() -> bool {
    FAIL_BEFORE_RECORD_APPEND.with(|c| {
        let prev = c.get();
        c.set(false);
        prev
    })
}

pub fn take_fail_before_relocate_first_manifest_save() -> bool {
    FAIL_BEFORE_RELOCATE_FIRST_MANIFEST.with(|c| {
        let prev = c.get();
        c.set(false);
        prev
    })
}
