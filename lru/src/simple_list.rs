use core::num::FpCategory::Nan;
use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicI64, AtomicI8};

struct Node {
    key: AtomicI64,
    ptr: *mut Node,
    state: AtomicI8,
}

struct List {
    head: *mut Node,
}

impl List {
    fn search_week(&self, key: i64) {}

    fn search_strong(&self, key: i64) {}
}
