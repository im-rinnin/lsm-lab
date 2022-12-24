use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use dashmap::{DashMap, ReadOnlyView};

use crate::db::common::{KVIterItem, ValueSliceTag, ValueWithTag};
use crate::db::key::{Key, KeySlice};
use crate::db::value::{Value, ValueSlice};

pub struct Memtable {
    hash_map: DashMap<Key, ValueWithTag>,
    size: AtomicUsize,
}

pub struct MemtableIter(
    BinaryHeap<Reverse<(KeySlice, ValueSliceTag)>>
);

impl Memtable {
    pub fn new() -> Self {
        Memtable { hash_map: DashMap::new(), size: AtomicUsize::new(0) }
    }

    pub fn iter(&self) -> MemtableIter {
        let mut iter = self.hash_map.iter();
        let mut heap = BinaryHeap::new();
        for i in iter {
            let p: (&Key, &ValueWithTag) = i.pair();
            let k = KeySlice::new(p.0.data());
            let v = if let Some(n) = p.1 {
                Some(ValueSlice::new(n.data()))
            } else {
                None
            };
            heap.push(Reverse((k, v)));
        }
        MemtableIter(heap)
    }

    pub fn insert(&mut self, key: &Key, value: &Value) {
        let size = key.len() + value.len();
        self.hash_map.insert(key.clone(), Some(value.clone()));
        *self.size.get_mut() += size;
    }
    pub fn get<>(&self, key: &Key) -> Option<Value> {
        if let Some(i) = self.hash_map.get(key) {
            if let Some(j) = &(*i) {
                return Some(j.clone());
            }
        }
        None
    }
    pub fn delete(&mut self, key: &Key) -> Option<Value> {
        let res = self.hash_map.insert(key.clone(), None);
        *self.size.get_mut() += key.len();
        if let Some(i) = res {
            return i;
        }
        None
    }
    pub fn size(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }
}

impl MemtableIter {
    pub fn has_next(&self) -> bool {
        !self.0.is_empty()
    }
}

impl Iterator for MemtableIter {
    type Item = KVIterItem;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(reversed_kv) = self.0.pop() {
            let (k, v) = reversed_kv.0;
            return Some((k, v));
        }
        None
    }
}

#[cfg(test)]
mod test {
    use crate::db::key::Key;
    use crate::db::memtable::Memtable;
    use crate::db::value::Value;

    #[test]
    fn test_memtable_get_set_delete() {
        let mut memtable = Memtable::new();
        memtable.insert(&Key::new("a"), &Value::new("a"));
        memtable.insert(&Key::new("b"), &Value::new("b"));
        memtable.insert(&Key::new("c"), &Value::new("c"));

        assert_eq!(memtable.size(), 6);

        assert_eq!(memtable.get(&Key::new("a")).unwrap(), Value::new("a"));
        assert_eq!(memtable.get(&Key::new("b")).unwrap(), Value::new("b"));
        memtable.insert(&Key::new("a"), &Value::new("aa"));
        // size add 3
        assert_eq!(memtable.get(&Key::new("a")).unwrap(), Value::new("aa"));
        // size add 1
        assert_eq!(memtable.delete(&Key::new("c")).unwrap(), Value::new("c"));
        assert_eq!(memtable.size(), 10);
        assert!(memtable.get(&Key::new("c")).is_none());
    }

    #[test]
    fn test_memtable_iter() {
        let mut memtable = Memtable::new();
        memtable.insert(&Key::new("a"), &Value::new("a"));
        memtable.insert(&Key::new("c"), &Value::new("c"));
        memtable.insert(&Key::new("b"), &Value::new("b"));

        let mut it = memtable.iter();
        assert!(it.has_next());
        let mut s = String::new();
        while it.has_next() {
            let i = it.next().unwrap();
            s.push_str(&i.0.to_string())
        }
        assert_eq!(s, "abc");
    }
}
