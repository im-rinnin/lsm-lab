use std::cmp::Reverse;
use std::collections::BinaryHeap;

use dashmap::{DashMap, ReadOnlyView};

use crate::db::common::{KVIterItem, ValueSliceTag, ValueWithTag};
use crate::db::key::{Key, KeySlice};
use crate::db::value::{Value, ValueSlice};

pub struct Memtable {
    hash_map: DashMap<Key, ValueWithTag>,
}

// pub struct MemtableReadOnly {
//     hash_map: DashMap<Key, ValueWithTag>,
// }

pub struct MemtableIter(
    BinaryHeap<Reverse<( Key,  ValueWithTag)>>
);

impl Memtable {
    pub fn new() -> Self {
        Memtable { hash_map: DashMap::new() }
    }

    pub fn iter(&self) -> MemtableIter {
        let mut iter = self.hash_map.iter();
        let mut heap = BinaryHeap::new();
        for i in iter {
            let a =i.pair();
            let k=a.0;
            let v=a.1;
            heap.push(Reverse((k.clone(), v.clone())));
        }
        MemtableIter(heap)
    }

    pub fn insert(&self, key: &Key, value: &Value) {
        self.hash_map.insert(key.clone(), Some(value.clone()));
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
        if let Some(i) = res {
            return i;
        }
        None
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
            let key_slice = KeySlice::new(k.data());
            let res = if v.is_some() {
                let a = v.as_ref().unwrap();
                Option::from(ValueSlice::new(a.data()))
            } else {
                None
            };
            return Some((key_slice, res));
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

        assert_eq!(memtable.get(&Key::new("a")).unwrap(), Value::new("a"));
        assert_eq!(memtable.get(&Key::new("b")).unwrap(), Value::new("b"));
        memtable.insert(&Key::new("a"), &Value::new("aa"));
        assert_eq!(memtable.get(&Key::new("a")).unwrap(), Value::new("aa"));
        assert_eq!(memtable.delete(&Key::new("c")).unwrap(), Value::new("c"));
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
