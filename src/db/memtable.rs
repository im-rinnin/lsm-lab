use std::cmp::Reverse;
use std::collections::BinaryHeap;

use dashmap::DashMap;
use dashmap::iter::Iter;

use crate::db::common::{ValueRefWithTag, ValueWithTag};
use crate::db::key::Key;
use crate::db::value::Value;

pub struct Memtable {
    // Value is None if deleted
    hash_map: DashMap<Key, ValueWithTag>,
}

pub struct MemtableIter<'a>(
    // Iter<'a, Key, Option<Value>>
    BinaryHeap<Reverse<Key>>,
    &'a Memtable,
);

impl Memtable {
    pub fn new() -> Self {
        Memtable { hash_map: DashMap::new() }
    }

    pub fn insert(&mut self, key: &Key, value: &Value) {
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

    pub fn to_iter(&self) -> MemtableIter {
        let mut iter = self.hash_map.iter();

        let mut heap = BinaryHeap::new();
        for i in iter {
            heap.push(Reverse(i.key().clone()));
        }

        MemtableIter(heap, self)
    }
}


impl<'a> Iterator for MemtableIter<'a> {
    type Item = (&'a Key, &'a ValueWithTag);

    fn next(& mut self) -> Option<Self::Item> {
        let t = self.0.iter().next();
        for i in self.0.iter() {
            let read=self.1.hash_map.into_read_only();
            let a =read.get(&i.0);
            let entry_ref = self.1.hash_map.get(&i.0).expect("must exits");
            let v=entry_ref.value();
            let k=entry_ref.key();
            return Some((k,v))
        }
        // if let Some(reversed_key)= self.0.pop(){
        //     let key=reversed_key.0;
        //     let v = &*self.1.hash_map.get(&key).unwrap();
        //     return Some((key, v.clone()));
        // }
        // None
        todo!()
    }
}

#[cfg(test)]
mod test {
    use dashmap::DashMap;

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

        let it = memtable.to_iter();
        let mut s = String::new();
        for i in it {
            s.push_str(i.0.to_string())
        }
        assert_eq!(s, "abc");
    }
}
