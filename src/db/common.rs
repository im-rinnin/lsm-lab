use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::db::key::{KeySlice};
use crate::db::value::{Value, ValueSlice};

// None if value is deleted
pub type ValueSliceTag = Option<ValueSlice>;
pub type ValueWithTag = Option<Value>;


#[derive(PartialEq, Eq)]
pub struct KVPair((KeySlice, ValueSliceTag), usize);

/// input: sorted kv pair(by key), output: sorted kv pair
pub struct SortedKVIter<'a> {
    iters: Vec<&'a mut dyn Iterator<Item=(KeySlice, ValueSliceTag)>>,
    heap: BinaryHeap<Reverse<KVPair>>,
}

impl PartialOrd for KVPair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        unsafe {
            Some(self.0.0.data().cmp(other.0.0.data()))
        }
    }
}

impl Ord for KVPair {
    fn cmp(&self, other: &Self) -> Ordering {
        unsafe {
            self.0.0.data().cmp(other.0.0.data())
        }
    }
}

impl<'a> SortedKVIter<'a> {
    pub fn new(mut iters: Vec<&'a mut dyn Iterator<Item=(KeySlice, ValueSliceTag)>>) -> Self {
        let mut heap = BinaryHeap::new();

        for (p, iter_ref) in iters.iter_mut().enumerate() {
            let iter = iter_ref;
            if let Some(kv) = iter.next() {
                heap.push(Reverse(KVPair(kv, p)));
            }
        }
        SortedKVIter { iters, heap }
    }
}

impl<'a> Iterator for SortedKVIter<'a> {
    type Item = (KeySlice, ValueSliceTag);


    fn next(&mut self) -> Option<Self::Item> {
        let res = self.heap.pop();
        match res {
            Some(reversed_entry) => {
                let entry = reversed_entry.0;
                let iter_index = entry.1;
                let next = self.iters[iter_index].next();
                if let Some(e) = next {
                    self.heap.push(Reverse(KVPair(e, iter_index)));
                }
                return Some(entry.0);
            }
            None => { None }
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::from_utf8;

    use crate::db::common::SortedKVIter;
    use crate::db::key::{Key, KeySlice};
    use crate::db::value::{Value, ValueSlice};

    #[test]
    pub fn test_sorted_kviter() {
        let a = vec![(Key::new("a"), Value::new("a")), (Key::new("d"), Value::new("a"))];
        let b = vec![(Key::new("b"), Value::new("a")), (Key::new("s"), Value::new("a"))];
        let mut it = a.iter().map(|e| (KeySlice::new(e.0.data()),
                                       Some(ValueSlice::new(e.1.data()))));
        let mut it_b = b.iter().map(|e| (KeySlice::new(e.0.data()),
                                         Some(ValueSlice::new(e.1.data()))));
        let kv_iter = SortedKVIter::new(vec![&mut it, &mut it_b]);
        let mut s = String::new();
        for i in kv_iter {
            unsafe {
                s.push_str(from_utf8(i.0.data()).unwrap());
            }
        }
        assert_eq!(s, "abds");
    }
}
