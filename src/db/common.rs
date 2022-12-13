use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fmt::{Display, Formatter};

use crate::db::key::KeySlice;
use crate::db::value::{Value, ValueSlice};

// None if value is deleted
pub type ValueSliceTag = Option<ValueSlice>;
pub type ValueWithTag = Option<Value>;
pub type KVIterItem = (KeySlice, ValueSliceTag);


#[derive(PartialEq, Eq)]
pub struct KVPair(KVIterItem, usize);

/// input: sorted kv pair(by key), output: sorted kv pair
/// if find same key, return the kv from the iter which was the smallest number in the input iter vec
/// that is to say, overwrite priority is decided by the order in the iters.eg iters[0]>iters[1]>..>iters[n]
pub struct SortedKVIter<'a> {
    iters: Vec<&'a mut dyn Iterator<Item=KVIterItem>>,
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
    pub fn new(mut iters: Vec<&'a mut dyn Iterator<Item=KVIterItem>>) -> Self {
        let mut heap = BinaryHeap::new();

        for (p, iter_ref) in iters.iter_mut().enumerate() {
            let iter = iter_ref;
            if let Some(kv) = iter.next() {
                heap.push(Reverse(KVPair(kv, p)));
            }
        }
        SortedKVIter { iters, heap }
    }

    pub fn has_next(&self) -> bool {
        self.top().is_some()
    }
    fn top(&self) -> Option<&KVPair> {
        let res = self.heap.peek();
        res.map(|f| &f.0)
    }
    fn pop_min(&mut self) -> Option<KVPair> {
        let res = self.heap.pop();
        match res {
            Some(reversed_entry) => {
                let entry = reversed_entry.0;
                let iter_index = entry.1;
                let next = self.iters[iter_index].next();
                if let Some(e) = next {
                    self.heap.push(Reverse(KVPair(e, iter_index)));
                }
                return Some(entry);
            }
            None => { None }
        }
    }
}

impl<'a> Iterator for SortedKVIter<'a> {
    type Item = KVIterItem;


    fn next(&mut self) -> Option<Self::Item> {
//         pop one,check if exits
        let res_option = self.pop_min();

        if res_option.is_none() {
            return None;
        }
//         loop check top and pop until key is not same
//         return kv from smallest iter
        let mut res = res_option.unwrap();

        loop {
            let top_option = self.top();
            if top_option.is_none() {
                break;
            }
            let res_kv = res.0;
            let res_key = res.0.0;
            let res_iter_position = res.1;

            let top = top_option.unwrap();
            let top_key = top.0.0;
            let top_iter_position = top.1;

            if !top_key.eq(&res_key) {
                return Some(res_kv);
            } else {
                // pop top
                let top_popped = self.pop_min();
                if top_iter_position < res_iter_position {
                    // set res to top
                    res = top_popped.unwrap();
                }
            }
        }
        Some(res.0)
    }
}

#[cfg(test)]
mod test {
    use std::str::from_utf8;

    use crate::db::common::SortedKVIter;
    use crate::db::key::{Key, KeySlice};
    use crate::db::value::{Value, ValueSlice};

    #[test]
    pub fn test_sorted_kv_iter() {
        // a,b,c,f
        let a = vec![(Key::new("a"), Value::new("a1")), (Key::new("b"), Value::new("b1")), (Key::new("c"), Value::new("c1")), (Key::new("f"), Value::new("f1"))];
        // a,b,e
        let b = vec![(Key::new("a"), Value::new("a2")), (Key::new("b"), Value::new("b2")), (Key::new("e"), Value::new("e2"))];
        // b,d,e
        let c = vec![(Key::new("b"), Value::new("b3")), (Key::new("d"), Value::new("d3")), (Key::new("e"), Value::new("e3"))];
        let mut it_a = a.iter().map(|e| (KeySlice::new(e.0.data()),
                                         Some(ValueSlice::new(e.1.data()))));
        let mut it_b = b.iter().map(|e| (KeySlice::new(e.0.data()),
                                         Some(ValueSlice::new(e.1.data()))));
        let mut it_c = c.iter().map(|e| (KeySlice::new(e.0.data()),
                                         Some(ValueSlice::new(e.1.data()))));
        // kv in a will overwrite b,c and b will overwrite c
        let kv_iter = SortedKVIter::new(vec![&mut it_a, &mut it_b, &mut it_c]);
        let mut s = String::new();
        for (_, value) in kv_iter {
            unsafe {
                s.push_str(from_utf8(value.unwrap().data()).unwrap());
            }
        }
        assert_eq!(s, "a1b1c1d3e2f1");
    }

    #[test]
    pub fn test_sorted_kv_iter_top() {
        let a = vec![(Key::new("a"), Value::new("a1")), (Key::new("b"), Value::new("b1"))];
        let mut it_a = a.iter().map(|e| (KeySlice::new(e.0.data()),
                                         Some(ValueSlice::new(e.1.data()))));
        let mut kv_iter = SortedKVIter::new(vec![&mut it_a]);
        kv_iter.next();
        assert!(kv_iter.has_next());
        kv_iter.next();
        assert!(!kv_iter.has_next());
    }
}
