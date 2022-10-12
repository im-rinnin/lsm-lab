use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::db::key::Key;
use crate::db::value::Value;

// None if value is deleted
pub type ValueRefWithTag<'a>=Option<&'a Value>;
pub type ValueWithTag =Option<Value>;


#[derive(PartialEq, Eq)]
pub struct KVPair<'a>(&'a (Key, Value), usize);

/// input: sorted kv pair(by key), output: sorted kv pair
struct SortedKVIter<'a> {
    iters: Vec<&'a mut dyn Iterator<Item=&'a (Key, Value)>>,
    heap: BinaryHeap<Reverse<KVPair<'a>>>,
}

impl<'a> PartialOrd for KVPair<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some((*self.0).0.cmp(&(*other.0).0))
    }
}

impl<'a> Ord for KVPair<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        (*self.0).0.cmp(&(*other.0).0)
    }
}

impl<'a> SortedKVIter<'a> {
    pub fn new(mut iters: Vec<&'a mut dyn Iterator<Item=&'a (Key, Value)>>) -> Self {
        let mut heap = BinaryHeap::new();

        for (p, iter) in iters.iter_mut().enumerate() {
            if let Some(kv) = iter.next() {
                heap.push(Reverse(KVPair(kv, p)));
            }
        }
        SortedKVIter { iters, heap }
    }
}

impl<'a> Iterator for SortedKVIter<'a> {
    type Item = &'a (Key, Value);


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
    use crate::db::common::SortedKVIter;
    use crate::db::key::Key;
    use crate::db::value::Value;

    #[test]
    pub fn test_sorted_kviter() {
        let mut a = vec![(Key::new("a"), Value::new("a")), (Key::new("d"), Value::new("a"))];
        let b = vec![(Key::new("b"), Value::new("a")), (Key::new("s"), Value::new("a"))];
        let it = &mut a.iter();
        let it_b = &mut b.iter();
        let kv_iter = SortedKVIter::new(vec![it, it_b]);
        let mut s = String::new();
        for i in kv_iter {
            s.push_str(i.0.to_string());
        }
        assert_eq!(s, "abds");
    }
}
