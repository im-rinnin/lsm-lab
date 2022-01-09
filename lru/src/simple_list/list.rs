#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
use core::num::FpCategory::Nan;
use std::borrow::Borrow;
use std::borrow::Cow::Borrowed;
use std::fs::read_to_string;
use std::ptr::{null, null_mut};
use std::rc::Rc;
use std::sync::atomic::{AtomicI64, AtomicI8, AtomicPtr, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::Duration;

use crate::simple_list::node::Node;
use std::cell::RefCell;
use std::fmt::Display;
use std::hash::Hash;

struct List<K: Copy + PartialOrd, V> {
    head: AtomicPtr<Node<K, V>>,
    lock: Arc<RwLock<()>>,
}

struct ListIterator<'a, K: Copy + PartialOrd, V> {
    lock: RwLockReadGuard<'a, ()>,
    node: *mut Node<K, V>,
}

impl<'a, K: Copy + PartialOrd, V> ListIterator<'a, K, V> {
    pub fn new(list: &'a List<K, V>) -> Self {
        let rd_lock = list.lock.read().unwrap();
        ListIterator {
            lock: rd_lock,
            node: list.head.load(Ordering::SeqCst),
        }
    }
}

impl<'a, K: 'a + Copy + PartialOrd, V: 'a> Iterator for ListIterator<'a, K, V> {
    type Item = &'a Node<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match !self.node.is_null() {
            true => unsafe {
                let res = self.node.as_ref().unwrap();
                self.node = res.get_next();
                Some(res)
            },
            false => None,
        }
    }
}

impl<K: Copy + PartialOrd, V> List<K, V> {
    pub fn new() -> List<K, V> {
        List {
            head: AtomicPtr::new(null_mut()),
            lock: Arc::new(RwLock::new(())),
        }
    }

    pub fn remove(&self) {
        unimplemented!()
    }

    pub fn len(&self) -> isize {
        let read = self.lock.read().unwrap();
        let mut res = 0;
        let mut node_ptr = self.head.load(Ordering::SeqCst);
        loop {
            if node_ptr.is_null() {
                break;
            }
            res += 1;
            unsafe {
                node_ptr = node_ptr.as_ref().unwrap().get_next();
            }
        }
        return res;
    }

    pub fn add(&self, key: K, value: V) {
        // lock
        let read_lock = self.lock.read().unwrap();

        let value_ptr = Box::into_raw(Box::new(value));
        let new_node_ptr = Box::into_raw(Box::new(Node::with_key(key)));

        unsafe {
            new_node_ptr.as_mut().unwrap().set_value(value_ptr);
        }
        let mut start_node = self.head.load(Ordering::SeqCst);
        loop {
            let found_res = self.get_node_eq_or_less(key, start_node);
            match found_res {
                // key match, overwrite value
                Some((n, current_next)) => {
                    // set next loop start_node to current found node
                    start_node = n;
                    assert!(n.get_key() <= key);

                    if n.is_deleted() {
                        return;
                    }

                    // overwrite
                    if n.get_key().eq(&key) {
                        unsafe {
                            // clean value ptr
                            new_node_ptr.as_mut().unwrap().set_value(null_mut());
                            n.set_value(value_ptr);
                        }
                        // drop node
                        drop(new_node_ptr);
                        break;
                    } else {
                        // try insert
                        assert!(n.get_key() < key);
                        unsafe {
                            new_node_ptr.as_mut().unwrap().set_next_ptr(current_next);
                        }
                        // try cas
                        let cas_res = n.cas_next_ptr(current_next, new_node_ptr);
                        if cas_res {
                            return;
                        }
                    }
                }
                // insert to head if not found
                None => {
                    let current_head = self.head.load(Ordering::SeqCst);
                    // set next to head
                    unsafe {
                        new_node_ptr.as_mut().unwrap().set_next_ptr(current_head);
                    }
                    let cas_res = self.head.compare_exchange(
                        current_head,
                        new_node_ptr,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    );

                    if cas_res.is_ok() {
                        return;
                    }
                    //     cas fail ,head is update, set start node to current head
                    start_node = self.head.load(Ordering::SeqCst);
                }
            }
        }
        // todo check if need gc fetch gc lock ,do gc
    }

    // fn try_insert(&self,node_before:AtomicPtr<Node<K,V>>,new_node:&mut Node<K,V>)->bool{
    //     unimplemented!()
    // }

    // pub fn overwrite(&self, value:node: &Node<K, V>) {

    // 3. create new node, set next point
    // 4. cas pre node point until success, if fail return to 2
    // 5. check if need gc
    // }
    pub fn delete(&self, key: i32) {
        // lock
        // 1. find ,return if fail
        // 2. set delete state
        // 3. cas previous node ptr to next ,return if success
        //  4. if fail ,to step 1,start in previous node,find the new previos node
        unimplemented!()
    }

    pub fn to_iter(&self) -> ListIterator<K, V> {
        ListIterator::new(self)
    }

    // --------------------------private-----------------

    // check if need check
    // lock gc prevent other thread do gc
    fn need_gc(&self) -> bool {
        unimplemented!()
    }

    // lock list stop other thread access until gc finish
    fn gc(&self) {
        //
        unimplemented!()
    }

    // need to check if deleted
    fn get_node_eq_or_less(
        &self,
        key: K,
        start_node: *mut Node<K, V>,
    ) -> Option<(&mut Node<K, V>, *mut Node<K, V>)> {
        let mut node_ptr = start_node;
        if node_ptr.is_null() {
            return None;
        }
        // return none if is less than head
        unsafe {
            let node_key = node_ptr.as_ref().unwrap().get_key();
            if node_key > key {
                return None;
            }
            let mut last_node_ptr = node_ptr;

            loop {
                let res = Some((last_node_ptr.as_mut().unwrap(), node_ptr));
                if node_ptr.is_null() {
                    return res;
                }
                let node_key = node_ptr.as_ref().unwrap().get_key();
                if node_key > key {
                    return res;
                }
                last_node_ptr = node_ptr;
                node_ptr = node_ptr.as_ref().unwrap().get_next();
            }
        }
    }
}
impl<K: Copy + PartialOrd + Display, V: Clone + Display> List<K, V> {
    fn get(&self, key: K) -> Option<V> {
        let read_lock = self.lock.read().unwrap();
        let res = self.get_node_eq_or_less(key, self.head.load(Ordering::SeqCst));
        if let Some(n) = res {
            if n.0.get_key() == key {
                return Some(n.0.get_value());
            }
        }
        return None;
    }

    fn to_str(&self) -> String {
        let iter = self.to_iter();
        let mut res = String::new();
        for i in iter {
            let s = format!("({}:{})", i.get_key(), i.get_value());
            res.push_str(s.as_str());
        }
        res
    }
}

#[cfg(test)]
mod test {
    use crate::simple_list::list;
    use std::env::temp_dir;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::thread::spawn;
    use std::time::Duration;

    #[test]
    fn test_one_write() {
        let list = list::List::new();
        list.add(1, 1);
        let res = list.get(1).unwrap();
        assert_eq!(res, 1);
        assert!(list.get(2).is_none());
    }
    #[test]
    fn test_five_write() {}
    #[test]
    fn test_order() {
        let list = list::List::new();
        list.add(3, 3);
        list.add(1, 1);
        list.add(5, 5);
        list.add(0, 0);
        assert_eq!(list.to_str(), "(0:0)(1:1)(3:3)(5:5)");
    }
    #[test]
    fn test_10_times() {
        for i in 0..10 {
            test_multiple_write_and_read();
        }
    }

    fn test_multiple_write_and_read() {
        let list = Arc::new(list::List::new());
        let mut joins = vec![];

        for i in 0..100 {
            let list_clone = list.clone();
            // list_clone.add(i, i);
            let join = spawn(move || {
                list_clone.add(i, i);
            });
            joins.push(join);
        }
        // overwrite
        for i in (0..100).step_by(2) {
            let list_clone = list.clone();
            // list_clone.add(i, i);
            let join = spawn(move || {
                list_clone.add(i, i * 100);
            });
            joins.push(join);
        }

        for j in joins {
            j.join().unwrap();
        }

        for i in 0..100 {
            if i % 2 == 0 {
                assert_eq!(list.get(i).unwrap(), i * 100);
            } else {
                assert_eq!(list.get(i).unwrap(), i);
            }
        }
    }
    #[test]
    fn test_only_gc() {}
    #[test]
    fn test_all() {}
    #[test]
    fn test_gc() {
        //     add count to drop, check node gc is working ,no memory leak
    }
}
