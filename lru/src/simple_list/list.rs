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
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::simple_list::node::Node;
use std::cell::RefCell;
use std::hash::Hash;

struct List<K: Copy + PartialOrd, V> {
    head: AtomicPtr<Node<K, V>>,
    lock: Arc<RwLock<()>>,
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
            let found_node = self.get_node_eq_or_less(key, start_node);
            match found_node {
                // key match, overwrite value
                Some(n) => {
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
                        // set next ptr
                        let next_ptr = n.get_next();
                        unsafe {
                            new_node_ptr.as_mut().unwrap().set_next_ptr(next_ptr);
                        }
                        // try cas
                        let cas_res = n.cas_next_ptr(new_node_ptr);
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
    fn get_node_eq_or_less(&self, key: K, start_node: *mut Node<K, V>) -> Option<&mut Node<K, V>> {
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
                if node_ptr.is_null() {
                    return Some(last_node_ptr.as_mut().unwrap());
                }
                let node_key = node_ptr.as_ref().unwrap().get_key();
                if node_key > key {
                    return Some(last_node_ptr.as_mut().unwrap());
                }
                last_node_ptr = node_ptr;
                node_ptr = node_ptr.as_ref().unwrap().get_next();
            }
        }
    }
}
impl<K: Copy + PartialOrd, V: Clone> List<K, V> {
    fn get(&self, key: K) -> Option<V> {
        let read_lock = self.lock.read().unwrap();
        let res = self.get_node_eq_or_less(key, self.head.load(Ordering::SeqCst));
        res.map(|n| n.get_value())
    }
}

#[cfg(test)]
mod test {
    use crate::simple_list::list;
    use std::sync::Arc;
    use std::thread::spawn;

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
    fn test_multiple_write_and_read() {
        let list = Arc::new(list::List::new());
        let mut joins = vec![];
        // list.add(-1, -1);
        for i in 1..100 {
            let list_clone = list.clone();
            // list_clone.add(i, i);
            let join = spawn(move || {
                list_clone.add(i, i * 1000);
            });
            joins.push(join);
        }
        for i in 1..100 {
            joins.pop().unwrap().join().unwrap();
        }

        for i in 1..100 {
            assert_eq!(list.get(i).unwrap(), i * 1000);
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
