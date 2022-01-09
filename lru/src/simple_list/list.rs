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
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use std::time::Duration;

use crate::simple_list::node::test::Item;
use crate::simple_list::node::Node;
use std::alloc::handle_alloc_error;
use std::cell::RefCell;
use std::fmt::Display;
use std::hash::Hash;

const GC_THRESHOLD: i64 = 100;

struct List<K: Copy + PartialOrd, V> {
    head: AtomicPtr<Node<K, V>>,
    lock: Arc<RwLock<()>>,
    gc_lock: Arc<Mutex<()>>,
    deleted_counter: AtomicI64,
    gc_threshold: i64,
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
    pub fn with_gc_threshold(threshold: i64) -> List<K, V> {
        List {
            head: AtomicPtr::new(null_mut()),
            lock: Arc::new(RwLock::new(())),
            gc_lock: Arc::new(Mutex::new(())),
            deleted_counter: AtomicI64::new(0),
            gc_threshold: threshold,
        }
    }
    pub fn new() -> List<K, V> {
        List {
            head: AtomicPtr::new(null_mut()),
            lock: Arc::new(RwLock::new(())),
            gc_lock: Arc::new(Mutex::new(())),
            deleted_counter: AtomicI64::new(0),
            gc_threshold: GC_THRESHOLD,
        }
    }

    pub fn len(&self) -> isize {
        let read = self.lock.read().unwrap();
        let mut res = 0;
        let mut node_ptr = self.head.load(Ordering::SeqCst);
        loop {
            if node_ptr.is_null() {
                break;
            }
            unsafe {
                let node = node_ptr.as_ref().unwrap();
                if !node.is_deleted() {
                    res += 1;
                }
                node_ptr = node.get_next();
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
    }

    pub fn delete(&self, key: K) {
        // lock
        let read_lock = self.lock.read().unwrap();
        // 1. find ,return if fail
        let node = self.get_node_eq_or_less(key, self.head.load(Ordering::SeqCst));
        if let Some((n, b)) = node {
            n.set_deleted();
            let count = self.deleted_counter.fetch_add(1, Ordering::SeqCst);
            // need drop lock first, gc need write lock
            drop(read_lock);
            // do gc if need
            if count > self.gc_threshold {
                // only one thread can do gc
                let gc_lock = self.gc_lock.try_lock();
                if gc_lock.is_ok() {
                    let gc_count = self.gc();
                    self.deleted_counter
                        .fetch_sub(gc_count as i64, Ordering::SeqCst);
                }
            }
        }
    }

    pub fn to_iter(&self) -> ListIterator<K, V> {
        ListIterator::new(self)
    }

    // --------------------------private-----------------

    // lock list stop other thread access until gc finish
    fn gc(&self) -> i32 {
        let w_lock = self.lock.write().unwrap();
        let mut gc_count = 0;
        // check if is null
        let head_ptr = self.head.load(Ordering::SeqCst);
        if head_ptr.is_null() {
            return gc_count;
        }
        // find first node is not delete
        let mut node_ptr = head_ptr;
        loop {
            // not found
            if node_ptr.is_null() {
                break;
            }
            unsafe {
                let node = node_ptr.as_ref().unwrap();
                if !node.is_deleted() {
                    break;
                }
                // delete node if is delete
                node_ptr.drop_in_place();
                gc_count += 1;

                node_ptr = node.get_next();
            }
        }

        if node_ptr.is_null() {
            return gc_count;
        }
        let first_live_node_ptr = node_ptr;

        unsafe {
            let mut last_node_ptr = node_ptr;
            let mut current_node_ptr = node_ptr.as_ref().unwrap().get_next();
            while !current_node_ptr.is_null() {
                let current_node = current_node_ptr.as_ref().unwrap();
                if current_node.is_deleted() {
                    // update last node ptr
                    let last_node = last_node_ptr.as_mut().unwrap();
                    last_node.set_next_ptr(current_node.get_next());
                    //     drop current node
                    // drop(current_node);
                    current_node_ptr.drop_in_place();
                    gc_count += 1;
                    //     set next node
                    current_node_ptr = current_node.get_next();
                } else {
                    last_node_ptr = current_node_ptr;
                    current_node_ptr = current_node.get_next();
                }
            }
        }
        return gc_count;
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
            if !n.0.is_deleted() && n.0.get_key() == key {
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
    use crate::simple_list::list::List;
    use crate::simple_list::node::test::Item;
    use std::borrow::{Borrow, BorrowMut};
    use std::cell::RefCell;
    use std::env::temp_dir;
    use std::rc::Rc;
    use std::sync::atomic::Ordering;
    use std::sync::{mpsc, Arc};
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
    fn test_simple_remove() {
        let list = list::List::new();
        list.add(1, 1);
        assert_eq!(list.get(1).unwrap(), 1);
        list.delete(0);
        assert_eq!(list.get(1).unwrap(), 1);
        list.delete(1);
        list.delete(1);
        assert!(list.get(1).is_none());
        assert_eq!(list.len(), 0);
    }
    #[test]
    fn test_remove_get_in_two_thread() {
        let list = Arc::new(list::List::new());
        list.add(1, 3);
        let list_clone = list.clone();
        let j = spawn(move || {
            list_clone.delete(1);
        });
        j.join().unwrap();
        assert!(list.get(1).is_none());
    }
    #[test]
    fn test_list_gc_all_node_is_deleted() {
        let list: List<i32, i32> = list::List::new();
        list.add(1, 1);
        list.add(2, 1);
        list.add(3, 1);
        list.delete(3);
        list.delete(1);
        list.delete(2);
        assert_eq!(list.len(), 0);
    }
    #[test]
    fn test_empty_list_gc() {
        let list: List<i32, i32> = list::List::new();
        assert_eq!(list.gc(), 0);
    }
    #[test]
    fn test_only_gc() {
        let count = Rc::new(RefCell::new(0));
        let list = list::List::new();
        // (delete)->(delete)->(alive)->(delete)->(alive)->(alive)
        for i in 0..6 {
            let item = Item::new(count.clone());
            list.add(i, item);
        }
        list.delete(0);
        list.delete(1);
        list.delete(3);
        assert_eq!(list.len(), 3);
        assert_eq!(list.gc(), 3);
        assert_eq!(*(count.borrow() as &RefCell<i32>).borrow_mut(), 3);
    }
    #[test]
    fn test_all() {}
    #[test]
    fn test_gc_with_gc_checker() {
        let list = list::List::with_gc_threshold(3);
        let list_cloned = Arc::new(list);
        let mut joins = vec![];
        for i in 0..6 {
            let l = list_cloned.clone();
            joins.push(spawn(move || {
                l.add(i, i);
                if i % 2 == 0 {
                    l.delete(i);
                }
            }))
        }

        for i in joins {
            i.join().unwrap();
        }

        assert_eq!(list_cloned.deleted_counter.load(Ordering::SeqCst), 3);

        //     add count to drop, check node gc is working ,no memory leak
    }
}
