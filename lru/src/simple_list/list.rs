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
use std::fmt::{Display, Formatter, Pointer};
use std::hash::Hash;

const GC_THRESHOLD: i64 = 100;

// Thread safe list
pub struct List<K: Copy + PartialOrd, V> {
    head: AtomicPtr<Node<K, V>>,
    lock: Arc<RwLock<()>>,
    gc_lock: Arc<Mutex<()>>,
    deleted_counter: AtomicI64,
    gc_threshold: i64,
    enable_gc: bool,
}

pub struct ListIterator<'a, K: Copy + PartialOrd, V> {
    lock: RwLockReadGuard<'a, ()>,
    node: *mut Node<K, V>,
}

pub struct ListSearchResult<K: Copy + PartialOrd, V> {
    pub last_node_less_or_equal: *mut Node<K, V>,
    pub next_node: *mut Node<K, V>,
}

impl<K: Copy + PartialOrd, V> ListSearchResult<K, V> {
    pub fn new(last_node_less_or_equal: *mut Node<K, V>, next_node: *mut Node<K, V>) -> Self {
        ListSearchResult {
            last_node_less_or_equal,
            next_node,
        }
    }
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
        let mut res = List::new();
        res.gc_threshold = threshold;
        res
    }
    pub fn with_no_gc() -> List<K, V> {
        let mut res = List::new();
        res.enable_gc = false;
        res
    }
    pub fn new() -> List<K, V> {
        List {
            head: AtomicPtr::new(null_mut()),
            lock: Arc::new(RwLock::new(())),
            gc_lock: Arc::new(Mutex::new(())),
            deleted_counter: AtomicI64::new(0),
            gc_threshold: GC_THRESHOLD,
            enable_gc: true,
        }
    }

    pub fn len(&self) -> usize {
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
        res
    }

    pub fn add(&self, key: K, value: V) -> Option<*mut Node<K, V>> {
        // lock
        let read_lock = self.lock.read().unwrap();

        let start_node = self.head.load(Ordering::SeqCst);
        self.cas_insert(start_node, key, value)
    }

    pub fn cas_insert_from_head(&self, key: K, value: V) -> Option<*mut Node<K, V>> {
        self.cas_insert(self.head.load(Ordering::SeqCst), key, value)
    }
    // not thread safe
    pub fn clean_deleted_node_with_start(start_node: &mut Node<K, V>) {
        assert!(!start_node.is_deleted());
        let mut node = start_node;
        loop {
            let next_node_ptr = &mut node.next_ptr;
            if next_node_ptr.load(Ordering::SeqCst).is_null() {
                break;
            }
            unsafe {
                let next_node = next_node_ptr.load(Ordering::SeqCst).as_mut().unwrap();
                if next_node.is_deleted() {
                    node.next_ptr
                        .store(next_node.next_ptr.load(Ordering::SeqCst), Ordering::SeqCst);
                    drop(next_node);
                } else {
                    node = next_node;
                }
            }
        }
    }

    pub fn clean_deleted_node(&mut self) {
        unsafe {
            // find first node not deleted
            let mut node_ptr = self.head.load(Ordering::SeqCst);
            loop {
                if node_ptr.is_null() {
                    return;
                }
                let node = node_ptr.as_ref().unwrap();
                if node.is_deleted() {
                    drop(node_ptr);
                    node_ptr = node.next_ptr.load(Ordering::SeqCst);
                } else {
                    break;
                }
            }
            //     set head to found
            self.head.store(node_ptr, Ordering::SeqCst);

            List::clean_deleted_node_with_start(node_ptr.as_mut().unwrap());
        }
    }

    // return none if overwrite
    pub fn cas_insert(
        &self,
        mut start_node: *mut Node<K, V>,
        key: K,
        value: V,
    ) -> Option<*mut Node<K, V>> {
        let value_ptr = Box::into_raw(Box::new(value));
        let new_node_ptr = Box::into_raw(Box::new(Node::with_key(key)));
        unsafe {
            new_node_ptr.as_mut().unwrap().set_value(value_ptr);
        }
        loop {
            let found_res = List::get_last_node_eq_or_less(key, start_node);
            match found_res {
                // key match, overwrite value
                Some(ListSearchResult {
                    last_node_less_or_equal,
                    next_node,
                }) => {
                    // set next loop start_node to current found node
                    let node;
                    unsafe {
                        node = last_node_less_or_equal.as_mut().unwrap();
                    }
                    start_node = last_node_less_or_equal;
                    assert!(node.get_key() <= key);

                    // overwrite
                    if !node.is_deleted() && node.get_key().eq(&key) {
                        unsafe {
                            // clean value ptr
                            new_node_ptr.as_mut().unwrap().set_value(null_mut());
                            let n = node;
                            n.set_value(value_ptr);
                            // drop node
                            new_node_ptr.drop_in_place();
                        }
                        return None;
                    } else {
                        // try insert
                        // if n is not delete n.key < key
                        assert!((!node.is_deleted() && node.get_key() < key) || node.is_deleted());
                        unsafe {
                            new_node_ptr.as_mut().unwrap().set_next_ptr(next_node);
                        }
                        // try cas
                        let cas_res = node.cas_next_ptr(next_node, new_node_ptr);
                        if cas_res {
                            return Some(new_node_ptr);
                        }
                    }
                }
                // insert to head if not found
                None => {
                    let current_head = self.head.load(Ordering::SeqCst);
                    // start node is diff, need search again
                    if start_node != current_head {
                        start_node = self.head.load(Ordering::SeqCst);
                        continue;
                    }
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
                        return Some(new_node_ptr);
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
        let node = List::get_last_node_eq_or_less(key, self.head.load(Ordering::SeqCst));
        if let Some(ListSearchResult {
            last_node_less_or_equal,
            next_node,
        }) = node
        {
            unsafe {
                let last_node_less_or_equal = last_node_less_or_equal.as_mut().unwrap();
                last_node_less_or_equal.set_deleted();
            }
            let count = self.deleted_counter.fetch_add(1, Ordering::SeqCst);
            // need drop lock first, gc need write lock
            drop(read_lock);
            // do gc if need
            if !self.enable_gc {
                return;
            }
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

    pub fn head(&self) -> *mut Node<K, V> {
        self.head.load(Ordering::SeqCst)
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
        gc_count
    }

    // need to check if deleted
    pub fn get_last_node_eq_or_less(
        key: K,
        start_node: *mut Node<K, V>,
    ) -> Option<ListSearchResult<K, V>>
// Option<(*mut Node<K, V>, *mut Node<K, V>)>
    {
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
                let res = ListSearchResult::new(last_node_ptr, node_ptr);
                // let res = Some((last_node_ptr.as_mut().unwrap(), node_ptr));
                if node_ptr.is_null() {
                    return Some(res);
                }
                let node_key = node_ptr.as_ref().unwrap().get_key();
                if node_key > key {
                    return Some(res);
                }
                last_node_ptr = node_ptr;
                node_ptr = node_ptr.as_ref().unwrap().get_next();
            }
        }
    }
}

impl<K: Copy + PartialOrd, V: Clone> List<K, V> {
    pub fn get(&self, key: K) -> Option<V> {
        let read_lock = self.lock.read().unwrap();
        let res = List::get_last_node_eq_or_less(key, self.head.load(Ordering::SeqCst));
        if let Some(ListSearchResult {
            last_node_less_or_equal,
            next_node,
        }) = res
        {
            unsafe {
                let last_node_less_or_equal = last_node_less_or_equal.as_ref().unwrap();
                if !last_node_less_or_equal.is_deleted() && last_node_less_or_equal.get_key() == key
                {
                    return Some(last_node_less_or_equal.get_value());
                }
            }
        }
        None
    }
}

impl<K: Copy + PartialOrd + Display, V: Clone + Display> Display for List<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let iter = self.to_iter();
        let mut res = String::new();
        for i in iter {
            let s = format!("({}:{}:{})", i.get_key(), i.get_value(), i.is_deleted());
            res.push_str(s.as_str());
        }
        write!(f, "{}", res)
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
        assert_eq!(
            format!("{}", list),
            "(0:0:false)(1:1:false)(3:3:false)(5:5:false)"
        );
    }

    #[test]
    fn test_50_times() {
        for i in 0..50 {
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
                if list.get(i).is_none() {
                    println!("{}", list)
                }
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

    #[test]
    fn test_remove_and_add() {
        let list = list::List::new();
        list.add(1, 2);
        list.add(2, 2);
        list.delete(2);
        list.add(2, 3);
        assert_eq!(list.get(2).unwrap(), 3);
        list.delete(2);
        list.add(2, 4);
        assert_eq!(list.get(2).unwrap(), 4);
        assert_eq!(
            format!("{}", list),
            "(1:2:false)(2:2:true)(2:3:true)(2:4:false)"
        );
    }
    #[test]
    fn test_clean_delete_node() {
        let mut l = List::new();
        for i in 0..10 {
            l.add(i, i);
        }

        l.delete(3);
        l.delete(0);
        l.delete(9);
        l.delete(5);

        l.clean_deleted_node();
        //     asset
    }

    #[test]
    fn test_clean_delete_node() {
        let mut l = List::new();

        // delete empty list
        l.delete(0);
        l.clean_deleted_node();
        assert_eq!(format!("{}", l), "");

        l.delete(3);
        for i in 0..10 {
            l.add(i, i);
        }

        assert_eq!(format!("{}", l), "(0:0:false)(1:1:false)(2:2:false)(3:3:false)(4:4:false)(5:5:false)(6:6:false)(7:7:false)(8:8:false)(9:9:false)");

        l.delete(3);
        l.delete(0);
        l.delete(9);
        l.delete(5);

        l.clean_deleted_node();
        assert_eq!(
            format!("{}", l),
            "(1:1:false)(2:2:false)(4:4:false)(6:6:false)(7:7:false)(8:8:false)"
        );
        assert_eq!(l.len(), 6);

        // no node is deleted
        l.clean_deleted_node();
        assert_eq!(
            format!("{}", l),
            "(1:1:false)(2:2:false)(4:4:false)(6:6:false)(7:7:false)(8:8:false)"
        );
        assert_eq!(l.len(), 6);
    }

    use super::ListSearchResult;

    #[test]
    fn test_search_from_node() {
        let list = list::List::new();
        list.add(1, 1);
        let n = list.add(2, 2);
        list.add(3, 3);
        list.add(6, 6);
        list.add(5, 5);
        let res = List::get_last_node_eq_or_less(4, n.unwrap());
        unsafe {
            let ListSearchResult {
                last_node_less_or_equal,
                next_node,
            } = res.unwrap();
            assert_eq!(last_node_less_or_equal.as_ref().unwrap().get_key(), 3);
            let k = next_node.as_ref().unwrap();
            assert_eq!(k.get_key(), 5);
        }
    }
}
