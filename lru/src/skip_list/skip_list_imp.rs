#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use crate::rand::simple_rand::Rand;
use crate::simple_list::list::{List, ListSearchResult};
use crate::simple_list::node::Node;
use crate::skip_list::search_result::NodeSearchResult;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::Add;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

const MAX_LEVEL: usize = 16;

// todo need add arc,skip list need thread safe
struct SkipListImp<K: Copy + PartialOrd, V> {
    // levels len is MAX_LEVEL
    // not all level are in use
    levels: [Arc<List<K, Ref<K, V>>>; MAX_LEVEL],
    base: Arc<List<K, V>>,
    // gc need stop all other thread
    // gc thread: fetch write lock
    // other thread: fetch read lock
    lock: RwLock<()>,
    current_max_level: AtomicUsize,
}

pub enum Ref<K: Copy + PartialOrd, V> {
    Base(*mut Node<K, V>),
    Level(*mut Node<K, Ref<K, V>>),
}

impl<K: Copy + PartialOrd + Display, V: Clone + Display> Display for SkipListImp<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut res = String::new();
        for i in 1..self.current_max_level.load(Ordering::SeqCst) {
            let str = format!("{}", self.levels.get(i).unwrap());
            res = res.add(str.as_str());
            res.push_str("\n");
        }
        let str = format!("{}", (self.base.borrow() as &List<K, V>));
        res = res.add(str.as_str());
        res.push_str("\n");
        write!(f, "{}", res)
    }
}

impl<K: Copy + PartialOrd + Display, V> Display for Ref<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unsafe {
            match self {
                Ref::Base(n) => {
                    write!(f, "(ref base {})", n.as_ref().unwrap().get_key())
                }
                Ref::Level(n) => {
                    write!(f, "(ref level {})", n.as_ref().unwrap().get_key())
                }
            }
        }
    }
}

impl<K: Copy + PartialOrd, V> Clone for Ref<K, V> {
    fn clone(&self) -> Self {
        match self {
            Ref::Level(n) => Ref::Level(n.clone()),
            Ref::Base(n) => Ref::Base(n.clone()),
        }
    }
}

impl<K: Copy + PartialOrd, V> SkipListImp<K, V> {
    fn new() -> Self {
        let array = [
            Arc::new(List::new()),
            Arc::new(List::new()),
            Arc::new(List::new()),
            Arc::new(List::new()),
            Arc::new(List::new()),
            Arc::new(List::new()),
            Arc::new(List::new()),
            Arc::new(List::new()),
            Arc::new(List::new()),
            Arc::new(List::new()),
            Arc::new(List::new()),
            Arc::new(List::new()),
            Arc::new(List::new()),
            Arc::new(List::new()),
            Arc::new(List::new()),
            Arc::new(List::new()),
        ];
        SkipListImp {
            levels: array,
            base: Arc::new(List::with_no_gc()),
            lock: RwLock::new(()),
            current_max_level: AtomicUsize::new(0),
        }
    }

    pub fn add(&self, key: K, value: V, rand_int: usize) {
        // read lock
        let read_lock = self.lock.read().unwrap();
        // call search

        let search_result = self.search_node(key);
        let add_res = search_result.add_value_to_base(value);
        // some if insert new node
        if let Some(n) = add_res {
            // cas insert all index nodes
            let level = self.random_level(self.len(), rand_int);
            if level > 0 {
                let mut res = search_result.add_index_to_level(level, n);

                // app to head
                if level > search_result.get_index_level() {
                    for l in search_result.get_index_level() + 1..level + 1 {
                        let list: Arc<List<K, Ref<K, V>>> = self.get_index_level(l);
                        let cas_res = (list.borrow() as &List<K, Ref<K, V>>)
                            .cas_insert_from_head(key, res)
                            .unwrap();
                        res = Ref::Level(cas_res);
                    }
                    self.current_max_level.fetch_max(level, Ordering::SeqCst);
                }
            }
        }
    }
    pub fn remove(&self, key: K) {
        // read lock
        let read_lock = self.lock.read().unwrap();

        // call search node
        let search_result = self.search_node(key);
        // if found ,delete it
        search_result.delete_value();
        // unlock for gc
        drop(read_lock);
        // check if need gc
        self.gc_when_necessary();
    }
    // search node level by level
    // return last node less or equal key, node next
    // record index node in search path
    fn search_node(&self, key: K) -> NodeSearchResult<K, V> {
        let mut search_result = NodeSearchResult::new(key);
        // from max level, find first index level whose head is less or equal key
        let max_level = self.current_max_level();
        let mut base_start = None;
        // if only base, to (B)
        if max_level > 0 {
            let mut start_level_option = None;
            for level in (1..max_level + 1).rev() {
                let list = self.get_index_level(level);
                unsafe {
                    let a = list.head().as_ref().unwrap().get_key();
                    if list.len() != 0 && list.head().as_ref().unwrap().get_key() <= key {
                        start_level_option = Some(level);
                        break;
                    }
                }
            }
            if let Some(start_level) = start_level_option {
                let mut start_node = self.get_index_level(start_level).head();
                assert!(!start_node.is_null());
                for level in (1..start_level).rev() {
                    let list = self.get_index_level(level);
                    // res won't be none
                    // (A) call list search_last_node_less_or_equal, use list head as start
                    let res = List::get_last_node_eq_or_less(key, start_node).unwrap();
                    search_result.save_index_node(list, res.last_node_less_or_equal, res.next_node);
                    unsafe {
                        let child = res.last_node_less_or_equal.as_ref().unwrap().get_value();
                        match child {
                            // use child as start node, repeat A
                            Ref::Level(n) => start_node = n,
                            // if child is base, break
                            Ref::Base(n) => {
                                // current level is level one, will break
                                assert_eq!(level, 1);
                                base_start = Some(n);
                            }
                        }
                    }
                }
            }
        }

        // (B) search in base ,save found
        let base_level = self.base_level();
        // if start is none, use base head
        let base_start = base_start.map_or(self.base_head_ptr(), |n| n);
        let res = List::get_last_node_eq_or_less(key, base_start);
        match res {
            Some(n) => {
                search_result.save_base_node(base_level, n.last_node_less_or_equal, n.next_node)
            }
            None => {
                search_result.base_node_not_found(base_level);
            }
        }
        search_result
    }
    fn gc_when_necessary(&self) {}
    fn len(&self) -> usize {
        self.base.len()
    }

    fn get_index_level(&self, level: usize) -> Arc<List<K, Ref<K, V>>> {
        assert!(level <= MAX_LEVEL);
        self.levels.get(level).unwrap().clone()
    }

    fn base_level(&self) -> Arc<List<K, V>> {
        self.base.clone()
    }

    fn base_head_ptr(&self) -> *mut Node<K, V> {
        self.base.head()
    }

    fn random_level(&self, len: usize, rand_int: usize) -> usize {
        let m = max_level(len);
        if m == 0 {
            0
        } else {
            rand_int as usize % m
        }
    }

    fn current_max_level(&self) -> usize {
        self.current_max_level.load(Ordering::SeqCst)
    }
}

fn max_level(len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    let res = fast_math::log2(len as f32) as usize;
    return if res >= MAX_LEVEL { MAX_LEVEL } else { res };
}

impl<K: Copy + PartialOrd, V: Clone> SkipListImp<K, V> {
    pub fn get(&self, key: K) -> Option<V> {
        self.get_with_debug(key).0
    }
    pub fn get_with_debug(&self, key: K) -> (Option<V>, NodeSearchResult<K, V>) {
        let read_lock = self.lock.read().unwrap();
        let search_result = self.search_node(key);
        if let Some(node) = search_result.get() {
            unsafe { (Some(node.as_ref().unwrap().get_value()), search_result) }
        } else {
            (None, search_result)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::skip_list::skip_list_imp::{max_level, SkipListImp, MAX_LEVEL};
    use std::borrow::{Borrow, BorrowMut};
    use std::sync::Arc;
    use std::thread::spawn;

    #[test]
    fn test_max_level() {
        assert_eq!(max_level(1), 0);
        assert_eq!(max_level(2), 1);
        assert_eq!(max_level(3), 1);
        assert_eq!(max_level(8), 3);
        assert_eq!(max_level(165525), 16);
    }

    #[test]
    fn test_random_level() {
        let l: SkipListImp<i32, i32> = SkipListImp::new();
        assert_eq!(l.random_level(3, 100), 0);
        assert_eq!(l.random_level(0, 100), 0);
        assert_eq!(l.random_level(256, 100), 4);
    }

    #[test]
    fn test_get_empty() {
        let sk: SkipListImp<i32, i32> = SkipListImp::new();
        assert_eq!(sk.len(), 0);
        assert!(sk.get(3).is_none());
    }

    #[test]
    #[ignore]
    fn test_thread_bench() {}
    #[test]
    #[ignore]
    fn test_single_bench() {}
    #[test]
    #[ignore]
    fn test_multiple_thread_read_write_remove() {}
    #[test]
    #[ignore]
    fn test_gc() {
        todo!()
    }

    #[test]
    #[ignore]
    fn test_remove() {
        let sk = SkipListImp::new();
        for i in 0..16 {
            sk.add(i * 2, i, 0);
        }
        sk.add(21, 21, 3);
        sk.add(15, 20, 3);
        sk.add(17, 20, 1);
        sk.remove(17);
        assert_eq!(format!("{}", sk), "(15:(ref base 15):false)(21:(ref base 21):false)
(15:(ref level 15):false)(17:(ref base 17):true)(21:(ref level 21):false)
(0:0:false)(2:1:false)(4:2:false)(6:3:false)(8:4:false)(10:5:false)(12:6:false)(14:7:false)(15:20:false)(16:8:false)(17:20:true)(18:9:false)(20:10:false)(21:21:false)(22:11:false)(24:12:false)(26:13:false)(28:14:false)(30:15:false)\n");

        sk.remove(21);
        assert_eq!(format!("{}", sk), "(15:(ref base 15):false)(21:(ref base 21):false)
(15:(ref level 15):false)(17:(ref base 17):true)(21:(ref level 21):true)
(0:0:false)(2:1:false)(4:2:false)(6:3:false)(8:4:false)(10:5:false)(12:6:false)(14:7:false)(15:20:false)(16:8:false)(17:20:true)(18:9:false)(20:10:false)(21:21:true)(22:11:false)(24:12:false)(26:13:false)(28:14:false)(30:15:false)\n");
    }

    #[test]
    fn test_level() {
        let sk = SkipListImp::new();
        for i in 0..16 {
            sk.add(i * 2, i, 0);
        }
        sk.add(21, 21, 3);
        sk.add(17, 20, 1);
        sk.add(15, 20, 3);
        println!("{}", sk);
        let res = sk.get_with_debug(18);
        println!("{}", res.1);
    }

    #[test]
    fn test_one_level_add_get() {
        let sk = SkipListImp::new();
        sk.add(1, 1, 0);
        let res = sk.get(1).unwrap();
        assert_eq!(res, 1);
        sk.add(2, 2, 0);
        let res = sk.get(2).unwrap();
        assert_eq!(res, 2);
        // overwrite
        sk.add(2, 3, 0);
        let res = sk.get(2).unwrap();
        assert_eq!(res, 3);
    }

    #[test]
    fn test_concurrency() {
        let sk = Arc::new(SkipListImp::new());
        for i in 0..2 {
            let mut t = sk.clone();
            let j = spawn(move || {
                (t.borrow_mut() as &SkipListImp<i32, i32>).add(1, 2, 100);
            });
        }
    }
}
