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
use std::sync::{Arc, RwLock};

const MAX_LEVEL: usize = 32;

// todo need add arc,skip list need thread safe
struct SkipList<K: Copy + PartialOrd, V> {
    // levels len is MAX_LEVEL
    // not all level are in use
    levels: Vec<Arc<List<K, Ref<K, V>>>>,
    base: Arc<List<K, V>>,
    // gc need stop all other thread
    // gc thread: fetch write lock
    // other thread: fetch read lock
    lock: RwLock<()>,
    r: RefCell<Rand>,
    // todo use atmoic
    current_max_level: usize,
}

pub enum Ref<K: Copy + PartialOrd, V> {
    Base(*mut Node<K, V>),
    Level(*mut Node<K, Ref<K, V>>),
}

impl<K: Copy + PartialOrd, V> Clone for Ref<K, V> {
    fn clone(&self) -> Self {
        match self {
            Ref::Level(n) => Ref::Level(n.clone()),
            Ref::Base(n) => Ref::Base(n.clone()),
        }
    }
}

impl<K: Copy + PartialOrd, V> SkipList<K, V> {
    fn new() -> Self {
        let mut levels = vec![];
        for i in 0..MAX_LEVEL {
            let mut list: List<K, Ref<K, V>> = List::with_no_gc();
            levels.push(Arc::new(list));
        }
        SkipList {
            levels,
            base: Arc::new(List::with_no_gc()),
            lock: RwLock::new(()),
            r: RefCell::new(Rand::new()),
            current_max_level: 0,
        }
    }

    pub fn add(&mut self, key: K, value: V) {
        // read lock
        let read_lock = self.lock.read().unwrap();
        // call search

        let search_result = self.search_node(key);
        search_result.add_value_to_base(value);

        // cas insert all index nodes
        let level = self.random_level(self.len());
        // todo add random index
        search_result.add_index_to_level(level);
    }
    pub fn delete(&self, key: K) {
        // read lock
        let read_lock = self.lock.read().unwrap();

        // call search node
        let search_result = self.search_node(key);
        // if found ,delete it
        if let Some(node) = search_result.get_found_node() {
            //     todo mark as delete
        }
        // unlock for gc
        drop(read_lock);
        // check if need gc
        if self.need_do_gc() {
            let gc_lock = self.lock.write().unwrap();
            // do gc if needed
            self.gc()
        }
    }
    // search node level by level
    // return last node less or equal key, node next
    // record index node in search path
    fn search_node(&self, key: K) -> NodeSearchResult<K, V> {
        let mut search_result = NodeSearchResult::new();
        // from max level, find first index level whose head is less or equal key
        let max_level = self.current_max_level;
        let mut base_start = None;
        // if only base, to (B)
        if max_level > 0 {
            let mut start_level_option = None;
            for level in (1..max_level + 1).rev() {
                let list = self.get_index_level(level);
                unsafe {
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
            None => search_result.base_node_not_found(),
        }
        search_result
    }
    // check if need gc
    fn need_do_gc(&self) -> bool {
        // check remove count
        // get gc
        unimplemented!()
    }
    fn gc(&self) {}

    fn top_level_head(&self) -> &List<K, Ref<K, V>> {
        self.levels.get(self.current_max_level).unwrap()
    }
    fn top_level(&self) -> usize {
        self.current_max_level
    }

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

    fn random_level(&self, len: usize) -> usize {
        let m = max_level(len);
        if m == 0 {
            0
        } else {
            self.r.borrow_mut().next() as usize % m
        }
    }
}
fn max_level(len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    let res = fast_math::log2(len as f32) as usize;
    return if res >= MAX_LEVEL { MAX_LEVEL } else { res };
}

impl<K: Copy + PartialOrd, V: Clone> SkipList<K, V> {
    pub fn get(&self, key: K) -> Option<V> {
        let read_lock = self.lock.read().unwrap();
        let search_result = self.search_node(key);
        if let Some(node) = search_result.get_found_node() {
            unsafe { Some(node.as_ref().unwrap().get_value()) }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use crate::skip_list::skip_list_imp::{max_level, SkipList, MAX_LEVEL};

    #[test]
    fn test_max_level() {
        assert_eq!(max_level(1), 0);
        assert_eq!(max_level(2), 1);
        assert_eq!(max_level(3), 1);
        assert_eq!(max_level(8), 3);
    }
    #[test]
    fn test_random_level() {
        let l: SkipList<i32, i32> = SkipList::new();
        assert_eq!(l.random_level(3), 0);
        assert_eq!(l.random_level(0), 0);
        assert_eq!(l.random_level(256), 4);
    }
}
