#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
use crate::rand::simple_rand::Rand;
use crate::simple_list::list::{List, ListSearchResult};
use crate::simple_list::node::Node;
use std::sync::RwLock;

const MAX_LEVEL: i32 = 32;

struct SkipList<K: Copy + PartialOrd, V> {
    // levels len is MAX_LEVEL
    // not all level are in use
    levels: Vec<List<K, Ref<K, V>>>,
    base: List<K, V>,
    // gc need stop all other thread
    // gc thread: fetch write lock
    // other thread: fetch read lock
    lock: RwLock<()>,
    r: Rand,
    current_max_level: usize,
}

struct NodesOnSearchPath<K: Copy + PartialOrd, V> {
    result: Vec<ListSearchResult<K, V>>,
}

impl<K: Copy + PartialOrd, V> NodesOnSearchPath<K, V> {
    fn new() -> Self {
        unimplemented!()
    }

    fn push_index(&mut self, node: *mut Node<K, Ref<K, V>>, next_node: *mut Node<K, Ref<K, V>>) {
        unimplemented!()
    }

    fn pop_result(&mut self) -> Option<ListSearchResult<K, V>> {
        unimplemented!()
    }
}

// struct ListSearchResult<K: Copy + PartialOrd, V> {
//     last_node_less_or_equal: Ref<K, V>,
//     next_node: Ref<K, V>,
// }

enum Ref<K: Copy + PartialOrd, V> {
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
            levels.push(list);
        }
        SkipList {
            levels,
            base: List::with_no_gc(),
            lock: RwLock::new(()),
            r: Rand::new(),
            current_max_level: 0,
        };
        unimplemented!()
    }

    pub fn add(&self, key: K, value: V) {
        // read lock
        // call search
        // if found key, over write return
        // cas insert base node until success
        // get random level
        // cas insert all index nodes
        unimplemented!()
    }
    pub fn delete(&self, key: K) {
        // read lock
        // call search node
        // if found ,delete it
        // check if need gc
        // do gc if needed

        unimplemented!()
    }
    // search node level by level
    // return last node less or equal key, node next
    // record index node in search path
    // None if skip list is empty or all nodes key is big than key
    fn search_node(&self, key: K) -> Option<NodesOnSearchPath<K, V>> {
        if self.is_empty() {
            return None;
        }

        if self.base_head().unwrap().get_key() > key {
            return None;
        }

        let mut res: NodesOnSearchPath<K, V> = NodesOnSearchPath::new();

        // get index head node
        let current_level_list = self.top_level_head();
        let mut start_node = current_level_list.head();
        let base_start;
        // let current_level=self.top_level();

        // loop call list.get_node_less_or_equal
        unsafe {
            for current_level in (0..self.top_level()).rev() {
                let search_option = current_level_list.get_last_node_eq_or_less(key, start_node);
                match search_option {
                    Some(ListSearchResult {
                        last_node_less_or_equal,
                        next_node,
                    }) => {
                        let node_ref;
                        let next_node_ref;
                        node_ref = last_node_less_or_equal.as_ref().unwrap();
                        next_node_ref = next_node.as_ref().unwrap();
                        // check child
                        match (node_ref.get_value(), next_node_ref.get_value()) {
                            // if is index ,save to res ,use child as start node,continue loop,
                            (Ref::Level(n), Ref::Level(next)) => {
                                res.push_index(n, next);
                                start_node = n
                            }
                            (Ref::Base(n), Ref::Base(next)) => {
                                base_start = n;
                                break;
                                // if is base, set base start node, break loop
                                //     todo continue
                            }
                            // must same type
                            _ => {
                                panic!()
                            }
                        }
                    }
                    None => {
                        // if current start node is null, check from lower level start
                        if start_node.is_null() {
                            // start_node = self.level_head(current_level)
                        }
                        // check child
                        // if is index ,continue loop, use currnt node's child as start node
                        // if is base break
                        // let t = start_node.get_value();
                    }
                }
            }
        }
        unimplemented!()
        // save to result
        // break if child is base
        // search in base
        // Some(res)
    }
    fn check_gc(&self) -> bool {
        unimplemented!()
    }
    fn gc(&self) {}

    fn is_empty(&self) -> bool {
        unimplemented!()
    }
    fn top_level_head(&self) -> &List<K, Ref<K, V>> {
        self.levels.get(self.current_max_level).unwrap()
    }
    fn top_level(&self) -> usize {
        self.current_max_level
    }

    // fn base_head(&self) -> List<K, V> {
    //     unimplemented!()
    // }

    fn level_head(&self, level: usize) -> Option<Node<K, V>> {
        unimplemented!()
    }

    fn base_head(&self) -> Option<Node<K, V>> {
        unimplemented!()
    }
}

impl<K: Copy + PartialOrd, V: Clone> SkipList<K, V> {
    pub fn get(&self, key: K) -> Option<V> {
        // read lock
        // get current max level
        // search level by level
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    fn test() {}
}
