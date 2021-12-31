#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
// todo add log debug, trace

mod skip_list {
    use std::cell::{RefCell, Ref};
    use std::rc::Rc;
    use std::borrow::{Borrow};
    use std::ops::{Deref, DerefMut};
    use std::alloc::handle_alloc_error;
    use std::fs::read_to_string;
    use std::fmt::Display;

    type BaseNodeInList<K, V> = Rc<RefCell<BaseNode<K, V>>>;
    type IndexNodeInList<K, V> = Rc<RefCell<IndexNode<K, V>>>;
    use crate::rand::simple_rand;


    // lowest node  level=0
    struct BaseNode<K: Copy + PartialOrd, V> {
        pub key: K,
        pub value: V,
        right: Option<BaseNodeInList<K, V>>,
    }

    // level >1
    struct IndexNode<K: Copy + PartialOrd, V> {
        key: K,
        right: Option<IndexNodeInList<K, V>>,
        child: IndexNodeChild<K, V>,
    }


    enum IndexNodeChild<K: Copy + PartialOrd, V> {
        Base(BaseNodeInList<K, V>),
        Index(IndexNodeInList<K, V>),
    }

    struct SkipList<K: Copy + PartialOrd, V> {
        // head_bass_node: Option<BaseNode<K, V>>,
        indexes: Vec<IndexNodeInList<K, V>>,
        base_head: Option<BaseNodeInList<K, V>>,
        len: usize,
    }

    struct VisitorHandle<K: Copy + PartialOrd, V> {
        op: Operation,
        key: K,
        value: Option<V>,
        // some if overwrite
        old_value: Option<V>,
        index_nodes: Vec<IndexNodeInList<K, V>>,
        max_level: usize,
    }


    fn max_level(len: usize) -> usize {
        match len {
            0 => 0,
            _ => {
                let num: f64 = len as f64;
                num.log2().ceil() as usize
            }
        }
    }


    impl<K: Copy + PartialOrd, V> VisitorHandle<K, V> {
        fn with_add_op(key: K, value: V, max_level: usize) -> VisitorHandle<K, V> {
            VisitorHandle { op: Operation::Add, key, value: Some(value), old_value: None, index_nodes: vec![], max_level }
        }
        fn add_index_node(&mut self, node: IndexNodeInList<K, V>) {
            self.index_nodes.push(node);
        }
        fn handle_operation(&mut self, node: BaseNodeInList<K, V>) {
            match self.op {
                Operation::Add => {
                    let mut n = (node.borrow() as &RefCell<BaseNode<K, V>>).borrow_mut();
                    let node_key = n.key;
                    assert!(self.key >= node_key);
                    // add
                    if self.key > node_key {
                        let right = n.right.clone();
                        let new_node = SkipList::new_base_node(self.key, self.value.take().unwrap(), right);
                        n.right = Some(new_node);
                        // get random level
                        // build index node
                        // fix index node right  on the search path
                    } else {
                        //     override exit node value
                    }
                }
                Operation::Set => {}
                Operation::Remove => {}
            }
        }
        fn handle_index_operation(&self, node: IndexNodeInList<K, V>) {}
    }

    #[derive(Copy, Clone)]
    enum Operation {
        Add,
        Set,
        Remove,
    }

    struct SkipListIter<K: Copy + PartialOrd, V> {
        node: Option<BaseNodeInList<K, V>>,
    }


    impl<K: Copy + PartialOrd, V> Iterator for SkipListIter<K, V> {
        type Item = BaseNodeInList<K, V>;

        fn next(&mut self) -> Option<Self::Item> {
            let current = self.node.take();
            match current {
                Some(n) => {
                    self.node = (n.borrow() as &RefCell<BaseNode<K, V>>).borrow().right.clone();
                    Some(n)
                }
                None => {
                    None
                }
            }
        }
    }

    impl<K: Copy + PartialOrd + Display, V: Display> SkipList<K, V> {
        pub fn to_graph() -> String {
            String::from("todo")
        }
    }

    impl<K: Copy + PartialOrd, V> SkipList<K, V> {
        pub fn new() -> SkipList<K, V> {
            SkipList { indexes: Vec::new(), base_head: None, len: 0 }
        }
        pub fn to_iter(&self) -> SkipListIter<K, V> {
            SkipListIter { node: self.base_head.clone() }
        }
        // need handle overrite todo
        pub fn add(&mut self, key: K, value: V) {
            if self.is_empty() {
                let base_node = SkipList::new_base_node(key, value, None);
                self.base_head = Some(base_node);
                self.len += 1;

                // get random level
                // build index node
                // add index node to indexs
                return;
            }
            let head = self.get_head_base();
            // insert to head
            if (head.borrow() as &RefCell<BaseNode<K, V>>).borrow().key.gt(&key) {
                let new_node = SkipList::new_base_node(key, value, Some(head));
                self.base_head = Some(new_node);
                self.len += 1;
                return;
            }

            let mut visitor_handle = VisitorHandle::with_add_op(key, value, max_level(self.len));
            if self.is_one_level() {
                SkipList::visit_base(key, head, &mut visitor_handle);
                if visitor_handle.old_value.is_none() { self.len += 1; }
                return;
            }
            SkipList::visit_level(key, self.get_head_index(), &mut visitor_handle);
            // todo add 1 if add succese
            // self.len++1;
            unimplemented!()
        }
        pub fn get(&self, key: K) -> Option<&V> {
            // handle empty
            // handle one level
            // visit by visit handle
            unimplemented!()
        }
        pub fn remove(&mut self, key: K) {
            // handle empty
            // handle one levels,len -=1
            // visit by visit handle
            unimplemented!()
        }

        pub fn len(&self) -> usize {
            self.len
        }

        // ---------private-------------


        fn visit_base(key: K, base_node: BaseNodeInList<K, V>, handle: &mut VisitorHandle<K, V>) {
            let mut node: BaseNodeInList<K, V> = base_node.clone();

            loop {
                let n = (node.borrow() as &RefCell<BaseNode<K, V>>).borrow();
                let current_key = n.key;
                if current_key.lt(&key) && n.right.is_some() {
                    let t = n.right.as_ref().unwrap().clone();
                    drop(n);
                    node = t;
                } else {
                    break;
                }
            }
            handle.handle_operation(node);
        }


        fn visit_level(key: K, index_node: IndexNodeInList<K, V>, visitor_handle: &mut VisitorHandle<K, V>) {
            visitor_handle.add_index_node(index_node.clone());
            let node = <SkipList<K, V>>::find_less_node(&key, index_node);
            let c = &(node.borrow() as &RefCell<IndexNode<K, V>>).borrow().child;
            match c {
                IndexNodeChild::Base(t) => { SkipList::visit_base(key, t.clone(), visitor_handle) }
                IndexNodeChild::Index(t) => { SkipList::visit_level(key, t.clone(), visitor_handle) }
            }
        }

        fn find_less_node(key: &K, index_node: IndexNodeInList<K, V>) -> IndexNodeInList<K, V> {
            let mut node: IndexNodeInList<K, V> = index_node.clone();
            loop {
                let n = (node.borrow() as &RefCell<IndexNode<K, V>>).borrow();
                let current_key = n.key;
                if current_key.lt(&key) && n.right.is_some() {
                    let t = n.right.as_ref().unwrap().clone();
                    drop(n);
                    node = t;
                    // std::mem::swap(&mut node, &mut t);
                } else {
                    break;
                }
                // let d=n.right.borrow().unwrap();
            }
            node
        }
        fn is_empty(&self) -> bool {
            self.len == 0
        }

        fn is_one_level(&self) -> bool {
            self.indexes.len() == 0
        }

        fn get_head_base(&self) -> BaseNodeInList<K, V> {
            self.base_head.as_ref().unwrap().clone()
        }

        fn get_head_index(&self) -> IndexNodeInList<K, V> {
            unimplemented!()
        }

        fn new_index_node(key: K, right: Option<IndexNodeInList<K, V>>, child: IndexNodeChild<K, V>) -> IndexNodeInList<K, V> {
            Rc::new(RefCell::new(IndexNode { key, right, child }))
        }
        fn new_base_node(key: K, value: V, right: Option<BaseNodeInList<K, V>>) -> BaseNodeInList<K, V> {
            Rc::new(RefCell::new(BaseNode { key, value, right }))
        }

        fn add_base(&mut self, node: &BaseNode<K, V>) {
            unimplemented!()
        }

        fn random_level(&mut self, max_level: usize) -> usize {
            unimplemented!()
        }
    }
    // 1.  find nearest base node
    // a. handle emtpy list
    // b. find nearest index node in this level
    // c. go to lower level ,if is base to 2, else to b
    // 2. check base node one by one


    #[cfg(test)]
    // remember test head ,tail node and empty list
    mod test {
        use super::max_level;
        use crate::skip_list::skip_list::{SkipList};

        #[test]
        fn test_new_list() {
            let list: SkipList<i32, i32> = SkipList::new();
            assert_eq!(list.len, 0);
            assert_eq!(list.indexes.len(), 0);
            assert_eq!(list.base_head.is_none(), true);
            //     create new skip list
            //     check fielda lens ,indexs etc
        }

        #[test]
        fn test_iter_list() {
            let mut list: SkipList<i32, i32> = SkipList::new();
            list.add(1, 1);
            list.add(2, 2);
            list.add(0, 0);
            list.add(3, 3);
            list.add(-1, -1);
            list.add(-2, -2);
            list.add(4, 4);
            let mut iter = list.to_iter();
            assert_eq!((&iter.next().unwrap().borrow().key), &-2);
            assert_eq!((&iter.next().unwrap().borrow().key), &-1);
            assert_eq!((&iter.next().unwrap().borrow().key), &0);
            assert_eq!((&iter.next().unwrap().borrow().key), &1);
            assert_eq!((&iter.next().unwrap().borrow().key), &2);
            assert_eq!((&iter.next().unwrap().borrow().key), &3);
            assert_eq!((&iter.next().unwrap().borrow().key), &4);
            assert_eq!(iter.next().is_none(), true);
            assert_eq!(list.len, 7)
        }

        #[test]
        #[ignore]
        fn test_add_list() {
            let mut list: SkipList<i32, i32> = SkipList::new();
            list.add(1, 1);
            assert_eq!(1, list.len);
            assert_eq!(0, list.indexes.len());
            assert_eq!(true, list.base_head.is_some());
            assert_eq!(true, list.is_one_level());

            list.add(2, 2);
            assert_eq!(2, list.len);
            assert_eq!(list.get(2).unwrap(), &2);
            assert_eq!(list.get(1).unwrap(), &1);

            list.add(-1, 2);

            //     add k,v to list
            //     check field
        }

        #[test]
        #[ignore]
        fn test_overwrite_list() {
            let mut list: SkipList<i32, i32> = SkipList::new();
            list.add(1, 1);
            list.add(2, 2);
            list.add(1, 2);
            assert_eq!(list.get(1).unwrap(), &2);
            assert_eq!(list.get(2).unwrap(), &2);
        }


        fn test_remove_list() {
            //     remove from list
            //     check field
        }

        #[test]
        fn test_max_level() {
            assert_eq!(max_level(0), 0);
            assert_eq!(max_level(1), 0);
            assert_eq!(max_level(2), 1);
            assert_eq!(max_level(64), 6);
        }
    }
}




