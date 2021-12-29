mod skip_list {
    use std::cell::{RefCell, Ref};
    use std::rc::Rc;
    use std::borrow::{Borrow, BorrowMut};
    use std::ops::DerefMut;

    type BaseNodeInList<K, V> = Rc<RefCell<BaseNode<K, V>>>;
    type IndexNodeInList<K, V> = Rc<RefCell<IndexNode<K, V>>>;


    // lowest node  level=0
    struct BaseNode<K: Copy + PartialOrd, V> {
        key: K,
        value: V,
        right: Option<BaseNodeInList<K, V>>,
    }

    // level >1
    struct IndexNode<K: Copy + PartialOrd, V> {
        key: K,
        right: Option<IndexNodeInList<K, V>>,
        child: IndexNodeChild<K, V>,
    }


    enum IndexNodeChild<K: Copy + PartialOrd, V> {
        base(BaseNodeInList<K, V>),
        index(IndexNodeInList<K, V>),
    }

    struct SkipList<K: Copy + PartialOrd, V> {
        // head_bass_node: Option<BaseNode<K, V>>,
        indexes: Vec<IndexNodeInList<K, V>>,
        len: usize,
        current_max_level: usize,
    }

    impl<K: Copy + PartialOrd, V> SkipList<K, V> {
        // need handle overrite todo
        pub fn add(&mut self, key: K, value: V) {
            let n = self.top_head_index_node();
            if n.is_none() {
                // let node =
            }
            // if is empty
            // todo funciton
            SkipList::visit_level(key, n.unwrap(), |s| {}, SkipList::add_base);
            unimplemented!()
        }
        pub fn get(&self, key: K) {
            let n = self.top_head_index_node();
            // if is empty
            SkipList::visit_level(key, n.unwrap(), |s| {}, |s| {});
            unimplemented!()
        }
        pub fn remove(&mut self, key: K) {
            let n = self.top_head_index_node();
            // if is empty
            SkipList::visit_level(key, n.unwrap(), |s| {}, |s| {});
            unimplemented!()
        }

        fn visit_base(key: K, base_node: BaseNodeInList<K, V>, f: fn(&mut BaseNode<K, V>)) {
            let mut node: BaseNodeInList<K, V> = base_node.clone();
            loop {
                let mut n = (node.borrow() as (&RefCell<BaseNode<K, V>> )).borrow();
                let current_key = n.key;
                if current_key.le(&key) && n.right.is_some() {
                    let mut t = n.right.as_ref().unwrap().clone();
                    drop(n);
                    node = t;
                    // std::mem::swap(&mut node, &mut t);
                } else {
                    break;
                }
                // let d=n.right.borrow().unwrap();
            }
            f((node.borrow() as (&RefCell<BaseNode<K, V>> )).borrow_mut().deref_mut());
            // let mut node: IndexNodeInList = index_node.clone();
            // loop{
            //
            // }
        }


        fn visit_level(key: K, index_node: IndexNodeInList<K, V>, f: fn(IndexNodeInList<K, V>), f2: fn(&mut BaseNode<K, V>)) {
            let node = <SkipList<K, V>>::findLessNode(&key, index_node);
            let c = &(node.borrow() as (&RefCell<IndexNode<K, V>> )).borrow().child;
            match c {
                IndexNodeChild::base(t) => { SkipList::visit_base(key, t.clone(), f2) }
                IndexNodeChild::index(t) => { SkipList::visit_level(key, t.clone(), f, f2) }
            }
            // f(node.clone())
        }

        fn findLessNode(key: &K, index_node: IndexNodeInList<K, V>) -> IndexNodeInList<K, V> {
            let mut node: IndexNodeInList<K, V> = index_node.clone();
            loop {
                let mut n = (node.borrow() as (&RefCell<IndexNode<K, V>> )).borrow();
                let current_key = n.key;
                if current_key.le(&key) && n.right.is_some() {
                    let mut t = n.right.as_ref().unwrap().clone();
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
        fn top_head_index_node(&self) -> Option<IndexNodeInList<K, V>> {
            let node = self.indexes.get(self.current_max_level).unwrap();
            let s = (node.borrow() as (&RefCell<IndexNode<K, V>> )).borrow();
            if s.right.is_none() {
                return None;
            }
            let res = s.right.as_ref().unwrap().clone();
            Some(res)
        }

        fn new_index_node(key: K, right: Option<IndexNodeInList<K, V>>, child: IndexNodeChild<K, V>) -> IndexNodeInList<K, V> {
            Rc::new(RefCell::new(IndexNode { key, right, child }))
        }
        fn new_base_node(key: K, value: V, right: Option<BaseNodeInList<K, V>>) -> BaseNodeInList<K, V> {
            Rc::new(RefCell::new(BaseNode { key, value, right }))
        }

        fn add_base(node: &mut BaseNode<K, V>) {
            unimplemented!()
        }
    }
    // 1.  find nearest base node
    // a. handle emtpy list
    // b. find nearest index node in this level
    // c. go to lower level ,if is base to 2, else to b
    // 2. check base node one by one


    fn max_level(len: usize) -> usize {
        match len {
            0 => 0,
            _ => {
                let num: f64 = len as f64;
                num.log2().ceil() as usize
            }
        }
    }

    #[cfg(test)]
    mod test {
        use super::max_level;

        fn test_new_list() {
            //     create new skip list
            //     check fielda lens ,indexs etc
        }

        fn test_add_list() {
            //     add k,v to list
            //     check field
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




