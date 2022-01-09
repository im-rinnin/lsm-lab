#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
use std::borrow::Borrow;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicI8, AtomicPtr, Ordering};
use std::sync::Arc;

pub struct Node<K: Copy + PartialOrd, V> {
    key: Arc<K>,
    value: AtomicPtr<V>,
    next_ptr: AtomicPtr<Node<K, V>>,
    gc_ptr: AtomicPtr<Node<K, V>>,
    state: AtomicI8,
}

impl<K: Copy + PartialOrd, V> Node<K, V> {
    pub fn with_key(key: K) -> Node<K, V> {
        Node {
            key: Arc::new(key),
            value: AtomicPtr::new(null_mut()),
            next_ptr: AtomicPtr::new(null_mut()),
            gc_ptr: AtomicPtr::new(null_mut()),
            state: AtomicI8::new(0),
        }
    }
    pub fn new(key: K, value: V, next: *mut Node<K, V>) -> Node<K, V> {
        Node {
            key: Arc::new(key),
            value: AtomicPtr::new(Box::into_raw(Box::new(value))),
            next_ptr: AtomicPtr::new(next),
            gc_ptr: AtomicPtr::new(null_mut()),
            state: AtomicI8::new(0),
        }
    }

    pub fn get_key(&self) -> K {
        *self.key.borrow()
    }
    pub fn set_value(&mut self, value_ptr: *mut V) {
        let ptr = self.value.load(Ordering::SeqCst);
        drop(ptr);
        self.value = AtomicPtr::new(value_ptr);
    }

    pub fn set_next_ptr(&mut self, ptr: *mut Self) {
        self.next_ptr.swap(ptr, Ordering::SeqCst);
    }

    pub fn is_deleted(&self) -> bool {
        self.state.load(Ordering::SeqCst) > 0
    }
    pub fn set_deleted(&mut self) {
        self.state.fetch_add(1, Ordering::SeqCst);
    }

    pub fn get_next(&self) -> *mut Self {
        self.next_ptr.load(Ordering::SeqCst)
    }
    pub fn cas_next_ptr(&self, new_ptr: *mut Self) -> bool {
        let current_ptr = self.next_ptr.load(Ordering::SeqCst);
        let res = self.next_ptr.compare_exchange(
            current_ptr,
            new_ptr,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        res.is_ok()
    }
}

impl<K: Copy + PartialOrd, V: Clone> Node<K, V> {
    pub fn get_value(&self) -> V {
        unsafe {
            let t = self.value.load(Ordering::SeqCst).as_ref().unwrap().clone();
            t
        }
    }
}

// todo called by gc
impl<K: Copy + PartialOrd, V> Drop for Node<K, V> {
    fn drop(&mut self) {
        let ptr = self.value.load(Ordering::SeqCst);
        unsafe {
            ptr.drop_in_place();
        }
    }
}

mod test {
    use super::Node;
    use std::borrow::{Borrow, BorrowMut};
    use std::cell::RefCell;
    use std::ptr::null_mut;
    use std::rc::Rc;

    struct Item {
        i: Rc<RefCell<i32>>,
    }
    impl Drop for Item {
        fn drop(&mut self) {
            let tmp: &RefCell<i32> = self.i.borrow();
            let mut t = tmp.borrow_mut();
            *t += 1;
        }
    }

    #[test]
    fn test_node_drop() {
        let n = Rc::new(RefCell::new(0));
        assert_eq!(*(n.borrow() as &RefCell<i32>).borrow_mut(), 0);
        let item = Item { i: n.clone() };
        {
            let node = Node::new(3, item, null_mut());
        }
        assert_eq!(*(n.borrow() as &RefCell<i32>).borrow_mut(), 1);

        // check key value is dropped
    }
}
