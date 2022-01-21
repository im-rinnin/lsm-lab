use crate::simple_list::list::List;
use crate::simple_list::list::ListSearchResult;
use crate::simple_list::node::Node;
use crate::skip_list::skip_list_imp::Ref;
use std::borrow::Borrow;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

struct LevelInfo<K: Copy + PartialOrd, V> {
    list: Arc<List<K, V>>,
    res: ListSearchResult<K, V>,
}
pub struct NodeSearchResult<K: Copy + PartialOrd, V> {
    // FIFO
    // (highest level .... level 1)
    index_node: Vec<LevelInfo<K, Ref<K, V>>>,
    base_result: Option<ListSearchResult<K, V>>,
    base: Option<Arc<List<K, V>>>,
    key: K,
}

impl<K: Copy + PartialOrd + Display, V: Clone + Display> Display for NodeSearchResult<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut res = String::new();
        for l in self.index_node.iter().rev() {
            unsafe {
                let n = l.res.last_node_less_or_equal.as_ref().unwrap();
                let node_ref = n.get_value();
                let s = match node_ref {
                    Ref::Level(_) => {
                        format!("(index key {})", n.get_key())
                    }
                    Ref::Base(_) => {
                        format!("(base key {})", n.get_key())
                    }
                };
                res.push_str(s.as_str());
                res.push_str("\n");
            }
        }
        write!(f, "{}", res)
    }
}

impl<K: Copy + PartialOrd, V> NodeSearchResult<K, V> {
    pub fn new(key: K) -> Self {
        NodeSearchResult {
            index_node: vec![],
            base_result: None,
            base: None,
            key,
        }
    }

    pub fn add_index_to_level(&self, level: usize, base_node: *mut Node<K, V>) -> Ref<K, V> {
        let mut node_ref = Ref::Base(base_node);
        let mut current_level = 1;
        for level_info in self.index_node.iter().rev() {
            match level_info {
                LevelInfo { list, res } => {
                    let res = (list.borrow() as &List<K, Ref<K, V>>).cas_insert(
                        res.last_node_less_or_equal,
                        self.key,
                        node_ref,
                    );
                    assert!(res.is_some());
                    node_ref = Ref::Level(res.unwrap());
                }
            }
            if current_level == level {
                break;
            }
            current_level += 1;
        }
        node_ref
    }

    pub fn delete_value(&self) {
        if let Some(node) = self.get() {
            unsafe {
                node.as_mut().unwrap().set_deleted();
            }
            for level_info in self.index_node.iter().rev() {
                unsafe {
                    let index_node = level_info.res.last_node_less_or_equal.as_mut().unwrap();
                    if index_node.get_key() == self.key {
                        index_node.set_deleted();
                    } else {
                        break;
                    }
                }
            }
        }
    }

    pub fn add_value_to_base(&self, value: V) -> Option<*mut Node<K, V>> {
        // assert!(self.base_result.is_some());
        let base = self.base.as_ref().unwrap();
        match &self.base_result {
            Some(n) => base.cas_insert(n.last_node_less_or_equal, self.key, value),
            None => base.cas_insert(base.head(), self.key, value),
        }
    }
    pub fn save_index_node(
        &mut self,
        list: Arc<List<K, Ref<K, V>>>,
        node: *mut Node<K, Ref<K, V>>,
        next_node: *mut Node<K, Ref<K, V>>,
    ) {
        #[cfg(test)]
        {
            //     todo assert
        }
        self.index_node.push(LevelInfo {
            list,
            res: ListSearchResult::new(node, next_node),
        });
    }
    pub fn base_node_not_found(&mut self, base_list: Arc<List<K, V>>) {
        self.base = Some(base_list);
    }
    pub fn save_base_node(
        &mut self,
        list: Arc<List<K, V>>,
        node: *mut Node<K, V>,
        next_node: *mut Node<K, V>,
    ) {
        self.base_result = Some(ListSearchResult::new(node, next_node));
        self.base = Some(list);
    }

    // for get
    pub fn get(&self) -> Option<*mut Node<K, V>> {
        match &self.base_result {
            Some(res) => unsafe {
                let n = res.last_node_less_or_equal.as_ref().unwrap();
                if n.is_deleted() {
                    return None;
                }
                if n.get_key() != self.key {
                    return None;
                }
                Some(res.last_node_less_or_equal)
            },
            None => None,
        }
    }

    pub fn get_index_level(&self) -> usize {
        self.index_node.len()
    }
}
