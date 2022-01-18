use crate::simple_list::list::List;
use crate::simple_list::list::ListSearchResult;
use crate::simple_list::node::Node;
use crate::skip_list::skip_list_imp::Ref;
use std::sync::Arc;

struct LevelInfo<K: Copy + PartialOrd, V> {
    list: Arc<List<K, V>>,
    res: ListSearchResult<K, V>,
}
pub struct NodeSearchResult<K: Copy + PartialOrd, V> {
    // FIFO
    // (highest level .... level 1)
    index_node: Vec<LevelInfo<K, Ref<K, V>>>,
    base_result: Option<LevelInfo<K, V>>,
    // fault false
    node_not_found: bool,
    key: K,
}

impl<K: Copy + PartialOrd, V> NodeSearchResult<K, V> {
    pub fn new() -> Self {
        unimplemented!()
    }

    // return node if key is found(not delete)
    pub fn get_found_node(&self) -> Option<*mut Node<K, V>> {
        unimplemented!()
    }

    pub fn overwrite_found_node(&self, value: V) {
        assert!(self.base_result.is_some());
        let mut node = self
            .base_result
            .as_ref()
            .unwrap()
            .res
            .last_node_less_or_equal;
        unsafe {
            node.as_mut()
                .unwrap()
                .set_value(Box::into_raw(Box::new(value)));
        }
    }

    pub fn add_index_to_level(&self, level: usize) {
        // todo
        unimplemented!()
    }

    pub fn add_value_to_base(&self, value: V) {
        // todo
        // assert!(self.base_result.is_some());
        // match &self.base_result {
        //     Some(n) => {}
        //     None => {}
        // }
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
    pub fn save_base_node(
        &mut self,
        list: Arc<List<K, V>>,
        node: *mut Node<K, V>,
        next_node: *mut Node<K, V>,
    ) {
        self.base_result = Some(LevelInfo {
            list,
            res: ListSearchResult::new(node, next_node),
        });
    }

    pub fn base_node_not_found(&mut self) {
        self.node_not_found = true
    }

    // for get
    fn get(&self) -> Option<*mut Node<K, V>> {
        unimplemented!()
    }
}
