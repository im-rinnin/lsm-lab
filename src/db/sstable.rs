use file_store::FileStore;

// use crate::db::file_store::FileStore;
use crate::db::key::Key;
use crate::db::value::Value;

use super::file_store;

pub struct SSTable {}

// blocks meta store at first
struct StableMeta {}

// data block,4k default
struct Block {}

trait KVIter<'a>: Iterator<Item=(&'a Key, &'a Value)> {}


impl SSTable {
    pub fn range(&self) -> (Key, Key) {
        todo!()
    }
    pub fn get(&self, key: &Key) -> Option<&Value> {
        todo!()
    }
    // build new sstable,store in file store
    pub fn build(kv_iters: Vec<Box<dyn KVIter>>, file_store: &mut FileStore) -> Self {
        todo!()
    }
}