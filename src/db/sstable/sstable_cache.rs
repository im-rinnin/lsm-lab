use std::sync::Arc;
use crate::db::file_storage::FileId;
use crate::db::sstable::SStableMeta;

// lru cache
// todo thread safe arc mutex
pub struct SSTableMetaCache {}

impl SSTableMetaCache {
    pub fn new(capacity: usize) -> Self { todo!() }

    pub fn add(&mut self, sstable_meta: SStableMeta, file_id: FileId) {}
    pub fn get(&self, file_id: FileId) -> Option<Arc<SStableMeta>> {
        todo!()
    }
}