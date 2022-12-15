use std::fs::File;
use std::sync::Arc;

use crate::db::file_storage::{FileId, FileStorageManager};
use crate::db::key::Key;
use crate::db::level::Level;
use crate::db::memtable::Memtable;
use crate::db::value::Value;

// all sstable meta
// immutable, thread safe,create new version after insert new sstable/compact
pub struct Version {
    // all level info,order by level number,vec[0]->level 0
    levels: Vec<Level>,
    // version create time
    timestamp:u64,
    file_manager: FileStorageManager,
    version_next: Option<Arc<Version>>,
}

impl Version {
    pub fn new() -> Self {
        todo!()
    }
    pub fn from_sstable_ids(file: Vec<Vec<FileId>>) -> Self {
        todo!()
    }

    pub fn get(&self, key: &Key) -> Option<Value> {
        todo!()
    }

    // return all sstable in level 0
    pub fn add_new_sstable_to_level_0(&self, memtable: Memtable) -> (Self, Vec<FileId>) {
        todo!()
    }

    // return all sstable file id in level
    pub fn compact_level(&self, level: usize) -> (Self, Vec<FileId>) {
        todo!()
    }
}

#[cfg(test)]
mod test {
    pub fn build_level() {}

    #[test]
    pub fn test_set() {}

    #[test]
    pub fn test_get() {}

    #[test]
    pub fn test_compact() {}

    #[test]
    pub fn test_add_sstable_from_memtable() {}
}
