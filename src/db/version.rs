use std::fs::File;
use std::sync::{Arc, Mutex};

use crate::db::file_storage::{FileId, FileStorageManager};
use crate::db::key::Key;
use crate::db::level::{Level, LevelChange, SStableFileMeta};
use crate::db::memtable::Memtable;
use crate::db::meta_log::MetaLog;
use crate::db::value::Value;

// all sstable meta
// immutable, thread safe,create new version after insert new sstable/compact
pub struct Version {
    // all level info,order by level number,vec[0]->level 0
    levels: Vec<Level>,
    file_manager: FileStorageManager,
}

impl Version {
    pub fn new() -> Self {
        todo!()
    }
    pub fn from(meta_log: MetaLog) -> Self {
        // read log and build version from log
        todo!()
    }

    pub fn get(&self, key: &Key) -> Option<Value> {
        // call get key from level 0 to level n
        todo!()
    }

    // return all sstable in level 0
    pub fn add_new_sstable_to_level_0(&self, memtable: Memtable) -> (Self, Vec<FileId>) {
        // build sstable from memtable (sstable::build)
        // create table
        todo!()
    }

    // return all sstable file id in level
    pub fn compact_level(&self, level: usize) -> (Self, Vec<FileId>) {
        todo!()
    }

    pub fn from_level_change(&self, level_change: &LevelChange) -> Self {
        todo!()
    }


    // for sstable file prune
    pub fn all_file_id(&self) -> Vec<FileId> {
        todo!()
    }

    pub fn get_level_file_meta(&self, level: usize) -> Vec<SStableFileMeta> {
        todo!()
    }

    pub fn get_level(&self, level: usize) -> Option<&Level> {
        self.levels.get(level)
    }

    pub fn depth(&self) -> usize {
        self.levels.len()
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
