use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::db::file_storage::{FileId, FileStorageManager, ThreadSafeFileManager};
use crate::db::key::Key;
use crate::db::level::{Level, LevelChange, SStableFileMeta, ThreadSafeSSTableMetaCache};
use crate::db::memtable::Memtable;
use crate::db::meta_log::{MetaLog, MetaLogIter};
use crate::db::value::Value;

// all sstable meta
// immutable, thread safe,create new version after insert new sstable/compact
pub struct Version {
    // all level info,order by level number,vec[0]->level 0
    levels: HashMap<usize, Level>,
    sstable_cache: ThreadSafeSSTableMetaCache,
    file_manager: ThreadSafeFileManager,
    home_path: PathBuf,
}

impl Version {
    pub fn new(home_path: PathBuf, file_manager: ThreadSafeFileManager, sstable_cache: ThreadSafeSSTableMetaCache) -> Self {
        Version { levels: HashMap::new(), sstable_cache, file_manager, home_path }
    }
    pub fn from(meta_log: MetaLogIter, home_path: PathBuf, file_manager: ThreadSafeFileManager) -> Result<Self> {
        //     iter meta log,get level change
        // for data in meta_log {
        //     let level_change: LevelChange = serde_json::from_slice(data?.as_slice())?;
        //     match level_change {
        //         LevelChange::LEVEL_COMPACT(compact_from_level,
        //                                    remove_sstable_file_ids,
        //                                    add_sstable_file_metas) => {
        //
        //         }
        //         LevelChange::MEMTABLE_COMPACT(sstable_file_metas) => {}
        //     }
        // }
        todo!()
        //     apply level change to level n
    }

    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        // call get key from level 0 to level n
        for level in &self.l {
            let res = level.get(&key)?;
            if let Some(v) = res {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    // return all sstable in level 0
    // pub fn add_new_sstable_to_level_0(&self, memtable: Memtable) -> (Self, Vec<FileId>) {
    // build sstable from memtable (sstable::build)
    // create table
    // todo!()
    // }

    // return all sstable file id in level
    // pub fn compact_level(&self, level: usize) -> (Self, Vec<FileId>) {
    //     todo!()
    // }

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
        self.levels.get(&level)
    }

    // return None if is empty
    pub fn depth(&self) -> Option<usize> {
        *self.levels.keys().max()
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
