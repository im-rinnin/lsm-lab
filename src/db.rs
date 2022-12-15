use key::Key;
use memtable::Memtable;
use value::Value;
use crate::db::memtable_log::MemtableLog;
use crate::db::meta_log::MetaLog;

use anyhow::Result;
use crate::db::version::Version;

pub mod key;
pub mod value;
mod sstable;
mod memtable;
mod level;
mod common;
mod file_storage;
mod meta_log;
mod memtable_log;

mod version;


// tood thread safe
pub struct DB {
    path: String,
    memtables: Memtable,
    memtable_log:MemtableLog,
    current: Version,
    oldest: Version,
    meta_log:MetaLog,
}

pub struct MyError {}

impl DB {
    pub fn get(&self, key: &Key) -> Option<&Value> {
        // search in memtable
        // search in current version
        todo!()
    }
    pub fn put(&mut self, key: &Key, value: Value) -> Result<(), MyError> {
        // put in memtable,put int memtable_log
        // if memtable is not full, return
        // create sstable from memtable, add it to level 0
        // check if need compact, if not return
        // do compact, get new sstable, build new level, set current to new level
        // check version number, discard oldest version if reach limit
        // find all unused file, delete it
        todo!()
    }


    pub fn open_db(path: String) -> Result<Self> {
        todo!()
    }
    pub fn new(path: String) -> Result<Self> {
        todo!()
    }
    pub fn close(self) -> Result<(), MyError> {
        todo!()
    }

    fn check_if_need_compact(&self) -> bool {
        todo!()
    }

    fn compact_routine(&mut self) {
        todo!()
    }
}


