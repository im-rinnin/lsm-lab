
use db_meta::DBMeta;
use key::Key;
use memtable::Memtable;
use value::Value;

pub mod key;
pub mod value;
mod sstable;
mod memtable;
mod db_meta;
mod level;
mod common;

mod version;


pub struct DB {
    path: String,
    db_meta: DBMeta,
    // levels: LevelInfos,
    memtables: Memtable,
}

pub struct MyError {}

impl DB {
    pub fn get(&self, key: &Key) -> Option<&Value> {
        todo!()
    }
    pub fn put(&mut self, key: &Key, value: Value) -> Result<(), MyError> {
        todo!()
    }

    pub fn new(path: String) -> Self {
        todo!()
    }
    pub fn close(self) -> Result<(), MyError> {
        todo!()
    }

    fn merge_sstable(&mut self) {
        todo!()
    }

    fn compact_routine(&mut self){

    }
}
