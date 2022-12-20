use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::Sender;

use anyhow::Result;

use key::Key;
use memtable::Memtable;
use value::Value;

use crate::db::file_storage::FileId;
use crate::db::memtable_log::MemtableLog;
use crate::db::meta_log::MetaLog;
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

type ThreadSafeVersion = Arc<Version>;

// todo thread safe design multiple thread access
pub struct DB {
    path: String,
    // todo add mutex
    current_memtable_ref: Arc<Mutex<RefCell<Rc<Memtable>>>>,
    // all memtable need to compact to level 0,new memtable push to queue front,
    memtable_to_be_compact: Arc<Mutex<VecDeque<Rc<Memtable>>>>,
    memtable_log: MemtableLog,
    versions: VecDeque<ThreadSafeVersion>,
    meta_log: MetaLog,
    // record file use count, init in db start, own by delete_unused_sstable_file_routine
    sstable_file_usage_count: HashMap<FileId, usize>,
}

// todo use myerror create
pub struct MyError {}

impl DB {
    pub fn get(&self, key: &Key) -> Result<Option<&Value>,MyError> {
        // search in current memtable
        // search in memtable_to_be_compact
        // search in current version
        todo!()
    }
    pub fn put(&mut self, key: &Key, value: Value) -> Result<(), MyError> {
        // put in current memtable,put int memtable_log
        // if memtable is not full, return
        // check compact trigger by cas
        // add memtable to memtable_to_be_compact, create new empty memtable

        // call compact thread do memtable compact by channel

        // return
        todo!()
    }


    pub fn open_db(path: String) -> Result<Self> {
        //     open memtable_log
        //     build version from log
        // call init routine
        todo!()
    }

    pub fn new(path: String) -> Result<Self> {
        // create open memtable_log
        // create file_manager
        // call init routine
        todo!()
    }
    pub fn close(self) -> Result<(), MyError> {
        todo!()
    }

    fn compact_routine(&mut self) {
        // 1. block on memtable need compact channel
        // 2. build sstable from memtable, add to level 0, write level change to meta_log,create new version
        // 3. check level 1 ,do compact if need ,repeat,return to step 1
        todo!()
    }

    // start routine in new() and from()
    fn init_routine(&self) {
        // start compact routine
    }
}


