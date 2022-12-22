use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::mpsc::{Receiver, Sender};

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
pub struct DBServer {
    path: String,
    current_memtable_ref: Arc<RwLock<RefCell<Rc<Memtable>>>>,
    immutable_memtable_ref: Arc<RwLock<RefCell<Rc<Memtable>>>>,
    versions: ThreadSafeVersion,
    meta_log: MetaLog,
}

#[derive(Clone)]
pub struct DBClient {
    current_memtable_ref: Arc<RwLock<Arc<Memtable>>>,
    immutable_memtable_ref: Arc<RwLock<Arc<Memtable>>>,
    versions: ThreadSafeVersion,
}

// todo use myerror create
#[derive(Debug)]
pub struct MyError {}

impl DBClient {
    pub fn get(&self, key: &Key) -> Result<Option<&Value>, MyError> {
        // search in current memtable
        // let memtable=(*self.current_memtable_ref).read();
        // if memtable.is_err(){
        //     return Err(MyError::)
        // }

        // search in memtable_to_be_compact
        // search in current version
        todo!()
    }

    pub fn put(&mut self, key: &Key, value: Value) -> Result<(), MyError> {
        // check if level 0 file number, if >8 , wait 1ms
        // check and return error if current memtable is full
        // put in current memtable,put int memtable_log
        // if memtable is not full, return
        // check compact trigger by cas
        // add memtable to memtable_to_be_compact, create new empty memtable

        // call compact thread do memtable compact by channel

        // return
        todo!()
    }
}

impl DBServer {
    pub fn open_db(path: String) -> Result<Self> {
        //     open memtable_log
        //     build version from log
        // call init routine
        todo!()
    }

    pub fn new_client(&self) -> Result<DBClient> {
        todo!()
    }
    pub fn new(path: &Path) -> Result<Self> {
        // create open memtable_log
        // create file_manager
        // call init routine
        todo!()
    }
    pub fn close(self) -> Result<(), MyError> {
        // wait all client exit()
        // stop routine

        todo!()
    }

    fn compact_routine(&mut self, receive: Receiver<Rc<Memtable>>) {
        // 1. block on memtable need compact channel
        // 2. build sstable from memtable, add to level 0, write level change to meta_log,create new version
        // 3. check level 1 ,do compact if need ,repeat,return to step 1
        todo!()
    }

    // start routine in new() and from()
    fn init_routines(&self) {
        // start compact routine
    }

    fn stop_routines(&self) {}
}


#[cfg(test)]
mod test {
    use std::thread;

    use tempfile::TempDir;

    use crate::db::DBServer;
    use crate::db::key::Key;
    use crate::db::value::Value;

    #[test]
    fn test_db_build_and_reopen() {
        //     build db from path
        //     close db
        //     reopen db
    }

    #[test]
    fn test_db_simple_get_set() {
        //     new db
        //     set a a
        //     get a
    }

    // todo #[test]
    fn test_db_multiple_thread_get_set() {
        //     new db
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path();
        let mut db_server = DBServer::new(dir_path).unwrap();
        //     create 3 thread
        let mut handles=Vec::new();
        for i in 0..3 {
            let mut db_client = db_server.new_client().unwrap();
            let handle = thread::spawn(move || {
                //     for each thread do set from 1 to 1000, and check by get key
                for i in 0..1000 {
                    db_client.put(&Key::from(i.to_string().as_bytes()), Value::new(&i.to_string()));
                }
                for i in 0..1000 {
                    let value = db_client.get(&Key::from(i.to_string().as_bytes())).unwrap().unwrap();
                    assert_eq!(*value, Value::new(&i.to_string()))
                }
            });
            handles.push(handle);
        }
        while handles.len() > 0 {
            let handle=handles.pop().unwrap();
            handle.join();
        }
        db_server.close();
    }
}
