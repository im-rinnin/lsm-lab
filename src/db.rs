use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::ops::Deref;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::mpsc::{Receiver, Sender};

use anyhow::Result;
use serde_json::{from_str, to_string};

use key::Key;
use memtable::Memtable;
use value::Value;

use crate::db::file_storage::{FileId, ThreadSafeFileManager};
use crate::db::level::{Level, LevelChange, SStableFileMeta};
use crate::db::memtable::MemtableReadOnly;
use crate::db::memtable_log::MemtableLog;
use crate::db::meta_log::MetaLog;
use crate::db::sstable::SSTable;
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

type ThreadSafeVersion = Arc<RwLock<Arc<Version>>>;

// todo thread safe design multiple thread access
pub struct DBServer {
    path: String,
    current_memtable_ref: Arc<RwLock<Rc<Memtable>>>,
    immutable_memtable_ref: Arc<RwLock<Option<Rc<MemtableReadOnly>>>>,
    versions: ThreadSafeVersion,
    meta_log: MetaLog,
    file_manager: ThreadSafeFileManager,
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

    fn compact_routine(&mut self, do_compact: Receiver<()>) -> Result<()> {
        // block on memtable need compact channel
        for _ in do_compact.iter() {
            // compact memtable
            let compact_memtable_level_change = self.compact_memtable()?;
            // change version
            let version = self.build_version(&compact_memtable_level_change);
            self.save_level_change_to_meta_log(&compact_memtable_level_change);
            self.set_current_version(version);
            // compact level
            let compact_level_change_option = self.compact_level()?;
            // change version
            if let Some(compact_level_change) = compact_level_change_option {
                let version = self.build_version(&compact_level_change);
                self.set_current_version(version);
            }
        }
        Ok(())
    }

    fn build_version(&self, level_change: &LevelChange) -> Version {
        let current_version = self.versions.read().expect("current version lock failed");
        let new_version = current_version.from_level_change(level_change);
        new_version
    }


    fn save_level_change_to_meta_log(&mut self, level_change: &LevelChange) -> Result<()> {
        let data = serde_json::to_string(level_change)?;
        self.meta_log.add_data(data.as_bytes())
    }

    fn set_current_version(&mut self, version: Version) -> Result<()> {
        let mut current_version = self.versions.write().expect("current version lock failed");
        *current_version = Arc::new(version);
        Ok(())
    }

    fn compact_memtable(&mut self) -> Result<LevelChange> {
        // - build sstable from memtable, add to level 0,
        let memtable = self.immutable_memtable_ref.read().unwrap();
        let a = memtable.as_deref();
        let d = a.expect("should have immutable memtable");
        let c: &MemtableReadOnly = d.borrow();
        let mut m = self.file_manager.lock().expect("fail to get file manager lock");
        let h = m.borrow_mut();
        let sstable_file_metas = Level::write_memtable_to_sstable_file(c, h)?;
        let level_change = LevelChange::MEMTABLE_COMPACT { sstable_file_metas };
        Ok(level_change)
    }

    // check and find one level to compact
    fn compact_level(&mut self) -> Result<Option<LevelChange>> {
        // - check level 1 to n ,find first level need compact,pick random file to compact for this level
        let version = self.versions.read().unwrap();
        let version_ref = version.as_ref();
        for i in 0..version_ref.depth() {
            let level_metas = version_ref.get_level_file_meta(i);
            if let Some(picked_sstable) = Self::pick_file_to_compact(level_metas) {
                let next_level_opt = version_ref.get_level(i + 1);
                if let Some(next_level) = next_level_opt {
                    let res = next_level.compact_sstable(picked_sstable)?;
                    // @continue
                    //     return
                } else {
                    // return Ok(Some(LevelChange::LEVEL_COMPACT { compact_from_level: i, remove_sstable_file_ids: Vec::new(), add_sstable_file_metas: picked_sstable }));
                }
            }
        }
        Ok(None)
    }

    fn pick_file_to_compact(level_metas: Vec<SStableFileMeta>) -> Option<Vec<SStableFileMeta>> {
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
        let mut handles = Vec::new();
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
            let handle = handles.pop().unwrap();
            handle.join();
        }
        db_server.close();
    }
}
